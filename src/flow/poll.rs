// Per-flow poll lifecycle: long-running loop that wraps the one-shot
// strategies in `crate::git` (auto_detect + per-strategy poll +
// compare_sha) and threads observations to the state writer. Emits
// `TriggerSignal` on a SHA diff.
//
// Behavioral contract:
//   - Strategy dispatch is URL-driven: `auto_detect(url)` picks
//     GithubApi / Grokmirror / LsRemote per the source URL's host
//     (or path), and the loop never asks for an explicit strategy
//     override.
//   - Each cycle is paced by `source_interval`, jittered through
//     `apply_jitter`, and gated by an optional per-credential
//     `RateBucket` so concurrent flows sharing one credential
//     don't pile up on the upstream. A `Refreshed` outcome whose
//     SHA differs from the previously-recorded value emits a
//     `TriggerSignal` to the dispatcher.
//   - State updates emitted by the loop: `PollObservation` on a
//     fresh ObjectId (the durable record of "this flow saw
//     this SHA at this time"), and `PollTimestamp` on a fast-path
//     "no change" (e.g. grokmirror manifest fingerprint match) so
//     `last_poll_at` reflects liveness even when the source SHA
//     is unchanged.
//
// Cancellation: a `CancellationToken` passed in by the supervisor
// signals shutdown. The loop checks on every tick boundary and on
// every fallible API call.
//
// Per-strategy fan-out:
//   - PollStrategy::GithubApi -> octocrab::Octocrab via
//     `crate::github::Client`. The credential resolver lives in the
//     supervisor; this module receives the octocrab handle pre-built.
//   - PollStrategy::Grokmirror -> shared reqwest::Client + the
//     repo-path extracted from the source URL. Same supervisor-owned
//     handle.
//   - PollStrategy::LsRemote -> URL + ref_name (no auth).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use octocrab::Octocrab;
use reqwest::Client as ReqwestClient;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::git::{
    auto_detect, compare_sha, default_interval, github_api, grokmirror, ls_remote, strategy,
    PollOutcome, PollStrategy, RateBucket,
};
use crate::state::StateUpdate;

use super::supervisor::record_last_error;
use super::TriggerSignal;

/// Wall-clock cadences + jitter for one flow. Built from
/// `FlowConfig.poll` overlaid on `PollDefaults`. The supervisor is
/// the only caller and computes this once per flow at startup AND
/// on every config reload. Carries both `source_interval` (poll loop)
/// and `job_interval` (per-run monitor loop) so the supervisor reads
/// both cadences from one place rather than threading them
/// separately.
#[derive(Debug, Clone, Copy)]
pub struct EffectivePoll {
    /// Base wall-clock interval between source polls. Already
    /// validated to be at least `git::MIN_INTERVAL` (15s) by config
    /// validation.
    pub source_interval: Duration,
    /// Per-run monitor cadence. Used by `flow::monitor` and threaded
    /// into `github::monitor::MonitorParams.job_interval`.
    pub job_interval: Duration,
    /// Jitter fraction applied to `source_interval` per
    /// `git::strategy::apply_jitter`. 0.0..=0.5.
    pub jitter: f64,
}

impl EffectivePoll {
    /// Compute the effective poll cadences by overlaying a flow's
    /// `PollOverride` on the global `PollDefaults`. The strategy's
    /// default `source_interval` (per `git::default_interval`) is the
    /// bottom fallback when neither config layer pinned a value;
    /// `job_interval` always falls back to `PollDefaults.job_interval`
    /// because the monitor cadence is strategy-agnostic.
    pub fn compute(
        defaults: &crate::config::PollDefaults,
        override_: &crate::config::PollOverride,
        strategy: PollStrategy,
    ) -> Self {
        let source_interval = override_
            .source_interval
            .or(defaults.source_interval)
            .unwrap_or_else(|| default_interval(strategy));
        let job_interval = override_.job_interval.unwrap_or(defaults.job_interval);
        let jitter = override_.jitter.unwrap_or(defaults.jitter);
        Self {
            source_interval,
            job_interval,
            jitter,
        }
    }
}

/// Per-flow poll-task parameters. Owned by the supervisor and built
/// from the flow's `SourceConfig` + the resolved transport handles.
pub struct PollParams {
    /// Stable name of the flow — surfaced in tracing spans and
    /// embedded in `StateUpdate::PollObservation { flow }`.
    pub flow_name: String,
    /// `flow.source.url`. Determines strategy + transport.
    pub url: String,
    /// `flow.source.ref` (e.g. `refs/heads/main`). Threaded into the
    /// per-strategy poll calls.
    pub ref_name: String,
    /// Effective cadence + jitter (see `EffectivePoll::compute`).
    pub effective_poll: EffectivePoll,
    /// Per-credential rate bucket. The poll task acquires before each
    /// network round-trip so multiple flows sharing a credential
    /// don't hit the upstream simultaneously. `None` for source-side
    /// fetches that don't need throttling (file:// URLs).
    pub rate_bucket: Option<Arc<RateBucket>>,
    /// Octocrab handle for `PollStrategy::GithubApi`. `None` for the
    /// other strategies — the supervisor only supplies it when the
    /// auto-detected strategy is GithubApi.
    pub octo: Option<Arc<Octocrab>>,
    /// Reqwest handle for `PollStrategy::Grokmirror`. `None` for the
    /// other strategies.
    pub reqwest: Option<Arc<ReqwestClient>>,
    /// Shared last-error map. The poll loop clears its entry on the
    /// first successful `PollOutcome::Refreshed` so a stale panic
    /// error from before respawn does not leave the flow labeled
    /// "errored" once it is healthy again.
    ///
    /// Visibility note: `pub` (matching its sibling fields) so the
    /// supervisor end-to-end test harness in tests/poll_unborn_ref.rs
    /// can pass an external `Arc<Mutex<BTreeMap<...>>>` and read it
    /// back to assert the recorded entries. The element type
    /// `super::supervisor::FlowLastError` is `pub` with private
    /// fields and `pub fn at/kind/message/retry_at` accessors —
    /// tests get read-only views without exposing the internal
    /// record shape. The supervisor is still the only production
    /// construction site.
    pub last_errors: Arc<Mutex<BTreeMap<String, super::supervisor::FlowLastError>>>,
}

/// Drive the per-flow poll loop until `cancel` fires.
///
/// Pipeline:
/// 1. Detect the strategy from the URL.
/// 2. Loop:
///    a. Sleep the jittered interval (cancellation-aware).
///    b. Acquire the rate bucket (if any).
///    c. Issue the strategy's one-shot poll.
///    d. On a `Refreshed` outcome with a SHA diff, send a
///    `TriggerSignal` to the dispatcher FIRST.
///    e. THEN send the `PollObservation` to the state writer and
///    advance the in-memory `last_sha`.
///
/// `last_sha` is owned by the loop (loaded once at startup from
/// `state::FlowState.last_sha`). The state writer is the durable
/// store; this in-memory copy avoids a round-trip on every cycle.
/// `trigger_tx` send is awaited so a backed-up dispatcher
/// backpressures rather than dropping signals; cancellation races
/// the send so a wedged dispatcher does not block shutdown.
///
/// Trigger-before-observation order: the `Refreshed` arm calls
/// `send_trigger_then_observation` (helper below carries the full
/// cancellation-race rationale). The observation only persists
/// once the trigger has been accepted by the dispatcher's mpsc, so
/// a cancel between the two leaves state.json with the OLD SHA and
/// the next-gen poll re-detects.
pub async fn run(
    params: PollParams,
    initial_last_sha: Option<gix_hash::ObjectId>,
    state_tx: Sender<StateUpdate>,
    trigger_tx: Sender<TriggerSignal>,
    cancel: CancellationToken,
) {
    let executor = RealPollExecutor::for_url(&params.url);
    run_with_executor(
        params,
        executor,
        initial_last_sha,
        state_tx,
        trigger_tx,
        cancel,
    )
    .await;
}

/// Generic poll-loop driver — production callers go through `run`,
/// which constructs a `RealPollExecutor`; the supervisor end-to-end
/// test harness in tests/poll_unborn_ref.rs wires in a
/// `ScriptedPollExecutor` to assert the loop's last_error / trigger /
/// PollTimestamp wiring around scripted PollOutcome sequences without
/// standing up real network transports.
/// `pub` so the supervisor end-to-end test harness in
/// tests/poll_unborn_ref.rs can drive the loop directly with its
/// `ScriptedPollExecutor`. Production callers go through the
/// thin-wrapper `run`.
pub async fn run_with_executor<E>(
    params: PollParams,
    executor: E,
    initial_last_sha: Option<gix_hash::ObjectId>,
    state_tx: Sender<StateUpdate>,
    trigger_tx: Sender<TriggerSignal>,
    cancel: CancellationToken,
) where
    E: PollExecutor,
{
    info!(
        target: "gcit::flow::poll",
        flow = %params.flow_name,
        url = %params.url,
        ref_name = %params.ref_name,
        strategy = executor.strategy_label(),
        interval_secs = params.effective_poll.source_interval.as_secs(),
        "poll loop starting",
    );
    let mut last_sha = initial_last_sha;
    // Per-loop RNG: keyed off the flow name so two flows with the
    // same source URL still phase-shift relative to each other.
    let mut rng = fastrand::Rng::with_seed(seed_from_name(&params.flow_name));
    // Track whether the post-respawn last_error has been cleared. The
    // first `PollOutcome::Refreshed` after spawn proves the flow is
    // healthy, so we drop a stale panic/credential/notifier_setup
    // error left over from before respawn (sticky-error fix). Done
    // once per spawn — subsequent successful polls do not lock the
    // mutex unnecessarily.
    let mut last_error_cleared = false;

    loop {
        // Compute the next sleep deadline (jittered).
        let sleep_for = strategy::apply_jitter(
            params.effective_poll.source_interval,
            params.effective_poll.jitter,
            &mut rng,
        );
        tokio::select! {
            _ = cancel.cancelled() => {
                info!(target: "gcit::flow::poll", flow = %params.flow_name, "poll loop cancelled");
                return;
            }
            _ = tokio::time::sleep(sleep_for) => {}
        }

        // Pacing gate per credential.
        if let Some(b) = &params.rate_bucket {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = b.acquire() => {}
            }
        }

        // Issue the strategy's one-shot poll via the pluggable
        // executor. Production: RealPollExecutor → poll_one →
        // auto_detect-driven dispatch. Tests: ScriptedPollExecutor.
        let outcome = executor.poll_cycle(&params, &cancel).await;
        let outcome = match outcome {
            Ok(o) => o,
            Err(PollCycleError::Cancelled) => {
                // Supervisor revoked the cancel token (SIGHUP
                // cancel-and-respawn or daemon shutdown). Exit the
                // loop without recording a FlowLastError — surfacing
                // "git_poll_failed: cancelled" in `gcit status` would
                // be a transient lie.
                info!(
                    target: "gcit::flow::poll",
                    flow = %params.flow_name,
                    "poll loop cancelled mid-cycle",
                );
                return;
            }
            Err(PollCycleError::Failed(message)) => {
                warn!(
                    target: "gcit::flow::poll",
                    flow = %params.flow_name,
                    error = %message,
                    "poll cycle failed; continuing on cadence",
                );
                // Surface the failure in `gcit status` via the
                // FlowLastError map under kind="git_poll_failed".
                // Without this, operators only see poll failures in
                // journald — the status surface stays clean even
                // when every poll cycle errors out (e.g. against a
                // misconfigured ls-remote URL or a 5xx upstream).
                record_last_error(
                    &params.last_errors,
                    &params.flow_name,
                    "git_poll_failed",
                    &message,
                    None,
                )
                .await;
                // Re-arm the once-per-recovery clear so the next
                // successful poll drops the entry. The
                // `last_error_cleared` flag exists to skip the mutex
                // lock on steady-state success polls; a freshly
                // recorded error means there IS something to clear,
                // and the next success should pay the lock cost.
                last_error_cleared = false;
                continue;
            }
        };

        // Persist + diff.
        match outcome {
            PollOutcome::Refreshed { sha } => {
                let now = Utc::now();
                let diff = compare_sha(last_sha, sha);
                let observation = StateUpdate::PollObservation {
                    flow: params.flow_name.clone(),
                    last_sha: sha,
                    last_poll_at: now,
                };
                let signal = diff.trigger.then_some(TriggerSignal {
                    observed_sha: sha,
                    observed_at: now,
                });
                match send_trigger_then_observation(
                    &params.flow_name,
                    signal,
                    observation,
                    &state_tx,
                    &trigger_tx,
                    &cancel,
                )
                .await
                {
                    SendOutcome::Sent => {}
                    SendOutcome::Cancelled
                    | SendOutcome::TriggerChannelClosed
                    | SendOutcome::StateChannelClosed => return,
                }
                if !last_error_cleared {
                    // First successful poll observation since spawn —
                    // the flow is healthy, so any stale last_error
                    // (e.g. a panic recorded before respawn) is no
                    // longer relevant. Clear once.
                    params.last_errors.lock().await.remove(&params.flow_name);
                    last_error_cleared = true;
                }
                last_sha = Some(diff.observed);
            }
            PollOutcome::Unchanged => {
                // Grokmirror short-circuit: the manifest fingerprint
                // matched the previous observation, so we have proof
                // of liveness without a fresh ObjectId. Refresh
                // last_poll_at via PollTimestamp so `gcit status`
                // reflects that the flow is still polling on cadence
                // even when the upstream has not changed.
                let now = Utc::now();
                if state_tx
                    .send(StateUpdate::PollTimestamp {
                        flow: params.flow_name.clone(),
                        last_poll_at: now,
                    })
                    .await
                    .is_err()
                {
                    debug!(target: "gcit::flow::poll", flow = %params.flow_name, "state writer dropped; exiting");
                    return;
                }
                if !last_error_cleared {
                    // A successful "unchanged" poll is also evidence
                    // the flow is healthy — clear stale post-respawn
                    // last_error.
                    params.last_errors.lock().await.remove(&params.flow_name);
                    last_error_cleared = true;
                }
            }
            PollOutcome::UnbornRef => {
                // The configured ref doesn't exist upstream. This is a
                // CONFIGURATION condition (operator typo, branch
                // deleted, branch not yet pushed) — not a crash and
                // not a transient network failure. Surface it under
                // `git_poll_failed` so `gcit status` shows which flow
                // is stuck; the WARN below is the journald companion
                // for the same event. The next successful poll
                // (`Refreshed` or `Unchanged`) will drop the entry via
                // the existing `last_error_cleared` latch — re-arm it
                // here so a freshly recorded UnbornRef gets cleared
                // when the ref appears.
                //
                // Transition handling: a flow that was failing with
                // `git_poll_failed` from a 5xx burst and then
                // recovers with a clean "ref doesn't exist" answer
                // gets the same kind ("git_poll_failed") with a new
                // message body. `record_last_error` insert overwrites
                // the FlowLastError under the flow key — the message
                // text differentiates UnbornRef from a generic poll
                // failure for operators reading `gcit status`.
                //
                // Liveness: emit a `PollTimestamp` so `last_poll_at`
                // advances even though the cycle produced no fresh
                // ObjectId. Without this, an unborn-ref flow's
                // `last_poll_at` would freeze at the spawn time and
                // `gcit status` would show the flow as stale even
                // while it polls on cadence — the same liveness gap
                // the `Unchanged` arm already closes for grokmirror's
                // fingerprint short-circuit.
                let now = Utc::now();
                if state_tx
                    .send(StateUpdate::PollTimestamp {
                        flow: params.flow_name.clone(),
                        last_poll_at: now,
                    })
                    .await
                    .is_err()
                {
                    debug!(target: "gcit::flow::poll", flow = %params.flow_name, "state writer dropped; exiting");
                    return;
                }
                let message = unborn_ref_message(&params.flow_name, &params.url, &params.ref_name);
                warn!(
                    target: "gcit::flow::poll",
                    flow = %params.flow_name,
                    url = %params.url,
                    ref_name = %params.ref_name,
                    "configured ref does not exist on remote; continuing on cadence",
                );
                record_last_error(
                    &params.last_errors,
                    &params.flow_name,
                    "git_poll_failed",
                    &message,
                    None,
                )
                .await;
                last_error_cleared = false;
            }
        }
    }
}

/// Outcome of `send_trigger_then_observation`. The poll loop matches
/// on this to decide whether to keep iterating (`Sent`) or exit
/// (every other variant). Distinct variants — rather than a single
/// "Stop" — let the caller (and the unit tests) tell which side of
/// the trigger-then-observation sequence terminated the loop.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SendOutcome {
    /// Both the trigger (when present) and the observation were
    /// accepted by the dispatcher's and writer's mpsc channels
    /// respectively. The loop continues.
    Sent,
    /// `cancel.cancelled()` won the trigger-arm select. The trigger
    /// is dropped (per tokio mpsc cancel-safety: when another branch
    /// completes first, the message is guaranteed not sent), the
    /// observation is also skipped, and state.json keeps the OLD SHA
    /// so the next-gen poll re-detects the diff. At-most-once
    /// dispatch invariant holds.
    Cancelled,
    /// `trigger_tx.send(...)` returned `Err(SendError(_))` — the
    /// dispatcher's recv-end was dropped (dispatcher task exited).
    /// The poll task exits without persisting the observation.
    TriggerChannelClosed,
    /// `state_tx.send(...)` returned `Err(SendError(_))` — the state
    /// writer's recv-end was dropped (writer thread exited / shut
    /// down). The poll task exits.
    StateChannelClosed,
}

/// Send the trigger (if any) and observation in the order required
/// by the at-most-once dispatch invariant: trigger FIRST, observation
/// SECOND.
///
/// If we sent the observation first and cancellation fired between
/// observation and trigger (SIGHUP, panic-respawn, or shutdown),
/// state.json would persist the new SHA while the trigger never
/// reached the dispatcher. The next-generation poll task spawned
/// after respawn would read the persisted SHA, observe it matches
/// upstream, see no diff, and silently lose the trigger forever.
///
/// Reordered: trigger goes onto the dispatcher's mpsc first. If
/// cancellation races between the trigger and observation sends,
/// the dispatcher may complete its in-flight HTTP POST (the POST
/// itself is uncancellable mid-flight — see dispatcher.rs::dispatch
/// which uses `tokio::time::timeout`, not cancel races, on the
/// octocrab call). The observation send runs to completion as long
/// as the writer is alive — so state.json advances to the new SHA,
/// and the next-gen poll sees no diff. If cancel wins the trigger
/// select!, the trigger is dropped and the observation is also
/// skipped, so state.json keeps the OLD SHA and the next-gen poll
/// re-detects the diff.
///
/// Backpressure on the dispatcher's trigger channel: `send().await`
/// blocks until the dispatcher recv-end accepts the message. Race
/// against `cancel` so a wedged dispatcher does not block the poll
/// task from observing shutdown.
///
/// The observation send is NOT cancel-raced — once the trigger has
/// been accepted we want the observation to flush even if cancel
/// fires immediately after (the state writer's mpsc has a 256-slot
/// buffer per state::STATE_QUEUE and a non-closed sender will always
/// accept). If `state_tx` is closed, `StateChannelClosed` is
/// returned and the caller exits.
///
/// Residual window: there is a microsecond gap between trigger_tx.send
/// completing and state_tx.send starting. If the state writer dies
/// (panic / thread kill) in that gap, the trigger is delivered to the
/// dispatcher but state.json does not advance. Recovery path:
///   - The poll task observes the next state_tx.send returning Err
///     and exits with `StateChannelClosed`.
///   - The state writer's death is itself a daemon-level fault: there
///     is no respawn for the writer; the supervisor's shutdown
///     sequence (after `select!` exits) drops state_tx and joins the
///     writer thread. In practice this means the residual window
///     fires only when the daemon is going down, NOT during normal
///     operation.
///   - On daemon restart, the next poll cycle reads the persisted
///     last_sha from state.json. Because the trigger-loss window only
///     fires when the writer dies, the persisted SHA reflects the
///     state BEFORE the lost trigger. The next poll detects the
///     upstream diff again and fires a fresh dispatch (with a fresh
///     `gcit_run_id` per dispatcher.rs `Uuid::new_v4()`).
///   - GitHub does NOT deduplicate workflow_dispatch by gcit_run_id
///     (the UUID is gcit's own correlation token, not a GitHub
///     idempotency key — every `Uuid::new_v4()` produces a new value
///     and GitHub sees a fresh dispatch). The "duplicate dispatch"
///     scenario after restart is therefore a single re-fire at most:
///     the workflow that was already dispatched into the dead-writer
///     window may still complete on GitHub's side (gcit lost
///     visibility into it), and the post-restart fire creates a
///     SECOND run for the same SHA.
///
/// At-most-once semantics is therefore relaxed to at-most-once-during-
/// normal-operation; an at-most-twice fire is possible across a writer
/// failure plus daemon restart. The trade is intentional: the
/// alternative (sending observation before trigger) loses the trigger
/// outright on the same race, with no recovery.
///
/// The window is bounded by a single straight `.await` (no select!
/// race) and closes as soon as the writer's mpsc accepts the
/// observation.
///
/// `signal == None` indicates "no SHA diff this cycle" (`compare_sha`
/// returned `trigger: false`). The trigger send is skipped entirely
/// and only the observation is persisted; this is the baseline-poll
/// and unchanged-poll path.
pub(crate) async fn send_trigger_then_observation(
    flow_name: &str,
    signal: Option<TriggerSignal>,
    observation: StateUpdate,
    state_tx: &Sender<StateUpdate>,
    trigger_tx: &Sender<TriggerSignal>,
    cancel: &CancellationToken,
) -> SendOutcome {
    if let Some(s) = signal {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!(
                    target: "gcit::flow::poll",
                    flow = %flow_name,
                    "poll loop cancelled while waiting on dispatcher trigger queue",
                );
                return SendOutcome::Cancelled;
            }
            r = trigger_tx.send(s) => {
                if let Err(e) = r {
                    debug!(
                        target: "gcit::flow::poll",
                        flow = %flow_name,
                        error = %e,
                        "dispatcher trigger channel closed; poll task exiting",
                    );
                    return SendOutcome::TriggerChannelClosed;
                }
            }
        }
    }
    if state_tx.send(observation).await.is_err() {
        debug!(
            target: "gcit::flow::poll",
            flow = %flow_name,
            "state writer dropped; exiting",
        );
        return SendOutcome::StateChannelClosed;
    }
    SendOutcome::Sent
}

/// Result of a single poll cycle.
///
/// Distinguishes cancellation (the supervisor revoked our token, e.g.
/// SIGHUP cancel-and-respawn or daemon shutdown) from a real failure
/// (network error, parse error, upstream 5xx). The loop must NOT
/// record a `FlowLastError` for `Cancelled` — cancellation is
/// supervisor-driven control flow, not an operator-visible failure
/// of the flow. Recording it would briefly surface a spurious
/// `git_poll_failed: cancelled` entry in `gcit status` during the
/// respawn window until the new-gen poll's first success clears it.
///
/// Visibility note: `pub` so the supervisor end-to-end test harness
/// in tests/poll_unborn_ref.rs can construct scripted error variants
/// for its `ScriptedPollExecutor` without reaching into a
/// `pub(crate)` boundary. The variants carry no daemon-private
/// state — `Failed(String)` wraps a free-form rendered message,
/// `Cancelled` is unit. There is nothing internal to leak.
#[derive(Debug)]
pub enum PollCycleError {
    /// `cancel.cancelled()` won a select! arm. The caller must exit
    /// the poll loop without recording any last_error.
    Cancelled,
    /// A real poll-cycle failure to surface to the operator. The
    /// string is the rendered error text; the loop records it under
    /// kind="git_poll_failed" and continues on cadence.
    Failed(String),
}

impl std::fmt::Display for PollCycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled => f.write_str("cancelled"),
            Self::Failed(s) => f.write_str(s),
        }
    }
}

/// Pluggable strategy executor — the seam the supervisor end-to-end
/// test harness slots into in place of the production
/// `auto_detect → github_api/grokmirror/ls_remote` dispatch chain.
/// Production callers use the default `run` entry point which wires
/// in `RealPollExecutor`; the harness in tests/poll_unborn_ref.rs
/// supplies a `ScriptedPollExecutor` that yields a pre-baked Vec of
/// outcomes so the loop's wiring (last_error recording, trigger
/// suppression on UnbornRef, PollTimestamp emission, etc.) can be
/// asserted without standing up real network transports.
///
/// Uses native async-fn-in-trait (stable since Rust 1.75). The loop
/// consumes `E: PollExecutor` as a generic type parameter, so no
/// dyn-trait erasure is needed and we avoid the `async_trait` macro
/// dependency.
///
/// Visibility note: `pub` so the supervisor end-to-end test harness
/// in tests/poll_unborn_ref.rs can implement the trait for its
/// `ScriptedPollExecutor`. There is no daemon-private state in the
/// trait or its method signature.
pub trait PollExecutor: Send + Sync {
    /// Strategy label for the startup info! log. Production:
    /// `strategy::kind_str(self.strategy)` ("github_api" |
    /// "grokmirror" | "ls_remote"). Tests: a stable opaque label
    /// like "scripted" so log readers can tell scripted-harness
    /// runs apart from production runs.
    fn strategy_label(&self) -> &'static str;

    /// Issue one poll cycle and return its outcome. The executor
    /// owns the strategy choice (production = `auto_detect`,
    /// tests = pre-baked) AND any per-cycle state that crosses
    /// cycles (e.g. grokmirror's prior-fingerprint cache). The
    /// loop body is strategy-agnostic.
    fn poll_cycle(
        &self,
        params: &PollParams,
        cancel: &CancellationToken,
    ) -> impl std::future::Future<Output = Result<PollOutcome, PollCycleError>> + Send;
}

/// Production executor: detects the strategy from the URL once at
/// construction time and dispatches each cycle through the existing
/// `poll_one` arms. Owns the grokmirror fingerprint cache (the only
/// per-cycle state that crosses cycles in production). Used by
/// `run`'s default path.
pub struct RealPollExecutor {
    strategy: PollStrategy,
    /// Grokmirror strategy carries the previous manifest fingerprint
    /// across cycles to detect "no change" via fingerprint compare
    /// (the manifest hash short-circuit). `Mutex` because the trait
    /// method takes `&self`; the loop only ever calls `poll_cycle`
    /// serially, so contention is impossible.
    grokmirror_fingerprint: tokio::sync::Mutex<Option<String>>,
}

impl RealPollExecutor {
    pub fn for_url(url: &str) -> Self {
        Self {
            strategy: auto_detect(url),
            grokmirror_fingerprint: tokio::sync::Mutex::new(None),
        }
    }
}

impl PollExecutor for RealPollExecutor {
    fn strategy_label(&self) -> &'static str {
        strategy::kind_str(self.strategy)
    }

    async fn poll_cycle(
        &self,
        params: &PollParams,
        cancel: &CancellationToken,
    ) -> Result<PollOutcome, PollCycleError> {
        let mut fingerprint = self.grokmirror_fingerprint.lock().await;
        poll_one(self.strategy, params, &mut fingerprint, cancel).await
    }
}

/// Issue one strategy-specific poll. Errors are typed as
/// `PollCycleError` so the caller can distinguish cancellation
/// (supervisor revoked the token) from a real failure that should
/// surface in `gcit status`. The loop continues on cadence
/// regardless of (Permanent | Transient) classification — gcit does
/// not back off on poll failures because the per-strategy retry
/// semantics are already captured in the `git::*::poll`
/// implementations (e.g., 404 -> UnbornRef rather than Err for
/// github_api).
async fn poll_one(
    strategy: PollStrategy,
    params: &PollParams,
    grokmirror_fingerprint: &mut Option<String>,
    cancel: &CancellationToken,
) -> Result<PollOutcome, PollCycleError> {
    match strategy {
        PollStrategy::GithubApi => {
            let octo = params.octo.as_ref().ok_or_else(|| {
                PollCycleError::Failed("github_api strategy requires Octocrab handle".to_string())
            })?;
            let (owner, repo) = split_github_repo(&params.url)
                .map_err(|e| PollCycleError::Failed(format!("github URL parse: {e}")))?;
            tokio::select! {
                _ = cancel.cancelled() => Err(PollCycleError::Cancelled),
                r = github_api::poll(octo, &owner, &repo, &params.ref_name) => {
                    r.map_err(|e| PollCycleError::Failed(format!("{e}")))
                }
            }
        }
        PollStrategy::Grokmirror => {
            let client = params.reqwest.as_ref().ok_or_else(|| {
                PollCycleError::Failed("grokmirror strategy requires reqwest handle".to_string())
            })?;
            // Manifest URL: derive from the source URL's host
            // (e.g. `https://git.kernel.org`).
            let base = base_url(&params.url).ok_or_else(|| {
                PollCycleError::Failed("grokmirror URL must carry a host".to_string())
            })?;
            let repo_path = grokmirror::extract_repo_path_from_url(&params.url)
                .map_err(|e| PollCycleError::Failed(format!("grokmirror repo path: {e}")))?;
            let manifest = tokio::select! {
                _ = cancel.cancelled() => return Err(PollCycleError::Cancelled),
                r = grokmirror::fetch_manifest(client, &base) => {
                    r.map_err(|e| PollCycleError::Failed(format!("{e}")))?
                }
            };
            match grokmirror::lookup_fingerprint(&manifest, &repo_path) {
                Ok(fp) => {
                    let new_fp = fp.to_string();
                    let unchanged = grokmirror_fingerprint
                        .as_deref()
                        .map(|prev| prev == new_fp.as_str())
                        .unwrap_or(false);
                    *grokmirror_fingerprint = Some(new_fp);
                    if unchanged {
                        Ok(PollOutcome::Unchanged)
                    } else {
                        // grokmirror only proves "fingerprint changed";
                        // resolving fingerprint -> per-ref SHA needs
                        // an additional round-trip. Fall back to
                        // ls-remote for the SHA on a fingerprint diff.
                        ls_remote_inline(&params.url, &params.ref_name, cancel).await
                    }
                }
                Err(grokmirror::GrokmirrorError::RepoNotInManifest { .. }) => {
                    Ok(PollOutcome::UnbornRef)
                }
                Err(e) => Err(PollCycleError::Failed(format!("grokmirror lookup: {e}"))),
            }
        }
        PollStrategy::LsRemote => ls_remote_inline(&params.url, &params.ref_name, cancel).await,
    }
}

/// Wrap `ls_remote::poll` in cancellation. `ls_remote::poll` itself
/// owns its `tokio::time::timeout`; this wrapper just races against
/// `cancel.cancelled()`.
async fn ls_remote_inline(
    url: &str,
    ref_name: &str,
    cancel: &CancellationToken,
) -> Result<PollOutcome, PollCycleError> {
    tokio::select! {
        _ = cancel.cancelled() => Err(PollCycleError::Cancelled),
        r = ls_remote::poll(url.to_string(), ref_name.to_string()) => {
            r.map_err(|e| PollCycleError::Failed(format!("{e}")))
        }
    }
}

/// Strip the leading scheme + host from a URL so a `urlencoding`
/// callee gets a path-only string. Returns the host-with-scheme as a
/// `https://host` form, suitable for `grokmirror::fetch_manifest`'s
/// `base_url` parameter.
fn base_url(url: &str) -> Option<String> {
    // gix_url::parse accepts file://, ssh, scp-style, but for
    // grokmirror we know the URL is https://git.kernel.org/...
    // url::Url::parse handles that and surfaces host + scheme cleanly.
    let parsed = url::Url::parse(url).ok()?;
    let host = parsed.host_str()?;
    let scheme = parsed.scheme();
    Some(format!("{scheme}://{host}"))
}

/// Parse `https://github.com/owner/repo[.git]` into `(owner, repo)`.
/// Drops the trailing `.git` if present. Used by the github_api
/// strategy to feed octocrab's `repos(owner, repo)`.
fn split_github_repo(url: &str) -> Result<(String, String), String> {
    let parsed = url::Url::parse(url).map_err(|e| format!("{e}"))?;
    let mut segments = parsed
        .path_segments()
        .ok_or_else(|| "github URL has no path".to_string())?
        .filter(|s| !s.is_empty());
    let owner = segments.next().ok_or_else(|| "missing owner".to_string())?;
    let repo = segments.next().ok_or_else(|| "missing repo".to_string())?;
    let repo = repo.strip_suffix(".git").unwrap_or(repo);
    Ok((owner.to_string(), repo.to_string()))
}

/// Render the operator-facing message for a `PollOutcome::UnbornRef`
/// observation. Names the flow, the configured ref, and the source
/// URL so an operator reading `gcit status` can route the fix
/// (typo'd ref name, branch not yet pushed, upstream renamed
/// default branch). Extracted for direct unit-test coverage; the
/// supervisor-level integration tests in tests/poll_unborn_ref.rs
/// gate the end-to-end propagation but are blocked behind the
/// supervisor end-to-end test harness.
fn unborn_ref_message(flow: &str, url: &str, ref_name: &str) -> String {
    format!(
        "ref {ref_name} not found at {url} — verify the ref exists upstream, wait if it's still being pushed, or update flow.{flow}.source.ref",
    )
}

/// Deterministic seed for the per-flow jitter RNG. Keying off the
/// flow name (rather than a process-global RNG) lets two flows
/// polling the same upstream stay phase-shifted relative to each
/// other across daemon restarts.
fn seed_from_name(name: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut h);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn effective_poll_uses_strategy_default_when_neither_layer_set() {
        let defaults = crate::config::PollDefaults::default();
        let override_ = crate::config::PollOverride::default();
        let p = EffectivePoll::compute(&defaults, &override_, PollStrategy::GithubApi);
        assert_eq!(p.source_interval, default_interval(PollStrategy::GithubApi));
    }

    #[test]
    fn effective_poll_override_wins_over_defaults() {
        let defaults = crate::config::PollDefaults {
            source_interval: Some(Duration::from_secs(120)),
            ..Default::default()
        };
        let override_ = crate::config::PollOverride {
            source_interval: Some(Duration::from_secs(45)),
            ..Default::default()
        };
        let p = EffectivePoll::compute(&defaults, &override_, PollStrategy::LsRemote);
        assert_eq!(p.source_interval, Duration::from_secs(45));
    }

    #[test]
    fn effective_poll_job_interval_falls_back_to_defaults() {
        let defaults = crate::config::PollDefaults::default();
        let override_ = crate::config::PollOverride::default();
        let p = EffectivePoll::compute(&defaults, &override_, PollStrategy::GithubApi);
        // PollDefaults::default() pins job_interval to 30s.
        assert_eq!(p.job_interval, defaults.job_interval);
    }

    #[test]
    fn effective_poll_job_interval_override_wins() {
        let defaults = crate::config::PollDefaults::default();
        let override_ = crate::config::PollOverride {
            job_interval: Some(Duration::from_secs(15)),
            ..Default::default()
        };
        let p = EffectivePoll::compute(&defaults, &override_, PollStrategy::GithubApi);
        assert_eq!(p.job_interval, Duration::from_secs(15));
    }

    #[test]
    fn split_github_repo_strips_dot_git() {
        let (o, r) = split_github_repo("https://github.com/owner/repo.git").unwrap();
        assert_eq!(o, "owner");
        assert_eq!(r, "repo");
    }

    #[test]
    fn split_github_repo_no_dot_git() {
        let (o, r) = split_github_repo("https://github.com/owner/repo").unwrap();
        assert_eq!(o, "owner");
        assert_eq!(r, "repo");
    }

    #[test]
    fn base_url_strips_path() {
        assert_eq!(
            base_url("https://git.kernel.org/pub/scm/foo.git"),
            Some("https://git.kernel.org".to_string()),
        );
    }

    #[test]
    fn seed_from_name_is_deterministic() {
        assert_eq!(seed_from_name("flow-a"), seed_from_name("flow-a"));
        assert_ne!(seed_from_name("flow-a"), seed_from_name("flow-b"));
    }

    #[test]
    fn split_github_repo_rejects_url_without_owner_and_repo() {
        // A URL with no path segments after the host (e.g. just
        // `https://github.com/`) must surface a clear error rather
        // than panicking on the segment iterator.
        let err = split_github_repo("https://github.com/").expect_err("must error");
        // `split_github_repo` reads owner BEFORE repo, so a path with
        // no segments fails on the owner read first — "missing owner"
        // is the deterministic surface for this input.
        assert!(
            err.contains("missing owner"),
            "error must name the missing owner; got: {err}",
        );
    }

    #[test]
    fn split_github_repo_rejects_url_with_only_owner() {
        // Owner present but no repo — distinct from the no-owner
        // path. The current implementation surfaces "missing repo".
        let err = split_github_repo("https://github.com/owner").expect_err("must error");
        assert!(
            err.contains("missing repo"),
            "error must name the missing repo; got: {err}",
        );
    }

    #[test]
    fn split_github_repo_rejects_unparseable_url() {
        // url::Url::parse rejects this — the error path wraps the
        // parser's Display; we just need to confirm the function
        // surfaces an error rather than panicking.
        let err = split_github_repo("not a url at all").expect_err("must error");
        assert!(
            !err.is_empty(),
            "unparseable URL must surface a non-empty error",
        );
    }

    #[test]
    fn base_url_returns_none_for_unparseable_input() {
        assert!(base_url("not a url").is_none());
    }

    fn sha(byte: u8) -> gix_hash::ObjectId {
        let hex = format!("{byte:02x}").repeat(20);
        gix_hash::ObjectId::from_hex(hex.as_bytes()).unwrap()
    }

    fn ts(secs: i64) -> chrono::DateTime<Utc> {
        chrono::DateTime::from_timestamp(secs, 0).unwrap()
    }

    /// Trigger-before-observation ordering invariant. Block the state
    /// writer's mpsc by pre-filling its single capacity slot with a
    /// sentinel; spawn the helper as a task; await the trigger arm
    /// first; then drain the sentinel so the helper's observation
    /// send unblocks and arrives second. The first state recv proves
    /// the sentinel is still in the channel (i.e. the helper has not
    /// yet sent its observation) at the moment the trigger has been
    /// received — establishing the trigger-then-observation order.
    /// A reordered helper (observation-first) would fill the slot
    /// with its own observation before the trigger ever arrived; the
    /// first-recv assertion below would then fail because we'd see
    /// the observation flow name instead of the sentinel "(prefilled)".
    #[tokio::test]
    async fn send_trigger_then_observation_sends_trigger_before_observation() {
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(1);
        let (trigger_tx, mut trigger_rx) = tokio::sync::mpsc::channel::<TriggerSignal>(1);
        let cancel = CancellationToken::new();

        // Pre-fill the state channel so the helper's observation send
        // backpressures until we drain it.
        state_tx
            .send(StateUpdate::PollObservation {
                flow: "(prefilled)".to_string(),
                last_sha: sha(0xff),
                last_poll_at: ts(0),
            })
            .await
            .unwrap();

        let observed_sha = sha(0xab);
        let signal = Some(TriggerSignal {
            observed_sha,
            observed_at: ts(100),
        });
        let observation = StateUpdate::PollObservation {
            flow: "ordering-flow".to_string(),
            last_sha: observed_sha,
            last_poll_at: ts(100),
        };

        let helper = tokio::spawn({
            let state_tx = state_tx.clone();
            let trigger_tx = trigger_tx.clone();
            let cancel = cancel.clone();
            async move {
                send_trigger_then_observation(
                    "ordering-flow",
                    signal,
                    observation,
                    &state_tx,
                    &trigger_tx,
                    &cancel,
                )
                .await
            }
        });

        // Trigger must arrive even though the state channel is full —
        // proves the helper attempted the trigger send before the
        // (blocked) observation send.
        let received_trigger = tokio::time::timeout(Duration::from_secs(5), trigger_rx.recv())
            .await
            .expect("trigger must arrive within 5s")
            .expect("trigger channel must yield Some");
        assert_eq!(received_trigger.observed_sha, observed_sha);
        assert_eq!(received_trigger.observed_at, ts(100));

        // The first state recv MUST be the sentinel — if the helper
        // had reversed the order it would have raced for the slot
        // before the trigger send completed, and we'd see the
        // ordering-flow observation here instead.
        let first_state = tokio::time::timeout(Duration::from_secs(5), state_rx.recv())
            .await
            .expect("state recv 1 must complete within 5s")
            .expect("state channel must yield Some");
        match first_state {
            StateUpdate::PollObservation { flow, .. } => assert_eq!(flow, "(prefilled)"),
            other => panic!("expected sentinel PollObservation, got {other:?}"),
        }

        // After draining the sentinel, the helper's observation send
        // unblocks. Recv the helper's observation — the second message
        // on the state channel — and confirm the helper returned Sent.
        let second_state = tokio::time::timeout(Duration::from_secs(5), state_rx.recv())
            .await
            .expect("state recv 2 must complete within 5s")
            .expect("state channel must yield Some");
        match second_state {
            StateUpdate::PollObservation { flow, last_sha, .. } => {
                assert_eq!(flow, "ordering-flow");
                assert_eq!(last_sha, observed_sha);
            }
            other => panic!("expected ordering-flow PollObservation, got {other:?}"),
        }
        let outcome = tokio::time::timeout(Duration::from_secs(5), helper)
            .await
            .expect("helper must complete within 5s")
            .expect("helper task panicked");
        assert_eq!(outcome, SendOutcome::Sent);
    }

    /// `signal == None` (baseline / unchanged poll path) must skip
    /// the trigger send entirely and only flush the observation. The
    /// trigger channel stays empty even after recv'ing the observation.
    #[tokio::test]
    async fn send_trigger_then_observation_skips_trigger_when_signal_none() {
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(1);
        let (trigger_tx, mut trigger_rx) = tokio::sync::mpsc::channel::<TriggerSignal>(1);
        let cancel = CancellationToken::new();

        let observation = StateUpdate::PollObservation {
            flow: "no-diff-flow".to_string(),
            last_sha: sha(0x10),
            last_poll_at: ts(50),
        };
        let outcome = send_trigger_then_observation(
            "no-diff-flow",
            None,
            observation,
            &state_tx,
            &trigger_tx,
            &cancel,
        )
        .await;
        assert_eq!(outcome, SendOutcome::Sent);

        let recv_state = state_rx.recv().await.expect("observation must arrive");
        match recv_state {
            StateUpdate::PollObservation { flow, .. } => assert_eq!(flow, "no-diff-flow"),
            other => panic!("expected PollObservation, got {other:?}"),
        }
        // Trigger channel must still be empty: try_recv returns Empty
        // (Disconnected only if the sender was dropped, which we keep
        // alive via `trigger_tx`).
        assert!(matches!(
            trigger_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty),
        ));
    }

    /// Cancellation breaks the helper out of a trigger send that
    /// would otherwise block on a wedged dispatcher. Setup: fill the
    /// trigger mpsc to capacity so the helper's `trigger_tx.send`
    /// suspends; spawn the helper; then cancel the token. The cancel
    /// branch of the inner select! becomes the only ready branch and
    /// wins, helper returns `Cancelled`. Neither the helper's trigger
    /// nor its observation reach the receivers — state.json keeps
    /// the OLD SHA and the next-gen poll re-detects the diff
    /// (at-most-once invariant).
    ///
    /// This test deliberately does NOT pre-cancel before calling the
    /// helper: when both `cancel.cancelled()` and `trigger_tx.send`
    /// are immediately ready (token already cancelled AND channel
    /// has free capacity), tokio's select! picks pseudo-randomly. The
    /// production guarantee is "cancellation can preempt a BLOCKED
    /// trigger send", which is what this scenario pins.
    #[tokio::test]
    async fn send_trigger_then_observation_returns_cancelled_when_trigger_send_blocks() {
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(1);
        let (trigger_tx, mut trigger_rx) = tokio::sync::mpsc::channel::<TriggerSignal>(1);
        let cancel = CancellationToken::new();

        // Pre-fill the trigger channel so the helper's send blocks
        // until either capacity opens or cancel fires.
        trigger_tx
            .send(TriggerSignal {
                observed_sha: sha(0x00),
                observed_at: ts(0),
            })
            .await
            .unwrap();

        let observation = StateUpdate::PollObservation {
            flow: "cancelled-flow".to_string(),
            last_sha: sha(0x42),
            last_poll_at: ts(200),
        };
        let helper = tokio::spawn({
            let state_tx = state_tx.clone();
            let trigger_tx = trigger_tx.clone();
            let cancel = cancel.clone();
            async move {
                send_trigger_then_observation(
                    "cancelled-flow",
                    Some(TriggerSignal {
                        observed_sha: sha(0x42),
                        observed_at: ts(200),
                    }),
                    observation,
                    &state_tx,
                    &trigger_tx,
                    &cancel,
                )
                .await
            }
        });

        // Yield so the helper enters the select! and parks on
        // trigger_tx.send (the channel is full). Then cancel — the
        // cancel arm becomes the only ready branch and the helper
        // exits with Cancelled.
        tokio::task::yield_now().await;
        cancel.cancel();

        let outcome = tokio::time::timeout(Duration::from_secs(5), helper)
            .await
            .expect("helper must complete within 5s")
            .expect("helper task panicked");
        assert_eq!(outcome, SendOutcome::Cancelled);

        // The pre-fill TriggerSignal is still in the channel; the
        // helper's TriggerSignal was NOT sent (per tokio mpsc
        // cancel-safety: when another select! branch completes first,
        // the message is guaranteed not enqueued).
        let pre_filled = trigger_rx.try_recv().expect("pre-fill trigger present");
        assert_eq!(pre_filled.observed_sha, sha(0x00));
        assert!(matches!(
            trigger_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty),
        ));
        // Observation was never attempted because the helper exited
        // on the trigger arm.
        assert!(matches!(
            state_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty),
        ));
    }

    /// Dispatcher recv-end dropped before the helper runs: trigger
    /// send returns SendError, helper returns `TriggerChannelClosed`,
    /// observation is NOT sent (state channel stays empty).
    #[tokio::test]
    async fn send_trigger_then_observation_returns_trigger_channel_closed() {
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(1);
        let (trigger_tx, trigger_rx) = tokio::sync::mpsc::channel::<TriggerSignal>(1);
        drop(trigger_rx);
        let cancel = CancellationToken::new();

        let observation = StateUpdate::PollObservation {
            flow: "closed-trigger".to_string(),
            last_sha: sha(0x07),
            last_poll_at: ts(7),
        };
        let outcome = send_trigger_then_observation(
            "closed-trigger",
            Some(TriggerSignal {
                observed_sha: sha(0x07),
                observed_at: ts(7),
            }),
            observation,
            &state_tx,
            &trigger_tx,
            &cancel,
        )
        .await;
        assert_eq!(outcome, SendOutcome::TriggerChannelClosed);
        assert!(matches!(
            state_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty),
        ));
    }

    /// State writer recv-end dropped before the helper runs: trigger
    /// send succeeds (the dispatcher receives), then state send
    /// returns SendError. Helper returns `StateChannelClosed`. The
    /// trigger has already been delivered — at-least-once for
    /// dispatch, even when the writer dies.
    #[tokio::test]
    async fn send_trigger_then_observation_returns_state_channel_closed_after_trigger_sent() {
        let (state_tx, state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(1);
        drop(state_rx);
        let (trigger_tx, mut trigger_rx) = tokio::sync::mpsc::channel::<TriggerSignal>(1);
        let cancel = CancellationToken::new();

        let observation = StateUpdate::PollObservation {
            flow: "closed-state".to_string(),
            last_sha: sha(0x09),
            last_poll_at: ts(9),
        };
        let outcome = send_trigger_then_observation(
            "closed-state",
            Some(TriggerSignal {
                observed_sha: sha(0x09),
                observed_at: ts(9),
            }),
            observation,
            &state_tx,
            &trigger_tx,
            &cancel,
        )
        .await;
        assert_eq!(outcome, SendOutcome::StateChannelClosed);
        let recv_trigger = trigger_rx.recv().await.expect("trigger must have arrived");
        assert_eq!(recv_trigger.observed_sha, sha(0x09));
    }

    /// `ls_remote_inline` (the underlying cancellation wrapper used by
    /// the LsRemote and Grokmirror strategy arms) must surface
    /// cancellation as `PollCycleError::Cancelled`, not a `Failed`
    /// variant carrying the literal string "cancelled". This pins the
    /// typed-enum split that prevents the loop from recording
    /// `git_poll_failed: cancelled` in the FlowLastError map during a
    /// SIGHUP cancel-and-respawn or daemon shutdown.
    ///
    /// Pre-cancel the token, then call the helper: the cancelled arm
    /// is the only ready branch (the ls_remote::poll future is fresh
    /// and not yet polled, so it cannot complete first), and the
    /// helper returns Cancelled.
    #[tokio::test]
    async fn ls_remote_inline_returns_cancelled_variant_on_pre_cancelled_token() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        // Use a syntactically valid file:// URL pointing at a path
        // that does not exist; cancellation must preempt the
        // ls_remote::poll attempt before any I/O is reached.
        let result = ls_remote_inline(
            "file:///nonexistent/cancelled-test-repo",
            "refs/heads/main",
            &cancel,
        )
        .await;
        match result {
            Err(PollCycleError::Cancelled) => {}
            other => panic!("expected Cancelled, got {other:?}"),
        }
    }

    /// Companion to the above: `Display` for `PollCycleError::Failed`
    /// must surface the wrapped error text verbatim. The poll loop
    /// renders the error via `&message` when calling
    /// `record_last_error`, so a divergent Display would garble the
    /// `gcit status` view.
    #[test]
    fn poll_cycle_error_failed_display_surfaces_inner_message_verbatim() {
        let e = PollCycleError::Failed("ls-refs parse: bad agent".to_string());
        assert_eq!(e.to_string(), "ls-refs parse: bad agent");
    }

    /// `Display` for `Cancelled` returns the literal "cancelled" so
    /// callers that DO want to log cancellation (e.g. the explicit
    /// `info!` arm in `run`) can stringify it via the same path as
    /// `Failed` without special-casing. The Err arm in `run` matches
    /// on the variant first and only stringifies `Failed`, so this
    /// Display impl is reached only by future / debug callers.
    #[test]
    fn poll_cycle_error_cancelled_display_is_lowercase_cancelled() {
        assert_eq!(PollCycleError::Cancelled.to_string(), "cancelled");
    }

    /// `unborn_ref_message` must name flow + ref + URL so an operator
    /// reading `gcit status` knows which flow is stuck and where to
    /// look. The format is shaped to be greppable: literal "ref " +
    /// the ref + " not found at " + the URL + an em-dash + remediation
    /// pointing at the operator's own config key (`flow.<name>.source.ref`).
    #[test]
    fn unborn_ref_message_names_flow_ref_and_url() {
        let m = unborn_ref_message("kernel-rc", "https://github.com/o/r", "refs/heads/main");
        assert!(
            m.contains("refs/heads/main"),
            "message must name the ref; got {m:?}"
        );
        assert!(
            m.contains("https://github.com/o/r"),
            "message must name the URL; got {m:?}"
        );
        assert!(
            m.contains("flow.kernel-rc.source.ref"),
            "message must name the operator's config key; got {m:?}"
        );
    }

    /// Pin the exact format so a refactor that drops one of the
    /// load-bearing fields (or that re-orders them in a way that
    /// breaks the operator's mental model) is caught at unit-test
    /// time. The integration tests in tests/poll_unborn_ref.rs are
    /// blocked on the supervisor end-to-end harness; this
    /// test is the only direct guard until that harness lands.
    #[test]
    fn unborn_ref_message_exact_format_is_stable() {
        let expected = "ref refs/tags/v1.0 not found at https://example.com/repo.git — verify the ref exists upstream, wait if it's still being pushed, or update flow.flow-x.source.ref";
        assert_eq!(
            unborn_ref_message("flow-x", "https://example.com/repo.git", "refs/tags/v1.0"),
            expected,
        );
    }

    /// Build a PollParams for the strategy-arm error tests. Constructs
    /// minimal-viable params: no rate_bucket, no octocrab, no reqwest,
    /// fresh last_errors map. Tests override the missing-handle
    /// fields by leaving them None (which is what each test's arm
    /// asserts the failure path on).
    fn params_with_url(url: &str) -> PollParams {
        PollParams {
            flow_name: "test-flow".to_string(),
            url: url.to_string(),
            ref_name: "refs/heads/main".to_string(),
            effective_poll: EffectivePoll {
                source_interval: Duration::from_secs(60),
                job_interval: Duration::from_secs(30),
                jitter: 0.0,
            },
            rate_bucket: None,
            octo: None,
            reqwest: None,
            last_errors: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
        }
    }

    #[tokio::test]
    async fn poll_one_github_api_errors_when_octocrab_handle_missing() {
        // Library-level invariant guard. In production, the supervisor
        // builds `params.octo` whenever auto_detect picks GithubApi
        // (flows::spawn_flow + credentials::acquire_github), so this
        // arm is unreachable from a real flow. The test pins the gate
        // anyway so a future caller wiring poll_one with a partial
        // PollParams (a new test harness, a refactor that reorders
        // auto_detect/strategy resolution, etc.) gets a clear
        // Failed("requires Octocrab handle") error rather than a
        // panic on the missing-handle unwrap. Catches regressions in
        // the gate itself, not in any production code path.
        let params = params_with_url("https://github.com/owner/repo");
        let cancel = CancellationToken::new();
        let mut fp: Option<String> = None;
        let err = poll_one(PollStrategy::GithubApi, &params, &mut fp, &cancel)
            .await
            .expect_err("missing octocrab must surface error");
        match err {
            PollCycleError::Failed(msg) => {
                assert!(
                    msg.contains("requires Octocrab handle"),
                    "error must name the missing handle; got: {msg}",
                );
            }
            other => panic!("expected Failed; got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn poll_one_grokmirror_errors_when_reqwest_handle_missing() {
        // Library-level invariant guard, companion to the GithubApi
        // arm above. Production wires `params.reqwest` whenever
        // auto_detect picks Grokmirror (the same supervisor wiring
        // path), so this arm is also unreachable from a real flow.
        // Pinned to catch regressions in the gate, not in any
        // production code path.
        let params = params_with_url("https://git.kernel.org/pub/scm/foo.git");
        let cancel = CancellationToken::new();
        let mut fp: Option<String> = None;
        let err = poll_one(PollStrategy::Grokmirror, &params, &mut fp, &cancel)
            .await
            .expect_err("missing reqwest must surface error");
        match err {
            PollCycleError::Failed(msg) => {
                assert!(
                    msg.contains("requires reqwest handle"),
                    "error must name the missing handle; got: {msg}",
                );
            }
            other => panic!("expected Failed; got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn poll_one_grokmirror_errors_when_url_lacks_host() {
        // The Grokmirror arm passes the source URL through `base_url`
        // to derive the manifest's scheme://host base. A URL that
        // parses but carries no host (file:// is the canonical case)
        // is rejected with "must carry a host" rather than panicking
        // or silently falling back. We pre-populate `reqwest` so the
        // earlier missing-handle gate doesn't fire — the test
        // exercises base_url's host check, not the reqwest gate.
        //
        // `reqwest::Client::new()` is currently a lazy constructor
        // that does not initialise rustls — but a future reqwest
        // version may eagerly construct a TLS backend at builder
        // time, which would panic without a registered global
        // CryptoProvider. Install the `ring` provider via the shared
        // Once helper so this test stays robust to that change.
        ensure_crypto_provider();
        let mut params = params_with_url("file:///local/repo");
        params.reqwest = Some(Arc::new(reqwest::Client::new()));
        let cancel = CancellationToken::new();
        let mut fp: Option<String> = None;
        let err = poll_one(PollStrategy::Grokmirror, &params, &mut fp, &cancel)
            .await
            .expect_err("hostless URL must surface error");
        match err {
            PollCycleError::Failed(msg) => {
                assert!(
                    msg.contains("must carry a host"),
                    "error must name the missing host; got: {msg}",
                );
            }
            other => panic!("expected Failed; got: {other:?}"),
        }
    }

    use crate::util::ensure_crypto_provider;

    #[tokio::test]
    async fn poll_one_github_api_errors_when_url_fails_to_parse() {
        // The GithubApi arm calls `split_github_repo` which wraps
        // `url::Url::parse`. An unparseable URL becomes a Failed
        // error with the "github URL parse" prefix so the operator
        // sees which stage of the github_api pipeline rejected the
        // input. Provide a real Octocrab handle so the prior
        // missing-handle gate doesn't fire; the parse failure is the
        // first real check after the gate.
        ensure_crypto_provider();
        let mut params = params_with_url("not a url at all");
        params.octo = Some(Arc::new(octocrab::Octocrab::builder().build().expect("octocrab")));
        let cancel = CancellationToken::new();
        let mut fp: Option<String> = None;
        let err = poll_one(PollStrategy::GithubApi, &params, &mut fp, &cancel)
            .await
            .expect_err("unparseable URL must surface error");
        match err {
            PollCycleError::Failed(msg) => {
                assert!(
                    msg.contains("github URL parse"),
                    "error must lead with the parse-stage prefix; got: {msg}",
                );
            }
            other => panic!("expected Failed; got: {other:?}"),
        }
    }

    /// Pre-baked PollExecutor that NEVER fires `poll_cycle`. Calls to
    /// poll_cycle would mean the loop reached its strategy-dispatch
    /// arm, which the entry-time-cancellation test wants to prove
    /// CANNOT happen with a pre-cancelled token. A panic surfaces as
    /// a test failure.
    struct NeverFiresExecutor;

    impl PollExecutor for NeverFiresExecutor {
        fn strategy_label(&self) -> &'static str {
            "never-fires"
        }
        async fn poll_cycle(
            &self,
            _params: &PollParams,
            _cancel: &CancellationToken,
        ) -> Result<PollOutcome, PollCycleError> {
            panic!(
                "NeverFiresExecutor::poll_cycle called — the cancel arm of the loop's \
                 sleep select! must have won before reaching strategy dispatch",
            );
        }
    }

    /// Pre-baked PollExecutor for the unit-level loop tests. Returns
    /// outcomes from a scripted queue; an exhausted queue surfaces as
    /// PollCycleError::Cancelled so the loop exits cleanly. Mirrors
    /// the integration-test ScriptedPollExecutor at
    /// tests/poll_unborn_ref.rs but defined here so the in-file unit
    /// tests can drive run_with_executor directly without crossing
    /// the integration-test boundary.
    struct UnitScriptedPollExecutor {
        script: tokio::sync::Mutex<std::collections::VecDeque<Result<PollOutcome, PollCycleError>>>,
    }

    impl UnitScriptedPollExecutor {
        fn new(outcomes: Vec<Result<PollOutcome, PollCycleError>>) -> Self {
            Self {
                script: tokio::sync::Mutex::new(outcomes.into()),
            }
        }
    }

    impl PollExecutor for UnitScriptedPollExecutor {
        fn strategy_label(&self) -> &'static str {
            "unit-scripted"
        }
        async fn poll_cycle(
            &self,
            _params: &PollParams,
            _cancel: &CancellationToken,
        ) -> Result<PollOutcome, PollCycleError> {
            let mut g = self.script.lock().await;
            g.pop_front().unwrap_or(Err(PollCycleError::Cancelled))
        }
    }

    fn unit_poll_params(
        flow_name: &str,
        last_errors: Arc<tokio::sync::Mutex<BTreeMap<String, crate::flow::supervisor::FlowLastError>>>,
    ) -> PollParams {
        PollParams {
            flow_name: flow_name.to_string(),
            url: "https://example.com/repo.git".to_string(),
            ref_name: "refs/heads/main".to_string(),
            effective_poll: EffectivePoll {
                // 1ms source_interval + zero jitter so the loop
                // proceeds through cycles in microseconds of virtual
                // time. The wiring is cadence-agnostic.
                source_interval: Duration::from_millis(1),
                job_interval: Duration::from_secs(30),
                jitter: 0.0,
            },
            rate_bucket: None,
            octo: None,
            reqwest: None,
            last_errors,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn run_with_executor_unchanged_emits_poll_timestamp_and_no_trigger() {
        // The Unchanged arm of `run_with_executor` sends
        // StateUpdate::PollTimestamp (NOT PollObservation) and clears
        // last_error on first success. No TriggerSignal because
        // Unchanged means the upstream did not change. Pin the per-arm
        // wiring: PollTimestamp emitted with the flow name, no
        // observation, no trigger.
        let last_errors = Arc::new(tokio::sync::Mutex::new(BTreeMap::new()));
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        let (trigger_tx, mut trigger_rx) = tokio::sync::mpsc::channel::<TriggerSignal>(8);
        let cancel = CancellationToken::new();
        let params = unit_poll_params("unchanged-flow", Arc::clone(&last_errors));
        let executor = UnitScriptedPollExecutor::new(vec![
            Ok(PollOutcome::Unchanged),
            Err(PollCycleError::Cancelled),
        ]);

        let cancel_for_task = cancel.clone();
        let task = tokio::spawn(async move {
            run_with_executor(params, executor, None, state_tx, trigger_tx, cancel_for_task).await;
        });

        // Drive virtual time forward and poll the state channel between
        // advances. apply_jitter clamps to MIN_INTERVAL (15s); under
        // parallel test load, fixed advance budgets can race the
        // task scheduler and miss the await-completion window. Polling
        // for the expected output stops as soon as it arrives.
        let mut saw_timestamp = false;
        for _ in 0..400 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            while let Ok(update) = state_rx.try_recv() {
                if let StateUpdate::PollTimestamp { flow, .. } = update {
                    assert_eq!(flow, "unchanged-flow");
                    saw_timestamp = true;
                }
            }
            if saw_timestamp {
                break;
            }
        }
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("loop must exit within 5s of cancel")
            .expect("loop must not panic");

        assert!(
            saw_timestamp,
            "Unchanged cycle must emit StateUpdate::PollTimestamp",
        );
        // No TriggerSignal — Unchanged means the upstream SHA did not
        // diverge.
        assert!(
            trigger_rx.try_recv().is_err(),
            "Unchanged must NOT emit a TriggerSignal",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn run_with_executor_failed_executor_records_last_error_and_continues() {
        // The Err(Failed(message)) arm of `run_with_executor` records
        // the failure under kind="git_poll_failed" with the message
        // body verbatim, then continues on cadence (no return — the
        // loop hits `continue`). Pin: last_error is recorded; the
        // loop proceeds to the next cycle and exits when the scripted
        // Cancelled is consumed.
        let last_errors = Arc::new(tokio::sync::Mutex::new(BTreeMap::new()));
        let (state_tx, _state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        let (trigger_tx, _trigger_rx) = tokio::sync::mpsc::channel::<TriggerSignal>(8);
        let cancel = CancellationToken::new();
        let params = unit_poll_params("failed-flow", Arc::clone(&last_errors));
        let executor = UnitScriptedPollExecutor::new(vec![
            Err(PollCycleError::Failed("upstream 502 bad gateway".to_string())),
            Err(PollCycleError::Cancelled),
        ]);

        let cancel_for_task = cancel.clone();
        let task = tokio::spawn(async move {
            run_with_executor(params, executor, None, state_tx, trigger_tx, cancel_for_task).await;
        });
        // `apply_jitter` clamps cycle sleeps to MIN_INTERVAL (15s).
        // Drive virtual time forward in chunks AND poll last_errors
        // between advances so the test stops as soon as the
        // record_last_error call lands. This is more deterministic
        // than a fixed advance budget — under high parallel test
        // load, tokio::time::advance + yield_now is not always enough
        // to let the spawned task drain a `.await` chain in one go.
        let mut recorded = false;
        for _ in 0..400 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            // Lightweight non-blocking probe: if the entry is present,
            // the Failed cycle has run record_last_error to completion
            // (await chain holds the mutex internally) and we can exit
            // the drive loop immediately.
            if last_errors.lock().await.contains_key("failed-flow") {
                recorded = true;
                break;
            }
        }
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("loop must exit")
            .expect("loop must not panic");

        assert!(
            recorded,
            "Failed cycle must record a last_error entry within the 200s virtual-time budget",
        );
        let g = last_errors.lock().await;
        let entry = g
            .get("failed-flow")
            .expect("Failed cycle must record last_error under flow name");
        assert_eq!(
            entry.kind(),
            "git_poll_failed",
            "Failed must route through kind=git_poll_failed",
        );
        assert!(
            entry.message().contains("upstream 502 bad gateway"),
            "last_error message must surface the verbatim Failed body; got: {:?}",
            entry.message(),
        );
    }

    #[tokio::test(start_paused = true)]
    async fn run_with_executor_refreshed_with_diff_emits_observation_and_trigger() {
        // The Refreshed arm of `run_with_executor` builds a
        // PollObservation, computes diff via compare_sha, and on
        // diff fires a TriggerSignal first, then the observation.
        // With initial_last_sha=Some(prev) and a Refreshed SHA that
        // differs, the trigger arm fires.
        let last_errors = Arc::new(tokio::sync::Mutex::new(BTreeMap::new()));
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        let (trigger_tx, mut trigger_rx) = tokio::sync::mpsc::channel::<TriggerSignal>(8);
        let cancel = CancellationToken::new();
        let params = unit_poll_params("diff-flow", Arc::clone(&last_errors));
        let new_sha = sha(0xab);
        let executor = UnitScriptedPollExecutor::new(vec![
            Ok(PollOutcome::Refreshed { sha: new_sha }),
            Err(PollCycleError::Cancelled),
        ]);
        let initial = Some(sha(0xff));

        let cancel_for_task = cancel.clone();
        let task = tokio::spawn(async move {
            run_with_executor(
                params,
                executor,
                initial,
                state_tx,
                trigger_tx,
                cancel_for_task,
            )
            .await;
        });
        // Poll for both the trigger AND observation as we drive virtual
        // time. The trigger arrives first (production order); the
        // observation lands once state_tx accepts. Polling stops when
        // both have been seen so the test does not depend on a fixed
        // advance budget under parallel test load.
        let mut received: Option<TriggerSignal> = None;
        let mut saw_observation = false;
        for _ in 0..400 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            if received.is_none() {
                if let Ok(t) = trigger_rx.try_recv() {
                    received = Some(t);
                }
            }
            while let Ok(update) = state_rx.try_recv() {
                if let StateUpdate::PollObservation { flow, last_sha, .. } = update {
                    if flow == "diff-flow" && last_sha == new_sha {
                        saw_observation = true;
                    }
                }
            }
            if received.is_some() && saw_observation {
                break;
            }
        }
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("loop must exit")
            .expect("loop must not panic");

        let received = received.expect("Refreshed-with-diff must emit a TriggerSignal");
        assert_eq!(received.observed_sha, new_sha);
        assert!(
            saw_observation,
            "Refreshed-with-diff must emit StateUpdate::PollObservation with the new SHA",
        );
    }

    #[tokio::test]
    async fn run_with_executor_returns_immediately_on_pre_cancelled_token() {
        // Entry-time cancellation: the supervisor signals cancel
        // before the first poll cycle has fired (e.g. SIGTERM during
        // a freshly-spawned flow). The loop's first awaitable arm is
        // the `tokio::select! { _ = cancel.cancelled() => return; _ =
        // sleep(source_interval) => {} }` race at the top of
        // `run_with_executor` (apply_jitter runs synchronously before
        // the select! so the select! is the first scheduling point,
        // not the first action outright). A pre-cancelled token makes
        // `cancel.cancelled()` immediately Ready while sleep stays
        // Pending until the full source_interval elapses; cancel wins
        // deterministically and the loop returns BEFORE the
        // strategy-dispatch executor is reached.
        //
        // Pin two facts:
        //   1. the loop returns within 100ms wall-clock (the test
        //      timeout) — proves the sleep arm did NOT win,
        //   2. NeverFiresExecutor::poll_cycle was NEVER invoked —
        //      proves the loop exited at the entry-time cancel arm
        //      rather than running one cycle and then cancelling.
        //
        // source_interval is set to 60s so even on a heavily-loaded
        // CI runner the sleep arm cannot accidentally win — 60s is
        // ~600x the test's 100ms wall-clock budget. The test never
        // actually waits 60s because cancel wins instantly.
        let cancel = CancellationToken::new();
        cancel.cancel();
        let params = PollParams {
            flow_name: "entry-cancel-flow".to_string(),
            url: "https://example.com/repo.git".to_string(),
            ref_name: "refs/heads/main".to_string(),
            effective_poll: EffectivePoll {
                source_interval: Duration::from_secs(60),
                job_interval: Duration::from_secs(30),
                jitter: 0.0,
            },
            rate_bucket: None,
            octo: None,
            reqwest: None,
            last_errors: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
        };
        let (state_tx, _state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        let (trigger_tx, _trigger_rx) = tokio::sync::mpsc::channel::<TriggerSignal>(8);
        let executor = NeverFiresExecutor;
        let start = std::time::Instant::now();
        tokio::time::timeout(
            Duration::from_millis(100),
            run_with_executor(params, executor, None, state_tx, trigger_tx, cancel),
        )
        .await
        .expect("pre-cancelled token must terminate the loop within 100ms wall-clock");
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(100),
            "loop must exit on the entry-time cancel arm, not consume the full source_interval; \
             elapsed: {:?}",
            elapsed,
        );
    }
}

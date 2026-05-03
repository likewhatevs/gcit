// Per-flow dispatch lifecycle.
//
// Pipeline (per `TriggerSignal`):
//   1. Build a `RunContext` from `FlowConfig` + the trigger.
//   2. Render the `[action.inputs]` map via handlebars (strict mode,
//      shared `notify::strict_handlebars()`).
//   3. Call `github::dispatcher::dispatch_with_retry`. The
//      `gcit_run_id` UUID generated here is injected into the
//      workflow_dispatch inputs payload.
//   4. On success, call `github::correlator::correlate` to resolve
//      the `Run.id` (correlator looks up the run carrying the same
//      `gcit_run_id` substring on `Run.name`).
//   5. Emit `StateUpdate::RunStarted` and spawn the per-run monitor.
//
// Errors at any stage are logged and surfaced via the supervisor's
// per-flow `last_error` tracking. The next `TriggerSignal` retries
// the whole pipeline from step 1 — there is no in-flight retry queue.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use crate::config::{ActionConfig, Destination};
use crate::git::rate_bucket::RateBucket;
use crate::github::client::Client as GithubClient;
use crate::github::correlator::{self, CorrelateParams, CorrelationError};
use crate::github::dispatcher::{self as gh_dispatcher, DispatchOutcome, DispatchParams};
use crate::github::error::GithubErrorKind;
use crate::github::rate_limit::RateLimitState;
use crate::notify::{self, ActionInfo, RunContext, SourceInfo};
use crate::state::StateUpdate;

use super::monitor::{spawn_monitor, MonitorParams};
use super::supervisor::{record_last_error, FlowLastError};
use super::TriggerSignal;

/// Maximum dispatch attempts (passed to backon's `with_max_times`).
/// Three matches the project's other retry policies (state writer,
/// poll loop) — enough to ride out a transient blip without masking
/// a hard failure.
pub const DISPATCH_MAX_ATTEMPTS: u32 = 3;

/// Per-flow dispatch parameters. Cheap to clone — every reference
/// is `Arc`-wrapped so the supervisor can hand identical handles to
/// multiple flows that share a credential.
///
/// `url` and `ref_name` are the source-side fields; the bare names
/// (no `source_` prefix) match `SourceConfig`/`SourceInfo`/`PollParams`
/// which already use the unprefixed form inside source-scoped scopes.
pub struct FlowDispatchParams {
    pub flow_name: String,
    pub flow_description: Option<String>,
    pub url: String,
    pub ref_name: String,
    pub action: ActionConfig,
    pub destinations: Vec<Destination>,
    /// GitHub HTTP client keyed off `action.credential_id`.
    pub github_client: Arc<GithubClient>,
    /// Per-credential pacing bucket. Shared across flows that use the
    /// same credential.
    pub rate_bucket: Arc<RateBucket>,
    /// Rate-limit snapshot for the credential. The poller in
    /// `github::rate_limit::poll_loop` refreshes this in the
    /// background.
    pub rate_limit: Arc<RateLimitState>,
    /// Effective `job_interval` for the per-run monitor. Already
    /// resolved by the supervisor (PollOverride.job_interval ?
    /// PollDefaults.job_interval).
    pub job_interval: Duration,
    /// Pre-built `Notifier` trait objects for the destinations on
    /// this flow. The supervisor builds these once and clones the
    /// Arcs into every per-flow dispatcher.
    pub notifiers: Vec<Arc<dyn DynNotifier>>,
}

/// Dyn-compatible wrapper over the per-kind `Notifier` impls.
///
/// `crate::notify::Notifier` uses native `async fn` in traits which
/// is not dyn-safe; this trait restates the public surface using
/// boxed futures so the supervisor can hold a heterogeneous
/// `Vec<Arc<dyn DynNotifier>>`.
///
/// All three lifecycle hooks are wired through the wrapper so the
/// fan-out paths in `flow::dispatcher` and `flow::monitor` can call
/// every variant without touching the concrete notifier types:
/// on_run_start fires after dispatch succeeds, on_job_complete fires
/// per-job in the monitor task, on_run_complete fires once at
/// terminal status.
pub trait DynNotifier: Send + Sync {
    fn kind(&self) -> &'static str;
    fn id(&self) -> &str;
    fn on_run_start<'a>(
        &'a self,
        ctx: &'a RunContext,
        cancel: &'a CancellationToken,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<crate::notify::NotifyOutcome, crate::notify::NotifyError>,
                > + Send
                + 'a,
        >,
    >;
    fn on_job_complete<'a>(
        &'a self,
        ctx: &'a RunContext,
        job: &'a crate::github::JobResult,
        cancel: &'a CancellationToken,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<crate::notify::NotifyOutcome, crate::notify::NotifyError>,
                > + Send
                + 'a,
        >,
    >;
    fn on_run_complete<'a>(
        &'a self,
        ctx: &'a RunContext,
        summary: &'a crate::github::RunSummary,
        cancel: &'a CancellationToken,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<crate::notify::NotifyOutcome, crate::notify::NotifyError>,
                > + Send
                + 'a,
        >,
    >;
}

impl<T> DynNotifier for T
where
    T: crate::notify::Notifier,
{
    fn kind(&self) -> &'static str {
        crate::notify::Notifier::kind(self)
    }
    fn id(&self) -> &str {
        crate::notify::Notifier::id(self)
    }
    fn on_run_start<'a>(
        &'a self,
        ctx: &'a RunContext,
        cancel: &'a CancellationToken,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<crate::notify::NotifyOutcome, crate::notify::NotifyError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(crate::notify::Notifier::on_run_start(self, ctx, cancel))
    }
    fn on_job_complete<'a>(
        &'a self,
        ctx: &'a RunContext,
        job: &'a crate::github::JobResult,
        cancel: &'a CancellationToken,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<crate::notify::NotifyOutcome, crate::notify::NotifyError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(crate::notify::Notifier::on_job_complete(
            self, ctx, job, cancel,
        ))
    }
    fn on_run_complete<'a>(
        &'a self,
        ctx: &'a RunContext,
        summary: &'a crate::github::RunSummary,
        cancel: &'a CancellationToken,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<crate::notify::NotifyOutcome, crate::notify::NotifyError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(crate::notify::Notifier::on_run_complete(
            self, ctx, summary, cancel,
        ))
    }
}

/// Test seam for the (dispatch + correlate) GitHub round-trip pair.
/// Production callers wire `RealDispatchExecutor`, which delegates to
/// `gh_dispatcher::dispatch_with_retry` followed by
/// `correlator::correlate`. Integration tests under `tests/` inject a
/// scripted impl that returns pre-baked outcomes without touching
/// the network — mirrors the `PollExecutor` seam in `flow::poll`.
///
/// Uses native async-fn-in-trait (stable since Rust 1.75) — callers
/// take `executor: &E` where `E: DispatchExecutor` (rather than
/// `&dyn DispatchExecutor`), avoiding `Pin<Box<dyn Future>>` allocation
/// per trigger and the `async_trait` macro dependency. The
/// MonitorEventSource and DynNotifier seams stay `Pin<Box<dyn Future>>`
/// because they are consumed by code that needs object-safe dyn
/// erasure (a JoinSet of heterogeneous notifier futures, an mpsc of
/// monitor events behind a boxed receiver).
pub trait DispatchExecutor: Send + Sync {
    fn execute(
        &self,
        dispatch_params: DispatchParams,
        branch: String,
        head_sha: String,
        cancel: CancellationToken,
    ) -> impl std::future::Future<Output = ExecuteOutcome> + Send;
}

/// Outcome of one (dispatch + correlate) round-trip.
pub enum ExecuteOutcome {
    /// Both dispatch and correlate succeeded. `handle_trigger`
    /// emits `StateUpdate::RunStarted`, fans out `on_run_start`,
    /// and spawns the per-run monitor.
    Success {
        dispatch: DispatchOutcome,
        correlation: crate::github::correlator::CorrelationOutcome,
    },
    /// `dispatch_with_retry` returned a terminal `GithubErrorKind`
    /// (or transient that exhausted the retry budget). `handle_trigger`
    /// surfaces this as `DispatchError::from_github_error("dispatch", ..)`,
    /// which the supervisor records under `last_error.kind = "dispatch"`.
    DispatchFailed(GithubErrorKind),
    /// Dispatch landed but the correlator could not resolve a `Run.id`
    /// (timeout, duplicate match, or a permanent GitHub error during
    /// the scan). `handle_trigger` surfaces this as
    /// `DispatchError::from_correlation_error("correlate", ..)`,
    /// which the supervisor records under `last_error.kind = "correlate"`.
    CorrelateFailed(CorrelationError),
}

/// Production `DispatchExecutor` — wraps the existing
/// `dispatch_with_retry` + `correlator::correlate` pair against a
/// real `GithubClient` + per-credential rate-limit / rate-bucket
/// state. The supervisor builds one of these per flow at startup.
pub struct RealDispatchExecutor {
    /// Per-credential octocrab client. Cloned by `Arc` so multiple
    /// flows that share a credential land on the same connection
    /// pool + rate-limit window.
    pub github_client: Arc<GithubClient>,
    /// Per-credential pacing bucket. Throttles the
    /// (dispatch + correlate) pair so concurrent flows on one
    /// credential do not starve each other's quota.
    pub rate_bucket: Arc<RateBucket>,
    /// Per-credential rate-limit snapshot from
    /// `X-RateLimit-Remaining`/`X-RateLimit-Reset` headers. Drives
    /// the cancel-aware `should_defer` wait in `dispatch()`.
    pub rate_limit: Arc<RateLimitState>,
    /// Backoff retry budget passed to `dispatch_with_retry`. The
    /// supervisor sets this to `DISPATCH_MAX_ATTEMPTS` (3) by
    /// default; tests script their own value.
    pub max_attempts: u32,
}

impl DispatchExecutor for RealDispatchExecutor {
    async fn execute(
        &self,
        dispatch_params: DispatchParams,
        branch: String,
        head_sha: String,
        cancel: CancellationToken,
    ) -> ExecuteOutcome {
        let outcome = match gh_dispatcher::dispatch_with_retry(
            self.github_client.as_ref(),
            Arc::clone(&self.rate_bucket),
            Arc::clone(&self.rate_limit),
            dispatch_params,
            self.max_attempts,
            cancel.clone(),
        )
        .await
        {
            Ok(o) => o,
            Err(e) => return ExecuteOutcome::DispatchFailed(e),
        };

        let correlate_params = CorrelateParams {
            repo: outcome.repo.clone(),
            workflow: outcome.workflow.clone(),
            gcit_run_id: outcome.gcit_run_id,
            branch,
            head_sha,
            dispatched_at: outcome.dispatched_at,
            run_name_configured: None,
        };
        match correlator::correlate(
            self.github_client.as_ref(),
            self.rate_limit.as_ref(),
            &correlate_params,
            cancel,
        )
        .await
        {
            Ok(c) => ExecuteOutcome::Success {
                dispatch: outcome,
                correlation: c,
            },
            Err(e) => ExecuteOutcome::CorrelateFailed(e),
        }
    }
}

/// Drive the per-flow dispatch lifecycle: receive `TriggerSignal`s
/// from the poll task and turn each into a (dispatch, correlate,
/// monitor) chain.
///
/// `state_tx` carries the `RunStarted` and `RunFinished` updates
/// onto the state writer's mpsc.
///
/// Cancellation: the supervisor's child token signals shutdown
/// (root cancel) or per-flow removal. In-flight monitor tasks are
/// owned by an internal `JoinSet`; they observe the same token and
/// drain mid-run rather than blocking shutdown.
pub async fn run(
    params: FlowDispatchParams,
    trigger_rx: Receiver<TriggerSignal>,
    state_tx: Sender<StateUpdate>,
    last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>>,
    cancel: CancellationToken,
) {
    let executor = RealDispatchExecutor {
        github_client: Arc::clone(&params.github_client),
        rate_bucket: Arc::clone(&params.rate_bucket),
        rate_limit: Arc::clone(&params.rate_limit),
        max_attempts: DISPATCH_MAX_ATTEMPTS,
    };
    run_with_executor(params, executor, trigger_rx, state_tx, last_errors, cancel).await;
}

/// Generic dispatch-loop driver — production callers go through `run`,
/// which constructs a `RealDispatchExecutor`; the supervisor end-to-end
/// test harness wires in a scripted `DispatchExecutor` to drive the
/// (dispatch + correlate) round-trip with pre-baked outcomes without
/// standing up wiremock + octocrab.
///
/// `#[doc(hidden)] pub` so the supervisor end-to-end test harness can
/// drive the loop directly with its scripted executor. Production
/// callers go through the thin-wrapper `run`.
///
/// Use `run_with_executor` for loop-level injection (the entire
/// dispatcher loop runs against the scripted executor). Use
/// `handle_trigger_with_executor` for per-trigger injection (a single
/// `TriggerSignal` is processed against a scripted executor without
/// spinning up the loop, channel, or JoinSet).
#[doc(hidden)]
pub async fn run_with_executor<E: DispatchExecutor>(
    params: FlowDispatchParams,
    executor: E,
    mut trigger_rx: Receiver<TriggerSignal>,
    state_tx: Sender<StateUpdate>,
    last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>>,
    cancel: CancellationToken,
) {
    info!(
        target: "gcit::flow::dispatcher",
        flow = %params.flow_name,
        "dispatcher loop starting",
    );
    // Track per-run monitor tasks so we can drain them on shutdown.
    let mut monitors: JoinSet<()> = JoinSet::new();
    let params = Arc::new(params);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                // Drain any triggers the poll task already enqueued
                // before observing cancel itself. Without this drain,
                // tokio's pseudo-random select! arm pick could fire
                // the cancel arm even though `trigger_rx.recv()` had
                // a queued message — and the trigger would be lost
                // forever (state.json already advanced because poll's
                // observation send is unraced after a successful
                // trigger send, so the next-gen poll sees no diff).
                //
                // try_recv() drains buffered messages without blocking;
                // it returns Empty as soon as the queue is exhausted.
                // Poll cannot enqueue NEW triggers post-cancel because
                // poll observes the same token (its trigger-send select!
                // races cancel and tokio mpsc send is cancel-safe — if
                // cancel wins, the message was not sent). The drain is
                // therefore bounded by TRIGGER_QUEUE.
                //
                // Each drained trigger goes through handle_trigger,
                // which propagates `cancel` into dispatch_with_retry +
                // correlator + monitor; those return Cancelled quickly
                // when the token is fired, so dispatching a backlog
                // mid-cancel does NOT block shutdown — each trigger
                // either completes the GitHub round-trip quickly or
                // surfaces GithubErrorKind::Cancelled and the result is
                // logged.
                let drained = drain_pending_triggers(
                    &params,
                    &mut trigger_rx,
                    &state_tx,
                    &last_errors,
                    &cancel,
                    &mut monitors,
                    &executor,
                ).await;
                info!(
                    target: "gcit::flow::dispatcher",
                    flow = %params.flow_name,
                    in_flight_monitors = monitors.len(),
                    drained_triggers = drained,
                    "dispatcher cancelled; draining monitors",
                );
                while monitors.join_next().await.is_some() {}
                return;
            }
            Some(_finished) = monitors.join_next(), if !monitors.is_empty() => {
                // monitor task completed. The monitor itself logs
                // its outcome; nothing to do here.
            }
            recv = trigger_rx.recv() => {
                let Some(trigger) = recv else {
                    info!(
                        target: "gcit::flow::dispatcher",
                        flow = %params.flow_name,
                        "trigger channel closed; dispatcher exiting",
                    );
                    while monitors.join_next().await.is_some() {}
                    return;
                };
                if let Err(e) = handle_trigger(
                    Arc::clone(&params),
                    trigger,
                    state_tx.clone(),
                    cancel.clone(),
                    &mut monitors,
                    &executor,
                )
                .await
                {
                    warn!(
                        target: "gcit::flow::dispatcher",
                        flow = %params.flow_name,
                        error = %e,
                        "dispatch failed",
                    );
                    record_last_error(
                        &last_errors,
                        &params.flow_name,
                        e.kind,
                        &e.message,
                        e.retry_at,
                    )
                    .await;
                }
            }
        }
    }
}

/// Drain any triggers the poll task enqueued before cancel was
/// observed. Each drained trigger is processed via the same
/// handle_trigger path as the normal loop body — the propagated
/// cancel token short-circuits the GitHub round-trip when the network
/// path can observe cancellation, so the drain returns quickly.
///
/// Returns the count of drained triggers for log fidelity.
async fn drain_pending_triggers<E: DispatchExecutor>(
    params: &Arc<FlowDispatchParams>,
    trigger_rx: &mut Receiver<TriggerSignal>,
    state_tx: &Sender<StateUpdate>,
    last_errors: &Arc<Mutex<BTreeMap<String, FlowLastError>>>,
    cancel: &CancellationToken,
    monitors: &mut JoinSet<()>,
    executor: &E,
) -> usize {
    let mut count = 0usize;
    loop {
        match trigger_rx.try_recv() {
            Ok(trigger) => {
                count += 1;
                if let Err(e) = handle_trigger(
                    Arc::clone(params),
                    trigger,
                    state_tx.clone(),
                    cancel.clone(),
                    monitors,
                    executor,
                )
                .await
                {
                    warn!(
                        target: "gcit::flow::dispatcher",
                        flow = %params.flow_name,
                        error = %e,
                        "dispatch failed during cancel drain",
                    );
                    record_last_error(
                        last_errors,
                        &params.flow_name,
                        e.kind,
                        &e.message,
                        e.retry_at,
                    )
                    .await;
                }
            }
            // Empty: no more buffered triggers — drain complete.
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => return count,
            // Disconnected: poll task closed its sender — same as
            // normal "trigger channel closed" path. No more triggers
            // possible.
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => return count,
        }
    }
}

/// Operator-visible error from a single dispatch attempt. Carries the
/// stringified message + the typed `kind` (matches the discriminator
/// the supervisor records under `last_error.kind`) + an optional
/// `retry_at` extracted from `GithubErrorKind::RateLimited.reset`.
/// The supervisor uses `retry_at` to render "next retry at ..." in
/// `gcit status` output; non-RateLimited errors leave it `None` and
/// the renderer hides the field.
#[derive(Debug)]
struct DispatchError {
    /// Discriminator routed into FlowLastError.kind. Stable across
    /// versions so operator dashboards / journalctl greps stay
    /// consistent.
    kind: &'static str,
    /// Operator-facing message body (the `Display` of the underlying
    /// error). Routed into FlowLastError.message.
    message: String,
    /// Wall-clock the daemon expects the error to clear. Populated
    /// only for `GithubErrorKind::RateLimited` (carries the quota
    /// reset epoch from response headers); `None` for every other
    /// kind. Routed into FlowLastError.retry_at.
    retry_at: Option<DateTime<Utc>>,
}

impl DispatchError {
    fn from_github_error(stage: &'static str, e: &GithubErrorKind) -> Self {
        let retry_at = match e {
            GithubErrorKind::RateLimited { reset, .. } => Some(*reset),
            _ => None,
        };
        Self {
            kind: stage,
            message: format!("{stage}: {e}"),
            retry_at,
        }
    }

    /// CorrelationError-variant adapter. Unwraps the Github(...)
    /// variant so the underlying GithubErrorKind's RateLimited.reset
    /// flows into retry_at; non-Github correlation errors
    /// (DuplicateMatch, Timeout) carry no retry hint.
    fn from_correlation_error(stage: &'static str, e: &CorrelationError) -> Self {
        let retry_at = match e {
            CorrelationError::Github(GithubErrorKind::RateLimited { reset, .. }) => Some(*reset),
            _ => None,
        };
        Self {
            kind: stage,
            message: format!("{stage}: {e}"),
            retry_at,
        }
    }

    fn from_message(kind: &'static str, message: String) -> Self {
        Self {
            kind,
            message,
            retry_at: None,
        }
    }
}

impl std::fmt::Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// Test seam for the per-trigger dispatch + correlate + RunStarted
/// pipeline. Integration tests under `tests/` cannot reach the
/// `pub(crate)` `run` loop nor the private `handle_trigger`, but they
/// need to drive the full pipeline against wiremock and observe the
/// emitted `StateUpdate::RunStarted`. This wrapper exposes
/// `handle_trigger` with a `Result<(), String>` signature so the
/// private `DispatchError` type doesn't leak into the test surface.
///
/// `#[doc(hidden)] pub` mirrors the `LocalMailNotifier::for_test` and
/// `Client::for_test` test-seam pattern: callable from integration
/// tests, hidden from rustdoc.
#[doc(hidden)]
pub async fn handle_trigger_for_test(
    params: Arc<FlowDispatchParams>,
    trigger: TriggerSignal,
    state_tx: Sender<StateUpdate>,
    cancel: CancellationToken,
    monitors: &mut JoinSet<()>,
) -> Result<(), String> {
    let executor = RealDispatchExecutor {
        github_client: Arc::clone(&params.github_client),
        rate_bucket: Arc::clone(&params.rate_bucket),
        rate_limit: Arc::clone(&params.rate_limit),
        max_attempts: DISPATCH_MAX_ATTEMPTS,
    };
    handle_trigger(params, trigger, state_tx, cancel, monitors, &executor)
        .await
        .map_err(|e| e.message)
}

/// Test seam parametric on the `DispatchExecutor` — lets integration
/// tests script the (dispatch + correlate) outcome pair WITHOUT
/// touching wiremock or octocrab. The post-execute lifecycle
/// (RunStarted, run_start fan-out, monitor spawn, on_run_complete
/// fan-out, RunFinished) is exercised against in-memory channels +
/// `RecordingNotifier`.
///
/// Production callers go through `handle_trigger_for_test` (or the
/// `run` loop) — both build a `RealDispatchExecutor`. This variant
/// is the explicit dependency-injection version.
#[doc(hidden)]
pub async fn handle_trigger_with_executor<E: DispatchExecutor>(
    params: Arc<FlowDispatchParams>,
    trigger: TriggerSignal,
    state_tx: Sender<StateUpdate>,
    cancel: CancellationToken,
    monitors: &mut JoinSet<()>,
    executor: &E,
) -> Result<(), String> {
    handle_trigger(params, trigger, state_tx, cancel, monitors, executor)
        .await
        .map_err(|e| e.message)
}

/// Process one trigger end-to-end.
#[instrument(level = "debug", skip(params, state_tx, cancel, monitors, executor), fields(
    flow = %params.flow_name,
    sha = %trigger.observed_sha,
))]
async fn handle_trigger<E: DispatchExecutor>(
    params: Arc<FlowDispatchParams>,
    trigger: TriggerSignal,
    state_tx: Sender<StateUpdate>,
    cancel: CancellationToken,
    monitors: &mut JoinSet<()>,
    executor: &E,
) -> Result<(), DispatchError> {
    // Extract action variant fields. ActionConfig is currently only
    // GithubWorkflowDispatch but match exhaustively to surface a
    // clear error if a new variant lands.
    let (repo, workflow, ref_name, inputs) = match &params.action {
        ActionConfig::GithubWorkflowDispatch {
            repo,
            workflow,
            ref_name,
            inputs,
            ..
        } => (
            repo.clone(),
            workflow.clone(),
            ref_name.clone(),
            inputs.clone(),
        ),
    };

    // Generate the gcit_run_id up front so the same UUID surfaces in
    // (a) the input-render data context's `{{gcit.run_id}}`, (b) the
    // dispatch payload's injected `inputs.gcit_run_id`, and (c) the
    // correlator's name-substring search needle. A UUID generated
    // inside `github::dispatcher` would diverge from the one used at
    // render time, leaving notifier templates with `Uuid::nil()` while
    // the workflow saw a real value. `inputs.gcit_run_id` is the ONE
    // auto-injected key; user-supplied `gcit_run_id` values are
    // rejected at config-load time.
    let gcit_run_id = Uuid::new_v4();
    // `trigger_dispatched_at` is the wall-clock at the point of input
    // render, which is the closest stand-in for dispatch time we have
    // before dispatch_with_retry returns its own DispatchOutcome.
    // Operator templates that render `{{action.dispatched_at}}` against
    // `inputs` get a meaningful timestamp instead of the upstream poll
    // observation time (which can lag dispatch by the poll interval).
    let trigger_dispatched_at = Utc::now();
    let trigger_run_ctx = RunContext {
        flow_name: params.flow_name.clone(),
        flow_description: params.flow_description.clone(),
        source: SourceInfo {
            url: params.url.clone(),
            ref_name: params.ref_name.clone(),
            sha: trigger.observed_sha,
            sha_short: short_sha(&trigger.observed_sha),
        },
        // ActionInfo is populated after correlation produces run_id +
        // run_url; the dispatcher's input rendering only uses source
        // + flow + gcit.run_id, all of which are known here. Carry a
        // placeholder for action.* — we'll overwrite before notifying.
        action: ActionInfo {
            repo: repo.clone(),
            workflow: workflow.clone(),
            run_id: 0,
            run_url: String::new(),
            dispatched_at: trigger_dispatched_at,
        },
        gcit_run_id,
    };
    let trigger_data = notify::render_context(&trigger_run_ctx, &empty_summary());

    let hb = notify::strict_handlebars();
    let rendered_inputs = match gh_dispatcher::render_inputs(&inputs, &trigger_data, &hb) {
        Ok(r) => r,
        Err(e) => {
            return Err(DispatchError::from_message(
                "input_render",
                format!("input render failed: {e}"),
            ));
        }
    };

    let dispatch_params = DispatchParams {
        repo: repo.clone(),
        workflow: workflow.clone(),
        ref_name: ref_name.clone(),
        gcit_run_id,
        rendered_inputs,
    };

    // Drive the (dispatch + correlate) pair via the executor seam.
    // Production callers pass `RealDispatchExecutor`; tests pass a
    // scripted executor. The seam lets tests cover the post-execute
    // lifecycle (RunStarted emission, run_start fan-out, monitor
    // spawn) without standing up wiremock + an octocrab client.
    let branch = correlator::ref_to_branch(&ref_name).to_string();
    let head_sha = trigger.observed_sha.to_string();
    let (outcome, correlation) = match executor
        .execute(dispatch_params, branch, head_sha, cancel.clone())
        .await
    {
        ExecuteOutcome::Success {
            dispatch,
            correlation,
        } => (dispatch, correlation),
        ExecuteOutcome::DispatchFailed(e) => {
            return Err(DispatchError::from_github_error("dispatch", &e));
        }
        ExecuteOutcome::CorrelateFailed(e) => {
            return Err(DispatchError::from_correlation_error("correlate", &e));
        }
    };
    debug!(
        gcit_run_id = %outcome.gcit_run_id,
        "dispatch succeeded; correlating",
    );

    // Emit RunStarted.
    if state_tx
        .send(StateUpdate::RunStarted {
            flow: params.flow_name.clone(),
            run_id: correlation.run_id,
            started_at: Utc::now(),
        })
        .await
        .is_err()
    {
        return Err(DispatchError::from_message(
            "state_writer",
            "state writer dropped".into(),
        ));
    }

    // Build the final RunContext (now with action.run_id +
    // action.run_url filled in). The monitor task uses this for the
    // notifier dispatch on terminal status.
    let final_run_ctx = RunContext {
        flow_name: params.flow_name.clone(),
        flow_description: params.flow_description.clone(),
        source: SourceInfo {
            url: params.url.clone(),
            ref_name: params.ref_name.clone(),
            sha: trigger.observed_sha,
            sha_short: short_sha(&trigger.observed_sha),
        },
        action: ActionInfo {
            repo: outcome.repo.clone(),
            workflow: outcome.workflow.clone(),
            run_id: correlation.run_id,
            run_url: correlation.summary.run_url.clone(),
            dispatched_at: outcome.dispatched_at,
        },
        gcit_run_id: outcome.gcit_run_id,
    };

    // Fan out `on_run_start` to every notifier. Fire-and-forget: a
    // slow notifier must not delay monitor spawn. Each per-notifier
    // task logs its own outcome via spawn_fan_out's uniform handler;
    // failures are isolated. Cancel propagates so an SIGTERM
    // mid-fan-out unblocks any blocking notifier syscall.
    {
        let ctx = final_run_ctx.clone();
        let cancel_for_fanout = cancel.clone();
        let _ = super::spawn_fan_out(&params.notifiers, "run-start", None, move |n| {
            let c = ctx.clone();
            let cancel = cancel_for_fanout.clone();
            async move { n.on_run_start(&c, &cancel).await }
        });
    }

    // Spawn the per-run monitor.
    let mparams = MonitorParams {
        flow_name: params.flow_name.clone(),
        repo: outcome.repo.clone(),
        workflow: outcome.workflow.clone(),
        run_id: correlation.run_id,
        job_interval: params.job_interval,
        github_client: Arc::clone(&params.github_client),
        rate_limit: Arc::clone(&params.rate_limit),
        notifiers: params.notifiers.clone(),
        run_context: final_run_ctx,
    };
    spawn_monitor(monitors, mparams, state_tx.clone(), cancel.clone());
    Ok(())
}

/// Short hex prefix of an `ObjectId` for `{{source.sha_short}}`. The
/// template-variable namespace pins this at "first 12 chars of sha";
/// use the same width here so notifier templates rendering against
/// the same data context get a stable shape.
fn short_sha(sha: &gix_hash::ObjectId) -> String {
    let s = sha.to_string();
    let n = s.chars().count().min(12);
    s.chars().take(n).collect()
}

/// Empty `RunSummary` for input-rendering against the trigger
/// context. Templates that probe `run.status` / `run.conclusion` get
/// "queued" / "(in progress)" — the dispatch hasn't happened yet so
/// no real run summary exists.
fn empty_summary() -> crate::github::RunSummary {
    crate::github::monitor::empty_run_summary(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_sha_pads_to_12_chars() {
        let sha = gix_hash::ObjectId::null(gix_hash::Kind::Sha1);
        let s = short_sha(&sha);
        assert_eq!(s.chars().count(), 12);
    }

    #[test]
    fn empty_summary_carries_zero_run_id() {
        let s = empty_summary();
        assert_eq!(s.run_id, 0);
    }

    /// Compile-time check: `DiscordNotifier` and `LocalMailNotifier`
    /// satisfy the `DynNotifier` blanket impl via their `Notifier`
    /// impl. Without this, the supervisor's `Vec<Arc<dyn DynNotifier>>`
    /// could silently drop a notifier kind.
    #[test]
    fn dyn_notifier_blanket_covers_concrete_kinds() {
        fn assert_dyn<T: DynNotifier>() {}
        assert_dyn::<crate::discord::DiscordNotifier>();
        assert_dyn::<crate::mail::LocalMailNotifier>();
    }

    #[test]
    fn short_sha_truncates_real_hex_to_12_chars() {
        // A non-null SHA covers the take(12) branch — the existing
        // null-SHA test only exercises the lower bound (count.min(12)
        // returns 12 because s has 40 chars). A real hex string
        // exercises the actual character iterator path.
        let sha = gix_hash::ObjectId::from_hex(b"deadbeefcafe1234567890abcdef1234567890ab")
            .expect("valid 40-hex sha");
        let s = short_sha(&sha);
        assert_eq!(s, "deadbeefcafe");
        assert_eq!(s.chars().count(), 12);
    }

    #[test]
    fn dispatch_error_from_github_error_rate_limited_carries_retry_at() {
        // RateLimited fills retry_at with the upstream-supplied reset
        // epoch so the supervisor's `gcit status` can render
        // "next retry at ...".
        use crate::config::CredentialId;
        let reset = chrono::Utc::now() + chrono::Duration::minutes(15);
        let e = GithubErrorKind::RateLimited {
            credential: CredentialId::new("gh-pat").expect("valid id"),
            reset,
        };
        let de = DispatchError::from_github_error("dispatch", &e);
        assert_eq!(de.kind, "dispatch");
        assert!(
            de.message.starts_with("dispatch:"),
            "message must lead with stage prefix; got: {}",
            de.message,
        );
        assert_eq!(
            de.retry_at,
            Some(reset),
            "RateLimited must propagate reset into retry_at",
        );
    }

    #[test]
    fn dispatch_error_from_github_error_non_rate_limited_leaves_retry_at_none() {
        // Every non-RateLimited GithubErrorKind variant must leave
        // retry_at as None — the renderer hides the field when None.
        // RateLimited is the only kind that carries a wall-clock hint.
        use crate::config::CredentialId;
        let e = GithubErrorKind::Unauthorized {
            credential: CredentialId::new("gh-pat").expect("valid id"),
        };
        let de = DispatchError::from_github_error("dispatch", &e);
        assert_eq!(de.kind, "dispatch");
        assert!(de.message.starts_with("dispatch:"));
        assert!(
            de.retry_at.is_none(),
            "non-RateLimited must leave retry_at None",
        );
    }

    #[test]
    fn dispatch_error_from_correlation_error_unwraps_github_rate_limited() {
        // Github(RateLimited) is the only CorrelationError variant
        // that surfaces a retry hint — the adapter must unwrap the
        // inner GithubErrorKind to extract `reset`.
        use crate::config::CredentialId;
        let reset = chrono::Utc::now() + chrono::Duration::minutes(30);
        let e = CorrelationError::Github(GithubErrorKind::RateLimited {
            credential: CredentialId::new("gh-pat").expect("valid id"),
            reset,
        });
        let de = DispatchError::from_correlation_error("correlate", &e);
        assert_eq!(de.kind, "correlate");
        assert!(de.message.starts_with("correlate:"));
        assert_eq!(de.retry_at, Some(reset));
    }

    #[test]
    fn dispatch_error_from_correlation_error_duplicate_match_has_no_retry_at() {
        // DuplicateMatch is a permanent correlation failure (the
        // run-name search returned multiple candidates). No retry
        // hint to surface — the operator must investigate.
        use uuid::Uuid;
        let e = CorrelationError::DuplicateMatch {
            repo: "myorg/linux-builder".to_string(),
            workflow: "ci.yml".to_string(),
            gcit_run_id: Uuid::nil(),
            run_ids: vec![101, 102],
        };
        let de = DispatchError::from_correlation_error("correlate", &e);
        assert_eq!(de.kind, "correlate");
        assert!(de.message.starts_with("correlate:"));
        // Both run ids must surface in the body so the operator can
        // identify the colliding runs from journalctl alone.
        assert!(
            de.message.contains("101") && de.message.contains("102"),
            "DuplicateMatch must list both run ids; got: {}",
            de.message,
        );
        assert!(de.retry_at.is_none());
    }

    #[test]
    fn dispatch_error_from_correlation_error_timeout_has_no_retry_at() {
        // Timeout: the correlator polled until its budget elapsed
        // without finding a match. retry_at is None because the
        // supervisor's standard 1-trigger-per-cycle poll already
        // bounds the retry rhythm.
        use uuid::Uuid;
        let e = CorrelationError::Timeout {
            repo: "myorg/linux-builder".to_string(),
            workflow: "ci.yml".to_string(),
            gcit_run_id: Uuid::nil(),
            timeout: Duration::from_secs(30),
        };
        let de = DispatchError::from_correlation_error("correlate", &e);
        assert_eq!(de.kind, "correlate");
        assert!(de.retry_at.is_none());
    }

    #[test]
    fn dispatch_error_from_correlation_error_github_non_rate_limited_has_no_retry_at() {
        // The Github(GithubErrorKind::*) variants OTHER than
        // RateLimited must NOT propagate a retry hint — the unwrap
        // logic only matches the RateLimited inner shape.
        use crate::config::CredentialId;
        let e = CorrelationError::Github(GithubErrorKind::Unauthorized {
            credential: CredentialId::new("gh-pat").expect("valid id"),
        });
        let de = DispatchError::from_correlation_error("correlate", &e);
        assert!(de.retry_at.is_none());
    }

    #[test]
    fn dispatch_error_from_message_carries_kind_and_body_with_no_retry_at() {
        // from_message is the input-render-failure constructor;
        // the body is supplied verbatim and retry_at is always None.
        let de = DispatchError::from_message(
            "input_render",
            "input render failed: undefined.var".to_string(),
        );
        assert_eq!(de.kind, "input_render");
        assert_eq!(de.message, "input render failed: undefined.var");
        assert!(de.retry_at.is_none());
    }

    #[test]
    fn dispatch_error_display_writes_message_body_verbatim() {
        // DispatchError's Display is the canonical "what hit
        // last_error.message" stringification — `f.write_str(&self.message)`.
        // A regression that wraps or prefixes the body would break the
        // dispatcher::run loop's `error = %e` log field.
        let de = DispatchError::from_message("k", "raw body".to_string());
        assert_eq!(format!("{de}"), "raw body");
    }

    #[test]
    fn dispatch_error_from_message_input_render_carries_input_render_kind() {
        let de = DispatchError::from_message(
            "input_render",
            "input render failed: undefined var".to_string(),
        );
        assert_eq!(de.kind, "input_render");
    }

    /// Pre-baked DispatchExecutor for the in-file unit tests.
    /// Returns one canned `ExecuteOutcome` per call, popped from the
    /// caller-loaded queue. Calls beyond the queue length panic so a
    /// forgotten outcome surfaces loudly. Mirrors the integration-test
    /// version in tests/flow_dispatcher_executor.rs but defined here so
    /// the in-file unit tests can drive `handle_trigger_with_executor`
    /// directly without crossing the integration-test boundary.
    struct UnitScriptedExecutor {
        outcomes: tokio::sync::Mutex<Vec<ExecuteOutcome>>,
    }

    impl UnitScriptedExecutor {
        fn new(outcomes: Vec<ExecuteOutcome>) -> Self {
            Self {
                outcomes: tokio::sync::Mutex::new(outcomes),
            }
        }
    }

    impl DispatchExecutor for UnitScriptedExecutor {
        async fn execute(
            &self,
            _dispatch_params: DispatchParams,
            _branch: String,
            _head_sha: String,
            _cancel: CancellationToken,
        ) -> ExecuteOutcome {
            self.outcomes
                .lock()
                .await
                .pop()
                .expect("UnitScriptedExecutor: outcome queue exhausted")
        }
    }

    use crate::util::ensure_crypto_provider;

    /// Build a minimal-viable `Arc<FlowDispatchParams>` for the
    /// scripted-executor tests. The `github_client` is required by the
    /// struct shape but never read by `UnitScriptedExecutor` — point at
    /// an unreachable URL so any accidental production-path call would
    /// fail loudly rather than reach github.com. `inputs` is supplied
    /// by the caller so the input-render-failure test can inject a
    /// strict-mode-rejected handlebars template.
    fn build_test_params(inputs: BTreeMap<String, String>) -> Arc<FlowDispatchParams> {
        ensure_crypto_provider();
        let github_client = Arc::new(
            GithubClient::builder()
                .credential(crate::config::CredentialId::new("github_pat").expect("valid id"))
                .token(secrecy::SecretString::from(
                    "github_pat_FFFF0000unit_test_unreachableFFFF".to_string(),
                ))
                .request_timeout(Duration::from_secs(1))
                .base_uri("http://127.0.0.1:1")
                .build()
                .expect("client build"),
        );
        Arc::new(FlowDispatchParams {
            flow_name: "unit-flow".into(),
            flow_description: Some("unit test flow".into()),
            url: "https://example.com/repo.git".into(),
            ref_name: "refs/heads/main".into(),
            action: ActionConfig::GithubWorkflowDispatch {
                repo: "myorg/linux-builder".into(),
                workflow: "ci.yml".into(),
                ref_name: "refs/heads/main".into(),
                credential_id: crate::config::CredentialId::new("github_pat").expect("valid id"),
                inputs,
            },
            destinations: Vec::new(),
            github_client,
            rate_bucket: Arc::new(RateBucket::new(Duration::from_millis(0))),
            rate_limit: Arc::new(RateLimitState::new()),
            job_interval: Duration::from_secs(30),
            notifiers: Vec::new(),
        })
    }

    fn unit_test_trigger() -> TriggerSignal {
        let head_sha = gix_hash::ObjectId::from_hex(b"deadbeefcafe1234567890abcdef1234567890ab")
            .expect("valid 40-hex sha");
        TriggerSignal {
            observed_sha: head_sha,
            observed_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn handle_trigger_with_executor_dispatch_failed_unauthorized_surfaces_dispatch_prefix() {
        // ExecuteOutcome::DispatchFailed(Unauthorized) routes through
        // `DispatchError::from_github_error("dispatch", ..)`. The
        // error body must lead with the canonical "dispatch:" stage
        // prefix so operators reading `gcit status` / journald can
        // route the failure to the dispatch stage rather than
        // confusing it with a correlate-stage failure carrying the
        // same kind label.
        let executor = UnitScriptedExecutor::new(vec![ExecuteOutcome::DispatchFailed(
            GithubErrorKind::Unauthorized {
                credential: crate::config::CredentialId::new("github_pat").expect("valid id"),
            },
        )]);
        let params = build_test_params(BTreeMap::new());
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        let cancel = CancellationToken::new();
        let mut monitors: JoinSet<()> = JoinSet::new();
        let err = handle_trigger_with_executor(
            params,
            unit_test_trigger(),
            state_tx,
            cancel,
            &mut monitors,
            &executor,
        )
        .await
        .expect_err("DispatchFailed(Unauthorized) must surface as Err");
        assert!(
            err.starts_with("dispatch:"),
            "error body must lead with the 'dispatch:' stage prefix; got: {err}",
        );
        assert!(
            state_rx.try_recv().is_err(),
            "DispatchFailed must NOT emit RunStarted",
        );
        assert!(
            monitors.is_empty(),
            "DispatchFailed must NOT spawn a monitor",
        );
    }

    #[tokio::test]
    async fn handle_trigger_with_executor_correlate_failed_timeout_surfaces_correlate_prefix() {
        // ExecuteOutcome::CorrelateFailed(CorrelationError::Timeout)
        // routes through `DispatchError::from_correlation_error(
        // "correlate", ..)`. The error body must lead with the
        // canonical "correlate:" stage prefix. Timeout is the
        // correlate-stage failure operators see when the
        // workflow_dispatch arrived but the run never appeared in the
        // window — distinct from DuplicateMatch (covered by the
        // integration test) and Github(...) variants (covered by the
        // unit tests on DispatchError::from_correlation_error above).
        let executor = UnitScriptedExecutor::new(vec![ExecuteOutcome::CorrelateFailed(
            CorrelationError::Timeout {
                repo: "myorg/linux-builder".into(),
                workflow: "ci.yml".into(),
                gcit_run_id: Uuid::nil(),
                timeout: Duration::from_secs(30),
            },
        )]);
        let params = build_test_params(BTreeMap::new());
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        let cancel = CancellationToken::new();
        let mut monitors: JoinSet<()> = JoinSet::new();
        let err = handle_trigger_with_executor(
            params,
            unit_test_trigger(),
            state_tx,
            cancel,
            &mut monitors,
            &executor,
        )
        .await
        .expect_err("CorrelateFailed(Timeout) must surface as Err");
        assert!(
            err.starts_with("correlate:"),
            "error body must lead with the 'correlate:' stage prefix; got: {err}",
        );
        assert!(
            state_rx.try_recv().is_err(),
            "CorrelateFailed must NOT emit RunStarted",
        );
        assert!(
            monitors.is_empty(),
            "CorrelateFailed must NOT spawn a monitor",
        );
    }

    #[tokio::test]
    async fn handle_trigger_with_executor_input_render_failure_short_circuits_before_executor() {
        // A handlebars template referencing an undefined variable in
        // action.inputs makes `gh_dispatcher::render_inputs` return Err.
        // `handle_trigger` surfaces this via
        // `DispatchError::from_message("input_render", "input render
        // failed: ...")`. The executor must NOT be invoked — load it
        // with an empty queue so any call panics with "queue
        // exhausted" and surfaces as a test failure.
        let mut inputs = BTreeMap::new();
        inputs.insert("broken".to_string(), "{{undefined.var.path}}".to_string());
        let params = build_test_params(inputs);
        let executor = UnitScriptedExecutor::new(Vec::new());
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        let cancel = CancellationToken::new();
        let mut monitors: JoinSet<()> = JoinSet::new();
        let err = handle_trigger_with_executor(
            params,
            unit_test_trigger(),
            state_tx,
            cancel,
            &mut monitors,
            &executor,
        )
        .await
        .expect_err("input render failure must surface as Err");
        assert!(
            err.starts_with("input render failed:"),
            "error must lead with the 'input render failed:' prefix; got: {err}",
        );
        assert!(
            state_rx.try_recv().is_err(),
            "input render failure must NOT emit RunStarted",
        );
        assert!(
            monitors.is_empty(),
            "input render failure must NOT spawn a monitor",
        );
    }

    #[tokio::test]
    async fn handle_trigger_with_executor_success_emits_run_started_and_spawns_monitor() {
        // ExecuteOutcome::Success drives the post-execute lifecycle:
        //   1. emits StateUpdate::RunStarted{flow, run_id, started_at}
        //      on state_tx
        //   2. fans out on_run_start to every notifier — this test
        //      ships an empty notifiers Vec so the fan-out is a no-op,
        //      isolating the assertion to the channel + monitor spawn
        //   3. calls spawn_monitor which inserts a task into the
        //      `monitors` JoinSet
        //
        // Pre-cancel the token so the spawned monitor's first await
        // (its source.drive's select!) observes cancel and exits
        // immediately — this prevents the monitor from making real
        // GitHub API calls against the unreachable test base_uri.
        let dispatched_at = Utc::now();
        let success_outcome = ExecuteOutcome::Success {
            dispatch: DispatchOutcome {
                gcit_run_id: Uuid::new_v4(),
                dispatched_at,
                repo: "myorg/linux-builder".to_string(),
                workflow: "ci.yml".to_string(),
                ref_name: "refs/heads/main".to_string(),
            },
            correlation: crate::github::correlator::CorrelationOutcome {
                run_id: 4242,
                summary: crate::github::monitor::empty_run_summary(4242),
            },
        };
        let executor = UnitScriptedExecutor::new(vec![success_outcome]);
        let params = build_test_params(BTreeMap::new());
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        let cancel = CancellationToken::new();
        // Pre-cancel BEFORE handle_trigger spawns the monitor so the
        // monitor's first poll observes the already-cancelled token
        // and unwinds without contacting the unreachable github base
        // URI. handle_trigger itself does not check the token between
        // executor.execute() and spawn_monitor — it propagates the
        // token clone through DispatchExecutor + spawn_monitor; our
        // UnitScriptedExecutor returns synchronously regardless of
        // cancel state, so the executor still produces Success.
        cancel.cancel();
        let mut monitors: JoinSet<()> = JoinSet::new();
        handle_trigger_with_executor(
            params,
            unit_test_trigger(),
            state_tx,
            cancel,
            &mut monitors,
            &executor,
        )
        .await
        .expect("Success outcome must NOT surface as Err");

        // (1) StateUpdate::RunStarted emitted with run_id=4242.
        let update = state_rx
            .try_recv()
            .expect("Success path must emit StateUpdate::RunStarted on state_tx");
        match update {
            StateUpdate::RunStarted {
                flow,
                run_id,
                started_at: _,
            } => {
                assert_eq!(
                    flow, "unit-flow",
                    "RunStarted.flow must echo params.flow_name set by `build_test_params`",
                );
                assert_eq!(
                    run_id, 4242,
                    "RunStarted.run_id must carry the correlator's resolved Run.id",
                );
            }
            other => panic!(
                "Success path must emit RunStarted (not {other:?}) as the first state update"
            ),
        }
        // No further state updates emitted by handle_trigger itself
        // (the monitor task may emit RunFinished later, but with the
        // pre-cancelled token it exits before reaching that path).
        // Don't pin "no more updates" since the monitor task races us.

        // (3) Monitor spawned. `spawn_monitor` inserts exactly one
        // task into the JoinSet; a regression that skipped
        // `spawn_monitor` would leave the JoinSet empty.
        assert_eq!(
            monitors.len(),
            1,
            "Success path must spawn exactly one monitor task into the JoinSet",
        );

        // Drain the cancelled monitor so the test's tokio runtime
        // doesn't observe a leaked spawned task on shutdown.
        while monitors.join_next().await.is_some() {}
    }

    #[tokio::test]
    async fn drain_pending_triggers_processes_each_buffered_trigger_and_records_last_error_per_failed_dispatch(
    ) {
        // The cancel arm of `run_with_executor` calls
        // `drain_pending_triggers` to flush triggers the poll task
        // had already enqueued before observing cancel. Each drained
        // trigger goes through `handle_trigger`; on failure,
        // `drain_pending_triggers` records last_error and continues.
        // The returned count matches the number of triggers actually
        // pulled from the channel.
        //
        // Drive the function directly with a pre-filled channel + a
        // scripted executor returning DispatchFailed for every call.
        let params = build_test_params(BTreeMap::new());
        let (trigger_tx, mut trigger_rx) =
            tokio::sync::mpsc::channel::<TriggerSignal>(crate::flow::TRIGGER_QUEUE);
        // Pre-fill 3 triggers. try_send is non-blocking and succeeds
        // because TRIGGER_QUEUE=8 leaves plenty of buffer slots.
        for _ in 0..3 {
            trigger_tx
                .try_send(unit_test_trigger())
                .expect("buffer must accept pre-filled trigger");
        }
        // Drop the sender so the channel state mirrors "poll task
        // finished sending before cancel was observed". With the
        // sender alive, try_recv would not see Disconnected after the
        // 3 buffered entries — but that doesn't matter for the drain
        // path, which exits on TryRecvError::Empty just as well.
        drop(trigger_tx);

        let executor = UnitScriptedExecutor::new(vec![
            ExecuteOutcome::DispatchFailed(GithubErrorKind::Unauthorized {
                credential: crate::config::CredentialId::new("github_pat").expect("valid id"),
            }),
            ExecuteOutcome::DispatchFailed(GithubErrorKind::Unauthorized {
                credential: crate::config::CredentialId::new("github_pat").expect("valid id"),
            }),
            ExecuteOutcome::DispatchFailed(GithubErrorKind::Unauthorized {
                credential: crate::config::CredentialId::new("github_pat").expect("valid id"),
            }),
        ]);
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        let last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>> =
            Arc::new(Mutex::new(BTreeMap::new()));
        let cancel = CancellationToken::new();
        // Pre-cancel so any monitor spawn (none on the failure path
        // but defense-in-depth) collapses immediately.
        cancel.cancel();
        let mut monitors: JoinSet<()> = JoinSet::new();

        let count = drain_pending_triggers(
            &params,
            &mut trigger_rx,
            &state_tx,
            &last_errors,
            &cancel,
            &mut monitors,
            &executor,
        )
        .await;

        // (1) Returned count matches the number of triggers buffered.
        assert_eq!(
            count, 3,
            "drain_pending_triggers must return exactly the number of triggers it pulled from the channel; got {count}",
        );
        // (2) After draining, try_recv on the receiver returns Disconnected
        // (sender was dropped above) — drain successfully consumed every
        // buffered trigger before the function returned.
        assert!(
            matches!(
                trigger_rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
            ),
            "after the drain, the receiver must be empty + disconnected (every buffered trigger consumed)",
        );
        // (3) last_error recorded with kind="dispatch" — the LAST
        // failed drain overwrites earlier entries (record_last_error
        // uses BTreeMap::insert).
        let errs = last_errors.lock().await;
        let entry = errs.get(&params.flow_name).expect(
            "each failed drain must record a last_error; the final overwrite must be present",
        );
        assert_eq!(
            entry.kind(),
            "dispatch",
            "DispatchFailed drains route through DispatchError::from_github_error('dispatch', ..); last_error.kind must be 'dispatch'",
        );
        assert!(
            entry.message().starts_with("dispatch:"),
            "drain-failure message must lead with the canonical 'dispatch:' stage prefix; got: {}",
            entry.message(),
        );
        // (4) No RunStarted emitted — every drained trigger failed at
        // executor.execute() so the post-execute lifecycle never ran.
        assert!(
            state_rx.try_recv().is_err(),
            "DispatchFailed drains must NOT emit RunStarted on state_tx",
        );
        assert!(
            monitors.is_empty(),
            "DispatchFailed drains must NOT spawn any monitor task",
        );
    }

    #[tokio::test]
    async fn drain_pending_triggers_returns_zero_count_on_empty_channel() {
        // Empty channel + dropped sender → try_recv returns
        // Disconnected on the first call → drain_pending_triggers
        // returns count=0 immediately. Pin so a regression that
        // off-by-ones the count (e.g., counts the Disconnected
        // sentinel as a drained trigger) surfaces.
        let params = build_test_params(BTreeMap::new());
        let (trigger_tx, mut trigger_rx) =
            tokio::sync::mpsc::channel::<TriggerSignal>(crate::flow::TRIGGER_QUEUE);
        drop(trigger_tx);

        // Empty queue — handle_trigger would panic if invoked, so the
        // drain MUST exit without dispatching anything.
        let executor = UnitScriptedExecutor::new(Vec::new());
        let (state_tx, _state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        let last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>> =
            Arc::new(Mutex::new(BTreeMap::new()));
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut monitors: JoinSet<()> = JoinSet::new();

        let count = drain_pending_triggers(
            &params,
            &mut trigger_rx,
            &state_tx,
            &last_errors,
            &cancel,
            &mut monitors,
            &executor,
        )
        .await;

        assert_eq!(
            count, 0,
            "empty/disconnected channel must yield count=0; got {count}",
        );
        assert!(
            last_errors.lock().await.is_empty(),
            "empty drain must not record any last_error",
        );
        assert!(monitors.is_empty());
    }

    /// Hand-rolled minimal recording notifier. The `tests/common/recording_notifier.rs`
    /// helper is gated behind the integration-test crate boundary
    /// (its `mod common;` declaration only compiles inside `tests/<name>.rs`
    /// binaries), so this in-file unit-test mod cannot import it.
    /// This local copy records every `on_run_start` call so the
    /// dispatcher's fan-out path can be asserted on at the unit level.
    struct UnitRecordingNotifier {
        kind: &'static str,
        id: String,
        on_run_start_calls: tokio::sync::Mutex<usize>,
    }

    impl UnitRecordingNotifier {
        fn new(id: impl Into<String>) -> Self {
            Self {
                kind: "recording",
                id: id.into(),
                on_run_start_calls: tokio::sync::Mutex::new(0),
            }
        }
    }

    impl DynNotifier for UnitRecordingNotifier {
        fn kind(&self) -> &'static str {
            self.kind
        }

        fn id(&self) -> &str {
            &self.id
        }

        fn on_run_start<'a>(
            &'a self,
            _ctx: &'a crate::notify::RunContext,
            _cancel: &'a CancellationToken,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<crate::notify::NotifyOutcome, crate::notify::NotifyError>,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                *self.on_run_start_calls.lock().await += 1;
                Ok(crate::notify::NotifyOutcome::Sent {
                    receipt: "unit-recorded".to_string(),
                })
            })
        }

        fn on_job_complete<'a>(
            &'a self,
            _ctx: &'a crate::notify::RunContext,
            _job: &'a crate::github::JobResult,
            _cancel: &'a CancellationToken,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<crate::notify::NotifyOutcome, crate::notify::NotifyError>,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                Ok(crate::notify::NotifyOutcome::Sent {
                    receipt: "unit-recorded".to_string(),
                })
            })
        }

        fn on_run_complete<'a>(
            &'a self,
            _ctx: &'a crate::notify::RunContext,
            _summary: &'a crate::github::RunSummary,
            _cancel: &'a CancellationToken,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<crate::notify::NotifyOutcome, crate::notify::NotifyError>,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                Ok(crate::notify::NotifyOutcome::Sent {
                    receipt: "unit-recorded".to_string(),
                })
            })
        }
    }

    #[tokio::test]
    async fn handle_trigger_with_executor_success_state_writer_dropped_returns_state_writer_error()
    {
        // The Success arm sends RunStarted to state_tx. If the
        // receiver was dropped (writer thread exited), the send
        // returns Err; `handle_trigger` surfaces this as
        // `DispatchError::from_message("state_writer", "state writer
        // dropped")`. Pin the error stage prefix so a regression that
        // labelled this as "dispatch:" or "correlate:" surfaces.
        let executor = UnitScriptedExecutor::new(vec![ExecuteOutcome::Success {
            dispatch: DispatchOutcome {
                gcit_run_id: Uuid::new_v4(),
                dispatched_at: Utc::now(),
                repo: "myorg/linux-builder".to_string(),
                workflow: "ci.yml".to_string(),
                ref_name: "refs/heads/main".to_string(),
            },
            correlation: crate::github::correlator::CorrelationOutcome {
                run_id: 123,
                summary: crate::github::monitor::empty_run_summary(123),
            },
        }]);
        let params = build_test_params(BTreeMap::new());
        // Drop the receiver before the helper sends RunStarted.
        let (state_tx, state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        drop(state_rx);
        let cancel = CancellationToken::new();
        let mut monitors: JoinSet<()> = JoinSet::new();
        let err = handle_trigger_with_executor(
            params,
            unit_test_trigger(),
            state_tx,
            cancel,
            &mut monitors,
            &executor,
        )
        .await
        .expect_err("dropped state_rx must surface as Err");
        assert_eq!(
            err, "state writer dropped",
            "state_writer error must carry the canonical 'state writer dropped' message; got: {err}",
        );
        // Monitor MUST NOT be spawned when RunStarted send fails —
        // `handle_trigger` returns BEFORE the `spawn_monitor` call.
        assert!(
            monitors.is_empty(),
            "state_writer error path must NOT spawn a monitor task",
        );
    }

    #[tokio::test]
    async fn handle_trigger_with_executor_success_with_inputs_renders_handlebars_template() {
        // The `render_inputs` branch in `handle_trigger` succeeds
        // when every input template renders against the trigger
        // RunContext. Drive the success path with a non-empty inputs
        // map so `render_inputs` runs to completion and the executor
        // receives the rendered map. The existing success tests use
        // empty inputs, so the render-success branch was uncovered
        // before.
        let mut inputs = BTreeMap::new();
        inputs.insert("flow_name".to_string(), "{{flow.name}}".to_string());
        inputs.insert("ref_name".to_string(), "{{source.ref_name}}".to_string());
        inputs.insert(
            "static_value".to_string(),
            "literal text no template".to_string(),
        );
        let executor = UnitScriptedExecutor::new(vec![ExecuteOutcome::Success {
            dispatch: DispatchOutcome {
                gcit_run_id: Uuid::new_v4(),
                dispatched_at: Utc::now(),
                repo: "myorg/linux-builder".to_string(),
                workflow: "ci.yml".to_string(),
                ref_name: "refs/heads/main".to_string(),
            },
            correlation: crate::github::correlator::CorrelationOutcome {
                run_id: 555,
                summary: crate::github::monitor::empty_run_summary(555),
            },
        }]);
        let params = build_test_params(inputs);
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut monitors: JoinSet<()> = JoinSet::new();
        handle_trigger_with_executor(
            params,
            unit_test_trigger(),
            state_tx,
            cancel,
            &mut monitors,
            &executor,
        )
        .await
        .expect("inputs that render successfully must NOT surface as Err");

        // RunStarted emitted with the correlator's resolved run_id.
        match state_rx.try_recv() {
            Ok(StateUpdate::RunStarted { run_id, flow, .. }) => {
                assert_eq!(run_id, 555);
                assert_eq!(flow, "unit-flow");
            }
            other => {
                panic!("Success path with rendered inputs must emit RunStarted; got: {other:?}")
            }
        }
        assert_eq!(monitors.len(), 1, "monitor task must be spawned on success");
        while monitors.join_next().await.is_some() {}
    }

    #[tokio::test]
    async fn handle_trigger_with_executor_dispatch_failed_rate_limited_propagates_retry_at() {
        // ExecuteOutcome::DispatchFailed(RateLimited) routes through
        // DispatchError::from_github_error("dispatch", ..) which
        // propagates `reset` into retry_at per the unit tests on
        // DispatchError above. handle_trigger does NOT itself read
        // retry_at — it returns the DispatchError as-is, so the
        // observed error message at the public surface MUST start
        // with "dispatch:" and contain the rate-limit prefix.
        use crate::config::CredentialId;
        let reset = chrono::Utc::now() + chrono::Duration::minutes(15);
        let executor = UnitScriptedExecutor::new(vec![ExecuteOutcome::DispatchFailed(
            GithubErrorKind::RateLimited {
                credential: CredentialId::new("github_pat").expect("valid id"),
                reset,
            },
        )]);
        let params = build_test_params(BTreeMap::new());
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        let cancel = CancellationToken::new();
        let mut monitors: JoinSet<()> = JoinSet::new();
        let err = handle_trigger_with_executor(
            params,
            unit_test_trigger(),
            state_tx,
            cancel,
            &mut monitors,
            &executor,
        )
        .await
        .expect_err("DispatchFailed(RateLimited) must surface as Err");
        assert!(
            err.starts_with("dispatch:"),
            "rate-limited error body must lead with the 'dispatch:' stage prefix; got: {err}",
        );
        assert!(
            state_rx.try_recv().is_err(),
            "rate-limited dispatch must NOT emit RunStarted",
        );
        assert!(
            monitors.is_empty(),
            "rate-limited dispatch must NOT spawn a monitor",
        );
    }

    #[tokio::test]
    async fn handle_trigger_with_executor_correlate_failed_duplicate_match_lists_run_ids() {
        // CorrelationError::DuplicateMatch is the permanent
        // correlation failure where the run-name search matched more
        // than one Run.id. The error message must list every matched
        // run id so an operator reading `gcit status` can identify
        // both runs and choose which to investigate.
        let executor = UnitScriptedExecutor::new(vec![ExecuteOutcome::CorrelateFailed(
            CorrelationError::DuplicateMatch {
                repo: "myorg/linux-builder".into(),
                workflow: "ci.yml".into(),
                gcit_run_id: Uuid::nil(),
                run_ids: vec![1001, 1002, 1003],
            },
        )]);
        let params = build_test_params(BTreeMap::new());
        let (state_tx, _state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        let cancel = CancellationToken::new();
        let mut monitors: JoinSet<()> = JoinSet::new();
        let err = handle_trigger_with_executor(
            params,
            unit_test_trigger(),
            state_tx,
            cancel,
            &mut monitors,
            &executor,
        )
        .await
        .expect_err("DuplicateMatch must surface as Err");
        assert!(
            err.starts_with("correlate:"),
            "duplicate-match error must lead with 'correlate:' prefix; got: {err}",
        );
        // Every run id in the duplicate-match must surface in the body.
        for id in [1001, 1002, 1003] {
            assert!(
                err.contains(&id.to_string()),
                "duplicate-match error must list run id {id}; got: {err}",
            );
        }
    }

    #[tokio::test]
    async fn handle_trigger_with_executor_success_fires_on_run_start_via_spawn_fan_out() {
        // The Success arm calls
        // `super::spawn_fan_out(&params.notifiers, "run-start", None, ..)`
        // which spawns one tokio::spawn task per notifier. Each task
        // awaits the closure `n.on_run_start(&ctx, &cancel)`. The
        // fan-out is fire-and-forget — `handle_trigger` does NOT wait
        // for the spawned tasks to complete before spawning the
        // monitor and returning Ok.
        //
        // Pre-cancel the test's token AFTER the fan-out had time to
        // run so the monitor exits cleanly without contacting the
        // unreachable test base_uri. Use a small real-time sleep
        // (single-digit ms) to give the spawned fan-out task a turn
        // before we assert on the recorded count.
        let recorder = Arc::new(UnitRecordingNotifier::new("notifier-1"));
        let mut params_inner = (*build_test_params(BTreeMap::new())).clone_for_test();
        params_inner.notifiers = vec![Arc::clone(&recorder) as Arc<dyn DynNotifier>];
        let params = Arc::new(params_inner);

        let executor = UnitScriptedExecutor::new(vec![ExecuteOutcome::Success {
            dispatch: DispatchOutcome {
                gcit_run_id: Uuid::new_v4(),
                dispatched_at: Utc::now(),
                repo: "myorg/linux-builder".to_string(),
                workflow: "ci.yml".to_string(),
                ref_name: "refs/heads/main".to_string(),
            },
            correlation: crate::github::correlator::CorrelationOutcome {
                run_id: 7777,
                summary: crate::github::monitor::empty_run_summary(7777),
            },
        }]);
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        let cancel = CancellationToken::new();
        let mut monitors: JoinSet<()> = JoinSet::new();
        handle_trigger_with_executor(
            params,
            unit_test_trigger(),
            state_tx,
            cancel.clone(),
            &mut monitors,
            &executor,
        )
        .await
        .expect("Success outcome must NOT surface as Err");

        // Sanity: RunStarted emitted with the correlated run_id.
        match state_rx.try_recv() {
            Ok(StateUpdate::RunStarted { run_id, .. }) => assert_eq!(run_id, 7777),
            other => panic!("Success path must emit RunStarted; got: {other:?}"),
        }

        // Wait briefly for the fire-and-forget fan-out task to run.
        // The recorder's increment runs inside a Mutex lock — single
        // microsecond; 100ms is generous so the test stays
        // deterministic on a loaded runner.
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Now cancel so the spawned monitor exits before we drain it.
        cancel.cancel();
        while monitors.join_next().await.is_some() {}

        // on_run_start fired exactly once (one notifier, one
        // successful trigger).
        let count = *recorder.on_run_start_calls.lock().await;
        assert_eq!(
            count, 1,
            "on_run_start must fire exactly once via spawn_fan_out for the single notifier; got {count}",
        );
    }

    // Helper extension trait: clone FlowDispatchParams for test parameter
    // rebuilding. FlowDispatchParams is not Clone (it carries Arcs but the
    // derive isn't there because some inner types lack Clone bounds).
    // Implemented locally as a copy-by-field for the dispatcher.rs unit
    // tests; not exposed as a public API.
    impl FlowDispatchParams {
        fn clone_for_test(&self) -> FlowDispatchParams {
            FlowDispatchParams {
                flow_name: self.flow_name.clone(),
                flow_description: self.flow_description.clone(),
                url: self.url.clone(),
                ref_name: self.ref_name.clone(),
                action: self.action.clone(),
                destinations: self.destinations.clone(),
                github_client: Arc::clone(&self.github_client),
                rate_bucket: Arc::clone(&self.rate_bucket),
                rate_limit: Arc::clone(&self.rate_limit),
                job_interval: self.job_interval,
                notifiers: self.notifiers.clone(),
            }
        }
    }
}

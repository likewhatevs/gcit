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
//
// Module layout (refactored from the prior single-file
// `src/flow/poll.rs`):
//   - `executor` — `PollCycleError`, `PollExecutor` trait,
//     `RealPollExecutor`, and the strategy-specific `poll_one`.
//   - `loop_`    — `run_with_executor` driver + per-arm helpers
//     (cooldown, jitter seed, unborn-ref message).
//   - `send`     — `SendOutcome` + `send_trigger_then_observation`
//     (the trigger-then-observation cancellation-aware sender).
//
// External callers (the supervisor, integration tests) name items
// at this module path (`gcit::flow::poll::*`); the submodules are
// private and items are re-exported below.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use octocrab::Octocrab;
use reqwest::Client as ReqwestClient;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::git::{default_interval, PollStrategy, RateBucket};
use crate::state::StateUpdate;

use super::TriggerSignal;

mod executor;
mod loop_;
mod send;

pub use executor::{PollCycleError, PollExecutor, RealPollExecutor};
pub use loop_::run_with_executor;

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
    /// Minimum elapsed wall time between two poll-originated dispatch
    /// acceptances. `Duration::ZERO` disables the throttle and every
    /// SHA-diff fires.
    pub cooldown: Duration,
}

impl EffectivePoll {
    /// Compute the effective poll cadences by overlaying a flow's
    /// `PollOverride` on the global `PollDefaults`. The strategy's
    /// default `source_interval` (per `git::default_interval`) is the
    /// bottom fallback when neither config layer pinned a value;
    /// `job_interval` always falls back to `PollDefaults.job_interval`
    /// because the monitor cadence is strategy-agnostic. `cooldown`
    /// also falls back to `PollDefaults.cooldown` because cooldown
    /// is strategy-agnostic — a flow's dispatch throttle is about
    /// downstream dispatch frequency, not source poll cadence.
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
        let cooldown = override_.cooldown.unwrap_or(defaults.cooldown);
        Self {
            source_interval,
            job_interval,
            jitter,
            cooldown,
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
    /// "errored" once it is healthy again. The supervisor is the
    /// only production construction site; integration tests reach
    /// past the marker to inject and inspect the map.
    #[doc(hidden)]
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
/// `send_trigger_then_observation` (helper in `send`) which carries
/// the full cancellation-race rationale. The observation only
/// persists once the trigger has been accepted by the dispatcher's
/// mpsc, so a cancel between the two leaves state.json with the OLD
/// SHA and the next-gen poll re-detects.
pub async fn run(
    params: PollParams,
    initial_last_sha: Option<gix_hash::ObjectId>,
    initial_last_dispatched_at: Option<DateTime<Utc>>,
    state_tx: Sender<StateUpdate>,
    trigger_tx: Sender<TriggerSignal>,
    cancel: CancellationToken,
) {
    let executor = RealPollExecutor::for_url(&params.url);
    run_with_executor(
        params,
        executor,
        initial_last_sha,
        initial_last_dispatched_at,
        state_tx,
        trigger_tx,
        cancel,
    )
    .await;
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
    fn effective_poll_cooldown_falls_back_to_defaults_when_override_unset() {
        // PollOverride::default() leaves cooldown=None; the resolved
        // value must come from PollDefaults (which defaults to 5min
        // per `default_cooldown` in config/parse.rs). Pin so a
        // regression that dropped the cooldown fallback wiring
        // surfaces here.
        let defaults = crate::config::PollDefaults::default();
        let override_ = crate::config::PollOverride::default();
        let p = EffectivePoll::compute(&defaults, &override_, PollStrategy::GithubApi);
        assert_eq!(p.cooldown, defaults.cooldown);
    }

    #[test]
    fn effective_poll_cooldown_override_wins_over_defaults() {
        let defaults = crate::config::PollDefaults {
            cooldown: Duration::from_secs(600),
            ..Default::default()
        };
        let override_ = crate::config::PollOverride {
            cooldown: Some(Duration::from_secs(45)),
            ..Default::default()
        };
        let p = EffectivePoll::compute(&defaults, &override_, PollStrategy::LsRemote);
        assert_eq!(p.cooldown, Duration::from_secs(45));
    }
}

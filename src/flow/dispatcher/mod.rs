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
//
// Module layout:
//   - `mod.rs`    — public types (`FlowDispatchParams`, `DynNotifier`)
//                   and the `run()` entry function.
//   - `executor`  — `DispatchExecutor` trait + `RealDispatchExecutor`
//                   + `ExecuteOutcome` + `DispatchError`.
//   - `handle`    — `handle_trigger` per-trigger pipeline + test
//                   seams.
//   - `loop_`     — `run_with_executor` driver + cancel-drain.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::sync::CancellationToken;

use crate::config::{ActionConfig, Destination};
use crate::git::rate_bucket::RateBucket;
use crate::github::client::Client as GithubClient;
use crate::github::rate_limit::RateLimitState;
use crate::notify::RunContext;
use crate::state::StateUpdate;

use super::supervisor::FlowLastError;
use super::TriggerSignal;

mod executor;
mod handle;
mod loop_;

pub use executor::{DispatchExecutor, ExecuteOutcome, RealDispatchExecutor};
pub use handle::{handle_trigger_for_test, handle_trigger_with_executor};
pub use loop_::run_with_executor;

/// Maximum dispatch attempts (passed to backon's `with_max_times`).
/// Three matches the project's other retry policies — enough to ride
/// out a transient blip without masking a hard failure.
pub const DISPATCH_MAX_ATTEMPTS: u32 = 3;

/// Per-flow dispatch parameters. Cheap to clone — every reference is
/// `Arc`-wrapped so the supervisor can hand identical handles to
/// multiple flows that share a credential.
///
/// `url` / `ref_name` are the source-side fields; the bare names
/// match `SourceConfig`/`SourceInfo`/`PollParams` which already use
/// the unprefixed form inside source-scoped scopes.
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
    /// Per-credential rate-limit snapshot, refreshed in the
    /// background by `github::rate_limit::poll_loop`.
    pub rate_limit: Arc<RateLimitState>,
    /// Effective `job_interval` for the per-run monitor.
    pub job_interval: Duration,
    /// Pre-built notifier handles for the destinations on this flow.
    pub notifiers: Vec<Arc<dyn DynNotifier>>,
}

/// Dyn-compatible wrapper over the per-kind `Notifier` impls.
///
/// `crate::notify::Notifier` uses native async-fn-in-trait which is
/// not dyn-safe; this trait restates the public surface using boxed
/// futures so the supervisor can hold a heterogeneous
/// `Vec<Arc<dyn DynNotifier>>`.
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

/// Drive the per-flow dispatch lifecycle: receive `TriggerSignal`s
/// from the poll task and turn each into a (dispatch, correlate,
/// monitor) chain.
///
/// `state_tx` carries the `RunStarted` and `RunFinished` updates to
/// the state writer's mpsc.
///
/// Cancellation: the supervisor's child token signals shutdown (root
/// cancel) or per-flow removal. In-flight monitor tasks are owned by
/// an internal `JoinSet` and drain mid-run rather than blocking
/// shutdown.
pub async fn run(
    params: FlowDispatchParams,
    trigger_rx: Receiver<TriggerSignal>,
    state_tx: Sender<StateUpdate>,
    last_errors: Arc<tokio::sync::Mutex<std::collections::BTreeMap<String, FlowLastError>>>,
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

/// Compile-time check: concrete notifier types must satisfy the
/// `DynNotifier` blanket impl via their `Notifier` impl.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dyn_notifier_blanket_covers_concrete_kinds() {
        fn assert_dyn<T: DynNotifier>() {}
        assert_dyn::<crate::discord::DiscordNotifier>();
        assert_dyn::<crate::mail::LocalMailNotifier>();
    }
}

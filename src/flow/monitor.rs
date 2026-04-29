// Per-run monitor lifecycle.
//
// Wraps `crate::github::monitor::monitor_run` in a task that:
//   - drains MonitorEvent::{Update, Done} from the underlying
//     monitor's mpsc on a `job_interval` cadence.
//   - on `Done`, fans the summary out to every flow notifier in
//     parallel (each notifier's failure is isolated; one Discord 401
//     does not block the local_mail destination).
//   - emits `StateUpdate::RunFinished` when the run reaches terminal
//     status.
//
// The `MonitorEventSource` trait is the test seam: production
// callers pass `RealEventSource` which wraps `monitor_run` against a
// real `Client`, while integration tests under tests/ pass a
// scripted impl that pushes pre-baked `MonitorEvent`s through the
// loop without standing up wiremock + an octocrab client. Mirrors
// the `PollExecutor` pattern in `flow::poll`.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn};

use crate::github::client::Client as GithubClient;
use crate::github::monitor::{
    self as gh_monitor, MonitorEvent, MonitorOutcome, MonitorParams as GhMonitorParams,
};
use crate::github::rate_limit::RateLimitState;
use crate::notify::RunContext;
use crate::state::StateUpdate;

use super::dispatcher::DynNotifier;

/// Test seam for the per-run event stream. The production
/// implementation (`RealEventSource`) wraps
/// `gh_monitor::monitor_run`; tests inject a scripted source that
/// pushes pre-baked `MonitorEvent` values through the loop and
/// returns whichever `MonitorOutcome` the test scenario requires.
///
/// `drive` is the only method: it owns the lifetime of the event
/// stream and resolves with the final outcome. `event_tx` is the
/// channel `run_monitor` reads `MonitorEvent`s from; `cancel` is the
/// per-flow token (drain / SIGTERM / per-flow reload).
pub trait MonitorEventSource: Send + 'static {
    fn drive<'a>(
        self: Box<Self>,
        event_tx: mpsc::Sender<MonitorEvent>,
        cancel: CancellationToken,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = MonitorOutcome> + Send + 'a>>
    where
        Self: 'a;
}

/// Production `MonitorEventSource` — wraps `monitor_run` against a
/// real `GithubClient`. Constructed by the supervisor when wiring a
/// real flow; tests construct `ScriptedEventSource` instead.
pub struct RealEventSource {
    /// Per-credential octocrab client. Same `Arc<GithubClient>` the
    /// dispatcher passed to `dispatch_with_retry` — sharing the
    /// handle keeps the rate-limit snapshot warm across the dispatch
    /// → monitor handoff.
    pub github_client: Arc<GithubClient>,
    /// Per-credential rate-limit snapshot. The github-layer monitor
    /// observes `X-RateLimit-*` headers on each get_run / list_jobs
    /// response and updates this snapshot in place; subsequent
    /// dispatches on the same credential see the refreshed quota.
    pub rate_limit: Arc<RateLimitState>,
    /// Github-layer monitor parameters: `(repo, workflow, run_id,
    /// job_interval)` — the values `monitor_run` itself reads, with
    /// no flow-name / notifier coupling. The flow-layer
    /// `MonitorParams` carries the additional fields and the
    /// production wrapper builds this struct from the flow params.
    pub gh_params: GhMonitorParams,
}

impl MonitorEventSource for RealEventSource {
    fn drive<'a>(
        self: Box<Self>,
        event_tx: mpsc::Sender<MonitorEvent>,
        cancel: CancellationToken,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = MonitorOutcome> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            gh_monitor::monitor_run(
                self.github_client,
                self.rate_limit,
                self.gh_params,
                event_tx,
                cancel,
            )
            .await
        })
    }
}

/// Per-run monitor parameters. Owned by the dispatcher and passed
/// into `spawn_monitor`. Named `MonitorParams` for consistency with
/// `flow::poll::PollParams` and `flow::dispatcher::FlowDispatchParams`.
/// The github-layer monitor's parameter struct is also called
/// `MonitorParams`; it is imported here as `GhMonitorParams` to keep
/// the per-layer naming convention intact.
pub struct MonitorParams {
    pub flow_name: String,
    pub repo: String,
    pub workflow: String,
    pub run_id: u64,
    pub job_interval: Duration,
    pub github_client: Arc<GithubClient>,
    pub rate_limit: Arc<RateLimitState>,
    pub notifiers: Vec<Arc<dyn DynNotifier>>,
    pub run_context: RunContext,
}

/// Spawn a per-run monitor task into the dispatcher's `JoinSet`. The
/// task drives `monitor_run` and on terminal status fans out to every
/// notifier in parallel.
///
/// `cancel` is the dispatcher's child token. When the supervisor
/// cancels (root or per-flow), the monitor's underlying loop returns
/// `MonitorOutcome::DrainedMidRun` and the task exits.
pub fn spawn_monitor(
    join_set: &mut JoinSet<()>,
    params: MonitorParams,
    state_tx: mpsc::Sender<StateUpdate>,
    cancel: CancellationToken,
) {
    join_set.spawn(async move { run_monitor(params, state_tx, cancel).await });
}

#[instrument(level = "debug", skip(params, state_tx, cancel), fields(
    flow = %params.flow_name,
    repo = %params.repo,
    workflow = %params.workflow,
    run_id = params.run_id,
))]
async fn run_monitor(
    params: MonitorParams,
    state_tx: mpsc::Sender<StateUpdate>,
    cancel: CancellationToken,
) {
    let gh_params = GhMonitorParams {
        repo: params.repo.clone(),
        workflow: params.workflow.clone(),
        run_id: params.run_id,
        job_interval: params.job_interval,
    };
    let source: Box<dyn MonitorEventSource> = Box::new(RealEventSource {
        github_client: Arc::clone(&params.github_client),
        rate_limit: Arc::clone(&params.rate_limit),
        gh_params,
    });
    run_monitor_with_source(params, source, state_tx, cancel).await
}

/// Test seam for the per-run monitor lifecycle. Drives the
/// notifier fan-out + RunFinished emission against a scripted
/// `MonitorEventSource` instead of `gh_monitor::monitor_run`.
/// Production callers go through `spawn_monitor`; integration tests
/// under `tests/` hand-roll a scripted source and call this
/// directly.
///
/// `#[doc(hidden)] pub` mirrors the existing test-seam pattern in
/// `flow::dispatcher::handle_trigger_for_test` and the seams under
/// `mail::Persist` / `flow::poll::run_with_executor`.
#[doc(hidden)]
pub async fn run_monitor_with_source(
    params: MonitorParams,
    source: Box<dyn MonitorEventSource>,
    state_tx: mpsc::Sender<StateUpdate>,
    cancel: CancellationToken,
) {
    let (event_tx, mut event_rx) = mpsc::channel::<MonitorEvent>(8);
    let source_cancel = cancel.clone();
    let monitor_handle = tokio::spawn(async move { source.drive(event_tx, source_cancel).await });

    // Track which job_ids have already fired their `on_job_complete`
    // notification this run so we don't double-fire when subsequent
    // Update cycles re-observe the same terminal status. Notifiers
    // receive on_job_complete once per (run, job) at the moment the
    // job's conclusion first transitions from None to Some(_).
    let mut completed_jobs: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
    let mut latest_summary: Option<crate::github::RunSummary> = None;
    while let Some(event) = event_rx.recv().await {
        match event {
            MonitorEvent::Update { summary } => {
                debug!(
                    target: "gcit::flow::monitor",
                    flow = %params.flow_name,
                    run_id = params.run_id,
                    status = ?summary.status,
                    "monitor update",
                );
                fan_out_job_completes(
                    &params.notifiers,
                    &params.run_context,
                    &summary,
                    &mut completed_jobs,
                    &cancel,
                )
                .await;
                latest_summary = Some(summary);
            }
            MonitorEvent::Done { summary } => {
                info!(
                    target: "gcit::flow::monitor",
                    flow = %params.flow_name,
                    run_id = params.run_id,
                    conclusion = ?summary.conclusion,
                    "run terminal; fanning out notifiers",
                );
                // Final per-job sweep — a job that completed in the
                // same cycle as the run's terminal status would
                // otherwise be missed if the github::monitor emitted
                // Done without a preceding Update for that job.
                fan_out_job_completes(
                    &params.notifiers,
                    &params.run_context,
                    &summary,
                    &mut completed_jobs,
                    &cancel,
                )
                .await;
                {
                    let ctx = params.run_context.clone();
                    let s = summary.clone();
                    let cancel_for_fanout = cancel.clone();
                    let handles =
                        super::spawn_fan_out(&params.notifiers, "run-complete", None, move |n| {
                            let c = ctx.clone();
                            let s = s.clone();
                            let cancel = cancel_for_fanout.clone();
                            async move { n.on_run_complete(&c, &s, &cancel).await }
                        });
                    for h in handles {
                        let _ = h.await;
                    }
                }
                // Persistence requires snake_case (round-trips with
                // Conclusion::from_api when state.json is reloaded).
                // label_for would emit prose ("timed out", "action
                // required") and break the round-trip.
                let conclusion_str = summary
                    .conclusion
                    .map(crate::github::Conclusion::to_api)
                    .unwrap_or("unknown")
                    .to_string();
                if state_tx
                    .send(StateUpdate::RunFinished {
                        flow: params.flow_name.clone(),
                        run_id: params.run_id,
                        conclusion: conclusion_str,
                        completed_at: summary.completed_at.unwrap_or_else(Utc::now),
                    })
                    .await
                    .is_err()
                {
                    warn!(
                        target: "gcit::flow::monitor",
                        flow = %params.flow_name,
                        "state writer dropped before RunFinished could be queued",
                    );
                }
                latest_summary = Some(summary);
                break;
            }
        }
    }

    // Wait for the monitor's underlying task to finish so we know the
    // outcome. JoinError on the inner task is not a panic in normal
    // shutdown — `monitor_run` returns by value.
    match monitor_handle.await {
        Ok(MonitorOutcome::Terminated) => {
            // Already handled via the Done event above.
        }
        Ok(MonitorOutcome::ReceiverDropped) => {
            debug!(
                target: "gcit::flow::monitor",
                flow = %params.flow_name,
                "monitor receiver dropped (supervisor teardown)",
            );
        }
        Ok(MonitorOutcome::DrainedMidRun) => {
            warn!(
                target: "gcit::flow::monitor",
                flow = %params.flow_name,
                run_id = params.run_id,
                "monitor drained mid-run; run may still be in flight on GitHub",
            );
        }
        Ok(MonitorOutcome::Failed { error }) => {
            warn!(
                target: "gcit::flow::monitor",
                flow = %params.flow_name,
                run_id = params.run_id,
                error = %error,
                "monitor failed permanently",
            );
        }
        Err(e) if e.is_panic() => {
            warn!(
                target: "gcit::flow::monitor",
                flow = %params.flow_name,
                run_id = params.run_id,
                "monitor task PANICKED",
            );
        }
        Err(e) => {
            debug!(
                target: "gcit::flow::monitor",
                flow = %params.flow_name,
                run_id = params.run_id,
                error = %e,
                "monitor task join error",
            );
        }
    }
    let _ = latest_summary;
}

/// Fan out per-job notifications for jobs that JUST transitioned to a
/// terminal status. `completed_jobs` carries the set of job_ids
/// already notified in this run; jobs whose `conclusion` is `Some(_)`
/// AND whose id is not yet in the set are notified once and added.
/// Monitor calls on_job_complete for each completed job, exactly once
/// per (run, job) pair.
///
/// Spawns the (notifier × newly_terminal) cross-product via
/// `super::spawn_fan_out` (one call per job; tasks within a call run
/// in parallel, calls are issued back-to-back so cross-job tasks also
/// overlap). One notifier × job task's failure does not affect the
/// others — see `super::spawn_fan_out` doc for the per-task isolation.
async fn fan_out_job_completes(
    notifiers: &[Arc<dyn DynNotifier>],
    ctx: &RunContext,
    summary: &crate::github::RunSummary,
    completed_jobs: &mut std::collections::BTreeSet<u64>,
    cancel: &CancellationToken,
) {
    // Identify the jobs that newly reached a terminal status this
    // cycle. github::JobResult carries `Option<Conclusion>`; a value of
    // `Some(_)` means GitHub reported a terminal job state.
    let newly_terminal: Vec<crate::github::JobResult> = summary
        .jobs
        .iter()
        .filter(|j| j.conclusion.is_some() && !completed_jobs.contains(&j.job_id))
        .cloned()
        .collect();
    if newly_terminal.is_empty() {
        return;
    }
    for j in &newly_terminal {
        completed_jobs.insert(j.job_id);
    }
    let mut all_handles = Vec::with_capacity(notifiers.len() * newly_terminal.len());
    for job in newly_terminal {
        let ctx = ctx.clone();
        let job_id = job.job_id;
        let cancel_for_fanout = cancel.clone();
        let handles = super::spawn_fan_out(notifiers, "job-complete", Some(job_id), move |n| {
            let c = ctx.clone();
            let j = job.clone();
            let cancel = cancel_for_fanout.clone();
            async move { n.on_job_complete(&c, &j, &cancel).await }
        });
        all_handles.extend(handles);
    }
    for h in all_handles {
        let _ = h.await;
    }
}

// Per-run monitor: poll get_run + list_jobs on `job_interval`
// until the run reaches a terminal status (Completed). Emit
// RunSummary updates on every poll cycle and one final
// RunFinished snapshot when the terminal cycle lands.
//
// Spec:
//   - monitor: job_interval, get_run+list_jobs -> on terminal status,
//     fire notifier -> RunFinished.
//   - `job_interval = "30s"` default per PollDefaults.
//   - tests/github_list_jobs.rs — list_jobs uses Filter::Latest,
//     pagination support, run_status mapping.
//   - tests/github_drain_timeout.rs — drain shortens deadlines.
//
// One monitor task spawned per dispatched run. Lives until the
// run terminates OR the supervisor cancels it (drain or flow
// removal).

use std::sync::Arc;
use std::time::Duration;

use octocrab::models::workflows::Job;
use octocrab::Page;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, warn};

use super::client::Client;
use super::correlator::{job_to_result, run_to_summary};
use super::error::GithubErrorKind;
use super::rate_limit::RateLimitState;
use super::{JobResult, RunStatus, RunSummary};

/// Maximum jobs the monitor will collect per run before bailing
/// with a Permanent error. A workflow with >1000 jobs is a
/// runaway matrix; gcit refuses to keep iterating.
pub const MAX_JOBS_PER_RUN: usize = 1000;

/// Cap pagination depth on list_jobs so a runaway response
/// doesn't DoS the monitor. 10 pages × 100 per_page = 1000 jobs;
/// matches MAX_JOBS_PER_RUN.
pub const MAX_JOBS_PAGES: u32 = 10;

/// Per-run monitor parameters. Owned by the supervisor.
#[derive(Debug, Clone)]
pub struct MonitorParams {
    pub repo: String,
    pub workflow: String,
    pub run_id: u64,
    /// How often to poll. Defaults to 30s but the supervisor
    /// passes the per-flow effective `job_interval` (PollOverride
    /// + PollDefaults).
    pub job_interval: Duration,
}

/// Streaming output from the monitor task. The supervisor sinks
/// these to:
///   - the state writer (RunSummary on every Update; final on Done)
///   - the notifier dispatch (only on Done, with the full summary)
#[derive(Debug, Clone)]
pub enum MonitorEvent {
    /// In-progress poll observation. Never carries a terminal
    /// conclusion; status is one of Queued / InProgress / Waiting /
    /// Other.
    Update { summary: RunSummary },
    /// Terminal observation. status == Completed; conclusion is
    /// the final outcome.
    Done { summary: RunSummary },
}

/// Outcome of `monitor_run`. The supervisor uses these to decide
/// whether to mark the run as cancelled (drain mid-monitor),
/// errored (last_error needed), or completed.
#[derive(Debug)]
pub enum MonitorOutcome {
    /// Run reached terminal state. The Done event was emitted
    /// before this returns. Distinct from `ReceiverDropped` —
    /// the run actually finished.
    Terminated,
    /// The supervisor disconnected from this monitor's event
    /// channel (its receiver was dropped). The run may still be
    /// in flight on GitHub; gcit just stopped tracking. Distinct
    /// from `Terminated` so the supervisor can avoid recording a
    /// false "completed" status when it's actually a teardown.
    ReceiverDropped,
    /// Drain fired and the run is still in flight. Supervisor
    /// records "monitor stopped during drain" rather than
    /// mid-run state.
    DrainedMidRun,
    /// API errors prevented further polling. The supervisor
    /// surfaces the carried error as last_error.
    Failed { error: GithubErrorKind },
}

/// Long-running task: poll the run on `job_interval` until it
/// terminates or `cancel` fires.
///
/// `tx` carries the streaming events; the supervisor's receive
/// side decides whether to forward to state-writer + notifier.
/// When `tx` is closed (the supervisor dropped its receiver), the
/// monitor exits silently.
#[instrument(level = "debug", skip(client, rate_limit, tx, cancel), fields(
    repo = %params.repo,
    workflow = %params.workflow,
    run_id = params.run_id,
))]
pub async fn monitor_run(
    client: Arc<Client>,
    rate_limit: Arc<RateLimitState>,
    params: MonitorParams,
    tx: mpsc::Sender<MonitorEvent>,
    cancel: CancellationToken,
) -> MonitorOutcome {
    let mut interval = tokio::time::interval(params.job_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Tick once immediately so the first poll fires at t=0 — gives
    // the supervisor a fresh snapshot without waiting a full cycle.
    interval.tick().await;

    loop {
        // Quota gate (opportunistic header observations refresh
        // the snapshot inline below).
        if let Some(wait) = rate_limit.should_defer().await {
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = cancel.cancelled() => return MonitorOutcome::DrainedMidRun,
            }
        }

        // Fetch the run + its jobs in one go.
        match poll_one(&client, &rate_limit, &params).await {
            Ok(summary) => {
                let is_terminal = summary.status.is_terminal();
                // Move (not clone) the summary into the event —
                // the loop reads `is_terminal` from the bool above
                // before the move so no borrow remains.
                let event = if is_terminal {
                    MonitorEvent::Done { summary }
                } else {
                    MonitorEvent::Update { summary }
                };
                if tx.send(event).await.is_err() {
                    debug!("monitor receiver dropped; exiting");
                    return MonitorOutcome::ReceiverDropped;
                }
                if is_terminal {
                    return MonitorOutcome::Terminated;
                }
            }
            Err(e) => {
                // Permanent errors (Unauthorized, etc.) abort
                // monitoring. Transient errors retry on cadence.
                if !e.is_transient() {
                    warn!(error = %e, "permanent error during run monitoring");
                    return MonitorOutcome::Failed { error: e };
                }
                warn!(error = %e, "transient error during run monitoring");
            }
        }

        // Wait for the next tick OR drain. tokio::Interval skips
        // ticks if we've fallen behind; that's the missed-tick
        // delay behaviour above.
        tokio::select! {
            _ = interval.tick() => {}
            _ = cancel.cancelled() => return MonitorOutcome::DrainedMidRun,
        }
    }
}

/// Issue one (get_run, list_jobs) pair and stitch them into a
/// RunSummary. Errors short-circuit on the first failure.
async fn poll_one(
    client: &Client,
    rate_limit: &RateLimitState,
    params: &MonitorParams,
) -> Result<RunSummary, GithubErrorKind> {
    let (owner, repo) = super::dispatcher::split_repo(&params.repo)?;

    // get_run via low-level _get so we can observe rate-limit
    // headers. FromResponse<Run> = serde-default for any
    // DeserializeOwned per octocrab/from_response.rs.
    let uri = uri_or_unknown(&format!(
        "/repos/{owner}/{repo}/actions/runs/{run_id}",
        owner = owner,
        repo = repo,
        run_id = params.run_id,
    ))?;
    let run: octocrab::models::workflows::Run =
        super::client::classified_get(client, rate_limit, &params.repo, &params.workflow, uri)
            .await?;

    // list_jobs: fetch all jobs (latest attempt only; per
    // tests/github_list_jobs.rs::list_jobs_uses_latest_filter_by_default).
    let jobs = list_all_jobs(client, rate_limit, params).await?;

    let mut summary = run_to_summary(&run);
    // run_to_summary defaults run_attempt=1; if any of the
    // returned jobs carry a higher attempt number, propagate it
    // — re-run scenarios bump the attempt on every job.
    summary.run_attempt = jobs
        .iter()
        .map(|j| j.run_attempt)
        .max()
        .unwrap_or(summary.run_attempt);
    summary.jobs = jobs;
    Ok(summary)
}

/// Walk pages of list_jobs and convert each Job + Step. Aborts
/// once MAX_JOBS_PER_RUN is exceeded so a runaway workflow
/// doesn't OOM the monitor.
async fn list_all_jobs(
    client: &Client,
    rate_limit: &RateLimitState,
    params: &MonitorParams,
) -> Result<Vec<JobResult>, GithubErrorKind> {
    let (owner, repo) = super::dispatcher::split_repo(&params.repo)?;
    let mut out: Vec<JobResult> = Vec::new();

    // First page via low-level _get so observe_headers fires per
    // response. `filter=latest` is hardcoded per
    // tests/github_list_jobs.rs::list_jobs_uses_latest_filter_by_default
    // (the monitor only cares about the most recent run attempt).
    let uri = uri_or_unknown(&format!(
        "/repos/{owner}/{repo}/actions/runs/{run_id}/jobs?filter=latest&per_page=100",
        owner = owner,
        repo = repo,
        run_id = params.run_id,
    ))?;
    let first: Page<Job> =
        super::client::classified_get(client, rate_limit, &params.repo, &params.workflow, uri)
            .await?;

    let mut current = first;
    let mut pages: u32 = 0;
    loop {
        pages += 1;
        for j in &current.items {
            out.push(job_to_result(j));
            if out.len() >= MAX_JOBS_PER_RUN {
                warn!(
                    job_count = out.len(),
                    cap = MAX_JOBS_PER_RUN,
                    "list_jobs exceeded MAX_JOBS_PER_RUN; truncating",
                );
                return Ok(out);
            }
        }
        if pages >= MAX_JOBS_PAGES {
            warn!(
                pages,
                cap = MAX_JOBS_PAGES,
                "list_jobs exceeded MAX_JOBS_PAGES; truncating",
            );
            return Ok(out);
        }
        match current.next.clone() {
            Some(uri) => match next_jobs_page(client, rate_limit, uri).await? {
                Some(p) => current = p,
                None => break,
            },
            None => break,
        }
    }
    Ok(out)
}

async fn next_jobs_page(
    client: &Client,
    rate_limit: &RateLimitState,
    uri: http::Uri,
) -> Result<Option<Page<Job>>, GithubErrorKind> {
    let page: Page<Job> = super::client::classified_get(client, rate_limit, "", "", uri).await?;
    Ok(Some(page))
}

/// Parse a path string into an `http::Uri`. Failure becomes
/// `Unknown` since this only fires on programmatic format-string
/// bugs, not on user input.
fn uri_or_unknown(path: &str) -> Result<http::Uri, GithubErrorKind> {
    path.parse()
        .map_err(|e: http::uri::InvalidUri| GithubErrorKind::Unknown {
            source: anyhow::anyhow!("monitor URI invalid: {e}"),
        })
}

/// Default `job_interval` of 30s. Pinned constant so
/// tests/github_list_jobs.rs::monitor_polls_at_job_interval can
/// assert it.
pub const DEFAULT_JOB_INTERVAL: Duration = Duration::from_secs(30);

/// Helper: should the supervisor stop the monitor when it sees
/// this status? Mirrors RunStatus::is_terminal but is the canonical
/// "stop polling" gate from the monitor's perspective; the only
/// terminal status is Completed.
pub fn is_terminal(status: RunStatus) -> bool {
    status.is_terminal()
}

/// Build the JobResult vector empty (used by tests + state-writer
/// snapshots when no jobs have been observed yet).
pub fn empty_jobs() -> Vec<JobResult> {
    Vec::new()
}

/// Build the StepResult equivalent — present for symmetry.
pub fn empty_run_summary(run_id: u64) -> RunSummary {
    RunSummary {
        run_id,
        run_url: String::new(),
        run_number: 0,
        run_attempt: 1,
        status: RunStatus::Queued,
        conclusion: None,
        started_at: None,
        completed_at: None,
        jobs: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::Conclusion;
    use super::*;

    #[test]
    fn default_job_interval_is_30s() {
        assert_eq!(DEFAULT_JOB_INTERVAL, Duration::from_secs(30));
    }

    #[test]
    fn is_terminal_only_when_completed() {
        assert!(is_terminal(RunStatus::Completed));
        for s in [
            RunStatus::Queued,
            RunStatus::InProgress,
            RunStatus::Waiting,
            RunStatus::Unknown,
        ] {
            assert!(!is_terminal(s), "{s:?} must not be terminal");
        }
    }

    #[test]
    fn max_jobs_constants_pinned() {
        assert_eq!(MAX_JOBS_PER_RUN, 1000);
        assert_eq!(MAX_JOBS_PAGES, 10);
    }

    #[test]
    fn empty_run_summary_carries_run_id() {
        let s = empty_run_summary(99);
        assert_eq!(s.run_id, 99);
        assert_eq!(s.status, RunStatus::Queued);
        assert_eq!(s.conclusion, None);
        assert_eq!(s.run_attempt, 1);
        assert!(s.jobs.is_empty());
    }

    #[test]
    fn empty_jobs_returns_empty_vec() {
        let v = empty_jobs();
        assert!(v.is_empty());
    }

    #[test]
    fn monitor_event_update_carries_summary() {
        let s = empty_run_summary(1);
        let e = MonitorEvent::Update { summary: s.clone() };
        match e {
            MonitorEvent::Update { summary } => assert_eq!(summary.run_id, 1),
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn uri_or_unknown_parses_valid_relative_path() {
        // `path` arguments to uri_or_unknown are programmatic format
        // strings (e.g. `format!("/repos/{owner}/{repo}/actions/runs/{run_id}/jobs?page={n}")`).
        // The happy path: a well-formed relative URI parses cleanly.
        let uri =
            uri_or_unknown("/repos/myorg/linux-builder/actions/runs/101/jobs").expect("must parse");
        assert_eq!(
            uri.path(),
            "/repos/myorg/linux-builder/actions/runs/101/jobs",
        );
    }

    #[test]
    fn uri_or_unknown_classifies_invalid_uri_as_unknown() {
        // The only path that can fire the error arm is a programmatic
        // format-string bug (e.g. an unencoded space or a control
        // char). Surface as `GithubErrorKind::Unknown` so the
        // operator-facing message names the malformed string rather
        // than an opaque "internal error".
        //
        // http::Uri::from_str rejects spaces (RFC 3986 disallows
        // unencoded SPACE in any URI segment) — pin that this surface
        // returns Unknown rather than panicking.
        let err = uri_or_unknown("/path with space").expect_err("must error");
        match err {
            GithubErrorKind::Unknown { source } => {
                assert!(
                    format!("{source:?}").contains("monitor URI invalid"),
                    "Unknown source must name the failure mode",
                );
            }
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    #[test]
    fn monitor_event_done_carries_summary() {
        let mut s = empty_run_summary(2);
        s.status = RunStatus::Completed;
        s.conclusion = Some(Conclusion::Success);
        let e = MonitorEvent::Done { summary: s.clone() };
        match e {
            MonitorEvent::Done { summary } => {
                assert_eq!(summary.run_id, 2);
                assert!(summary.status.is_terminal());
                assert_eq!(summary.conclusion, Some(Conclusion::Success));
            }
            _ => panic!("expected Done"),
        }
    }
}

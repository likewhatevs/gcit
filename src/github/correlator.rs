// Workflow run correlation: from dispatch (gcit_run_id) -> Run.id.
//
// Algorithm:
//
//   Each poll cycle starts from page 1 (preserving the "match
//   appears between polls" semantics — restart from page 1 each
//   cycle so a run that lands mid-pagination is not missed).
//
//   Each page: walk `Vec<Run>` looking for any run whose
//   `name.contains("gcit-<uuid>")`. Two outcomes:
//     * 0 matches: continue to next page (Page::next_uri); when no
//       more pages, sleep on backoff and retry.
//     * 1 match: return Ok(matched.id).
//     * 2+ matches: abort with DuplicateMatch (collect all ids).
//
//   Fallback (when run-name was not configured by the workflow):
//     The first poll cycle sees no name match. After the first full
//     pagination scan exhausts without a match, the correlator
//     re-runs the scan with `?head_sha=<sha>` and selects the most
//     recent run whose `created_at >= dispatched_at`. Selecting
//     "most recent" without the run-name marker is best-effort and
//     surfaces a WARN log entry recommending `run-name`
//     configuration.
//
// Cancellation: a CancellationToken passed in from the supervisor
// signals "drain mode" — when the token fires, the deadline
// shortens to DRAIN_TIMEOUT (or the remaining normal budget,
// whichever is less).

use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBuilder};
use chrono::{DateTime, Utc};
use octocrab::models::workflows::Run;
use octocrab::Page;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, warn};
use uuid::Uuid;

use super::client::Client;
use super::error::GithubErrorKind;
use super::Conclusion;
use super::JobResult;
use super::RunStatus;
use super::RunSummary;
use super::StepResult;

/// Normal-mode hard deadline for correlation: 5 minutes of run
/// appearance during normal operation.
pub const NORMAL_TIMEOUT: Duration = Duration::from_secs(300);

/// Drain-mode deadline. SIGTERM in flight — bound the wait at 30s
/// so shutdown is predictable.
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Backoff schedule. The correlator polls list_workflow_runs on a
/// polite cadence — 5s, then exponential up to a cap, with the
/// cap chosen so a 5-minute deadline still produces multiple
/// attempts. We use a tight cap (60s) because the correlator's
/// deadline is only 5min and we want at least 5 attempts before
/// giving up.
pub const POLL_INTERVAL_INITIAL: Duration = Duration::from_secs(5);
pub const POLL_INTERVAL_MAX: Duration = Duration::from_secs(60);
pub const POLL_INTERVAL_FACTOR: f64 = 2.0;

/// Cap pagination depth so a runaway workflow that emits thousands
/// of runs in a 5-minute window doesn't DoS the correlator's
/// pagination loop. 5 pages = 500 runs at default per_page=100
/// before giving up and returning a Transient error.
pub const MAX_PAGES_PER_POLL: u32 = 5;

/// Clock-skew tolerance for the fallback path's
/// `created_at >= dispatched_at` filter. NTP corrections can step the
/// system clock by tens of seconds; without a buffer, a legitimate
/// run created seconds before `dispatched_at` (per the local clock)
/// is silently discarded. 2 minutes covers normal NTP step scenarios
/// without widening the fallback window so much that pre-existing
/// stale runs get matched.
pub const CLOCK_SKEW_BUFFER: Duration = Duration::from_secs(120);

/// Per-correlation parameters. Owned by the supervisor, derived
/// from FlowConfig + DispatchOutcome at correlation time.
#[derive(Debug, Clone)]
pub struct CorrelateParams {
    pub repo: String,
    pub workflow: String,
    pub gcit_run_id: Uuid,
    /// The `branch` query parameter (e.g. "main"). Derived from
    /// the dispatched ref (`refs/heads/main` -> "main"). Empty
    /// string when the ref isn't a branch (the API filters by
    /// branch name only).
    pub branch: String,
    /// SHA the dispatch targeted. Used by the fallback path
    /// (`?head_sha=<sha>` + most-recent-by-created_at).
    pub head_sha: String,
    /// Wall-clock at the moment of dispatch. The fallback path
    /// excludes runs created before this instant.
    pub dispatched_at: DateTime<Utc>,
    /// Whether the workflow declares `run-name: gcit-${{ ... }}`.
    /// `true` -> use name-substring matching.
    /// `false` -> skip name check, go straight to fallback.
    /// gcit can't introspect the workflow YAML, but we record
    /// "run-name configured" in state once a name match has been
    /// seen at least once. For first-ever dispatches we try
    /// name-match first and degrade to fallback.
    pub run_name_configured: Option<bool>,
}

/// Successful correlation outcome — the GitHub `Run.id` plus the
/// fully-populated `RunSummary` snapshot. The supervisor hands
/// this to the monitor and to the state writer.
#[derive(Debug, Clone)]
pub struct CorrelationOutcome {
    pub run_id: u64,
    pub summary: RunSummary,
}

/// Correlation-specific errors that aren't already in
/// GithubErrorKind. The duplicate-match case is unique to the
/// correlator.
#[derive(Debug, thiserror::Error)]
pub enum CorrelationError {
    /// Multiple runs carry the same gcit-<uuid> in their name.
    /// Emit ALL matched ids — operator's workflow has a
    /// misconfiguration (matrix expansion or repeated job).
    #[error(
        "workflow {repo}/{workflow} matched gcit_run_id={gcit_run_id} on \
         {} runs (ids: {run_ids:?}); this means the workflow spawned multiple parallel runs \
         with the same input. fix: ensure exactly one job carries `run-name: gcit-${{{{ inputs.gcit_run_id }}}}` \
         and that no matrix expansion creates duplicates. gcit refuses to track an ambiguous correlation.",
         .run_ids.len()
    )]
    DuplicateMatch {
        repo: String,
        workflow: String,
        gcit_run_id: Uuid,
        run_ids: Vec<u64>,
    },

    /// Hard deadline elapsed without a match.
    #[error(
        "github_run_correlation_timeout: workflow {repo}/{workflow} did not produce a \
         matching run within {timeout:?} (gcit_run_id={gcit_run_id}). \
         Check https://github.com/{repo}/actions for the run status, or add a \
         `run-name: gcit-${{{{ inputs.gcit_run_id }}}}` directive to the workflow if not present."
    )]
    Timeout {
        repo: String,
        workflow: String,
        gcit_run_id: Uuid,
        timeout: Duration,
    },

    /// Error from the underlying API call. Wrap the typed
    /// classification so the supervisor can surface the same
    /// last_error.kind values it does for direct dispatch errors.
    #[error("github error during correlation: {0}")]
    Github(GithubErrorKind),
}

impl From<GithubErrorKind> for CorrelationError {
    fn from(e: GithubErrorKind) -> Self {
        CorrelationError::Github(e)
    }
}

/// Correlate a single dispatch to a `Run.id` with the configured
/// timeout. The `cancel` token signals drain — when fired, the
/// correlator's deadline shortens to `DRAIN_TIMEOUT`.
///
/// `rate_limit` is consulted on each scan response so the snapshot
/// stays fresh (per-response observation, not just the /rate_limit
/// poller cadence).
///
/// `#[instrument(skip(client, rate_limit, cancel))]` — the params are
/// useful in logs (repo, workflow, gcit_run_id) but the client,
/// rate-limit state, and cancellation token are noise.
#[instrument(level = "debug", skip(client, rate_limit, cancel), fields(
    repo = %params.repo,
    workflow = %params.workflow,
    gcit_run_id = %params.gcit_run_id,
))]
pub async fn correlate(
    client: &Client,
    rate_limit: &super::rate_limit::RateLimitState,
    params: &CorrelateParams,
    cancel: CancellationToken,
) -> Result<CorrelationOutcome, CorrelationError> {
    let started = Instant::now();
    let normal_deadline = started + NORMAL_TIMEOUT;

    // Polling cadence (5s -> 60s, factor 2.0, jitter on). Driven by
    // backon's ExponentialBackoff iterator rather than hand-rolled
    // saturating arithmetic. The iterator is used directly (not
    // `.retry()`) because the correlator's outer loop is
    // deadline-bounded with cancellation-aware sleeps, not a
    // per-attempt error retry.
    let mut backoff_iter = ExponentialBuilder::default()
        .with_min_delay(POLL_INTERVAL_INITIAL)
        .with_max_delay(POLL_INTERVAL_MAX)
        .with_factor(POLL_INTERVAL_FACTOR as f32)
        .with_jitter()
        .without_max_times()
        .build();

    let mut attempts: u32 = 0;
    // Anchor for the drain deadline. `None` until we first observe
    // `cancel.is_cancelled()`; on that observation we capture the
    // current Instant once and reuse it so the drain budget cannot
    // slide forward by recomputing `now + DRAIN_TIMEOUT` each loop.
    let mut drain_start: Option<Instant> = None;

    loop {
        attempts += 1;

        let effective_deadline =
            compute_effective_deadline(&cancel, normal_deadline, &mut drain_start);
        if Instant::now() >= effective_deadline {
            return Err(CorrelationError::Timeout {
                repo: params.repo.clone(),
                workflow: params.workflow.clone(),
                gcit_run_id: params.gcit_run_id,
                timeout: if cancel.is_cancelled() {
                    DRAIN_TIMEOUT
                } else {
                    NORMAL_TIMEOUT
                },
            });
        }

        if let Some(outcome) = scan_name_match(client, rate_limit, params, attempts).await? {
            return Ok(outcome);
        }

        if matches!(params.run_name_configured, Some(false) | None) {
            if let Some(outcome) = scan_head_sha_fallback(client, rate_limit, params).await? {
                return Ok(outcome);
            }
        }

        sleep_with_cancel_awareness(&mut backoff_iter, effective_deadline, &cancel).await;
    }
}

/// Compute the effective deadline for this iteration. If drain has
/// fired, take the lesser of `(normal_deadline, drain_start + DRAIN_TIMEOUT)`.
/// On the first observation of `cancel.is_cancelled()`, capture the
/// onset Instant in `drain_start` so subsequent iterations bound the
/// drain budget by the actual cancel onset rather than recomputing
/// `now + DRAIN_TIMEOUT` each loop.
fn compute_effective_deadline(
    cancel: &CancellationToken,
    normal_deadline: Instant,
    drain_start: &mut Option<Instant>,
) -> Instant {
    if cancel.is_cancelled() {
        let onset = *drain_start.get_or_insert_with(Instant::now);
        std::cmp::min(normal_deadline, onset + DRAIN_TIMEOUT)
    } else {
        normal_deadline
    }
}

/// Run one full pagination scan looking for a name-match. Returns
/// `Ok(Some)` on a single match (correlation succeeded), propagates
/// the underlying `CorrelationError` on a duplicate-match or a
/// permanent GitHub error, and returns `Ok(None)` on either NoMatch
/// or a transient GitHub error so the outer loop falls through to
/// the fallback / backoff.
async fn scan_name_match(
    client: &Client,
    rate_limit: &super::rate_limit::RateLimitState,
    params: &CorrelateParams,
    attempts: u32,
) -> Result<Option<CorrelationOutcome>, CorrelationError> {
    match scan_runs_for_match(client, rate_limit, params).await {
        Ok(ScanResult::SingleMatch(run)) => {
            debug!(
                run_id = run.id.0,
                attempts, "correlation matched on Run.name",
            );
            Ok(Some(CorrelationOutcome {
                run_id: run.id.0,
                summary: run_to_summary(&run),
            }))
        }
        Ok(ScanResult::DuplicateMatch(ids)) => {
            warn!(?ids, "duplicate gcit_run_id matches; aborting correlation");
            Err(CorrelationError::DuplicateMatch {
                repo: params.repo.clone(),
                workflow: params.workflow.clone(),
                gcit_run_id: params.gcit_run_id,
                run_ids: ids,
            })
        }
        Ok(ScanResult::NoMatch) => Ok(None),
        Err(e) => {
            // Permanent errors (Unauthorized/Forbidden/etc.) bypass
            // the retry loop. Transient retry via the outer backoff.
            if !e.is_transient() {
                return Err(CorrelationError::Github(e));
            }
            warn!(error = %e, attempts, "transient error during correlation scan");
            Ok(None)
        }
    }
}

/// Fallback path: when the workflow doesn't carry a `run-name`
/// directive (`run_name_configured == Some(false) | None`), the
/// name-substring match is impossible. Try `?head_sha=<sha>` plus
/// `created>=dispatched_at` instead. Emits a one-shot WARN when the
/// fallback succeeds against an `Unknown` (`None`) config, suggesting
/// the operator add the `run-name` directive.
async fn scan_head_sha_fallback(
    client: &Client,
    rate_limit: &super::rate_limit::RateLimitState,
    params: &CorrelateParams,
) -> Result<Option<CorrelationOutcome>, CorrelationError> {
    match scan_runs_for_fallback(client, rate_limit, params).await {
        Ok(Some(run)) => {
            if params.run_name_configured.is_none() {
                warn!(
                    "correlation succeeded via fallback (head_sha + dispatched_at). \
                     Recommend adding `run-name: gcit-${{{{ inputs.gcit_run_id }}}}` \
                     to the workflow for unambiguous correlation."
                );
            }
            Ok(Some(CorrelationOutcome {
                run_id: run.id.0,
                summary: run_to_summary(&run),
            }))
        }
        Ok(None) => Ok(None),
        Err(e) => {
            if !e.is_transient() {
                return Err(CorrelationError::Github(e));
            }
            warn!(error = %e, "transient error during fallback scan");
            Ok(None)
        }
    }
}

/// Pull the next delay from backon's exponential schedule, cap it
/// at `effective_deadline - now`, and sleep cancellation-aware so a
/// drain event immediately re-triggers the deadline computation
/// rather than waiting through the whole backoff.
async fn sleep_with_cancel_awareness(
    backoff_iter: &mut impl Iterator<Item = Duration>,
    effective_deadline: Instant,
    cancel: &CancellationToken,
) {
    // None never fires in practice (without_max_times), but saturate
    // at POLL_INTERVAL_MAX as a defense.
    let next_delay = backoff_iter.next().unwrap_or(POLL_INTERVAL_MAX);
    let now = Instant::now();
    let sleep_for = if effective_deadline > now {
        std::cmp::min(next_delay, effective_deadline - now)
    } else {
        Duration::from_millis(0)
    };
    tokio::select! {
        _ = tokio::time::sleep(sleep_for) => {}
        _ = cancel.cancelled() => {
            // Drain just fired; the outer loop re-computes the
            // effective deadline and keeps going.
        }
    }
}

// `Run` is a large struct; box the single-match payload so the
// enum size is dominated by the discriminant rather than the
// 12 KB Run footprint. The "Match" suffix is shared across
// variants for read clarity (`scan_runs_for_match`'s outcomes
// are NoMatch / SingleMatch / DuplicateMatch); silence the
// stylistic clippy lint locally.
#[allow(clippy::enum_variant_names)]
enum ScanResult {
    NoMatch,
    SingleMatch(Box<Run>),
    DuplicateMatch(Vec<u64>),
}

/// Walk pages of list_workflow_runs looking for a Run.name
/// substring match on `gcit-<uuid>`. Restart from page 1 each
/// call so a run that lands between polls isn't missed.
async fn scan_runs_for_match(
    client: &Client,
    rate_limit: &super::rate_limit::RateLimitState,
    params: &CorrelateParams,
) -> Result<ScanResult, GithubErrorKind> {
    let needle = format!("gcit-{}", params.gcit_run_id);
    let mut matches: Vec<Run> = Vec::new();
    let mut page_count: u32 = 0;

    let first_page = list_runs_page(client, rate_limit, params, None).await?;
    let mut current = first_page;

    loop {
        page_count += 1;
        for run in &current.items {
            if run.name.contains(&needle) {
                matches.push(run.clone());
            }
        }

        if page_count >= MAX_PAGES_PER_POLL {
            break;
        }

        // Walk to the next page, if any.
        match current.next.clone() {
            Some(uri) => {
                match next_page(client, rate_limit, &params.repo, &params.workflow, uri).await? {
                    Some(p) => current = p,
                    None => break,
                }
            }
            None => break,
        }
    }

    match matches.len() {
        0 => Ok(ScanResult::NoMatch),
        1 => Ok(ScanResult::SingleMatch(Box::new(matches.pop().unwrap()))),
        _ => {
            let ids: Vec<u64> = matches.iter().map(|r| r.id.0).collect();
            Ok(ScanResult::DuplicateMatch(ids))
        }
    }
}

/// Fallback: filter list_workflow_runs by `head_sha` + dispatched
/// time. Selects the most recent run whose `created_at >=
/// dispatched_at - CLOCK_SKEW_BUFFER`. The buffer covers NTP step
/// scenarios so a run created seconds before the local clock's
/// `dispatched_at` (per the GitHub server's clock) isn't silently
/// discarded. The created>= filter is enforced client-side because
/// octocrab's ListRunsBuilder does not surface a `created` query
/// param (the GitHub API does, but the builder does not).
async fn scan_runs_for_fallback(
    client: &Client,
    rate_limit: &super::rate_limit::RateLimitState,
    params: &CorrelateParams,
) -> Result<Option<Run>, GithubErrorKind> {
    let mut newest: Option<Run> = None;
    let mut page_count: u32 = 0;
    let cutoff = params.dispatched_at
        - chrono::Duration::from_std(CLOCK_SKEW_BUFFER)
            .unwrap_or_else(|_| chrono::Duration::zero());
    let mut current = list_runs_page(client, rate_limit, params, Some(&params.head_sha)).await?;
    loop {
        page_count += 1;
        for run in &current.items {
            if run.created_at < cutoff {
                continue;
            }
            match newest.as_ref() {
                None => newest = Some(run.clone()),
                Some(prev) if run.created_at > prev.created_at => newest = Some(run.clone()),
                _ => {}
            }
        }
        if page_count >= MAX_PAGES_PER_POLL {
            break;
        }
        match current.next.clone() {
            Some(uri) => {
                match next_page(client, rate_limit, &params.repo, &params.workflow, uri).await? {
                    Some(p) => current = p,
                    None => break,
                }
            }
            None => break,
        }
    }
    Ok(newest)
}

/// Issue a single list_workflow_runs request scoped to the
/// (repo, workflow). Uses classified_get so observe_headers fires
/// per response.
async fn list_runs_page(
    client: &Client,
    rate_limit: &super::rate_limit::RateLimitState,
    params: &CorrelateParams,
    head_sha: Option<&str>,
) -> Result<Page<Run>, GithubErrorKind> {
    let (owner, repo) = super::dispatcher::split_repo(&params.repo)?;
    let mut path = format!(
        "/repos/{owner}/{repo}/actions/workflows/{workflow}/runs?event=workflow_dispatch&per_page=100",
        owner = owner,
        repo = repo,
        workflow = params.workflow,
    );
    if !params.branch.is_empty() {
        path.push_str("&branch=");
        path.push_str(&params.branch);
    }
    if let Some(sha) = head_sha {
        path.push_str("&head_sha=");
        path.push_str(sha);
    }
    let uri = path
        .parse::<http::Uri>()
        .map_err(|e| GithubErrorKind::Unknown {
            source: anyhow::anyhow!("correlator route URI invalid: {e}"),
        })?;
    super::client::classified_get(client, rate_limit, &params.repo, &params.workflow, uri).await
}

/// Walk to the next page via classified_get so observe_headers fires.
/// `repo` and `workflow` are threaded through so that any 404 surfaced
/// by the pagination link carries the same path context the caller's
/// initial request did, rather than collapsing to an empty-string
/// "/repos//actions/workflows/" message.
async fn next_page(
    client: &Client,
    rate_limit: &super::rate_limit::RateLimitState,
    repo: &str,
    workflow: &str,
    uri: http::Uri,
) -> Result<Option<Page<Run>>, GithubErrorKind> {
    let page: Page<Run> =
        super::client::classified_get(client, rate_limit, repo, workflow, uri).await?;
    Ok(Some(page))
}

/// Convert an octocrab `Run` into gcit's `RunSummary`. Steps and
/// jobs come from a separate list_jobs call (the monitor handles
/// that — see `monitor.rs`); for the correlation outcome we leave
/// `jobs` empty and let the monitor populate it.
///
/// `run.run_number` is `i64` in octocrab; `try_from` produces 0 for
/// the impossible-negative case rather than panicking on cast or
/// silently wrapping.
pub fn run_to_summary(run: &Run) -> RunSummary {
    let conclusion = run.conclusion.as_deref().map(Conclusion::from_api);
    let status = RunStatus::from_api(&run.status);
    RunSummary {
        run_id: run.id.0,
        run_url: run.html_url.to_string(),
        run_number: u64::try_from(run.run_number).unwrap_or(0),
        run_attempt: 1,
        status,
        conclusion,
        started_at: Some(run.created_at),
        completed_at: Some(run.updated_at),
        jobs: Vec::new(),
    }
}

/// Translate octocrab's `Conclusion` enum into gcit's `Conclusion`.
/// Shared by `job_to_result` and `step_to_result` because the mapping
/// is identical — they both target the same set of variants. octocrab
/// marks its enum `#[non_exhaustive]`, so future additions map to
/// `Conclusion::Unknown` rather than crashing.
pub fn map_conclusion(c: &octocrab::models::workflows::Conclusion) -> Conclusion {
    match c {
        octocrab::models::workflows::Conclusion::ActionRequired => Conclusion::ActionRequired,
        octocrab::models::workflows::Conclusion::Cancelled => Conclusion::Cancelled,
        octocrab::models::workflows::Conclusion::Failure => Conclusion::Failure,
        octocrab::models::workflows::Conclusion::Neutral => Conclusion::Neutral,
        octocrab::models::workflows::Conclusion::Skipped => Conclusion::Skipped,
        octocrab::models::workflows::Conclusion::Success => Conclusion::Success,
        octocrab::models::workflows::Conclusion::TimedOut => Conclusion::TimedOut,
        _ => Conclusion::Unknown,
    }
}

/// Helpers for the monitor: convert octocrab Job + Step into gcit
/// types. Lives here (alongside run_to_summary) to keep all
/// model translations in one place.
pub fn job_to_result(job: &octocrab::models::workflows::Job) -> JobResult {
    JobResult {
        job_id: job.id.0,
        name: job.name.clone(),
        html_url: job.html_url.to_string(),
        conclusion: job.conclusion.as_ref().map(map_conclusion),
        started_at: Some(job.started_at),
        completed_at: job.completed_at,
        steps: job.steps.iter().map(step_to_result).collect(),
        run_attempt: job.run_attempt,
    }
}

/// Convert an octocrab `Step` into gcit's `StepResult`. `step.number`
/// is `i64` in octocrab; `try_from` produces 0 for the impossible-
/// negative case rather than panicking on cast or silently wrapping.
pub fn step_to_result(step: &octocrab::models::workflows::Step) -> StepResult {
    StepResult {
        name: step.name.clone(),
        number: u32::try_from(step.number).unwrap_or(0),
        conclusion: step.conclusion.as_ref().map(map_conclusion),
        started_at: step.started_at,
        completed_at: step.completed_at,
    }
}

/// Strip refs/heads/ prefix from a fully-qualified ref to get
/// just the branch name (the form GitHub's list_runs `branch`
/// query param expects). Returns empty string for non-branch
/// refs (the API filter doesn't apply to tags).
pub fn ref_to_branch(ref_name: &str) -> &str {
    ref_name.strip_prefix("refs/heads/").unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ref_to_branch_strips_heads_prefix() {
        assert_eq!(ref_to_branch("refs/heads/main"), "main");
        assert_eq!(ref_to_branch("refs/heads/feature/foo"), "feature/foo");
    }

    #[test]
    fn ref_to_branch_returns_empty_for_tags() {
        // `?branch=` query is meaningless for tag dispatches.
        assert_eq!(ref_to_branch("refs/tags/v1.0"), "");
        assert_eq!(ref_to_branch("refs/heads/"), "");
        assert_eq!(ref_to_branch("not-a-ref"), "");
    }

    #[test]
    fn timeout_constants_pinned() {
        assert_eq!(NORMAL_TIMEOUT, Duration::from_secs(300));
        assert_eq!(DRAIN_TIMEOUT, Duration::from_secs(30));
    }

    #[test]
    fn poll_interval_constants_pinned() {
        assert_eq!(POLL_INTERVAL_INITIAL, Duration::from_secs(5));
        assert_eq!(POLL_INTERVAL_MAX, Duration::from_secs(60));
        assert!(
            (POLL_INTERVAL_FACTOR - 2.0_f64).abs() < f64::EPSILON,
            "factor must be 2.0",
        );
    }

    #[test]
    fn max_pages_per_poll_pinned() {
        assert_eq!(MAX_PAGES_PER_POLL, 5);
    }

    #[test]
    fn duplicate_match_error_message_explains_misconfiguration() {
        // tests/github_duplicate_match.rs::duplicate_match_error_message_explains_workflow_misconfig
        // requires the message to mention "matrix", "run-name",
        // "duplicate", and "ambiguous". Pin the substrings.
        let err = CorrelationError::DuplicateMatch {
            repo: "owner/repo".into(),
            workflow: "ci.yml".into(),
            gcit_run_id: Uuid::nil(),
            run_ids: vec![201, 202, 203],
        };
        let msg = format!("{err}");
        assert!(msg.contains("matrix"), "msg: {msg}");
        assert!(msg.contains("run-name"), "msg: {msg}");
        assert!(msg.contains("ambiguous"), "msg: {msg}");
        assert!(
            msg.contains("3 runs") || msg.contains("ids: [201"),
            "msg: {msg}"
        );
    }

    #[test]
    fn timeout_error_message_pins_kind_and_workflow() {
        let err = CorrelationError::Timeout {
            repo: "owner/repo".into(),
            workflow: "ci.yml".into(),
            gcit_run_id: Uuid::nil(),
            timeout: NORMAL_TIMEOUT,
        };
        let msg = format!("{err}");
        // tests/github_drain_timeout.rs::timeout_emits_last_error_kind_correlation_timeout
        // requires the kind string "github_run_correlation_timeout".
        assert!(msg.contains("github_run_correlation_timeout"), "msg: {msg}",);
        assert!(msg.contains("ci.yml"), "msg: {msg}");
        assert!(msg.contains("owner/repo"), "msg: {msg}");
    }

    #[test]
    fn from_github_error_wraps_correctly() {
        let g = GithubErrorKind::Unauthorized {
            credential: crate::config::CredentialId::new("github_pat").unwrap(),
        };
        let e: CorrelationError = g.into();
        assert!(matches!(e, CorrelationError::Github(_)));
    }

    #[test]
    fn compute_effective_deadline_uses_normal_when_not_cancelled() {
        // Without cancel, the effective deadline must equal the
        // normal deadline. Pin so a regression that swapped the two
        // arms (or that lost the cancel check) surfaces here.
        let cancel = CancellationToken::new();
        let normal = Instant::now() + NORMAL_TIMEOUT;
        let mut drain_start: Option<Instant> = None;
        let effective = compute_effective_deadline(&cancel, normal, &mut drain_start);
        assert_eq!(effective, normal);
        assert!(
            drain_start.is_none(),
            "no cancel observed must leave drain_start as None",
        );
    }

    #[test]
    fn compute_effective_deadline_captures_drain_onset_on_first_cancel_observation() {
        // First call after cancel.cancel() must populate drain_start.
        // Subsequent calls must reuse the same anchor — without
        // capture, a long-running poll cycle would keep sliding the
        // drain deadline forward.
        let cancel = CancellationToken::new();
        cancel.cancel();
        let normal = Instant::now() + NORMAL_TIMEOUT;
        let mut drain_start: Option<Instant> = None;

        let first = compute_effective_deadline(&cancel, normal, &mut drain_start);
        assert!(
            drain_start.is_some(),
            "first cancel observation must capture onset"
        );
        let captured = drain_start.expect("captured above");

        // Second call must NOT advance the anchor.
        let second = compute_effective_deadline(&cancel, normal, &mut drain_start);
        assert_eq!(drain_start, Some(captured), "anchor must not slide forward");
        assert_eq!(
            first, second,
            "effective deadline must be stable across calls"
        );
    }

    #[test]
    fn compute_effective_deadline_picks_lesser_when_drain_shorter_than_normal() {
        // When drain_start + DRAIN_TIMEOUT lands BEFORE normal_deadline,
        // the effective deadline is the drain one (shutdown is bounded
        // by DRAIN_TIMEOUT, not NORMAL_TIMEOUT). Pin the min() shape.
        let cancel = CancellationToken::new();
        cancel.cancel();
        let normal = Instant::now() + NORMAL_TIMEOUT;
        let mut drain_start: Option<Instant> = None;
        let effective = compute_effective_deadline(&cancel, normal, &mut drain_start);
        let onset = drain_start.expect("captured above");
        let drain_deadline = onset + DRAIN_TIMEOUT;
        assert_eq!(
            effective, drain_deadline,
            "DRAIN_TIMEOUT (30s) < NORMAL_TIMEOUT (5min); drain must win",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_with_cancel_awareness_returns_immediately_when_pre_cancelled() {
        // Cancelled token must short-circuit the sleep arm — the outer
        // loop relies on this to re-evaluate the (now drain-bounded)
        // deadline without waiting through the full backoff.
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut iter = std::iter::repeat(POLL_INTERVAL_MAX);
        let start = Instant::now();
        let deadline = start + Duration::from_secs(600);
        sleep_with_cancel_awareness(&mut iter, deadline, &cancel).await;
        let advanced = Instant::now() - start;
        assert!(
            advanced < Duration::from_millis(1),
            "cancel arm must fire instantly; virtual clock advanced {advanced:?}",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_with_cancel_awareness_sleeps_for_next_backoff_delay() {
        // When neither deadline nor cancel intervenes, the function
        // sleeps for exactly the next backoff slot.
        let cancel = CancellationToken::new();
        let mut iter =
            std::iter::once(Duration::from_secs(5)).chain(std::iter::repeat(POLL_INTERVAL_MAX));
        let start = Instant::now();
        let deadline = start + Duration::from_secs(600);
        sleep_with_cancel_awareness(&mut iter, deadline, &cancel).await;
        let advanced = Instant::now() - start;
        assert_eq!(
            advanced,
            Duration::from_secs(5),
            "must sleep exactly the iterator's next delay",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_with_cancel_awareness_caps_sleep_at_remaining_budget() {
        // Backoff iterator yields 60s but deadline is only 5s away —
        // the sleep must clip to the deadline so the outer loop wakes
        // and returns Timeout promptly rather than over-running.
        let cancel = CancellationToken::new();
        let mut iter = std::iter::repeat(POLL_INTERVAL_MAX);
        let start = Instant::now();
        let deadline = start + Duration::from_secs(5);
        sleep_with_cancel_awareness(&mut iter, deadline, &cancel).await;
        let advanced = Instant::now() - start;
        assert_eq!(
            advanced,
            Duration::from_secs(5),
            "sleep must clip to deadline (5s), not the 60s backoff slot",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_with_cancel_awareness_zero_sleep_when_deadline_already_passed() {
        // The outer loop checks `now >= deadline` before calling sleep,
        // but defense-in-depth: an in-the-past deadline must produce a
        // zero-duration sleep, not an underflow or wedge.
        let cancel = CancellationToken::new();
        let mut iter = std::iter::repeat(POLL_INTERVAL_MAX);
        let now = Instant::now();
        let past_deadline = now;
        // Advance virtual time so deadline is strictly in the past.
        tokio::time::sleep(Duration::from_millis(10)).await;
        let before = Instant::now();
        sleep_with_cancel_awareness(&mut iter, past_deadline, &cancel).await;
        let advanced = Instant::now() - before;
        assert!(
            advanced < Duration::from_millis(1),
            "past deadline must short-circuit to zero-sleep; advanced {advanced:?}",
        );
    }
}

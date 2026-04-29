// list_jobs + latest_attempts filter.
// monitor: job_interval, get_run+list_jobs -> on terminal status,
// fire notifier -> RunFinished.
// JobResult / RunSummary types include `run_attempt`, implying
// support for re-runs.
//
// Per my prior verification (octocrab/src/api/workflows.rs:119,334):
//   list_jobs(run_id).filter(Filter::All|Latest).per_page(100).page(1).send()
//   GET /repos/{o}/{r}/actions/runs/{run_id}/jobs
//   Page<workflows::Job>
//
// "latest_attempts filter": when a workflow run is re-run
// (e.g., user clicks "Re-run failed jobs"), each job gets a new
// attempt. By default, list_jobs returns only the latest attempt
// per job. The Filter::All variant returns ALL attempts.
//
// gcit's behavior: report on the LATEST attempt of each job (the
// run_attempt field implies this; the filter choice pins the
// semantics).

use rstest::rstest;

#[tokio::test]
#[ignore = "requires gcit::github::monitor::list_jobs (not yet implemented)"]
async fn list_jobs_uses_latest_filter_by_default() {
    // Wiremock matcher includes query_param("filter", "latest") (or no
    // filter param if octocrab's default is latest — confirm via
    // ListJobsBuilder::filter docs; per workflows.rs:328, filter is
    // Optional and serialized only when set).
    //
    // SPEC GAP: spec doesn't explicitly say "latest". Recommend:
    // explicitly set filter=latest_attempts (octocrab's
    // params::workflows::Filter::Latest). Mutation target: defaulting
    // to Filter::All would over-report job statuses. flag.
}

#[tokio::test]
#[ignore = "requires GitHub monitor implementation"]
async fn list_jobs_returns_jobs_with_steps() {
    // Wiremock body:
    //   { "jobs": [
    //       { "id": 1001, "name": "build", "html_url": "...",
    //         "status": "completed", "conclusion": "success",
    //         "started_at": "...", "completed_at": "...",
    //         "run_attempt": 1,
    //         "steps": [
    //           { "name": "Set up", "number": 1, "conclusion": "success",
    //             "started_at": "...", "completed_at": "..." },
    //           { "name": "Build", "number": 2, "conclusion": "success", ... }
    //         ]
    //       }
    //     ] }
    //
    // Assert gcit's JobResult and StepResult.
    // Field-by-field: id, name, html_url, conclusion, started_at,
    // completed_at, steps, run_attempt — all match the wiremock body.
    //
    // Mutation target: dropped fields, wrong types, off-by-one indices.
}

#[rstest]
#[case::queued("queued", "RunStatus::Queued")]
#[case::in_progress("in_progress", "RunStatus::InProgress")]
#[case::completed("completed", "RunStatus::Completed")]
#[case::waiting("waiting", "RunStatus::Waiting")]
#[case::other_unknown("requested", "RunStatus::Other")]
#[case::other_pending("pending", "RunStatus::Other")]
#[ignore = "requires GitHub monitor implementation"]
fn run_status_string_to_enum(#[case] api_str: &str, #[case] expect: &str) {
    let _ = (api_str, expect);
    // Pure logic: GitHub's "status" field maps to gcit::RunStatus.
    // Pin the exact mapping.
    //
    // Mutation target: case mismatch (API uses snake_case; enum impl
    // might compare with PascalCase by accident). Pin via this test.
    //
    // SPEC GAP: spec line 411 lists Queued/InProgress/Completed/Waiting/
    // Other but doesn't pin the API->enum mapping. Recommend:
    // serde rename_all="snake_case" + Other catch-all. flag.
}

#[tokio::test]
#[ignore = "requires GitHub monitor implementation"]
async fn list_jobs_paginates_when_run_has_many_jobs() {
    // A run with >100 jobs (per_page max) requires pagination via
    // octocrab's all_pages. Mock 2 pages of 100 jobs each (200 total);
    // assert gcit collects all 200.
    //
    // SPEC GAP: spec doesn't address job-count caps. Recommend: cap at
    // some reasonable N (1000 jobs?) before bailing with a Permanent
    // error (operator's workflow has a runaway matrix). flag.
}

#[tokio::test]
#[ignore = "requires GitHub monitor implementation"]
async fn monitor_polls_at_job_interval() {
    // `job_interval = "30s"` default per PollDefaults.
    // Monitor uses job_interval (NOT source_interval).
    //
    // Under start_paused:
    //   t=0:    monitor polls; run still in_progress
    //   t=30:   monitor polls again; assert ~30s elapsed
    //   t=60:   monitor polls third time
    //
    // Mutation target: using source_interval (60s) instead of
    // job_interval (30s) for monitor. Test catches.
}

#[tokio::test]
#[ignore = "requires GitHub monitor implementation"]
async fn monitor_stops_on_terminal_status() {
    // "on terminal status, fire notifier -> RunFinished".
    // Terminal = Completed (any conclusion). The monitor must STOP
    // polling that run after observing Completed.
    //
    // Sequence:
    //   poll 1: status=queued -> continue
    //   poll 2: status=in_progress -> continue
    //   poll 3: status=completed -> fire notifier, emit RunFinished, STOP
    //   no further polls
    //
    // Assert wiremock saw exactly 3 polls, no more.
    //
    // Mutation target: an off-by-one or "always poll N times" loop. Test
    // catches by counting wiremock invocations.
}

#[tokio::test]
#[ignore = "requires GitHub monitor implementation"]
async fn monitor_emits_run_finished_with_full_summary() {
    // The RunFinished StateUpdate carries the FULL RunSummary —
    // including jobs (Vec<JobResult>). Pin the shape.
    //
    // Mutation target: stripping the jobs array (would lose per-job
    // notification payload).
}

#[tokio::test]
#[ignore = "requires GitHub monitor implementation"]
async fn monitor_acquires_rate_bucket_per_request() {
    // Each get_run + list_jobs call goes through the rate bucket. With
    // a 5000/hour quota and many flows, monitors must NOT exhaust the
    // bucket via polling.
    //
    // Each list_jobs response carries X-RateLimit-Remaining; the
    // monitor must opportunistically update the bucket even though
    // it's a "GET" path.
}

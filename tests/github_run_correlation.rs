// Run correlation: workflow_dispatch -> Run.id.
//
// The pipeline under test is `gcit::github::correlator::correlate`,
// which:
//   1. Polls `GET /repos/{o}/{r}/actions/workflows/{wf}/runs?event=
//      workflow_dispatch&per_page=100[&branch=NAME]` for the
//      gcit-<uuid> name marker. Found exactly once -> Ok.
//   2. Falls back to `?head_sha=<sha>` + client-side
//      `created_at >= dispatched_at - CLOCK_SKEW_BUFFER` filter when
//      either (a) `run_name_configured == Some(false)` (operator
//      told us run-name isn't there) or (b) `run_name_configured ==
//      None` (unknown — try name first, then fallback). Most-recent
//      created_at wins.
//   3. Repeats on backon-driven exponential backoff
//      (5s -> 60s, factor 2.0) until match, until DuplicateMatch
//      surfaces, or until the NORMAL_TIMEOUT (5min) / DRAIN_TIMEOUT
//      (30s when cancel fires) deadline elapses.
//
// These integration tests pin the wiring across that pipeline by
// driving real HTTP through wiremock. They mirror the harness
// shape established in tests/github_dispatch.rs and use the shared
// helpers in tests/common/mod.rs (PAT, ensure_crypto_provider).
// The correlator does not consume the rate-limit bucket the way
// dispatcher does, so a local helper builds correlator-shaped
// dependencies (no RateBucket).

mod common;

use std::time::Duration;

use chrono::{TimeZone, Utc};
use rstest::rstest;
use secrecy::SecretString;
use serde_json::json;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use gcit::config::CredentialId;
use gcit::github::client::Client;
use gcit::github::correlator::{correlate, CorrelateParams, CorrelationError, CLOCK_SKEW_BUFFER};
use gcit::github::error::GithubErrorKind;
use gcit::github::rate_limit::RateLimitState;

use common::{ensure_crypto_provider, make_run_json, PAT, RUNS_PATH};

/// Build a correlator-shaped Client + pre-seeded RateLimitState.
/// Mirrors `common::build_dispatch_deps` minus the RateBucket — the
/// correlator's GET path doesn't reserve quota slots the way
/// `dispatch()` does. The snapshot is seeded with full quota +
/// future reset so observe_headers calls during the test don't
/// trigger a defer.
async fn build_correlator_deps(mock_uri: &str) -> (Client, RateLimitState) {
    let client = Client::builder()
        .credential(CredentialId::new("github_pat").expect("valid id"))
        .token(SecretString::from(PAT.to_string()))
        .request_timeout(Duration::from_secs(5))
        .base_uri(mock_uri)
        .build()
        .expect("client build");
    let rate_limit = RateLimitState::new();
    rate_limit
        .observe_full(5000, 5000, Utc::now() + chrono::Duration::seconds(3600))
        .await;
    (client, rate_limit)
}

/// Default CorrelateParams targeting the canonical wiremock route.
/// Tests override individual fields (gcit_run_id, head_sha,
/// dispatched_at, run_name_configured) as needed.
fn default_params(gcit_run_id: Uuid) -> CorrelateParams {
    CorrelateParams {
        repo: "myorg/linux-builder".to_string(),
        workflow: "ci.yml".to_string(),
        gcit_run_id,
        branch: "main".to_string(),
        head_sha: "deadbeefcafe1234567890abcdef1234567890ab".to_string(),
        dispatched_at: Utc::now(),
        run_name_configured: Some(true),
    }
}

#[tokio::test]
async fn run_correlator_matches_via_run_name_first() {
    // The correlator's name-match scan walks the page items and
    // selects the run whose `Run.name` contains `gcit-<uuid>`. Three
    // runs in the body, only one carries the marker. The correlator
    // MUST select that one — mutation that flips to first or last
    // would pick the wrong run.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    let gcit_run_id = Uuid::new_v4();
    let now = Utc::now();
    let needle = format!("gcit-{gcit_run_id}");

    let body = json!({
        "total_count": 3,
        "workflow_runs": [
            make_run_json(100, "regular ci run", "deadbeefcafe1234567890abcdef1234567890ab", now),
            make_run_json(101, &format!("scheduled build {needle}"), "deadbeefcafe1234567890abcdef1234567890ab", now),
            make_run_json(102, "another", "abc1230000000000000000000000000000000000", now),
        ],
    });

    Mock::given(method("GET"))
        .and(path(RUNS_PATH))
        .and(query_param("event", "workflow_dispatch"))
        .and(query_param("branch", "main"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .expect(1)
        .mount(&mock)
        .await;

    let (client, rate_limit) = build_correlator_deps(&mock.uri()).await;
    let params = default_params(gcit_run_id);
    let cancel = CancellationToken::new();
    let outcome = correlate(&client, &rate_limit, &params, cancel)
        .await
        .expect("name-match scan should resolve immediately");
    assert_eq!(
        outcome.run_id, 101,
        "correlator must select the run whose name contains gcit-<uuid>",
    );
    assert_eq!(outcome.summary.run_id, 101);
    assert_eq!(outcome.summary.run_number, 101);
    drop(mock);
}

#[tokio::test]
async fn fallback_filter_by_head_sha_and_created_when_run_name_absent() {
    // When `run_name_configured == Some(false)`, the correlator
    // skips the name-match scan and goes directly to the fallback
    // (`?head_sha=<sha>` + client-side created_at filter). The
    // returned run is selected by most-recent `created_at`. The
    // wiremock mock matches on `head_sha` query param to confirm
    // the fallback path actually fired.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    let gcit_run_id = Uuid::new_v4();
    let dispatched_at = Utc.with_ymd_and_hms(2026, 4, 26, 12, 34, 56).unwrap();
    let head_sha = "fa11ba1100000000000000000000000000000000";

    // Two runs with the same head_sha. run 200 is older, run 201
    // is newer — fallback must pick 201.
    let body = json!({
        "total_count": 2,
        "workflow_runs": [
            make_run_json(200, "build", head_sha, dispatched_at + chrono::Duration::seconds(2)),
            make_run_json(201, "build", head_sha, dispatched_at + chrono::Duration::seconds(10)),
        ],
    });

    Mock::given(method("GET"))
        .and(path(RUNS_PATH))
        .and(query_param("event", "workflow_dispatch"))
        .and(query_param("branch", "main"))
        .and(query_param("head_sha", head_sha))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .expect(1)
        .mount(&mock)
        .await;

    let (client, rate_limit) = build_correlator_deps(&mock.uri()).await;
    let mut params = default_params(gcit_run_id);
    params.dispatched_at = dispatched_at;
    params.head_sha = head_sha.to_string();
    params.run_name_configured = Some(false);
    let cancel = CancellationToken::new();
    let outcome = correlate(&client, &rate_limit, &params, cancel)
        .await
        .expect("fallback scan should resolve immediately");
    assert_eq!(
        outcome.run_id, 201,
        "fallback must pick the run with the most recent created_at",
    );
    drop(mock);
}

#[tokio::test]
async fn run_correlator_paginates_via_all_pages() {
    // octocrab's Page<T> follows the `Link: <...>; rel="next"` header
    // to walk pages. The correlator's first scan calls list_runs;
    // octocrab parses the Link header and the correlator dispatches
    // to `next_page()` if no match landed on page 1. The match on
    // page 2 must be discovered.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    let gcit_run_id = Uuid::new_v4();
    let now = Utc::now();
    let needle = format!("gcit-{gcit_run_id}");
    let mock_base = mock.uri();

    let next_link = format!(
        "<{mock_base}{RUNS_PATH}?event=workflow_dispatch&per_page=100&branch=main&page=2>; rel=\"next\""
    );

    // Page 1: three non-matching runs + Link rel="next".
    let page1 = json!({
        "total_count": 5,
        "workflow_runs": [
            make_run_json(300, "build", "abc", now),
            make_run_json(301, "test", "abc", now),
            make_run_json(302, "lint", "abc", now),
        ],
    });
    Mock::given(method("GET"))
        .and(path(RUNS_PATH))
        .and(query_param("event", "workflow_dispatch"))
        .and(query_param("branch", "main"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Link", next_link.as_str())
                .set_body_json(page1),
        )
        .up_to_n_times(1)
        .with_priority(1)
        .expect(1)
        .mount(&mock)
        .await;

    // Page 2: two runs, the second one carries the gcit marker.
    let page2 = json!({
        "total_count": 5,
        "workflow_runs": [
            make_run_json(303, "package", "abc", now),
            make_run_json(304, &format!("release {needle}"), "abc", now),
        ],
    });
    Mock::given(method("GET"))
        .and(path(RUNS_PATH))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page2))
        .with_priority(2)
        .expect(1)
        .mount(&mock)
        .await;

    let (client, rate_limit) = build_correlator_deps(&mock.uri()).await;
    let params = default_params(gcit_run_id);
    let cancel = CancellationToken::new();
    let outcome = correlate(&client, &rate_limit, &params, cancel)
        .await
        .expect("pagination should reach page 2 and find the match");
    assert_eq!(
        outcome.run_id, 304,
        "correlator must follow Link rel=next and match on page 2",
    );
    drop(mock);
}

#[tokio::test]
async fn run_correlator_polls_with_backoff_until_match_or_timeout() {
    // The correlator's outer backoff loop:
    //   poll #1: empty workflow_runs -> NoMatch -> sleep(POLL_INTERVAL_INITIAL).
    //   poll #2: match -> return Ok immediately.
    //
    // POLL_INTERVAL_INITIAL is 5s real time; with backon's jitter
    // this is up to 10s. The test runs on the real tokio clock
    // because wiremock is real IO — start_paused would fire the
    // request_timeout before the wiremock future polled. Wall-clock
    // budget: ~5-10s for one backoff cycle.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    let gcit_run_id = Uuid::new_v4();
    let now = Utc::now();
    let needle = format!("gcit-{gcit_run_id}");

    // First mock (priority 1, up_to_n_times=1, expect 1): empty
    // runs list — correlator's first scan finds nothing and sleeps.
    let empty_body = json!({"total_count": 0, "workflow_runs": []});
    Mock::given(method("GET"))
        .and(path(RUNS_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(empty_body))
        .up_to_n_times(1)
        .with_priority(1)
        .expect(1)
        .mount(&mock)
        .await;

    // Second mock (priority 2, expect 1): the matching run lands.
    // After the backoff sleep, the correlator's second scan picks
    // it up and returns.
    let match_body = json!({
        "total_count": 1,
        "workflow_runs": [
            make_run_json(401, &format!("ci {needle}"), "abc", now),
        ],
    });
    Mock::given(method("GET"))
        .and(path(RUNS_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(match_body))
        .with_priority(2)
        .expect(1)
        .mount(&mock)
        .await;

    let (client, rate_limit) = build_correlator_deps(&mock.uri()).await;
    let params = default_params(gcit_run_id);
    let cancel = CancellationToken::new();
    let outcome = correlate(&client, &rate_limit, &params, cancel)
        .await
        .expect("second poll should find the match");
    assert_eq!(
        outcome.run_id, 401,
        "correlator must keep polling on backoff until the match appears",
    );
    drop(mock);
}

#[rstest]
// 1s before dispatch — well inside the 120s buffer; matches.
#[case::just_before_dispatch(-1)]
// Same second as dispatch — at the boundary; matches.
#[case::same_second(0)]
// 5s after dispatch — clearly post-dispatch; matches.
#[case::after_dispatch(5)]
// 119s before dispatch — just inside the 120s buffer; matches.
#[case::just_inside_buffer(-119)]
#[tokio::test]
async fn fallback_keeps_runs_inside_clock_skew_buffer(#[case] offset_secs: i64) {
    // The fallback path filters runs by `created_at < cutoff` where
    // `cutoff = dispatched_at - CLOCK_SKEW_BUFFER` (computed in
    // correlate's fallback arm). CLOCK_SKEW_BUFFER is 120s — it
    // covers NTP step scenarios so a run created seconds before the
    // local clock's dispatched_at (per the GitHub server's clock)
    // isn't silently discarded.
    //
    // This rstest pins the inclusive lower bound: runs at or after
    // `dispatched_at - 120s` match. Mutation target: dropping the
    // buffer (cutoff = dispatched_at) would skip both
    // `just_before_dispatch` and `just_inside_buffer`. Flipping the
    // inequality (`>` instead of `<`) would skip `same_second` and
    // `after_dispatch`. The companion test
    // `fallback_skips_runs_outside_clock_skew_buffer` pins the
    // exclusive upper bound (runs created before the cutoff don't
    // match).
    ensure_crypto_provider();

    // Sanity: pin the buffer constant to the value these cases were
    // written against. If the constant changes, the -119 offset
    // would silently misalign with the new boundary.
    assert_eq!(
        CLOCK_SKEW_BUFFER,
        Duration::from_secs(120),
        "CLOCK_SKEW_BUFFER must be 120s; case offsets here are tuned to that value",
    );

    let mock = MockServer::start().await;
    let gcit_run_id = Uuid::new_v4();
    let dispatched_at = Utc.with_ymd_and_hms(2026, 4, 26, 12, 34, 56).unwrap();
    let head_sha = "deafdeafdeafdeafdeafdeafdeafdeafdeafdeaf";
    let run_created_at = dispatched_at + chrono::Duration::seconds(offset_secs);

    let body = json!({
        "total_count": 1,
        "workflow_runs": [
            make_run_json(500, "build", head_sha, run_created_at),
        ],
    });

    Mock::given(method("GET"))
        .and(path(RUNS_PATH))
        .and(query_param("event", "workflow_dispatch"))
        .and(query_param("branch", "main"))
        .and(query_param("head_sha", head_sha))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .expect(1)
        .mount(&mock)
        .await;

    let (client, rate_limit) = build_correlator_deps(&mock.uri()).await;
    let mut params = default_params(gcit_run_id);
    params.dispatched_at = dispatched_at;
    params.head_sha = head_sha.to_string();
    params.run_name_configured = Some(false);
    let cancel = CancellationToken::new();
    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        correlate(&client, &rate_limit, &params, cancel),
    )
    .await
    .expect("correlate must resolve quickly when the fallback finds a match")
    .expect("matching run should produce Ok");
    assert_eq!(
        outcome.run_id, 500,
        "run created at offset {offset_secs}s must match the fallback filter",
    );
}

#[tokio::test]
async fn fallback_skips_runs_outside_clock_skew_buffer() {
    // Companion to `fallback_keeps_runs_inside_clock_skew_buffer`:
    // a run created 121s before dispatch (1s past the cutoff) MUST
    // be skipped. The correlator then has nothing to return from the
    // first cycle and enters the backoff sleep; the test infers the
    // skip from the fact that no Ok lands within a short test
    // budget (a non-skipping implementation would return Ok within
    // the first scan).
    //
    // Why this isn't an rstest case alongside the matching cases:
    // the matching path returns Ok in the first cycle (~ms), so a
    // matched test asserts on the outcome directly. The non-match
    // path enters backon's 5s sleep and would only surface a hard
    // result on the 30s drain timeout (cancel doesn't abort the
    // correlator, just shortens its deadline — see DRAIN_TIMEOUT in
    // the correlator module).
    // Wrapping with a tighter `tokio::time::timeout` lets us
    // distinguish "skip" (timeout fires) from "match" (Ok lands)
    // without paying the 30s drain cost.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    let gcit_run_id = Uuid::new_v4();
    let dispatched_at = Utc.with_ymd_and_hms(2026, 4, 26, 12, 34, 56).unwrap();
    let head_sha = "feedbeefcafe1234567890abcdef1234567890ab";
    // 121s before dispatched_at == 1s past the cutoff =
    // dispatched_at - CLOCK_SKEW_BUFFER (120s). MUST be skipped.
    let run_created_at = dispatched_at - chrono::Duration::seconds(121);

    let body = json!({
        "total_count": 1,
        "workflow_runs": [
            make_run_json(501, "build", head_sha, run_created_at),
        ],
    });

    Mock::given(method("GET"))
        .and(path(RUNS_PATH))
        .and(query_param("head_sha", head_sha))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&mock)
        .await;

    let (client, rate_limit) = build_correlator_deps(&mock.uri()).await;
    let mut params = default_params(gcit_run_id);
    params.dispatched_at = dispatched_at;
    params.head_sha = head_sha.to_string();
    params.run_name_configured = Some(false);
    let cancel = CancellationToken::new();
    // Tight outer timeout: a matching run would resolve in tens of
    // ms (one wire round-trip + parse). 1.5s is enough to clear that
    // budget but well under the POLL_INTERVAL_INITIAL (5s) the
    // correlator would sleep on after a NoMatch, so we know the
    // correlator hasn't escaped from its first-poll empty-result
    // path. The Elapsed return proves the run was filtered out.
    let result = tokio::time::timeout(
        Duration::from_millis(1500),
        correlate(&client, &rate_limit, &params, cancel),
    )
    .await;
    assert!(
        result.is_err(),
        "correlator must NOT produce Ok within 1.5s when the only available run is outside the clock-skew buffer; got {result:?}",
    );
}

#[tokio::test]
async fn run_correlator_returns_unauthorized_immediately_no_retry() {
    // The list_runs endpoint returns 401. classify_status maps 401
    // to GithubErrorKind::Unauthorized -> Permanent. The correlator
    // checks `is_transient()` and bails immediately rather than
    // entering the backoff loop. expect(1) pins the no-retry
    // contract: exactly one wire request before the error surfaces.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(RUNS_PATH))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({
            "message": "Bad credentials",
            "documentation_url": "https://docs.github.com/rest"
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let (client, rate_limit) = build_correlator_deps(&mock.uri()).await;
    let params = default_params(Uuid::new_v4());
    let cancel = CancellationToken::new();
    let err = correlate(&client, &rate_limit, &params, cancel)
        .await
        .expect_err("401 must surface as Err");
    match err {
        CorrelationError::Github(GithubErrorKind::Unauthorized { credential }) => {
            assert_eq!(
                credential.as_str(),
                "github_pat",
                "Unauthorized must carry the configured credential id",
            );
        }
        other => panic!(
            "expected Github(Unauthorized), got {other:?} (Permanent classification must surface immediately)",
        ),
    }
    // expect(1) on Drop confirms the correlator did NOT retry on a
    // Permanent classification — a regression that flipped the
    // is_transient() check would loop and produce wire-count > 1.
    drop(mock);
}

#[tokio::test]
async fn correlator_handles_run_appearing_during_pagination() {
    // The correlator restarts pagination from page 1 each poll cycle
    // (per the module-level invariant in correlate) so a run
    // appearing between polls is not missed. To exercise this
    // contract: the first poll cycle gets an empty page 1 (no match,
    // no Link header -> single-page scan exhausted). The correlator
    // sleeps on backon. Before the second poll cycle fires, a NEW
    // run appears on page 1; the second poll cycle starts from
    // page 1 (NOT continuing from wherever the previous cycle left
    // off) and finds the match.
    //
    // Mutation target: a correlator that maintains pagination
    // state across cycles would never see the new page 1 entry.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    let gcit_run_id = Uuid::new_v4();
    let now = Utc::now();
    let needle = format!("gcit-{gcit_run_id}");

    // First request: empty page (no match, no Link header).
    let empty = json!({"total_count": 0, "workflow_runs": []});
    Mock::given(method("GET"))
        .and(path(RUNS_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(empty))
        .up_to_n_times(1)
        .with_priority(1)
        .expect(1)
        .mount(&mock)
        .await;

    // Second request: page 1 now carries the matching run.
    let with_match = json!({
        "total_count": 1,
        "workflow_runs": [
            make_run_json(601, &format!("delayed {needle}"), "abc", now),
        ],
    });
    Mock::given(method("GET"))
        .and(path(RUNS_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(with_match))
        .with_priority(2)
        .expect(1)
        .mount(&mock)
        .await;

    let (client, rate_limit) = build_correlator_deps(&mock.uri()).await;
    let params = default_params(gcit_run_id);
    let cancel = CancellationToken::new();
    let outcome = correlate(&client, &rate_limit, &params, cancel)
        .await
        .expect("second poll cycle must rediscover page 1 with the match");
    assert_eq!(
        outcome.run_id, 601,
        "correlator must restart pagination from page 1 each cycle",
    );
    drop(mock);
}

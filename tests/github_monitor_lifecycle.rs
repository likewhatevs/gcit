// Lifecycle tests for `gcit::github::monitor::monitor_run`.
// Pins:
//   1. Terminal status reached → MonitorEvent::Done emitted, monitor
//      returns MonitorOutcome::Terminated.
//   2. Cancel mid-poll → returns MonitorOutcome::DrainedMidRun before
//      the next event lands on tx.
//   3. Permanent error (401 on get_run) → returns
//      MonitorOutcome::Failed.
//
// Wiremock backs the GitHub side; tokio mpsc captures emitted
// MonitorEvents so the test can assert each transition.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use secrecy::SecretString;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use gcit::config::CredentialId;
use gcit::github::client::Client;
use gcit::github::monitor::{monitor_run, MonitorEvent, MonitorOutcome, MonitorParams};
use gcit::github::rate_limit::RateLimitState;

mod common;

const PAT: &str = "github_pat_test_token_for_wiremock_only_no_real_secret";
const RUN_ID: u64 = 42;
const RUN_PATH: &str = "/repos/myorg/linux-builder/actions/runs/42";
const JOBS_PATH: &str = "/repos/myorg/linux-builder/actions/runs/42/jobs";

fn make_run_json(status: &str, conclusion: Option<&str>, created_at: DateTime<Utc>) -> Value {
    json!({
        "id": RUN_ID,
        "workflow_id": 7,
        "node_id": "MDEwOlJ1bk5vZGUx",
        "name": "ci build",
        "head_branch": "main",
        "head_sha": "deadbeefcafe1234567890abcdef1234567890ab",
        "run_number": RUN_ID,
        "event": "workflow_dispatch",
        "status": status,
        "conclusion": conclusion,
        "created_at": created_at.to_rfc3339(),
        "updated_at": created_at.to_rfc3339(),
        "url": format!("https://api.github.com/repos/myorg/linux-builder/actions/runs/{RUN_ID}"),
        "html_url": format!("https://github.com/myorg/linux-builder/actions/runs/{RUN_ID}"),
        "jobs_url": format!("https://api.github.com/repos/myorg/linux-builder/actions/runs/{RUN_ID}/jobs"),
        "logs_url": format!("https://api.github.com/repos/myorg/linux-builder/actions/runs/{RUN_ID}/logs"),
        "check_suite_url": "https://api.github.com/repos/myorg/linux-builder/check-suites/1",
        "artifacts_url": format!("https://api.github.com/repos/myorg/linux-builder/actions/runs/{RUN_ID}/artifacts"),
        "cancel_url": format!("https://api.github.com/repos/myorg/linux-builder/actions/runs/{RUN_ID}/cancel"),
        "rerun_url": format!("https://api.github.com/repos/myorg/linux-builder/actions/runs/{RUN_ID}/rerun"),
        "workflow_url": "https://api.github.com/repos/myorg/linux-builder/actions/workflows/7",
        "head_commit": {
            "id": "deadbeefcafe1234567890abcdef1234567890ab",
            "tree_id": "deadbeefcafe1234567890abcdef1234567890ab",
            "message": "test commit",
            "timestamp": created_at.to_rfc3339(),
            "author": {"name": "ci"},
            "committer": {"name": "ci"},
        },
        "repository": {
            "id": 1,
            "name": "linux-builder",
            "url": "https://api.github.com/repos/myorg/linux-builder",
        },
    })
}

fn empty_jobs_page() -> Value {
    json!({"total_count": 0, "jobs": []})
}

async fn build_client(mock_uri: &str) -> Arc<Client> {
    Arc::new(
        Client::builder()
            .credential(CredentialId::new("github_pat").expect("valid id"))
            .token(SecretString::from(PAT.to_string()))
            .request_timeout(Duration::from_secs(2))
            .base_uri(mock_uri)
            .build()
            .expect("client build"),
    )
}

async fn build_rate_limit() -> Arc<RateLimitState> {
    let r = Arc::new(RateLimitState::new());
    r.observe_full(5000, 5000, Utc::now() + chrono::Duration::seconds(3600))
        .await;
    r
}

fn monitor_params(job_interval: Duration) -> MonitorParams {
    MonitorParams {
        repo: "myorg/linux-builder".to_string(),
        workflow: "ci.yml".to_string(),
        run_id: RUN_ID,
        job_interval,
    }
}

#[tokio::test]
async fn terminal_status_emits_done_and_terminates() {
    // Run is already Completed/success on the very first poll. The
    // monitor must emit a single MonitorEvent::Done carrying a
    // terminal RunStatus and return Terminated.
    common::ensure_crypto_provider();
    let mock = MockServer::start().await;
    let now = Utc::now();
    Mock::given(method("GET"))
        .and(path(RUN_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(make_run_json(
            "completed",
            Some("success"),
            now,
        )))
        .mount(&mock)
        .await;
    Mock::given(method("GET"))
        .and(path(JOBS_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(empty_jobs_page()))
        .mount(&mock)
        .await;

    let client = build_client(&mock.uri()).await;
    let rate_limit = build_rate_limit().await;
    let (tx, mut rx) = mpsc::channel::<MonitorEvent>(8);
    let cancel = CancellationToken::new();

    // job_interval is short so the test does not have to wait the
    // production 30s default. The interval gates SUBSEQUENT polls;
    // the first tick fires immediately so a terminal first-poll
    // resolves in milliseconds.
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        monitor_run(
            client,
            rate_limit,
            monitor_params(Duration::from_millis(50)),
            tx,
            cancel,
        ),
    )
    .await
    .expect("monitor_run must complete within 10s");

    assert!(
        matches!(outcome, MonitorOutcome::Terminated),
        "outcome must be Terminated; got {outcome:?}",
    );

    // Exactly one event: Done.
    let first = rx.recv().await.expect("Done event must be emitted");
    match first {
        MonitorEvent::Done { summary } => {
            assert!(
                summary.status.is_terminal(),
                "Done summary must carry a terminal status; got {:?}",
                summary.status,
            );
            assert_eq!(summary.run_id, RUN_ID);
        }
        MonitorEvent::Update { .. } => panic!("first event must be Done, not Update"),
    }
    // No further events.
    assert!(rx.try_recv().is_err(), "monitor must not emit after Done");
}

#[tokio::test]
async fn cancel_mid_poll_returns_drained_mid_run() {
    // Run is in_progress (non-terminal). Cancel fires before the
    // next interval tick — monitor's select! arm picks cancel and
    // returns DrainedMidRun. The first poll DOES emit an Update
    // event before the loop gets back to the cancel-aware select.
    common::ensure_crypto_provider();
    let mock = MockServer::start().await;
    let now = Utc::now();
    Mock::given(method("GET"))
        .and(path(RUN_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(make_run_json(
            "in_progress",
            None,
            now,
        )))
        .mount(&mock)
        .await;
    Mock::given(method("GET"))
        .and(path(JOBS_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(empty_jobs_page()))
        .mount(&mock)
        .await;

    let client = build_client(&mock.uri()).await;
    let rate_limit = build_rate_limit().await;
    let (tx, mut rx) = mpsc::channel::<MonitorEvent>(8);
    let cancel = CancellationToken::new();

    // job_interval = 5s so the first poll fires immediately and the
    // monitor lands on the cancel-aware select.tick() before the
    // next tick. Fire cancel after a short delay.
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        cancel_clone.cancel();
    });

    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        monitor_run(
            client,
            rate_limit,
            monitor_params(Duration::from_secs(5)),
            tx,
            cancel,
        ),
    )
    .await
    .expect("monitor_run must complete within 10s");

    assert!(
        matches!(outcome, MonitorOutcome::DrainedMidRun),
        "outcome must be DrainedMidRun; got {outcome:?}",
    );

    // Exactly one Update event, then nothing further (the cancel
    // arm fired before the next tick produced another poll).
    let first = rx.recv().await.expect("first Update event must land");
    assert!(
        matches!(first, MonitorEvent::Update { .. }),
        "first event must be Update for in_progress run",
    );
}

#[tokio::test]
async fn permanent_error_401_returns_failed_no_events() {
    // get_run returns 401 (Unauthorized). The monitor's
    // permanent-vs-transient classification short-circuits and the
    // task returns MonitorOutcome::Failed without emitting any
    // events.
    common::ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(RUN_PATH))
        .respond_with(
            ResponseTemplate::new(401).set_body_json(json!({"message": "Bad credentials"})),
        )
        .mount(&mock)
        .await;

    let client = build_client(&mock.uri()).await;
    let rate_limit = build_rate_limit().await;
    let (tx, mut rx) = mpsc::channel::<MonitorEvent>(8);
    let cancel = CancellationToken::new();

    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        monitor_run(
            client,
            rate_limit,
            monitor_params(Duration::from_millis(50)),
            tx,
            cancel,
        ),
    )
    .await
    .expect("monitor_run must complete within 10s");

    match outcome {
        MonitorOutcome::Failed { error } => {
            // The classifier maps 401 → GithubErrorKind::Unauthorized.
            assert!(
                !error.is_transient(),
                "401 must classify as Permanent; got {error:?}",
            );
        }
        other => panic!("outcome must be Failed; got {other:?}"),
    }
    assert!(
        rx.try_recv().is_err(),
        "permanent error must NOT emit MonitorEvent",
    );
}

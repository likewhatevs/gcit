// Cancel arm in `DiscordNotifier::on_run_complete`.
//
// Production at src/discord/notifier.rs::on_run_complete wraps
// `self.deliver(embed)` in a `tokio::select! { biased; ... }` arm
// that races `cancel.cancelled()` against the deliver future.
// When cancel fires while a request is in flight:
//   - The select arm returns `Err(NotifyError::Transient {
//     source: anyhow!("cancelled before discord webhook {} delivered",
//     self.parsed.id.get()), retry_after: None })`.
//   - The deliver future is dropped at the select boundary;
//     dropping the future cancels the in-flight reqwest request as
//     a side effect (futures cancel on drop), so the underlying
//     TCP/TLS work unwinds cleanly.
//
// Without this arm, the notifier would wait the full per-client
// `request_timeout` (30s, set in supervisor::build_notifiers)
// before observing SIGTERM. The arm makes cancel propagation
// tight enough that supervisor drain stays bounded.
//
// This test pins that contract by:
//   1. Wiremock that delays its 204 response for longer than the
//      test bound (response_delay >> cancel_delay).
//   2. Spawning a task that fires cancel.cancel() after a short
//      window.
//   3. Asserting on_run_complete returns Transient("cancelled")
//      well before the wiremock delay would have completed.
//
// Wiremock API: ResponseTemplate::set_delay(Duration) at
// wiremock-0.6.5/src/response_template.rs:306. The delay is
// applied between Mock matching and response delivery, so the
// underlying reqwest request is sent and the server starts
// holding open the response — exactly the in-flight-request
// shape the cancel arm is meant to interrupt.

use std::sync::Arc;
use std::time::{Duration, Instant};

use handlebars::Handlebars;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

use gcit::config::{DiscordTemplateConfig, FireEvent};
use gcit::discord::webhook::{parse_webhook_url, Client};
use gcit::discord::DiscordNotifier;
use gcit::github::{Conclusion, JobResult, RunStatus, RunSummary};
use gcit::notify::{strict_handlebars, ActionInfo, Notifier, NotifyError, RunContext, SourceInfo};

mod common;

const WEBHOOK_ID: u64 = 1234567890;
const WEBHOOK_TOKEN: &str = "testtokensecret";

async fn build_notifier(mock: &MockServer) -> DiscordNotifier {
    common::ensure_crypto_provider();
    let client = Client::for_test(mock.uri(), Duration::from_secs(60)).expect("for_test build");
    let parsed = parse_webhook_url(&format!(
        "https://discord.com/api/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}"
    ))
    .expect("parse");
    let handlebars: Arc<Handlebars<'static>> = Arc::new(strict_handlebars());
    DiscordNotifier::new(
        "test-discord-cancel",
        client,
        parsed,
        vec![FireEvent::RunComplete],
        DiscordTemplateConfig::default(),
        handlebars,
    )
}

#[tokio::test]
async fn cancel_during_inflight_webhook_returns_transient() {
    // Wiremock delays its 204 response for 10s. Cancel fires at
    // 200ms. The cancel arm in on_run_complete must surface
    // Transient with "cancelled" in the source message well
    // before the 10s delay would complete.
    //
    // The Client's request_timeout is set to 60s in build_notifier
    // — far above both the cancel deadline (200ms) and the
    // wiremock delay (10s) — so a regression that drops the
    // cancel arm would hang for the full 10s wiremock delay (the
    // 204 would arrive first, so the request would surface as
    // Sent, not Transient). Either way, the test catches a
    // missing cancel arm: missing arm → Sent or 60s wait.
    //
    // Mutation target: dropping the tokio::select!
    // cancel arm — the test would see Sent at ~10s (from the
    // delayed 204) or hit the 2s assertion bound.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path_regex(format!(
            r"^/api/v\d+/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}$",
        )))
        .respond_with(ResponseTemplate::new(204).set_delay(Duration::from_secs(10)))
        .mount(&mock)
        .await;

    let notifier = build_notifier(&mock).await;
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        cancel_clone.cancel();
    });

    let started = Instant::now();
    let result = notifier
        .on_run_complete(&run_ctx(), &run_summary(Conclusion::Success, 0), &cancel)
        .await;
    let elapsed = started.elapsed();

    match result {
        Err(NotifyError::Transient { source, retry_after }) => {
            let msg = source.to_string();
            assert!(
                msg.contains("cancelled"),
                "cancel-aware Transient must carry 'cancelled' in source; got: {msg}",
            );
            assert!(
                msg.contains(&WEBHOOK_ID.to_string()),
                "cancel message must name the webhook id ({WEBHOOK_ID}) for operator triage; got: {msg}",
            );
            assert!(
                retry_after.is_none(),
                "cancel Transient should not carry a retry_after hint; got {retry_after:?}",
            );
        }
        other => panic!(
            "cancel during in-flight webhook must surface Transient; got {other:?} after {elapsed:?}",
        ),
    }

    // The cancel arm fired at 200ms; the notifier must return
    // well before the wiremock 10s delay or the client's 60s
    // request_timeout would have been reached. 1s is the budget:
    // a regression that bypasses the cancel arm would either hit
    // the 10s delay (surfacing as Sent) or block on the 60s
    // timeout — both far above 1s. The cancel arm typically
    // returns within ~200ms (fires at cancel.cancel() time);
    // 1s leaves headroom for scheduler jitter under concurrent
    // suite load.
    assert!(
        elapsed < Duration::from_secs(1),
        "cancel must short-circuit the in-flight HTTP request well below 1s; elapsed {elapsed:?}",
    );

    // Wiremock recorded the request was sent — the client
    // started the round-trip before cancel fired. This
    // distinguishes "cancel pre-empted before send" (which is
    // the early-exit path, not what we're testing) from "cancel
    // pre-empted in-flight" (the select! arm).
    let received = mock.received_requests().await.expect("recording on");
    assert_eq!(
        received.len(),
        1,
        "exactly one POST should have been sent before cancel fired; got {}",
        received.len(),
    );
}

// ---- helpers --------------------------------------------------

fn run_ctx() -> RunContext {
    RunContext {
        flow_name: "ci-flow".into(),
        flow_description: None,
        source: SourceInfo {
            url: "https://example.com/repo.git".into(),
            ref_name: "refs/heads/main".into(),
            sha: gix_hash::ObjectId::null(gix_hash::Kind::Sha1),
            sha_short: "0000000".into(),
        },
        action: ActionInfo {
            repo: "owner/repo".into(),
            workflow: "ci.yml".into(),
            run_id: 42,
            run_url: "https://github.com/owner/repo/actions/runs/42".into(),
            dispatched_at: chrono::Utc::now(),
        },
        gcit_run_id: uuid::Uuid::nil(),
    }
}

fn run_summary(conclusion: Conclusion, jobs: usize) -> RunSummary {
    RunSummary {
        run_id: 42,
        run_url: "https://github.com/owner/repo/actions/runs/42".into(),
        run_number: 7,
        run_attempt: 1,
        status: RunStatus::Completed,
        conclusion: Some(conclusion),
        started_at: Some(chrono::Utc::now()),
        completed_at: Some(chrono::Utc::now()),
        jobs: (0..jobs)
            .map(|i| JobResult {
                job_id: 100 + i as u64,
                name: format!("job-{i}"),
                html_url: format!("https://github.com/owner/repo/actions/jobs/{i}"),
                conclusion: Some(Conclusion::Failure),
                started_at: Some(chrono::Utc::now()),
                completed_at: Some(chrono::Utc::now()),
                steps: Vec::new(),
                run_attempt: 1,
            })
            .collect(),
    }
}

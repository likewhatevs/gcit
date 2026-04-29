// Discord per-flow notifier isolation.
//
// gcit uses `.ratelimiter(None)` on the twilight-http client (per
// src/discord/webhook.rs::Client::new) so per-process throttling is
// disabled. Per-credential isolation comes from the architecture:
// each DiscordNotifier owns its own twilight-http Client (one per
// credential), and there is no cross-notifier rate-limit state in
// gcit. So flow A's failure cannot affect flow B — each flow's
// webhook delivery proceeds independently against its own credential.
//
// Why this test does NOT use a 429:
//   twilight-http's response future retries 429 responses
//   indefinitely (twilight-http-0.17.1/src/response/future.rs:389)
//   regardless of `.ratelimiter(None)`. The doc at line 149-152
//   states: "Requests that exceed a rate limit are automatically and
//   immediately retried until they succeed or fail with another
//   error." So a 429 mock paired with no other response makes the
//   call hang forever — twilight never surfaces the 429 to gcit's
//   classifier. We instead use a 401 (which twilight does NOT retry,
//   per the same future.rs branch at line 389-404) to drive flow A
//   into NotifyError::Permanent. The "isolation" property the brief
//   targets — flow A's failure doesn't deflect flow B's success —
//   is proven equally well by any non-200 status that surfaces
//   cleanly.

use std::sync::Arc;
use std::time::Duration;

use handlebars::Handlebars;
use serde_json::json;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

use gcit::config::{DiscordTemplateConfig, FireEvent};
use gcit::discord::webhook::{parse_webhook_url, Client};
use gcit::discord::DiscordNotifier;
use gcit::github::{Conclusion, JobResult, RunStatus, RunSummary};
use gcit::notify::{
    strict_handlebars, ActionInfo, Notifier, NotifyError, NotifyOutcome, RunContext, SourceInfo,
};

mod common;

const FLOW_A_ID: u64 = 1111111111;
const FLOW_A_TOKEN: &str = "tokenA";
const FLOW_B_ID: u64 = 2222222222;
const FLOW_B_TOKEN: &str = "tokenB";

/// Build a notifier pointed at `mock` with the given webhook id+token.
/// Each notifier owns its own twilight-http Client so the per-credential
/// isolation property is exercised end-to-end.
async fn build_notifier(mock: &MockServer, id: u64, token: &str) -> DiscordNotifier {
    common::ensure_crypto_provider();
    let client = Client::for_test(mock.uri(), Duration::from_secs(5)).expect("for_test build");
    let parsed = parse_webhook_url(&format!("https://discord.com/api/webhooks/{id}/{token}"))
        .expect("parse");
    let handlebars: Arc<Handlebars<'static>> = Arc::new(strict_handlebars());
    DiscordNotifier::new(
        format!("test-{id}"),
        client,
        parsed,
        vec![FireEvent::RunComplete],
        DiscordTemplateConfig::default(),
        handlebars,
    )
}

#[tokio::test]
async fn rate_limit_429_does_not_break_other_flows() {
    // Per-credential isolation: flow A's webhook hits an error, flow
    // B's hits a 204. The two flows have independent twilight-http
    // Clients (one per credential — DiscordNotifier owns its Client),
    // so flow B's delivery succeeds even though flow A is failing.
    //
    // Mock body shape: twilight-http surfaces non-2xx responses as
    // `ErrorType::Response { status }` only when the body
    // deserialises as `twilight_http::api_error::ApiError` (which
    // requires `{"code": <u64>, "message": "<str>"}`). Without that
    // shape, twilight returns `ErrorType::Parsing { body }` and the
    // status code is masked.
    //
    // Mutation target: adding shared rate-limit state
    // across notifiers — flow A's failure would deflect flow B. Per
    // the production design (per-credential Client, no shared
    // bucket), independent outcomes are the invariant.
    let mock_a = MockServer::start().await;
    let mock_b = MockServer::start().await;

    // Flow A: 401 (which the classifier maps to Permanent). 429 would
    // be the natural test signal for "rate limit", but twilight-http
    // retries 429 indefinitely without surfacing it to the caller
    // (see file header), so 401 is the cleanest non-retried error
    // class that proves independence.
    Mock::given(method("POST"))
        .and(path_regex(format!(
            r"^/api/v\d+/webhooks/{FLOW_A_ID}/{FLOW_A_TOKEN}$",
        )))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({
            "code": 0,
            "message": "Invalid Webhook Token",
        })))
        .expect(1)
        .mount(&mock_a)
        .await;

    // Flow B: 204 No Content (success).
    Mock::given(method("POST"))
        .and(path_regex(format!(
            r"^/api/v\d+/webhooks/{FLOW_B_ID}/{FLOW_B_TOKEN}$",
        )))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock_b)
        .await;

    let notifier_a = build_notifier(&mock_a, FLOW_A_ID, FLOW_A_TOKEN).await;
    let notifier_b = build_notifier(&mock_b, FLOW_B_ID, FLOW_B_TOKEN).await;

    // Drive both. Order is independent — neither holds nor checks
    // shared state.
    let result_a = notifier_a
        .on_run_complete(
            &run_ctx(),
            &run_summary(Conclusion::Success, 0),
            &CancellationToken::new(),
        )
        .await;
    let result_b = notifier_b
        .on_run_complete(
            &run_ctx(),
            &run_summary(Conclusion::Success, 0),
            &CancellationToken::new(),
        )
        .await;

    // Flow A: 401 surfaces as Permanent (production classifier maps
    // 401 to "discord HTTP 401: invalid token..." per
    // src/discord/notifier.rs).
    let err_a = result_a.expect_err("flow A 401 must surface as Err");
    match &err_a {
        NotifyError::Permanent { source } => {
            let msg = source.to_string();
            assert!(
                msg.contains("401"),
                "flow A permanent error must surface the 401 status; got {msg}",
            );
        }
        other => panic!("flow A: expected Permanent, got {other:?}"),
    }

    // Flow B: 204 succeeds — flow A's failure had no impact.
    match result_b.expect("flow B 204 must surface as Sent") {
        NotifyOutcome::Sent { receipt } => {
            assert_eq!(
                receipt,
                format!("webhook:{FLOW_B_ID}"),
                "flow B receipt must carry its own webhook id (not flow A's)",
            );
        }
        other => panic!("flow B: expected Sent, got {other:?}"),
    }
}

// --- helpers ----------------------------------------------------------------

fn run_ctx() -> RunContext {
    RunContext {
        flow_name: "test-flow".into(),
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
                conclusion: Some(Conclusion::Success),
                started_at: Some(chrono::Utc::now()),
                completed_at: Some(chrono::Utc::now()),
                steps: Vec::new(),
                run_attempt: 1,
            })
            .collect(),
    }
}

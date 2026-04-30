// twilight-http execute_webhook with typed Embed.
//
// Pipeline (`DiscordNotifier::on_run_complete`):
//   1. Build Embed via `embed::build_run_complete_embed`.
//   2. Validate via twilight-validate (defense-in-depth — pre-truncation
//      makes per-field check unconditionally pass).
//   3. Aggregate codepoint cap check (gcit-side, not twilight).
//   4. Call `Client.execute_webhook(id, &token).embeds(&[embed]).await`.
//   5. Map twilight-http errors per status:
//        * 401/403/404/410         → Permanent (token revoked, etc.)
//        * 429                     → Transient with retry_after
//        * 5xx / Hyper / Timeout   → Transient
//        * Validation              → Permanent (config bug)
//
// Wire shape (per Discord webhook docs + twilight-http):
//   POST /api/v<API_VERSION>/webhooks/<id>/<token>
//   Body: {"embeds": [{...}]} (no "content" key)
//   204 No Content on success without ?wait=true.
//
// Test strategy: wiremock + twilight-http `Client::for_test(host, timeout)`
// which sets `.proxy(host, /* use_http */ true)` — twilight composes the
// final URL as `http://<host>/api/v<V>/<path>`, so we must pass HOST:PORT
// only (scheme stripped from `mock.uri()`).

use std::sync::Arc;
use std::time::Duration;

use handlebars::Handlebars;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{body_partial_json, method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

use gcit::config::{DiscordTemplateConfig, FireEvent};
use gcit::discord::webhook::{parse_webhook_url, Client};
use gcit::discord::DiscordNotifier;
use gcit::github::{Conclusion, JobResult, RunStatus, RunSummary};
use gcit::notify::{
    strict_handlebars, ActionInfo, Notifier, NotifyError, NotifyOutcome, RunContext, SourceInfo,
};

mod common;

const WEBHOOK_ID: u64 = 1234567890;
const WEBHOOK_TOKEN: &str = "testtokensecret";

/// Build a `DiscordNotifier` pointed at `mock`. Tests invoke the
/// production `Notifier::on_run_complete` directly, so the deliver +
/// classify path runs end-to-end against wiremock. The `Notifier`
/// trait uses native async-fn-in-trait (not dyn-safe), so we hand
/// back the concrete type rather than a trait object.
async fn build_notifier(mock: &MockServer) -> DiscordNotifier {
    common::ensure_crypto_provider();
    let client = Client::for_test(mock.uri(), Duration::from_secs(5)).expect("for_test build");
    let parsed = parse_webhook_url(&format!(
        "https://discord.com/api/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}"
    ))
    .expect("parse");
    let handlebars: Arc<Handlebars<'static>> = Arc::new(strict_handlebars());
    DiscordNotifier::new(
        "test-discord",
        client,
        parsed,
        vec![FireEvent::RunComplete],
        DiscordTemplateConfig::default(),
        handlebars,
    )
}

#[tokio::test]
async fn execute_webhook_posts_to_correct_url_path() {
    // twilight-http composes the webhook POST URL as
    // `/api/v<API_VERSION>/webhooks/<id>/<token>`. The API_VERSION
    // segment is twilight's affordance (currently v10) and may bump
    // with twilight upgrades; we match it via path_regex to assert
    // the trailing `/webhooks/<id>/<token>` shape without coupling
    // to a specific API version. Wiremock's expect(1) confirms one
    // dispatch lands on the configured webhook id+token, ruling out
    // a hand-rolled URL builder that misses encoding.
    //
    // Mutation target: constructing the URL by string
    // concat — the typed builder is what guarantees correct encoding.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path_regex(format!(
            r"^/api/v\d+/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}$",
        )))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let notifier = build_notifier(&mock).await;
    notifier
        .on_run_complete(
            &run_ctx(),
            &run_summary(Conclusion::Success, 0),
            &CancellationToken::new(),
        )
        .await
        .expect("204 must surface as Sent");
    // Implicit verification: drop on the MockServer at end-of-test
    // checks expect(1) holds.
}

#[tokio::test]
async fn execute_webhook_body_carries_typed_embed_not_string() {
    // The wire body MUST be `{"embeds": [{...}]}` — twilight-model's
    // serde derives produce camelCase Discord field names. We assert
    // via two layers: (1) body_partial_json gates the matcher on the
    // shape, (2) received_requests inspection pulls the body and
    // confirms `embeds` is a JSON array carrying an object (not a
    // string-encoded blob).
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path_regex(format!(
            r"^/api/v\d+/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}$",
        )))
        .and(body_partial_json(json!({
            "embeds": [{}]
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let notifier = build_notifier(&mock).await;
    notifier
        .on_run_complete(
            &run_ctx(),
            &run_summary(Conclusion::Success, 0),
            &CancellationToken::new(),
        )
        .await
        .expect("204 must surface as Sent");

    let received = mock.received_requests().await.expect("recording on");
    assert_eq!(received.len(), 1);
    let body: Value = serde_json::from_slice(&received[0].body).expect("valid JSON body");
    let embeds = body
        .get("embeds")
        .and_then(Value::as_array)
        .expect("body has embeds array");
    assert_eq!(embeds.len(), 1, "exactly one embed per run-complete");
    assert!(
        embeds[0].is_object(),
        "embed must be a typed object, not a string-encoded blob; got {:?}",
        embeds[0],
    );
}

#[tokio::test]
async fn execute_webhook_no_content_field_when_only_embed() {
    // gcit only sets `embeds`, never `content`. twilight-http's serde
    // derives skip None fields, so the wire body must NOT contain
    // either `"content": ""` (which Discord rejects with 400 "Cannot
    // send an empty message") or `"content": null`. Inspect the raw
    // body to assert the key is absent entirely.
    //
    // Mutation target: setting content = Some("") which
    // Discord rejects.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path_regex(format!(
            r"^/api/v\d+/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}$",
        )))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let notifier = build_notifier(&mock).await;
    notifier
        .on_run_complete(
            &run_ctx(),
            &run_summary(Conclusion::Success, 0),
            &CancellationToken::new(),
        )
        .await
        .expect("204 must surface as Sent");

    let received = mock.received_requests().await.expect("recording on");
    let body: Value = serde_json::from_slice(&received[0].body).expect("valid JSON body");
    let obj = body.as_object().expect("body is object");
    assert!(
        !obj.contains_key("content"),
        "wire body must not carry a `content` key (would trigger Discord 400 on empty); body keys: {:?}",
        obj.keys().collect::<Vec<_>>(),
    );
}

#[tokio::test]
async fn execute_webhook_204_treated_as_success() {
    // Default Discord webhook response (without ?wait=true) is 204
    // No Content. The notifier maps 2xx to NotifyOutcome::Sent with
    // a `webhook:<id>` receipt format (per discord::notifier::deliver).
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path_regex(format!(
            r"^/api/v\d+/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}$",
        )))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let notifier = build_notifier(&mock).await;
    let outcome = notifier
        .on_run_complete(
            &run_ctx(),
            &run_summary(Conclusion::Success, 0),
            &CancellationToken::new(),
        )
        .await
        .expect("204 must surface as Sent");
    match outcome {
        NotifyOutcome::Sent { receipt } => {
            assert_eq!(
                receipt,
                format!("webhook:{WEBHOOK_ID}"),
                "receipt must carry the webhook id",
            );
        }
        other => panic!("expected Sent, got {other:?}"),
    }
}

#[tokio::test]
async fn execute_webhook_5xx_returns_transient_error() {
    // Discord 500 must classify as NotifyError::Transient so upstream
    // backoff retries. The mock body must be a Discord-shaped
    // `{"code": N, "message": "..."}` so twilight-http deserialises
    // it as `api_error::GeneralApiError` and surfaces an
    // `ErrorType::Response { status }` — our classifier's 5xx arm
    // then maps it to Transient. Without a JSON body twilight
    // surfaces `ErrorType::Parsing { body }` which collapses through
    // the catch-all transport-error arm, masking the status code.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path_regex(format!(
            r"^/api/v\d+/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}$",
        )))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({
            "code": 0,
            "message": "internal server error",
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let notifier = build_notifier(&mock).await;
    let err = notifier
        .on_run_complete(
            &run_ctx(),
            &run_summary(Conclusion::Success, 0),
            &CancellationToken::new(),
        )
        .await
        .expect_err("500 must surface as Err");
    match &err {
        NotifyError::Transient { source, .. } => {
            let msg = source.to_string();
            // Production classifier emits "discord HTTP 500 ..." for
            // 5xx (see discord::notifier::classify_twilight_error).
            assert!(
                msg.contains("500"),
                "transient error must surface the 500 status; got {msg}",
            );
        }
        other => panic!("expected Transient, got {other:?}"),
    }
}

#[tokio::test]
async fn execute_webhook_4xx_non_429_returns_permanent_error() {
    // 401, 403, 404, 410 each map to NotifyError::Permanent with a
    // distinct, operator-actionable message. The production
    // classifier in discord::notifier assigns specific phrases
    // ("invalid token", "forbidden", "webhook deleted", "channel
    // closed"); this integration test pins the wiring + the message
    // distinguishability for each 4xx code.
    //
    // Mutation target: wrapping everything in Transient and
    // gcit retries forever against a deleted webhook.
    for (status_code, expected_msg_substring) in [
        (401_u16, "invalid token"),
        (403, "forbidden"),
        (404, "webhook deleted"),
        (410, "channel closed"),
    ] {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path_regex(format!(
                r"^/api/v\d+/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}$",
            )))
            .respond_with(ResponseTemplate::new(status_code).set_body_json(json!({
                "code": 0,
                "message": format!("HTTP {status_code}"),
            })))
            .expect(1)
            .mount(&mock)
            .await;

        let notifier = build_notifier(&mock).await;
        let err = notifier
            .on_run_complete(
                &run_ctx(),
                &run_summary(Conclusion::Success, 0),
                &CancellationToken::new(),
            )
            .await
            .expect_err(&format!("status {status_code}: must surface as Err",));
        match &err {
            NotifyError::Permanent { source } => {
                let msg = source.to_string();
                assert!(
                    msg.contains(&status_code.to_string()),
                    "status {status_code}: error must surface the status code; got {msg}",
                );
                assert!(
                    msg.contains(expected_msg_substring),
                    "status {status_code}: error must contain {expected_msg_substring:?}; got {msg}",
                );
            }
            other => panic!("status {status_code}: expected Permanent, got {other:?}",),
        }
    }
}

/// Build a `DiscordNotifier` with a tight per-request timeout so the
/// caller can drive the RequestTimedOut classification arm. Mirrors
/// `build_notifier` but takes the timeout as a parameter — production
/// `build_notifier` uses 5s which is too long for a deterministic
/// timeout test against a wiremock with a few-second delay.
async fn build_notifier_with_timeout(
    mock: &MockServer,
    request_timeout: Duration,
) -> DiscordNotifier {
    common::ensure_crypto_provider();
    let client = Client::for_test(mock.uri(), request_timeout).expect("for_test build");
    let parsed = parse_webhook_url(&format!(
        "https://discord.com/api/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}"
    ))
    .expect("parse");
    let handlebars: Arc<Handlebars<'static>> = Arc::new(strict_handlebars());
    DiscordNotifier::new(
        "test-discord-timeout",
        client,
        parsed,
        vec![FireEvent::RunComplete],
        DiscordTemplateConfig::default(),
        handlebars,
    )
}

#[tokio::test]
async fn execute_webhook_request_timeout_classified_as_transient() {
    // Production classify_twilight_error maps ErrorType::RequestTimedOut
    // to NotifyError::Transient with the message "discord request
    // timed out:". The integration tests above cover Response{status}
    // variants; this test covers the RequestTimedOut variant by
    // setting the client's per-request timeout BELOW the wiremock
    // response delay so the round-trip surfaces a TimedOut error
    // rather than a status response.
    //
    // 200ms request_timeout vs 5s mock delay — the gap is large
    // enough that the timeout arm always fires deterministically,
    // even on a heavily-loaded CI runner.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path_regex(format!(
            r"^/api/v\d+/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}$",
        )))
        .respond_with(ResponseTemplate::new(204).set_delay(Duration::from_secs(5)))
        .mount(&mock)
        .await;

    let notifier = build_notifier_with_timeout(&mock, Duration::from_millis(200)).await;
    let started = std::time::Instant::now();
    let err = notifier
        .on_run_complete(
            &run_ctx(),
            &run_summary(Conclusion::Success, 0),
            &CancellationToken::new(),
        )
        .await
        .expect_err("client timeout must surface as Err");
    let elapsed = started.elapsed();

    match &err {
        NotifyError::Transient {
            source,
            retry_after,
        } => {
            let msg = source.to_string();
            assert!(
                msg.contains("timed out") || msg.contains("timeout"),
                "transient timeout must mention 'timed out' or 'timeout'; got: {msg}",
            );
            assert!(
                retry_after.is_none(),
                "RequestTimedOut classification must leave retry_after None; got {retry_after:?}",
            );
        }
        other => panic!("RequestTimedOut must classify as Transient; got {other:?}"),
    }
    // Sanity: the error surfaced well before the 5s mock delay.
    assert!(
        elapsed < Duration::from_secs(2),
        "timeout must fire before the mock delay; elapsed {elapsed:?}",
    );
}

#[tokio::test]
async fn execute_webhook_other_4xx_returns_permanent_error_via_catch_all_arm() {
    // Production classify_twilight_error maps any 4xx code NOT in
    // {401, 403, 404, 410, 429} to a generic Permanent error with
    // "discord HTTP {code}" in the message. The existing 4xx test
    // only exercises the enumerated codes; this test covers the
    // catch-all Permanent fallback arm. 422 is the canonical "your
    // request was malformed in some way Discord didn't hardcode" code
    // that would land here in practice.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path_regex(format!(
            r"^/api/v\d+/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}$",
        )))
        .respond_with(ResponseTemplate::new(422).set_body_json(json!({
            "code": 50035,
            "message": "Invalid Form Body",
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let notifier = build_notifier(&mock).await;
    let err = notifier
        .on_run_complete(
            &run_ctx(),
            &run_summary(Conclusion::Success, 0),
            &CancellationToken::new(),
        )
        .await
        .expect_err("422 must surface as Err");
    match &err {
        NotifyError::Permanent { source } => {
            let msg = source.to_string();
            assert!(
                msg.contains("422"),
                "permanent 4xx error must surface the status code; got: {msg}",
            );
        }
        other => panic!("422 must classify as Permanent (catch-all 4xx arm); got {other:?}"),
    }
}

#[tokio::test]
async fn execute_webhook_unreachable_host_classified_as_transient_transport_error() {
    // Production classify_twilight_error maps every
    // non-Response/non-Validation/non-RequestTimedOut error type
    // (Hyper, Parsing, network, cancellation) to NotifyError::Transient
    // with "discord transport error:" in the message body. Drive the
    // catch-all arm by pointing the client at a guaranteed-unreachable
    // address — port 1 is reserved on every modern OS, and
    // 127.0.0.1:1 produces an ECONNREFUSED at the OS level which
    // surfaces through twilight-http as a hyper-shaped error type,
    // NOT Response/Validation/Timeout.
    common::ensure_crypto_provider();
    let client = Client::for_test("127.0.0.1:1".to_string(), Duration::from_secs(2))
        .expect("for_test build");
    let parsed = parse_webhook_url(&format!(
        "https://discord.com/api/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}"
    ))
    .expect("parse");
    let handlebars: Arc<Handlebars<'static>> = Arc::new(strict_handlebars());
    let notifier = DiscordNotifier::new(
        "test-discord-unreachable",
        client,
        parsed,
        vec![FireEvent::RunComplete],
        DiscordTemplateConfig::default(),
        handlebars,
    );

    let err = notifier
        .on_run_complete(
            &run_ctx(),
            &run_summary(Conclusion::Success, 0),
            &CancellationToken::new(),
        )
        .await
        .expect_err("ECONNREFUSED must surface as Err");
    match &err {
        NotifyError::Transient {
            source,
            retry_after,
        } => {
            let msg = source.to_string();
            // The catch-all arm produces "discord transport error:";
            // a future twilight version that maps connection refused
            // to RequestTimedOut would change the prefix to "discord
            // request timed out:" — both are Transient (the
            // operator-facing classification is correct either way).
            // Pin: the message must mention either 'transport' or
            // 'timeout' so the operator knows the failure was a
            // network-shape error rather than a Discord rejection.
            assert!(
                msg.contains("transport") || msg.contains("timed out") || msg.contains("timeout"),
                "transport-error classification must mention transport/timeout; got: {msg}",
            );
            assert!(
                retry_after.is_none(),
                "transport-error classification must leave retry_after None; got {retry_after:?}",
            );
        }
        other => panic!("ECONNREFUSED must classify as Transient; got {other:?}"),
    }
}

// --- helpers ----------------------------------------------------------------

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

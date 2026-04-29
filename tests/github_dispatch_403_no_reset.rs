// Dispatcher-path 403 with X-RateLimit-Remaining: 0 and NO Reset
// header: must classify as RateLimited (Transient) with a default 60s
// reset window, not Forbidden (Permanent).
//
// Why this needs an integration test rather than only the unit tests in
// src/github/error.rs and src/github/dispatcher.rs:
//   - The unit tests drive `classify_status` and
//     `reclassify_403_via_snapshot` in isolation.
//   - The dispatcher path actually calls `octocrab._post`, lets octocrab
//     parse the response into Error::GitHub, then calls `classify` and
//     pipes through `reclassify_403_via_snapshot`. An end-to-end test
//     pins both halves: the response-header observation (via
//     `RateLimitState::observe_headers`) populates the snapshot with
//     remaining=0 + no reset, and the reclassifier defaults to ~now+60s
//     so the dispatcher's retry loop backs off rather than treating the
//     403 as a permanent scope denial.
//
// Setup mirrors the structure pinned in tests/poll_github_api.rs (the
// other crate-boundary wiremock test): octocrab base_uri override,
// rustls ring provider Once-init, real `gcit::github::client::Client`
// + `RateBucket` + `RateLimitState`.

mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::Utc;
use secrecy::SecretString;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use gcit::config::CredentialId;
use gcit::git::rate_bucket::RateBucket;
use gcit::github::client::Client;
use gcit::github::dispatcher::{dispatch, DispatchParams};
use gcit::github::error::GithubErrorKind;
use gcit::github::rate_limit::RateLimitState;

use common::{ensure_crypto_provider, DISPATCH_PATH, PAT};

#[tokio::test]
async fn dispatch_403_remaining_zero_no_reset_header_classifies_as_rate_limited_with_default_window(
) {
    // wiremock returns 403 carrying X-RateLimit-Remaining: 0 but NO
    // X-RateLimit-Reset. The dispatcher's `dispatch()` calls
    // `client.octocrab()._post`, observes the response headers, then
    // routes the 403 through `reclassify_403_via_snapshot`. Because
    // remaining is 0 and reset is missing from both the snapshot and
    // the error's status context, the reclassifier must default the
    // reset to ~now+60s and surface RateLimited (Transient).
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(
            ResponseTemplate::new(403)
                .insert_header("X-RateLimit-Remaining", "0")
                .insert_header("X-RateLimit-Limit", "5000")
                // Deliberately no X-RateLimit-Reset header.
                .set_body_json(serde_json::json!({
                    "message": "API rate limit exceeded",
                    "documentation_url": "https://docs.github.com/rest"
                })),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let client = Client::builder()
        .credential(CredentialId::new("github_pat").expect("valid id"))
        .token(SecretString::from(PAT.to_string()))
        .request_timeout(Duration::from_secs(5))
        .base_uri(mock.uri())
        .build()
        .expect("client build");
    let rate_bucket = RateBucket::new(Duration::from_millis(0));
    let rate_limit = RateLimitState::new();
    // Seed the snapshot so should_defer returns None and the
    // dispatcher proceeds to fire the request. The seed's reset is
    // intentionally in the past — `observe_headers` will leave it
    // alone (response carries no Reset header), and the
    // reclassify_403_via_snapshot path then sees a stale reset and
    // applies the now+60s default fallback. A future reset would
    // satisfy the `r > now` arm and bypass the default.
    rate_limit
        .observe_full(5000, 5000, Utc::now() - chrono::Duration::seconds(120))
        .await;

    let dispatch_params = DispatchParams {
        repo: "myorg/linux-builder".to_string(),
        workflow: "ci.yml".to_string(),
        ref_name: "refs/heads/main".to_string(),
        gcit_run_id: Uuid::nil(),
        rendered_inputs: BTreeMap::new(),
    };
    let cancel = CancellationToken::new();
    let before = Utc::now();
    let err = dispatch(
        &client,
        &rate_bucket,
        &rate_limit,
        &dispatch_params,
        &cancel,
    )
    .await
    .expect_err("403 must surface as Err");
    let after = Utc::now();
    match err {
        GithubErrorKind::RateLimited { credential, reset } => {
            assert_eq!(credential.as_str(), "github_pat");
            // Default fallback window is 60s out from now (per
            // `reclassify_403_via_snapshot` and `classify_status`'s
            // 403/Remaining=0/no-Reset arm). The seeded snapshot's
            // reset is in the past and the response carries no Reset
            // header, so observe_headers leaves the past reset
            // unchanged and the reclassifier takes the default arm.
            // Allow a wide window because both `before` and `after`
            // straddle the implementation's `Utc::now()` call.
            let lower = before + chrono::Duration::seconds(58);
            let upper = after + chrono::Duration::seconds(62);
            assert!(
                reset >= lower && reset <= upper,
                "reset must be ~now+60s; got {reset} not in [{lower}, {upper}]",
            );
        }
        other => panic!("expected RateLimited (Transient) with default 60s window, got {other:?}"),
    }
}

#[tokio::test]
async fn dispatch_403_remaining_zero_no_reset_classification_is_transient() {
    // Companion assertion to the above — pin retryability so a
    // mutation that flips RateLimited to Permanent is caught.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(
            ResponseTemplate::new(403)
                .insert_header("X-RateLimit-Remaining", "0")
                .set_body_json(serde_json::json!({"message": "API rate limit exceeded"})),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let client = Client::builder()
        .credential(CredentialId::new("github_pat").expect("valid id"))
        .token(SecretString::from(PAT.to_string()))
        .request_timeout(Duration::from_secs(5))
        .base_uri(mock.uri())
        .build()
        .expect("client build");
    let rate_bucket = RateBucket::new(Duration::from_millis(0));
    let rate_limit = RateLimitState::new();
    rate_limit
        .observe_full(5000, 5000, Utc::now() - chrono::Duration::seconds(120))
        .await;

    let dispatch_params = DispatchParams {
        repo: "myorg/linux-builder".to_string(),
        workflow: "ci.yml".to_string(),
        ref_name: "refs/heads/main".to_string(),
        gcit_run_id: Uuid::nil(),
        rendered_inputs: BTreeMap::new(),
    };
    let cancel = CancellationToken::new();
    let err = dispatch(
        &client,
        &rate_bucket,
        &rate_limit,
        &dispatch_params,
        &cancel,
    )
    .await
    .expect_err("403 must surface as Err");
    assert!(
        err.is_transient(),
        "403/Remaining=0/no-Reset must classify as Transient (RateLimited), got {err:?}",
    );
}

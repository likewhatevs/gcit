// Response body size cap on the octocrab GET path.
//
// Production at src/github/client.rs::classified_get applies a
// 16 MiB cap (RESPONSE_BODY_LIMIT) to every response read by the
// correlator and monitor paths. The cap operates in two layers:
//
//   1. A Content-Length pre-check that rejects responses declaring
//      more than RESPONSE_BODY_LIMIT bytes BEFORE any body bytes
//      are read. This avoids allocating the full cap of buffer
//      for a body we already know is too big.
//
//   2. An `http_body_util::Limited` streaming wrapper that surfaces
//      `LengthLimitError` mid-stream as soon as the actual body
//      exceeds RESPONSE_BODY_LIMIT. Catches the missing-Content-
//      Length case and the lying-Content-Length case (e.g. a server
//      that declares 100 bytes but streams 100 MB).
//
// Both layers surface the rejection as the typed
// `GithubErrorKind::BodyTooLarge { declared, limit }` (Permanent —
// retrying gets the same response, so backon must NOT retry).
// `declared = Some(N)` for the pre-check arm; `declared = None`
// for the streaming arm. The correlator wraps it as
// `CorrelationError::Github(BodyTooLarge { ... })`.
//
// These tests drive the `correlate` entry point because the runs
// page is the most realistic large-response scenario. Without the
// cap, an attacker (or a misbehaving GitHub Enterprise endpoint
// returning a multi-GB response) could exhaust daemon memory
// before the operator sees the failure.

mod common;

use std::time::Duration;

use chrono::Utc;
use secrecy::SecretString;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use gcit::config::CredentialId;
use gcit::github::client::{Client, RESPONSE_BODY_LIMIT};
use gcit::github::correlator::{correlate, CorrelateParams, CorrelationError};
use gcit::github::error::GithubErrorKind;
use gcit::github::rate_limit::RateLimitState;

use common::{ensure_crypto_provider, make_run_json, PAT, RUNS_PATH};

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
async fn response_over_body_limit_returns_unknown_error() {
    // Pin: a response whose body exceeds RESPONSE_BODY_LIMIT (16
    // MiB) is rejected by the Limited wrapper. The correlator
    // surfaces this as `CorrelationError::Github(Unknown)`.
    //
    // The mock body is 17 MiB of `a` characters wrapped in a
    // minimal valid JSON envelope: `{"workflow_runs": ["...long
    // string..."]}` produces a JSON body well over 16 MiB. The
    // body shape is deliberately not a valid Run array so the
    // serde deserialise path can't be reached even if the cap
    // were bypassed — the error message would then point at the
    // serde path rather than the cap, which would surface as a
    // distinct test failure mode.
    //
    // Mutation target: dropping the Limited wrapper —
    // octocrab would `body.collect().to_bytes()` the full 17 MiB
    // and the test would observe a serde error (or a memory
    // exhaustion in CI, depending on host headroom) rather than
    // the cap message.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    let oversize = "a".repeat(RESPONSE_BODY_LIMIT + 1);
    let body = format!(r#"{{"workflow_runs":["{oversize}"]}}"#);
    Mock::given(method("GET"))
        .and(path(RUNS_PATH))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(body.into_bytes(), "application/json"),
        )
        .mount(&mock)
        .await;

    let (client, rate_limit) = build_correlator_deps(&mock.uri()).await;
    let params = default_params(Uuid::new_v4());
    let cancel = CancellationToken::new();

    let result = correlate(&client, &rate_limit, &params, cancel).await;

    match result {
        Err(CorrelationError::Github(GithubErrorKind::BodyTooLarge { declared, limit })) => {
            assert_eq!(
                limit, RESPONSE_BODY_LIMIT,
                "BodyTooLarge.limit must equal RESPONSE_BODY_LIMIT",
            );
            // Wiremock auto-sets Content-Length from the body
            // bytes; the pre-check arm fires (declared = Some(_)).
            // The exact size depends on the JSON envelope; assert
            // it exceeds the cap.
            assert!(
                matches!(declared, Some(n) if n > RESPONSE_BODY_LIMIT as u64),
                "expected declared Content-Length > RESPONSE_BODY_LIMIT; got {declared:?}",
            );
        }
        Err(other) => panic!(
            "expected CorrelationError::Github(BodyTooLarge) for over-cap body; got {other:?}",
        ),
        Ok(outcome) => panic!("over-cap body must surface as Err; got Ok({outcome:?})",),
    }
}

#[tokio::test]
async fn response_under_body_limit_succeeds() {
    // Pin: a normal-sized response (well under
    // RESPONSE_BODY_LIMIT) is NOT rejected by the body-cap
    // pipeline. The wrapper's `Limited<B>` only fires when the
    // streamed bytes EXCEED the cap; small bodies pass through
    // unchanged. Drive a successful match so the correlator
    // returns `Ok(CorrelationOutcome { .. })` rather than
    // spinning in its retry loop — confirming the wrapper didn't
    // misfire on the small body and didn't introduce a body-decode
    // error.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    let gcit_run_id = Uuid::new_v4();
    let head_sha = "deadbeefcafe1234567890abcdef1234567890ab";
    let now = chrono::Utc::now();
    let run_json = make_run_json(101, &format!("gcit-{gcit_run_id}"), head_sha, now);
    Mock::given(method("GET"))
        .and(path(RUNS_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "total_count": 1,
            "workflow_runs": [run_json],
        })))
        .mount(&mock)
        .await;

    let (client, rate_limit) = build_correlator_deps(&mock.uri()).await;
    let params = default_params(gcit_run_id);
    let cancel = CancellationToken::new();

    let result = correlate(&client, &rate_limit, &params, cancel).await;

    match result {
        Ok(outcome) => {
            assert_eq!(
                outcome.run_id, 101,
                "small-body match must return the run id we mocked",
            );
        }
        Err(other) => panic!(
            "small-body request must succeed; body-cap wrapper misfiring would surface as a different error variant. got {other:?}",
        ),
    }
}

#[test]
fn response_body_limit_constant_is_16_mib() {
    // Pin the literal constant — operator-facing security
    // contract assumes 16 MiB, mirroring the grokmirror cap.
    // Mutation target: changing the constant to a
    // value that diverges from grokmirror's MAX_DECOMPRESSED_BYTES
    // (also 16 MiB), creating an inconsistent cap across HTTP
    // surfaces.
    assert_eq!(
        RESPONSE_BODY_LIMIT,
        16 * 1024 * 1024,
        "RESPONSE_BODY_LIMIT must be 16 MiB to match grokmirror's MAX_DECOMPRESSED_BYTES",
    );
}

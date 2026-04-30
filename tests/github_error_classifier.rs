// GitHub error classification via octocrab + wiremock.
// 7 GithubErrorKind variants:
//   Unauthorized      <- HTTP 401
//   Forbidden         <- HTTP 403 (NOT rate-limit)
//   WorkflowNotFound  <- HTTP 404 on workflow path
//   DispatchInvalid   <- HTTP 422 with "Unexpected inputs" or similar
//   RateLimited       <- HTTP 403 with X-RateLimit-Remaining: 0
//   ServerError       <- HTTP 5xx
//   Timeout           <- request timeout
//
// Each variant has a specific Display message that the operator sees.
// This integration suite pins the wiring: octocrab `_post` →
// `octocrab::map_github_error` → `classify` → `classify_status` →
// `reclassify_403_via_snapshot` (for 403). The in-module unit tests in
// src/github/error.rs cover `classify_status` in isolation (status →
// variant); these tests cover the path from real-HTTP-response (via
// wiremock) to the typed enum a dispatcher caller sees.
//
// Pure-logic tests (retryability table, redaction inspection,
// non-GitHub error variants) call `classify` / `classify_status` /
// `retryability` directly without wiremock — those properties have no
// HTTP wire dependency.

mod common;

use std::time::Duration;

use chrono::Utc;
use rstest::rstest;
use secrecy::SecretString;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use gcit::config::CredentialId;
use gcit::git::rate_bucket::RateBucket;
use gcit::github::client::Client;
use gcit::github::dispatcher::dispatch;
use gcit::github::error::{
    classify_status, GithubErrorKind, Retryability, StatusContext, SERVER_ERROR_MAX_ATTEMPTS,
};
use gcit::github::rate_limit::RateLimitState;

use common::{default_params, drive_dispatch, ensure_crypto_provider, DISPATCH_PATH, PAT};

#[tokio::test]
async fn http_401_classified_as_unauthorized() {
    // 401 with the canonical "Bad credentials" body must surface as
    // GithubErrorKind::Unauthorized carrying the configured
    // credential id, not as ServerError or a generic Unknown. The
    // Display string includes the operator-facing recovery hint
    // ("Re-issue via GitHub UI") that operators grep for.
    //
    // Mutation target: classifying 401 as ServerError or Timeout.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "message": "Bad credentials",
            "documentation_url": "https://docs.github.com/rest"
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let err = drive_dispatch(&mock).await;
    match &err {
        GithubErrorKind::Unauthorized { credential } => {
            assert_eq!(credential.as_str(), "github_pat");
        }
        other => panic!("expected Unauthorized, got {other:?}"),
    }
    let msg = err.to_string();
    assert!(msg.contains("Re-issue via GitHub UI"), "msg: {msg}");
    assert!(
        msg.contains("'github_pat'"),
        "credential id must surface: {msg}",
    );
    assert_eq!(err.retryability(), Retryability::Permanent);
}

#[tokio::test]
async fn http_403_without_rate_limit_classified_as_forbidden() {
    // 403 + "Resource not accessible by integration" body without
    // X-RateLimit-Remaining: 0 indicates insufficient PAT scope, NOT
    // rate limiting. The dispatcher's reclassify_403_via_snapshot
    // double-checks the rate-limit snapshot — but the snapshot is
    // seeded with remaining=5000 here, so the 403 falls through to
    // Forbidden. Display carries the operator-facing scope hint
    // ('workflow' + 'repo') and the `gh auth status` recovery action.
    //
    // Mutation target: collapsing 403 to RateLimited regardless of
    // headers, or stripping the operator hints from Display.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
            "message": "Resource not accessible by integration",
            "documentation_url": "https://docs.github.com/rest"
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let err = drive_dispatch(&mock).await;
    match &err {
        GithubErrorKind::Forbidden { credential } => {
            assert_eq!(credential.as_str(), "github_pat");
        }
        other => panic!("expected Forbidden, got {other:?}"),
    }
    let msg = err.to_string();
    assert!(msg.contains("'workflow' + 'repo'"), "msg: {msg}");
    assert!(msg.contains("gh auth status"), "msg: {msg}");
    assert_eq!(err.retryability(), Retryability::Permanent);
}

#[tokio::test]
async fn http_403_with_rate_limit_remaining_zero_classified_as_rate_limited() {
    // 403 + X-RateLimit-Remaining: 0 + X-RateLimit-Reset: <future>
    // is the canonical rate-limit shape. Two layers must observe it:
    //   1. RateLimitState::observe_headers refreshes the snapshot
    //      with remaining=0 + reset=future.
    //   2. reclassify_403_via_snapshot maps the resulting Forbidden
    //      to RateLimited carrying the snapshot's reset.
    //
    // The classifier MUST inspect the headers, not just the status
    // body. Mutation target: dropping the snapshot reclassification
    // step.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    let reset = Utc::now() + chrono::Duration::seconds(120);
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(
            ResponseTemplate::new(403)
                .insert_header("X-RateLimit-Remaining", "0")
                .insert_header("X-RateLimit-Limit", "5000")
                .insert_header("X-RateLimit-Reset", reset.timestamp().to_string())
                .set_body_json(serde_json::json!({
                    "message": "API rate limit exceeded",
                    "documentation_url": "https://docs.github.com/rest"
                })),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let err = drive_dispatch(&mock).await;
    match &err {
        GithubErrorKind::RateLimited {
            credential,
            reset: r,
        } => {
            assert_eq!(credential.as_str(), "github_pat");
            // Header is parsed as epoch seconds; sub-second precision
            // is dropped so equality is only safe to the second.
            assert_eq!(r.timestamp(), reset.timestamp());
        }
        other => panic!("expected RateLimited, got {other:?}"),
    }
    let msg = err.to_string();
    assert!(msg.contains("Reset at"), "msg: {msg}");
    assert!(msg.contains("'github_pat'"), "msg: {msg}");
    assert_eq!(err.retryability(), Retryability::Transient);
}

#[tokio::test]
async fn http_404_on_workflow_path_classified_as_workflow_not_found() {
    // 404 on the workflow_dispatch route. WorkflowNotFound carries
    // the (repo, workflow) so the operator-facing Display lists both
    // — they need to know which file to edit. The Display
    // intentionally lists all three causes that produce an identical
    // 404 response (file missing on default branch | workflow YAML
    // lacks `on: workflow_dispatch:` | PAT lacks repo visibility) so
    // the operator can triage without the daemon needing to know
    // which one fired.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "message": "Not Found",
            "documentation_url": "https://docs.github.com/rest"
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let err = drive_dispatch(&mock).await;
    match &err {
        GithubErrorKind::WorkflowNotFound { repo, workflow } => {
            assert_eq!(repo, "myorg/linux-builder");
            assert_eq!(workflow, "ci.yml");
        }
        other => panic!("expected WorkflowNotFound, got {other:?}"),
    }
    let msg = err.to_string();
    assert!(msg.contains("on: workflow_dispatch:"), "msg: {msg}");
    assert!(msg.contains("default branch"), "msg: {msg}");
    assert!(
        msg.contains("myorg/linux-builder/ci.yml"),
        "repo+workflow must surface: {msg}",
    );
    assert_eq!(err.retryability(), Retryability::Permanent);
}

#[tokio::test]
async fn http_422_unprocessable_entity_classified_as_dispatch_invalid() {
    // 422 + "Unexpected inputs" body indicates the gcit_run_id case:
    // the workflow YAML is missing the `inputs.gcit_run_id` declaration
    // or the `run-name` directive. The Display embeds a copy-paste-able
    // YAML snippet; this test pins all five substrings the operator
    // needs (the 2-space-indented YAML scaffold, the type annotation,
    // the run-name directive, and the closing call-to-action).
    //
    // Mutation target: any of the 5 substrings dropped from the
    // template — operators copy-paste the YAML literally, so any
    // missing line breaks the recovery flow.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(ResponseTemplate::new(422).set_body_json(serde_json::json!({
            "message": "Unexpected inputs",
            "documentation_url": "https://docs.github.com/rest"
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let err = drive_dispatch(&mock).await;
    match &err {
        GithubErrorKind::DispatchInvalid { workflow } => {
            assert_eq!(workflow, "ci.yml");
        }
        other => panic!("expected DispatchInvalid, got {other:?}"),
    }
    let msg = err.to_string();
    // 1. workflow filename in the message
    assert!(msg.contains("ci.yml"), "msg: {msg}");
    // 2. canonical YAML scaffold operators paste in
    assert!(msg.contains("on:\n  workflow_dispatch:"), "msg: {msg}");
    assert!(
        msg.contains("    inputs:\n      gcit_run_id:"),
        "msg: {msg}",
    );
    // 3. type annotation
    assert!(msg.contains("type: string"), "msg: {msg}");
    // 4. run-name directive that the correlator looks for
    assert!(
        msg.contains("run-name: gcit-${{ inputs.gcit_run_id }}"),
        "msg: {msg}",
    );
    // 5. closing call-to-action
    assert!(
        msg.contains("Then commit, push to default branch, and try again."),
        "msg: {msg}",
    );
    assert_eq!(err.retryability(), Retryability::Permanent);
}

#[tokio::test]
async fn http_422_unrelated_message_classified_as_unknown() {
    // 422 messages that do NOT mention "unexpected inputs" or
    // "gcit_run_id" — e.g. "Reference does not exist" or
    // "Workflow has been disabled" — must NOT classify as
    // DispatchInvalid. The DispatchInvalid arm in classify_status
    // only fires when `message_indicates_dispatch_invalid` matches;
    // everything else falls through to Unknown carrying the raw API
    // message so operators see what GitHub actually rejected (rather
    // than the misleading gcit_run_id YAML guidance).
    //
    // Mutation target: broadening the DispatchInvalid
    // arm to fire on every 422 — operators chasing a "ref does not
    // exist" failure would be told to fix their inputs.gcit_run_id
    // declaration, which is wrong. This test pins the split.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(ResponseTemplate::new(422).set_body_json(serde_json::json!({
            "message": "Reference does not exist",
            "documentation_url": "https://docs.github.com/rest"
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let err = drive_dispatch(&mock).await;
    match &err {
        GithubErrorKind::Unknown { source } => {
            let msg = source.to_string();
            assert!(
                msg.contains("Reference does not exist"),
                "Unknown must carry the raw API message so operators see what GitHub rejected; got: {msg}",
            );
            assert!(
                msg.contains("422"),
                "Unknown must carry the status code; got: {msg}",
            );
        }
        other => panic!(
            "expected Unknown for 422 with unrelated message; got {other:?} (would mislead operator with gcit_run_id YAML)",
        ),
    }
    // Negative property: the Display string of the resulting
    // GithubErrorKind must NOT carry the DispatchInvalid YAML
    // scaffold — that would tell the operator to edit the wrong
    // thing.
    let display_msg = err.to_string();
    assert!(
        !display_msg.contains("workflow_dispatch"),
        "Unknown must NOT surface the DispatchInvalid YAML guidance; got: {display_msg}",
    );
    assert!(
        !display_msg.contains("inputs:\n      gcit_run_id:"),
        "Unknown must NOT surface the gcit_run_id YAML scaffold; got: {display_msg}",
    );
}

#[rstest]
#[case::server_error_500(500)]
#[case::server_error_502(502)]
#[case::server_error_503(503)]
#[case::server_error_504(504)]
#[tokio::test]
async fn http_5xx_classified_as_server_error(#[case] status: u16) {
    // 5xx responses must classify as ServerError carrying the actual
    // status code AND the configured backon attempt cap so the
    // operator-facing Display reads "GitHub server error 503.
    // Retrying with exponential backoff (max N attempts)."
    //
    // Mutation target: classifying 5xx as Permanent (the dispatcher
    // would never retry transient outages).
    //
    // No `.expect(N)` constraint on the mock: octocrab's default
    // `RetryConfig::Simple(3)` retries 5xx at its connector layer,
    // so wiremock receives 4 total requests for one `dispatch` call
    // (initial + 3 retries). Pinning a request count here would
    // couple the test to octocrab's internals; we only assert the
    // FINAL classified error variant, which is the property the
    // dispatcher's caller actually depends on.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(
            ResponseTemplate::new(status).set_body_json(serde_json::json!({
                "message": "upstream error"
            })),
        )
        .mount(&mock)
        .await;

    let err = drive_dispatch(&mock).await;
    match &err {
        GithubErrorKind::ServerError {
            status: s,
            max_attempts,
        } => {
            assert_eq!(*s, status);
            assert_eq!(*max_attempts, SERVER_ERROR_MAX_ATTEMPTS);
        }
        other => panic!("expected ServerError for {status}, got {other:?}"),
    }
    let msg = err.to_string();
    assert!(msg.contains(&status.to_string()), "msg: {msg}");
    assert!(msg.contains("exponential backoff"), "msg: {msg}");
    assert_eq!(err.retryability(), Retryability::Transient);
}

#[tokio::test]
async fn request_timeout_classified_as_timeout() {
    // wiremock holds the response longer than `request_timeout`. The
    // dispatcher wraps `_post` in `tokio::time::timeout`; on Elapsed
    // it surfaces the configured deadline through `timeout_error`.
    // The Display string pins "Will retry" so operators see the
    // daemon will recover on the next backon attempt.
    //
    // The wiremock delay (3s) exceeds the configured 1s timeout. The
    // 5s request_timeout in `build_dispatch_deps` is the standing
    // default; here we override to 1s so the test wallclock stays
    // under 5s.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(ResponseTemplate::new(204).set_delay(Duration::from_secs(3)))
        .expect(1)
        .mount(&mock)
        .await;

    let request_timeout = Duration::from_secs(1);
    let client = Client::builder()
        .credential(CredentialId::new("github_pat").expect("valid id"))
        .token(SecretString::from(PAT.to_string()))
        .request_timeout(request_timeout)
        .base_uri(mock.uri())
        .build()
        .expect("client build");
    let rate_bucket = RateBucket::new(Duration::from_millis(0));
    let rate_limit = RateLimitState::new();
    rate_limit
        .observe_full(5000, 5000, Utc::now() + chrono::Duration::seconds(3600))
        .await;
    let params = default_params();
    let cancel = CancellationToken::new();

    let err = dispatch(&client, &rate_bucket, &rate_limit, &params, &cancel)
        .await
        .expect_err("delayed response must time out");
    match &err {
        GithubErrorKind::Timeout { timeout } => {
            assert_eq!(*timeout, request_timeout);
        }
        other => panic!("expected Timeout, got {other:?}"),
    }
    let msg = err.to_string();
    assert!(msg.contains("Will retry"), "msg: {msg}");
    assert_eq!(err.retryability(), Retryability::Transient);
}

#[test]
fn classifier_dispatches_transient_for_5xx_timeout_rate_limited() {
    // The retry-policy mapping (table documented at the top of
    // github::error):
    //   ServerError      -> Transient (backon decides retry_after)
    //   Timeout          -> Transient
    //   RateLimited      -> Transient (caller awaits reset)
    //   Transport        -> Transient (network blip)
    //   Unknown          -> Transient (best-effort retry)
    //   Unauthorized     -> Permanent
    //   Forbidden        -> Permanent
    //   WorkflowNotFound -> Permanent
    //   DispatchInvalid  -> Permanent
    //   Cancelled        -> Permanent (supervisor decides next gen)
    //
    // Drives `retryability()` directly (pure logic, no wiremock).
    // Mutation target: any arm flipped between Transient and Permanent.
    let credential = CredentialId::new("github_pat").expect("valid id");

    let transient: Vec<(GithubErrorKind, &str)> = vec![
        (
            GithubErrorKind::ServerError {
                status: 503,
                max_attempts: SERVER_ERROR_MAX_ATTEMPTS,
            },
            "ServerError",
        ),
        (
            GithubErrorKind::Timeout {
                timeout: Duration::from_secs(30),
            },
            "Timeout",
        ),
        (
            GithubErrorKind::RateLimited {
                credential: credential.clone(),
                reset: Utc::now() + chrono::Duration::seconds(60),
            },
            "RateLimited",
        ),
        (
            GithubErrorKind::Transport {
                source: anyhow::anyhow!("connection refused"),
            },
            "Transport",
        ),
        (
            GithubErrorKind::Unknown {
                source: anyhow::anyhow!("decode failure"),
            },
            "Unknown",
        ),
    ];
    for (variant, label) in &transient {
        assert_eq!(
            variant.retryability(),
            Retryability::Transient,
            "{label} must be Transient",
        );
        assert!(variant.is_transient(), "{label} is_transient must be true");
    }

    let permanent: Vec<(GithubErrorKind, &str)> = vec![
        (
            GithubErrorKind::Unauthorized {
                credential: credential.clone(),
            },
            "Unauthorized",
        ),
        (
            GithubErrorKind::Forbidden {
                credential: credential.clone(),
            },
            "Forbidden",
        ),
        (
            GithubErrorKind::WorkflowNotFound {
                repo: "myorg/linux-builder".into(),
                workflow: "ci.yml".into(),
            },
            "WorkflowNotFound",
        ),
        (
            GithubErrorKind::DispatchInvalid {
                workflow: "ci.yml".into(),
            },
            "DispatchInvalid",
        ),
        (GithubErrorKind::Cancelled, "Cancelled"),
    ];
    for (variant, label) in &permanent {
        assert_eq!(
            variant.retryability(),
            Retryability::Permanent,
            "{label} must be Permanent",
        );
        assert!(
            !variant.is_transient(),
            "{label} is_transient must be false",
        );
    }

    // RateLimited's retry_after surfaces the time-until-reset; other
    // variants do not surface a retry hint (backon picks the schedule).
    let now = Utc::now();
    let limited = GithubErrorKind::RateLimited {
        credential: credential.clone(),
        reset: now + chrono::Duration::seconds(45),
    };
    let after = limited
        .retry_after(now)
        .expect("RateLimited surfaces retry_after");
    assert!(
        after >= Duration::from_secs(44) && after <= Duration::from_secs(46),
        "RateLimited retry_after ~= 45s; got {after:?}",
    );
    for variant in &[
        GithubErrorKind::ServerError {
            status: 503,
            max_attempts: SERVER_ERROR_MAX_ATTEMPTS,
        },
        GithubErrorKind::Timeout {
            timeout: Duration::from_secs(30),
        },
        GithubErrorKind::Unauthorized {
            credential: credential.clone(),
        },
    ] {
        assert!(
            variant.retry_after(now).is_none(),
            "non-rate-limited must not surface retry_after: {variant:?}",
        );
    }
}

#[test]
fn classifier_handles_unknown_variants_safely() {
    // octocrab::Error has variants beyond the GithubErrorKind set
    // (Hyper, Service, Encoder, Http, UriParse, etc.). gcit's
    // `classify` MUST handle every one without panicking AND produce a
    // Transient classification so backon retries — if the unknown
    // error persists, it surfaces in `gcit status` last_error. The
    // routing in github::error::classify:
    //   Hyper / Service / Encoder / Http -> Transport (Transient)
    //   anything else                    -> Unknown   (Transient)
    //
    // Driving classify with a real `octocrab::Error::Hyper` is
    // awkward (its inner type is private); the equivalent test runs
    // `classify_status` against a plausible-but-uncategorised 4xx
    // (410 Gone, 418 I'm a Teapot) — those flow through the GitHub
    // branch and out the catch-all Unknown arm.
    let credential = CredentialId::new("github_pat").expect("valid id");
    for status in [http::StatusCode::GONE, http::StatusCode::IM_A_TEAPOT] {
        let kind = classify_status(StatusContext {
            status,
            message: "uncategorised",
            rate_limit_remaining: None,
            rate_limit_reset: None,
            credential: &credential,
            repo: "owner/repo",
            workflow: "ci.yml",
        });
        match &kind {
            GithubErrorKind::Unknown { source } => {
                let msg = source.to_string();
                assert!(
                    msg.contains(&status.as_u16().to_string()),
                    "{status}: source must carry status: {msg}",
                );
            }
            other => panic!("expected Unknown for {status}, got {other:?}"),
        }
        assert_eq!(
            kind.retryability(),
            Retryability::Transient,
            "Unknown must be Transient (best-effort retry): {status}",
        );
    }
}

#[tokio::test]
async fn error_messages_redact_credential_value() {
    // The Display impl on every variant carrying `credential` (an
    // `CredentialId`) emits the configuration-time **id**, never the
    // wrapped `SecretString` token. The secret value lives in
    // `Client` (held in a `secrecy::SecretString`), not in any error
    // variant — the type system
    // gives the variants no slot to leak it. This test is a
    // defense-in-depth check: format every variant that sees a
    // credential, assert the id appears AND the test PAT (a structurally
    // distinct literal that cannot occur in any other artifact) does
    // not appear anywhere in the message. Then drive a 401 through the
    // live HTTP path to pin the same property end-to-end.
    let credential = CredentialId::new("github_pat").expect("valid id");
    let variants: Vec<(GithubErrorKind, &str)> = vec![
        (
            GithubErrorKind::Unauthorized {
                credential: credential.clone(),
            },
            "Unauthorized",
        ),
        (
            GithubErrorKind::Forbidden {
                credential: credential.clone(),
            },
            "Forbidden",
        ),
        (
            GithubErrorKind::RateLimited {
                credential: credential.clone(),
                reset: Utc::now() + chrono::Duration::seconds(60),
            },
            "RateLimited",
        ),
    ];
    for (variant, label) in &variants {
        let msg = variant.to_string();
        // Positive: the id surfaces (operators triage by id).
        assert!(
            msg.contains("github_pat"),
            "{label}: credential id must appear; msg: {msg}",
        );
        // Negative: the secret value (test PAT literal) does NOT
        // surface. Type-system already prevents this — there is no
        // field of type `SecretString` on these variants — so this
        // assertion is mostly belt-and-braces.
        assert!(
            !msg.contains(PAT),
            "{label}: PAT secret value must NOT appear in Display; msg: {msg}",
        );
        // Also defend against the test PAT's distinguishing literal
        // (the suffix that cannot occur in any other artifact)
        // leaking through a Debug-derived path — Debug renders nested
        // fields, so a future refactor wiring the SecretString in
        // would surface it via the suffix.
        assert!(
            !msg.contains("test_token_for_wiremock"),
            "{label}: token-like literal must not leak; msg: {msg}",
        );
    }

    // Variants that do NOT carry a credential field still must not
    // accidentally embed the test PAT through some path-formatting
    // accident. Format each and assert the literal is absent.
    for variant in [
        GithubErrorKind::WorkflowNotFound {
            repo: "myorg/linux-builder".into(),
            workflow: "ci.yml".into(),
        },
        GithubErrorKind::DispatchInvalid {
            workflow: "ci.yml".into(),
        },
        GithubErrorKind::ServerError {
            status: 503,
            max_attempts: SERVER_ERROR_MAX_ATTEMPTS,
        },
        GithubErrorKind::Timeout {
            timeout: Duration::from_secs(30),
        },
        GithubErrorKind::Transport {
            source: anyhow::anyhow!("connection refused"),
        },
        GithubErrorKind::Unknown {
            source: anyhow::anyhow!("rare situation"),
        },
        GithubErrorKind::Cancelled,
    ] {
        let msg = variant.to_string();
        assert!(
            !msg.contains(PAT),
            "{variant:?}: PAT must not appear; msg: {msg}",
        );
    }

    // Live HTTP path: drive a 401 through wiremock + dispatch + classify.
    // Confirms the rendered string carries the credential id but never
    // the PAT — pins the redaction property end-to-end where a future
    // refactor would most likely leak (Unauthorized is the variant
    // closest in scope to the credential).
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "message": "Bad credentials"
        })))
        .expect(1)
        .mount(&mock)
        .await;
    let err = drive_dispatch(&mock).await;
    let live_msg = err.to_string();
    assert!(
        !live_msg.contains(PAT),
        "live 401 path must not leak PAT in Display: {live_msg}",
    );
    assert!(
        live_msg.contains("github_pat"),
        "live 401 must surface credential id: {live_msg}",
    );
}

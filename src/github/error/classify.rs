// Pure-logic classifier for octocrab errors and HTTP responses.
// `classify_status` is the pure function tests can drive directly;
// `classify` is the octocrab adapter; `timeout_error` is the
// `tokio::time::error::Elapsed` adapter.

use std::time::Duration;

use chrono::{DateTime, Utc};
use http::StatusCode;

use crate::config::CredentialId;

use super::{GithubErrorKind, SERVER_ERROR_MAX_ATTEMPTS};

/// Inputs to the pure-logic classifier extracted from an octocrab
/// `Error::GitHub` branch. Tests can drive this directly without
/// constructing an `octocrab::Error` (the latter is
/// `#[non_exhaustive]` with private snafu plumbing).
#[derive(Debug, Clone)]
pub struct StatusContext<'a> {
    pub status: StatusCode,
    pub message: &'a str,
    /// `X-RateLimit-Remaining` if the response carried one.
    /// `Some(0)` is the rate-limited signal.
    pub rate_limit_remaining: Option<u64>,
    /// `X-RateLimit-Reset` parsed as `DateTime<Utc>` by the caller.
    pub rate_limit_reset: Option<DateTime<Utc>>,
    pub credential: &'a CredentialId,
    pub repo: &'a str,
    pub workflow: &'a str,
}

/// Branch order:
///   401            -> Unauthorized
///   403 + rate-lim -> RateLimited
///   403            -> Forbidden
///   404            -> WorkflowNotFound
///   422 + matching -> DispatchInvalid
///   422 + other    -> Unknown (carries the raw message)
///   429            -> RateLimited (reset may be missing or stale)
///   5xx            -> ServerError
///   anything else  -> Unknown
///
/// The 422 split: only the gcit_run_id case maps to DispatchInvalid
/// (the YAML snippet is the actionable fix). Other 422s ("ref does
/// not exist", "workflow has been disabled", etc.) surface the raw
/// API message so the operator sees what GitHub actually rejected.
pub fn classify_status(ctx: StatusContext<'_>) -> GithubErrorKind {
    let StatusContext {
        status,
        message,
        rate_limit_remaining,
        rate_limit_reset,
        credential,
        repo,
        workflow,
    } = ctx;

    if status == StatusCode::UNAUTHORIZED {
        return GithubErrorKind::Unauthorized {
            credential: credential.clone(),
        };
    }

    // 403 split: rate-limit (Transient) vs scope-denied (Permanent).
    // `Remaining: 0` is the quota-exhaustion signal. When the header
    // is missing or the parsed reset is in the past (stale echo from
    // a previous quota window), default to 60s out so the dispatcher
    // backs off to a real future window rather than hot-looping.
    // Mirrors the 429 fallback below + `github::dispatcher::reclassify_403_via_snapshot`.
    if status == StatusCode::FORBIDDEN {
        if rate_limit_remaining == Some(0) {
            return GithubErrorKind::RateLimited {
                credential: credential.clone(),
                reset: resolve_reset(rate_limit_reset),
            };
        }
        return GithubErrorKind::Forbidden {
            credential: credential.clone(),
        };
    }

    if status == StatusCode::NOT_FOUND {
        return GithubErrorKind::WorkflowNotFound {
            repo: repo.to_string(),
            workflow: workflow.to_string(),
        };
    }

    if status == StatusCode::UNPROCESSABLE_ENTITY {
        // Only the "Unexpected inputs"/gcit_run_id case gets the
        // tailored YAML guidance. Other 422s name a different problem
        // ("ref does not exist", "workflow has been disabled");
        // surface the raw message so operators see what was rejected.
        if message_indicates_dispatch_invalid(message) {
            return GithubErrorKind::DispatchInvalid {
                workflow: workflow.to_string(),
            };
        }
        return GithubErrorKind::Unknown {
            source: anyhow::anyhow!("github 422 on {repo}/{workflow}: {message}"),
        };
    }

    // 429: same stale-reset fallback as the 403/Remaining=0 path.
    if status == StatusCode::TOO_MANY_REQUESTS {
        return GithubErrorKind::RateLimited {
            credential: credential.clone(),
            reset: resolve_reset(rate_limit_reset),
        };
    }

    if status.is_server_error() {
        return GithubErrorKind::ServerError {
            status: status.as_u16(),
            max_attempts: SERVER_ERROR_MAX_ATTEMPTS,
        };
    }

    // 4xx other than the enumerated ones is an unknown surface; flag
    // as Unknown so the operator sees the raw status in last_error.
    GithubErrorKind::Unknown {
        source: anyhow::anyhow!("uncategorised github status {}", status.as_u16()),
    }
}

/// Choose a rate-limit reset that's guaranteed to be in the future.
/// `Some(r)` past `now` passes through; `None` or a stale `r` falls
/// back to `now + 60s` so the dispatcher always has a real future
/// hold-off rather than retrying against an already-expired window.
fn resolve_reset(reset: Option<DateTime<Utc>>) -> DateTime<Utc> {
    let now = Utc::now();
    match reset {
        Some(r) if r > now => r,
        _ => now + chrono::Duration::seconds(60),
    }
}

/// True if a 422 message indicates the gcit_run_id case. Detection
/// is permissive: case-insensitive substring match on either
/// "unexpected inputs" (the canonical message) or "gcit_run_id"
/// (the rejected input name as it appears in errors[]).
fn message_indicates_dispatch_invalid(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("unexpected inputs") || lower.contains("gcit_run_id")
}

/// Wrap an `octocrab::Error` into a `GithubErrorKind`. Pulls the
/// status code + message out of the `Error::GitHub` variant and
/// delegates to `classify_status`. Other variants:
///
///   `Service` / `Hyper` / `Encoder` / `Http` -> Transport (Transient)
///   everything else                          -> Unknown (Transient)
pub fn classify(
    err: octocrab::Error,
    credential: &CredentialId,
    repo: &str,
    workflow: &str,
    rate_limit_remaining: Option<u64>,
    rate_limit_reset: Option<DateTime<Utc>>,
) -> GithubErrorKind {
    if let octocrab::Error::GitHub { source, .. } = &err {
        return classify_status(StatusContext {
            status: source.status_code,
            message: &source.message,
            rate_limit_remaining,
            rate_limit_reset,
            credential,
            repo,
            workflow,
        });
    }
    match err {
        octocrab::Error::Hyper { .. }
        | octocrab::Error::Service { .. }
        | octocrab::Error::Encoder { .. }
        | octocrab::Error::Http { .. } => GithubErrorKind::Transport {
            source: anyhow::anyhow!("octocrab transport error: {err}"),
        },
        other => GithubErrorKind::Unknown {
            source: anyhow::anyhow!("octocrab error: {other}"),
        },
    }
}

/// Translate a `tokio::time::error::Elapsed` (from
/// `tokio::time::timeout`) into a `Timeout` variant carrying the
/// configured per-request deadline.
pub fn timeout_error(deadline: Duration) -> GithubErrorKind {
    GithubErrorKind::Timeout { timeout: deadline }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cred(id: &str) -> CredentialId {
        CredentialId::new(id).expect("valid id")
    }

    fn ctx<'a>(
        status: StatusCode,
        message: &'a str,
        remaining: Option<u64>,
        reset: Option<DateTime<Utc>>,
        credential: &'a CredentialId,
    ) -> StatusContext<'a> {
        StatusContext {
            status,
            message,
            rate_limit_remaining: remaining,
            rate_limit_reset: reset,
            credential,
            repo: "owner/repo",
            workflow: "ci.yml",
        }
    }

    /// Assert the reset is approximately `now + 60s` (within a small
    /// window). Used by the stale-reset / missing-reset fallback tests.
    fn assert_reset_is_now_plus_60s(
        reset: DateTime<Utc>,
        before: DateTime<Utc>,
        after: DateTime<Utc>,
    ) {
        let lower = before + chrono::Duration::seconds(60);
        let upper = after + chrono::Duration::seconds(60);
        assert!(
            reset >= lower && reset <= upper,
            "reset must be ~now+60s; got {reset} not in [{lower}, {upper}]",
        );
    }

    #[test]
    fn classify_401_is_unauthorized_with_credential() {
        let c = cred("github_pat");
        let e = classify_status(ctx(
            StatusCode::UNAUTHORIZED,
            "Bad credentials",
            None,
            None,
            &c,
        ));
        match e {
            GithubErrorKind::Unauthorized { credential } => {
                assert_eq!(credential.as_str(), "github_pat")
            }
            other => panic!("expected Unauthorized, got {other:?}"),
        }
    }

    #[test]
    fn classify_403_no_rate_limit_header_is_forbidden() {
        let c = cred("github_pat");
        let e = classify_status(ctx(
            StatusCode::FORBIDDEN,
            "Resource not accessible by integration",
            None,
            None,
            &c,
        ));
        match e {
            GithubErrorKind::Forbidden { credential } => {
                assert_eq!(credential.as_str(), "github_pat")
            }
            other => panic!("expected Forbidden, got {other:?}"),
        }
        let msg = format!(
            "{}",
            classify_status(ctx(StatusCode::FORBIDDEN, "", None, None, &c))
        );
        assert!(msg.contains("'workflow' + 'repo'"), "msg: {msg}");
        assert!(msg.contains("gh auth status"), "msg: {msg}");
    }

    #[test]
    fn classify_403_with_rate_limit_remaining_zero_is_rate_limited() {
        let c = cred("github_pat");
        let reset = Utc::now() + chrono::Duration::seconds(120);
        let e = classify_status(ctx(
            StatusCode::FORBIDDEN,
            "API rate limit exceeded",
            Some(0),
            Some(reset),
            &c,
        ));
        match e {
            GithubErrorKind::RateLimited {
                credential,
                reset: r,
            } => {
                assert_eq!(credential.as_str(), "github_pat");
                assert_eq!(r, reset);
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    #[test]
    fn classify_403_with_rate_limit_remaining_zero_but_reset_in_past_uses_default() {
        // Stale reset must be replaced by ~now+60s; using it as-is
        // would tell the dispatcher "retry immediately" against a
        // quota that has not actually opened.
        let c = cred("github_pat");
        let stale_reset = Utc::now() - chrono::Duration::seconds(120);
        let before = Utc::now();
        let e = classify_status(ctx(
            StatusCode::FORBIDDEN,
            "API rate limit exceeded",
            Some(0),
            Some(stale_reset),
            &c,
        ));
        let after = Utc::now();
        match e {
            GithubErrorKind::RateLimited {
                credential,
                reset: r,
            } => {
                assert_eq!(credential.as_str(), "github_pat");
                assert_reset_is_now_plus_60s(r, before, after);
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    #[test]
    fn classify_429_with_rate_limit_reset_in_past_uses_default() {
        // 429 mirrors the 403/Remaining=0 stale-reset path.
        let c = cred("github_pat");
        let stale_reset = Utc::now() - chrono::Duration::seconds(30);
        let before = Utc::now();
        let e = classify_status(ctx(
            StatusCode::TOO_MANY_REQUESTS,
            "slow down",
            None,
            Some(stale_reset),
            &c,
        ));
        let after = Utc::now();
        match e {
            GithubErrorKind::RateLimited { reset, .. } => {
                assert_reset_is_now_plus_60s(reset, before, after);
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    #[test]
    fn classify_403_with_rate_limit_remaining_zero_but_reset_missing_is_rate_limited() {
        let c = cred("github_pat");
        let before = Utc::now();
        let e = classify_status(ctx(
            StatusCode::FORBIDDEN,
            "API rate limit exceeded",
            Some(0),
            None,
            &c,
        ));
        let after = Utc::now();
        match e {
            GithubErrorKind::RateLimited {
                credential,
                reset: r,
            } => {
                assert_eq!(credential.as_str(), "github_pat");
                assert_reset_is_now_plus_60s(r, before, after);
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    #[test]
    fn classify_403_with_rate_limit_header_but_remaining_nonzero_is_forbidden() {
        // Remaining > 0 + 403 means scope denial, not quota
        // exhaustion. Defensive — GitHub's response shape allows this.
        let c = cred("github_pat");
        let e = classify_status(ctx(
            StatusCode::FORBIDDEN,
            "Resource not accessible",
            Some(4500),
            Some(Utc::now()),
            &c,
        ));
        assert!(matches!(e, GithubErrorKind::Forbidden { .. }), "got {e:?}");
    }

    #[test]
    fn classify_404_is_workflow_not_found_carrying_repo_and_workflow() {
        let c = cred("github_pat");
        let e = classify_status(ctx(StatusCode::NOT_FOUND, "Not Found", None, None, &c));
        match e {
            GithubErrorKind::WorkflowNotFound { repo, workflow } => {
                assert_eq!(repo, "owner/repo");
                assert_eq!(workflow, "ci.yml");
            }
            other => panic!("expected WorkflowNotFound, got {other:?}"),
        }
    }

    #[test]
    fn classify_422_is_dispatch_invalid_with_yaml_snippet() {
        let c = cred("github_pat");
        let e = classify_status(ctx(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Unexpected inputs",
            None,
            None,
            &c,
        ));
        let msg = format!("{e}");
        assert!(msg.contains("on:\n  workflow_dispatch:"), "msg: {msg}");
        assert!(
            msg.contains("    inputs:\n      gcit_run_id:"),
            "msg: {msg}"
        );
        assert!(msg.contains("        type: string"), "msg: {msg}");
        assert!(
            msg.contains("run-name: gcit-${{ inputs.gcit_run_id }}"),
            "msg: {msg}"
        );
        assert!(
            msg.contains("Then commit, push to default branch, and try again."),
            "msg: {msg}"
        );
        assert!(matches!(e, GithubErrorKind::DispatchInvalid { .. }));
    }

    #[test]
    fn classify_429_is_rate_limited_with_reset_default_when_header_missing() {
        let c = cred("github_pat");
        let before = Utc::now();
        let e = classify_status(ctx(
            StatusCode::TOO_MANY_REQUESTS,
            "slow down",
            None,
            None,
            &c,
        ));
        let after = Utc::now();
        match e {
            GithubErrorKind::RateLimited { reset, .. } => {
                assert_reset_is_now_plus_60s(reset, before, after);
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    #[test]
    fn classify_500_is_server_error_with_status_and_max_attempts() {
        let c = cred("github_pat");
        let e = classify_status(ctx(
            StatusCode::INTERNAL_SERVER_ERROR,
            "boom",
            None,
            None,
            &c,
        ));
        match e {
            GithubErrorKind::ServerError {
                status,
                max_attempts,
            } => {
                assert_eq!(status, 500);
                assert_eq!(max_attempts, SERVER_ERROR_MAX_ATTEMPTS);
            }
            other => panic!("expected ServerError, got {other:?}"),
        }
    }

    #[test]
    fn classify_502_503_504_are_server_error() {
        let c = cred("github_pat");
        for s in [
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::GATEWAY_TIMEOUT,
        ] {
            let e = classify_status(ctx(s, "upstream", None, None, &c));
            assert!(
                matches!(e, GithubErrorKind::ServerError { .. }),
                "{s}: got {e:?}",
            );
        }
    }

    #[test]
    fn classify_random_4xx_is_unknown() {
        // 410 Gone, 451 Unavailable For Legal Reasons — plausible but
        // not enumerated. Maps to Unknown so last_error reflects the
        // raw status.
        let c = cred("github_pat");
        for s in [
            StatusCode::GONE,
            StatusCode::from_u16(451).expect("451 is a valid status code"),
        ] {
            let e = classify_status(ctx(s, "rare", None, None, &c));
            assert!(
                matches!(e, GithubErrorKind::Unknown { .. }),
                "{s}: got {e:?}"
            );
        }
    }

    #[test]
    fn classify_422_unexpected_inputs_is_dispatch_invalid() {
        let c = cred("github_pat");
        let e = classify_status(ctx(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Unexpected inputs",
            None,
            None,
            &c,
        ));
        assert!(
            matches!(e, GithubErrorKind::DispatchInvalid { .. }),
            "got {e:?}",
        );
    }

    #[test]
    fn classify_422_unexpected_inputs_case_insensitive() {
        let c = cred("github_pat");
        for msg in [
            "Unexpected Inputs",
            "UNEXPECTED INPUTS",
            "unexpected inputs",
        ] {
            let e = classify_status(ctx(StatusCode::UNPROCESSABLE_ENTITY, msg, None, None, &c));
            assert!(
                matches!(e, GithubErrorKind::DispatchInvalid { .. }),
                "msg {msg:?} got {e:?}",
            );
        }
    }

    #[test]
    fn classify_422_with_gcit_run_id_in_errors_is_dispatch_invalid() {
        let c = cred("github_pat");
        let e = classify_status(ctx(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Unprocessable Entity (input gcit_run_id is not declared)",
            None,
            None,
            &c,
        ));
        assert!(
            matches!(e, GithubErrorKind::DispatchInvalid { .. }),
            "got {e:?}",
        );
    }

    #[test]
    fn classify_422_other_message_falls_through_to_unknown() {
        // 422 not matching the gcit_run_id case must NOT emit the YAML
        // snippet. Unknown carries the raw API message.
        let c = cred("github_pat");
        let e = classify_status(ctx(
            StatusCode::UNPROCESSABLE_ENTITY,
            "workflow has been disabled",
            None,
            None,
            &c,
        ));
        match e {
            GithubErrorKind::Unknown { source } => {
                let msg = source.to_string();
                assert!(
                    msg.contains("workflow has been disabled"),
                    "raw message must surface; got: {msg}",
                );
                assert!(msg.contains("422"), "status code must surface; got: {msg}");
            }
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    #[test]
    fn message_indicates_dispatch_invalid_helper() {
        assert!(message_indicates_dispatch_invalid("Unexpected inputs"));
        assert!(message_indicates_dispatch_invalid("unexpected inputs"));
        assert!(message_indicates_dispatch_invalid("UNEXPECTED INPUTS"));
        assert!(message_indicates_dispatch_invalid(
            "errors mention gcit_run_id explicitly"
        ));
        assert!(!message_indicates_dispatch_invalid(
            "workflow has been disabled"
        ));
        assert!(!message_indicates_dispatch_invalid("ref does not exist"));
        assert!(!message_indicates_dispatch_invalid(""));
    }

    #[test]
    fn timeout_error_carries_deadline() {
        let e = timeout_error(Duration::from_secs(30));
        match e {
            GithubErrorKind::Timeout { timeout } => {
                assert_eq!(timeout, Duration::from_secs(30))
            }
            other => panic!("expected Timeout, got {other:?}"),
        }
    }
}

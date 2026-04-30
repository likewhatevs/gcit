// GithubErrorKind — typed classification of every GitHub-side failure
// the dispatcher / monitor / correlator / rate-limit poller can hit.
//
// The Display strings here are operator-visible and stable; operators
// copy them out of journald/CLI output and grep for them in their
// workflow files. Drift breaks that workflow.
//
// Three layers:
//   1. `GithubErrorKind` — the typed enum.
//   2. `classify_status` — pure-logic classifier from
//      (StatusCode, message, rate_limit_remaining_header,
//       rate_limit_reset_header). Tests drive it directly.
//   3. `classify` — wraps an `octocrab::Error` and routes the
//      `Error::GitHub` branch through `classify_status`. Other
//      octocrab variants (Hyper, Service, etc.) map to
//      `ServerError`/`Timeout`/`Unknown`.
//
// Each variant carries the structured fields the operator sees:
// `credential` (CredentialId — the ID, never the secret value);
// `repo`/`workflow` (so the operator knows which file to edit);
// `reset` (chrono::DateTime<Utc> from the `X-RateLimit-Reset`
// header, parsed as epoch seconds); `status`/`max_attempts`
// (server-error context); `timeout` (Duration of the elapsed
// request, for Timeout). Anything outside the 11 ratified variants
// — non-status octocrab errors, future API additions — falls
// through to `Unknown { source }` so the daemon never crashes on
// a never-before-seen error shape.

use std::time::Duration;

use chrono::{DateTime, Utc};
use http::StatusCode;

use crate::config::CredentialId;

/// The retry-policy classification gcit uses to drive backon's
/// `.when(predicate)` retry guard. Transient errors are retryable
/// (network blip, 5xx, rate-limit reset will eventually pass);
/// Permanent errors are not (PAT scope wrong, workflow YAML wrong —
/// no amount of retrying changes the answer).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Retryability {
    Transient,
    Permanent,
}

/// GitHub error variants. Each variant produces a Display string
/// operators can act on directly. The strings are pinned byte-for-byte
/// by tests/github_error_classifier.rs.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum GithubErrorKind {
    /// HTTP 401. Token is invalid or expired. PAT must be re-issued
    /// via the GitHub UI; gcit will not retry until the operator
    /// rotates the credential.
    #[error("GitHub auth failed (401). PAT for credential '{credential}' invalid or expired. Re-issue via GitHub UI.")]
    Unauthorized { credential: CredentialId },

    /// HTTP 403 *without* a rate-limit header. PAT lacks the required
    /// scope. The fix is operator-facing: regenerate the token with
    /// `workflow` + `repo` (or `public_repo`).
    #[error("GitHub auth denied (403). PAT for credential '{credential}' lacks scope. Required: 'workflow' + 'repo' (or 'public_repo' for public repos). Run: gh auth status")]
    Forbidden { credential: CredentialId },

    /// HTTP 404 on a workflow path. Either the workflow file doesn't
    /// exist on the default branch, lacks `on: workflow_dispatch:`,
    /// or the PAT can't see the repo.
    #[error("Workflow not found: {repo}/{workflow}. Verify (a) the file exists in the repository's default branch, (b) `on: workflow_dispatch:` is declared, (c) PAT has access.")]
    WorkflowNotFound { repo: String, workflow: String },

    /// HTTP 422 on workflow_dispatch. Most common cause: workflow
    /// YAML missing the `gcit_run_id` input declaration. The Display
    /// includes a copy-paste-able YAML snippet (the indentation is
    /// pinned; YAML is whitespace-sensitive). Note the doubled
    /// `{{ }}` escaping the GitHub Actions template syntax inside
    /// thiserror's format string.
    #[error("GitHub rejected workflow_dispatch (422 Unprocessable Entity). Most common cause: workflow YAML lacks `workflow_dispatch:` in `on:`. Add this to .github/workflows/{workflow}:\n\non:\n  workflow_dispatch:\n    inputs:\n      gcit_run_id:\n        type: string\n\nrun-name: gcit-${{{{ inputs.gcit_run_id }}}}\n\nThen commit, push to default branch, and try again.")]
    DispatchInvalid { workflow: String },

    /// HTTP 403 *with* `X-RateLimit-Remaining: 0`. Quota is
    /// exhausted; gcit defers requests until the reset epoch.
    #[error("GitHub rate limit exhausted on credential '{credential}'. Reset at {reset}. gcit will defer requests until reset.")]
    RateLimited {
        credential: CredentialId,
        reset: DateTime<Utc>,
    },

    /// HTTP 5xx. Retryable via backon's exponential schedule.
    /// `max_attempts` carries the configured retry cap so the message
    /// is precise rather than a vague "will retry".
    #[error("GitHub server error {status}. Retrying with exponential backoff (max {max_attempts} attempts).")]
    ServerError { status: u16, max_attempts: u32 },

    /// Request elapsed past the configured per-request timeout
    /// (`http.request_timeout` from config). Retryable.
    #[error("GitHub request timeout after {timeout:?}. Will retry.")]
    Timeout { timeout: Duration },

    /// Transport-layer failure (Hyper, Service, Encoder, Http) —
    /// octocrab couldn't even land an HTTP exchange. Distinct from
    /// `ServerError` (which carries a real status code from the
    /// server) so operators can tell "GitHub is down" from "the
    /// network is broken". Retryable.
    #[error("GitHub transport error: {source}. Will retry.")]
    Transport {
        #[source]
        source: anyhow::Error,
    },

    /// Catch-all for octocrab error variants gcit doesn't explicitly
    /// classify (decode errors, future octocrab additions). Per
    /// tests/github_error_classifier.rs::classifier_handles_unknown_variants_safely
    /// these are classified as Transient — retry once; if persistent,
    /// the underlying issue surfaces in `gcit status` last_error.
    #[error("GitHub request failed (uncategorised): {source}")]
    Unknown {
        #[source]
        source: anyhow::Error,
    },

    /// Daemon shutdown / per-flow reload fired while the dispatcher
    /// was waiting on a quota deferral or another cancellable wait.
    /// Permanent: the dispatcher must NOT retry; the new flow
    /// generation will pick up the work after the reload completes
    /// (when the cancel was a SIGHUP), and shutdown is final (when
    /// the cancel was a SIGTERM/SIGINT). Operators reading
    /// last_error can tell which fired by checking the immediately
    /// preceding daemon log line in journalctl: the supervisor
    /// emits "SIGHUP received; reloading", "SIGTERM received;
    /// shutting down", or "SIGINT received; shutting down" right
    /// before propagating cancel. After SIGHUP, the same flow
    /// re-reads its trigger from the next poll cycle; after
    /// SIGTERM/SIGINT, the work is intentionally dropped.
    #[error(
        "dispatch cancelled by supervisor (SIGHUP reload OR SIGTERM/SIGINT shutdown — see daemon log line preceding this error to identify which); after a reload the new generation re-fires from the next poll, after shutdown the work is intentionally dropped"
    )]
    Cancelled,

    /// GitHub API response declared (or streamed) more bytes than
    /// `client::RESPONSE_BODY_LIMIT` (16 MiB). Permanent: a body that
    /// exceeds the cap is either a misbehaving server or a
    /// gzip-bomb-shaped attack from upstream; retrying gets the
    /// same response. The two layers that can produce this:
    ///   - Pre-check: `Content-Length` header > limit (declared
    ///     length too big; `declared` carries that value).
    ///   - Streaming: `http_body_util::Limited` triggers
    ///     `LengthLimitError` mid-stream when the actual body
    ///     overruns the cap regardless of Content-Length. In that
    ///     branch `declared` is `None` because the server didn't
    ///     declare an over-cap length up front.
    #[error(
        "GitHub API response body exceeds {limit}-byte cap{}",
        match declared {
            Some(d) => format!("; server declared Content-Length: {d}"),
            None => String::from("; streamed body exceeded the cap mid-read"),
        }
    )]
    BodyTooLarge {
        /// `Content-Length` header value when present at pre-check.
        /// `None` when the streaming wrapper rejected mid-read.
        declared: Option<u64>,
        /// `client::RESPONSE_BODY_LIMIT` at the moment of rejection.
        /// Carried by value so the message is self-contained when
        /// inspected post-classification.
        limit: usize,
    },
}

impl GithubErrorKind {
    /// Drive the retry decision. Permanent errors short-circuit
    /// backon's loop; Transient errors retry until the schedule
    /// exhausts. Per github_error_classifier::classifier_dispatches_*:
    ///   ServerError      -> Transient
    ///   Timeout          -> Transient
    ///   RateLimited      -> Transient (caller awaits reset before next attempt)
    ///   Unauthorized     -> Permanent
    ///   Forbidden        -> Permanent
    ///   WorkflowNotFound -> Permanent
    ///   DispatchInvalid  -> Permanent
    ///   Unknown          -> Transient (best-effort retry; will surface if persistent)
    pub fn retryability(&self) -> Retryability {
        match self {
            GithubErrorKind::Unauthorized { .. }
            | GithubErrorKind::Forbidden { .. }
            | GithubErrorKind::WorkflowNotFound { .. }
            | GithubErrorKind::DispatchInvalid { .. }
            | GithubErrorKind::Cancelled
            | GithubErrorKind::BodyTooLarge { .. } => Retryability::Permanent,
            GithubErrorKind::ServerError { .. }
            | GithubErrorKind::Timeout { .. }
            | GithubErrorKind::RateLimited { .. }
            | GithubErrorKind::Transport { .. }
            | GithubErrorKind::Unknown { .. } => Retryability::Transient,
        }
    }

    /// Convenience predicate for `backon::ExponentialBuilder.when(...)`.
    pub fn is_transient(&self) -> bool {
        matches!(self.retryability(), Retryability::Transient)
    }

    /// `RateLimited.reset` if applicable. Used by the dispatcher to
    /// sleep until the quota is restored before issuing the next
    /// attempt — overrides backon's exponential schedule when the
    /// reset is later than the next scheduled retry.
    pub fn retry_after(&self, now: DateTime<Utc>) -> Option<Duration> {
        match self {
            GithubErrorKind::RateLimited { reset, .. } => {
                if *reset > now {
                    (*reset - now).to_std().ok()
                } else {
                    Some(Duration::from_secs(0))
                }
            }
            _ => None,
        }
    }
}

/// Default backon attempt cap for ServerError messages. Pinned in
/// one place so the backoff defaults and the operator-facing message
/// stay aligned.
pub const SERVER_ERROR_MAX_ATTEMPTS: u32 = 6;

/// Inputs to the pure-logic classifier extracted from an octocrab
/// `Error::GitHub` branch. Tests can drive this directly without
/// needing to construct an `octocrab::Error` (`octocrab::Error` is
/// `#[non_exhaustive]` with private snafu plumbing).
#[derive(Debug, Clone)]
pub struct StatusContext<'a> {
    pub status: StatusCode,
    pub message: &'a str,
    /// Value of `X-RateLimit-Remaining` if the response carried one.
    /// `Some(0)` is the rate-limited signal.
    pub rate_limit_remaining: Option<u64>,
    /// Value of `X-RateLimit-Reset` if the response carried one.
    /// Epoch seconds, parsed by the caller before invoking
    /// `classify_status` so this function stays pure.
    pub rate_limit_reset: Option<DateTime<Utc>>,
    pub credential: &'a CredentialId,
    pub repo: &'a str,
    pub workflow: &'a str,
}

/// Pure-logic classifier. Inspect `(status, headers, message)` and
/// return the matching `GithubErrorKind`. The cross-checking against
/// rate-limit headers happens here so the dispatcher path and
/// monitor path use the same logic.
///
/// Branch order matters:
///   401            -> Unauthorized
///   403 + rate-lim -> RateLimited
///   403            -> Forbidden
///   404            -> WorkflowNotFound
///   422 + matching message -> DispatchInvalid
///   422 + other message    -> Unknown (carries the raw message)
///   429            -> RateLimited (best-effort; reset may be missing)
///   5xx            -> ServerError
///   anything else  -> Unknown (defensive)
///
/// The 422 split: only the gcit_run_id case maps to DispatchInvalid
/// (the YAML snippet is the actionable fix). Other 422s ("ref does
/// not exist", "workflow has been disabled", etc.) surface the raw
/// API message so the operator sees what GitHub actually rejected.
/// Detection heuristic: message contains "Unexpected inputs" or
/// refers to "gcit_run_id" — covers the canonical body shape
/// `{ "message": "Unexpected inputs", "errors": [...] }` and any
/// "inputs.gcit_run_id" mention in errors[].
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
    // 403 with `X-RateLimit-Remaining: 0` means quota exhausted; 403
    // without that header means PAT scope is wrong. The header-driven
    // check is more reliable than the body-string heuristic gcit uses
    // elsewhere (see src/git/github_api.rs, which has access to the
    // string body but not the header — the contexts differ).
    //
    // When `Remaining: 0` is present but `Reset` is missing OR is in
    // the past (the header echoed the previous quota window's reset
    // epoch), default the reset to one minute out so the variant still
    // classifies as RateLimited (Transient) and the dispatcher backs
    // off rather than treating the 403 as a permanent scope denial OR
    // immediately retrying against an already-expired hold-off.
    // Mirrors `github::dispatcher::reclassify_403_via_snapshot` (the
    // dispatcher path has the same cross-check) and the 429 fallback
    // below.
    if status == StatusCode::FORBIDDEN {
        if rate_limit_remaining == Some(0) {
            let now = Utc::now();
            let reset = match rate_limit_reset {
                Some(r) if r > now => r,
                _ => now + chrono::Duration::seconds(60),
            };
            return GithubErrorKind::RateLimited {
                credential: credential.clone(),
                reset,
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
        // surface the raw GitHub message so the operator sees what
        // was actually rejected.
        if message_indicates_dispatch_invalid(message) {
            return GithubErrorKind::DispatchInvalid {
                workflow: workflow.to_string(),
            };
        }
        return GithubErrorKind::Unknown {
            source: anyhow::anyhow!(
                "github 422 on {repo}/{workflow}: {message}",
                repo = repo,
                workflow = workflow,
                message = message,
            ),
        };
    }

    // 429 = explicit rate-limit. Reset header may or may not be
    // present; if absent OR if the parsed reset is already in the past
    // (the header echoed a previous quota window's epoch), default to
    // one minute out so the bucket has a definite future hold-off
    // window. Mirrors the 403/Remaining=0 fallback above and
    // `github::dispatcher::reclassify_403_via_snapshot`.
    if status == StatusCode::TOO_MANY_REQUESTS {
        let now = Utc::now();
        let reset = match rate_limit_reset {
            Some(r) if r > now => r,
            _ => now + chrono::Duration::seconds(60),
        };
        return GithubErrorKind::RateLimited {
            credential: credential.clone(),
            reset,
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

/// True if a 422 message indicates the gcit_run_id case. Detection
/// is permissive: case-insensitive substring match on either
/// "unexpected inputs" (the canonical message) or "gcit_run_id"
/// (the rejected input name as it appears in errors[]).
/// Any 422 not matching this surfaces as Unknown carrying the raw
/// message rather than misleading the operator with a YAML fix that
/// doesn't apply (e.g. when the workflow file has been disabled or
/// the ref doesn't exist).
fn message_indicates_dispatch_invalid(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("unexpected inputs") || lower.contains("gcit_run_id")
}

/// Wrap an `octocrab::Error` into a `GithubErrorKind`. Pulls the
/// status code + message out of the `Error::GitHub` variant and
/// delegates to `classify_status`. Other variants (Hyper, Service,
/// Encoder, Other, etc.) map to:
///
///   - `Service` / `Hyper` / `Encoder` / `Http` -> Transport (Transient)
///   - everything else                          -> Unknown (Transient)
///
/// `request_timeout` is the configured per-request timeout; the
/// caller passes it through so a tokio `Elapsed` translates into a
/// Timeout variant carrying the actual deadline rather than a hard-
/// coded zero.
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

/// Translate a tokio `tokio::time::error::Elapsed` (from
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
        // The Display string carries the operator hint operators
        // grep for. Pin the substring.
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
        // GitHub's response sometimes echoes the previous quota window's
        // `X-RateLimit-Reset` epoch (clock skew, response served from a
        // stale cache, classifier called on a delayed snapshot). A past
        // reset must be treated as missing — using it as-is would tell
        // the dispatcher "retry immediately" against a quota that has
        // not actually opened, which would loop hot. Default to 60s out
        // mirrors the reset-missing fallback and
        // `github::dispatcher::reclassify_403_via_snapshot`.
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
                let lower = before + chrono::Duration::seconds(60);
                let upper = after + chrono::Duration::seconds(60);
                assert!(
                    r >= lower && r <= upper,
                    "stale reset must be replaced with ~now+60s; got {r} not in [{lower}, {upper}]",
                );
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    #[test]
    fn classify_429_with_rate_limit_reset_in_past_uses_default() {
        // 429 path mirrors the 403/Remaining=0 path: a past reset must
        // be treated as missing, defaulting to 60s out so the dispatcher
        // backs off to a real future window rather than retrying
        // immediately against an already-expired hold-off.
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
                let lower = before + chrono::Duration::seconds(60);
                let upper = after + chrono::Duration::seconds(60);
                assert!(
                    reset >= lower && reset <= upper,
                    "stale 429 reset must be replaced with ~now+60s; got {reset} not in [{lower}, {upper}]",
                );
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    #[test]
    fn classify_403_with_rate_limit_remaining_zero_but_reset_missing_is_rate_limited() {
        // GitHub's response sometimes carries `X-RateLimit-Remaining: 0`
        // without `X-RateLimit-Reset`. The 403 still indicates quota
        // exhaustion (Transient), not scope denial (Permanent); fall
        // back to a 60s default reset window mirroring the 429 path.
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
                let lower = before + chrono::Duration::seconds(60);
                let upper = after + chrono::Duration::seconds(60);
                assert!(
                    r >= lower && r <= upper,
                    "default reset must be ~now+60s; got {r} not in [{lower}, {upper}]",
                );
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    #[test]
    fn classify_403_with_rate_limit_header_but_remaining_nonzero_is_forbidden() {
        // X-RateLimit-Remaining: 4500 + 403 means scope denial, not
        // quota exhaustion. Defensive — GitHub's response shape
        // technically allows this combination.
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
        // Per tests/github_422_input_mapping.rs: pin the exact
        // 2-space-indented YAML and the run-name directive.
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
                let lower = before + chrono::Duration::seconds(60);
                let upper = after + chrono::Duration::seconds(60);
                assert!(
                    reset >= lower && reset <= upper,
                    "default reset must be ~now+60s; got {reset} not in [{lower}, {upper}]",
                );
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
        // Defensive: 410 Gone, 451 Unavailable For Legal Reasons —
        // not enumerated in the typed variants but plausible. Map to
        // Unknown so last_error reflects the raw status.
        let c = cred("github_pat");
        let e = classify_status(ctx(StatusCode::GONE, "gone", None, None, &c));
        assert!(matches!(e, GithubErrorKind::Unknown { .. }), "got {e:?}");
    }

    #[test]
    fn retryability_matches_spec() {
        let c = cred("github_pat");
        // Permanent
        for v in [
            GithubErrorKind::Unauthorized {
                credential: c.clone(),
            },
            GithubErrorKind::Forbidden {
                credential: c.clone(),
            },
            GithubErrorKind::WorkflowNotFound {
                repo: "o/r".into(),
                workflow: "ci.yml".into(),
            },
            GithubErrorKind::DispatchInvalid {
                workflow: "ci.yml".into(),
            },
            GithubErrorKind::Cancelled,
            GithubErrorKind::BodyTooLarge {
                declared: Some(20_000_000),
                limit: 16 * 1024 * 1024,
            },
            GithubErrorKind::BodyTooLarge {
                declared: None,
                limit: 16 * 1024 * 1024,
            },
        ] {
            assert_eq!(v.retryability(), Retryability::Permanent, "{v:?}");
            assert!(!v.is_transient(), "{v:?}");
        }
        // Transient
        for v in [
            GithubErrorKind::ServerError {
                status: 500,
                max_attempts: SERVER_ERROR_MAX_ATTEMPTS,
            },
            GithubErrorKind::Timeout {
                timeout: Duration::from_secs(30),
            },
            GithubErrorKind::RateLimited {
                credential: c.clone(),
                reset: Utc::now(),
            },
            GithubErrorKind::Transport {
                source: anyhow::anyhow!("connection refused"),
            },
            GithubErrorKind::Unknown {
                source: anyhow::anyhow!("x"),
            },
        ] {
            assert_eq!(v.retryability(), Retryability::Transient, "{v:?}");
            assert!(v.is_transient(), "{v:?}");
        }
    }

    #[test]
    fn classify_422_unexpected_inputs_is_dispatch_invalid() {
        // 422 with "Unexpected inputs" body indicates the gcit_run_id
        // case → tailored YAML guidance applies.
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
        // Permissive substring detection: GitHub's exact casing may
        // change ("Unexpected Inputs" vs "Unexpected inputs"). Both must
        // surface DispatchInvalid.
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
        // GitHub error responses sometimes name the rejected input in
        // errors[]. Detect the gcit_run_id name as a fallback.
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
        // Per tests/github_422_input_mapping.rs::dispatch_422_with_other_error_message_falls_back_to_generic:
        // 422 not matching the gcit_run_id case must NOT emit the YAML
        // snippet. Instead Unknown carries the raw API message.
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
                assert!(msg.contains("422"), "status code must surface; got: {msg}",);
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
    fn transport_message_pins_will_retry() {
        // Transport variant is distinct from ServerError so operators
        // can tell "GitHub is down" from "the network is broken".
        let e = GithubErrorKind::Transport {
            source: anyhow::anyhow!("connection refused"),
        };
        let msg = format!("{e}");
        assert!(msg.contains("transport"), "msg: {msg}");
        assert!(msg.contains("Will retry"), "msg: {msg}");
        assert!(msg.contains("connection refused"), "msg: {msg}");
    }

    #[test]
    fn retry_after_only_set_for_rate_limited() {
        let c = cred("github_pat");
        let now = Utc::now();
        let reset = now + chrono::Duration::seconds(45);
        let e = GithubErrorKind::RateLimited {
            credential: c.clone(),
            reset,
        };
        let after = e
            .retry_after(now)
            .expect("rate-limited returns retry_after");
        assert!(
            after >= Duration::from_secs(44) && after <= Duration::from_secs(46),
            "retry_after ~= 45s; got {after:?}",
        );
        // Non-rate-limited variants do not surface retry_after.
        for v in [
            GithubErrorKind::Unauthorized {
                credential: c.clone(),
            },
            GithubErrorKind::ServerError {
                status: 500,
                max_attempts: 1,
            },
            GithubErrorKind::Timeout {
                timeout: Duration::from_secs(30),
            },
        ] {
            assert!(v.retry_after(now).is_none(), "{v:?}");
        }
    }

    #[test]
    fn retry_after_zero_when_reset_is_in_past() {
        let c = cred("github_pat");
        let now = Utc::now();
        let reset = now - chrono::Duration::seconds(10);
        let e = GithubErrorKind::RateLimited {
            credential: c,
            reset,
        };
        // Past reset -> zero (don't sleep, retry immediately).
        assert_eq!(e.retry_after(now), Some(Duration::from_secs(0)));
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

    #[test]
    fn unauthorized_message_pins_string() {
        // Pin the literal string operators see.
        let c = cred("github_pat");
        let msg = format!("{}", GithubErrorKind::Unauthorized { credential: c });
        assert_eq!(
            msg,
            "GitHub auth failed (401). PAT for credential 'github_pat' invalid or expired. \
             Re-issue via GitHub UI."
        );
    }

    #[test]
    fn forbidden_message_pins_string() {
        let c = cred("github_pat");
        let msg = format!("{}", GithubErrorKind::Forbidden { credential: c });
        assert_eq!(
            msg,
            "GitHub auth denied (403). PAT for credential 'github_pat' lacks scope. \
             Required: 'workflow' + 'repo' (or 'public_repo' for public repos). Run: gh auth status"
        );
    }

    #[test]
    fn workflow_not_found_message_pins_string() {
        let msg = format!(
            "{}",
            GithubErrorKind::WorkflowNotFound {
                repo: "myorg/linux-builder".into(),
                workflow: "ci.yml".into(),
            }
        );
        assert_eq!(
            msg,
            "Workflow not found: myorg/linux-builder/ci.yml. \
             Verify (a) the file exists in the repository's default branch, \
             (b) `on: workflow_dispatch:` is declared, (c) PAT has access."
        );
    }

    #[test]
    fn rate_limited_message_carries_reset_timestamp() {
        let c = cred("github_pat");
        let reset = DateTime::parse_from_rfc3339("2026-04-26T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let msg = format!(
            "{}",
            GithubErrorKind::RateLimited {
                credential: c,
                reset,
            }
        );
        // The Display uses chrono's default format; pin the reset
        // string substring rather than full equality (chrono's
        // default is RFC3339-ish but version-sensitive).
        assert!(
            msg.contains("'github_pat'") && msg.contains("2026-04-26"),
            "msg: {msg}",
        );
    }

    #[test]
    fn server_error_message_includes_status_and_max_attempts() {
        let msg = format!(
            "{}",
            GithubErrorKind::ServerError {
                status: 503,
                max_attempts: SERVER_ERROR_MAX_ATTEMPTS,
            }
        );
        assert!(msg.contains("503"), "msg: {msg}");
        assert!(
            msg.contains(&SERVER_ERROR_MAX_ATTEMPTS.to_string()),
            "msg: {msg}",
        );
    }

    #[test]
    fn timeout_message_pins_will_retry() {
        let msg = format!(
            "{}",
            GithubErrorKind::Timeout {
                timeout: Duration::from_secs(30),
            }
        );
        assert!(
            msg.contains("30s") || msg.contains("Duration"),
            "msg: {msg}"
        );
        assert!(msg.contains("Will retry"), "msg: {msg}");
    }

    #[test]
    fn unknown_message_carries_source() {
        let msg = format!(
            "{}",
            GithubErrorKind::Unknown {
                source: anyhow::anyhow!("rare situation"),
            }
        );
        assert!(msg.contains("uncategorised"), "msg: {msg}");
        assert!(msg.contains("rare situation"), "msg: {msg}");
    }
}

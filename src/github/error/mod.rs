// `GithubErrorKind` — typed classification of every GitHub-side
// failure the dispatcher / monitor / correlator / rate-limit poller
// can hit.
//
// The Display strings are operator-visible and stable; operators
// copy them out of journald/CLI output and grep for them in their
// workflow files. Drift breaks that workflow.
//
// Layout:
//   - `mod.rs`     — `Retryability`, `GithubErrorKind` enum + impl,
//                    `SERVER_ERROR_MAX_ATTEMPTS`.
//   - `classify`   — pure-logic classifier (`StatusContext`,
//                    `classify_status`), octocrab adapter
//                    (`classify`), and `timeout_error` constructor.

use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::config::CredentialId;

mod classify;

pub use classify::{classify, classify_status, timeout_error, StatusContext};

/// Retry-policy classification used by backon's `.when(predicate)`
/// retry guard. Transient = retryable (network blip, 5xx, rate-limit
/// will pass); Permanent = not (PAT scope wrong, workflow YAML wrong).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Retryability {
    Transient,
    Permanent,
}

/// GitHub error variants. Each variant produces a Display string
/// operators act on directly. The strings are pinned byte-for-byte
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
    /// YAML missing the `gcit_run_id` input declaration. Display
    /// includes a copy-paste YAML snippet (indentation is pinned;
    /// YAML is whitespace-sensitive). The doubled `{{ }}` escapes
    /// the GitHub Actions template syntax inside thiserror's format.
    #[error("GitHub rejected workflow_dispatch (422 Unprocessable Entity). Most common cause: workflow YAML lacks `workflow_dispatch:` in `on:`. Add this to .github/workflows/{workflow}:\n\non:\n  workflow_dispatch:\n    inputs:\n      gcit_run_id:\n        type: string\n\nrun-name: gcit-${{{{ inputs.gcit_run_id }}}}\n\nThen commit, push to default branch, and try again.")]
    DispatchInvalid { workflow: String },

    /// HTTP 403 *with* `X-RateLimit-Remaining: 0`, or HTTP 429.
    /// Quota exhausted; gcit defers requests until the reset epoch.
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
    /// `ServerError` so operators can tell "GitHub is down" from
    /// "the network is broken". Retryable.
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
    /// (SIGHUP), and shutdown is final (SIGTERM/SIGINT). Operators
    /// reading last_error can tell which fired by checking the daemon
    /// log line preceding this error.
    #[error(
        "dispatch cancelled by supervisor (SIGHUP reload OR SIGTERM/SIGINT shutdown — see daemon log line preceding this error to identify which); after a reload the new generation re-fires from the next poll, after shutdown the work is intentionally dropped"
    )]
    Cancelled,

    /// GitHub API response declared (or streamed) more bytes than
    /// `client::RESPONSE_BODY_LIMIT` (16 MiB). Permanent: a body
    /// over the cap is a misbehaving server or a gzip-bomb-shaped
    /// attack from upstream; retrying gets the same response.
    /// `declared = Some(d)` is the Content-Length pre-check path;
    /// `declared = None` is the mid-stream `LengthLimitError` path.
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
        /// Carried by value so the message is self-contained.
        limit: usize,
    },
}

impl GithubErrorKind {
    /// Drive the retry decision. Permanent errors short-circuit
    /// backon's loop; Transient errors retry until the schedule
    /// exhausts. Per `github_error_classifier::classifier_dispatches_*`:
    ///   ServerError      -> Transient
    ///   Timeout          -> Transient
    ///   RateLimited      -> Transient (caller awaits reset before next attempt)
    ///   Transport        -> Transient
    ///   Unknown          -> Transient (best-effort retry)
    ///   Unauthorized     -> Permanent
    ///   Forbidden        -> Permanent
    ///   WorkflowNotFound -> Permanent
    ///   DispatchInvalid  -> Permanent
    ///   Cancelled        -> Permanent
    ///   BodyTooLarge     -> Permanent
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
    /// sleep until quota is restored before issuing the next attempt.
    /// Returns `Some(Duration::ZERO)` when the reset is in the past
    /// (retry immediately); `None` for non-RateLimited variants.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::test_cred as cred;

    #[test]
    fn retryability_matches_spec() {
        let c = cred("github_pat");
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
    fn unauthorized_message_pins_string() {
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
        // Display uses chrono's default format; pin the reset
        // substring (chrono's RFC3339-ish default is version-sensitive).
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
    fn transport_message_pins_will_retry() {
        let e = GithubErrorKind::Transport {
            source: anyhow::anyhow!("connection refused"),
        };
        let msg = format!("{e}");
        assert!(msg.contains("transport"), "msg: {msg}");
        assert!(msg.contains("Will retry"), "msg: {msg}");
        assert!(msg.contains("connection refused"), "msg: {msg}");
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

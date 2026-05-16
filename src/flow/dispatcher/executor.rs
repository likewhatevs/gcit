// `DispatchExecutor` seam + `RealDispatchExecutor` + `DispatchError`.
//
// The seam lets the supervisor end-to-end test harness inject a
// scripted (dispatch + correlate) outcome stream so the
// post-execute lifecycle (RunStarted, monitor spawn, fan-out) can
// be exercised without wiremock + octocrab.

use std::future::Future;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio_util::sync::CancellationToken;

use crate::git::rate_bucket::RateBucket;
use crate::github::client::Client as GithubClient;
use crate::github::correlator::{self, CorrelateParams, CorrelationError};
use crate::github::dispatcher::{self as gh_dispatcher, DispatchOutcome, DispatchParams};
use crate::github::error::GithubErrorKind;
use crate::github::rate_limit::RateLimitState;

/// Test seam for the (dispatch + correlate) GitHub round-trip pair.
/// Production wires `RealDispatchExecutor`; integration tests inject
/// a scripted impl. Uses native async-fn-in-trait so callers take
/// `&E: DispatchExecutor` and avoid the `Pin<Box<dyn Future>>`
/// allocation per trigger.
pub trait DispatchExecutor: Send + Sync {
    fn execute(
        &self,
        dispatch_params: DispatchParams,
        branch: String,
        head_sha: String,
        cancel: CancellationToken,
    ) -> impl Future<Output = ExecuteOutcome> + Send;
}

/// Outcome of one (dispatch + correlate) round-trip.
pub enum ExecuteOutcome {
    /// Both stages succeeded. `handle_trigger` emits RunStarted,
    /// fans out `on_run_start`, and spawns the monitor.
    Success {
        dispatch: DispatchOutcome,
        correlation: correlator::CorrelationOutcome,
    },
    /// Dispatch stage failed (terminal or transient retry exhausted).
    /// Routed through `DispatchError::from_github_error("dispatch", ..)`.
    DispatchFailed(GithubErrorKind),
    /// Correlate stage failed (timeout, duplicate match, or a
    /// permanent GitHub error during scan). Routed through
    /// `DispatchError::from_correlation_error("correlate", ..)`.
    CorrelateFailed(CorrelationError),
}

/// Production executor: wraps `dispatch_with_retry` + `correlate`
/// against a real `GithubClient` plus per-credential pacing state.
/// The supervisor builds one per flow.
pub struct RealDispatchExecutor {
    pub github_client: Arc<GithubClient>,
    pub rate_bucket: Arc<RateBucket>,
    pub rate_limit: Arc<RateLimitState>,
    /// Retry budget passed to `dispatch_with_retry`. Supervisor sets
    /// `DISPATCH_MAX_ATTEMPTS`; tests script their own value.
    pub max_attempts: u32,
}

impl DispatchExecutor for RealDispatchExecutor {
    async fn execute(
        &self,
        dispatch_params: DispatchParams,
        branch: String,
        head_sha: String,
        cancel: CancellationToken,
    ) -> ExecuteOutcome {
        let outcome = match gh_dispatcher::dispatch_with_retry(
            self.github_client.as_ref(),
            Arc::clone(&self.rate_bucket),
            Arc::clone(&self.rate_limit),
            dispatch_params,
            self.max_attempts,
            cancel.clone(),
        )
        .await
        {
            Ok(o) => o,
            Err(e) => return ExecuteOutcome::DispatchFailed(e),
        };

        let correlate_params = CorrelateParams {
            repo: outcome.repo.clone(),
            workflow: outcome.workflow.clone(),
            gcit_run_id: outcome.gcit_run_id,
            branch,
            head_sha,
            dispatched_at: outcome.dispatched_at,
            run_name_configured: None,
        };
        match correlator::correlate(
            self.github_client.as_ref(),
            self.rate_limit.as_ref(),
            &correlate_params,
            cancel,
        )
        .await
        {
            Ok(c) => ExecuteOutcome::Success {
                dispatch: outcome,
                correlation: c,
            },
            Err(e) => ExecuteOutcome::CorrelateFailed(e),
        }
    }
}

/// Operator-visible error from a single dispatch attempt. Carries
/// the stringified message + the typed `kind` (matches the
/// discriminator the supervisor records under `last_error.kind`) +
/// an optional `retry_at` extracted from
/// `GithubErrorKind::RateLimited.reset` (used by `gcit status` to
/// render "next retry at ...").
#[derive(Debug)]
pub(super) struct DispatchError {
    pub(super) kind: &'static str,
    pub(super) message: String,
    pub(super) retry_at: Option<DateTime<Utc>>,
}

impl DispatchError {
    pub(super) fn from_github_error(stage: &'static str, e: &GithubErrorKind) -> Self {
        let retry_at = match e {
            GithubErrorKind::RateLimited { reset, .. } => Some(*reset),
            _ => None,
        };
        Self {
            kind: stage,
            message: format!("{stage}: {e}"),
            retry_at,
        }
    }

    /// Unwrap `CorrelationError::Github(...)` so the inner
    /// `RateLimited.reset` flows into `retry_at`; non-Github
    /// correlation errors carry no retry hint.
    pub(super) fn from_correlation_error(stage: &'static str, e: &CorrelationError) -> Self {
        let retry_at = match e {
            CorrelationError::Github(GithubErrorKind::RateLimited { reset, .. }) => Some(*reset),
            _ => None,
        };
        Self {
            kind: stage,
            message: format!("{stage}: {e}"),
            retry_at,
        }
    }

    pub(super) fn from_message(kind: &'static str, message: String) -> Self {
        Self {
            kind,
            message,
            retry_at: None,
        }
    }
}

impl std::fmt::Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use uuid::Uuid;

    #[test]
    fn dispatch_error_from_github_error_rate_limited_carries_retry_at() {
        use crate::config::CredentialId;
        let reset = chrono::Utc::now() + chrono::Duration::minutes(15);
        let e = GithubErrorKind::RateLimited {
            credential: CredentialId::new("gh-pat").expect("valid id"),
            reset,
        };
        let de = DispatchError::from_github_error("dispatch", &e);
        assert_eq!(de.kind, "dispatch");
        assert!(de.message.starts_with("dispatch:"));
        assert_eq!(de.retry_at, Some(reset));
    }

    #[test]
    fn dispatch_error_from_github_error_non_rate_limited_leaves_retry_at_none() {
        // Only RateLimited carries a wall-clock hint.
        use crate::config::CredentialId;
        let e = GithubErrorKind::Unauthorized {
            credential: CredentialId::new("gh-pat").expect("valid id"),
        };
        let de = DispatchError::from_github_error("dispatch", &e);
        assert_eq!(de.kind, "dispatch");
        assert!(de.message.starts_with("dispatch:"));
        assert!(de.retry_at.is_none());
    }

    #[test]
    fn dispatch_error_from_correlation_error_unwraps_github_rate_limited() {
        // Github(RateLimited) is the only CorrelationError variant
        // that surfaces a retry hint.
        use crate::config::CredentialId;
        let reset = chrono::Utc::now() + chrono::Duration::minutes(30);
        let e = CorrelationError::Github(GithubErrorKind::RateLimited {
            credential: CredentialId::new("gh-pat").expect("valid id"),
            reset,
        });
        let de = DispatchError::from_correlation_error("correlate", &e);
        assert_eq!(de.kind, "correlate");
        assert!(de.message.starts_with("correlate:"));
        assert_eq!(de.retry_at, Some(reset));
    }

    #[test]
    fn dispatch_error_from_correlation_error_duplicate_match_has_no_retry_at() {
        let e = CorrelationError::DuplicateMatch {
            repo: "myorg/linux-builder".to_string(),
            workflow: "ci.yml".to_string(),
            gcit_run_id: Uuid::nil(),
            run_ids: vec![101, 102],
        };
        let de = DispatchError::from_correlation_error("correlate", &e);
        assert_eq!(de.kind, "correlate");
        assert!(de.message.starts_with("correlate:"));
        // Both run ids must surface in the body so operators reading
        // journalctl can identify the colliding runs.
        assert!(de.message.contains("101") && de.message.contains("102"));
        assert!(de.retry_at.is_none());
    }

    #[test]
    fn dispatch_error_from_correlation_error_timeout_has_no_retry_at() {
        let e = CorrelationError::Timeout {
            repo: "myorg/linux-builder".to_string(),
            workflow: "ci.yml".to_string(),
            gcit_run_id: Uuid::nil(),
            timeout: Duration::from_secs(30),
        };
        let de = DispatchError::from_correlation_error("correlate", &e);
        assert_eq!(de.kind, "correlate");
        assert!(de.retry_at.is_none());
    }

    #[test]
    fn dispatch_error_from_correlation_error_github_non_rate_limited_has_no_retry_at() {
        use crate::config::CredentialId;
        let e = CorrelationError::Github(GithubErrorKind::Unauthorized {
            credential: CredentialId::new("gh-pat").expect("valid id"),
        });
        let de = DispatchError::from_correlation_error("correlate", &e);
        assert!(de.retry_at.is_none());
    }

    #[test]
    fn dispatch_error_from_message_carries_kind_and_body_with_no_retry_at() {
        let de = DispatchError::from_message(
            "input_render",
            "input render failed: undefined.var".to_string(),
        );
        assert_eq!(de.kind, "input_render");
        assert_eq!(de.message, "input render failed: undefined.var");
        assert!(de.retry_at.is_none());
    }

    #[test]
    fn dispatch_error_display_writes_message_body_verbatim() {
        let de = DispatchError::from_message("k", "raw body".to_string());
        assert_eq!(format!("{de}"), "raw body");
    }
}

// Strategy-specific poll execution.
//
// `PollExecutor` is the seam that lets the supervisor end-to-end test
// harness in `tests/poll_unborn_ref.rs` swap a `ScriptedPollExecutor`
// in for `RealPollExecutor` so the loop body can be exercised without
// real network transports. Production goes through `super::run`,
// which constructs a `RealPollExecutor`.
//
// All `pub` items in this module are reachable from integration
// tests via `gcit::flow::poll::*` (re-exported from `mod.rs`) so
// scripted executors can be defined externally.

use std::future::Future;

use tokio_util::sync::CancellationToken;

use crate::git::{
    auto_detect, github_api, grokmirror, ls_remote, strategy, PollOutcome, PollStrategy,
};

use super::PollParams;

/// Result of a single poll cycle. `Cancelled` is supervisor-driven
/// control flow (SIGHUP / shutdown) and must NOT be recorded as a
/// `last_error` — that would surface "git_poll_failed: cancelled" in
/// `gcit status` during the respawn window. `Failed(String)` carries
/// the rendered error text the loop records under
/// kind="git_poll_failed".
#[derive(Debug)]
pub enum PollCycleError {
    Cancelled,
    Failed(String),
}

impl std::fmt::Display for PollCycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled => f.write_str("cancelled"),
            Self::Failed(s) => f.write_str(s),
        }
    }
}

/// Pluggable strategy executor — the seam tests use to drive the
/// loop with a scripted outcome stream. The loop consumes
/// `E: PollExecutor` as a generic type parameter, so dyn-trait
/// erasure isn't needed.
///
/// Not sealed (no `Sealed` supertrait): `tests/poll_unborn_ref.rs`
/// and `tests/supervisor_loop_factories.rs` are separate cargo crates
/// that ship `ScriptedPollExecutor` impls for harnessing the loop.
/// Sealing would force the harness through a `pub` test-support
/// feature flag or relocating those tests into `#[cfg(test)] mod`
/// blocks; neither buys safety for a binary-only crate that doesn't
/// publish its lib surface (see `lib.rs`).
#[doc(hidden)]
pub trait PollExecutor: Send + Sync {
    /// Strategy label for the startup info! log. Production:
    /// `"github_api"` | `"grokmirror"` | `"ls_remote"`. Tests pick a
    /// stable opaque label so log readers can distinguish harnessed
    /// runs.
    fn strategy_label(&self) -> &'static str;

    /// Issue one poll cycle. The executor owns the strategy choice
    /// (production = `auto_detect`, tests = pre-baked) AND any
    /// per-cycle state that crosses cycles (e.g. grokmirror's
    /// fingerprint cache). The loop body is strategy-agnostic.
    fn poll_cycle(
        &self,
        params: &PollParams,
        cancel: &CancellationToken,
    ) -> impl Future<Output = Result<PollOutcome, PollCycleError>> + Send;
}

/// Production executor: detects the strategy from the URL once at
/// construction and dispatches each cycle through `poll_one`. Owns
/// the grokmirror fingerprint cache (the only per-cycle state that
/// crosses cycles in production).
pub struct RealPollExecutor {
    strategy: PollStrategy,
    /// `Mutex` is for trait-shape compatibility (`&self` method);
    /// the loop only ever calls `poll_cycle` serially.
    grokmirror_fingerprint: tokio::sync::Mutex<Option<String>>,
}

impl RealPollExecutor {
    pub fn for_url(url: &str) -> Self {
        Self {
            strategy: auto_detect(url),
            grokmirror_fingerprint: tokio::sync::Mutex::new(None),
        }
    }
}

impl PollExecutor for RealPollExecutor {
    fn strategy_label(&self) -> &'static str {
        strategy::kind_str(self.strategy)
    }

    async fn poll_cycle(
        &self,
        params: &PollParams,
        cancel: &CancellationToken,
    ) -> Result<PollOutcome, PollCycleError> {
        let mut fingerprint = self.grokmirror_fingerprint.lock().await;
        poll_one(self.strategy, params, &mut fingerprint, cancel).await
    }
}

/// Issue one strategy-specific poll. The loop continues on cadence
/// regardless of (Permanent | Transient) classification — per-strategy
/// retry semantics are captured in the `git::*::poll` implementations
/// (e.g., GitHub-API 404 -> `UnbornRef`, not `Err`).
async fn poll_one(
    strategy: PollStrategy,
    params: &PollParams,
    grokmirror_fingerprint: &mut Option<String>,
    cancel: &CancellationToken,
) -> Result<PollOutcome, PollCycleError> {
    match strategy {
        PollStrategy::GithubApi => {
            let octo = params.octo.as_ref().ok_or_else(|| {
                PollCycleError::Failed("github_api strategy requires Octocrab handle".to_string())
            })?;
            let (owner, repo) = split_github_repo(&params.url)
                .map_err(|e| PollCycleError::Failed(format!("github URL parse: {e}")))?;
            tokio::select! {
                _ = cancel.cancelled() => Err(PollCycleError::Cancelled),
                r = github_api::poll(octo, &owner, &repo, &params.ref_name) => {
                    r.map_err(|e| PollCycleError::Failed(format!("{e}")))
                }
            }
        }
        PollStrategy::Grokmirror => {
            let client = params.reqwest.as_ref().ok_or_else(|| {
                PollCycleError::Failed("grokmirror strategy requires reqwest handle".to_string())
            })?;
            let base = base_url(&params.url).ok_or_else(|| {
                PollCycleError::Failed("grokmirror URL must carry a host".to_string())
            })?;
            let repo_path = grokmirror::extract_repo_path_from_url(&params.url)
                .map_err(|e| PollCycleError::Failed(format!("grokmirror repo path: {e}")))?;
            let manifest = tokio::select! {
                _ = cancel.cancelled() => return Err(PollCycleError::Cancelled),
                r = grokmirror::fetch_manifest(client, &base) => {
                    r.map_err(|e| PollCycleError::Failed(format!("{e}")))?
                }
            };
            match grokmirror::lookup_fingerprint(&manifest, &repo_path) {
                Ok(fp) => {
                    let new_fp = fp.to_string();
                    let unchanged = grokmirror_fingerprint
                        .as_deref()
                        .map(|prev| prev == new_fp.as_str())
                        .unwrap_or(false);
                    *grokmirror_fingerprint = Some(new_fp);
                    if unchanged {
                        Ok(PollOutcome::Unchanged)
                    } else {
                        // Fingerprint changed but the manifest only
                        // proves "something changed"; resolve the ref
                        // -> SHA via ls-remote.
                        ls_remote_inline(&params.url, &params.ref_name, cancel).await
                    }
                }
                Err(grokmirror::GrokmirrorError::RepoNotInManifest { .. }) => {
                    Ok(PollOutcome::UnbornRef)
                }
                Err(e) => Err(PollCycleError::Failed(format!("grokmirror lookup: {e}"))),
            }
        }
        PollStrategy::LsRemote => ls_remote_inline(&params.url, &params.ref_name, cancel).await,
    }
}

/// `ls_remote::poll` with cancellation. `poll` owns its own
/// `tokio::time::timeout`; this wrapper just races against `cancel`.
pub(super) async fn ls_remote_inline(
    url: &str,
    ref_name: &str,
    cancel: &CancellationToken,
) -> Result<PollOutcome, PollCycleError> {
    tokio::select! {
        _ = cancel.cancelled() => Err(PollCycleError::Cancelled),
        r = ls_remote::poll(url.to_string(), ref_name.to_string()) => {
            r.map_err(|e| PollCycleError::Failed(format!("{e}")))
        }
    }
}

/// Extract `scheme://host` from a URL for `grokmirror::fetch_manifest`.
fn base_url(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    let host = parsed.host_str()?;
    let scheme = parsed.scheme();
    Some(format!("{scheme}://{host}"))
}

/// Parse `https://github.com/owner/repo[.git]` into `(owner, repo)`.
fn split_github_repo(url: &str) -> Result<(String, String), String> {
    let parsed = url::Url::parse(url).map_err(|e| format!("{e}"))?;
    let mut segments = parsed
        .path_segments()
        .ok_or_else(|| "github URL has no path".to_string())?
        .filter(|s| !s.is_empty());
    let owner = segments.next().ok_or_else(|| "missing owner".to_string())?;
    let repo = segments.next().ok_or_else(|| "missing repo".to_string())?;
    let repo = repo.strip_suffix(".git").unwrap_or(repo);
    Ok((owner.to_string(), repo.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flow::poll::EffectivePoll;
    use crate::util::ensure_crypto_provider;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn split_github_repo_strips_dot_git() {
        let (o, r) = split_github_repo("https://github.com/owner/repo.git").unwrap();
        assert_eq!(o, "owner");
        assert_eq!(r, "repo");
    }

    #[test]
    fn split_github_repo_no_dot_git() {
        let (o, r) = split_github_repo("https://github.com/owner/repo").unwrap();
        assert_eq!(o, "owner");
        assert_eq!(r, "repo");
    }

    #[test]
    fn base_url_strips_path() {
        assert_eq!(
            base_url("https://git.kernel.org/pub/scm/foo.git"),
            Some("https://git.kernel.org".to_string()),
        );
    }

    #[test]
    fn split_github_repo_rejects_url_without_owner_and_repo() {
        let err = split_github_repo("https://github.com/").expect_err("must error");
        assert!(
            err.contains("missing owner"),
            "no-path URL must surface 'missing owner'; got: {err}",
        );
    }

    #[test]
    fn split_github_repo_rejects_url_with_only_owner() {
        let err = split_github_repo("https://github.com/owner").expect_err("must error");
        assert!(
            err.contains("missing repo"),
            "owner-only URL must surface 'missing repo'; got: {err}",
        );
    }

    #[test]
    fn split_github_repo_rejects_unparseable_url() {
        let err = split_github_repo("not a url at all").expect_err("must error");
        assert!(!err.is_empty(), "unparseable URL must surface error");
    }

    #[test]
    fn base_url_returns_none_for_unparseable_input() {
        assert!(base_url("not a url").is_none());
    }

    #[test]
    fn poll_cycle_error_failed_display_surfaces_inner_message_verbatim() {
        let e = PollCycleError::Failed("ls-refs parse: bad agent".to_string());
        assert_eq!(e.to_string(), "ls-refs parse: bad agent");
    }

    #[test]
    fn poll_cycle_error_cancelled_display_is_lowercase_cancelled() {
        assert_eq!(PollCycleError::Cancelled.to_string(), "cancelled");
    }

    fn params_with_url(url: &str) -> PollParams {
        PollParams {
            flow_name: "test-flow".to_string(),
            url: url.to_string(),
            ref_name: "refs/heads/main".to_string(),
            effective_poll: EffectivePoll {
                source_interval: Duration::from_secs(60),
                job_interval: Duration::from_secs(30),
                jitter: 0.0,
                cooldown: Duration::ZERO,
            },
            rate_bucket: None,
            octo: None,
            reqwest: None,
            last_errors: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
        }
    }

    /// Missing-handle gate: in production the supervisor wires
    /// `params.octo` whenever auto_detect picks GithubApi, so this
    /// arm is unreachable. Pin the gate so a future harness or
    /// refactor that wires partial params gets a clear `Failed`
    /// rather than a panic.
    #[tokio::test]
    async fn poll_one_github_api_errors_when_octocrab_handle_missing() {
        let params = params_with_url("https://github.com/owner/repo");
        let cancel = CancellationToken::new();
        let mut fp: Option<String> = None;
        let err = poll_one(PollStrategy::GithubApi, &params, &mut fp, &cancel)
            .await
            .expect_err("missing octocrab must surface error");
        match err {
            PollCycleError::Failed(msg) => assert!(
                msg.contains("requires Octocrab handle"),
                "error must name the missing handle; got: {msg}",
            ),
            other => panic!("expected Failed; got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn poll_one_grokmirror_errors_when_reqwest_handle_missing() {
        let params = params_with_url("https://git.kernel.org/pub/scm/foo.git");
        let cancel = CancellationToken::new();
        let mut fp: Option<String> = None;
        let err = poll_one(PollStrategy::Grokmirror, &params, &mut fp, &cancel)
            .await
            .expect_err("missing reqwest must surface error");
        match err {
            PollCycleError::Failed(msg) => assert!(
                msg.contains("requires reqwest handle"),
                "error must name the missing handle; got: {msg}",
            ),
            other => panic!("expected Failed; got: {other:?}"),
        }
    }

    /// Grokmirror arm derives the manifest base via `base_url`; a
    /// hostless URL (file://) is rejected with "must carry a host".
    /// Future reqwest versions may eagerly construct TLS at builder
    /// time, so install the `ring` provider via the shared Once
    /// helper for robustness.
    #[tokio::test]
    async fn poll_one_grokmirror_errors_when_url_lacks_host() {
        ensure_crypto_provider();
        let mut params = params_with_url("file:///local/repo");
        params.reqwest = Some(Arc::new(reqwest::Client::new()));
        let cancel = CancellationToken::new();
        let mut fp: Option<String> = None;
        let err = poll_one(PollStrategy::Grokmirror, &params, &mut fp, &cancel)
            .await
            .expect_err("hostless URL must surface error");
        match err {
            PollCycleError::Failed(msg) => assert!(
                msg.contains("must carry a host"),
                "error must name the missing host; got: {msg}",
            ),
            other => panic!("expected Failed; got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn poll_one_github_api_errors_when_url_fails_to_parse() {
        ensure_crypto_provider();
        let mut params = params_with_url("not a url at all");
        params.octo = Some(Arc::new(
            octocrab::Octocrab::builder().build().expect("octocrab"),
        ));
        let cancel = CancellationToken::new();
        let mut fp: Option<String> = None;
        let err = poll_one(PollStrategy::GithubApi, &params, &mut fp, &cancel)
            .await
            .expect_err("unparseable URL must surface error");
        match err {
            PollCycleError::Failed(msg) => assert!(
                msg.contains("github URL parse"),
                "error must lead with the parse-stage prefix; got: {msg}",
            ),
            other => panic!("expected Failed; got: {other:?}"),
        }
    }

    /// `ls_remote_inline` (used by LsRemote + Grokmirror arms) must
    /// surface cancellation as `PollCycleError::Cancelled`, not
    /// `Failed("cancelled")`. This pins the typed-enum split that
    /// keeps `git_poll_failed: cancelled` out of `gcit status`
    /// during a SIGHUP cancel-and-respawn.
    #[tokio::test]
    async fn ls_remote_inline_returns_cancelled_variant_on_pre_cancelled_token() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let result = ls_remote_inline(
            "file:///nonexistent/cancelled-test-repo",
            "refs/heads/main",
            &cancel,
        )
        .await;
        match result {
            Err(PollCycleError::Cancelled) => {}
            other => panic!("expected Cancelled, got {other:?}"),
        }
    }
}

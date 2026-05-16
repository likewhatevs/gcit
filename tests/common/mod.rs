// Shared helpers for the GitHub-side integration tests.
//
// Cargo treats files under `tests/common/` (with mod.rs) as
// support modules — they are not compiled as integration tests on
// their own and only land in a binary when a top-level
// `tests/<name>.rs` declares `mod common;`. Each such file then
// gets its own private copy of these symbols, which suppresses
// dead-code warnings caused by partial use across tests.
//
// Consumed by integration tests that need crypto-provider init,
// euid skip-guards, the RecordingNotifier, or GitHub mock helpers.
//
// `tracing-test` no-env-filter feature: tests that use `#[traced_test]`
// to assert on log output rely on the `no-env-filter` Cargo feature
// (declared in dev-dependencies, see Cargo.toml). Cargo treats every
// `tests/<name>.rs` as its own crate, so the per-crate filter
// `tracing-test` installs by default would scope event capture to
// that test crate — hiding events emitted by `gcit` (the lib under
// test). `no-env-filter` widens the env filter to "trace" across all
// crates so events `gcit::flow::supervisor::record_last_error` (or
// any other production code) emits land in the capture buffer where
// `logs_contain` can find them.

#![allow(dead_code)]

pub mod bare_repo;
pub mod fake_daemon;
pub mod recording_notifier;

use std::collections::BTreeMap;
use std::sync::Once;
use std::time::Duration;

use chrono::{DateTime, Utc};
use secrecy::SecretString;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use wiremock::MockServer;

use gcit::config::CredentialId;
use gcit::git::rate_bucket::RateBucket;
use gcit::github::client::Client;
use gcit::github::dispatcher::{dispatch, DispatchParams};
use gcit::github::error::GithubErrorKind;
use gcit::github::rate_limit::RateLimitState;

/// Test PAT carrying a structurally distinct literal so redaction
/// assertions can search for it. Matches the GitHub fine-grained PAT
/// prefix gcit's `Client::builder().build()` requires.
pub const PAT: &str = "github_pat_test_token_for_wiremock_only_no_real_secret";

/// Canonical wiremock route every test mounts a Mock against.
/// `/repos/{owner}/{repo}/actions/workflows/{workflow}/dispatches` is
/// the route `octocrab._post` writes when `dispatcher::dispatch`
/// fires; pinning the literal in one place keeps wiremock matchers
/// aligned with the production URL builder in
/// `src/github/dispatcher.rs::dispatch`.
pub const DISPATCH_PATH: &str = "/repos/myorg/linux-builder/actions/workflows/ci.yml/dispatches";

/// Canonical wiremock route for the workflow-runs list endpoint.
/// `/repos/{owner}/{repo}/actions/workflows/{workflow}/runs` is the
/// route the correlator's `list-runs` GET writes when it scans for a
/// dispatched run by `gcit_run_id` substring or `head_sha` fallback.
/// Pinning the literal in one place keeps wiremock matchers aligned
/// with the production URL builder in `src/github/correlator.rs`.
pub const RUNS_PATH: &str = "/repos/myorg/linux-builder/actions/workflows/ci.yml/runs";

/// Install rustls' ring CryptoProvider once across the test process.
/// octocrab's builder constructs an internal hyper-rustls client that
/// requires a process-level provider; integration tests do not run
/// `main()`, so each crate must install its own. std::sync::Once
/// guarantees idempotency when multiple integration crates call this
/// in parallel.
pub fn ensure_crypto_provider() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        // install_default returns Err when a provider is already
        // installed; treat that as success — another integration test
        // crate may have installed first.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// `true` if the current effective uid is 0 (root). Tests that exercise
/// DAC-mode-bit failure modes (EACCES at open(2), 0o000 traversals)
/// must skip under root because CAP_DAC_OVERRIDE bypasses the
/// permission check and the targeted error never surfaces. Centralized
/// here so the eprintln "<test_name>: skipped — <reason>" callers all
/// share one geteuid() wrapper.
pub fn euid_is_root() -> bool {
    // SAFETY: `geteuid` is async-signal-safe and cannot fail.
    let euid = unsafe { libc::geteuid() };
    euid == 0
}

/// Build a `Client`, `RateBucket`, and pre-seeded `RateLimitState`
/// pointed at `mock_uri`. The snapshot is seeded with full quota +
/// a future reset so `should_defer` returns None and the dispatch
/// fires immediately.
///
/// Without the seed, `RateLimitState::new()` produces an unobserved
/// snapshot, and `should_defer` returns `Some(POLL_INTERVAL=60s)` so
/// the rate-limit poller can populate it before the first request —
/// correct production behaviour but a 60s stall under wiremock.
/// Seeding with remaining > 0 short-circuits the unobserved-defer
/// arm.
///
/// Tests that need a different seed shape (e.g. stale-reset for the
/// 403/Remaining=0/no-Reset reclassifier path) construct deps inline
/// rather than calling this helper.
pub async fn build_dispatch_deps(mock_uri: &str) -> (Client, RateBucket, RateLimitState) {
    let client = Client::builder()
        .credential(CredentialId::new("github_pat").expect("valid id"))
        .token(SecretString::from(PAT.to_string()))
        .request_timeout(Duration::from_secs(5))
        .base_uri(mock_uri)
        .build()
        .expect("client build");
    let rate_bucket = RateBucket::new(Duration::from_millis(0));
    let rate_limit = RateLimitState::new();
    rate_limit
        .observe_full(5000, 5000, Utc::now() + chrono::Duration::seconds(3600))
        .await;
    (client, rate_bucket, rate_limit)
}

/// Default DispatchParams targeting the canonical wiremock route.
/// Each test mounts a Mock matching this exact path/method.
pub fn default_params() -> DispatchParams {
    DispatchParams {
        repo: "myorg/linux-builder".to_string(),
        workflow: "ci.yml".to_string(),
        ref_name: "refs/heads/main".to_string(),
        gcit_run_id: Uuid::nil(),
        rendered_inputs: BTreeMap::new(),
    }
}

/// Drive a single dispatch against `mock` and return the classified
/// error. Every status-code test follows the same shape; this helper
/// keeps per-test setup focused on the status/header/body the test
/// actually pins.
pub async fn drive_dispatch(mock: &MockServer) -> GithubErrorKind {
    let (client, rate_bucket, rate_limit) = build_dispatch_deps(&mock.uri()).await;
    let params = default_params();
    let cancel = CancellationToken::new();
    dispatch(&client, &rate_bucket, &rate_limit, &params, &cancel)
        .await
        .expect_err("non-success status must surface as Err")
}

/// Construct a JSON value matching octocrab's `Run` schema (cf.
/// octocrab's `models::workflows::Run`). Every non-Option field
/// octocrab's deserializer expects must be present or the response
/// body fails to deserialize. Used by tests that mount a list-runs
/// response with a single matching run for the correlator's name-
/// substring or head_sha fallback path.
///
/// This is the 4-arg variant `(id, name, head_sha, created_at)` used
/// across flow_dispatcher_pipeline / flow_dispatcher_failures /
/// github_run_correlation / github_body_limit. The
/// github_monitor_lifecycle test takes `(status, conclusion,
/// created_at)`; that 3-arg shape is kept local to its file since
/// it diverges from this canonical signature.
pub fn make_run_json(id: u64, name: &str, head_sha: &str, created_at: DateTime<Utc>) -> Value {
    json!({
        "id": id,
        "workflow_id": 7,
        "node_id": format!("MDEwOlJ1bk5vZGUx{id}"),
        "name": name,
        "head_branch": "main",
        "head_sha": head_sha,
        "run_number": id,
        "event": "workflow_dispatch",
        "status": "in_progress",
        "conclusion": null,
        "created_at": created_at.to_rfc3339(),
        "updated_at": created_at.to_rfc3339(),
        "url": format!("https://api.github.com/repos/myorg/linux-builder/actions/runs/{id}"),
        "html_url": format!("https://github.com/myorg/linux-builder/actions/runs/{id}"),
        "jobs_url": format!("https://api.github.com/repos/myorg/linux-builder/actions/runs/{id}/jobs"),
        "logs_url": format!("https://api.github.com/repos/myorg/linux-builder/actions/runs/{id}/logs"),
        "check_suite_url": format!("https://api.github.com/repos/myorg/linux-builder/check-suites/{id}"),
        "artifacts_url": format!("https://api.github.com/repos/myorg/linux-builder/actions/runs/{id}/artifacts"),
        "cancel_url": format!("https://api.github.com/repos/myorg/linux-builder/actions/runs/{id}/cancel"),
        "rerun_url": format!("https://api.github.com/repos/myorg/linux-builder/actions/runs/{id}/rerun"),
        "workflow_url": "https://api.github.com/repos/myorg/linux-builder/actions/workflows/7",
        "head_commit": {
            "id": head_sha,
            "tree_id": head_sha,
            "message": "test commit",
            "timestamp": created_at.to_rfc3339(),
            "author": {"name": "ci"},
            "committer": {"name": "ci"},
        },
        "repository": {
            "id": 1,
            "name": "linux-builder",
            "url": "https://api.github.com/repos/myorg/linux-builder",
        },
    })
}

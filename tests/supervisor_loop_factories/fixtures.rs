// Shared scripted executors, daemon fixture, and config-TOML
// constants for the supervisor end-to-end submodules. Every sibling
// imports `super::fixtures::*` so the per-theme test files stay
// focused on the lifecycle assertion under test rather than the
// boilerplate setup.

#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use gcit::flow::dispatcher::{DispatchExecutor, ExecuteOutcome};
use gcit::flow::poll::{PollCycleError, PollExecutor, PollParams};
use gcit::git::PollOutcome;
use gcit::github::correlator::CorrelationOutcome;
use gcit::github::dispatcher::{DispatchOutcome, DispatchParams};
use gcit::github::monitor::empty_run_summary;

use super::common;

/// Pre-baked `PollExecutor` that yields one `Refreshed` outcome per
/// poll cycle and then returns `Cancelled` so the loop exits cleanly
/// when its outer cancel token fires.
///
/// `invocations` is shared via `Arc<AtomicUsize>` across every
/// poll-task spawn so the test can assert on the cumulative
/// across-flow count without per-flow plumbing.
pub struct ScriptedPollExecutor {
    invocations: Arc<AtomicUsize>,
    sha: gix_hash::ObjectId,
}

impl ScriptedPollExecutor {
    pub fn new(invocations: Arc<AtomicUsize>, sha_byte: u8) -> Self {
        Self {
            invocations,
            sha: sha_filled(sha_byte),
        }
    }
}

impl PollExecutor for ScriptedPollExecutor {
    fn strategy_label(&self) -> &'static str {
        "scripted"
    }

    async fn poll_cycle(
        &self,
        _params: &PollParams,
        _cancel: &CancellationToken,
    ) -> Result<PollOutcome, PollCycleError> {
        let n = self.invocations.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            // First cycle: report a fresh SHA to drive the diff path.
            Ok(PollOutcome::Refreshed { sha: self.sha })
        } else {
            // Subsequent cycles: cancel cleanly so the outer loop
            // exits and tokio drops the task.
            Err(PollCycleError::Cancelled)
        }
    }
}

/// Pre-baked `DispatchExecutor` — each `execute` call records that the
/// dispatcher loop reached the (dispatch + correlate) stage. Returns
/// `Success` so the post-execute lifecycle (RunStarted emission,
/// run_start fan-out, monitor spawn) runs end-to-end.
pub struct ScriptedDispatchExecutor {
    invocations: Arc<AtomicUsize>,
}

impl ScriptedDispatchExecutor {
    pub fn new(invocations: Arc<AtomicUsize>) -> Self {
        Self { invocations }
    }
}

impl DispatchExecutor for ScriptedDispatchExecutor {
    async fn execute(
        &self,
        _dispatch_params: DispatchParams,
        _branch: String,
        _head_sha: String,
        _cancel: CancellationToken,
    ) -> ExecuteOutcome {
        let n = self.invocations.fetch_add(1, Ordering::SeqCst);
        let run_id = 1000 + n as u64;
        ExecuteOutcome::Success {
            dispatch: DispatchOutcome {
                gcit_run_id: Uuid::new_v4(),
                dispatched_at: Utc::now(),
                repo: "owner/repo-x".to_string(),
                workflow: "ci.yml".to_string(),
                ref_name: "refs/heads/main".to_string(),
            },
            correlation: CorrelationOutcome {
                run_id,
                summary: empty_run_summary(run_id),
            },
        }
    }
}

pub fn sha_filled(byte: u8) -> gix_hash::ObjectId {
    let hex = format!("{byte:02x}").repeat(20);
    gix_hash::ObjectId::from_hex(hex.as_bytes()).expect("valid hex SHA")
}

pub fn write_config(dir: &std::path::Path, body: &str) -> PathBuf {
    let path = dir.join("gcit.toml");
    std::fs::write(&path, body).expect("write tempdir config");
    path
}

/// Bundle of tempdirs + env-var setup the multi-test fixture shares.
/// Holding the `TempDir`s on the returned struct keeps them alive for
/// the duration of the test (Drop on each TempDir runs `remove_dir_all`).
pub struct DaemonFixture {
    pub state_dir: tempfile::TempDir,
    pub runtime_dir: tempfile::TempDir,
    pub config_dir: tempfile::TempDir,
    pub creds_dir: tempfile::TempDir,
}

/// Set up the per-test process state: tempdirs, dummy credential file
/// at chmod 0o600, env vars STATE_DIRECTORY / RUNTIME_DIRECTORY /
/// CREDENTIALS_DIRECTORY, and rustls' CryptoProvider.
///
/// SAFETY of env mutation: the `unsafe` env writes are gated by
/// `#[serial_test::serial]` on every consumer test, so no concurrent
/// thread mutates env. The daemon reads each var ONCE at boot (before
/// any tokio spawn touches it).
pub fn setup_daemon_fixture() -> DaemonFixture {
    let state_dir = tempfile::tempdir().expect("state tempdir");
    let runtime_dir = tempfile::tempdir().expect("runtime tempdir");
    let config_dir = tempfile::tempdir().expect("config tempdir");
    let creds_dir = tempfile::tempdir().expect("creds tempdir");

    let pat_path = creds_dir.path().join("github_pat");
    std::fs::write(&pat_path, "github_pat_dummy_for_test").expect("write credential file");
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&pat_path, std::fs::Permissions::from_mode(0o600))
            .expect("chmod 0o600 on credential file");
    }

    common::ensure_crypto_provider();

    unsafe {
        std::env::set_var("STATE_DIRECTORY", state_dir.path());
        std::env::set_var("RUNTIME_DIRECTORY", runtime_dir.path());
        std::env::set_var("CREDENTIALS_DIRECTORY", creds_dir.path());
        std::env::remove_var("NOTIFY_SOCKET");
    }

    DaemonFixture {
        state_dir,
        runtime_dir,
        config_dir,
        creds_dir,
    }
}

/// Drop the env vars `setup_daemon_fixture` set so a panic in a
/// later assertion does not leak state into the next test.
pub fn teardown_daemon_env() {
    unsafe {
        std::env::remove_var("STATE_DIRECTORY");
        std::env::remove_var("RUNTIME_DIRECTORY");
        std::env::remove_var("CREDENTIALS_DIRECTORY");
    }
}

/// Wait for the control socket to appear on disk. The supervisor binds
/// it synchronously inside `bind_control_listener` before entering the
/// select! loop, so socket existence is the cleanest "daemon is
/// listening" signal.
///
/// Uses `tokio::time::sleep` for the polling delay even though the
/// test runs under `start_paused = true` — under paused time + the
/// current_thread runtime, parking the test task on a short sleep is
/// what triggers the runtime's auto-advance to the next earliest
/// deadline among all parked tasks, letting the daemon task continue
/// past its own `await` points.
///
/// Iteration cap (1000 polls × 10ms virtual = ~10s budget under
/// paused time) bounds the wait — a daemon that hangs in boot
/// surfaces as a failed assertion, not an infinite loop.
pub async fn wait_for_control_socket(socket_path: &std::path::Path) {
    use std::os::unix::fs::FileTypeExt;
    for _ in 0..1000 {
        if let Ok(meta) = std::fs::metadata(socket_path) {
            if meta.file_type().is_socket() {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "control socket {} did not appear within ~10s of daemon spawn",
        socket_path.display()
    );
}

/// Send a control-channel `Status` request and return the per-flow
/// JSON object. Drives the live control listener bound by the daemon
/// — proves the panic-respawn / reload paths are observable to the
/// same wire-protocol surface operators see via `gcit status`.
/// Bundled output of `build_test_factories`: the PollTaskFactory +
/// DispatchTaskFactory the supervisor harness needs to drive
/// `run_with_factories`, plus the three Arc<AtomicUsize> counters
/// each underlying executor increments. Tests can ignore any
/// counter they don't assert on (e.g. boot tests usually only
/// check `factory_calls`).
pub struct TestFactories {
    pub poll_factory: gcit::flow::supervisor::PollTaskFactory,
    pub dispatch_factory: gcit::flow::supervisor::DispatchTaskFactory,
    pub factory_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub poll_cycle_invocations: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub dispatch_invocations: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

/// Construct the standard ScriptedPollExecutor + ScriptedDispatchExecutor
/// pair wired through PollTaskFactory + DispatchTaskFactory closures
/// that increment a shared counter on every spawn. `sha_byte` seeds
/// the poll executor's `Refreshed { sha }` so distinct tests pin
/// distinct hex values when asserting state.json contents.
///
/// Duplicates eliminated: 6 sites (boot.rs, reload.rs ×3, control.rs,
/// the SIGINT test) previously inlined ~50 lines of identical
/// boilerplate per test.
pub fn build_test_factories(sha_byte: u8) -> TestFactories {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use gcit::flow::dispatcher::run_with_executor as dispatcher_run_with_executor;
    use gcit::flow::poll::run_with_executor as poll_run_with_executor;
    use gcit::flow::supervisor::{DispatchTaskFactory, PollTaskFactory};

    let factory_calls = Arc::new(AtomicUsize::new(0));
    let poll_cycle_invocations = Arc::new(AtomicUsize::new(0));
    let dispatch_invocations = Arc::new(AtomicUsize::new(0));

    let poll_factory: PollTaskFactory = {
        let factory_calls = Arc::clone(&factory_calls);
        let poll_cycle_invocations = Arc::clone(&poll_cycle_invocations);
        Arc::new(
            move |params, last_sha, last_dispatched_at, state_tx, trigger_tx, cancel| {
                factory_calls.fetch_add(1, Ordering::SeqCst);
                let executor =
                    ScriptedPollExecutor::new(Arc::clone(&poll_cycle_invocations), sha_byte);
                let fut: Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
                    Box::pin(poll_run_with_executor(
                        params,
                        executor,
                        last_sha,
                        last_dispatched_at,
                        state_tx,
                        trigger_tx,
                        cancel,
                    ));
                fut
            },
        )
    };
    let dispatch_factory: DispatchTaskFactory = {
        let invocations = Arc::clone(&dispatch_invocations);
        Arc::new(move |params, trigger_rx, state_tx, last_errors, cancel| {
            let executor = ScriptedDispatchExecutor::new(Arc::clone(&invocations));
            let fut: Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
                Box::pin(dispatcher_run_with_executor(
                    params,
                    executor,
                    trigger_rx,
                    state_tx,
                    last_errors,
                    cancel,
                ));
            fut
        })
    };

    TestFactories {
        poll_factory,
        dispatch_factory,
        factory_calls,
        poll_cycle_invocations,
        dispatch_invocations,
    }
}

pub async fn fetch_status(socket_path: &std::path::Path, flow: Option<&str>) -> serde_json::Value {
    let mut client = gcit::control::Client::connect(socket_path)
        .await
        .expect("connect to control socket");
    let req = gcit::control::Request::Status {
        id: Uuid::new_v4(),
        flow: flow.map(|s| s.to_string()),
    };
    let resp = client.send(req).await.expect("status reply");
    match resp {
        gcit::control::Response::Ok { data, .. } => data,
        gcit::control::Response::Error { message, .. } => {
            panic!("status returned Error: {message}")
        }
    }
}

pub const TWO_FLOW_CONFIG: &str = r#"
[poll]
source_interval = "15s"
job_interval = "15s"
jitter = 0.0

[http]
request_timeout = "1s"

[[flow]]
name = "flow-a"

[flow.source]
url = "https://git.example.com/a/r.git"
ref = "refs/heads/main"

[flow.action]
kind          = "github_workflow_dispatch"
repo          = "owner/repo-a"
workflow      = "ci.yml"
ref           = "refs/heads/main"
credential_id = "github_pat"

[[flow.destination]]
kind          = "local_mail"
user          = "ci"
fire_on       = ["run_complete"]

[[flow]]
name = "flow-b"

[flow.source]
url = "https://git.example.com/b/r.git"
ref = "refs/heads/main"

[flow.action]
kind          = "github_workflow_dispatch"
repo          = "owner/repo-b"
workflow      = "ci.yml"
ref           = "refs/heads/main"
credential_id = "github_pat"

[[flow.destination]]
kind          = "local_mail"
user          = "ci"
fire_on       = ["run_complete"]
"#;

pub const ONE_FLOW_CONFIG: &str = r#"
[poll]
source_interval = "15s"
job_interval = "15s"
jitter = 0.0

[http]
request_timeout = "1s"

[[flow]]
name = "flow-a"

[flow.source]
url = "https://git.example.com/a/r.git"
ref = "refs/heads/main"

[flow.action]
kind          = "github_workflow_dispatch"
repo          = "owner/repo-a"
workflow      = "ci.yml"
ref           = "refs/heads/main"
credential_id = "github_pat"

[[flow.destination]]
kind          = "local_mail"
user          = "ci"
fire_on       = ["run_complete"]
"#;

pub const ONE_FLOW_CONFIG_URL_CHANGED: &str = r#"
[poll]
source_interval = "15s"
job_interval = "15s"
jitter = 0.0

[http]
request_timeout = "1s"

[[flow]]
name = "flow-a"

[flow.source]
url = "https://git.example.com/a-NEW/r.git"
ref = "refs/heads/main"

[flow.action]
kind          = "github_workflow_dispatch"
repo          = "owner/repo-a"
workflow      = "ci.yml"
ref           = "refs/heads/main"
credential_id = "github_pat"

[[flow.destination]]
kind          = "local_mail"
user          = "ci"
fire_on       = ["run_complete"]
"#;

pub const TWO_FLOW_CONFIG_FOR_RELOAD: &str = r#"
[poll]
source_interval = "15s"
job_interval = "15s"
jitter = 0.0

[http]
request_timeout = "1s"

[[flow]]
name = "flow-a"

[flow.source]
url = "https://git.example.com/a/r.git"
ref = "refs/heads/main"

[flow.action]
kind          = "github_workflow_dispatch"
repo          = "owner/repo-a"
workflow      = "ci.yml"
ref           = "refs/heads/main"
credential_id = "github_pat"

[[flow.destination]]
kind          = "local_mail"
user          = "ci"
fire_on       = ["run_complete"]

[[flow]]
name = "flow-b"

[flow.source]
url = "https://git.example.com/b/r.git"
ref = "refs/heads/main"

[flow.action]
kind          = "github_workflow_dispatch"
repo          = "owner/repo-b"
workflow      = "ci.yml"
ref           = "refs/heads/main"
credential_id = "github_pat"

[[flow.destination]]
kind          = "local_mail"
user          = "ci"
fire_on       = ["run_complete"]
"#;

pub const ONE_FLOW_CONFIG_REF_CHANGED: &str = r#"
[poll]
source_interval = "15s"
job_interval = "15s"
jitter = 0.0

[http]
request_timeout = "1s"

[[flow]]
name = "flow-a"

[flow.source]
url = "https://git.example.com/a/r.git"
ref = "refs/heads/develop"

[flow.action]
kind          = "github_workflow_dispatch"
repo          = "owner/repo-a"
workflow      = "ci.yml"
ref           = "refs/heads/main"
credential_id = "github_pat"

[[flow.destination]]
kind          = "local_mail"
user          = "ci"
fire_on       = ["run_complete"]
"#;

pub const TWO_FLOWS_DUPLICATE_NAME_CONFIG: &str = r#"
[poll]
source_interval = "15s"
job_interval = "15s"
jitter = 0.0

[http]
request_timeout = "1s"

[[flow]]
name = "flow-a"

[flow.source]
url = "https://git.example.com/a/r.git"
ref = "refs/heads/main"

[flow.action]
kind          = "github_workflow_dispatch"
repo          = "owner/repo-a"
workflow      = "ci.yml"
ref           = "refs/heads/main"
credential_id = "github_pat"

[[flow]]
name = "flow-a"

[flow.source]
url = "https://git.example.com/b/r.git"
ref = "refs/heads/main"

[flow.action]
kind          = "github_workflow_dispatch"
repo          = "owner/repo-b"
workflow      = "ci.yml"
ref           = "refs/heads/main"
credential_id = "github_pat"
"#;

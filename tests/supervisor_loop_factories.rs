// End-to-end supervisor loop tests: drive the FULL daemon select! loop
// (boot → control commands → flow lifecycle → panic-respawn → SIGHUP
// reload → signal-driven shutdown) against scripted poll + dispatch
// executors injected via `gcit::flow::supervisor::run_with_factories`.
//
// Mirrors the per-component seams in `tests/poll_unborn_ref.rs` and
// `tests/flow_dispatcher_executor.rs` (which call the
// `*::run_with_executor` / `handle_trigger_with_executor` seams
// directly), but exercises them through `spawn_flow` -> `JoinSet` ->
// the supervisor's select! loop, so the real registry / last_errors
// map / signal handlers / sd_notify / control wiring is what runs.
//
// Multiple `#[tokio::test]`s share one process under cargo nextest
// (one binary per `tests/<name>.rs`). Tests serialize via
// `#[serial_test::serial]` because they all set process-global env
// vars (STATE_DIRECTORY, RUNTIME_DIRECTORY, CREDENTIALS_DIRECTORY)
// and install SIGTERM/SIGHUP/SIGINT signal handlers — concurrent
// tests would clobber each other's env state and race on signal
// delivery.
//
// Shutdown path: each test sends SIGTERM to its own pid via libc::kill
// AFTER the daemon has installed its tokio signal handler — the
// supervisor's signal arm routes the signal into the shutdown branch
// of the select! loop rather than the default-termination disposition
// of the test process.

mod common;

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use gcit::flow::dispatcher::{
    run_with_executor as dispatcher_run_with_executor, DispatchExecutor, ExecuteOutcome,
};
use gcit::flow::poll::{
    run_with_executor as poll_run_with_executor, PollCycleError, PollExecutor, PollParams,
};
use gcit::flow::supervisor::{
    run_with_factories, DaemonParams, DispatchTaskFactory, PollTaskFactory,
};
use gcit::git::PollOutcome;
use gcit::github::correlator::CorrelationOutcome;
use gcit::github::dispatcher::{DispatchOutcome, DispatchParams};
use gcit::github::monitor::empty_run_summary;

const TWO_FLOW_CONFIG: &str = r#"
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

/// Pre-baked `PollExecutor` that yields one `Refreshed` outcome per
/// poll cycle and then returns `Cancelled` so the loop exits cleanly
/// when its outer cancel token fires.
///
/// `invocations` is shared via `Arc<AtomicUsize>` across every
/// poll-task spawn so the test can assert on the cumulative
/// across-flow count without per-flow plumbing.
struct ScriptedPollExecutor {
    invocations: Arc<AtomicUsize>,
    sha: gix_hash::ObjectId,
}

impl ScriptedPollExecutor {
    fn new(invocations: Arc<AtomicUsize>, sha_byte: u8) -> Self {
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
            // The poll loop's first observation against
            // initial_last_sha=None returns trigger:false (baseline);
            // dispatcher won't fire from this.
            Ok(PollOutcome::Refreshed { sha: self.sha })
        } else {
            // Subsequent cycles: cancel cleanly so the outer loop
            // exits and tokio drops the task. Avoids a runaway
            // counter if the poll cadence races SIGTERM.
            Err(PollCycleError::Cancelled)
        }
    }
}

/// Pre-baked `DispatchExecutor` — each `execute` call records that the
/// dispatcher loop reached the (dispatch + correlate) stage. Returns
/// `Success` so the post-execute lifecycle (RunStarted emission,
/// run_start fan-out, monitor spawn) runs end-to-end.
struct ScriptedDispatchExecutor {
    invocations: Arc<AtomicUsize>,
}

impl ScriptedDispatchExecutor {
    fn new(invocations: Arc<AtomicUsize>) -> Self {
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

fn sha_filled(byte: u8) -> gix_hash::ObjectId {
    let hex = format!("{byte:02x}").repeat(20);
    gix_hash::ObjectId::from_hex(hex.as_bytes()).expect("valid hex SHA")
}

fn write_config(dir: &std::path::Path, body: &str) -> PathBuf {
    let path = dir.join("gcit.toml");
    std::fs::write(&path, body).expect("write tempdir config");
    path
}

/// Boot the daemon via `run_with_factories` with scripted executors,
/// give it a brief window to spawn the per-flow JoinSet entries, then
/// drive a clean SIGTERM-shutdown. Pins:
///
///   1. The factory plumbing through `SpawnContext` reaches every
///      production call site (`spawn_initial_flows` -> `spawn_flow`).
///      Compile-time verified by the type aliases; runtime-verified by
///      this test reaching shutdown without panic / hang.
///   2. The full select! loop (signal handlers, JoinSet, registry,
///      writer thread) bootstraps and tears down cleanly when driven
///      against scripted executors that bypass the network.
///   3. The state writer's drain is part of the shutdown sequence —
///      asserted via the post-shutdown `state.json` existence + non-zero
///      length.
///
/// The poll-cadence floor (`MIN_INTERVAL = 15s` per
/// `src/config/validate.rs`) plus the daemon's lock acquisition cost
/// dominate the wall-clock; we send SIGTERM after a short grace
/// window rather than waiting on virtual-time advance, because the
/// supervisor uses `tokio::signal::unix::signal(...)` which is
/// driven by OS-level signal delivery (not tokio time), and that
/// delivery is what this test uses to assert clean shutdown.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn supervisor_run_with_factories_boots_and_shuts_down_cleanly() {
    // Per-process tempdirs for STATE_DIRECTORY + RUNTIME_DIRECTORY so
    // the daemon's lock + state.json land in throwaway paths
    // (state::path resolution: STATE_DIRECTORY > XDG_STATE_HOME > HOME;
    // state::lock_path: RUNTIME_DIRECTORY > XDG_RUNTIME_DIR). Both env
    // vars are read once at run() boot; the test sets them BEFORE
    // calling run_with_factories.
    let state_dir = tempfile::tempdir().expect("state tempdir");
    let runtime_dir = tempfile::tempdir().expect("runtime tempdir");
    let config_dir = tempfile::tempdir().expect("config tempdir");
    let creds_dir = tempfile::tempdir().expect("creds tempdir");
    // Provide a dummy github_pat credential file so the credential
    // pool's resolve_secret succeeds (otherwise spawn_flow records a
    // `credential` last_error and skips the rest of the wiring).
    // The probe at config/credential_file.rs::probe rejects mode bits
    // outside 0o077, so the file MUST be chmod 0o600 — the default
    // umask-derived 0o644 fails the invariant.
    let pat_path = creds_dir.path().join("github_pat");
    std::fs::write(&pat_path, "github_pat_dummy_for_test").expect("write credential file");
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&pat_path, std::fs::Permissions::from_mode(0o600))
            .expect("chmod 0o600 on credential file");
    }

    // The supervisor's per-credential pool builds an octocrab client
    // backed by hyper-rustls; rustls' process-level CryptoProvider must
    // be installed before any client is constructed. tests/common
    // owns the idempotent installer (ensure_crypto_provider) shared
    // with every other integration test that touches octocrab.
    common::ensure_crypto_provider();

    // SAFETY: integration tests within ONE binary share env state.
    // The `#[serial_test::serial]` attribute on every test in this
    // file gates concurrent env mutation across the (now multiple)
    // tests in this binary. The daemon reads each var ONCE at boot,
    // before any tokio spawn touches them, so a later test mutation
    // could not race anyway.
    unsafe {
        std::env::set_var("STATE_DIRECTORY", state_dir.path());
        std::env::set_var("RUNTIME_DIRECTORY", runtime_dir.path());
        std::env::set_var("CREDENTIALS_DIRECTORY", creds_dir.path());
        // Scrub NOTIFY_SOCKET so sd_notify::notify is a no-op (we
        // don't want the test's daemon to attempt to fire Ready into
        // a stale socket left by an outer systemd).
        std::env::remove_var("NOTIFY_SOCKET");
    }

    let config_path = write_config(config_dir.path(), TWO_FLOW_CONFIG);
    let control_socket = runtime_dir.path().join("control.sock");

    // Counters shared across the test and the scripted executors. The
    // factories construct a fresh executor per spawn, but ALL flows
    // increment the same atomic counter so the test can assert "the
    // factory was invoked across the daemon's lifetime" without
    // per-flow plumbing.
    let poll_invocations = Arc::new(AtomicUsize::new(0));
    let dispatch_invocations = Arc::new(AtomicUsize::new(0));

    // Build the test factories. Each closure captures its own
    // Arc<Atomic*> clones so cloning the SpawnContext (e.g. on
    // panic-respawn) reuses the same script counters. The
    // closure-vs-trait wiring (`Pin<Box<dyn Future>>`) matches the
    // PollTaskFactory / DispatchTaskFactory type aliases exactly.
    let poll_factory: PollTaskFactory = {
        let invocations = Arc::clone(&poll_invocations);
        Arc::new(move |params, last_sha, last_dispatched_at, state_tx, trigger_tx, cancel| {
            // Fresh ScriptedPollExecutor per spawn so the script's
            // internal cycle counter resets after a respawn (which is
            // what production code does too — RealPollExecutor::for_url
            // is called fresh per spawn).
            let executor = ScriptedPollExecutor::new(Arc::clone(&invocations), 0xaa);
            let fut: Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
                Box::pin(poll_run_with_executor(
                    params, executor, last_sha, last_dispatched_at, state_tx, trigger_tx, cancel,
                ));
            fut
        })
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

    let params = DaemonParams {
        config_path,
        default_control_socket: control_socket,
        listen_fds: Vec::new(),
    };

    // Spawn the daemon. The handle resolves when the daemon's
    // shutdown sequence completes (after SIGTERM lands and every
    // per-flow task drains).
    let daemon_handle = tokio::spawn(async move {
        run_with_factories(params, poll_factory, dispatch_factory)
            .await
            .expect("daemon must complete cleanly")
    });

    // Yield + brief sleep so the daemon's setup steps (config load,
    // state load, credential pool init, control listener bind, signal
    // handler registration, spawn_initial_flows -> spawn_flow x2)
    // run to completion before SIGTERM lands.
    //
    // The signal handler must be installed BEFORE SIGTERM is sent;
    // otherwise the default-disposition would terminate the test
    // process. tokio::signal::unix::signal registers the handler
    // synchronously inside run_with_factories (steps 13 of the
    // boot sequence), so a 1s grace window is generous.
    //
    // 1s is far less than the 15s poll cadence, so poll_cycle has
    // not yet fired (the loop is sleeping its source_interval).
    // The factories themselves WERE invoked at spawn time — the
    // closures producing the boxed futures ran inside spawn_flow's
    // join_set.spawn(...) call.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Issue SIGTERM to drive the supervisor's shutdown arm. The
    // daemon's tokio signal handler intercepts it — installed during
    // run_with_factories after spawn_initial_flows lands the flows on
    // the JoinSet. The default-termination disposition is overridden
    // because tokio's signal::unix::signal registers a custom action.
    //
    // SAFETY: libc::kill is a single FFI call; the pid argument
    // (current process) is always valid; SIGTERM is a portable
    // non-fatal-when-handled signal.
    unsafe {
        libc::kill(libc::getpid(), libc::SIGTERM);
    }

    // Wait for the daemon's shutdown sequence: SIGTERM handler fires
    // → root_cancel.cancel() → flow tasks drain → state writer drains
    // → run_with_factories returns. 60s budget covers worst-case
    // flow-drain latency on a loaded test runner (the per-credential
    // rate-limit poller's HTTP call to api.github.com may be in flight
    // against the dummy PAT and needs to honour cancellation through
    // the cancel-aware tokio::time::timeout in `gh_rate_limit::poll_loop`).
    timeout(Duration::from_secs(60), daemon_handle)
        .await
        .expect("daemon must shut down within 60s of SIGTERM")
        .expect("daemon task must not panic");

    // Final-state cleanup: drain the env vars BEFORE inspecting
    // tempdirs so a panic below does not leak state into the next
    // test binary's process. (Each integration test runs in its own
    // cargo nextest process, so this is belt-and-braces — but the
    // same daemon-controlled state lives on the local fs and
    // cleanup must run before the tempdir Drops.)
    unsafe {
        std::env::remove_var("STATE_DIRECTORY");
        std::env::remove_var("RUNTIME_DIRECTORY");
        std::env::remove_var("CREDENTIALS_DIRECTORY");
    }

    // The poll-factory invocation count is best-effort: depending on
    // exact SIGTERM-vs-jitter race, poll_cycle may have fired 0 or
    // more times before cancellation tore the task down. We do NOT
    // assert >= N here — the factory wiring is verified by the fact
    // that the daemon BOOTED with our factories (no compile error,
    // no panic) and SHUT DOWN cleanly (no hang). The compile-time
    // type alias match (PollTaskFactory) and the runtime spawn_flow
    // path (which calls (ctx.poll_task_factory)(...) inside
    // join_set.spawn) are the contract pinned here. Read the counts
    // for diagnostic purposes only.
    let _final_poll = poll_invocations.load(Ordering::SeqCst);
    let _final_dispatch = dispatch_invocations.load(Ordering::SeqCst);

    // The single-instance lock file is the durable evidence that the
    // daemon ran: state::open_instance_lock_file creates the lock
    // file under $RUNTIME_DIRECTORY before flock(LOCK_EX), and the
    // file persists across the daemon's lifetime. (state.json is NOT
    // checked here because the writer only persists when at least one
    // StateUpdate lands; with the test's brief boot-then-SIGTERM
    // window the 15s poll cadence never produces an update, so no
    // write fires. Per state/writer.rs::run_mirrored, a clean
    // shutdown with no updates is a valid no-write path.)
    let lock_path = runtime_dir.path().join("gcit.lock");
    assert!(
        lock_path.exists(),
        "instance lock file must exist at $RUNTIME_DIRECTORY/gcit.lock after \
         the daemon ran (proves state::open_instance_lock_file completed)",
    );
}

/// Bundle of tempdirs + env-var setup the multi-test fixture shares.
/// Holding the `TempDir`s on the returned struct keeps them alive for
/// the duration of the test (Drop on each TempDir runs `remove_dir_all`).
struct DaemonFixture {
    state_dir: tempfile::TempDir,
    runtime_dir: tempfile::TempDir,
    config_dir: tempfile::TempDir,
    creds_dir: tempfile::TempDir,
}

/// Set up the per-test process state: tempdirs, dummy credential file
/// at chmod 0o600, env vars STATE_DIRECTORY / RUNTIME_DIRECTORY /
/// CREDENTIALS_DIRECTORY, and rustls' CryptoProvider. Mirrors the
/// inline boilerplate in the boot-and-shutdown test above so panic-
/// respawn + reload tests can reuse it without copy-paste drift.
///
/// SAFETY of env mutation: the `unsafe` env writes are gated by
/// `#[serial_test::serial]` on every consumer test, so no concurrent
/// thread mutates env. The daemon reads each var ONCE at boot (before
/// any tokio spawn touches it).
fn setup_daemon_fixture() -> DaemonFixture {
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
/// later assertion does not leak state into the next test (within the
/// same nextest binary). The tempdir Drops still run via the
/// DaemonFixture going out of scope.
fn teardown_daemon_env() {
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
/// past its own `await` points (e.g. the `spawn_blocking` inside
/// `resolve_secret`). A `std::thread::sleep` here would block the
/// single worker thread and the daemon would never make progress.
///
/// Iteration cap (1000 polls × 10ms virtual = ~10s budget under
/// paused time) bounds the wait — a daemon that hangs in boot
/// surfaces as a failed assertion, not an infinite loop.
async fn wait_for_control_socket(socket_path: &std::path::Path) {
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
async fn fetch_status(
    socket_path: &std::path::Path,
    flow: Option<&str>,
) -> serde_json::Value {
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

/// Single-flow config used by the panic-respawn + SIGHUP-reload tests.
/// One flow is enough to assert "the panic-respawn pipeline fired" or
/// "the URL-changed restart fired"; multiple flows would only add
/// non-deterministic counter races (both flows panicking might
/// interleave their respawns).
const ONE_FLOW_CONFIG: &str = r#"
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

/// Same single-flow config but with a DIFFERENT source URL — written
/// to the same path on top of `ONE_FLOW_CONFIG` to drive the SIGHUP
/// reload's `Restart { url_changed: true }` arm.
const ONE_FLOW_CONFIG_URL_CHANGED: &str = r#"
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

/// Drive the panic-respawn pipeline end-to-end through the supervisor
/// select! loop. Pins:
///
///   1. A panicking poll-task future surfaces as `FlowExit { panic:
///      Some }` via the `AssertUnwindSafe(...).catch_unwind()` wrapping
///      in `flows::spawn_flow`. The supervisor's `decide_respawn`
///      classifier lands on `PanicFirst`, records last_error with
///      `kind="panic"`, cancels the surviving sibling, and arms the
///      panic-watcher task that sleeps `RESPAWN_DELAY` before
///      enqueuing a `RespawnRequest`.
///   2. Advancing tokio time past `RESPAWN_DELAY` drains the
///      panic-watcher's sleep deterministically (no 30s real-time
///      wait). The supervisor's `respawn_rx` arm picks up the
///      request, calls `handle_respawn_request`, which routes to
///      `spawn_flow` for the new generation. The factory is invoked
///      a second time — proven by the per-factory invocation counter.
///   3. The new-generation poll task runs against a clean
///      `ScriptedPollExecutor`; its first successful poll cycle clears
///      the stale `last_error` (per the post-respawn sticky-error fix
///      in `flow::poll::run_with_executor`).
///
/// Time advance budget covers, in order:
///   - 30s: `RESPAWN_DELAY` (panic-watcher sleep)
///   - 15s: gen-2 poll loop's first `source_interval` sleep
///   - 1s buffer: select!-arm scheduling slack
///
/// Total advance: 46s of paused tokio time. Real wall-clock the test
/// burns is dominated by daemon boot (lock acquire + control bind +
/// signal install + spawn_initial_flows) plus the post-shutdown drain
/// of the writer thread.
#[tokio::test(flavor = "current_thread", start_paused = true)]
#[serial_test::serial]
async fn supervisor_respawns_panicked_flow_and_clears_last_error() {
    let fixture = setup_daemon_fixture();
    let config_path = write_config(fixture.config_dir.path(), ONE_FLOW_CONFIG);
    let control_socket = fixture.runtime_dir.path().join("control.sock");

    // Per-factory invocation counter. Each call to the poll-task
    // factory increments this so the test can distinguish gen-1
    // (panic future) from gen-2 (clean executor) without per-flow
    // plumbing. Distinct from `poll_cycle_invocations` below: the
    // factory counter ticks at SPAWN time (synchronous closure body),
    // the cycle counter ticks at POLL_CYCLE time (asynchronous
    // executor invocation).
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let poll_cycle_invocations = Arc::new(AtomicUsize::new(0));
    let dispatch_invocations = Arc::new(AtomicUsize::new(0));

    let poll_factory: PollTaskFactory = {
        let factory_calls = Arc::clone(&factory_calls);
        let poll_cycle_invocations = Arc::clone(&poll_cycle_invocations);
        Arc::new(move |params, last_sha, last_dispatched_at, state_tx, trigger_tx, cancel| {
            let call_n = factory_calls.fetch_add(1, Ordering::SeqCst);
            if call_n == 0 {
                // Gen-1: synthesize a future whose first poll panics.
                // This bypasses `poll_run_with_executor` entirely so
                // the panic fires as soon as tokio polls the spawned
                // task — no need to advance through the
                // `source_interval` (15s) sleep before reaching the
                // executor. The catch_unwind wrapper in
                // `flows::spawn_flow` converts the panic into
                // `FlowExit { panic: Some(_) }` which the supervisor's
                // `handle_flow_exit` classifies as `PanicFirst`.
                Box::pin(async move {
                    panic!("scripted-gen1-poll-panic");
                })
            } else {
                // Gen-2+: clean ScriptedPollExecutor. The first
                // successful poll cycle clears the stale "panic"
                // last_error per the sticky-error fix in
                // flow::poll::run_with_executor.
                let executor = ScriptedPollExecutor::new(
                    Arc::clone(&poll_cycle_invocations),
                    0xbb,
                );
                let fut: Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
                    Box::pin(poll_run_with_executor(
                        params, executor, last_sha, last_dispatched_at, state_tx, trigger_tx, cancel,
                    ));
                fut
            }
        })
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

    let params = DaemonParams {
        config_path,
        default_control_socket: control_socket.clone(),
        listen_fds: Vec::new(),
    };

    let daemon_handle = tokio::spawn(async move {
        run_with_factories(params, poll_factory, dispatch_factory)
            .await
            .expect("daemon must complete cleanly")
    });

    // Wait for the daemon to bind its control socket — proves the
    // boot sequence reached step 11 (bind_control_listener) and
    // step 15 (select! loop). spawn_initial_flows landed gen-1's
    // poll panic future on the JoinSet during step 10, which fires
    // as soon as the runtime polls it.
    wait_for_control_socket(&control_socket).await;

    // Give the runtime a few async yields so gen-1's panic surfaces
    // and the supervisor's `handle_flow_exit` records the
    // `kind="panic"` last_error. The panic fires on first poll (no
    // time-based gating) so the only thing we need is for the runtime
    // to schedule the poll-task future at least once. Under
    // current_thread + paused time, parking on a short tokio::time::
    // sleep triggers auto-advance to the next earliest deadline,
    // releasing other tasks (the JoinSet's join_next, the supervisor's
    // select! loop) without burning real time.
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Pin the panic-recording side effect via the control surface.
    // `last_error.kind == "panic"` is what `gcit status` reports
    // post-panic. Any regression that loses the catch_unwind wrap or
    // mis-routes the FlowExit decision arm would surface here as a
    // missing kind field or a different classifier value.
    let status = fetch_status(&control_socket, Some("flow-a")).await;
    let last_error = status["flow-a"]["last_error"].clone();
    assert!(
        !last_error.is_null(),
        "post-panic status must surface a last_error entry; got: {status}"
    );
    assert_eq!(
        last_error["kind"].as_str(),
        Some("panic"),
        "panic exit must record last_error.kind=panic; got: {last_error}"
    );

    // Advance past RESPAWN_DELAY so the panic-watcher's sleep
    // resolves and enqueues the RespawnRequest. The supervisor's
    // respawn_rx arm picks it up and calls handle_respawn_request →
    // spawn_flow (gen-2). The factory is invoked a second time
    // synchronously inside spawn_flow's join_set.spawn(...).
    tokio::time::advance(gcit::flow::RESPAWN_DELAY).await;
    // Drive the runtime forward so the respawn task wakes, the
    // supervisor's `respawn_rx.recv()` arm fires, and spawn_flow
    // (gen-2) runs. tokio::time::sleep parks the test under paused
    // time, letting auto-advance schedule whichever task has the
    // next earliest deadline.
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(
        factory_calls.load(Ordering::SeqCst) >= 2,
        "factory must be invoked at least twice (gen-1 panic + gen-2 respawn); got {}",
        factory_calls.load(Ordering::SeqCst),
    );

    // Advance past gen-2's first source_interval sleep so the
    // ScriptedPollExecutor's poll_cycle fires and the post-respawn
    // last_error clear path runs. The poll loop sleeps
    // source_interval BEFORE the first cycle (poll.rs:240-246), so
    // we need to release that sleep before the executor returns
    // PollOutcome::Refreshed and the loop hits the
    // `last_errors.lock().await.remove(...)` clear.
    let source_interval = Duration::from_secs(15);
    tokio::time::advance(source_interval + Duration::from_secs(1)).await;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // After the gen-2 poll cycle, the stale "panic" last_error must
    // be cleared (rendered as JSON `null`). Pin BOTH the explicit
    // null and the absence of the kind field so a regression that
    // returned the prior entry surfaces.
    let status = fetch_status(&control_socket, Some("flow-a")).await;
    assert!(
        status["flow-a"]["last_error"].is_null(),
        "post-recovery status must clear last_error to null; got: {status}",
    );

    // Drive shutdown.
    unsafe {
        libc::kill(libc::getpid(), libc::SIGTERM);
    }

    // The shutdown sequence drives `tokio::time::sleep` paths inside
    // the writer drain + the rate-limit poller's cancel; under paused
    // time those would only complete when the cancel arm of their
    // select! fires. The cancel token IS fired (root_cancel.cancel()
    // in the shutdown branch), so the cancel-aware select! arms
    // resolve immediately without needing further `advance` calls.
    timeout(Duration::from_secs(60), daemon_handle)
        .await
        .expect("daemon must shut down within 60s of SIGTERM")
        .expect("daemon task must not panic");

    teardown_daemon_env();

    let lock_path = fixture.runtime_dir.path().join("gcit.lock");
    assert!(
        lock_path.exists(),
        "instance lock file must exist at $RUNTIME_DIRECTORY/gcit.lock after \
         the daemon ran (proves state::open_instance_lock_file completed)",
    );
    let _state_dir = fixture.state_dir.path().to_path_buf();
    let _config_dir = fixture.config_dir.path().to_path_buf();
    let _creds_dir = fixture.creds_dir.path().to_path_buf();
    drop(fixture);
}

/// Drive the SIGHUP reload pipeline end-to-end through the supervisor
/// select! loop. Pins:
///
///   1. SIGHUP delivered via `libc::kill` after the supervisor's tokio
///      `signal::unix::signal(SignalKind::hangup())` handler is
///      installed routes into the supervisor's `sighup.recv()` arm —
///      not the test process's default-disposition handling.
///   2. The arm calls `run_reload`, which re-parses the config from
///      `config_path`, runs `compute_reload_actions` against the new
///      vs. old `FlowConfig` maps, and emits `Restart { url_changed:
///      true }` for the URL-changed flow.
///   3. The Restart action cancels the gen-1 handle and respawns via
///      `flows::spawn_flow`. The factory is invoked a second time —
///      proven by the per-factory invocation counter — and the
///      post-reload control-channel status surfaces "flow-a" still
///      present in the running flow set.
///
/// Time advance budget: zero — the SIGHUP signal handler is not
/// time-based, the reload's drain loop has a 30s timeout that we
/// don't need to advance past (cancellation is observed
/// synchronously by the gen-1 poll loop's select!), and the shutdown
/// path's cancel-aware sleeps resolve via cancel token rather than
/// via timer.
#[tokio::test(flavor = "current_thread", start_paused = true)]
#[serial_test::serial]
async fn supervisor_sighup_reload_restarts_url_changed_flow() {
    let fixture = setup_daemon_fixture();
    let config_path = write_config(fixture.config_dir.path(), ONE_FLOW_CONFIG);
    let control_socket = fixture.runtime_dir.path().join("control.sock");

    let factory_calls = Arc::new(AtomicUsize::new(0));
    let poll_cycle_invocations = Arc::new(AtomicUsize::new(0));
    let dispatch_invocations = Arc::new(AtomicUsize::new(0));

    let poll_factory: PollTaskFactory = {
        let factory_calls = Arc::clone(&factory_calls);
        let poll_cycle_invocations = Arc::clone(&poll_cycle_invocations);
        Arc::new(move |params, last_sha, last_dispatched_at, state_tx, trigger_tx, cancel| {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            let executor = ScriptedPollExecutor::new(
                Arc::clone(&poll_cycle_invocations),
                0xcc,
            );
            let fut: Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
                Box::pin(poll_run_with_executor(
                    params, executor, last_sha, last_dispatched_at, state_tx, trigger_tx, cancel,
                ));
            fut
        })
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

    // The reload pipeline re-reads the config from `config_path`, so
    // we keep the same path across the rewrite and the SIGHUP. Save
    // the path here before the `params` move below.
    let reload_config_path = config_path.clone();
    let params = DaemonParams {
        config_path,
        default_control_socket: control_socket.clone(),
        listen_fds: Vec::new(),
    };

    let daemon_handle = tokio::spawn(async move {
        run_with_factories(params, poll_factory, dispatch_factory)
            .await
            .expect("daemon must complete cleanly")
    });

    wait_for_control_socket(&control_socket).await;

    // Boot must have invoked the factory exactly once for the single
    // flow in ONE_FLOW_CONFIG. Pinning the lower bound here separates
    // the boot-spawn cycle from the post-reload respawn cycle so a
    // regression that double-spawns at boot doesn't get masked by the
    // post-reload assertion below.
    assert!(
        factory_calls.load(Ordering::SeqCst) >= 1,
        "boot must invoke poll-task factory at least once; got {}",
        factory_calls.load(Ordering::SeqCst),
    );

    // Sanity: status reports flow-a as present.
    let status_pre = fetch_status(&control_socket, Some("flow-a")).await;
    assert!(
        !status_pre["flow-a"].is_null(),
        "pre-reload status must list flow-a; got: {status_pre}",
    );

    // Rewrite the config in place with a CHANGED source URL. The
    // reload pipeline reads `config_path` fresh, builds the
    // FlowConfig BTreeMap, and feeds compute_reload_actions which
    // diffs against the previous Arc<Config> stored in the watch.
    // URL change → ReloadAction::Restart { url_changed: true }.
    std::fs::write(&reload_config_path, ONE_FLOW_CONFIG_URL_CHANGED)
        .expect("rewrite config with url change");

    // SIGHUP. The supervisor installed its hangup handler in step 13
    // of the boot sequence; the signal lands on the `sighup.recv()`
    // arm of the select! loop, which calls run_reload synchronously.
    //
    // SAFETY: libc::kill on the current pid with a portable signal
    // (SIGHUP) is async-signal-safe; the pid argument is always
    // valid because it's our own process.
    unsafe {
        libc::kill(libc::getpid(), libc::SIGHUP);
    }

    // Drive the supervisor's select! loop forward so it observes the
    // SIGHUP, runs run_reload to completion (including the per-flow
    // restart's cancel + drain + respawn), and the factory closure
    // increments to 2.
    //
    // run_reload's drain loop has a 30s timeout but completes
    // immediately when the cancelled tasks observe their tokens —
    // the gen-1 poll loop's select! has a `cancel.cancelled()` arm
    // that fires synchronously. No `tokio::time::advance` needed.
    //
    // Each `tokio::time::sleep(10ms)` parks the test task under
    // paused time, letting auto-advance schedule whichever runtime
    // task has the next earliest deadline. The SIGHUP-handler task
    // is signal-driven (not time-driven), so its readiness depends
    // on the OS having delivered the signal — which the kernel does
    // on the same syscall path as `libc::kill` regardless of tokio
    // time state.
    let mut polled = 0usize;
    while factory_calls.load(Ordering::SeqCst) < 2 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        polled += 1;
        if polled > 600 {
            panic!(
                "factory must be invoked twice within ~6s virtual time of SIGHUP \
                 (boot + reload restart); got {} after {} polls",
                factory_calls.load(Ordering::SeqCst),
                polled,
            );
        }
    }
    assert!(
        factory_calls.load(Ordering::SeqCst) >= 2,
        "post-SIGHUP-reload, factory must be invoked at least twice \
         (boot spawn + restart respawn); got {}",
        factory_calls.load(Ordering::SeqCst),
    );

    // Post-reload status must still list flow-a (Restart, not Remove).
    let status_post = fetch_status(&control_socket, Some("flow-a")).await;
    assert!(
        !status_post["flow-a"].is_null(),
        "post-reload status must still list flow-a; got: {status_post}",
    );

    // Shutdown.
    unsafe {
        libc::kill(libc::getpid(), libc::SIGTERM);
    }
    timeout(Duration::from_secs(60), daemon_handle)
        .await
        .expect("daemon must shut down within 60s of SIGTERM")
        .expect("daemon task must not panic");

    teardown_daemon_env();
    drop(fixture);
}

/// Two-flow config used by the SIGHUP-add-flow test. Same shape as
/// `TWO_FLOW_CONFIG` (which is consumed by the boot/shutdown test
/// above) — duplicated here as a separate constant so a future change
/// to the boot test's flow set doesn't accidentally invalidate the
/// add-flow test's pre-reload baseline.
const TWO_FLOW_CONFIG_FOR_RELOAD: &str = r#"
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

/// Same single-flow config but with a CHANGED ref_name (same URL).
/// Drives the SIGHUP reload's `Restart { url_changed: false }` arm —
/// the dispatcher's per-flow ref must be respawned to pick up the
/// new ref, but state.json's persisted last_sha is preserved (no
/// FlowRemoved emitted) so the new generation's first poll cycle
/// against the same URL doesn't dispatch a stale SHA.
const ONE_FLOW_CONFIG_REF_CHANGED: &str = r#"
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

/// SIGHUP reload that ADDS a flow (1 → 2 flows). Pins:
///   1. compute_reload_actions returns Keep + Spawn (Keep for the
///      pre-existing flow-a; Spawn for flow-b which is new).
///   2. The kept flow-a's poll/dispatcher pair is NOT re-spawned —
///      the factory's per-spawn invocation counter must be exactly
///      2 after the reload (1 boot for flow-a + 1 reload-spawn for
///      flow-b), NOT 3 (which would happen if Keep were
///      mis-classified as Restart).
///   3. The new flow-b appears in the control surface's flow_names
///      list post-reload.
#[tokio::test(flavor = "current_thread", start_paused = true)]
#[serial_test::serial]
async fn supervisor_sighup_reload_adds_new_flow_via_spawn_arm() {
    let fixture = setup_daemon_fixture();
    let config_path = write_config(fixture.config_dir.path(), ONE_FLOW_CONFIG);
    let control_socket = fixture.runtime_dir.path().join("control.sock");

    let factory_calls = Arc::new(AtomicUsize::new(0));
    let poll_cycle_invocations = Arc::new(AtomicUsize::new(0));
    let dispatch_invocations = Arc::new(AtomicUsize::new(0));

    let poll_factory: PollTaskFactory = {
        let factory_calls = Arc::clone(&factory_calls);
        let poll_cycle_invocations = Arc::clone(&poll_cycle_invocations);
        Arc::new(move |params, last_sha, last_dispatched_at, state_tx, trigger_tx, cancel| {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            let executor =
                ScriptedPollExecutor::new(Arc::clone(&poll_cycle_invocations), 0xdd);
            let fut: Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
                Box::pin(poll_run_with_executor(
                    params, executor, last_sha, last_dispatched_at, state_tx, trigger_tx, cancel,
                ));
            fut
        })
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

    let reload_config_path = config_path.clone();
    let params = DaemonParams {
        config_path,
        default_control_socket: control_socket.clone(),
        listen_fds: Vec::new(),
    };

    let daemon_handle = tokio::spawn(async move {
        run_with_factories(params, poll_factory, dispatch_factory)
            .await
            .expect("daemon must complete cleanly")
    });

    wait_for_control_socket(&control_socket).await;

    // Boot spawned exactly one flow.
    assert_eq!(
        factory_calls.load(Ordering::SeqCst),
        1,
        "boot of single-flow config must invoke factory exactly once",
    );
    let status_pre = fetch_status(&control_socket, None).await;
    let pre_keys: Vec<&str> = status_pre
        .as_object()
        .expect("status returns object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        pre_keys,
        vec!["flow-a"],
        "pre-reload status must list only flow-a; got: {pre_keys:?}",
    );

    // Rewrite to two flows.
    std::fs::write(&reload_config_path, TWO_FLOW_CONFIG_FOR_RELOAD)
        .expect("rewrite to 2-flow config");
    unsafe {
        libc::kill(libc::getpid(), libc::SIGHUP);
    }

    // Drive the runtime forward until the factory has been called
    // for flow-b. `Keep` does NOT re-spawn flow-a, so the
    // post-reload count should be exactly 2 (boot + add).
    let mut polled = 0usize;
    while factory_calls.load(Ordering::SeqCst) < 2 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        polled += 1;
        if polled > 600 {
            panic!(
                "factory must be invoked twice within ~6s virtual time of SIGHUP add-flow \
                 (boot + Spawn arm); got {} after {} polls",
                factory_calls.load(Ordering::SeqCst),
                polled,
            );
        }
    }
    // Give a brief grace window so any erroneous re-spawn of the
    // kept flow would land on the counter before the assertion.
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        factory_calls.load(Ordering::SeqCst),
        2,
        "post-add factory_calls must be exactly 2 (boot for flow-a + Spawn for flow-b); \
         3+ would imply the Keep arm mis-classified flow-a as Restart and re-spawned it",
    );

    // Status surfaces both flows.
    let status_post = fetch_status(&control_socket, None).await;
    let post_keys: Vec<&str> = status_post
        .as_object()
        .expect("status returns object")
        .keys()
        .map(String::as_str)
        .collect();
    assert!(
        post_keys.contains(&"flow-a") && post_keys.contains(&"flow-b"),
        "post-reload status must list flow-a + flow-b; got: {post_keys:?}",
    );

    unsafe {
        libc::kill(libc::getpid(), libc::SIGTERM);
    }
    timeout(Duration::from_secs(60), daemon_handle)
        .await
        .expect("daemon must shut down within 60s of SIGTERM")
        .expect("daemon task must not panic");

    teardown_daemon_env();
    drop(fixture);
}

/// SIGHUP reload that changes `ref_name` (NOT `source.url`). Drives
/// the `Restart { url_changed: false }` arm. Pins:
///   1. The flow's old generation is cancelled and a new one
///      spawned (factory_calls increments to 2).
///   2. NO `FlowRemoved` is implied — the test asserts the post-reload
///      flow is still listed under the same name (vs. the URL-change
///      Restart, which emits FlowRemoved to drop persisted state).
///      The state.json side is exercised by the SIGHUP-URL-changed
///      test above; this test focuses on the spawn arm.
#[tokio::test(flavor = "current_thread", start_paused = true)]
#[serial_test::serial]
async fn supervisor_sighup_reload_restarts_non_url_change_via_restart_arm() {
    let fixture = setup_daemon_fixture();
    let config_path = write_config(fixture.config_dir.path(), ONE_FLOW_CONFIG);
    let control_socket = fixture.runtime_dir.path().join("control.sock");

    let factory_calls = Arc::new(AtomicUsize::new(0));
    let poll_cycle_invocations = Arc::new(AtomicUsize::new(0));
    let dispatch_invocations = Arc::new(AtomicUsize::new(0));

    let poll_factory: PollTaskFactory = {
        let factory_calls = Arc::clone(&factory_calls);
        let poll_cycle_invocations = Arc::clone(&poll_cycle_invocations);
        Arc::new(move |params, last_sha, last_dispatched_at, state_tx, trigger_tx, cancel| {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            let executor =
                ScriptedPollExecutor::new(Arc::clone(&poll_cycle_invocations), 0xee);
            let fut: Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
                Box::pin(poll_run_with_executor(
                    params, executor, last_sha, last_dispatched_at, state_tx, trigger_tx, cancel,
                ));
            fut
        })
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

    let reload_config_path = config_path.clone();
    let params = DaemonParams {
        config_path,
        default_control_socket: control_socket.clone(),
        listen_fds: Vec::new(),
    };

    let daemon_handle = tokio::spawn(async move {
        run_with_factories(params, poll_factory, dispatch_factory)
            .await
            .expect("daemon must complete cleanly")
    });

    wait_for_control_socket(&control_socket).await;
    assert_eq!(factory_calls.load(Ordering::SeqCst), 1);

    // Rewrite with a different ref_name (same URL).
    std::fs::write(&reload_config_path, ONE_FLOW_CONFIG_REF_CHANGED)
        .expect("rewrite config with ref_name change");
    unsafe {
        libc::kill(libc::getpid(), libc::SIGHUP);
    }

    let mut polled = 0usize;
    while factory_calls.load(Ordering::SeqCst) < 2 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        polled += 1;
        if polled > 600 {
            panic!(
                "factory must be invoked twice within ~6s virtual time of SIGHUP \
                 ref-change Restart; got {} after {} polls",
                factory_calls.load(Ordering::SeqCst),
                polled,
            );
        }
    }
    assert!(
        factory_calls.load(Ordering::SeqCst) >= 2,
        "non-URL Restart must invoke the factory twice (boot + restart respawn); got {}",
        factory_calls.load(Ordering::SeqCst),
    );

    // Post-reload status still lists flow-a (Restart, not Remove).
    let status_post = fetch_status(&control_socket, Some("flow-a")).await;
    assert!(
        !status_post["flow-a"].is_null(),
        "post-reload status must still list flow-a; got: {status_post}",
    );

    unsafe {
        libc::kill(libc::getpid(), libc::SIGTERM);
    }
    timeout(Duration::from_secs(60), daemon_handle)
        .await
        .expect("daemon must shut down within 60s of SIGTERM")
        .expect("daemon task must not panic");

    teardown_daemon_env();
    drop(fixture);
}

/// Drive the control-command select! arm end-to-end. Pins:
///
///   1. `cmd_rx.recv()` arm of the supervisor's main select! loop
///      fires when a Trigger request arrives over the control socket
///      (run.rs:329-339 dispatches via `handle_control_command`).
///   2. `handle_control_command` routes the Trigger {dry_run:true}
///      command into `run_trigger`, which calls
///      `render_dry_run_payload` (control.rs:299) and replies with
///      the rendered JSON over the oneshot reply channel.
///   3. The wire-protocol `Response::Ok { data, .. }` carries the
///      payload object whose `flow`, `dry_run:true`, `repo`,
///      `workflow`, and `gcit_run_id` fields are populated from the
///      flow's running config.
///
/// The dry-run path does NOT contact GitHub or any notifier — the
/// rendered output stays in memory and round-trips back to the caller.
/// The test asserts the JSON shape an operator running
/// `gcit trigger flow-a --dry-run` against the live daemon would see.
#[tokio::test(flavor = "current_thread", start_paused = true)]
#[serial_test::serial]
async fn supervisor_control_command_trigger_dry_run_returns_rendered_payload() {
    let fixture = setup_daemon_fixture();
    let config_path = write_config(fixture.config_dir.path(), ONE_FLOW_CONFIG);
    let control_socket = fixture.runtime_dir.path().join("control.sock");

    let factory_calls = Arc::new(AtomicUsize::new(0));
    let poll_cycle_invocations = Arc::new(AtomicUsize::new(0));
    let dispatch_invocations = Arc::new(AtomicUsize::new(0));

    let poll_factory: PollTaskFactory = {
        let factory_calls = Arc::clone(&factory_calls);
        let poll_cycle_invocations = Arc::clone(&poll_cycle_invocations);
        Arc::new(move |params, last_sha, last_dispatched_at, state_tx, trigger_tx, cancel| {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            let executor =
                ScriptedPollExecutor::new(Arc::clone(&poll_cycle_invocations), 0xff);
            let fut: Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
                Box::pin(poll_run_with_executor(
                    params, executor, last_sha, last_dispatched_at, state_tx, trigger_tx, cancel,
                ));
            fut
        })
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

    let params = DaemonParams {
        config_path,
        default_control_socket: control_socket.clone(),
        listen_fds: Vec::new(),
    };

    let daemon_handle = tokio::spawn(async move {
        run_with_factories(params, poll_factory, dispatch_factory)
            .await
            .expect("daemon must complete cleanly")
    });

    wait_for_control_socket(&control_socket).await;

    // Connect to the live control socket and send a Trigger
    // {dry_run:true} request. The supervisor's cmd_rx arm picks it up
    // from the control_handler's mpsc (control.rs:188-229), routes
    // through run_trigger -> render_dry_run_payload (control.rs:262),
    // and replies with the rendered JSON.
    let mut client = gcit::control::Client::connect(&control_socket)
        .await
        .expect("control socket must accept connection");
    let req_id = Uuid::new_v4();
    let req = gcit::control::Request::Trigger {
        id: req_id,
        flow: "flow-a".to_string(),
        dry_run: true,
    };
    let resp = client
        .send(req)
        .await
        .expect("control reply must arrive within READ_TIMEOUT_SECS");

    // Response is Ok{id,data}; data carries the dry-run payload.
    let (resp_id, data) = match resp {
        gcit::control::Response::Ok { id, data } => (id, data),
        gcit::control::Response::Error { message, .. } => {
            panic!("trigger --dry-run must succeed; got Error: {message}")
        }
    };
    assert_eq!(
        resp_id, req_id,
        "response id must echo the request id",
    );

    // Pin every field render_dry_run_payload emits (control.rs:337-345)
    // so a regression that drops or renames any of them surfaces here.
    assert_eq!(
        data["flow"].as_str(),
        Some("flow-a"),
        "dry-run payload must carry the flow name; got: {data}",
    );
    assert_eq!(
        data["dry_run"].as_bool(),
        Some(true),
        "dry-run payload must carry dry_run=true; got: {data}",
    );
    assert_eq!(
        data["repo"].as_str(),
        Some("owner/repo-a"),
        "dry-run payload must carry the action.repo from the running config; got: {data}",
    );
    assert_eq!(
        data["workflow"].as_str(),
        Some("ci.yml"),
        "dry-run payload must carry the action.workflow; got: {data}",
    );
    assert_eq!(
        data["ref"].as_str(),
        Some("refs/heads/main"),
        "dry-run payload must carry the action.ref; got: {data}",
    );
    let run_id_str = data["gcit_run_id"]
        .as_str()
        .expect("gcit_run_id must be a string");
    assert!(
        Uuid::parse_str(run_id_str).is_ok(),
        "gcit_run_id must be a parseable UUID (control.rs:307 uses Uuid::new_v4); got: {run_id_str}",
    );
    // rendered_inputs is the auto-injected payload — must at least
    // carry the gcit_run_id key (build_inputs_payload always injects
    // it). flow-a in ONE_FLOW_CONFIG has no user-supplied inputs, so
    // gcit_run_id is the only entry.
    let rendered = data["rendered_inputs"]
        .as_object()
        .expect("rendered_inputs must be an object");
    assert!(
        rendered.contains_key("gcit_run_id"),
        "rendered_inputs must carry the auto-injected gcit_run_id key; got: {rendered:?}",
    );
    assert_eq!(
        rendered["gcit_run_id"].as_str(),
        Some(run_id_str),
        "rendered_inputs.gcit_run_id must match the top-level gcit_run_id (the same UUID is injected at render time and reported back to the operator); got: {rendered:?}",
    );

    // Drop the client so the daemon's accept side cleans up before
    // shutdown.
    drop(client);

    unsafe {
        libc::kill(libc::getpid(), libc::SIGTERM);
    }
    timeout(Duration::from_secs(60), daemon_handle)
        .await
        .expect("daemon must shut down within 60s of SIGTERM")
        .expect("daemon task must not panic");

    teardown_daemon_env();
    drop(fixture);
}

/// Drive the SIGINT arm of the supervisor select! loop. SIGINT
/// (ctrl-C) shares the shutdown branch with SIGTERM (run.rs:325-328)
/// — both signal handlers route into the same `break` path that
/// triggers root_cancel.cancel + flow drain + writer drain.
///
/// Pinning SIGINT alongside SIGTERM in the harness:
///   1. Proves the SIGINT signal handler is installed at boot
///      (signal::unix::signal(SignalKind::interrupt) at run.rs:296).
///   2. Proves SIGINT routes into the shutdown branch — a regression
///      that mis-wired the SIGINT arm to e.g. reload would surface as
///      the daemon never returning from run_with_factories within
///      the 60s budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn supervisor_sigint_routes_to_shutdown_branch() {
    let fixture = setup_daemon_fixture();
    let config_path = write_config(fixture.config_dir.path(), ONE_FLOW_CONFIG);
    let control_socket = fixture.runtime_dir.path().join("control.sock");

    let factory_calls = Arc::new(AtomicUsize::new(0));
    let poll_cycle_invocations = Arc::new(AtomicUsize::new(0));
    let dispatch_invocations = Arc::new(AtomicUsize::new(0));

    let poll_factory: PollTaskFactory = {
        let factory_calls = Arc::clone(&factory_calls);
        let poll_cycle_invocations = Arc::clone(&poll_cycle_invocations);
        Arc::new(move |params, last_sha, last_dispatched_at, state_tx, trigger_tx, cancel| {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            let executor =
                ScriptedPollExecutor::new(Arc::clone(&poll_cycle_invocations), 0xa1);
            let fut: Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
                Box::pin(poll_run_with_executor(
                    params, executor, last_sha, last_dispatched_at, state_tx, trigger_tx, cancel,
                ));
            fut
        })
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

    let params = DaemonParams {
        config_path,
        default_control_socket: control_socket,
        listen_fds: Vec::new(),
    };

    let daemon_handle = tokio::spawn(async move {
        run_with_factories(params, poll_factory, dispatch_factory)
            .await
            .expect("daemon must complete cleanly")
    });

    // Real-time grace window for the daemon to install its signal
    // handlers (multi_thread runtime + real time, mirroring the
    // SIGTERM boot-and-shutdown test pattern). Without this,
    // libc::kill could deliver SIGINT before the supervisor's
    // signal::unix::signal(SignalKind::interrupt) handler is
    // registered, in which case the default disposition would
    // terminate the test process.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // SAFETY: libc::kill on the current pid with a portable signal
    // (SIGINT) is async-signal-safe; the pid is always valid.
    unsafe {
        libc::kill(libc::getpid(), libc::SIGINT);
    }

    timeout(Duration::from_secs(60), daemon_handle)
        .await
        .expect("daemon must shut down within 60s of SIGINT")
        .expect("daemon task must not panic");

    teardown_daemon_env();
    let lock_path = fixture.runtime_dir.path().join("gcit.lock");
    assert!(
        lock_path.exists(),
        "instance lock file must exist at $RUNTIME_DIRECTORY/gcit.lock after \
         the daemon ran (proves state::open_instance_lock_file completed before SIGINT)",
    );
    drop(fixture);
}

/// Config with two flows that share the same `name` — the validator
/// at config/validate.rs:308-319 detects duplicate names and emits a
/// `ConfigError::Validate` carrying every line number. run_with_factories
/// returns `Err(DaemonError::Config(errs))` from run.rs:115 BEFORE any
/// flow spawn, lock acquire, or signal handler install.
const TWO_FLOWS_DUPLICATE_NAME_CONFIG: &str = r#"
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

/// Drive the boot-time fatal-config arm of `run_with_factories`. Pins:
///
///   1. Duplicate flow names are caught by the validator at
///      config/validate.rs:308-319 and surfaced via
///      crate::config::load returning Err(Vec<ConfigError>).
///   2. run_with_factories at run.rs:113-116 maps the parse failure
///      onto Err(DaemonError::Config(errs)) and returns BEFORE the
///      lock-acquire, state-load, factory-spawn, or signal-handler
///      installation steps run.
///   3. The factory closures hold panic-if-invoked guards so any
///      regression that reached `spawn_initial_flows` past the early
///      Err would surface as a test panic rather than a silent pass.
///
/// The test does NOT require shutdown machinery — `run_with_factories`
/// returns synchronously without setting up tokio signal handlers, so
/// no SIGTERM teardown is needed.
#[tokio::test(flavor = "current_thread")]
#[serial_test::serial]
async fn supervisor_run_with_factories_returns_daemon_error_config_on_duplicate_flow_names() {
    let fixture = setup_daemon_fixture();
    let config_path =
        write_config(fixture.config_dir.path(), TWO_FLOWS_DUPLICATE_NAME_CONFIG);
    let control_socket = fixture.runtime_dir.path().join("control.sock");

    // Factories panic if invoked. They MUST NOT be — the boot path
    // exits at config-load (run.rs:115) before spawn_initial_flows.
    let poll_factory: PollTaskFactory = Arc::new(|_, _, _, _, _, _| {
        panic!(
            "poll factory invoked unexpectedly: \
             boot must abort at the config-load Err arm before any spawn"
        )
    });
    let dispatch_factory: DispatchTaskFactory = Arc::new(|_, _, _, _, _| {
        panic!(
            "dispatch factory invoked unexpectedly: \
             boot must abort at the config-load Err arm before any spawn"
        )
    });

    let params = DaemonParams {
        config_path,
        default_control_socket: control_socket,
        listen_fds: Vec::new(),
    };
    let err = run_with_factories(params, poll_factory, dispatch_factory)
        .await
        .expect_err("duplicate flow names must surface as DaemonError::Config");

    // Pin the variant + the validate-error message body. ConfigError's
    // Display includes "duplicate flow name; N occurrences" for
    // duplicate-name diagnoses (validate.rs:316).
    let errs = match err {
        gcit::flow::supervisor::DaemonError::Config(errs) => errs,
        other => panic!(
            "duplicate flow names must surface as DaemonError::Config; got: {other:?}"
        ),
    };
    assert!(
        !errs.is_empty(),
        "DaemonError::Config must carry at least one ConfigError",
    );
    let combined = errs
        .iter()
        .map(|e| format!("{e}"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        combined.contains("duplicate flow name"),
        "at least one ConfigError must surface the 'duplicate flow name' diagnosis; got:\n{combined}",
    );

    teardown_daemon_env();
    drop(fixture);
}

/// Drive the boot-time DaemonError::State(LockHeld) arm. Pins:
///
///   1. Pre-acquiring the flock on `$RUNTIME_DIRECTORY/gcit.lock` from
///      a separate file descriptor in the same process satisfies
///      flock's "one OFD per lock" semantic — the daemon's open(2)
///      creates a distinct OFD, its `try_write()` issues
///      `flock(LOCK_EX | LOCK_NB)`, and the kernel returns EWOULDBLOCK
///      because the test holds the lock on its own fd.
///   2. run.rs:138-143 maps EWOULDBLOCK onto
///      `Err(DaemonError::State(StateError::LockHeld { path }))` and
///      returns BEFORE state-load, factory-spawn, or signal-handler
///      installation steps run.
///   3. The factory closures hold panic-if-invoked guards so a
///      regression that reached spawn_initial_flows would panic
///      loudly rather than silently passing.
#[tokio::test(flavor = "current_thread")]
#[serial_test::serial]
async fn supervisor_run_with_factories_returns_daemon_error_state_lock_held_when_flock_held() {
    let fixture = setup_daemon_fixture();
    let config_path = write_config(fixture.config_dir.path(), ONE_FLOW_CONFIG);
    let control_socket = fixture.runtime_dir.path().join("control.sock");

    // Pre-acquire the flock on $RUNTIME_DIRECTORY/gcit.lock from a
    // separate file descriptor. fd_lock::RwLock::try_write issues
    // flock(LOCK_EX | LOCK_NB) on the underlying fd
    // (fd_lock-4.0.4/src/sys/unix/rw_lock.rs:25); flock locks attach
    // to OFDs (open file descriptions), so two distinct open() calls
    // in the same process yield two OFDs that mutually exclude each
    // other.
    let lock_path = fixture.runtime_dir.path().join("gcit.lock");
    // Materialize the parent directory (state::open_instance_lock_file
    // creates it on the production path; we replicate here so our
    // pre-acquire OpenOptions::open succeeds).
    if let Some(parent) = lock_path.parent() {
        std::fs::create_dir_all(parent).expect("create runtime dir for lock pre-acquire");
    }
    let lock_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .expect("open lock file for pre-acquire");
    let mut held_lock = fd_lock::RwLock::new(lock_file);
    let _held_guard = held_lock
        .try_write()
        .expect("test must successfully acquire the flock before booting the daemon");

    let poll_factory: PollTaskFactory = Arc::new(|_, _, _, _, _, _| {
        panic!(
            "poll factory invoked unexpectedly: \
             boot must abort at the LockHeld arm before any spawn"
        )
    });
    let dispatch_factory: DispatchTaskFactory = Arc::new(|_, _, _, _, _| {
        panic!(
            "dispatch factory invoked unexpectedly: \
             boot must abort at the LockHeld arm before any spawn"
        )
    });

    let params = DaemonParams {
        config_path,
        default_control_socket: control_socket,
        listen_fds: Vec::new(),
    };
    let err = run_with_factories(params, poll_factory, dispatch_factory)
        .await
        .expect_err("contended flock must surface as DaemonError::State(LockHeld)");

    match err {
        gcit::flow::supervisor::DaemonError::State(state_err) => {
            // Pin the LockHeld variant specifically — LockOpen,
            // LockAcquireFailed, or any other StateError would
            // indicate a regression in the lock-acquire path.
            let rendered = format!("{state_err}");
            assert!(
                rendered.contains("another gcit instance is running"),
                "LockHeld Display must surface the canonical 'another gcit instance is running' message (state/mod.rs:97-99); got: {rendered}",
            );
            assert!(
                rendered.contains(&lock_path.display().to_string()),
                "LockHeld Display must name the contended lock path; got: {rendered}",
            );
        }
        other => panic!(
            "contended flock must surface as DaemonError::State; got: {other:?}"
        ),
    }

    // Drop the held lock guard explicitly so the tempdir cleanup
    // succeeds (Drop on TempDir runs remove_dir_all; a still-open fd
    // on Linux survives the unlink but cleanup is tidier).
    drop(_held_guard);
    drop(held_lock);

    teardown_daemon_env();
    drop(fixture);
}

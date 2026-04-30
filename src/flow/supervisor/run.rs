// Daemon run-loop entry point. `run` boots the per-credential pool,
// state writer + mirror, control listener, watchdog, signal handlers,
// then drives the main `select!` loop that fans out into the per-arm
// handlers in the sibling sub-modules (reload, control, respawn).
//
// Shutdown sequence:
//     Stopping -> root.cancel() -> await flows -> drop(state_tx)
//     -> writer.join().
//
// `DaemonError` covers every fatal-at-boot failure mode. The Display
// impl renders a single `Config` enum variant as a multi-line bulleted
// list of nested `ConfigError` Displays so the operator sees actionable
// text rather than a `{:?}` debug dump.

use std::collections::BTreeMap;
use std::os::unix::io::{FromRawFd, RawFd};
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};

use sd_notify::NotifyState;
use tokio::net::UnixListener;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::{mpsc, watch, Mutex, RwLock};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::ConfigError;
use crate::control;
use crate::mail;
use crate::state::{self, State, StateUpdate};

use super::control::{handle_control_command, ControlCommand, ControlHandler};
use super::credentials::CredentialPool;
use super::flows::{
    production_dispatch_task_factory, production_poll_task_factory, spawn_initial_flows,
    DispatchTaskFactory, PollTaskFactory, SpawnContext,
};
use super::reload::run_reload;
use super::respawn::{handle_flow_exit, handle_respawn_request};
use super::types::{FlowExit, FlowLastError, FlowRegistry};

/// Capacity of the per-daemon `StateUpdate` mpsc into the writer.
/// 256-slot mpsc buffer, batched in groups of 64 by the writer.
pub const STATE_QUEUE: usize = 256;

/// Capacity of the supervisor command mpsc that routes control-channel
/// requests (Reload, Trigger) into the supervisor's select! loop. The
/// control handler ships a command + reply channel through this mpsc
/// rather than running reload/trigger inline so the per-flow state
/// mutations stay on the supervisor task. A small buffer (8) is plenty
/// — control commands are rare.
const CONTROL_COMMAND_QUEUE: usize = 8;

/// Capacity of the respawn-request mpsc fed by panic-watcher tasks.
/// Each panicked flow produces ONE request after `RESPAWN_DELAY`; a
/// pathologically misbehaving deployment with many concurrent panics
/// would still be far below this cap.
const RESPAWN_QUEUE: usize = 32;

/// Top-level daemon-entry parameters. Built by `bin/gcit.rs::main`
/// and passed through to `run`.
pub struct DaemonParams {
    /// Absolute path to the config file. The supervisor re-reads this
    /// on SIGHUP and on Reload control messages.
    pub config_path: PathBuf,
    /// Default control-socket path used when no `control` listen-fd
    /// is supplied. Resolved by the binary entry from
    /// `--control-socket` / XDG_RUNTIME_DIR / /run/gcit.
    pub default_control_socket: PathBuf,
    /// Listen-fds inherited from systemd via the `LISTEN_FDS` /
    /// `LISTEN_FDNAMES` env var pair. Each tuple is `(fd, name)` —
    /// gcit looks for the `control` name to find the control socket.
    pub listen_fds: Vec<(RawFd, String)>,
}

/// Run the daemon to completion.
///
/// Returns when SIGTERM/SIGINT fires or any unrecoverable boot
/// failure surfaces. The caller (the binary entry) maps the result
/// onto an `ExitCode`.
pub async fn run(params: DaemonParams) -> Result<(), DaemonError> {
    run_with_factories(
        params,
        production_poll_task_factory(),
        production_dispatch_task_factory(),
    )
    .await
}

/// Daemon entry point with caller-supplied per-flow task factories —
/// the seam the supervisor end-to-end test harness uses to inject
/// scripted poll/dispatch executors at every spawn site (boot, reload,
/// panic-respawn). Production callers go through `run`, which builds
/// `production_{poll,dispatch}_task_factory()` and delegates here.
///
/// `#[doc(hidden)] pub` mirrors the test-seam pattern in
/// `flow::dispatcher::handle_trigger_with_executor`: callable from
/// integration tests, hidden from rustdoc.
#[doc(hidden)]
pub async fn run_with_factories(
    params: DaemonParams,
    poll_task_factory: PollTaskFactory,
    dispatch_task_factory: DispatchTaskFactory,
) -> Result<(), DaemonError> {
    let DaemonParams {
        config_path,
        default_control_socket,
        listen_fds,
    } = params;

    // 1. Load + validate the config. A failure here is fatal — the
    // operator must fix it before the daemon can start.
    let initial_config = match crate::config::load(&config_path) {
        Ok(c) => Arc::new(c),
        Err(errs) => return Err(DaemonError::Config(errs)),
    };
    info!(
        target: "gcit::supervisor",
        flows = initial_config.flow.len(),
        config = %config_path.display(),
        "config loaded",
    );

    // 2. Acquire the single-instance lock BEFORE loading state.
    // The exclusive flock on $RUNTIME_DIRECTORY/gcit.lock prevents
    // a second gcit instance from racing on state.json or
    // re-dispatching the same SHA. Held for the daemon's lifetime
    // — the guard is dropped automatically when `run()` returns.
    //
    // The lock acquire happens BEFORE state load so a second
    // instance fails fast with a clear error instead of partly
    // initialising and then colliding on state-writer mpsc /
    // state.json.
    let lock_path = state::lock_path().map_err(DaemonError::State)?;
    let mut instance_lock =
        state::open_instance_lock_file(&lock_path).map_err(DaemonError::State)?;
    let _instance_lock_guard = match instance_lock.try_write() {
        Ok(guard) => guard,
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            return Err(DaemonError::State(state::StateError::LockHeld {
                path: lock_path.clone(),
            }));
        }
        Err(e) => {
            return Err(DaemonError::State(state::StateError::LockAcquireFailed {
                path: lock_path.clone(),
                source: e,
            }));
        }
    };
    info!(
        target: "gcit::supervisor",
        path = %lock_path.display(),
        "instance lock acquired",
    );

    // 3. Load the persistent state (state.json).
    let state_path = state::path().map_err(DaemonError::State)?;
    let initial_state: State = state::load_or_init(&state_path).map_err(DaemonError::State)?;

    // 3. State writer + status mirror. The writer thread owns the
    // canonical State; `state_mirror` holds a clone refreshed after
    // every successful disk persist. Status reads borrow from the
    // mirror under a stdlib Mutex — no cross-runtime contention
    // because the mirror is only locked for the duration of a clone
    // or a single read.
    let (state_tx, state_rx) = mpsc::channel::<StateUpdate>(STATE_QUEUE);
    let state_mirror: Arc<StdMutex<State>> = Arc::new(StdMutex::new(State::default()));
    let writer_handle = state::spawn_with_mirror(
        initial_state,
        state_path,
        state_rx,
        Arc::clone(&state_mirror),
    );

    // 4. The watch carries the live `Arc<Config>` so the supervisor's
    // panic-respawn path reads the current config without holding a
    // lock or threading the value through every callsite. The watch
    // is NOT used for per-flow propagation (changed flows are
    // cancelled and respawned during reload, while unchanged flows
    // keep their existing pair alive); it remains because the
    // panic-respawn path and the control handler both need to read
    // "what config is the supervisor currently running" without an
    // extra channel.
    let (config_watch_tx, _config_watch_rx) = watch::channel(Arc::clone(&initial_config));
    let config_watch_tx = Arc::new(config_watch_tx);

    // 5. Per-credential clients + rate buckets + rate-limit pollers.
    // Cached across config reloads so SIGHUP doesn't re-issue
    // /rate_limit per credential.
    //
    // The shared HTTP client is built once here from
    // `config.http.request_timeout` and threaded through the credential
    // pool. Octocrab manages its own client; `reqwest::Client` is
    // shared by the grokmirror polling strategy.
    let shared_reqwest = Arc::new(
        reqwest::Client::builder()
            .timeout(initial_config.http.request_timeout)
            .build()
            .map_err(|e| DaemonError::HttpClient(e.to_string()))?,
    );
    let credential_pool = Arc::new(RwLock::new(CredentialPool::with_config_path(&config_path)));

    // 6. Cancellation tree: root cancel signals daemon shutdown;
    // per-flow children let us cancel a single flow during config
    // reload.
    let root_cancel = CancellationToken::new();

    // 7. Last-error tracker: keyed by flow name. Surfaced via
    // `gcit status` and updated by per-flow tasks on a panic /
    // permanent error.
    let last_errors = Arc::new(Mutex::new(BTreeMap::<String, FlowLastError>::new()));

    // 8. Hostname for local_mail notifiers — read once at boot.
    // `read_hostname_or_default` is best-effort and logs a WARN on
    // /etc/hostname read failure; subsequent reads would observe the
    // same value.
    let hostname = Arc::new(mail::read_hostname_or_default());

    // 9. Respawn channel. Panic-watcher tasks (spawned per panicked
    // flow with a RESPAWN_DELAY sleep + a request enqueue) feed the
    // supervisor's select! loop. This keeps SIGTERM/SIGHUP/control
    // commands responsive during the 30-second respawn window: the
    // sleep happens in a separate task, never inline in the select!
    // arm.
    let (respawn_tx, mut respawn_rx) =
        mpsc::channel::<super::respawn::RespawnRequest>(RESPAWN_QUEUE);

    // 10. Build per-flow notifier vectors + dispatch wiring, then spawn.
    // `respawning_flows` tracks flows that have observed a panic and
    // have a respawn timer in flight but have not yet been respawned.
    // Distinct from `registry.handles` so a clean exit of one role
    // does not silently mark the surviving panicking sibling as
    // "already-respawning" and skip the respawn (the bug fixed by
    // tracking respawn state explicitly rather than inferring it
    // from handle membership).
    let mut flow_join_set: JoinSet<FlowExit> = JoinSet::new();
    // `FlowRegistry` bundles handles + respawning_flows + pending_exits
    // — see the FlowRegistry doc for the per-collection invariants.
    let mut registry = FlowRegistry::new();
    // `SpawnContext` bundles every cross-cutting input the per-flow
    // spawn path needs: shared transports, per-flow trackers, and the
    // task-building factories. Production builds production_*_factory()
    // here; integration tests reach `run_with_factories` to inject
    // scripted factories that wrap `flow::{poll,dispatcher}::run_with_executor`.
    let spawn_ctx = SpawnContext {
        credential_pool: Arc::clone(&credential_pool),
        shared_reqwest: Arc::clone(&shared_reqwest),
        hostname: Arc::clone(&hostname),
        state_tx: state_tx.clone(),
        state_mirror: Arc::clone(&state_mirror),
        root_cancel: root_cancel.clone(),
        last_errors: Arc::clone(&last_errors),
        poll_task_factory,
        dispatch_task_factory,
    };
    spawn_initial_flows(
        &initial_config,
        &spawn_ctx,
        &mut flow_join_set,
        &mut registry,
    )
    .await;

    // 11. Bring up the control socket. Look for a `control` listen-fd;
    // fall back to binding `default_control_socket` if none.
    let control_listener = bind_control_listener(&listen_fds, &default_control_socket)?;
    let (control_cmd_tx, mut control_cmd_rx) =
        mpsc::channel::<ControlCommand>(CONTROL_COMMAND_QUEUE);
    let control_handler = Arc::new(ControlHandler {
        cmd_tx: control_cmd_tx.clone(),
        state_mirror: Arc::clone(&state_mirror),
        last_errors: Arc::clone(&last_errors),
        flow_names: Arc::new(RwLock::new(
            registry.handles.keys().cloned().collect::<Vec<_>>(),
        )),
    });
    let control_cancel = root_cancel.child_token();
    let control_handle = tokio::spawn({
        let h = Arc::clone(&control_handler);
        async move {
            control::serve(control_listener, h, control_cancel).await;
        }
    });

    // 12. Watchdog. Only fires when running under a systemd unit with
    // WatchdogSec= set — otherwise sd_notify::watchdog_enabled returns
    // None and we skip the spawn.
    let watchdog_handle = spawn_watchdog(root_cancel.clone());

    // 13. Signal handlers: SIGHUP (reload), SIGTERM, SIGINT.
    let mut sighup =
        signal(SignalKind::hangup()).map_err(|e| DaemonError::SignalSetup(e.to_string()))?;
    let mut sigterm =
        signal(SignalKind::terminate()).map_err(|e| DaemonError::SignalSetup(e.to_string()))?;
    let mut sigint =
        signal(SignalKind::interrupt()).map_err(|e| DaemonError::SignalSetup(e.to_string()))?;

    // 14. Ready. Notify systemd we're up. Failures here are non-fatal —
    // when running outside systemd, $NOTIFY_SOCKET is unset and
    // sd_notify::notify is a no-op.
    if let Err(e) = sd_notify::notify(&[NotifyState::Ready]) {
        warn!(target: "gcit::supervisor", error = %e, "sd_notify Ready failed");
    }
    info!(target: "gcit::supervisor", "daemon ready");

    // 15. Main select! loop.
    loop {
        tokio::select! {
            _ = sighup.recv() => {
                info!(target: "gcit::supervisor", "SIGHUP received; reloading");
                run_reload(
                    &config_path,
                    Arc::clone(&config_watch_tx),
                    &spawn_ctx,
                    &mut flow_join_set,
                    &mut registry,
                    &control_handler,
                ).await;
            }
            _ = sigterm.recv() => {
                info!(target: "gcit::supervisor", "SIGTERM received; shutting down");
                break;
            }
            _ = sigint.recv() => {
                info!(target: "gcit::supervisor", "SIGINT received; shutting down");
                break;
            }
            Some(cmd) = control_cmd_rx.recv() => {
                handle_control_command(
                    cmd,
                    &config_path,
                    Arc::clone(&config_watch_tx),
                    &spawn_ctx,
                    &mut flow_join_set,
                    &mut registry,
                    &control_handler,
                ).await;
            }
            Some(joined) = flow_join_set.join_next() => {
                handle_flow_exit(
                    joined,
                    Arc::clone(&last_errors),
                    respawn_tx.clone(),
                    &mut registry,
                    &root_cancel,
                ).await;
            }
            Some(req) = respawn_rx.recv() => {
                handle_respawn_request(
                    req,
                    Arc::clone(&config_watch_tx),
                    &spawn_ctx,
                    respawn_tx.clone(),
                    &mut flow_join_set,
                    &mut registry,
                    &control_handler,
                ).await;
            }
        }
    }

    // 16. Shutdown sequence:
    //     Stopping -> root.cancel() -> await flows -> drop(state_tx)
    //     -> writer.join().
    if let Err(e) = sd_notify::notify(&[NotifyState::Stopping]) {
        warn!(target: "gcit::supervisor", error = %e, "sd_notify Stopping failed");
    }
    root_cancel.cancel();
    info!(target: "gcit::supervisor", "awaiting per-flow tasks");
    while let Some(joined) = flow_join_set.join_next().await {
        if let Err(e) = joined {
            warn!(target: "gcit::supervisor", error = %e, "flow task join error during shutdown");
        }
    }
    info!(target: "gcit::supervisor", "awaiting control server");
    let _ = control_handle.await;
    if let Some(h) = watchdog_handle {
        let _ = h.await;
    }
    // Drop the SpawnContext BEFORE the local state_tx so the
    // writer's `blocking_recv_many` can observe `Disconnected` and
    // exit. SpawnContext owns its own `state_tx.clone()` (cloned in
    // step 10 above so reload / respawn can hand fresh clones to new
    // generations); leaving it live across `drop(state_tx)` would
    // pin the writer forever — every state_tx clone must drop before
    // the channel signals disconnect.
    drop(spawn_ctx);
    info!(target: "gcit::supervisor", "dropping state_tx; awaiting writer drain");
    drop(state_tx);
    if let Err(e) = writer_handle.join() {
        warn!(target: "gcit::supervisor", "state-writer thread join failed: {:?}", e);
    }
    info!(target: "gcit::supervisor", "shutdown complete");
    Ok(())
}

/// Spawn the watchdog notifier task. Returns `None` when the unit
/// does not have `WatchdogSec=` configured (sd_notify::watchdog_enabled
/// returns None).
///
/// The watchdog ticks at `interval / 2` per the standard pattern
/// (give systemd a margin so transient scheduling delays don't kill
/// the daemon mid-tick).
fn spawn_watchdog(cancel: CancellationToken) -> Option<tokio::task::JoinHandle<()>> {
    let interval = sd_notify::watchdog_enabled()?;
    let tick = interval / 2;
    info!(
        target: "gcit::supervisor",
        ?interval,
        ?tick,
        "watchdog enabled",
    );
    Some(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(tick);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = ticker.tick() => {
                    if let Err(e) = sd_notify::notify(&[NotifyState::Watchdog]) {
                        warn!(target: "gcit::supervisor", error = %e, "watchdog notify failed");
                    }
                }
            }
        }
    }))
}

/// Bind the control socket. Prefers a `control`-named listen-fd from
/// systemd; falls back to binding `default_control_socket` directly.
fn bind_control_listener(
    listen_fds: &[(RawFd, String)],
    default_path: &std::path::Path,
) -> Result<UnixListener, DaemonError> {
    if let Some((fd, _name)) = listen_fds.iter().find(|(_, name)| name == "control") {
        // Convert the inherited fd. SAFETY: systemd hands us an
        // O_CLOEXEC fd that we now own; from_raw_fd takes ownership.
        let std_listener = unsafe { std::os::unix::net::UnixListener::from_raw_fd(*fd) };
        std_listener
            .set_nonblocking(true)
            .map_err(|e| DaemonError::ControlListener(e.to_string()))?;
        UnixListener::from_std(std_listener)
            .map_err(|e| DaemonError::ControlListener(e.to_string()))
    } else {
        // Bind a fresh socket. Used in foreground mode (gcit run
        // outside systemd) and in tests.
        if let Some(parent) = default_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| DaemonError::ControlListener(e.to_string()))?;
        }
        // If a socket file is already at the path, probe it before
        // removing: a connect() success means a live daemon is
        // listening (we are NOT under systemd here, so fd-lock would
        // not have fired — e.g. operator launched a second
        // foreground instance pointing at the same --control-socket
        // path, or LISTEN_FDS got dropped). Only stale files (left
        // by a crashed prior run) are removed.
        if default_path.exists() {
            match std::os::unix::net::UnixStream::connect(default_path) {
                Ok(_) => {
                    return Err(DaemonError::ControlListener(format!(
                        "{}: another gcit daemon is already listening; refusing to start",
                        default_path.display(),
                    )));
                }
                Err(_) => {
                    let _ = std::fs::remove_file(default_path);
                }
            }
        }
        UnixListener::bind(default_path).map_err(|e| DaemonError::ControlListener(e.to_string()))
    }
}

/// Errors that can prevent the daemon from booting cleanly.
///
/// The `Config` variant's Display formats every nested `ConfigError`
/// via its own Display (path:line: message + suggestion), one per
/// line, so the operator sees actionable text rather than a `{:?}`
/// debug dump. The Display impl is hand-rolled rather than thiserror-
/// derived because the `Config` variant's formatting is not a single
/// `#[error("...")]` template.
#[derive(Debug)]
pub enum DaemonError {
    Config(Vec<ConfigError>),
    State(crate::state::StateError),
    ControlListener(String),
    SignalSetup(String),
    HttpClient(String),
}

impl std::fmt::Display for DaemonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DaemonError::Config(errs) => {
                writeln!(f, "config: {} error(s):", errs.len())?;
                for e in errs {
                    writeln!(f, "  - {}", e)?;
                }
                Ok(())
            }
            DaemonError::State(e) => write!(f, "state: {}", e),
            DaemonError::ControlListener(s) => write!(f, "control listener: {}", s),
            DaemonError::SignalSetup(s) => write!(f, "signal setup: {}", s),
            DaemonError::HttpClient(s) => write!(f, "http client construction: {}", s),
        }
    }
}

impl std::error::Error for DaemonError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DaemonError::State(e) => Some(e),
            // Other variants carry strings or vectors; no nested
            // source. ConfigError's Vec is rendered into Display
            // directly above.
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigError;
    use std::error::Error as _;

    fn make_config_error() -> ConfigError {
        ConfigError::Parse {
            path: PathBuf::from("config.toml"),
            line: 7,
            message: "duplicate flow name".to_string(),
        }
    }

    #[test]
    fn daemon_error_config_display_starts_with_config_and_lists_count() {
        let err = DaemonError::Config(vec![make_config_error()]);
        let rendered = err.to_string();
        assert!(
            rendered.starts_with("config: "),
            "Config Display must lead with 'config: '; got: {rendered}",
        );
        assert!(
            rendered.contains("error(s)"),
            "Config Display must surface the count phrase 'error(s)'; got: {rendered}",
        );
    }

    #[test]
    fn daemon_error_config_display_renders_each_nested_error_on_its_own_line() {
        let err = DaemonError::Config(vec![make_config_error(), make_config_error()]);
        let rendered = err.to_string();
        let dashes = rendered.matches("\n  - ").count();
        assert_eq!(
            dashes, 2,
            "Config Display must render one bulleted line per nested ConfigError; got: {rendered}",
        );
    }

    #[test]
    fn daemon_error_state_display_starts_with_state_prefix() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "missing");
        let state_err = crate::state::StateError::LockOpen {
            path: PathBuf::from("state.json"),
            source: io_err,
        };
        let rendered = DaemonError::State(state_err).to_string();
        assert!(
            rendered.starts_with("state: "),
            "State Display must lead with 'state: '; got: {rendered}",
        );
    }

    #[test]
    fn daemon_error_control_listener_display_starts_with_control_listener_prefix() {
        let rendered =
            DaemonError::ControlListener("bind socket.path: address in use".to_string())
                .to_string();
        assert!(
            rendered.starts_with("control listener: "),
            "ControlListener Display must lead with 'control listener: '; got: {rendered}",
        );
    }

    #[test]
    fn daemon_error_signal_setup_display_starts_with_signal_setup_prefix() {
        let rendered =
            DaemonError::SignalSetup("registering SIGHUP failed".to_string()).to_string();
        assert!(
            rendered.starts_with("signal setup: "),
            "SignalSetup Display must lead with 'signal setup: '; got: {rendered}",
        );
    }

    #[test]
    fn daemon_error_http_client_display_starts_with_http_client_construction_prefix() {
        // Pin the FULL production prefix ("http client construction: ")
        // rather than a substring like "http client". A regression that
        // shortens the prefix to "http client error:" or rewords it
        // would pass a loose `contains("http client")` check while
        // breaking the operator-readable message that journald
        // consumers grep for.
        let rendered = DaemonError::HttpClient("invalid TLS config".to_string()).to_string();
        assert!(
            rendered.starts_with("http client construction: "),
            "HttpClient Display must lead with 'http client construction: '; got: {rendered}",
        );
    }

    #[test]
    fn daemon_error_source_returns_some_for_state_variant() {
        // Only the State variant carries a nested error; source()
        // surfaces it so callers walking the error chain can reach the
        // underlying `state::StateError` and its inner io::Error.
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let state_err = crate::state::StateError::LockOpen {
            path: PathBuf::from("state.json"),
            source: io_err,
        };
        let err = DaemonError::State(state_err);
        assert!(
            err.source().is_some(),
            "State variant must surface its nested StateError via source()",
        );
    }

    #[test]
    fn daemon_error_source_returns_none_for_string_carrying_variants() {
        // Per the `_ => None` arm in DaemonError::source: every variant
        // other than State (Config carries a Vec, the rest carry
        // String) returns None. Pin all four so a future variant rename
        // doesn't silently start surfacing a synthesized source.
        assert!(DaemonError::Config(vec![make_config_error()]).source().is_none());
        assert!(DaemonError::ControlListener("x".to_string()).source().is_none());
        assert!(DaemonError::SignalSetup("y".to_string()).source().is_none());
        assert!(DaemonError::HttpClient("z".to_string()).source().is_none());
    }

    #[tokio::test]
    async fn bind_control_listener_falls_back_to_default_path_when_no_listen_fd() {
        // Empty listen_fds: the function takes the fallback branch and
        // creates a fresh socket file at `default_path`. After the call
        // the path must point at a unix-domain SOCKET (not a regular
        // file) — pin the file type via FileTypeExt::is_socket so a
        // regression that opens the path with `File::create` (which
        // would also produce a non-empty file) is caught before it
        // breaks every control client.
        use std::os::unix::fs::FileTypeExt;
        let tmp = tempfile::tempdir().expect("tempdir");
        let socket_path = tmp.path().join("gcit.sock");
        let listener =
            bind_control_listener(&[], &socket_path).expect("bind must succeed on empty fds");
        assert!(
            socket_path.exists(),
            "fallback path must produce a socket file on disk; expected at {}",
            socket_path.display(),
        );
        let meta = std::fs::metadata(&socket_path).expect("stat the bound path");
        assert!(
            meta.file_type().is_socket(),
            "fallback bind must produce a unix-domain socket; got file_type={:?}",
            meta.file_type(),
        );
        // Drop the listener so the fd is closed before the tempdir's
        // Drop runs `remove_dir_all`. UnixListener::drop closes the
        // file descriptor only — the socket file in the filesystem is
        // removed by the tempdir teardown, not by listener drop.
        drop(listener);
    }

    #[tokio::test]
    async fn bind_control_listener_removes_stale_socket_file_left_by_crashed_run() {
        // A prior crashed daemon may have left a regular file at the
        // socket path with no listener attached. The fallback branch
        // probes via UnixStream::connect; the connect fails (no
        // listener), and the function removes the file and binds a
        // fresh socket on top. Pin BOTH that the path exists AFTER
        // the rebind AND that it is a unix-domain socket (not the
        // stale regular file) — without the `is_socket` check, a
        // regression that skipped the remove_file step would leave
        // the regular file in place and pass the `exists()` check
        // while breaking the control listener.
        use std::os::unix::fs::FileTypeExt;
        let tmp = tempfile::tempdir().expect("tempdir");
        let socket_path = tmp.path().join("gcit.sock");
        std::fs::write(&socket_path, "stale data").expect("write stale file");
        assert!(socket_path.exists(), "precondition: stale file present");
        let listener = bind_control_listener(&[], &socket_path)
            .expect("bind must succeed after removing stale socket");
        assert!(
            socket_path.exists(),
            "after rebind the new socket file must exist at {}",
            socket_path.display(),
        );
        let meta = std::fs::metadata(&socket_path).expect("stat after rebind");
        assert!(
            meta.file_type().is_socket(),
            "rebind must replace the stale regular file with a unix-domain socket; got file_type={:?}",
            meta.file_type(),
        );
        // Drop the listener so the fd is closed before the tempdir's
        // Drop runs `remove_dir_all`. UnixListener::drop only closes
        // the fd; the filesystem entry is cleaned up by the tempdir.
        drop(listener);
    }

    #[tokio::test]
    async fn bind_control_listener_refuses_when_a_live_daemon_is_already_listening() {
        // A second foreground gcit pointed at the same --control-socket
        // path must fail loudly rather than racing the live one. Bind
        // a real UnixListener at the path first; the probe in
        // bind_control_listener succeeds (connect is accepted) and the
        // function returns ControlListener with the production-canonical
        // message AND names the colliding socket path so the operator
        // can resolve the conflict without log-diving.
        let tmp = tempfile::tempdir().expect("tempdir");
        let socket_path = tmp.path().join("gcit.sock");
        let _live = std::os::unix::net::UnixListener::bind(&socket_path)
            .expect("setup: bind real listener at the path");
        let err = bind_control_listener(&[], &socket_path)
            .expect_err("must reject when another daemon is listening");
        match err {
            DaemonError::ControlListener(msg) => {
                assert!(
                    msg.contains("another gcit daemon is already listening"),
                    "rejection must surface the full production phrase; got: {msg}",
                );
                let path_str = socket_path.display().to_string();
                assert!(
                    msg.contains(&path_str),
                    "rejection must name the colliding socket path {path_str}; got: {msg}",
                );
            }
            other => panic!("expected ControlListener variant; got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn bind_control_listener_creates_missing_parent_directories_on_fallback() {
        // Production callers may point `--control-socket` at a path
        // whose parent directory does not yet exist (e.g. fresh
        // `$XDG_RUNTIME_DIR/gcit/control` after a daemon restart on a
        // tmpfs that drops empty subtrees). The fallback branch's
        // `fs::create_dir_all(parent)` must materialise every missing
        // component before binding the socket. The earlier fallback
        // test only exercised `tmp.path()` (which already exists), so
        // the create_dir_all call was effectively a no-op there.
        let tmp = tempfile::tempdir().expect("tempdir");
        let socket_path = tmp.path().join("nested/dirs/gcit.sock");
        let parent = socket_path.parent().expect("socket has parent");
        assert!(
            !parent.exists(),
            "precondition: nested parent must not yet exist",
        );
        let listener =
            bind_control_listener(&[], &socket_path).expect("bind must materialise parents");
        assert!(
            parent.exists(),
            "fallback bind must create the missing parent directory chain at {}",
            parent.display(),
        );
        assert!(
            socket_path.exists(),
            "fallback bind must produce the socket file at {}",
            socket_path.display(),
        );
        drop(listener);
    }
}

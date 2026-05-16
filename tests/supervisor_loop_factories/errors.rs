// Boot-time fatal errors: duplicate flow names → DaemonError::Config,
// contended flock → DaemonError::State(LockHeld). Both surface
// BEFORE the lock-acquire / state-load / factory-spawn / signal-
// handler installation steps run, so the panic-if-invoked factories
// hold the gate.

use std::sync::Arc;

use gcit::flow::supervisor::{
    run_with_factories, DaemonParams, DispatchTaskFactory, PollTaskFactory,
};

use super::fixtures::{
    setup_daemon_fixture, teardown_daemon_env, write_config, ONE_FLOW_CONFIG,
    TWO_FLOWS_DUPLICATE_NAME_CONFIG,
};

#[tokio::test(flavor = "current_thread")]
#[serial_test::serial]
async fn supervisor_run_with_factories_returns_daemon_error_config_on_duplicate_flow_names() {
    let fixture = setup_daemon_fixture();
    let config_path = write_config(fixture.config_dir.path(), TWO_FLOWS_DUPLICATE_NAME_CONFIG);
    let control_socket = fixture.runtime_dir.path().join("control.sock");

    // Factories panic if invoked. They MUST NOT be — the boot path
    // exits at the config-load Err arm before spawn_initial_flows.
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

    let errs = match err {
        gcit::flow::supervisor::DaemonError::Config(errs) => errs,
        other => panic!("duplicate flow names must surface as DaemonError::Config; got: {other:?}"),
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

#[tokio::test(flavor = "current_thread")]
#[serial_test::serial]
async fn supervisor_run_with_factories_returns_daemon_error_state_lock_held_when_flock_held() {
    // Pre-acquire the flock on `$RUNTIME_DIRECTORY/gcit.lock` from a
    // separate file descriptor in the same process. flock locks attach
    // to OFDs (open file descriptions); two distinct open() calls
    // mutually exclude each other.
    let fixture = setup_daemon_fixture();
    let config_path = write_config(fixture.config_dir.path(), ONE_FLOW_CONFIG);
    let control_socket = fixture.runtime_dir.path().join("control.sock");

    let lock_path = fixture.runtime_dir.path().join("gcit.lock");
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
            let rendered = format!("{state_err}");
            assert!(
                rendered.contains("another gcit instance is running"),
                "LockHeld Display must surface the canonical 'another gcit instance is running' message; got: {rendered}",
            );
            assert!(
                rendered.contains(&lock_path.display().to_string()),
                "LockHeld Display must name the contended lock path; got: {rendered}",
            );
        }
        other => panic!("contended flock must surface as DaemonError::State; got: {other:?}"),
    }

    // Drop the held lock guard explicitly so the tempdir cleanup
    // succeeds.
    drop(_held_guard);
    drop(held_lock);

    teardown_daemon_env();
    drop(fixture);
}

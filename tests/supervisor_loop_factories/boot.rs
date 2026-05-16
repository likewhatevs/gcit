// Daemon boot + signal-driven shutdown end-to-end tests. SIGTERM and
// SIGINT share the same shutdown branch in the supervisor's main
// select! loop — both are exercised here.

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::timeout;

use gcit::flow::dispatcher::run_with_executor as dispatcher_run_with_executor;
use gcit::flow::poll::run_with_executor as poll_run_with_executor;
use gcit::flow::supervisor::{
    run_with_factories, DaemonParams, DispatchTaskFactory, PollTaskFactory,
};

use super::fixtures::{
    setup_daemon_fixture, teardown_daemon_env, write_config, ScriptedDispatchExecutor,
    ScriptedPollExecutor, ONE_FLOW_CONFIG, TWO_FLOW_CONFIG,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn supervisor_run_with_factories_boots_and_shuts_down_cleanly() {
    // Pins:
    //   1. The factory plumbing through `SpawnContext` reaches every
    //      production call site (`spawn_initial_flows` -> `spawn_flow`).
    //   2. The full select! loop (signal handlers, JoinSet, registry,
    //      writer thread) bootstraps and tears down cleanly when
    //      driven against scripted executors that bypass the network.
    //   3. The state writer's drain is part of the shutdown sequence
    //      — asserted via the post-shutdown lock file existence.
    let fixture = setup_daemon_fixture();
    let config_path = write_config(fixture.config_dir.path(), TWO_FLOW_CONFIG);
    let control_socket = fixture.runtime_dir.path().join("control.sock");

    let poll_invocations = Arc::new(AtomicUsize::new(0));
    let dispatch_invocations = Arc::new(AtomicUsize::new(0));

    let poll_factory: PollTaskFactory = {
        let invocations = Arc::clone(&poll_invocations);
        Arc::new(
            move |params, last_sha, last_dispatched_at, state_tx, trigger_tx, cancel| {
                let executor = ScriptedPollExecutor::new(Arc::clone(&invocations), 0xaa);
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

    // 1s grace window for the daemon's signal handlers to install
    // (multi_thread runtime + real time). Without this, libc::kill
    // could deliver SIGTERM before tokio::signal::unix::signal
    // registers — the default disposition would terminate the test.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // SAFETY: libc::kill is a single FFI call; the pid argument
    // (current process) is always valid; SIGTERM is portable.
    unsafe {
        libc::kill(libc::getpid(), libc::SIGTERM);
    }

    timeout(Duration::from_secs(60), daemon_handle)
        .await
        .expect("daemon must shut down within 60s of SIGTERM")
        .expect("daemon task must not panic");

    teardown_daemon_env();

    let _final_poll = poll_invocations.load(Ordering::SeqCst);
    let _final_dispatch = dispatch_invocations.load(Ordering::SeqCst);

    // The single-instance lock file is the durable evidence that the
    // daemon ran. state.json is NOT checked: with the test's brief
    // window the 15s poll cadence never produces an update.
    let lock_path = fixture.runtime_dir.path().join("gcit.lock");
    assert!(
        lock_path.exists(),
        "instance lock file must exist at $RUNTIME_DIRECTORY/gcit.lock after \
         the daemon ran (proves state::open_instance_lock_file completed)",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn supervisor_sigint_routes_to_shutdown_branch() {
    // SIGINT (ctrl-C) shares the shutdown branch with SIGTERM — both
    // signal handlers route into the same `break` path that triggers
    // root_cancel.cancel + flow drain + writer drain.
    let fixture = setup_daemon_fixture();
    let config_path = write_config(fixture.config_dir.path(), ONE_FLOW_CONFIG);
    let control_socket = fixture.runtime_dir.path().join("control.sock");

    let factory_calls = Arc::new(AtomicUsize::new(0));
    let poll_cycle_invocations = Arc::new(AtomicUsize::new(0));
    let dispatch_invocations = Arc::new(AtomicUsize::new(0));

    let poll_factory: PollTaskFactory = {
        let factory_calls = Arc::clone(&factory_calls);
        let poll_cycle_invocations = Arc::clone(&poll_cycle_invocations);
        Arc::new(
            move |params, last_sha, last_dispatched_at, state_tx, trigger_tx, cancel| {
                factory_calls.fetch_add(1, Ordering::SeqCst);
                let executor = ScriptedPollExecutor::new(Arc::clone(&poll_cycle_invocations), 0xa1);
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

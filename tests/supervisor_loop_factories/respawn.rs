// Panic-respawn pipeline end-to-end test. A panicking poll-task
// future surfaces as `FlowExit { panic: Some }`; the supervisor's
// `decide_respawn` classifier lands on `PanicFirst`, records
// last_error with `kind="panic"`, arms the panic-watcher, and after
// RESPAWN_DELAY enqueues a `RespawnRequest`. The new generation's
// first successful poll cycle clears the stale `last_error`.

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
    fetch_status, setup_daemon_fixture, teardown_daemon_env, wait_for_control_socket, write_config,
    ScriptedDispatchExecutor, ScriptedPollExecutor, ONE_FLOW_CONFIG,
};

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[serial_test::serial]
async fn supervisor_respawns_panicked_flow_and_clears_last_error() {
    let fixture = setup_daemon_fixture();
    let config_path = write_config(fixture.config_dir.path(), ONE_FLOW_CONFIG);
    let control_socket = fixture.runtime_dir.path().join("control.sock");

    // Per-factory invocation counter. The factory counter ticks at
    // SPAWN time (synchronous closure body); the cycle counter ticks
    // at POLL_CYCLE time (asynchronous executor invocation).
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let poll_cycle_invocations = Arc::new(AtomicUsize::new(0));
    let dispatch_invocations = Arc::new(AtomicUsize::new(0));

    let poll_factory: PollTaskFactory = {
        let factory_calls = Arc::clone(&factory_calls);
        let poll_cycle_invocations = Arc::clone(&poll_cycle_invocations);
        Arc::new(
            move |params, last_sha, last_dispatched_at, state_tx, trigger_tx, cancel| {
                let call_n = factory_calls.fetch_add(1, Ordering::SeqCst);
                if call_n == 0 {
                    // Gen-1: synthesize a future whose first poll
                    // panics. The catch_unwind wrapper in
                    // `flows::spawn_flow` converts the panic into
                    // `FlowExit { panic: Some(_) }` which the
                    // supervisor's `handle_flow_exit` classifies as
                    // `PanicFirst`.
                    Box::pin(async move {
                        panic!("scripted-gen1-poll-panic");
                    })
                } else {
                    // Gen-2+: clean ScriptedPollExecutor. The first
                    // successful poll cycle clears the stale "panic"
                    // last_error per the sticky-error fix in
                    // flow::poll::run_with_executor.
                    let executor =
                        ScriptedPollExecutor::new(Arc::clone(&poll_cycle_invocations), 0xbb);
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
                }
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
        default_control_socket: control_socket.clone(),
        listen_fds: Vec::new(),
    };

    let daemon_handle = tokio::spawn(async move {
        run_with_factories(params, poll_factory, dispatch_factory)
            .await
            .expect("daemon must complete cleanly")
    });

    wait_for_control_socket(&control_socket).await;

    // Yield for gen-1's panic to surface and last_error to record.
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

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
    // resolves and enqueues the RespawnRequest.
    tokio::time::advance(gcit::flow::RESPAWN_DELAY).await;
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
    // last_error clear path runs.
    let source_interval = Duration::from_secs(15);
    tokio::time::advance(source_interval + Duration::from_secs(1)).await;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let status = fetch_status(&control_socket, Some("flow-a")).await;
    assert!(
        status["flow-a"]["last_error"].is_null(),
        "post-recovery status must clear last_error to null; got: {status}",
    );

    unsafe {
        libc::kill(libc::getpid(), libc::SIGTERM);
    }

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
    drop(fixture);
}

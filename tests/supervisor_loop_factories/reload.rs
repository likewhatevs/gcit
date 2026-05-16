// SIGHUP reload pipeline tests:
//   * URL change → `Restart { url_changed: true }` arm.
//   * Adding a flow → `Spawn` arm (kept flow stays alive).
//   * Non-URL change (ref_name) → `Restart { url_changed: false }` arm.

use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::time::timeout;

use gcit::flow::supervisor::{run_with_factories, DaemonParams};

use super::fixtures::{
    build_test_factories, fetch_status, setup_daemon_fixture, teardown_daemon_env,
    wait_for_control_socket, write_config, ONE_FLOW_CONFIG, ONE_FLOW_CONFIG_REF_CHANGED,
    ONE_FLOW_CONFIG_URL_CHANGED, TWO_FLOW_CONFIG_FOR_RELOAD,
};

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[serial_test::serial]
async fn supervisor_sighup_reload_restarts_url_changed_flow() {
    // SIGHUP routes into the supervisor's `sighup.recv()` arm; the
    // arm calls `run_reload`, which re-parses the config and runs
    // `compute_reload_actions`. URL change → `Restart { url_changed:
    // true }`.
    let fixture = setup_daemon_fixture();
    let config_path = write_config(fixture.config_dir.path(), ONE_FLOW_CONFIG);
    let control_socket = fixture.runtime_dir.path().join("control.sock");

    let factories = build_test_factories(0xcc);
    let factory_calls = std::sync::Arc::clone(&factories.factory_calls);

    let reload_config_path = config_path.clone();
    let params = DaemonParams {
        config_path,
        default_control_socket: control_socket.clone(),
        listen_fds: Vec::new(),
    };

    let daemon_handle = tokio::spawn(async move {
        run_with_factories(params, factories.poll_factory, factories.dispatch_factory)
            .await
            .expect("daemon must complete cleanly")
    });

    wait_for_control_socket(&control_socket).await;

    assert!(
        factory_calls.load(Ordering::SeqCst) >= 1,
        "boot must invoke poll-task factory at least once; got {}",
        factory_calls.load(Ordering::SeqCst),
    );

    let status_pre = fetch_status(&control_socket, Some("flow-a")).await;
    assert!(
        !status_pre["flow-a"].is_null(),
        "pre-reload status must list flow-a; got: {status_pre}",
    );

    std::fs::write(&reload_config_path, ONE_FLOW_CONFIG_URL_CHANGED)
        .expect("rewrite config with url change");

    // SAFETY: libc::kill on the current pid with a portable signal
    // (SIGHUP) is async-signal-safe.
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
                 (boot + reload restart); got {} after {} polls",
                factory_calls.load(Ordering::SeqCst),
                polled,
            );
        }
    }

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

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[serial_test::serial]
async fn supervisor_sighup_reload_adds_new_flow_via_spawn_arm() {
    // SIGHUP reload that ADDS a flow (1 → 2 flows). Keep arm leaves
    // the existing flow alone; Spawn arm adds the new one.
    let fixture = setup_daemon_fixture();
    let config_path = write_config(fixture.config_dir.path(), ONE_FLOW_CONFIG);
    let control_socket = fixture.runtime_dir.path().join("control.sock");

    let factories = build_test_factories(0xdd);
    let factory_calls = std::sync::Arc::clone(&factories.factory_calls);

    let reload_config_path = config_path.clone();
    let params = DaemonParams {
        config_path,
        default_control_socket: control_socket.clone(),
        listen_fds: Vec::new(),
    };

    let daemon_handle = tokio::spawn(async move {
        run_with_factories(params, factories.poll_factory, factories.dispatch_factory)
            .await
            .expect("daemon must complete cleanly")
    });

    wait_for_control_socket(&control_socket).await;

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

    std::fs::write(&reload_config_path, TWO_FLOW_CONFIG_FOR_RELOAD)
        .expect("rewrite to 2-flow config");
    unsafe {
        libc::kill(libc::getpid(), libc::SIGHUP);
    }

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
    // Grace window for any erroneous re-spawn of the kept flow.
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        factory_calls.load(Ordering::SeqCst),
        2,
        "post-add factory_calls must be exactly 2 (boot for flow-a + Spawn for flow-b); \
         3+ would imply the Keep arm mis-classified flow-a as Restart and re-spawned it",
    );

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

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[serial_test::serial]
async fn supervisor_sighup_reload_restarts_non_url_change_via_restart_arm() {
    // SIGHUP reload changing `ref_name` (NOT `source.url`). Drives
    // the `Restart { url_changed: false }` arm.
    let fixture = setup_daemon_fixture();
    let config_path = write_config(fixture.config_dir.path(), ONE_FLOW_CONFIG);
    let control_socket = fixture.runtime_dir.path().join("control.sock");

    let factories = build_test_factories(0xee);
    let factory_calls = std::sync::Arc::clone(&factories.factory_calls);

    let reload_config_path = config_path.clone();
    let params = DaemonParams {
        config_path,
        default_control_socket: control_socket.clone(),
        listen_fds: Vec::new(),
    };

    let daemon_handle = tokio::spawn(async move {
        run_with_factories(params, factories.poll_factory, factories.dispatch_factory)
            .await
            .expect("daemon must complete cleanly")
    });

    wait_for_control_socket(&control_socket).await;
    assert_eq!(factory_calls.load(Ordering::SeqCst), 1);

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

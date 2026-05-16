// Control-command Trigger {dry_run} round-trips through the
// supervisor's cmd_rx select! arm → handle_control_command →
// run_trigger → render_dry_run_payload. The dry-run path stays
// in-memory and does not contact GitHub or any notifier.

use std::time::Duration;

use tokio::time::timeout;
use uuid::Uuid;

use gcit::flow::supervisor::{run_with_factories, DaemonParams};

use super::fixtures::{
    build_test_factories, setup_daemon_fixture, teardown_daemon_env, wait_for_control_socket,
    write_config, ONE_FLOW_CONFIG,
};

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[serial_test::serial]
async fn supervisor_control_command_trigger_dry_run_returns_rendered_payload() {
    let fixture = setup_daemon_fixture();
    let config_path = write_config(fixture.config_dir.path(), ONE_FLOW_CONFIG);
    let control_socket = fixture.runtime_dir.path().join("control.sock");

    let factories = build_test_factories(0xff);

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

    let (resp_id, data) = match resp {
        gcit::control::Response::Ok { id, data } => (id, data),
        gcit::control::Response::Error { message, .. } => {
            panic!("trigger --dry-run must succeed; got Error: {message}")
        }
    };
    assert_eq!(resp_id, req_id, "response id must echo the request id");

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
        "gcit_run_id must be a parseable UUID (render_dry_run_payload uses Uuid::new_v4); got: {run_id_str}",
    );
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
        "rendered_inputs.gcit_run_id must match the top-level gcit_run_id; got: {rendered:?}",
    );

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

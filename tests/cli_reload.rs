// Black-box tests for `gcit reload` against a fake daemon.
//
// `tests/cli_control_socket_unreachable.rs` covers the
// daemon-not-running arm. This file covers the response-driven
// arms — Response::Ok (exit 0), Response::Error (exit 75 with
// message on stderr), and an id-mismatched reply (transport error
// branch). The wire-format round-trip is also pinned by capturing
// the Request the fake daemon receives and asserting on the
// Request::Reload shape.

mod common;

use std::sync::{Arc, Mutex as StdMutex};

use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::json;
use tokio::runtime::Builder as RtBuilder;
use uuid::Uuid;

use gcit::control::{Request, Response};

use crate::common::fake_daemon::FakeDaemon;

const OK: i32 = 0;
const TEMPFAIL: i32 = 75;

fn multi_thread_runtime() -> tokio::runtime::Runtime {
    RtBuilder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("test runtime")
}

#[test]
fn reload_ok_exits_zero_and_forwards_reload_request() {
    // Daemon answers Response::Ok — the post-SIGHUP-style success
    // path. The CLI must exit 0 without emitting anything to stdout
    // (reload is fire-and-forget; no rendered payload to print). Pin
    // the wire shape so a regression that swapped Reload for another
    // variant surfaces here.
    let rt = multi_thread_runtime();
    let received: Arc<StdMutex<Option<Request>>> = Arc::new(StdMutex::new(None));
    let received_for_script = Arc::clone(&received);
    let daemon = rt.block_on(FakeDaemon::spawn(move |req| {
        *received_for_script.lock().expect("script lock") = Some(req.clone());
        Response::Ok {
            id: req.id(),
            data: json!({}),
        }
    }));

    Command::cargo_bin("gcit")
        .unwrap()
        .arg("--control-socket")
        .arg(&daemon.socket_path)
        .arg("reload")
        .assert()
        .code(OK);

    let captured = received
        .lock()
        .expect("captured lock")
        .take()
        .expect("daemon must have received a request");
    match captured {
        Request::Reload { .. } => {}
        other => panic!("expected Request::Reload; got {other:?}"),
    }

    rt.block_on(daemon.shutdown());
}

#[test]
fn reload_error_surfaces_message_on_stderr_and_exits_tempfail() {
    // Daemon answers Response::Error — typical paths are the
    // 1-per-second rate limit hit, or a config-reload that surfaced
    // a parse failure. Either way the CLI must re-emit the message
    // on stderr and exit TEMPFAIL.
    let rt = multi_thread_runtime();
    let daemon = rt.block_on(FakeDaemon::spawn(|req| Response::Error {
        id: req.id(),
        message: "reload rate-limited".to_string(),
    }));

    Command::cargo_bin("gcit")
        .unwrap()
        .arg("--control-socket")
        .arg(&daemon.socket_path)
        .arg("reload")
        .assert()
        .code(TEMPFAIL)
        .stderr(predicate::str::contains("gcit reload: reload rate-limited"));

    rt.block_on(daemon.shutdown());
}

#[test]
fn reload_id_mismatch_in_daemon_reply_surfaces_as_transport_error() {
    // Mirror the same id-correlation pin as cli_trigger.rs: a reply
    // carrying the wrong uuid must be rejected at the transport
    // layer, not treated as an OK. Exit TEMPFAIL with "transport
    // error" on stderr.
    let rt = multi_thread_runtime();
    let daemon = rt.block_on(FakeDaemon::spawn(|_req| Response::Ok {
        id: Uuid::nil(),
        data: json!({}),
    }));

    Command::cargo_bin("gcit")
        .unwrap()
        .arg("--control-socket")
        .arg(&daemon.socket_path)
        .arg("reload")
        .assert()
        .code(TEMPFAIL)
        .stderr(predicate::str::contains("gcit reload: transport error"));

    rt.block_on(daemon.shutdown());
}

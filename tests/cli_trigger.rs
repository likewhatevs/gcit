// Black-box tests for `gcit trigger` against a fake daemon.
//
// `tests/cli_control_socket_unreachable.rs` already covers the
// "daemon not running" arm (exit 75 with the connect-error hint).
// This file covers the response-driven arms — what `gcit trigger`
// does when the daemon answers with `Response::Ok`, with
// `Response::Error`, with a malformed reply, or before the request
// even reaches the wire (empty-flow guard).
//
// The `FakeDaemon` harness binds a Unix socket inside a tempdir and
// returns the path the CLI must be pointed at via
// `--control-socket`. Each test wires a one-shot scripted response
// then drives the binary through `assert_cmd`. The runtime stays
// alive on a multi-thread `tokio::runtime` so the accept loop is
// driven concurrently with the blocking CLI invocation.

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
const USAGE: i32 = 64;
const TEMPFAIL: i32 = 75;

fn multi_thread_runtime() -> tokio::runtime::Runtime {
    // 2 workers: one for the accept loop, one for the per-connection
    // task. The blocking `Command::assert()` call runs on the test
    // thread (outside the runtime), so the runtime workers stay
    // available for the listener regardless of CLI duration.
    RtBuilder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("test runtime")
}

#[test]
fn trigger_ok_prints_data_payload_and_exits_zero_and_forwards_request_fields() {
    // Happy path: daemon answers Response::Ok with a dry-run-style
    // data payload. The CLI must pretty-print the JSON to stdout and
    // exit 0 — operators piping `gcit trigger --dry-run` into `jq`
    // rely on the response being a single self-contained JSON value.
    //
    // Capture the Request the daemon receives to pin the wire-format
    // contract: `gcit trigger <FLOW> [--dry-run]` must serialize as
    // a Request::Trigger carrying the literal flow name and the
    // dry_run bool from the CLI.
    let rt = multi_thread_runtime();
    let received: Arc<StdMutex<Option<Request>>> = Arc::new(StdMutex::new(None));
    let received_for_script = Arc::clone(&received);
    let daemon = rt.block_on(FakeDaemon::spawn(move |req| {
        *received_for_script.lock().expect("script lock") = Some(req.clone());
        Response::Ok {
            id: req.id(),
            data: json!({
                "dry_run": true,
                "rendered_inputs": {"branch": "main"}
            }),
        }
    }));

    Command::cargo_bin("gcit")
        .unwrap()
        .arg("--control-socket")
        .arg(&daemon.socket_path)
        .arg("trigger")
        .arg("my-flow")
        .arg("--dry-run")
        .assert()
        .code(OK)
        .stdout(predicate::str::contains("\"dry_run\": true"))
        .stdout(predicate::str::contains("\"rendered_inputs\""));

    let captured = received
        .lock()
        .expect("captured lock")
        .take()
        .expect("daemon must have received a request");
    match captured {
        Request::Trigger { flow, dry_run, .. } => {
            assert_eq!(flow, "my-flow", "flow name must round-trip verbatim");
            assert!(dry_run, "--dry-run must serialize as dry_run=true");
        }
        other => panic!("expected Request::Trigger; got {other:?}"),
    }

    rt.block_on(daemon.shutdown());
}

#[test]
fn trigger_without_dry_run_sets_dry_run_false_on_wire() {
    // Symmetric pin to the --dry-run test: omitting the flag must
    // serialize as `dry_run: false`. A regression that flipped the
    // clap default to `true` would silently turn every `gcit trigger`
    // into a render-only no-op — pin the wire shape so it surfaces.
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
        .arg("trigger")
        .arg("real-flow")
        .assert()
        .code(OK);

    let captured = received
        .lock()
        .expect("captured lock")
        .take()
        .expect("daemon must have received a request");
    match captured {
        Request::Trigger { dry_run, flow, .. } => {
            assert_eq!(flow, "real-flow");
            assert!(!dry_run, "omitted --dry-run must serialize as false");
        }
        other => panic!("expected Request::Trigger; got {other:?}"),
    }

    rt.block_on(daemon.shutdown());
}

#[test]
fn trigger_error_surfaces_message_on_stderr_and_exits_tempfail() {
    // Daemon answers Response::Error — the supervisor's typical
    // "unknown flow" or "dispatch failed" path. The CLI must
    // re-emit the error body on stderr verbatim and exit TEMPFAIL.
    let rt = multi_thread_runtime();
    let daemon = rt.block_on(FakeDaemon::spawn(|req| Response::Error {
        id: req.id(),
        message: "unknown flow: missing-flow".to_string(),
    }));

    Command::cargo_bin("gcit")
        .unwrap()
        .arg("--control-socket")
        .arg(&daemon.socket_path)
        .arg("trigger")
        .arg("missing-flow")
        .assert()
        .code(TEMPFAIL)
        .stderr(predicate::str::contains(
            "gcit trigger: unknown flow: missing-flow",
        ));

    rt.block_on(daemon.shutdown());
}

#[test]
fn trigger_empty_flow_name_short_circuits_to_usage_before_connect() {
    // The empty-flow guard fires before `Client::connect`, so the
    // socket arg never matters. Pin the USAGE exit and the
    // stderr-message contract so a regression that lowered the guard
    // to a no-op (and silently sent `flow: ""` over the wire) would
    // surface here. We point at an irrelevant tempdir path so any
    // connect attempt would fail loudly — the test passes by exiting
    // USAGE before that.
    let td = tempfile::tempdir().expect("tempdir");
    let sock = td.path().join("never-used.sock");
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("--control-socket")
        .arg(&sock)
        .arg("trigger")
        .arg("")
        .assert()
        .code(USAGE)
        .stderr(predicate::str::contains(
            "FLOW name is required and must not be empty",
        ));
}

#[test]
fn trigger_id_mismatch_in_daemon_reply_surfaces_as_transport_error() {
    // A daemon that returns Response::Ok with a wrong id (multiplex
    // bug, crossed wires, replay attack against the same socket)
    // must NOT be treated as a real OK. The client's id-correlation
    // check rejects the frame, the CLI surfaces it as "transport
    // error: ...", and exits TEMPFAIL — pin the path so a regression
    // that loosened the check to "first reply wins" surfaces here.
    let rt = multi_thread_runtime();
    let daemon = rt.block_on(FakeDaemon::spawn(|_req| Response::Ok {
        id: Uuid::nil(),
        data: json!({}),
    }));

    Command::cargo_bin("gcit")
        .unwrap()
        .arg("--control-socket")
        .arg(&daemon.socket_path)
        .arg("trigger")
        .arg("any-flow")
        .assert()
        .code(TEMPFAIL)
        .stderr(predicate::str::contains("gcit trigger: transport error"));

    rt.block_on(daemon.shutdown());
}

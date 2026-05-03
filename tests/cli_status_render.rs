// Black-box CLI tests for `gcit status` against a live in-process
// control listener. Drives the success-path branches of
// `cli::status::run` and every branch of `cli::status::render_text`
// that no other test reaches.
//
// Architecture: each test binds a `tokio::net::UnixListener` on a
// tempfile path, spawns `gcit::control::serve` against a canned
// `Handler` that returns a fixed JSON shape, then runs the production
// `gcit` binary via `assert_cmd` with `--control-socket <path> status`.
// The binary connects, sends `Request::Status`, the canned handler
// replies, and the binary's `cli::status::run` formats the response.
// The test asserts on stdout/stderr + exit code so a regression in the
// renderer or the wire-format glue surfaces here.
//
// `assert_cmd::Command::output` is blocking; we run the listener task
// on a tokio runtime and the binary on a `spawn_blocking` helper so
// they make progress concurrently.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use assert_cmd::Command;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tempfile::TempDir;
use tokio::net::{UnixListener, UnixStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use gcit::control::{serve, Handler, Response, MAX_FRAME_LEN};

const OK_EXIT: i32 = 0;
const TEMPFAIL: i32 = 75;

/// Canned handler: every method returns the configured JSON or error.
/// `status_response` is the only method exercised by `gcit status`,
/// but the trait requires every method, so the rest are stub-Ok.
struct CannedHandler {
    /// What the handler returns from `status(...)`. `Ok(value)`
    /// produces `Response::Ok { data: value }`; `Err(message)`
    /// produces `Response::Error { message }`.
    status_response: Result<serde_json::Value, String>,
}

impl Handler for CannedHandler {
    async fn trigger(&self, _flow: &str, _dry_run: bool) -> Result<serde_json::Value, String> {
        Ok(json!({}))
    }

    async fn status(&self, _flow: Option<&str>) -> Result<serde_json::Value, String> {
        self.status_response.clone()
    }

    async fn reload(&self) -> Result<serde_json::Value, String> {
        Ok(json!({}))
    }

    async fn version(&self) -> Result<serde_json::Value, String> {
        Ok(json!({}))
    }
}

/// Bind a fresh `UnixListener` at `<td>/control.sock` and spawn the
/// production `serve` accept loop against it with the canned handler.
/// Returns the socket path + a cancel token the caller fires after
/// asserting on the binary's output.
fn spawn_serve(td: &TempDir, handler: CannedHandler) -> (PathBuf, CancellationToken) {
    let socket = td.path().join("control.sock");
    let listener = UnixListener::bind(&socket).expect("bind unix listener");
    let cancel = CancellationToken::new();
    let cancel_for_serve = cancel.clone();
    tokio::spawn(async move {
        serve(listener, Arc::new(handler), cancel_for_serve).await;
    });
    (socket, cancel)
}

/// Run `gcit status [args]` against `socket` and return the
/// `(exit_code, stdout, stderr)` tuple. The binary call blocks; we
/// run it on `spawn_blocking` so the listener task can make progress.
async fn run_gcit_status(
    socket: &std::path::Path,
    extra_args: Vec<&'static str>,
) -> (i32, String, String) {
    let socket = socket.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut cmd = Command::cargo_bin("gcit").expect("gcit cargo bin");
        cmd.arg("--control-socket").arg(&socket).arg("status");
        for a in extra_args {
            cmd.arg(a);
        }
        let output = cmd.output().expect("run gcit status");
        let code = output.status.code().expect("gcit must exit normally");
        let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
        let stderr = String::from_utf8(output.stderr).expect("stderr utf-8");
        (code, stdout, stderr)
    })
    .await
    .expect("spawn_blocking join")
}

/// Drive `gcit status` against the canned handler and return its
/// stdout/stderr/exit. Wraps `spawn_serve` + `run_gcit_status` +
/// cancellation cleanup in a single helper so each test stays focused
/// on the JSON shape and the assertions.
async fn drive_status(
    handler: CannedHandler,
    extra_args: Vec<&'static str>,
) -> (i32, String, String) {
    let td = TempDir::new().expect("tempdir");
    let (socket, cancel) = spawn_serve(&td, handler);
    let (code, stdout, stderr) = run_gcit_status(&socket, extra_args).await;
    cancel.cancel();
    // Brief grace window so the serve task observes cancellation and
    // unbinds the socket before the tempdir Drop tries to remove it.
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(td);
    (code, stdout, stderr)
}

/// Status JSON is `{}` (empty object). Renderer must emit "(no flows)"
/// and exit 0. Pins the empty-map arm of `render_text`.
#[tokio::test]
async fn status_empty_map_renders_no_flows_and_exits_ok() {
    let handler = CannedHandler {
        status_response: Ok(json!({})),
    };
    let (code, stdout, _stderr) = drive_status(handler, vec![]).await;
    assert_eq!(code, OK_EXIT, "empty-map status must exit 0");
    assert!(
        stdout.contains("(no flows)"),
        "empty-map render must print '(no flows)'; got stdout: {stdout}",
    );
}

/// Status JSON is a single flow with every populated field. Renderer
/// must emit the flow name + state header, indented `last_sha`,
/// `last_poll_at`, `active_runs`, `notified_runs`, and the
/// `last_error[kind] at: message` line WITHOUT a `retry_at` indented
/// line because the error kind is not RateLimited (retry_at is JSON
/// null for every other error kind).
#[tokio::test]
async fn status_single_flow_full_payload_text_render() {
    let handler = CannedHandler {
        status_response: Ok(json!({
            "ci-flow": {
                "state": "running",
                "last_sha": "abcdef1234567890",
                "last_poll_at": "2026-05-01T12:00:00Z",
                "active_runs": 2,
                "notified_runs": 5,
                "last_error": {
                    "at": "2026-05-01T11:59:00Z",
                    "kind": "transport",
                    "message": "connection reset",
                    "retry_at": null,
                },
            }
        })),
    };
    let (code, stdout, _stderr) = drive_status(handler, vec![]).await;
    assert_eq!(
        code, OK_EXIT,
        "successful status must exit 0; stdout={stdout}"
    );
    assert!(
        stdout.contains("ci-flow: running"),
        "header line; got: {stdout}"
    );
    assert!(
        stdout.contains("last_sha: abcdef1234567890"),
        "last_sha line; got: {stdout}"
    );
    assert!(
        stdout.contains("last_poll_at: 2026-05-01T12:00:00Z"),
        "last_poll_at line; got: {stdout}"
    );
    assert!(
        stdout.contains("active_runs: 2"),
        "active_runs line; got: {stdout}"
    );
    assert!(
        stdout.contains("notified_runs: 5"),
        "notified_runs line; got: {stdout}"
    );
    assert!(
        stdout.contains("last_error[transport] 2026-05-01T11:59:00Z: connection reset"),
        "last_error line; got: {stdout}",
    );
    assert!(
        !stdout.contains("retry_at:"),
        "retry_at line must be hidden when retry_at is JSON null; got: {stdout}",
    );
}

/// Status JSON carries a `last_error` with a populated `retry_at`
/// string (the renderer's RateLimited path emits this in the daemon).
/// Renderer must emit the indented `retry_at:` line.
#[tokio::test]
async fn status_last_error_with_retry_at_emits_retry_line() {
    let handler = CannedHandler {
        status_response: Ok(json!({
            "ci-flow": {
                "state": "rate_limited",
                "last_error": {
                    "at": "2026-05-01T12:00:00Z",
                    "kind": "rate_limited",
                    "message": "GitHub API quota exhausted",
                    "retry_at": "2026-05-01T13:00:00Z",
                },
            }
        })),
    };
    let (code, stdout, _stderr) = drive_status(handler, vec![]).await;
    assert_eq!(
        code, OK_EXIT,
        "successful status must exit 0; stdout={stdout}"
    );
    assert!(
        stdout.contains("retry_at: 2026-05-01T13:00:00Z"),
        "retry_at line must be emitted when retry_at is a string; got: {stdout}",
    );
}

/// Synthetic daemon-key (parens-wrapped, e.g. `(reload)`) must render
/// with a `[daemon]` prefix on the header so an operator does not
/// mistake it for a real flow name. Pins the
/// `is_synthetic_daemon_key` branch in `render_text`.
#[tokio::test]
async fn status_synthetic_daemon_key_renders_with_daemon_prefix() {
    let handler = CannedHandler {
        status_response: Ok(json!({
            "(reload)": {
                "state": "config_error",
                "last_error": {
                    "at": "2026-05-01T12:00:00Z",
                    "kind": "config_invalid",
                    "message": "TOML parse error",
                    "retry_at": null,
                },
            }
        })),
    };
    let (code, stdout, _stderr) = drive_status(handler, vec![]).await;
    assert_eq!(
        code, OK_EXIT,
        "successful status must exit 0; stdout={stdout}"
    );
    assert!(
        stdout.contains("[daemon] (reload):"),
        "synthetic daemon key must render with [daemon] prefix; got: {stdout}",
    );
}

/// `active_runs: 0` and `notified_runs: 0` must NOT emit their lines —
/// the renderer suppresses zero counters to keep the output focused on
/// non-zero state. Pins both `if active > 0` / `if notified > 0`
/// suppression arms.
#[tokio::test]
async fn status_zero_run_counters_suppress_their_lines() {
    let handler = CannedHandler {
        status_response: Ok(json!({
            "ci-flow": {
                "state": "idle",
                "active_runs": 0,
                "notified_runs": 0,
            }
        })),
    };
    let (code, stdout, _stderr) = drive_status(handler, vec![]).await;
    assert_eq!(code, OK_EXIT, "idle status must exit 0; stdout={stdout}");
    assert!(
        stdout.contains("ci-flow: idle"),
        "header line; got: {stdout}"
    );
    assert!(
        !stdout.contains("active_runs:"),
        "active_runs=0 must suppress its line; got: {stdout}",
    );
    assert!(
        !stdout.contains("notified_runs:"),
        "notified_runs=0 must suppress its line; got: {stdout}",
    );
}

/// Per-flow object with `last_error: null` must NOT emit any
/// last_error line. The renderer matches explicitly on `Object(...)`
/// so a JSON null falls through. Pins the `if let Some(Object(err))`
/// negative arm.
#[tokio::test]
async fn status_null_last_error_suppresses_last_error_line() {
    let handler = CannedHandler {
        status_response: Ok(json!({
            "ci-flow": {
                "state": "running",
                "last_error": null,
            }
        })),
    };
    let (code, stdout, _stderr) = drive_status(handler, vec![]).await;
    assert_eq!(code, OK_EXIT);
    assert!(
        !stdout.contains("last_error"),
        "null last_error must suppress its line; got: {stdout}",
    );
}

/// `--format json` round-trips the daemon's payload verbatim through
/// `serde_json::to_string_pretty`. Pins the `Format::Json` arm of
/// `cli::status::run`.
#[tokio::test]
async fn status_json_format_emits_pretty_json() {
    let handler = CannedHandler {
        status_response: Ok(json!({
            "ci-flow": {
                "state": "running",
                "last_sha": "abc123",
            }
        })),
    };
    let (code, stdout, _stderr) = drive_status(handler, vec!["--format", "json"]).await;
    assert_eq!(code, OK_EXIT, "json format must exit 0; stdout={stdout}");
    // Pretty output indents nested keys; canonical output wraps the
    // top-level object on its own line.
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout must parse as JSON");
    assert_eq!(
        parsed["ci-flow"]["state"], "running",
        "json data round-trip; got: {stdout}"
    );
    assert_eq!(
        parsed["ci-flow"]["last_sha"], "abc123",
        "json data round-trip; got: {stdout}"
    );
    assert!(
        stdout.contains('\n'),
        "pretty-print must wrap on newlines; got compact: {stdout}",
    );
}

/// Daemon returns a non-Object payload (e.g. a JSON string). The
/// renderer's else-branch dumps the value verbatim via the Display
/// impl on `serde_json::Value`. Pins the fallback arm at the bottom
/// of `render_text`.
#[tokio::test]
async fn status_non_object_payload_falls_through_to_verbatim_dump() {
    let handler = CannedHandler {
        status_response: Ok(json!("just-a-string")),
    };
    let (code, stdout, _stderr) = drive_status(handler, vec![]).await;
    assert_eq!(
        code, OK_EXIT,
        "non-object payload must still exit 0; stdout={stdout}"
    );
    // Display on Value::String emits the JSON-quoted form.
    assert!(
        stdout.contains("\"just-a-string\""),
        "non-object payload must dump verbatim; got: {stdout}",
    );
}

/// Daemon returns `Response::Error { message }` (the handler returned
/// `Err(...)`). The CLI must print "gcit status: <message>" on stderr
/// and exit TEMPFAIL. Pins the `Ok(Response::Error { ... })` arm of
/// `cli::status::run`.
#[tokio::test]
async fn status_response_error_writes_to_stderr_and_exits_tempfail() {
    let handler = CannedHandler {
        status_response: Err("flow not found".to_string()),
    };
    let (code, _stdout, stderr) = drive_status(handler, vec!["nonexistent-flow"]).await;
    assert_eq!(
        code, TEMPFAIL,
        "Response::Error must exit TEMPFAIL; stderr={stderr}"
    );
    assert!(
        stderr.contains("gcit status: flow not found"),
        "Response::Error message must surface on stderr with 'gcit status:' prefix; got: {stderr}",
    );
}

/// Server accepts the connection, reads the frame, then closes the
/// stream without replying. Client::send maps the early EOF onto
/// io::Error::UnexpectedEof; the CLI's `Err(e)` arm in `cli::status::
/// run` writes "gcit status: transport error: <e>" to stderr and exits
/// TEMPFAIL. Pins the transport-error arm at L67-69 of
/// cli/status.rs.
#[tokio::test]
async fn status_transport_error_writes_to_stderr_and_exits_tempfail() {
    // Bypass the production `serve` helper because we need byte-level
    // control: read the request, drop the connection without writing.
    // `serve` never closes mid-response on its own (it always sends
    // a Response back), so we drive a custom listener.
    let td = TempDir::new().expect("tempdir");
    let socket = td.path().join("control.sock");
    let listener = UnixListener::bind(&socket).expect("bind unix listener");

    let server_task = tokio::spawn(async move {
        let (stream, _addr) = listener.accept().await.expect("accept");
        let codec = LengthDelimitedCodec::builder()
            .max_frame_length(MAX_FRAME_LEN)
            .length_field_type::<u32>()
            .big_endian()
            .new_codec();
        let mut framed: Framed<UnixStream, _> = Framed::new(stream, codec);
        // Read one frame, then drop the framed stream — UnixStream
        // closes on drop. The client's framed.next() yields None on
        // EOF; Client::send maps that to UnexpectedEof.
        let _ = framed.next().await;
        drop(framed);
    });

    let socket_for_cmd = socket.clone();
    let (code, _stdout, stderr) = tokio::task::spawn_blocking(move || {
        let output = Command::cargo_bin("gcit")
            .expect("gcit cargo bin")
            .arg("--control-socket")
            .arg(&socket_for_cmd)
            .arg("status")
            .output()
            .expect("run gcit status");
        let code = output.status.code().expect("gcit must exit normally");
        let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
        let stderr = String::from_utf8(output.stderr).expect("stderr utf-8");
        (code, stdout, stderr)
    })
    .await
    .expect("spawn_blocking join");
    let _ = server_task.await;

    assert_eq!(
        code, TEMPFAIL,
        "transport error must exit TEMPFAIL; stderr={stderr}"
    );
    assert!(
        stderr.contains("gcit status: transport error:"),
        "transport-error message must surface on stderr; got: {stderr}",
    );
}

/// Server replies with a Response whose id does NOT match the request
/// id. Client::send rejects the mismatch via io::Error::other("response
/// id mismatch ..."), surfacing through the CLI's transport-error arm.
/// Pins the same L67-69 arm via a different cause path so a regression
/// in either failure mode produces a test failure.
#[tokio::test]
async fn status_response_id_mismatch_routes_through_transport_error_arm() {
    let td = TempDir::new().expect("tempdir");
    let socket = td.path().join("control.sock");
    let listener = UnixListener::bind(&socket).expect("bind unix listener");

    let server_task = tokio::spawn(async move {
        let (stream, _addr) = listener.accept().await.expect("accept");
        let codec = LengthDelimitedCodec::builder()
            .max_frame_length(MAX_FRAME_LEN)
            .length_field_type::<u32>()
            .big_endian()
            .new_codec();
        let mut framed: Framed<UnixStream, _> = Framed::new(stream, codec);
        // Read the request frame so the protocol is satisfied, then
        // reply with a Response whose id is a different uuid. The
        // client's id-correlation check fires.
        let _ = framed.next().await;
        let resp = Response::Ok {
            id: Uuid::new_v4(),
            data: serde_json::json!({}),
        };
        let body = serde_json::to_vec(&resp).expect("serialize");
        let _ = framed.send(body.into()).await;
    });

    let socket_for_cmd = socket.clone();
    let (code, _stdout, stderr) = tokio::task::spawn_blocking(move || {
        let output = Command::cargo_bin("gcit")
            .expect("gcit cargo bin")
            .arg("--control-socket")
            .arg(&socket_for_cmd)
            .arg("status")
            .output()
            .expect("run gcit status");
        let code = output.status.code().expect("gcit must exit normally");
        let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
        let stderr = String::from_utf8(output.stderr).expect("stderr utf-8");
        (code, stdout, stderr)
    })
    .await
    .expect("spawn_blocking join");
    let _ = server_task.await;

    assert_eq!(
        code, TEMPFAIL,
        "id mismatch must exit TEMPFAIL; stderr={stderr}"
    );
    assert!(
        stderr.contains("gcit status: transport error:"),
        "id-mismatch must surface as transport error on stderr; got: {stderr}",
    );
    assert!(
        stderr.contains("response id mismatch"),
        "transport-error body must carry the canonical 'response id mismatch'; got: {stderr}",
    );
}

/// Per-flow value with `state` missing (or non-string) must render
/// `(unknown)` as the state label. Pins the
/// `unwrap_or("(unknown)")` arm in the state lookup.
#[tokio::test]
async fn status_missing_state_renders_unknown_placeholder() {
    let handler = CannedHandler {
        status_response: Ok(json!({
            "ci-flow": {
                // No "state" key at all.
                "last_sha": "abc123",
            }
        })),
    };
    let (code, stdout, _stderr) = drive_status(handler, vec![]).await;
    assert_eq!(code, OK_EXIT);
    assert!(
        stdout.contains("ci-flow: (unknown)"),
        "missing state must render '(unknown)'; got: {stdout}",
    );
}

/// `last_error.at` and `last_error.kind` missing must render `?`
/// placeholders. Pins both `unwrap_or("?")` arms in the last_error
/// renderer.
#[tokio::test]
async fn status_partial_last_error_renders_question_mark_placeholders() {
    let handler = CannedHandler {
        status_response: Ok(json!({
            "ci-flow": {
                "state": "running",
                // Object present but missing `at`, `kind`, `message`.
                "last_error": {},
            }
        })),
    };
    let (code, stdout, _stderr) = drive_status(handler, vec![]).await;
    assert_eq!(code, OK_EXIT);
    // The renderer emits "last_error[<kind>] <at>: <message>" — with
    // missing kind/at/message the placeholders fill in.
    assert!(
        stdout.contains("last_error[?] ?: "),
        "missing last_error fields must render with ? placeholders; got: {stdout}",
    );
}

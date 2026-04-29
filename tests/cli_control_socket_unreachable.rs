// Black-box CLI tests for `gcit reload`, `gcit status`, and
// `gcit trigger` when the daemon's control socket is not reachable.
//
// Each command resolves the socket path from `--control-socket` (or
// the XDG_RUNTIME_DIR / /run/gcit fallback) and tries to connect. When
// the path does not exist, `Client::connect` returns an I/O error and
// the CLI exits 75 (EX_TEMPFAIL) with a "is the daemon running?" hint
// on stderr. Pinning these paths covers every cli::* command's
// connection-error arm without spinning a real daemon — the seam is
// the `--control-socket` flag itself.
//
// The tempfile path is generated under `tempfile::tempdir()` so test
// isolation is clean across parallel test runs.

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;

const TEMPFAIL: i32 = 75;

/// Build a path inside a fresh tempdir that does NOT exist on disk.
/// `Client::connect` is guaranteed to fail with ENOENT.
fn nonexistent_socket_path(td: &TempDir) -> std::path::PathBuf {
    td.path().join("control.sock")
}

#[test]
fn reload_when_daemon_not_running_exits_tempfail() {
    let td = TempDir::new().unwrap();
    let sock = nonexistent_socket_path(&td);
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("--control-socket")
        .arg(&sock)
        .arg("reload")
        .assert()
        .code(TEMPFAIL)
        .stderr(predicate::str::contains("is the daemon running?"));
}

#[test]
fn status_when_daemon_not_running_exits_tempfail() {
    let td = TempDir::new().unwrap();
    let sock = nonexistent_socket_path(&td);
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("--control-socket")
        .arg(&sock)
        .arg("status")
        .assert()
        .code(TEMPFAIL)
        .stderr(predicate::str::contains("is the daemon running?"));
}

#[test]
fn status_with_flow_filter_when_daemon_not_running_exits_tempfail() {
    // Same path but with the optional FLOW positional. The CLI
    // accepts the arg via clap and resolves the socket BEFORE looking
    // at the flow filter — so unreachable-daemon takes precedence
    // over flow-existence checks.
    let td = TempDir::new().unwrap();
    let sock = nonexistent_socket_path(&td);
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("--control-socket")
        .arg(&sock)
        .arg("status")
        .arg("some-flow-name")
        .assert()
        .code(TEMPFAIL);
}

#[test]
fn status_json_format_when_daemon_not_running_exits_tempfail() {
    // --format json parses cleanly via clap ValueEnum; the
    // unreachable-daemon path then produces TEMPFAIL.
    let td = TempDir::new().unwrap();
    let sock = nonexistent_socket_path(&td);
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("--control-socket")
        .arg(&sock)
        .arg("status")
        .arg("--format")
        .arg("json")
        .assert()
        .code(TEMPFAIL);
}

#[test]
fn trigger_when_daemon_not_running_exits_tempfail() {
    let td = TempDir::new().unwrap();
    let sock = nonexistent_socket_path(&td);
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("--control-socket")
        .arg(&sock)
        .arg("trigger")
        .arg("some-flow")
        .assert()
        .code(TEMPFAIL)
        .stderr(predicate::str::contains("is the daemon running?"));
}

#[test]
fn trigger_dry_run_when_daemon_not_running_exits_tempfail() {
    // --dry-run is parsed by clap before the connect attempt; same
    // TEMPFAIL outcome when the socket does not exist.
    let td = TempDir::new().unwrap();
    let sock = nonexistent_socket_path(&td);
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("--control-socket")
        .arg(&sock)
        .arg("trigger")
        .arg("some-flow")
        .arg("--dry-run")
        .assert()
        .code(TEMPFAIL);
}

#[test]
fn reload_socket_path_is_a_directory_not_a_socket_exits_tempfail() {
    // tempdir() itself is a directory. Connecting to it as if it were
    // a socket fails at the AF_UNIX layer (ECONNREFUSED or similar).
    // Verifies the connection-error branch handles non-ENOENT errors
    // gracefully rather than panicking.
    let td = TempDir::new().unwrap();
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("--control-socket")
        .arg(td.path()) // a directory, not a socket
        .arg("reload")
        .assert()
        .code(TEMPFAIL);
}

// Control socket peer-credential authentication — exactly one #[test]
// per file (the test is env-mutating + uid-manipulating).
//
// Security contract:
// - SO_PEERCRED check after accept(); peer uid must == daemon's
//   effective uid.
// - Reload requests rate-limited 1/sec across all peers.
// - SocketMode=0600 in the unit gives a kernel-level rejection for
//   non-owners; SO_PEERCRED is defense-in-depth.
//
// Same-uid acceptance is testable inside cargo nextest. Cross-uid
// REJECTION is gated SYSTEMD_TESTS=1 because spawning a sub-process
// under a different uid requires either CAP_SETUID or systemd-run
// with User=. The cross-uid test lives in
// tests/journey/socket_activation.sh.

use std::os::unix::net::{UnixListener, UnixStream};
use tempfile::TempDir;

#[test]
#[ignore = "requires gcit::control::accept hook (not yet implemented)"]
fn same_uid_connection_accepted_then_dispatched() {
    let dir = TempDir::new().unwrap();
    let sock_path = dir.path().join("control.sock");
    let listener = UnixListener::bind(&sock_path).expect("bind tempdir control sock");

    // Implementer hook: spawn the daemon's accept loop on `listener`.
    //   let server = gcit::control::Server::new(listener, ...);
    //   tokio::spawn(server.run());
    //
    // Connect from this process (same uid as the listener):
    //   let client = UnixStream::connect(&sock_path).expect("connect must succeed");
    //
    // Send a Version request (the cheapest control-channel round-
    // trip). Receive the Ok response. Assert no rejection log was
    // emitted (peer uid matched).
    //
    // Cross-uid rejection assertion lives in the journey test; the
    // log-entry-includes-pid-and-uid path is covered by a
    // tracing-subscriber test layer in the journey script.

    let _ = UnixStream::connect(&sock_path);
    let _ = listener;
}

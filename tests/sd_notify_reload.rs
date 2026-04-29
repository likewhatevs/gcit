// SIGHUP-triggered reload notification: Reloading + MonotonicUsec then
// Ready. Exactly one #[test] per file (env-mutating). systemd >=253
// REQUIRES MonotonicUsec to follow Reloading or the reload fails and
// the service is terminated.
//
// This test asserts the EXACT byte sequence:
// "RELOADING=1\nMONOTONIC_USEC=N\n" where N is parseable as i128
// (matches the sd-notify crate's NotifyState::MonotonicUsec(i128)).

use std::os::unix::net::UnixDatagram;
use tempfile::TempDir;

#[test]
#[ignore = "requires SIGHUP -> Supervisor::reload wiring (not yet implemented)"]
fn reload_emits_reloading_then_monotonic_then_ready() {
    let dir = TempDir::new().unwrap();
    let sock_path = dir.path().join("notify.sock");
    let _ = std::fs::remove_file(&sock_path);
    let receiver = UnixDatagram::bind(&sock_path).unwrap();

    unsafe {
        std::env::set_var("NOTIFY_SOCKET", &sock_path);
    }

    // Trigger Supervisor::reload() (testing strategy: SIGHUP via
    // Supervisor::reload() + a serial actual-SIGHUP test).
    //
    // The bytes-on-wire assertion:
    // (a) first datagram: starts with b"RELOADING=1\n", contains
    //     "MONOTONIC_USEC=" followed by a parseable integer.
    // (b) second datagram: equals b"READY=1\n".
    //
    // Reloading and MonotonicUsec MAY arrive in a single datagram with
    // both lines, OR as two separate datagrams. sd-notify's notify()
    // takes a slice and emits all states in ONE datagram, so the gcit
    // call should be `notify(&[Reloading, MonotonicUsec(now)])` —
    // single datagram with both lines.

    let _ = receiver;
}

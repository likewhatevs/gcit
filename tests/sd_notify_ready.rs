// sd_notify Ready emission after daemon startup completes. Exactly
// one #[test] per file: env::set_var is unsafe and not thread-safe;
// nextest runs each integration test binary in a separate process by
// default, but tests within a SINGLE binary share the env. Do NOT
// add more #[test] functions here.
//
// Pattern (verified against the sd-notify crate's own test suite):
// 1. Bind a UnixDatagram to a tempdir path.
// 2. Set NOTIFY_SOCKET env var to that path BEFORE the daemon starts.
// 3. Drive the gcit run() codepath to completion of the startup phase.
// 4. recv from the socket; assert the byte sequence equals "READY=1\n" exactly.

use std::os::unix::net::UnixDatagram;
use tempfile::TempDir;

#[test]
#[ignore = "requires gcit::run startup hook (not yet implemented)"]
fn ready_emitted_after_startup() {
    let dir = TempDir::new().unwrap();
    let sock_path = dir.path().join("notify.sock");
    let _ = std::fs::remove_file(&sock_path);
    let receiver = UnixDatagram::bind(&sock_path).unwrap();

    // SAFETY: this binary holds exactly one #[test], so no other
    // thread is running when set_var fires. The sd-notify crate
    // marks its `_and_unset_env` variants as unsafe before the tokio
    // runtime spins up; this test is the moral equivalent.
    unsafe {
        std::env::set_var("NOTIFY_SOCKET", &sock_path);
    }

    // Drive gcit startup. Implementer fills in the hook:
    //   gcit::systemd::sd_notify::ready().expect("notify ready");
    //
    // For now the assertion is shaped against the eventual call.

    // let mut buf = [0u8; 1024];
    // receiver.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
    // let len = receiver.recv(&mut buf).expect("Ready datagram must arrive");
    // assert_eq!(&buf[..len], b"READY=1\n");
    let _ = receiver;
}

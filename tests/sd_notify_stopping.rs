// Shutdown sequence: emits sd_notify Stopping before drain. Exactly
// one #[test] per file (env-mutating).

use std::os::unix::net::UnixDatagram;
use tempfile::TempDir;

#[test]
#[ignore = "requires shutdown drain hook (not yet implemented)"]
fn stopping_emitted_on_shutdown() {
    let dir = TempDir::new().unwrap();
    let sock_path = dir.path().join("notify.sock");
    let _ = std::fs::remove_file(&sock_path);
    let receiver = UnixDatagram::bind(&sock_path).unwrap();

    unsafe {
        std::env::set_var("NOTIFY_SOCKET", &sock_path);
    }

    // Trigger gcit shutdown. Implementer hook:
    //   let token = gcit::flow::CancellationToken::new();
    //   gcit::run_with_token(token.clone()).await; // spawn
    //   token.cancel();
    //   // shutdown() must call sd_notify(&[Stopping]) before any
    //   // drain.
    //
    // assert_eq!(&buf[..len], b"STOPPING=1\n");

    let _ = receiver;
}

// Control-channel socket activation via
// sd_notify::listen_fds_with_names(). Exactly one #[test] per file
// (env-mutating).
//
// Unit-side configuration:
//   gcit.socket: ListenStream=%t/gcit/control.sock,
//                SocketMode=0600,
//                FileDescriptorName=control.
//
// sd_notify::listen_fds behavior (verified against the sd-notify
// crate source):
// - returns Ok(empty) when LISTEN_PID is unset
// - returns Ok(empty) when LISTEN_PID != process::id()
// - sets O_CLOEXEC on each inherited fd
//
// fd inheritance across exec is genuinely hard to test inside cargo
// nextest: the fd at the SD_LISTEN_FDS_START slot must be a real
// inherited fd, not just a number. The realistic in-test assertion
// is the NEGATIVE case: with LISTEN_FDS unset, listen_fds() returns
// empty and the daemon falls back to UnixListener::bind() on its own
// socket path. Real LISTEN_FDS inheritance is gated SYSTEMD_TESTS=1
// in journey/socket_activation.sh.

use std::os::unix::fs::FileTypeExt;
use tempfile::TempDir;

use gcit::systemd::accept_control_socket;

/// LISTEN_PID unset means listen_fds() returns Ok(empty iterator).
/// `accept_control_socket(path)` must then fall back to binding the
/// supplied path as a fresh unix-domain socket. Pin both the negative
/// case (sd_notify::listen_fds is empty under our env) and the
/// positive case (the path materialises as a socket).
///
/// `env::remove_var` is unsafe and not thread-safe; this binary holds
/// exactly one `#[test]` so no other thread races us.
#[tokio::test(flavor = "current_thread")]
async fn no_listen_fds_falls_back_to_bind() {
    let dir = TempDir::new().unwrap();
    let socket_path = dir.path().join("control.sock");

    unsafe {
        std::env::remove_var("LISTEN_PID");
        std::env::remove_var("LISTEN_FDS");
        std::env::remove_var("LISTEN_FDNAMES");
    }

    // Negative case: confirm sd-notify reports no inherited fds in
    // this environment, proving the test is exercising the fallback
    // arm rather than masking a misconfigured listen_fds return.
    let fds = sd_notify::listen_fds().expect("listen_fds must not error when env unset");
    assert_eq!(fds.len(), 0, "fallback path requires empty listen_fds");

    // Positive case: accept_control_socket reads listen-fds-with-names
    // internally, sees the empty iterator, and falls back to
    // UnixListener::bind on the supplied path.
    let listener = accept_control_socket(&socket_path).expect("fallback bind must succeed");
    let meta = std::fs::metadata(&socket_path).expect("stat the bound path");
    assert!(
        meta.file_type().is_socket(),
        "fallback bind must produce a unix-domain socket; got file_type={:?}",
        meta.file_type(),
    );
    drop(listener);
}

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

use std::os::unix::net::UnixDatagram;
use tempfile::TempDir;

#[test]
#[ignore = "requires gcit::systemd::accept_control_socket (not yet implemented)"]
fn no_listen_fds_falls_back_to_bind() {
    let _dir = TempDir::new().unwrap();

    // LISTEN_PID unset means listen_fds() returns Ok(empty iterator).
    // The daemon must then fall back to UnixListener::bind on a known
    // path under $RUNTIME_DIRECTORY/gcit/control.sock.
    //
    // env::remove_var is unsafe and not thread-safe; this binary holds
    // exactly one #[test] so no other thread runs.
    unsafe {
        std::env::remove_var("LISTEN_PID");
        std::env::remove_var("LISTEN_FDS");
        std::env::remove_var("LISTEN_FDNAMES");
    }

    // TODO:
    //   let listener = gcit::systemd::accept_control_socket(tmp_dir.path()).unwrap();
    //   // Asserts the daemon bound a UnixListener at the fallback path
    //   // and listen_fds_with_names() returned empty.
    //
    // Negative case proven by absence: if listen_fds_with_names() returned
    // a non-empty iterator while LISTEN_PID was unset, sd-notify is broken.

    // sanity: confirm sd-notify behaves as documented in our environment.
    let fds = sd_notify::listen_fds().expect("listen_fds must not error when env unset");
    assert_eq!(fds.len(), 0, "fallback path requires empty listen_fds");
    let _ = UnixDatagram::unbound(); // smoke-link sanity
}

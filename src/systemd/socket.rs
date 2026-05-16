// Control-socket acceptance: prefer a systemd-inherited listen-fd
// named `control`; fall back to binding `default_path` directly.
//
// Two entry points:
//
//   * `accept_control_socket(default_path)` reads
//     `sd_notify::listen_fds_with_names()` internally and is the
//     ergonomic surface for the binary entry + integration tests
//     under tests/socket_activation.rs.
//
//   * `accept_control_socket_from_fds(listen_fds, default_path)`
//     takes the parsed fd list as an argument. Used by the
//     supervisor's `flow::supervisor::run::run_with_factories`,
//     which already threads `DaemonParams::listen_fds` through from
//     `bin/gcit.rs::main` so the test-injection path doesn't have
//     to mutate `LISTEN_PID` / `LISTEN_FDS` env vars under
//     `serial_test`.
//
// Fallback semantics: when no `control` listen-fd is supplied the
// function probes the existing socket file (if any) with a one-shot
// `UnixStream::connect`. A connect that succeeds means a live daemon
// is already listening — refuse to start. A connect that fails leaves
// a stale socket file from a crashed prior run; remove it and bind a
// fresh socket on top.

use std::os::unix::io::{FromRawFd, RawFd};
use std::path::Path;

use tokio::net::UnixListener;

/// Errors from control-socket acceptance. Distinct from the
/// supervisor's `DaemonError` so test callers can match without
/// pulling the whole supervisor crate into scope.
#[derive(Debug, thiserror::Error)]
pub enum AcceptError {
    /// `from_raw_fd` -> `set_nonblocking` -> `from_std` failed for an
    /// inherited fd. Carries the underlying message.
    #[error("control listener (inherited fd): {0}")]
    InheritedFd(String),

    /// Parent directory creation, stale-file probe, or `UnixListener::bind`
    /// failed for the fallback path.
    #[error("control listener (fallback bind): {0}")]
    FallbackBind(String),

    /// The fallback probe found a live peer at `default_path`.
    /// A second foreground gcit pointing at the same --control-socket
    /// path would race the live one; refuse to start.
    #[error("{path}: another gcit daemon is already listening; refusing to start")]
    AlreadyListening { path: std::path::PathBuf },
}

/// Read systemd listen-fds-with-names. Returns an empty vec when
/// `LISTEN_PID` is unset (the test default) or any read error fires —
/// the caller falls back to binding the supplied path.
fn read_listen_fds_with_names() -> Vec<(RawFd, String)> {
    sd_notify::listen_fds_with_names()
        .map(|iter| iter.collect())
        .unwrap_or_default()
}

/// Ergonomic accept: read listen-fds from the environment and route
/// through `accept_control_socket_from_fds`. Used by integration
/// tests and any caller that doesn't already have the fd list in
/// hand.
pub fn accept_control_socket(default_path: &Path) -> Result<UnixListener, AcceptError> {
    let listen_fds = read_listen_fds_with_names();
    accept_control_socket_from_fds(&listen_fds, default_path)
}

/// Lower-level accept: caller supplies the parsed fd list. The
/// supervisor uses this form because `bin/gcit.rs::main` already
/// reads `listen_fds_with_names()` once at boot.
pub fn accept_control_socket_from_fds(
    listen_fds: &[(RawFd, String)],
    default_path: &Path,
) -> Result<UnixListener, AcceptError> {
    if let Some((fd, _name)) = listen_fds.iter().find(|(_, name)| name == "control") {
        // SAFETY: systemd hands us an O_CLOEXEC fd that we now own;
        // from_raw_fd takes ownership.
        let std_listener = unsafe { std::os::unix::net::UnixListener::from_raw_fd(*fd) };
        std_listener
            .set_nonblocking(true)
            .map_err(|e| AcceptError::InheritedFd(e.to_string()))?;
        return UnixListener::from_std(std_listener)
            .map_err(|e| AcceptError::InheritedFd(e.to_string()));
    }
    // Fallback: bind a fresh socket. Used in foreground mode (gcit
    // run outside systemd) and in tests.
    if let Some(parent) = default_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| AcceptError::FallbackBind(e.to_string()))?;
    }
    if default_path.exists() {
        match std::os::unix::net::UnixStream::connect(default_path) {
            Ok(_) => {
                return Err(AcceptError::AlreadyListening {
                    path: default_path.to_path_buf(),
                });
            }
            Err(_) => {
                let _ = std::fs::remove_file(default_path);
            }
        }
    }
    UnixListener::bind(default_path).map_err(|e| AcceptError::FallbackBind(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn falls_back_to_bind_when_no_control_fd_in_listen_fds() {
        // The fd list is empty (systemd not in play). The function
        // creates a fresh unix-domain socket at `default_path`.
        use std::os::unix::fs::FileTypeExt;
        let tmp = tempfile::tempdir().expect("tempdir");
        let socket_path = tmp.path().join("control.sock");
        let listener = accept_control_socket_from_fds(&[], &socket_path)
            .expect("empty listen_fds must fall back to bind");
        let meta = std::fs::metadata(&socket_path).expect("stat the bound path");
        assert!(
            meta.file_type().is_socket(),
            "fallback bind must produce a unix-domain socket; got file_type={:?}",
            meta.file_type(),
        );
        drop(listener);
    }

    #[tokio::test]
    async fn refuses_when_live_peer_is_already_listening() {
        // A real listener at the target path makes the connect-probe
        // succeed; accept_control_socket_from_fds returns
        // AlreadyListening rather than racing the live one.
        let tmp = tempfile::tempdir().expect("tempdir");
        let socket_path = tmp.path().join("control.sock");
        let _live = std::os::unix::net::UnixListener::bind(&socket_path)
            .expect("setup: bind real listener at the path");
        let err = accept_control_socket_from_fds(&[], &socket_path)
            .expect_err("must reject when another daemon is listening");
        assert!(
            matches!(err, AcceptError::AlreadyListening { ref path } if path == &socket_path),
            "expected AlreadyListening for {socket_path:?}; got: {err:?}",
        );
        // Display message must name the path so journalctl readers
        // can resolve the conflict.
        let msg = format!("{err}");
        assert!(
            msg.contains(socket_path.to_string_lossy().as_ref()),
            "AlreadyListening Display must name the colliding path; got: {msg}",
        );
    }

    #[tokio::test]
    async fn removes_stale_socket_file_left_by_crashed_run() {
        // A regular (non-socket) file at the path stands in for a
        // crashed-daemon leftover. Probe fails connect, function
        // removes the file and binds a fresh socket on top.
        use std::os::unix::fs::FileTypeExt;
        let tmp = tempfile::tempdir().expect("tempdir");
        let socket_path = tmp.path().join("control.sock");
        std::fs::write(&socket_path, b"stale data from a crashed run").expect("write");
        let listener = accept_control_socket_from_fds(&[], &socket_path)
            .expect("stale file must be replaced by a fresh socket");
        let meta = std::fs::metadata(&socket_path).expect("stat after rebind");
        assert!(
            meta.file_type().is_socket(),
            "rebind must replace stale file with a unix-domain socket; got {:?}",
            meta.file_type(),
        );
        drop(listener);
    }

    #[tokio::test]
    async fn creates_missing_parent_directory_chain() {
        // $XDG_RUNTIME_DIR/gcit/control.sock on a tmpfs that dropped
        // the per-process subdirectory between boots: the parent must
        // be created recursively before bind.
        let tmp = tempfile::tempdir().expect("tempdir");
        let socket_path = tmp.path().join("nested/a/b/control.sock");
        let parent = socket_path.parent().expect("has parent");
        assert!(
            !parent.exists(),
            "precondition: nested parent must be absent"
        );
        let listener = accept_control_socket_from_fds(&[], &socket_path)
            .expect("missing parent must be created");
        assert!(parent.exists(), "fallback bind must materialise the parent");
        drop(listener);
    }
}

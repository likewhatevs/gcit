// Fake-daemon harness for CLI integration tests.
//
// `gcit reload` and `gcit trigger` open a one-shot control-socket
// client and treat anything other than `Response::Ok` as
// `EX_TEMPFAIL=75`. To cover the response-driven branches without
// booting the real supervisor, we stand up a minimal Unix-domain
// listener that decodes one length-delimited frame per accepted
// connection, parses it as a `Request`, and writes a caller-supplied
// `Response` back. The seam is identical to the production
// `src/control/server.rs` wire format (4-byte big-endian u32 prefix +
// serde_json body, 64 KiB cap from `MAX_FRAME_LEN`).
//
// The daemon is intentionally one-shot per connection — `Client`
// itself sends a single request and closes. Multi-frame protocols are
// out of scope for these tests.

use std::path::PathBuf;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tempfile::TempDir;
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinHandle;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use gcit::control::{build_codec, Request, Response};

/// Scripted response factory: given an incoming Request, returns the
/// Response the fake daemon should write back. Boxed and `Arc`-shared
/// so connection-per-task closures can pick up the same script.
pub type ResponseScript = Arc<dyn Fn(Request) -> Response + Send + Sync + 'static>;

/// Live fake daemon. `socket_path` is the absolute path the CLI must
/// be pointed at via `--control-socket`. The accept loop runs on the
/// supplied runtime until `shutdown` is called.
///
/// The `_tempdir` field keeps the parent directory alive — when the
/// FakeDaemon is dropped the tempdir is cleaned up and the socket
/// file vanishes with it. Tests do not need to clean up explicitly.
///
/// No manual `Drop` impl: the runtime that owns the accept task is
/// per-test and gets dropped at the end of each test, which aborts
/// the task. `shutdown()` is the deterministic way to ensure every
/// accepted connection has drained before the test exits.
pub struct FakeDaemon {
    pub socket_path: PathBuf,
    cancel: CancellationToken,
    accept_handle: JoinHandle<()>,
    _tempdir: TempDir,
}

impl FakeDaemon {
    /// Spawn a fake daemon backed by `script`. Returns once the
    /// listener is bound — the caller can immediately point the CLI
    /// at `socket_path` knowing the accept loop is live.
    ///
    /// MUST be called from inside a tokio runtime context (every
    /// caller uses `Runtime::block_on` or `#[tokio::test]` so this
    /// invariant is structurally enforced by the call site).
    pub async fn spawn<F>(script: F) -> Self
    where
        F: Fn(Request) -> Response + Send + Sync + 'static,
    {
        let tempdir = tempfile::tempdir().expect("fake-daemon tempdir");
        let socket_path = tempdir.path().join("fake-control.sock");
        let listener = UnixListener::bind(&socket_path).expect("fake-daemon bind");
        let cancel = CancellationToken::new();
        let script: ResponseScript = Arc::new(script);

        let accept_handle = tokio::spawn({
            let cancel = cancel.clone();
            async move {
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        accept = listener.accept() => {
                            match accept {
                                Ok((stream, _)) => {
                                    let script = Arc::clone(&script);
                                    let cancel = cancel.clone();
                                    tokio::spawn(async move {
                                        handle_connection(stream, script, cancel).await;
                                    });
                                }
                                Err(_) => return,
                            }
                        }
                    }
                }
            }
        });

        Self {
            socket_path,
            cancel,
            accept_handle,
            _tempdir: tempdir,
        }
    }

    /// Stop accepting new connections and wait for the loop task to
    /// finish. Idempotent — repeated calls are safe but redundant.
    pub async fn shutdown(self) {
        self.cancel.cancel();
        let _ = self.accept_handle.await;
    }
}

/// Per-connection driver: read exactly one frame, decode as
/// `Request`, hand off to `script`, write the resulting `Response`,
/// and close. A cancelled token between accept and read short-circuits
/// the read so a `FakeDaemon::shutdown()` mid-test does not wedge on
/// a peer that opened the socket but never wrote.
async fn handle_connection(stream: UnixStream, script: ResponseScript, cancel: CancellationToken) {
    let mut framed = Framed::new(stream, build_codec());

    let frame = tokio::select! {
        _ = cancel.cancelled() => return,
        f = framed.next() => f,
    };
    let bytes = match frame {
        Some(Ok(b)) => b,
        // Peer closed before sending, or decode error. Either way,
        // there's nothing to respond to — drop the connection.
        _ => return,
    };
    let request: Request = match serde_json::from_slice(&bytes) {
        Ok(r) => r,
        Err(_) => return,
    };
    let response = script(request);
    let body = match serde_json::to_vec(&response) {
        Ok(b) => b,
        Err(_) => return,
    };
    let _ = framed.send(body.into()).await;
}

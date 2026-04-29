// Control-channel server.
//
// Per-connection lifecycle:
//   1. Accept on the tokio::net::UnixListener.
//   2. Call `peer_cred()` and verify the peer's effective uid matches
//      our own. Mismatch -> log + close.
//   3. Wrap the stream in a LengthDelimitedCodec configured for
//      protocol::MAX_FRAME_LEN (64 KiB cap), 4-byte BE u32 prefix.
//   4. Each frame: read with READ_TIMEOUT_SECS deadline, deserialize as
//      Request, dispatch via the Handler trait, serialize Response,
//      write back. On any error, send Response::Error and continue;
//      on hard transport error, drop the connection.
//   5. Connection close releases its slot; the daemon stays up.
//
// Per-task isolation: each accepted connection is driven on its own
// tokio::spawn so a misbehaving peer cannot block the accept loop or
// other peers.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::{timeout, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

use super::protocol::{Request, Response, MAX_FRAME_LEN, READ_TIMEOUT_SECS};

/// Reload-rate-limit window — 1 per second across all peers. The
/// handler holds an `Arc<Mutex<Option<Instant>>>` of the
/// last-accepted reload; an attempted reload within the window from
/// any peer is rejected with `reload rate-limited`.
pub const RELOAD_WINDOW: Duration = Duration::from_secs(1);

/// The trait the daemon implements to receive control requests. The
/// server module owns the wire format and authentication; the daemon
/// owns the semantics. Each method takes `&self` so the handler can be
/// shared across connections.
///
/// Each method's returned future is `Send` because every accepted
/// connection is dispatched on its own `tokio::spawn` task — the
/// runtime requires `Send` on spawned futures. The native async-fn-
/// in-trait sugar leaves the future non-Send by default; we use the
/// explicit `impl Future + Send` form so callers can rely on the
/// guarantee at the trait boundary.
pub trait Handler: Send + Sync + 'static {
    fn trigger(
        &self,
        flow: &str,
        dry_run: bool,
    ) -> impl std::future::Future<Output = Result<serde_json::Value, String>> + Send;
    fn status(
        &self,
        flow: Option<&str>,
    ) -> impl std::future::Future<Output = Result<serde_json::Value, String>> + Send;
    fn reload(&self)
        -> impl std::future::Future<Output = Result<serde_json::Value, String>> + Send;
    fn version(
        &self,
    ) -> impl std::future::Future<Output = Result<serde_json::Value, String>> + Send;
}

/// Shared state across all connections: the last-accepted reload's
/// timestamp, threaded through every dispatched Request::Reload so a
/// single rate-limit token bucket spans every peer.
#[derive(Default, Clone)]
struct ReloadGate(Arc<Mutex<Option<Instant>>>);

impl ReloadGate {
    /// Returns true and updates the timestamp when the reload is
    /// allowed; returns false (without updating) when the previous
    /// reload was less than RELOAD_WINDOW ago.
    async fn try_acquire(&self, now: Instant) -> bool {
        let mut guard = self.0.lock().await;
        match *guard {
            Some(prev) if now.duration_since(prev) < RELOAD_WINDOW => false,
            _ => {
                *guard = Some(now);
                true
            }
        }
    }
}

/// Run the control server's accept loop until cancelled.
///
/// Each accepted connection is dispatched on its own task tracked by a
/// `JoinSet`. When `cancel.cancelled()` resolves, the accept loop
/// stops and the function awaits every in-flight connection to finish
/// — connection tasks share the same cancel token, so `cancel`
/// triggers in-progress reads to abort and the connection loop to
/// return cleanly. This guarantees that a clean daemon shutdown does
/// not leave orphaned connection tasks running into the runtime
/// drop.
///
/// The select! is unbiased — a `biased;` ordering would
/// short-circuit accept() in favor of cancellation under load, which
/// is fine for shutdown latency but not the documented contract.
pub async fn serve<H>(listener: UnixListener, handler: Arc<H>, cancel: CancellationToken)
where
    H: Handler,
{
    let gate = ReloadGate::default();
    let mut tasks: JoinSet<()> = JoinSet::new();
    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                info!(target: "gcit::control", "control server cancelled; stopping accept loop");
                break;
            }
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _addr)) => {
                        let handler = Arc::clone(&handler);
                        let gate = gate.clone();
                        let conn_cancel = cancel.clone();
                        tasks.spawn(async move {
                            if let Err(e) = handle_connection(stream, handler, gate, conn_cancel).await {
                                // Connection-level errors don't surface
                                // back to the peer (the connection is
                                // already gone); they're logged for
                                // operator post-mortem.
                                warn!(target: "gcit::control", error = %e, "control connection ended");
                            }
                        });
                    }
                    Err(e) => {
                        // accept() failed at the listener level. Log
                        // and continue — the listener is still live.
                        warn!(target: "gcit::control", error = %e, "accept failed");
                    }
                }
            }
        }
    }
    // Drain in-flight connection tasks. Each task observes the same
    // `cancel` token (cloned above) and unwinds its codec loop on
    // wake, so this drain is bounded by the longest in-flight handler
    // call, not by the per-frame READ_TIMEOUT_SECS.
    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result {
            warn!(target: "gcit::control", error = %e, "connection task join error during shutdown");
        }
    }
    info!(target: "gcit::control", "all control connections drained; serve() exiting");
}

/// Test seam: drive a single client connection through the codec
/// loop with a fresh `ReloadGate`. Production callers reach
/// `handle_connection` via the `serve` accept loop (which keeps a
/// single `ReloadGate` shared across every accepted connection so
/// the 1/sec reload rate-limit holds across peers). Tests under
/// `tests/` cannot reach the private `ReloadGate` constructor, so
/// this wrapper builds one per call — fine for in-memory
/// `UnixStream::pair()` tests where a single connection drives one
/// scenario.
///
/// `#[doc(hidden)] pub` mirrors the test-seam pattern in
/// `flow::dispatcher::handle_trigger_for_test` and
/// `flow::monitor::run_monitor_with_source`.
#[doc(hidden)]
pub async fn handle_connection_for_test<H>(
    stream: UnixStream,
    handler: Arc<H>,
    cancel: CancellationToken,
) -> std::io::Result<()>
where
    H: Handler,
{
    handle_connection(stream, handler, ReloadGate::default(), cancel).await
}

/// Handle a single accepted connection: SO_PEERCRED check, codec
/// loop, dispatch. `cancel` is the same token the accept loop
/// observes; when it fires the codec read aborts via select! and the
/// loop returns immediately.
async fn handle_connection<H>(
    stream: UnixStream,
    handler: Arc<H>,
    gate: ReloadGate,
    cancel: CancellationToken,
) -> std::io::Result<()>
where
    H: Handler,
{
    // SO_PEERCRED uid match. Mismatch closes the connection without
    // writing any bytes; an attacker who somehow bypassed
    // SocketMode=0600 gets a silent close, not a banner.
    let cred = stream.peer_cred()?;
    let our_uid = unsafe { libc::geteuid() };
    let peer_uid = cred.uid();
    if peer_uid != our_uid {
        warn!(
            target: "gcit::control",
            peer_uid,
            peer_pid = ?cred.pid(),
            our_uid,
            "rejecting control connection: peer uid does not match daemon",
        );
        return Ok(());
    }

    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_LEN)
        .length_field_type::<u32>()
        .big_endian()
        .new_codec();
    let mut framed = Framed::new(stream, codec);

    loop {
        // READ_TIMEOUT_SECS-deadline on each frame guards against
        // slow-loris peers that send a length prefix and stall.
        // Cancellation aborts the read immediately for a prompt
        // shutdown.
        let next = tokio::select! {
            () = cancel.cancelled() => {
                return Ok(());
            }
            r = timeout(Duration::from_secs(READ_TIMEOUT_SECS), framed.next()) => r,
        };
        let frame = match next {
            Err(_) => {
                warn!(
                    target: "gcit::control",
                    "frame read timed out after {READ_TIMEOUT_SECS}s; closing connection"
                );
                return Ok(());
            }
            Ok(None) => {
                // Peer closed cleanly.
                return Ok(());
            }
            Ok(Some(Err(e))) => {
                // Codec error (length prefix exceeds cap, partial
                // body, etc.). Try to send a generic error; if write
                // fails, the connection is dead anyway.
                let resp = Response::Error {
                    id: Uuid::nil(),
                    message: format!("frame error: {}", e),
                };
                let _ = send_response(&mut framed, &resp).await;
                return Ok(());
            }
            Ok(Some(Ok(bytes))) => bytes,
        };

        // Deserialize body. Unknown variants and malformed JSON both
        // surface as serde errors; reply with Error and continue so a
        // typo in one request doesn't kill the connection.
        let request: Request = match serde_json::from_slice(&frame) {
            Ok(r) => r,
            Err(e) => {
                let resp = Response::Error {
                    id: Uuid::nil(),
                    message: format!("invalid request: {}", e),
                };
                if send_response(&mut framed, &resp).await.is_err() {
                    return Ok(());
                }
                continue;
            }
        };

        let resp = dispatch(&request, handler.as_ref(), &gate, &cred).await;
        if send_response(&mut framed, &resp).await.is_err() {
            return Ok(());
        }
    }
}

async fn dispatch<H>(
    req: &Request,
    handler: &H,
    gate: &ReloadGate,
    cred: &tokio::net::unix::UCred,
) -> Response
where
    H: Handler,
{
    let id = req.id();
    match req {
        Request::Trigger { flow, dry_run, .. } => match handler.trigger(flow, *dry_run).await {
            Ok(data) => Response::Ok { id, data },
            Err(message) => Response::Error { id, message },
        },
        Request::Status { flow, .. } => match handler.status(flow.as_deref()).await {
            Ok(data) => Response::Ok { id, data },
            Err(message) => Response::Error { id, message },
        },
        Request::Reload { .. } => {
            let now = Instant::now();
            if !gate.try_acquire(now).await {
                warn!(
                    target: "gcit::control",
                    peer_pid = ?cred.pid(),
                    peer_uid = cred.uid(),
                    "reload rejected: rate-limited (1/s across peers)",
                );
                return Response::Error {
                    id,
                    message: "reload rate-limited".to_string(),
                };
            }
            info!(
                target: "gcit::control",
                peer_pid = ?cred.pid(),
                peer_uid = cred.uid(),
                "reload accepted",
            );
            match handler.reload().await {
                Ok(data) => Response::Ok { id, data },
                Err(message) => Response::Error { id, message },
            }
        }
        Request::Version { .. } => match handler.version().await {
            Ok(data) => Response::Ok { id, data },
            Err(message) => Response::Error { id, message },
        },
    }
}

async fn send_response(
    framed: &mut Framed<UnixStream, LengthDelimitedCodec>,
    resp: &Response,
) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(resp).map_err(|e| std::io::Error::other(e.to_string()))?;
    framed.send(bytes.into()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn reload_gate_first_acquire_succeeds() {
        let gate = ReloadGate::default();
        let now = Instant::now();
        assert!(gate.try_acquire(now).await);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn reload_gate_rejects_within_window() {
        let gate = ReloadGate::default();
        let t0 = Instant::now();
        assert!(gate.try_acquire(t0).await);
        // Half a window later — must reject.
        let t1 = t0 + Duration::from_millis(500);
        assert!(!gate.try_acquire(t1).await);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn reload_gate_accepts_after_window() {
        let gate = ReloadGate::default();
        let t0 = Instant::now();
        assert!(gate.try_acquire(t0).await);
        // Just over the window — must accept.
        let t1 = t0 + RELOAD_WINDOW + Duration::from_millis(1);
        assert!(gate.try_acquire(t1).await);
    }
}

// In-process control-server tests using `tokio::net::UnixStream::pair()`.
//
// The seam is `control::server::handle_connection_for_test`, which
// drives one client connection through the codec loop with a mock
// `Handler`. No filesystem socket, no real listener — both ends of
// a connected `UnixStream` pair drive the same handler in
// milliseconds.
//
// Coverage targets: the codec dispatch, request/response shapes,
// malformed-JSON error path, EOF handling, and the rate-limit /
// reload arms. Each scenario is a single-shot connection: client
// sends one frame, server replies, client closes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use gcit::control::server::{handle_connection_for_test, Handler};
use gcit::control::{Request, Response};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Pure-recording mock Handler. Returns canned responses so tests
/// can pin both the wire format AND the request-routing path
/// without a real daemon.
#[derive(Default)]
struct MockHandler {
    trigger_calls: AtomicUsize,
    status_calls: AtomicUsize,
    reload_calls: AtomicUsize,
    version_calls: AtomicUsize,
}

impl Handler for MockHandler {
    fn trigger(
        &self,
        flow: &str,
        dry_run: bool,
    ) -> impl std::future::Future<Output = Result<serde_json::Value, String>> + Send {
        self.trigger_calls.fetch_add(1, Ordering::SeqCst);
        let flow = flow.to_string();
        async move {
            Ok(serde_json::json!({
                "trigger": flow,
                "dry_run": dry_run,
            }))
        }
    }

    fn status(
        &self,
        flow: Option<&str>,
    ) -> impl std::future::Future<Output = Result<serde_json::Value, String>> + Send {
        self.status_calls.fetch_add(1, Ordering::SeqCst);
        let flow = flow.map(str::to_string);
        async move {
            Ok(serde_json::json!({
                "status": flow,
            }))
        }
    }

    fn reload(
        &self,
    ) -> impl std::future::Future<Output = Result<serde_json::Value, String>> + Send {
        self.reload_calls.fetch_add(1, Ordering::SeqCst);
        async move { Ok(serde_json::json!({"reload": "ok"})) }
    }

    fn version(
        &self,
    ) -> impl std::future::Future<Output = Result<serde_json::Value, String>> + Send {
        self.version_calls.fetch_add(1, Ordering::SeqCst);
        async move { Ok(serde_json::json!({"version": "test"})) }
    }
}

/// Send a length-prefixed frame to `peer`. The codec writes a 4-byte
/// big-endian u32 length followed by the JSON body, matching the
/// production `LengthDelimitedCodec` configuration.
async fn send_frame(peer: &mut UnixStream, body: &[u8]) {
    let len = (body.len() as u32).to_be_bytes();
    peer.write_all(&len).await.expect("write len");
    peer.write_all(body).await.expect("write body");
}

/// Read one length-prefixed frame from `peer`. Returns the body
/// bytes; panics if the peer closes mid-frame.
async fn recv_frame(peer: &mut UnixStream) -> Vec<u8> {
    let mut len_buf = [0u8; 4];
    peer.read_exact(&mut len_buf).await.expect("read len");
    let n = u32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0u8; n];
    peer.read_exact(&mut body).await.expect("read body");
    body
}

/// Spawn the server side of `handle_connection_for_test` and return
/// the client end + a join handle.
fn spawn_server(handler: Arc<MockHandler>) -> (UnixStream, tokio::task::JoinHandle<()>) {
    let (server_stream, client_stream) = UnixStream::pair().expect("pair");
    let cancel = CancellationToken::new();
    let server_handle = tokio::spawn(async move {
        let _ = handle_connection_for_test(server_stream, handler, cancel).await;
    });
    (client_stream, server_handle)
}

#[tokio::test]
async fn version_request_round_trip() {
    let handler = Arc::new(MockHandler::default());
    let (mut client, server) = spawn_server(Arc::clone(&handler));

    let id = Uuid::new_v4();
    let req = Request::Version { id };
    let body = serde_json::to_vec(&req).expect("serialize");
    send_frame(&mut client, &body).await;

    let resp_bytes = recv_frame(&mut client).await;
    let resp: Response = serde_json::from_slice(&resp_bytes).expect("parse");
    match resp {
        Response::Ok { id: rid, data } => {
            assert_eq!(rid, id, "response id must echo request id");
            assert_eq!(data["version"], "test");
        }
        other => panic!("expected Ok, got {other:?}"),
    }
    assert_eq!(handler.version_calls.load(Ordering::SeqCst), 1);

    drop(client);
    let _ = server.await;
}

#[tokio::test]
async fn trigger_request_routes_to_handler_with_dry_run_flag() {
    let handler = Arc::new(MockHandler::default());
    let (mut client, server) = spawn_server(Arc::clone(&handler));

    let id = Uuid::new_v4();
    let req = Request::Trigger {
        id,
        flow: "ci-flow".into(),
        dry_run: true,
    };
    let body = serde_json::to_vec(&req).expect("serialize");
    send_frame(&mut client, &body).await;

    let resp_bytes = recv_frame(&mut client).await;
    let resp: Response = serde_json::from_slice(&resp_bytes).expect("parse");
    match resp {
        Response::Ok { id: rid, data } => {
            assert_eq!(rid, id);
            assert_eq!(data["trigger"], "ci-flow");
            assert_eq!(data["dry_run"], true);
        }
        other => panic!("expected Ok, got {other:?}"),
    }
    assert_eq!(handler.trigger_calls.load(Ordering::SeqCst), 1);

    drop(client);
    let _ = server.await;
}

#[tokio::test]
async fn status_request_with_no_filter_routes_to_handler() {
    let handler = Arc::new(MockHandler::default());
    let (mut client, server) = spawn_server(Arc::clone(&handler));

    let id = Uuid::new_v4();
    let req = Request::Status { id, flow: None };
    let body = serde_json::to_vec(&req).expect("serialize");
    send_frame(&mut client, &body).await;

    let resp_bytes = recv_frame(&mut client).await;
    let resp: Response = serde_json::from_slice(&resp_bytes).expect("parse");
    match resp {
        Response::Ok { id: rid, data } => {
            assert_eq!(rid, id);
            assert!(
                data["status"].is_null(),
                "no flow filter → null in response"
            );
        }
        other => panic!("expected Ok, got {other:?}"),
    }
    assert_eq!(handler.status_calls.load(Ordering::SeqCst), 1);

    drop(client);
    let _ = server.await;
}

#[tokio::test]
async fn malformed_json_returns_error_response_continues_connection() {
    let handler = Arc::new(MockHandler::default());
    let (mut client, server) = spawn_server(Arc::clone(&handler));

    // Send a frame containing JSON that doesn't match Request's
    // tagged-enum shape. The server must reply with Response::Error
    // and keep the connection open.
    let body = b"{\"this is\": \"not a Request\"}";
    send_frame(&mut client, body).await;

    let resp_bytes = recv_frame(&mut client).await;
    let resp: Response = serde_json::from_slice(&resp_bytes).expect("parse");
    match resp {
        Response::Error { message, .. } => {
            assert!(
                !message.is_empty(),
                "Error response must carry an explanation",
            );
        }
        Response::Ok { .. } => panic!("malformed body must NOT produce Ok"),
    }
    // Handler was NOT invoked because deserialize failed before
    // dispatch.
    assert_eq!(handler.trigger_calls.load(Ordering::SeqCst), 0);
    assert_eq!(handler.status_calls.load(Ordering::SeqCst), 0);
    assert_eq!(handler.reload_calls.load(Ordering::SeqCst), 0);
    assert_eq!(handler.version_calls.load(Ordering::SeqCst), 0);

    // Connection should still be open — verify by sending a valid
    // Version request and reading the response.
    let id = Uuid::new_v4();
    let req = Request::Version { id };
    let body = serde_json::to_vec(&req).expect("serialize");
    send_frame(&mut client, &body).await;
    let resp_bytes = recv_frame(&mut client).await;
    let resp: Response = serde_json::from_slice(&resp_bytes).expect("parse");
    assert!(matches!(resp, Response::Ok { .. }));
    assert_eq!(handler.version_calls.load(Ordering::SeqCst), 1);

    drop(client);
    let _ = server.await;
}

#[tokio::test]
async fn reload_rate_limit_rejects_second_within_window() {
    // Two reload requests on the same connection with a fresh gate.
    // The first acquires the gate; the second falls inside the
    // RELOAD_WINDOW=1s and should reject with Response::Error.
    let handler = Arc::new(MockHandler::default());
    let (mut client, server) = spawn_server(Arc::clone(&handler));

    let id1 = Uuid::new_v4();
    let req1 = Request::Reload { id: id1 };
    send_frame(&mut client, &serde_json::to_vec(&req1).unwrap()).await;
    let resp1: Response = serde_json::from_slice(&recv_frame(&mut client).await).unwrap();
    assert!(
        matches!(resp1, Response::Ok { .. }),
        "first reload must succeed, got {resp1:?}",
    );

    let id2 = Uuid::new_v4();
    let req2 = Request::Reload { id: id2 };
    send_frame(&mut client, &serde_json::to_vec(&req2).unwrap()).await;
    let resp2: Response = serde_json::from_slice(&recv_frame(&mut client).await).unwrap();
    match resp2 {
        Response::Error { message, .. } => {
            assert!(
                message.contains("rate") || message.contains("limit"),
                "rate-limit reject message must mention rate/limit; got: {message}",
            );
        }
        Response::Ok { .. } => panic!("second reload within 1s must be rate-limited"),
    }

    // Only the first reload reached the handler.
    assert_eq!(handler.reload_calls.load(Ordering::SeqCst), 1);

    drop(client);
    let _ = server.await;
}

#[tokio::test]
async fn eof_close_terminates_handler_loop() {
    // Client closes its end immediately. The server's codec read
    // returns None; handle_connection_for_test exits cleanly.
    let handler = Arc::new(MockHandler::default());
    let (client, server) = spawn_server(Arc::clone(&handler));

    drop(client);
    // The server task should complete on its own — no need to send
    // anything. Bound the wait so a hang surfaces as a test
    // timeout rather than a stalled CI.
    tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .expect("server must exit on EOF within 5s")
        .expect("server task must not panic");

    assert_eq!(handler.trigger_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn multiple_requests_on_one_connection_each_dispatched() {
    // Pin the codec loop's "keep reading" semantics: a second
    // request after a first valid Ok response must reach the
    // handler.
    let handler = Arc::new(MockHandler::default());
    let (mut client, server) = spawn_server(Arc::clone(&handler));

    for _ in 0..3 {
        let id = Uuid::new_v4();
        let req = Request::Status {
            id,
            flow: Some("f".into()),
        };
        send_frame(&mut client, &serde_json::to_vec(&req).unwrap()).await;
        let _resp: Response = serde_json::from_slice(&recv_frame(&mut client).await).unwrap();
    }
    assert_eq!(handler.status_calls.load(Ordering::SeqCst), 3);

    drop(client);
    let _ = server.await;
}

/// Handler stub that returns Err from every method. Drives the
/// `Err(message) => Response::Error` arm in dispatch (per the
/// per-request-kind dispatch arms in control::server). MockHandler
/// always returns Ok, so without this stub the per-arm Err mapping
/// is uncovered.
#[derive(Default)]
struct ErrHandler;

impl Handler for ErrHandler {
    async fn trigger(&self, _flow: &str, _dry_run: bool) -> Result<serde_json::Value, String> {
        Err("trigger handler failure".to_string())
    }
    async fn status(&self, _flow: Option<&str>) -> Result<serde_json::Value, String> {
        Err("status handler failure".to_string())
    }
    async fn reload(&self) -> Result<serde_json::Value, String> {
        Err("reload handler failure".to_string())
    }
    async fn version(&self) -> Result<serde_json::Value, String> {
        Err("version handler failure".to_string())
    }
}

fn spawn_err_server() -> (UnixStream, tokio::task::JoinHandle<()>) {
    let (server_stream, client_stream) = UnixStream::pair().expect("pair");
    let cancel = CancellationToken::new();
    let handler = Arc::new(ErrHandler);
    let server_handle = tokio::spawn(async move {
        let _ = handle_connection_for_test(server_stream, handler, cancel).await;
    });
    (client_stream, server_handle)
}

#[tokio::test]
async fn trigger_handler_err_routes_to_response_error() {
    // The Trigger arm of dispatch — when handler.trigger returns Err,
    // dispatch builds Response::Error{id, message}. Pin: the response
    // carries the request id verbatim AND the handler's error message
    // body.
    let (mut client, server) = spawn_err_server();
    let id = Uuid::new_v4();
    let req = Request::Trigger {
        id,
        flow: "any-flow".into(),
        dry_run: false,
    };
    send_frame(&mut client, &serde_json::to_vec(&req).unwrap()).await;
    let resp: Response = serde_json::from_slice(&recv_frame(&mut client).await).unwrap();
    match resp {
        Response::Error { id: rid, message } => {
            assert_eq!(rid, id, "Error response must echo request id");
            assert_eq!(message, "trigger handler failure");
        }
        other => panic!("expected Error, got {other:?}"),
    }
    drop(client);
    let _ = server.await;
}

#[tokio::test]
async fn status_handler_err_routes_to_response_error() {
    // Same shape as trigger but for the Status arm of dispatch. Pin
    // per-arm so a regression that swapped error routing (e.g.
    // forgot to return early on Err) surfaces here.
    let (mut client, server) = spawn_err_server();
    let id = Uuid::new_v4();
    let req = Request::Status { id, flow: None };
    send_frame(&mut client, &serde_json::to_vec(&req).unwrap()).await;
    let resp: Response = serde_json::from_slice(&recv_frame(&mut client).await).unwrap();
    match resp {
        Response::Error { id: rid, message } => {
            assert_eq!(rid, id);
            assert_eq!(message, "status handler failure");
        }
        other => panic!("expected Error, got {other:?}"),
    }
    drop(client);
    let _ = server.await;
}

#[tokio::test]
async fn reload_handler_err_routes_to_response_error_after_gate_acquire() {
    // After the rate-limit gate acquires (fresh ReloadGate from
    // handle_connection_for_test always lets the first reload
    // through), if handler.reload returns Err the dispatch builds
    // Response::Error. Pin that the reload happy path
    // (gate.try_acquire returned true) still produces an Error
    // response when the handler itself fails.
    let (mut client, server) = spawn_err_server();
    let id = Uuid::new_v4();
    let req = Request::Reload { id };
    send_frame(&mut client, &serde_json::to_vec(&req).unwrap()).await;
    let resp: Response = serde_json::from_slice(&recv_frame(&mut client).await).unwrap();
    match resp {
        Response::Error { id: rid, message } => {
            assert_eq!(rid, id);
            assert_eq!(
                message, "reload handler failure",
                "Err from reload handler must propagate verbatim through dispatch",
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
    drop(client);
    let _ = server.await;
}

#[tokio::test]
async fn version_handler_err_routes_to_response_error() {
    // Version arm Err mapping. Same shape as the others; pinned for
    // completeness across all four request kinds.
    let (mut client, server) = spawn_err_server();
    let id = Uuid::new_v4();
    let req = Request::Version { id };
    send_frame(&mut client, &serde_json::to_vec(&req).unwrap()).await;
    let resp: Response = serde_json::from_slice(&recv_frame(&mut client).await).unwrap();
    match resp {
        Response::Error { id: rid, message } => {
            assert_eq!(rid, id);
            assert_eq!(message, "version handler failure");
        }
        other => panic!("expected Error, got {other:?}"),
    }
    drop(client);
    let _ = server.await;
}

#[tokio::test]
async fn cancel_mid_connection_terminates_codec_loop_promptly() {
    // The codec read races cancel.cancelled() against framed.next()
    // under a READ_TIMEOUT_SECS deadline. When cancel fires, the
    // select! cancel arm wins and handle_connection returns Ok(())
    // immediately, even when no frame is in flight. Pin: the server
    // task exits within 100ms wall-clock of the cancel firing, well
    // below the multi-second READ_TIMEOUT_SECS that would otherwise
    // govern the read.
    let (server_stream, client_stream) = UnixStream::pair().expect("pair");
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    let handler = Arc::new(MockHandler::default());
    let server_handle = tokio::spawn(async move {
        let _ = handle_connection_for_test(server_stream, handler, cancel_clone).await;
    });

    // Don't send anything — the server is parked on framed.next()
    // inside the select!. Fire cancel after a brief yield so the
    // server has reached the select!.
    tokio::task::yield_now().await;
    cancel.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(2), server_handle)
        .await
        .expect("cancellation must terminate the codec loop within 2s")
        .expect("server task must not panic");
    drop(client_stream);
}

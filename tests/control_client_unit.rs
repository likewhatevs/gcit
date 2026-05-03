// Unit-level tests for `gcit::control::client::Client` — the one-shot
// control client used by `gcit reload`, `gcit status`, and `gcit
// trigger`. The CLI-side tests in cli_control_socket_unreachable.rs
// already cover the connect-failure path through the binary; these
// tests exercise `Client::connect` and `Client::send` directly so
// regressions in the framing / id-correlation / timeout logic surface
// independently of the binary harness.
//
// Wire format mirror: 4-byte big-endian u32 length prefix, body is
// serde_json bytes (per the LengthDelimitedCodec builder in
// control::client and the MAX_FRAME_LEN constant in control::protocol).

use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tempfile::TempDir;
use tokio::net::{UnixListener, UnixStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use uuid::Uuid;

use gcit::control::{Client, Request, Response, MAX_FRAME_LEN};

/// Build a tempdir-rooted Unix listener at `<dir>/control.sock`. The
/// path is unique per TempDir so parallel test runs cannot collide.
fn bind_listener(dir: &TempDir) -> (UnixListener, std::path::PathBuf) {
    let path = dir.path().join("control.sock");
    let listener = UnixListener::bind(&path).expect("bind unix listener");
    (listener, path)
}

/// Wrap an accepted stream in the same LengthDelimitedCodec the
/// production server uses (control::server uses the same builder
/// shape). Pinning these knobs in the test helper guards against a
/// regression in either side that would silently break wire
/// compatibility.
fn wrap_server(stream: UnixStream) -> Framed<UnixStream, LengthDelimitedCodec> {
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_LEN)
        .length_field_type::<u32>()
        .big_endian()
        .new_codec();
    Framed::new(stream, codec)
}

#[tokio::test]
async fn connect_to_nonexistent_path_returns_io_error() {
    // Client::connect calls UnixStream::connect which fails with
    // NotFound for a path that never existed. Client::connect
    // forwards that error verbatim via the `?`. Pinned at the unit
    // level so a regression that wraps the error or drops the kind
    // surfaces in the unit test rather than only in the CLI tests.
    let td = TempDir::new().unwrap();
    let nonexistent = td.path().join("does-not-exist.sock");
    // `Client` does not implement Debug (its only field is the wrapped
    // Framed stream), so `expect_err` is unavailable. Match on the
    // Result instead — same coverage, no Debug bound.
    let err = match Client::connect(&nonexistent).await {
        Ok(_) => panic!("connect to nonexistent path must fail"),
        Err(e) => e,
    };
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::NotFound,
        "ENOENT must propagate as ErrorKind::NotFound; got {:?}",
        err.kind(),
    );
}

#[tokio::test]
async fn connect_to_bound_listener_succeeds_and_returns_client() {
    // Round-trip the construction path: bind a UnixListener (no accept
    // loop yet — connect doesn't require accept to land), then call
    // Client::connect against the same path. The connect should land
    // synchronously enough that we don't need a separate accept task.
    let td = TempDir::new().unwrap();
    let (_listener, path) = bind_listener(&td);
    // Client::connect returns Ok(Self { framed }) — drop the value to
    // exercise the connection-close path through Drop.
    let _client = Client::connect(&path)
        .await
        .expect("connect to bound listener");
}

#[tokio::test]
async fn send_round_trips_request_id_through_response() {
    // Bind a listener, accept one connection, drive a Request::Version
    // through it and reply with a matching Response::Ok. The client's
    // send() must return the response with the same id.
    let td = TempDir::new().unwrap();
    let (listener, path) = bind_listener(&td);
    let id = Uuid::new_v4();

    // Server side: accept one, read one frame, write one frame, drop.
    let server = tokio::spawn(async move {
        let (stream, _addr) = listener.accept().await.expect("accept");
        let mut framed = wrap_server(stream);
        let bytes = framed
            .next()
            .await
            .expect("server must receive one frame")
            .expect("frame decode");
        let req: Request = serde_json::from_slice(&bytes).expect("server-side request deserialize");
        // Echo the id verbatim — the client must accept this as a
        // matching response.
        assert_eq!(req.id(), id, "server saw mismatched request id");
        let resp = Response::Ok {
            id: req.id(),
            data: serde_json::json!({"version": "test"}),
        };
        let body = serde_json::to_vec(&resp).expect("server-side response serialize");
        framed.send(body.into()).await.expect("server send");
    });

    let mut client = Client::connect(&path).await.expect("client connect");
    let resp = client
        .send(Request::Version { id })
        .await
        .expect("client send must succeed");
    match resp {
        Response::Ok { id: rid, data } => {
            assert_eq!(rid, id, "Response::Ok id must echo Request id");
            assert_eq!(
                data,
                serde_json::json!({"version": "test"}),
                "Response::Ok data must round-trip verbatim",
            );
        }
        other => panic!("expected Response::Ok, got {:?}", other),
    }
    server.await.expect("server task must not panic");
}

#[tokio::test]
async fn send_returns_error_when_response_id_does_not_match_request_id() {
    // Client::send enforces id correlation: a response whose id does
    // not equal the request's id is rejected with io::Error::other
    // so a multiplexing client cannot accidentally consume an
    // unrelated reply. Drive the mismatch path: server replies with
    // a different uuid → client's send returns Err.
    let td = TempDir::new().unwrap();
    let (listener, path) = bind_listener(&td);
    let request_id = Uuid::new_v4();
    let response_id = Uuid::new_v4();
    assert_ne!(
        request_id, response_id,
        "uuid v4 collision is statistically impossible — re-run if seen",
    );

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut framed = wrap_server(stream);
        let _ = framed.next().await.expect("recv").expect("decode");
        let resp = Response::Ok {
            id: response_id,
            data: serde_json::json!({}),
        };
        let body = serde_json::to_vec(&resp).expect("serialize");
        framed.send(body.into()).await.expect("send");
    });

    let mut client = Client::connect(&path).await.expect("connect");
    let err = client
        .send(Request::Reload { id: request_id })
        .await
        .expect_err("mismatched response id must surface as Err");
    let msg = err.to_string();
    assert!(
        msg.contains("response id mismatch"),
        "error must lead with the canonical 'response id mismatch' message; got: {msg}",
    );
    assert!(
        msg.contains(&request_id.to_string()),
        "mismatch error must name the want id; got: {msg}",
    );
    assert!(
        msg.contains(&response_id.to_string()),
        "mismatch error must name the got id; got: {msg}",
    );
    server.await.expect("server");
}

#[tokio::test]
async fn send_returns_unexpected_eof_when_server_closes_before_replying() {
    // Client::send maps `Ok(None)` from the codec stream to
    // `io::Error::new(UnexpectedEof, "control connection closed
    // before response")`. Drive that path: the server accepts, reads
    // the frame, then drops the connection without writing any
    // reply. The client's framed.next() yields None on EOF; send
    // must surface UnexpectedEof.
    let td = TempDir::new().unwrap();
    let (listener, path) = bind_listener(&td);

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut framed = wrap_server(stream);
        let _ = framed.next().await.expect("recv").expect("decode");
        // Drop without replying. tokio's UnixStream closes on drop;
        // the client's read side observes EOF.
        drop(framed);
    });

    let mut client = Client::connect(&path).await.expect("connect");
    let err = client
        .send(Request::Reload { id: Uuid::new_v4() })
        .await
        .expect_err("server EOF before reply must surface as Err");
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::UnexpectedEof,
        "EOF before reply must surface as ErrorKind::UnexpectedEof; got {:?}",
        err.kind(),
    );
    assert!(
        err.to_string().contains("closed before response"),
        "EOF error must name 'closed before response'; got: {err}",
    );
    server.await.expect("server");
}

#[tokio::test]
async fn send_returns_timed_out_when_server_holds_open_without_replying() {
    // Client::send wraps the framed.next() poll in a
    // tokio::time::timeout against READ_TIMEOUT_SECS=5 (defined in
    // control::protocol). When that elapses, send returns
    // io::Error::new(TimedOut, "control reply timed out after 5s").
    //
    // Driving a real 5-second wall-clock test would be slow; instead
    // we use tokio::time::pause() to advance the runtime clock past
    // the timeout instantaneously. The server holds the connection
    // open without replying so the client's read polls indefinitely
    // until the (now-virtual) clock advances past the timeout.
    tokio::time::pause();

    let td = TempDir::new().unwrap();
    let (listener, path) = bind_listener(&td);

    let server = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.expect("accept");
        // Hold the stream open; never write. The client's read times
        // out via its 5s deadline; we keep the listener task pinned
        // to a long-but-bounded sleep so the test runtime can
        // advance and unwind the task at end-of-test.
        tokio::time::sleep(Duration::from_secs(60)).await;
    });

    let mut client = Client::connect(&path).await.expect("connect");
    // Spawn the send so we can advance the clock from this task.
    let send_task =
        tokio::spawn(async move { client.send(Request::Reload { id: Uuid::new_v4() }).await });

    // Yield so the send task gets a chance to install its timeout
    // future before we advance the clock past it. With paused time,
    // the send's 5s timeout fires the moment we advance past it.
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(6)).await;

    // Bound the .await on the spawned task to a short virtual deadline
    // — the timeout fires immediately under paused time, so the task
    // resolves in finite virtual time.
    let started = Instant::now();
    let result = send_task.await.expect("send task must not panic");
    // Real wall-clock elapsed must be tiny (the timeout fired in
    // virtual time, not wall-clock time). Pin a generous ceiling so
    // the test stays robust on a loaded runner.
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(5),
        "paused-time test must complete in negligible wall-clock; took {:?}",
        elapsed,
    );

    let err = result.expect_err("read timeout must surface as Err");
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::TimedOut,
        "READ_TIMEOUT must surface as ErrorKind::TimedOut; got {:?}",
        err.kind(),
    );
    assert!(
        err.to_string().contains("timed out"),
        "timeout error must mention 'timed out'; got: {err}",
    );

    // Don't leak the server task; abort it explicitly.
    server.abort();
    let _ = server.await;
}

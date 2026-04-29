// Control channel protocol round-trip for all 4 Request variants.
// Wire framing: tokio_util::codec::LengthDelimitedCodec, u32 BE prefix.
// Body: serde_json.
// Request: Trigger{id,flow,dry_run}, Status{id,flow}, Reload{id}, Version{id}.
// Response: Ok{id,data} | Error{id,message}.
//
// Same-binary multiple #[test]s OK here because no env mutation.

use rstest::rstest;
use std::os::unix::net::{UnixListener, UnixStream};
use tempfile::TempDir;

#[rstest]
#[case::version("Version")]
#[case::status_global("Status")]
#[case::status_specific("Status")]
#[case::reload("Reload")]
#[case::trigger_dry_run("Trigger")]
#[case::trigger_real("Trigger")]
#[ignore = "requires gcit::control::{Request,Response,Server} (not yet implemented)"]
fn round_trip_request_returns_matching_response_id(#[case] _kind: &str) {
    // Each Request variant carries `id: Uuid`. The Response
    // MUST echo that id so the client can correlate. This is the most
    // common mutation target: an arm that drops the id or generates a new
    // one. Mutation testing will catch it; this test asserts canonically.
    //
    // Implementer hook:
    //   let id = uuid::Uuid::new_v4();
    //   let req = Request::Version { id };
    //   client_send(&mut stream, &req);
    //   let resp = client_recv(&mut stream);
    //   match resp {
    //       Response::Ok { id: rid, .. } | Response::Error { id: rid, .. } => {
    //           assert_eq!(id, rid, "response MUST echo request id");
    //       }
    //   }
}

#[test]
#[ignore = "requires control-channel server implementation"]
fn version_returns_crate_version_and_git_sha() {
    // `gcit --version` returns version + git SHA. The control-channel
    // Version request MUST return the same string (single source of
    // truth via vergen-gix).
    //
    // Assert: Response::Ok { data: { "version": "X.Y.Z (sha)" } } matches
    // the same format as `gcit --version` stdout.
}

#[test]
#[ignore = "requires control-channel server implementation"]
fn status_returns_per_flow_chain_with_last_error() {
    // Status JSON includes `last_error` per flow with fields: at,
    // kind, message, retry_at?. Single most recent error overwritten
    // on next.
    //
    // Build a daemon with one flow that has experienced a failure; assert
    // the Status response carries the last_error JSON shape exactly.
}

#[test]
#[ignore = "requires control-channel server implementation"]
fn trigger_dry_run_does_not_fire_workflow() {
    // `gcit trigger <FLOW> --dry-run` prints the GitHub + Discord
    // payloads WITHOUT sending. The control-channel Trigger request
    // with dry_run=true must (a) return the rendered payloads in
    // Response::Ok.data, (b) NOT call octocrab.create_workflow_dispatch,
    // (c) NOT call twilight.execute_webhook.
    //
    // Asserted by registering a wiremock that records calls; assert call
    // count == 0 after dry-run.
}

#[test]
#[ignore = "requires control-channel server implementation"]
fn length_delimited_codec_u32_be_framing() {
    // u32 BE length prefix. Send a hand-rolled wire frame
    // (4-byte BE length + JSON body) and assert the daemon parses it.
    // Catches a regression where the codec is built with a different
    // length_field_type or endianness.
    //
    // let listener = UnixListener::bind(...);
    // let mut client = UnixStream::connect(...);
    // let body = serde_json::to_vec(&Request::Version { id }).unwrap();
    // client.write_all(&(body.len() as u32).to_be_bytes()).unwrap();
    // client.write_all(&body).unwrap();
    // // assert response prefix is also u32 BE.
}

#[test]
fn smoke_unixlistener_bind() {
    // sanity: confirm tempfile + UnixListener pattern works in this
    // tester's environment so the actual tests above aren't blocked on
    // env oddities when they get unblocked.
    let dir = TempDir::new().unwrap();
    let sock_path = dir.path().join("smoke.sock");
    let _ = std::fs::remove_file(&sock_path);
    let listener = UnixListener::bind(&sock_path).unwrap();
    let _client = UnixStream::connect(&sock_path).unwrap();
    drop(listener);
}

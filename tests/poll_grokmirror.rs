// Grokmirror manifest parse + lookup + HTTP integration.
//
// gcit's grokmirror strategy fetches manifest.js.gz, decompresses it,
// parses the JSON map of repo path -> entry, and reads the
// fingerprint to detect upstream changes. The integration tests here
// drive the public `gcit::git::grokmirror::*` API. The pure-parse
// tests gzip-compress synthetic JSON bodies and call
// `parse_manifest_bytes` directly. The HTTP tests stand up a wiremock
// server that serves a real `manifest.js.gz` response so the full
// `fetch_manifest` path — request, status classification, body
// streaming, and JSON parse — runs end-to-end.
//
// Wire-format reminder: the manifest is a STATIC `.gz` file, NOT
// served with `Content-Encoding: gzip`. reqwest's transparent
// decompression therefore does not apply; the strategy decodes the
// body explicitly via flate2. The wiremock mocks below mirror that
// behavior — they hand back raw gzipped bytes with no Content-
// Encoding header, exactly like git.kernel.org would.

use std::io::Write;
use std::time::Duration;

use flate2::write::GzEncoder;
use flate2::Compression;
use reqwest::Client;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use gcit::git::grokmirror::{self, GrokmirrorError};

fn gzip_json(value: &serde_json::Value) -> Vec<u8> {
    let raw = serde_json::to_vec(value).expect("serialize JSON");
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(&raw).expect("gzip write");
    enc.finish().expect("gzip finish")
}

/// Build a reqwest client for the wiremock tests with an explicit
/// 10-second total request timeout. The crate-level reqwest
/// dependency is built with `rustls-tls-native-roots` + `gzip`, so
/// these defaults match the daemon's runtime configuration. The
/// timeout caps wall-clock if `fetch_manifest` ever regresses to a
/// hanging request shape — without it a wiremock test could stall
/// nextest indefinitely instead of failing fast.
fn build_client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("reqwest client builds with 10s timeout")
}

#[test]
fn parse_minimal_manifest_round_trips_through_lookup() {
    let body = json!({
        "/pub/scm/linux/kernel/git/torvalds/linux.git": {
            "fingerprint": "abc123",
            "modified": 1_700_000_000_u64
        },
        "/pub/scm/linux/kernel/git/stable/linux.git": {
            "fingerprint": "def456",
            "modified": 1_700_000_500_u64
        }
    });
    let bytes = gzip_json(&body);
    let manifest = grokmirror::parse_manifest_bytes(&bytes).expect("parse");
    let fp =
        grokmirror::lookup_fingerprint(&manifest, "/pub/scm/linux/kernel/git/torvalds/linux.git")
            .expect("lookup");
    assert_eq!(fp, "abc123");
}

#[test]
fn lookup_fingerprint_for_unknown_repo_returns_repo_not_in_manifest() {
    let body = json!({
        "/pub/scm/torvalds/linux.git": {
            "fingerprint": "x",
            "modified": 0_u64
        }
    });
    let bytes = gzip_json(&body);
    let manifest = grokmirror::parse_manifest_bytes(&bytes).expect("parse");
    let err = grokmirror::lookup_fingerprint(&manifest, "/not/in/manifest.git").unwrap_err();
    match err {
        GrokmirrorError::RepoNotInManifest { repo_path } => {
            assert_eq!(repo_path, "/not/in/manifest.git");
        }
        other => panic!("expected RepoNotInManifest, got {other:?}"),
    }
}

#[test]
fn parse_rejects_non_gzip_bytes() {
    // The manifest filename is .js.gz; the function expects gzip-
    // compressed input. Plain JSON without compression must fail
    // with a Permanent error so the operator sees a clear diagnosis
    // rather than a "JSON parse error" pointing at gibberish.
    let plain_json = b"{\"/repo.git\": {\"fingerprint\": \"x\", \"modified\": 0}}";
    let err = grokmirror::parse_manifest_bytes(plain_json).unwrap_err();
    match err {
        GrokmirrorError::Permanent { message } => {
            assert!(
                message.to_ascii_lowercase().contains("gzip")
                    || message.to_ascii_lowercase().contains("decompress"),
                "error must mention gzip/decompression: {message}",
            );
        }
        other => panic!("expected Permanent, got {other:?}"),
    }
}

#[test]
fn parse_rejects_gzip_with_truncated_json_body() {
    // Gzip-encoded but the JSON inside is truncated — surfaces as a
    // serde parse error after decompression.
    let truncated = b"{\"/repo.git\": {\"fingerprint";
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(truncated).unwrap();
    let bytes = enc.finish().unwrap();
    let err = grokmirror::parse_manifest_bytes(&bytes).unwrap_err();
    match err {
        GrokmirrorError::Permanent { message } => {
            assert!(
                message.to_ascii_lowercase().contains("parse")
                    || message.to_ascii_lowercase().contains("expected"),
                "error must mention parse failure: {message}",
            );
        }
        other => panic!("expected Permanent on truncated JSON, got {other:?}"),
    }
}

#[test]
fn extract_repo_path_from_kernel_url() {
    // `repo_path` to avoid shadowing the wiremock `path` matcher
    // imported above; the value here is a manifest-key path
    // (`/pub/scm/...`), not an HTTP path.
    let repo_path = grokmirror::extract_repo_path_from_url(
        "https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git",
    )
    .expect("extract");
    assert_eq!(repo_path, "/pub/scm/linux/kernel/git/torvalds/linux.git");
}

/// Full HTTP fetch path: stand up a wiremock server, serve a gzipped
/// manifest at `/manifest.js.gz`, and assert `fetch_manifest` parses
/// it end-to-end. Pins:
///   - the URL shape `<base>/manifest.js.gz` (per build_manifest_url
///     at src/git/grokmirror.rs:188)
///   - that the raw gzipped body decompresses without reqwest's gzip
///     feature getting in the way (kernel.org does NOT set
///     Content-Encoding: gzip on the static .gz file, so reqwest must
///     hand the bytes through verbatim)
///   - that the parsed manifest is then queryable via
///     `lookup_fingerprint`
#[tokio::test]
async fn fetch_manifest_returns_parsed_body_for_200_response() {
    let mock = MockServer::start().await;
    let body = json!({
        "/pub/scm/linux/kernel/git/torvalds/linux.git": {
            "fingerprint": "abc123",
            "modified": 1_700_000_000_u64
        },
        "/pub/scm/linux/kernel/git/stable/linux.git": {
            "fingerprint": "def456",
            "modified": 1_700_000_500_u64
        }
    });
    let gzipped = gzip_json(&body);
    Mock::given(method("GET"))
        .and(path("/manifest.js.gz"))
        .respond_with(
            // Hand back the raw gzipped bytes with the same
            // Content-Type kernel.org serves. Crucially DO NOT
            // set Content-Encoding: gzip — that would let reqwest's
            // built-in gzip layer decompress the body before our
            // flate2 path runs, and the strategy would then attempt
            // to gunzip the already-decompressed JSON and fail. The
            // mime-type argument to `set_body_raw` already sets
            // Content-Type, so no separate insert_header is needed.
            ResponseTemplate::new(200).set_body_raw(gzipped, "application/octet-stream"),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let client = build_client();
    let manifest = grokmirror::fetch_manifest(&client, &mock.uri())
        .await
        .expect("fetch_manifest succeeds against wiremock");
    let fp =
        grokmirror::lookup_fingerprint(&manifest, "/pub/scm/linux/kernel/git/torvalds/linux.git")
            .expect("torvalds repo present");
    assert_eq!(
        fp, "abc123",
        "torvalds fingerprint must round-trip from gzipped wiremock body",
    );
    let fp =
        grokmirror::lookup_fingerprint(&manifest, "/pub/scm/linux/kernel/git/stable/linux.git")
            .expect("stable repo present");
    assert_eq!(
        fp, "def456",
        "stable fingerprint must round-trip from gzipped wiremock body",
    );
}

/// HTTP 503 from the manifest endpoint maps to
/// `GrokmirrorError::Transient` per the `status.is_server_error()`
/// arm at src/git/grokmirror.rs:117-126. The supervisor retries on
/// Transient — a regression that turned 5xx into Permanent would
/// stop polling forever after the first hiccup. 503 is the realistic
/// upstream-overload signal kernel.org's grokmirror returns under
/// load; pinning it (rather than a generic 500) anchors the test to
/// the operator-visible failure mode.
#[tokio::test]
async fn fetch_manifest_503_returns_transient_error() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/manifest.js.gz"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&mock)
        .await;

    let client = build_client();
    let err = grokmirror::fetch_manifest(&client, &mock.uri())
        .await
        .expect_err("503 must surface as Err");
    match err {
        GrokmirrorError::Transient { message } => {
            assert!(
                message.contains("503"),
                "transient error message must include the status code: {message}",
            );
        }
        other => panic!("expected Transient on 503, got {other:?}"),
    }
}

/// HTTP 404 from the manifest endpoint maps to
/// `GrokmirrorError::Permanent` per the trailing arm at
/// src/git/grokmirror.rs:127-133 (any non-success, non-5xx status).
/// 404 means the operator pointed gcit at a host that does not
/// publish a grokmirror manifest at all — retrying will not help.
#[tokio::test]
async fn fetch_manifest_404_returns_permanent_error() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/manifest.js.gz"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&mock)
        .await;

    let client = build_client();
    let err = grokmirror::fetch_manifest(&client, &mock.uri())
        .await
        .expect_err("404 must surface as Err");
    match err {
        GrokmirrorError::Permanent { message } => {
            assert!(
                message.contains("404"),
                "permanent error message must include the status code: {message}",
            );
        }
        other => panic!("expected Permanent on 404, got {other:?}"),
    }
}

/// Pins the URL-shape contract: `fetch_manifest` MUST request
/// `<base>/manifest.js.gz`. wiremock's `path()` matcher rejects
/// requests that don't match the literal path; this test mounts a
/// `/manifest.js.gz` matcher with `.expect(1)`. A regression that
/// switched the appended path (e.g. `/manifest.js`,
/// `/grokmirror.json`, `/manifest.gz`) would leave the matcher
/// unsatisfied and surface as a wiremock verification error in
/// Drop.
#[tokio::test]
async fn fetch_manifest_pins_manifest_js_gz_path() {
    let mock = MockServer::start().await;
    let body = json!({
        "/repo.git": { "fingerprint": "fp", "modified": 1_u64 }
    });
    let gzipped = gzip_json(&body);
    Mock::given(method("GET"))
        .and(path("/manifest.js.gz"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(gzipped, "application/octet-stream"))
        .expect(1)
        .mount(&mock)
        .await;

    let client = build_client();
    let manifest = grokmirror::fetch_manifest(&client, &mock.uri())
        .await
        .expect("fetch_manifest succeeds against the pinned path");
    assert_eq!(
        grokmirror::lookup_fingerprint(&manifest, "/repo.git").expect("repo present"),
        "fp",
        "path-pin test: fingerprint must round-trip from the /manifest.js.gz route",
    );
    // wiremock verifies `.expect(1)` was satisfied when the MockServer
    // is dropped at end of test; if the request hit a different path
    // the count would be 0 and Drop would panic.
}

/// `build_manifest_url` (src/git/grokmirror.rs:188-191) trims a
/// trailing slash from the base URL before appending
/// `/manifest.js.gz`. If the trim regresses, the request would go to
/// `<base>//manifest.js.gz` and either 404 or hit a non-matching
/// wiremock route. Pin the trim by passing a base URL with a
/// trailing slash and asserting the request still resolves at the
/// expected path.
#[tokio::test]
async fn fetch_manifest_handles_trailing_slash_in_base_url() {
    let mock = MockServer::start().await;
    let body = json!({
        "/r.git": { "fingerprint": "abc", "modified": 0_u64 }
    });
    let gzipped = gzip_json(&body);
    Mock::given(method("GET"))
        .and(path("/manifest.js.gz"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(gzipped, "application/octet-stream"))
        .expect(1)
        .mount(&mock)
        .await;

    let base_with_slash = format!("{}/", mock.uri());
    let client = build_client();
    let manifest = grokmirror::fetch_manifest(&client, &base_with_slash)
        .await
        .expect("trailing-slash base_url must resolve to the same /manifest.js.gz");
    assert_eq!(
        grokmirror::lookup_fingerprint(&manifest, "/r.git").expect("repo present"),
        "abc",
        "trailing-slash test: fingerprint must round-trip when base_url ends in /",
    );
}

/// Pins the Content-Length pre-check at src/git/grokmirror.rs:141-149:
/// when the server advertises a body larger than `MAX_COMPRESSED_BYTES`
/// (64 MiB), the strategy rejects with `Permanent` BEFORE allocating
/// or streaming a multi-GB buffer.
///
/// `#[ignore]` because the wiremock + hyper test scaffolding cannot
/// emit a response whose `Content-Length` header lies about the body
/// size: hyper's HTTP/1 serializer asserts the body byte count
/// matches the custom Content-Length header and panics the worker
/// task with `payload claims content-length of N, custom
/// content-length header claims M` when they disagree. The
/// connection then breaks before reqwest sees a parsed
/// `Content-Length`, so the strategy classifies the failure as
/// `Transient` (network error) rather than `Permanent` (over-cap),
/// which masks the real cap-check arm.
///
/// Real coverage exists for the streaming-counter arm via the
/// in-module `parse_rejects_decompressed_over_cap` test
/// (src/git/grokmirror.rs::tests at the file's bottom) — that one
/// pins the post-decompression cap on a synthetic body. The
/// pre-check via Content-Length is unreachable from a wiremock
/// fixture without swapping the HTTP server for a hand-rolled
/// hyper service. The 64 MiB body brief explicitly skipped is the
/// other path; both lead to the same Permanent-cap rejection in
/// production. Re-enable this test if the scaffolding gains a
/// mechanism to override Content-Length without hyper rejecting
/// the body shape.
#[tokio::test]
#[ignore = "hyper rejects mismatched Content-Length custom header — connection breaks before pre-check fires"]
async fn fetch_manifest_oversized_content_length_returns_permanent() {
    let mock = MockServer::start().await;
    // 65 MiB — one byte past MAX_COMPRESSED_BYTES (64 MiB).
    let oversized: u64 = 65 * 1024 * 1024;
    Mock::given(method("GET"))
        .and(path("/manifest.js.gz"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", oversized.to_string().as_str())
                .set_body_raw(b"\x1f\x8b\x08\x00".as_slice(), "application/octet-stream"),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let client = build_client();
    let err = grokmirror::fetch_manifest(&client, &mock.uri())
        .await
        .expect_err("oversize Content-Length must be Err");
    assert!(matches!(err, GrokmirrorError::Permanent { .. }));
}

/// `POLL_TIMEOUT` is the wall-clock cap on a single `fetch_manifest`
/// round trip; mirrors `ls_remote::POLL_TIMEOUT` (60s). Pin both the
/// exposed public constant and its 60-second value so the symmetry
/// with ls_remote can't drift silently.
#[test]
fn poll_timeout_is_60s_matching_ls_remote() {
    assert_eq!(grokmirror::POLL_TIMEOUT, Duration::from_secs(60));
    assert_eq!(
        grokmirror::POLL_TIMEOUT,
        gcit::git::ls_remote::POLL_TIMEOUT,
        "grokmirror and ls_remote must share the same wall-clock backstop",
    );
}

/// Behavioral pin: a server that accepts the connection but stalls
/// the response past `POLL_TIMEOUT` must surface as `Transient`,
/// not hang the poll task indefinitely. Without the timeout wrap
/// in `fetch_manifest`, this test would block until the test
/// runner's overall timeout (or forever) instead of returning
/// cleanly.
///
/// Mental model — runtimes are NOT shared between test and
/// wiremock. `MockServer::start()` spawns wiremock's listener on a
/// separate runtime that runs on real time. The test runtime is
/// configured with `start_paused = true` and uses virtual time:
/// when the test awaits `fetch_manifest`, the
/// `tokio::time::timeout(POLL_TIMEOUT, ...)` wrap is the only
/// timer registered on the test runtime, so its clock auto-
/// advances to that deadline immediately and fires the timeout in
/// virtual time. Meanwhile wiremock's `set_delay` is sleeping for
/// real wall-clock duration on its own runtime, but the test never
/// observes that delay completing — the strategy-layer timeout
/// races and wins because the test runtime has nothing else to
/// wait for. The end result: the assertion runs in milliseconds of
/// real time even though the strategy-layer timeout it pins is
/// 60 seconds.
///
/// `start_paused` (rather than `pause()` after `MockServer::start`)
/// keeps any internal test-runtime timers wiremock might queue
/// from racing with the auto-advance behavior we depend on.
#[tokio::test(start_paused = true)]
async fn fetch_manifest_times_out_when_server_stalls_past_poll_timeout() {
    let mock = MockServer::start().await;
    let body = json!({
        "/repo.git": { "fingerprint": "fp", "modified": 0_u64 }
    });
    let gzipped = gzip_json(&body);
    Mock::given(method("GET"))
        .and(path("/manifest.js.gz"))
        .respond_with(
            ResponseTemplate::new(200)
                // Stall the response until POLL_TIMEOUT + 1s of
                // virtual time elapses. The strategy's
                // `tokio::time::timeout(POLL_TIMEOUT, ...)` must
                // fire before this delay completes.
                .set_delay(grokmirror::POLL_TIMEOUT + Duration::from_secs(1))
                .set_body_raw(gzipped, "application/octet-stream"),
        )
        // Note: NOT `.expect(1)` — the request is in flight when
        // the timeout fires; whether wiremock counts it as
        // satisfied depends on internal timing. The behavioral
        // assertion below (Transient with "timed out") is the load-
        // bearing check.
        .mount(&mock)
        .await;

    // Build a client with a 600s reqwest deadline — far longer than
    // the 60s strategy-layer POLL_TIMEOUT — so that this test
    // exercises grokmirror's own `tokio::time::timeout` wrap, NOT
    // reqwest's `ClientBuilder::timeout`. (reqwest's timeout is a
    // total request deadline covering connect + headers + body
    // read; production supervisors set it from
    // `http.request_timeout`, default 30s, so in default-config
    // deployments the reqwest deadline at 30s fires first. The
    // strategy-layer cap is the operative backstop only when an
    // operator configures a longer `http.request_timeout`. This
    // test isolates the strategy-layer cap by configuring the
    // reqwest deadline well past it.)
    let client = Client::builder()
        .timeout(Duration::from_secs(600))
        .build()
        .expect("client builds");

    let err = tokio::time::timeout(
        // Outer test-level timeout in virtual time — if the
        // strategy timeout fails to fire, this catches it. Set
        // generously past POLL_TIMEOUT so we observe the strategy's
        // Transient before this fires.
        grokmirror::POLL_TIMEOUT + Duration::from_secs(30),
        grokmirror::fetch_manifest(&client, &mock.uri()),
    )
    .await
    .expect("strategy-layer timeout must fire before the outer test guard")
    .expect_err("stalled server must surface as Err, not Ok");

    match err {
        GrokmirrorError::Transient { message } => {
            assert!(
                message.contains("timed out"),
                "transient timeout error must mention 'timed out': {message}",
            );
        }
        other => panic!("expected Transient on stall, got {other:?}"),
    }
}

/// Connection refused: the manifest server is not listening at the
/// configured address. The strategy must return `Transient` so the
/// next poll cycle retries — connection refused is a recoverable
/// signal (mirror briefly down, network flap, etc.).
///
/// Target `http://127.0.0.1:1` — port 1 is the IANA-reserved
/// `tcpmux` service, which by convention is never bound on dev or
/// CI systems. The TCP connect attempt fails with ECONNREFUSED on
/// Linux, which reqwest surfaces as a Connect error. The strategy
/// maps every reqwest send error to `Transient` via the
/// `client.get(...).send().await.map_err` branch in
/// `fetch_manifest_inner`.
///
/// (`MockServer::start()` then `drop` was tried first but is
/// race-prone — the kernel may not release the listener socket
/// immediately, and concurrent test runners can rebind the port
/// before our connect attempt fires. Reserved-port targets are
/// deterministic.)
#[tokio::test]
async fn fetch_manifest_connection_refused_returns_transient() {
    let dead_uri = "http://127.0.0.1:1";

    let client = build_client();
    let err = grokmirror::fetch_manifest(&client, dead_uri)
        .await
        .expect_err("connect-refused must surface as Err");
    match err {
        GrokmirrorError::Transient { message } => {
            // The exact connect error string varies by reqwest /
            // hyper version. Pin the URL surface so an operator
            // reading `gcit status` sees which mirror is
            // unreachable; do not pin the underlying error wording.
            assert!(
                message.contains("127.0.0.1") || message.contains("manifest.js.gz"),
                "transient connect error must reference the dead URL: {message}",
            );
        }
        other => panic!("expected Transient on connect-refused, got {other:?}"),
    }
}

/// HTTP 403 from the manifest endpoint maps to
/// `GrokmirrorError::Permanent`. The non-success / non-5xx arm in
/// `fetch_manifest_inner` covers all 4xx codes uniformly, so 403
/// (operator's IP banned, hot-link blocked, server access policy)
/// rejects retries the same way 404 does. Pin 403 specifically
/// because it's an operator-actionable signal distinct from
/// "manifest doesn't exist" (404) — the operator needs to fix
/// access, not the URL.
#[tokio::test]
async fn fetch_manifest_403_returns_permanent_error() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/manifest.js.gz"))
        .respond_with(ResponseTemplate::new(403))
        .expect(1)
        .mount(&mock)
        .await;

    let client = build_client();
    let err = grokmirror::fetch_manifest(&client, &mock.uri())
        .await
        .expect_err("403 must surface as Err");
    match err {
        GrokmirrorError::Permanent { message } => {
            assert!(
                message.contains("403"),
                "permanent error message must include the status code: {message}",
            );
        }
        other => panic!("expected Permanent on 403, got {other:?}"),
    }
}

/// HTTP 504 (Gateway Timeout) maps to `GrokmirrorError::Transient`
/// the same way 503 does — both fall under
/// `status.is_server_error()` and both signal "upstream is having
/// trouble, retry later". Pinned distinctly from 503 because real
/// CDN fronts (kernel.org sits behind one) commonly emit 504 on a
/// slow origin, and a regression that classified 504 as Permanent
/// would stop polling that flow forever after one timeout
/// hiccup.
#[tokio::test]
async fn fetch_manifest_504_gateway_timeout_returns_transient_error() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/manifest.js.gz"))
        .respond_with(ResponseTemplate::new(504))
        .expect(1)
        .mount(&mock)
        .await;

    let client = build_client();
    let err = grokmirror::fetch_manifest(&client, &mock.uri())
        .await
        .expect_err("504 must surface as Err");
    match err {
        GrokmirrorError::Transient { message } => {
            assert!(
                message.contains("504"),
                "transient error message must include the status code: {message}",
            );
        }
        other => panic!("expected Transient on 504, got {other:?}"),
    }
}

/// Wire-format pin: the manifest is a STATIC `.gz` file, NOT
/// served with `Content-Encoding: gzip`. If the test mock — or a
/// real mirror — sets the Content-Encoding header, reqwest's
/// transparent gzip layer (enabled via the `gzip` cargo feature)
/// decompresses the body BEFORE our flate2 path runs. Our path
/// then tries to gunzip the already-decompressed JSON and fails
/// with a Permanent error.
///
/// This test sets `Content-Encoding: gzip` deliberately to pin
/// the failure mode. If a future mirror starts setting this header
/// (or a config regression introduces the header in the
/// `set_body_raw` callsites of the other tests), gcit will fail
/// fast with a clear "decompression failed" message rather than
/// silently returning empty / nonsense manifests.
///
/// The Permanent classification is intentional: a server
/// double-decoding is a wire-format bug, not a transient blip.
/// The operator must either configure the upstream to stop
/// claiming Content-Encoding or point gcit at a different mirror.
#[tokio::test]
async fn fetch_manifest_with_content_encoding_gzip_returns_permanent() {
    let mock = MockServer::start().await;
    let body = json!({
        "/repo.git": { "fingerprint": "fp", "modified": 0_u64 }
    });
    let gzipped = gzip_json(&body);
    Mock::given(method("GET"))
        .and(path("/manifest.js.gz"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-encoding", "gzip")
                .set_body_raw(gzipped, "application/octet-stream"),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let client = build_client();
    let err = grokmirror::fetch_manifest(&client, &mock.uri())
        .await
        .expect_err("Content-Encoding: gzip + already-gzipped body must surface as Err");
    match err {
        GrokmirrorError::Permanent { message } => {
            // reqwest's transparent gzip strips the outer encoding,
            // so flate2 sees the inner JSON bytes and fails to
            // decompress them as gzip — Permanent error message
            // should mention decompression / gzip parsing.
            assert!(
                message.to_ascii_lowercase().contains("decompress")
                    || message.to_ascii_lowercase().contains("gzip"),
                "permanent error must mention decompression or gzip: {message}",
            );
        }
        other => panic!("expected Permanent on double-decompress, got {other:?}"),
    }
}

/// End-to-end shape: the response body is a valid gzip stream
/// (so reqwest + flate2 both succeed) but the decompressed
/// payload is not valid JSON. The strategy must surface this as
/// `Permanent` — a malformed manifest is a wire-contract
/// violation, not a transient failure. Operators must fix the
/// upstream or switch mirrors; retrying does not help.
///
/// Companion to the in-module
/// `parse_rejects_gzip_with_truncated_json_body` test, which
/// covers the same path but injects bytes directly into
/// `parse_manifest_bytes`. This test pins the same outcome via
/// the full HTTP pipeline so a regression in the
/// `read_capped → parse_manifest_bytes` plumbing is caught.
#[tokio::test]
async fn fetch_manifest_with_invalid_json_inside_gzip_returns_permanent() {
    let mock = MockServer::start().await;
    // Gzip-encode a body that is NOT JSON. flate2 decodes
    // successfully; serde_json then rejects it.
    let raw = b"this is not json at all" as &[u8];
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(raw).unwrap();
    let gzipped = enc.finish().unwrap();
    Mock::given(method("GET"))
        .and(path("/manifest.js.gz"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(gzipped, "application/octet-stream"))
        .expect(1)
        .mount(&mock)
        .await;

    let client = build_client();
    let err = grokmirror::fetch_manifest(&client, &mock.uri())
        .await
        .expect_err("invalid JSON in gzip must surface as Err");
    match err {
        GrokmirrorError::Permanent { message } => {
            assert!(
                message.to_ascii_lowercase().contains("parse")
                    || message.to_ascii_lowercase().contains("expected"),
                "permanent error must mention parse failure: {message}",
            );
        }
        other => panic!("expected Permanent on invalid JSON, got {other:?}"),
    }
}

/// `extract_repo_path_from_url` returns `Permanent` for URLs that
/// have no path component. The strategy uses this helper to derive
/// the manifest key — without a path component there is no way to
/// look up a fingerprint, so the failure must be unambiguous and
/// non-retriable.
///
/// Pin via two scenarios: a bare-host URL (no path at all) and a
/// host-only-with-slash URL (path is just "/"). Both must reject
/// with a Permanent error whose message names the offending URL.
///
/// Pure unit test (no wiremock) — the helper takes a string in,
/// returns a Result out, and never touches the network.
#[test]
fn extract_repo_path_from_url_rejects_path_less_urls() {
    // Bare host, no path.
    let err = grokmirror::extract_repo_path_from_url("https://git.kernel.org")
        .expect_err("bare-host URL must surface as Err");
    match err {
        GrokmirrorError::Permanent { message } => {
            assert!(
                message.contains("git.kernel.org"),
                "permanent error must name the URL: {message}",
            );
        }
        other => panic!("expected Permanent on bare host, got {other:?}"),
    }

    // Host with trailing slash and no path content.
    let err = grokmirror::extract_repo_path_from_url("https://git.kernel.org/")
        .expect_err("host-with-slash URL must surface as Err");
    match err {
        GrokmirrorError::Permanent { message } => {
            assert!(
                message.contains("git.kernel.org"),
                "permanent error must name the URL: {message}",
            );
        }
        other => panic!("expected Permanent on host-with-slash, got {other:?}"),
    }
}

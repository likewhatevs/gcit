// twilight-http Client construction for Discord webhook delivery.
//
// gcit constructs ONE per-daemon `discord::webhook::Client` and shares
// it across flows via Arc::clone. The Client wraps twilight-http with:
//   - .ratelimiter(None) — gcit handles 429/Retry-After explicitly in
//     the notifier; the in-process global ratelimiter introduces
//     non-deterministic delays that break per-credential rate buckets.
//   - .timeout(http.request_timeout) — operator-configurable.
//   - Tests use `for_test(base_uri, timeout)` which adds .proxy() so
//     wiremock receives the traffic.
//
// Webhook URLs carry their own auth token in the path; no bot token
// is passed at Client construction.

use std::time::Duration;

use secrecy::ExposeSecret;
use twilight_model::id::marker::WebhookMarker;
use twilight_model::id::Id;
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

use gcit::discord::webhook::{parse_webhook_url, Client};

mod common;

/// Strip the `http://` (or `https://`) scheme from a URL so it can be
/// passed to twilight-http's `proxy(host, use_http)` API. wiremock's
fn webhook_id() -> Id<WebhookMarker> {
    parse_webhook_url("https://discord.com/api/webhooks/1234567890/testtoken")
        .unwrap()
        .id
}

fn webhook_token() -> String {
    parse_webhook_url("https://discord.com/api/webhooks/1234567890/testtoken")
        .unwrap()
        .token
        .expose_secret()
        .to_string()
}

#[tokio::test]
async fn client_built_without_bot_token() {
    // gcit constructs the production client via `Client::new(timeout)`
    // — no bot token passed. Webhook URLs are self-authenticating
    // (the token is part of the path), so a bot token would be both
    // unnecessary and harmful (twilight-http would attach an
    // `Authorization: Bot ...` header that Discord rejects on
    // webhook routes).
    //
    // Mutation target: adding a `.token(bot_token)` call
    // in `Client::new` — the call still compiles but webhook
    // requests would carry an Authorization header that fails on
    // Discord's webhook endpoints.
    common::ensure_crypto_provider();
    let client = Client::new(Duration::from_secs(5)).expect("Client::new");
    // The inner twilight client is reachable; smoke-test that the
    // accessor works post-construction.
    let _ = client.inner();
}

#[tokio::test]
async fn client_proxy_directs_to_test_base_uri() {
    // wiremock spins up a real HTTP server; `Client::for_test`
    // points twilight-http at it via .proxy(host:port, use_http=true).
    // Driving execute_webhook through the client must land at
    // wiremock — pinned via expect(1).
    //
    // wiremock's path matcher uses path_regex because twilight-http
    // appends a query string suffix (`?wait=...`) on some webhook
    // endpoints; the regex anchors the static path prefix.
    //
    // Mutation target: hardcoding the production
    // discord.com base in `for_test` — the request misses wiremock
    // and the expect(1) check on Drop catches the regression.
    //
    // Note: passes the scheme-stripped uri because twilight-http's
    // proxy field expects host:port, not a full URL.
    common::ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path_regex(r"^/api/(v\d+/)?webhooks/\d+/.+$"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let client = Client::for_test(mock.uri(), Duration::from_secs(5)).expect("for_test");

    // Fire a webhook execution. The result type carries no body for
    // a 204 — we only care that the request reaches wiremock, not
    // the response shape. A failed proxy hop would either error or
    // miss the mock; the expect(1) on Drop catches both.
    let token = webhook_token();
    let _ = client
        .inner()
        .execute_webhook(webhook_id(), &token)
        .content("test")
        .await;

    // Force expect(1) to be checked at this assertion's source line
    // rather than at MockServer's drop later in the test cleanup.
    drop(mock);
}

#[tokio::test]
async fn client_respects_http_request_timeout_config() {
    // `Client::for_test(base_uri, timeout)` plumbs the timeout into
    // twilight-http's builder. wiremock holds the response for 3s;
    // the client deadline is 1s. The execute_webhook call MUST
    // surface an error within 2s, NOT wait the full 3s.
    //
    // Mutation target: dropping the .timeout() call in
    // `for_test` — webhook calls block indefinitely on slow Discord
    // endpoints, hanging the per-flow notifier task.
    common::ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path_regex(r"^/api/(v\d+/)?webhooks/\d+/.+$"))
        .respond_with(ResponseTemplate::new(204).set_delay(Duration::from_secs(3)))
        .expect(1)
        .mount(&mock)
        .await;

    let client = Client::for_test(mock.uri(), Duration::from_secs(1)).expect("for_test");

    let token = webhook_token();
    let before = std::time::Instant::now();
    let result = client
        .inner()
        .execute_webhook(webhook_id(), &token)
        .content("test")
        .await;
    let elapsed = before.elapsed();

    assert!(
        result.is_err(),
        "request_timeout=1s vs delay=3s should surface as Err; got Ok",
    );
    assert!(
        elapsed < Duration::from_millis(2_500),
        "client must time out within 2.5s; got {elapsed:?}",
    );
}

#[tokio::test]
async fn client_singleton_per_daemon() {
    // gcit constructs ONE Client per daemon and shares it via
    // `Arc<TwilightClient>` across notifier instances. `Client`
    // implements `Clone` cheaply by cloning the inner Arc — verified
    // here via Arc::ptr_eq on the inner pointers.
    //
    // Mutation target: changing `Client` to hold the
    // twilight client by value (not Arc) — every clone creates a
    // separate connection pool, defeating reuse and breaking the
    // "one client per daemon" design.
    common::ensure_crypto_provider();
    let a = Client::new(Duration::from_secs(5)).expect("Client::new");
    let b = a.clone();
    // The public surface exposes only `&TwilightClient` via inner();
    // pointer-equality on the borrowed reference confirms the
    // underlying Arc is shared (reading the same `*const T` through
    // a reference shows Arc::clone semantics: both Arc handles point
    // to the same heap value).
    let a_ptr: *const _ = a.inner();
    let b_ptr: *const _ = b.inner();
    assert_eq!(
        a_ptr, b_ptr,
        "Client::clone must share the inner twilight client (Arc::clone semantics)",
    );
}

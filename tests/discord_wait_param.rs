// ?wait=true for message ID receipt.
// `pub enum NotifyOutcome { Sent { receipt: String }, ... }` — the
// receipt is the operator-facing identifier of the delivered
// notification.
//
// Per Discord webhook docs (verified at twilight-http/src/request/channel/
// webhook/execute_webhook.rs):
//   - Default behavior: POST returns 204 No Content with empty body.
//   - With ?wait=true: POST returns 200 OK with the resulting Message
//     object (containing message.id).
// gcit's Sent receipt should be the Discord message id, so operators can
// search Discord for the specific message gcit sent.
//
// twilight-http's ExecuteWebhook builder exposes .wait() (or .wait(bool)
// — verify exact shape against twilight-http/src/request/channel/webhook/
// execute_webhook.rs:.wait at implementation time).

// twilight_model referenced only inside commented assertion shapes; the
// implementer will add the use clause when wiring the asserts.

#[tokio::test]
#[ignore = "requires gcit::discord::webhook::send (not yet implemented)"]
async fn webhook_request_includes_wait_query_param() {
    // wiremock matcher:
    //   method("POST")
    //   path("/api/webhooks/12345/secret")
    //   query_param("wait", "true")
    //   .respond_with(ResponseTemplate::new(200).set_body_json(...message...))
    //
    // The ?wait=true must be in the URL or wiremock won't match.
    //
    // Mutation target: implementer never calls .wait() — the request
    // goes without ?wait=true, Discord returns 204, gcit can't extract
    // a receipt. Test catches via the query_param matcher.
    //
    // SPEC GAP: spec doesn't pin "always use ?wait=true". Could
    // also be conditional on, e.g., dry-run or status emission. Recommend:
    // always use ?wait=true so gcit always returns a meaningful receipt
    // (cost: 200 vs 204 response, ~100 bytes per webhook). flag.
}

#[tokio::test]
#[ignore = "requires gcit trigger --dry-run feature (not yet implemented)"]
async fn webhook_response_message_id_extracted_to_receipt() {
    // wiremock returns 200 with body:
    //   { "id": "1234567890123456789", "channel_id": "...", ... }
    //
    // twilight-http's ExecuteWebhook with .wait(true) returns
    // Result<Option<Response<Message>>, Error> (verify exact return type).
    // The Message struct is twilight_model::channel::Message; field `id`
    // is Id<MessageMarker> wrapping a u64.
    //
    // gcit::discord::webhook::send returns NotifyOutcome::Sent {
    //   receipt: "1234567890123456789".to_string()
    // }.
    //
    // Mutation target: implementer returns Sent with empty receipt OR
    // with channel_id instead of message id. Test catches via assert_eq
    // on the receipt.
}

#[tokio::test]
#[ignore = "requires gcit trigger --dry-run feature (not yet implemented)"]
async fn webhook_204_with_wait_unset_returns_sent_with_marker() {
    // SPEC GAP fallback path: if implementer chooses NOT to use ?wait=true
    // for some reason, the 204 response (no message id) needs a stable
    // receipt value. Recommend: receipt = "no-receipt" or "" with a doc
    // comment explaining why. flag — depends on the resolution of the
    // recommended-always-?wait=true gap above.
    //
    // For now, this test asserts the receipt is non-None and is a stable
    // sentinel value (not a uuid that would change between runs).
}

#[tokio::test]
#[ignore = "requires gcit trigger --dry-run feature (not yet implemented)"]
async fn webhook_response_message_id_lossless_through_u64() {
    // Discord message ids are u64 snowflakes (16-19 decimal digits).
    // twilight-model's Id<MessageMarker> wraps NonZeroU64. The receipt
    // string MUST be the decimal representation of that u64 — NOT a
    // truncated or floating-point approximation.
    //
    // Test: 200 response with id = "1234567890123456789" (max range).
    // Assert receipt == "1234567890123456789" exactly.
    //
    // Mutation target: implementer parses id as f64 (loses precision past
    // 2^53) or as i32 (overflow). serde_json's Number → u64 path is
    // correct; this test guards against any custom parsing.
    //
    // Cross-references twilight-model's Id<...> use of NonZeroU64.
}

#[tokio::test]
#[ignore = "requires gcit trigger --dry-run feature (not yet implemented)"]
async fn webhook_wait_true_does_not_block_when_response_arrives_immediately() {
    // The ?wait=true Discord behavior: server waits until the message is
    // posted in the channel before responding. In practice this is fast
    // (<1s for a healthy webhook). Test pins that no extra client-side
    // delay is introduced.
    //
    // wiremock returns 200 immediately with a synthetic message body.
    // Assert the request completes within a small bound (e.g., 500ms).
    //
    // This is a regression guard against accidentally introducing a
    // sleep or polling pattern in the notifier.
}

#[tokio::test]
#[ignore = "requires gcit trigger --dry-run feature (not yet implemented)"]
async fn dry_run_sets_wait_to_false_or_skips_request_entirely() {
    // `gcit trigger <FLOW> [--dry-run]` prints outbound payloads
    // without making the network call. So the ?wait=true question is
    // moot for dry-run — there IS no request.
    //
    // Pin: in dry-run mode, the discord notifier returns a synthetic
    // outcome (or NotifyOutcome::Sent with receipt = "dry-run") and
    // wiremock receives ZERO POSTs.
    //
    // wiremock expectation: expect(0).
    //
    // Cross-references discord_dry_run.rs (separate file for the
    // payload-printing behavior).
}

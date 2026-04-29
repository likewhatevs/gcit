// Control channel reload rate-limit.
// Reload requests rate-limited to 1 per second across all peers.
// Excess rejected with Error{message: "reload rate-limited"}.
// Each accepted reload logs peer pid and uid.

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore = "requires control-channel server implementation"]
async fn second_reload_within_one_second_is_rejected() {
    // Under tokio::time::pause, advance time deterministically:
    //
    // 1. send Reload request A; assert Response::Ok
    // 2. advance 0.5s
    // 3. send Reload request B; assert Response::Error { message: "reload rate-limited" }
    // 4. advance another 0.6s (total 1.1s since A); now > 1s elapsed
    // 5. send Reload request C; assert Response::Ok
    //
    // Mutation target: the rate-limit window constant (1s). Flip to 100s,
    // assert step (3) still rejects; flip to 0s, assert step (3) is
    // accepted (test fails — catches the regression).
    //
    // start_paused = true puts the runtime under pause from start without
    // an explicit pause() call.
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore = "requires control-channel server implementation"]
async fn rate_limit_is_global_not_per_peer() {
    // "1 per second across all peers". Two distinct
    // connections must share the rate-limit token bucket; A's reload at
    // t=0 must cause B's reload at t=0.5 to be rejected, even though they
    // are different connections.
    //
    // Mutation target: a per-peer rate-limit (HashMap keyed on peer pid)
    // — would silently allow N reloads per second from N peers. This test
    // catches that regression.
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore = "requires control-channel server implementation"]
async fn accepted_reload_logs_peer_pid_and_uid() {
    // "Each accepted reload logs peer pid and uid".
    // Capture via tracing-subscriber test layer; assert exactly one log
    // event per accepted reload with structured fields {peer_pid, peer_uid}.
    //
    // Rejected reloads also log (per spec line 397 "Excess reload requests
    // are rejected"). Pin BOTH log shapes:
    //   - accepted: tracing target=control level=INFO message="reload" peer_pid=N peer_uid=M
    //   - rejected: tracing target=control level=WARN message="reload rate-limited" peer_pid=N peer_uid=M
    //
    // SPEC GAP: spec line 397 says "Each accepted reload logs". Doesn't
    // explicitly say rejected reloads log. But DoS investigations need
    // this. Recommend logging at WARN. Flag for review.
}

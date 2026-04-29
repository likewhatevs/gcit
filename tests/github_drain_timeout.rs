// 5min normal / 30s drain timeout for run correlation.
// 5 minute hard timeout for run appearance during normal operation.
// During a drain (e.g. SIGTERM in flight), the timeout is reduced to
// 30s so shutdown is bounded.
//
// Two distinct timeout regimes:
//   Normal:  300s — run-name correlation tries hard before giving up.
//   Drain:   30s  — daemon is shutting down; bounded wait so SIGTERM
//                   doesn't take >30s past the in-flight dispatch's
//                   correlation attempt.
//
// On timeout: the correlator gives up, emits a last_error (kind=
// "github_run_correlation_timeout"), and the dispatch is considered
// "fired but not tracked". The workflow run may still complete on
// GitHub's side; gcit just won't notify on it.

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore = "requires GitHub correlator drain-timeout implementation"]
async fn normal_correlation_timeout_at_5min() {
    // Configure: not in drain. Wiremock returns no-match indefinitely.
    //
    // Under start_paused, advance time:
    //   t=0:    correlator starts polling
    //   t=4:59: still polling, no timeout yet
    //   t=5:01: timeout fires; correlator returns Err(CorrelationTimeout)
    //
    // Assert wiremock saw multiple poll attempts (at least 5, since
    // backoff 5s -> 10s -> 20s -> 40s -> 80s -> 160s by t=5:00 hits
    // the cap at the 5m mark).
    //
    // Mutation target: the 5min constant. cargo-mutants will flip to
    // 50min or 5s; this test catches.
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore = "requires GitHub correlator drain-timeout implementation"]
async fn drain_correlation_timeout_at_30s() {
    // Same setup but the supervisor signals "draining" (SIGTERM in
    // flight) BEFORE correlation starts. Correlator's effective
    // deadline is 30s.
    //
    // Under start_paused, advance:
    //   t=0:   correlator starts; drain=true
    //   t=29:  no timeout
    //   t=31:  timeout fires
    //
    // Mutation target: failing to honor drain flag (would use 5min and
    // block shutdown).
    //
    // SPEC GAP: how is "drain" signaled to the correlator? Options:
    //   (a) CancellationToken passed into correlator.run()
    //   (b) AtomicBool shared between Supervisor and correlator
    //   (c) Two separate timeout values selected at task spawn time
    // Recommend (a) — already the pattern used for poll cancellation
    // per the cancellation pattern. Pin via test. flag.
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore = "requires GitHub correlator drain-timeout implementation"]
async fn drain_starting_mid_correlation_shortens_remaining_deadline() {
    // Edge: correlation starts at t=0 with normal 5min timeout. At t=20s,
    // SIGTERM lands and the supervisor flips drain=true.
    //
    // Spec doesn't pin behavior. Two options:
    //   (a) restart with 30s budget from t=20s -> deadline t=50s
    //   (b) take min(remaining_normal, 30s) -> remaining = 280s, drain budget = 30s, so deadline t=50s
    // Both equivalent for first switch.
    //
    //   (c) simply set remaining_deadline = min(remaining, 30s); no restart
    //
    // Recommend (c) — preserves elapsed-time semantics. flag.
    //
    // Test pins: total time in correlation function <= 50s when drain
    // flips at t=20s, regardless of which option.
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore = "requires GitHub correlator drain-timeout implementation"]
async fn timeout_emits_last_error_kind_correlation_timeout() {
    // After timeout, last_error must be set with:
    //   { kind: "github_run_correlation_timeout",
    //     message: "...",
    //     at: <RFC3339>,
    //     retry_at: None }
    //
    // The kind string maps to the documented last_error kinds.
    // SPEC GAP: spec lists "github_4xx", "github_5xx", "git_poll_failed",
    // "discord_send_failed", "panic", "transient_network", "etc" — but
    // does NOT enumerate "github_run_correlation_timeout".
    //
    // Recommend adding that kind to the spec's documented list. Without
    // it, operators see an "etc" event with a custom kind string they
    // can't grep documentation for. flag.
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore = "requires GitHub correlator drain-timeout implementation"]
async fn correlation_timeout_does_not_stall_other_flows() {
    // Two flows, one stuck in correlation timeout, one polling normally.
    // Per-flow failure isolation. Assert flow B's
    // poll/dispatch/correlate cycle continues normally while flow A is
    // in its 5min wait.
    //
    // Mutation target: a single-task correlator shared across flows
    // (would block all flows on one's timeout). Test catches.
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore = "requires GitHub correlator drain-timeout implementation"]
async fn timeout_constants_pinned_via_assert_eq() {
    // Pin 5min and 30s as named constants:
    //   gcit::github::correlator::NORMAL_TIMEOUT == Duration::from_secs(300)
    //   gcit::github::correlator::DRAIN_TIMEOUT == Duration::from_secs(30)
    //
    // All time/limit constants get the same pinning treatment to
    // catch silent change.
    //
    // SPEC GAP: spec doesn't name the constants. Recommend exact names
    // above. flag.
}

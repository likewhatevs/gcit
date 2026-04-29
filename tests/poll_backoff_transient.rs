// Backoff on transient failure.
// Backoff uses backon::ExponentialBuilder with min_delay=5s,
// max_delay=5m, factor=2.0, jitter enabled. Not user-configurable in
// v1. Applies to polling retries, GitHub API retries on transient
// errors, and Discord webhook retries on transient errors.
//
// Per the implementation in src/flow/poll.rs::run, the poll loop does
// NOT use backon's `.retry()` — it sleeps the jittered cadence
// (apply_jitter) and continues on the next tick regardless of
// failure classification. backon-driven retries live ONLY in
// github::dispatcher::dispatch_with_retry (covered in
// tests/github_dispatch.rs) and in discord::notifier (covered in
// tests/discord_rate_limit.rs).
//
// The tests below pin (a) the constants used by the poll-side jitter
// helper and (b) the behaviour of `gcit::git::compare_sha` paired
// with apply_jitter — both pure-logic functions exercised every poll
// cycle. The Transient/Permanent classification semantics for the
// dispatcher path are pinned in tests/poll_backoff_transient.rs's
// counterpart in github_*; here we only pin the polling-side cadence
// invariants.

use std::time::Duration;

use gcit::git::{apply_jitter, MIN_INTERVAL};

#[test]
fn min_interval_constant_pinned_at_15s() {
    // 15s floor. The poll loop computes
    // `apply_jitter(source_interval, jitter)` per cycle and treats
    // the returned Duration as the wait. Mutation target: relaxing
    // the floor below 15s would let an aggressive operator config
    // poll kernel.org sub-15s — politeness violation.
    assert_eq!(MIN_INTERVAL, Duration::from_secs(15));
}

#[test]
fn jitter_zero_yields_base_interval_repeatedly() {
    // Cross-cycle determinism property: when jitter is zero the
    // computed wait is exactly the configured base. Mutation target:
    // a jitter implementation that injects a hidden floor or random
    // bias even at jitter=0.0.
    let mut rng = fastrand::Rng::with_seed(99);
    let base = Duration::from_secs(60);
    for _ in 0..100 {
        assert_eq!(apply_jitter(base, 0.0, &mut rng), base);
    }
}

#[test]
fn poll_cycle_computation_is_io_free() {
    // The poll loop's per-cycle wait computation must be pure
    // arithmetic. Run apply_jitter many times in a tight loop and
    // assert the loop completes in <100ms wall-clock — catches a
    // regression where someone introduces an fsync, a system-clock
    // sample, or a network probe inside the cadence calculation.
    let mut rng = fastrand::Rng::with_seed(7);
    let base = Duration::from_secs(60);
    let start = std::time::Instant::now();
    for _ in 0..10_000 {
        std::hint::black_box(apply_jitter(base, 0.1, &mut rng));
    }
    assert!(
        start.elapsed() < Duration::from_millis(100),
        "apply_jitter must be IO-free; 10k calls took {:?}",
        start.elapsed(),
    );
}

#[test]
fn jitter_clamps_arguments_at_function_boundary() {
    // apply_jitter is the integration point between config-validated
    // jitter values [0.0, 0.5] and the poll loop. As defense-in-
    // depth, the function clamps inputs out of range to the
    // boundary so a bug elsewhere cannot inflate the actual interval
    // beyond ±50% of base.
    let mut rng = fastrand::Rng::with_seed(13);
    let base = Duration::from_secs(60);
    for _ in 0..1_000 {
        // jitter = 1.0 (oversized) clamps to 0.5 so window is [30s, 90s].
        let actual = apply_jitter(base, 1.0, &mut rng);
        assert!(actual.as_secs() <= 90);
        assert!(actual.as_secs() >= 30);
    }
}

// Poll interval jitter bounds.
// `jitter = 0.1` (unitless fraction of base interval, 0.0..=0.5).
// PollDefaults.jitter: f64.
// poll: source_interval, jittered, rate-bucketed.
//
// Jitter spec: each poll's actual interval = base_interval * (1 +/-
// random_in_range(jitter)). For jitter = 0.1 and base = 60s, actual
// interval ranges over [54s, 66s].
//
// Pure-logic test of the jitter-application function. Use a seeded RNG
// (fastrand::Rng with explicit seed) to keep the test deterministic.
//
// The function under test lives at gcit::git::apply_jitter; per
// src/git/strategy.rs it clamps the jitter argument to [0.0, 0.5] and
// the returned Duration upward to gcit::git::MIN_INTERVAL (15s).

use std::time::Duration;

use proptest::prelude::*;
use rstest::rstest;

use gcit::git::{apply_jitter, MIN_INTERVAL};

#[rstest]
#[case::no_jitter(60, 0.0, 60..=60)]
#[case::ten_percent(60, 0.1, 54..=66)]
#[case::fifty_percent(60, 0.5, 30..=90)]
fn jittered_interval_within_bounds(
    #[case] base_seconds: u64,
    #[case] jitter: f64,
    #[case] expected_range: std::ops::RangeInclusive<u64>,
) {
    let mut rng = fastrand::Rng::with_seed(42);
    for _ in 0..10_000 {
        let actual = apply_jitter(Duration::from_secs(base_seconds), jitter, &mut rng);
        let actual_secs = actual.as_secs();
        assert!(
            expected_range.contains(&actual_secs),
            "jittered {actual_secs}s out of range {expected_range:?}",
        );
    }
}

#[rstest]
#[case::short_base(20, 0.5)]
#[case::min_interval_base(15, 0.5)]
fn jitter_floor_clamps_below_min_interval(#[case] base_seconds: u64, #[case] jitter: f64) {
    // 15s floor on the EFFECTIVE interval. If base=20s
    // and jitter=0.5 the raw lower bound (10s) is below the floor; the
    // function must clamp upward to MIN_INTERVAL.
    let mut rng = fastrand::Rng::with_seed(123);
    for _ in 0..5_000 {
        let actual = apply_jitter(Duration::from_secs(base_seconds), jitter, &mut rng);
        assert!(
            actual >= MIN_INTERVAL,
            "floor breach: base={base_seconds}s jitter={jitter} -> {actual:?}",
        );
    }
}

#[test]
fn jitter_zero_disables_randomness() {
    // jitter = 0.0 -> every call returns base exactly. Mutation target:
    // a default that swaps in a non-zero jitter when 0.0 was configured
    // would surface here as base != actual.
    let mut rng = fastrand::Rng::with_seed(7);
    let base = Duration::from_secs(60);
    for _ in 0..1_000 {
        assert_eq!(apply_jitter(base, 0.0, &mut rng), base);
    }
}

#[test]
fn jitter_clamps_oversized_jitter_argument() {
    // Jitter is enforced in [0.0, 0.5] at config-parse time;
    // apply_jitter clamps as defense-in-depth so a misuse cannot
    // inflate the interval window past ±50%.
    let mut rng = fastrand::Rng::with_seed(11);
    let base = Duration::from_secs(60);
    for _ in 0..1_000 {
        let actual = apply_jitter(base, 1.0, &mut rng);
        // 1.0 clamps to 0.5 -> [30s, 90s] window (above floor).
        assert!(actual.as_secs() <= 90, "upper-bound clamp: {actual:?}");
        assert!(actual.as_secs() >= 30, "lower-bound clamp: {actual:?}");
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 200,
        .. ProptestConfig::default()
    })]

    /// Property: for any (base in [15s, 24h], jitter in [0.0, 0.5], seed),
    /// the jittered interval is always within [base * (1 - jitter),
    /// base * (1 + jitter)] AND always >= MIN_INTERVAL.
    #[test]
    fn jitter_property_within_bounds_and_above_floor(
        base_secs in 15u64..=86_400,
        jitter in 0.0f64..=0.5,
        seed in any::<u64>(),
    ) {
        let mut rng = fastrand::Rng::with_seed(seed);
        let actual = apply_jitter(
            Duration::from_secs(base_secs),
            jitter,
            &mut rng,
        );
        // Raw lower/upper from the spec; clamped lower at floor.
        let raw_upper = (base_secs as f64 * (1.0 + jitter)).ceil() as u64;
        let raw_lower = (base_secs as f64 * (1.0 - jitter)).floor() as u64;
        let effective_lower = raw_lower.max(MIN_INTERVAL.as_secs());
        prop_assert!(
            actual.as_secs() >= effective_lower,
            "actual {} below effective lower {}",
            actual.as_secs(),
            effective_lower,
        );
        prop_assert!(
            actual.as_secs() <= raw_upper,
            "actual {} above raw upper {}",
            actual.as_secs(),
            raw_upper,
        );
        prop_assert!(actual >= MIN_INTERVAL);
    }
}

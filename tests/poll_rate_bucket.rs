// Per-credential RateBucket — minimum-interval gate.
// Spawn rate-limit poller per credential.
// poll: source_interval, jittered, rate-bucketed.
// HTTP errors are scoped per-credential (RateBucket key); exhausted
// bucket on credential A does not affect credential B.
//
// `gcit::git::RateBucket` is the simple polling-side gate that enforces a
// minimum spacing between successful acquires sharing the same credential.
// It does NOT track remaining/reset counts — that's `RateLimitState` over
// in src/github/rate_limit.rs (covered in tests/github_rate_limit_poller.rs).
// This file focuses on the poll-side spacing guarantee:
//   - first acquire is immediate
//   - second acquire blocks until min_interval has elapsed since the first
//   - distinct buckets are independent
//   - try_acquire returns retry_after when not yet open

use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use gcit::git::RateBucket;

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn first_acquire_is_immediate() {
    let b = RateBucket::new(Duration::from_secs(1));
    let t0 = Instant::now();
    b.acquire().await;
    assert_eq!(Instant::now(), t0, "first acquire must be immediate");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn second_acquire_waits_min_interval() {
    let b = RateBucket::new(Duration::from_secs(1));
    let t0 = Instant::now();
    b.acquire().await;
    b.acquire().await;
    let elapsed = Instant::now().duration_since(t0);
    assert!(
        elapsed >= Duration::from_secs(1),
        "second acquire must wait min_interval; elapsed={elapsed:?}",
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn try_acquire_first_call_succeeds() {
    let b = RateBucket::new(Duration::from_secs(1));
    b.try_acquire()
        .await
        .expect("first try_acquire must succeed");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn try_acquire_within_window_returns_retry_after() {
    let b = RateBucket::new(Duration::from_secs(1));
    b.try_acquire().await.unwrap();
    let err = b
        .try_acquire()
        .await
        .expect_err("immediate retry must reject");
    assert!(
        err > Duration::ZERO && err <= Duration::from_secs(1),
        "retry_after expected in (0,1s]: {err:?}",
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn try_acquire_after_window_succeeds() {
    let b = RateBucket::new(Duration::from_secs(1));
    b.try_acquire().await.unwrap();
    tokio::time::advance(Duration::from_millis(1100)).await;
    b.try_acquire()
        .await
        .expect("after window try_acquire must succeed");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn buckets_are_isolated_per_credential() {
    // Exhausted bucket on credential A must not affect
    // credential B. Two distinct RateBucket instances share no state.
    let a = RateBucket::new(Duration::from_secs(60));
    let b = RateBucket::new(Duration::from_secs(60));
    a.try_acquire().await.unwrap();
    // Bucket a is now in its 60s window; bucket b is independent.
    b.try_acquire()
        .await
        .expect("bucket b must not see bucket a's state");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn min_interval_constructor_argument_round_trips() {
    let dur = Duration::from_millis(1500);
    let b = RateBucket::new(dur);
    assert_eq!(b.min_interval(), dur);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn concurrent_acquires_serialize_via_reservation() {
    // Three callers race into acquire() at t=0. The bucket reserves
    // slots in-lock so caller 1 wakes at t=0, caller 2 at t=1s,
    // caller 3 at t=2s. Without per-call reservation all three would
    // see last_acquire=None, compute sleep_until=now, and stampede.
    let b = Arc::new(RateBucket::new(Duration::from_secs(1)));
    let t0 = Instant::now();
    let h1 = tokio::spawn({
        let b = Arc::clone(&b);
        async move {
            b.acquire().await;
            Instant::now()
        }
    });
    let h2 = tokio::spawn({
        let b = Arc::clone(&b);
        async move {
            b.acquire().await;
            Instant::now()
        }
    });
    let h3 = tokio::spawn({
        let b = Arc::clone(&b);
        async move {
            b.acquire().await;
            Instant::now()
        }
    });
    let (a, c, d) = tokio::join!(h1, h2, h3);
    let mut times = [
        a.unwrap().duration_since(t0),
        c.unwrap().duration_since(t0),
        d.unwrap().duration_since(t0),
    ];
    times.sort();
    assert!(
        times[0] < Duration::from_millis(100),
        "first caller must be immediate: {:?}",
        times[0],
    );
    assert!(
        times[1] >= Duration::from_secs(1),
        "second caller must wait one full interval: {:?}",
        times[1],
    );
    assert!(
        times[2] >= Duration::from_secs(2),
        "third caller must wait two full intervals: {:?}",
        times[2],
    );
}

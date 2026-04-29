// Per-credential rate bucket: simple token-free min-interval gate.
//
// Behavior:
//   - Spawned per-credential so polling cadence and HTTP errors are
//     scoped by credential id.
//   - Enforces a minimum interval between requests sharing the same
//     credential id. Multiple flows that reference the same credential
//     serialize their HTTP calls behind one bucket. Exhausted bucket
//     on credential A does not affect credential B.
//
// What this is and isn't:
//   - This is NOT the GitHub /rate_limit poller. That richer mechanism
//     (refresh remaining/reset, X-RateLimit-* opportunistic updates,
//     5xx classification) lives in `src/github/rate_limit.rs`. The
//     bucket here is the polling-side guard so an aggressive
//     `flow.poll.source_interval` can't double-count its credential's
//     quota across N flows.
//   - The bucket has no internal token counter — its only state is
//     `Option<Instant>` (the timestamp of the last accepted acquire).
//
// API:
//   - `RateBucket::new(min_interval)` constructs an empty bucket.
//   - `acquire().await` returns once enough time has passed since the
//     last successful acquire. The first call is always immediate.
//   - `try_acquire(now)` is the synchronous variant: returns `Ok(())`
//     and updates the timestamp when permitted, or
//     `Err(retry_after: Duration)` when the next slot is in the
//     future. Used by tests and by callers that prefer a hint over
//     await.
//
// Concurrency: the inner state is a `tokio::sync::Mutex<Option<Instant>>`
// so awaits across the lock are safe. An async lock is overkill for a
// pure timestamp update; the rationale is that `acquire` does
// `tokio::time::sleep` while holding nothing — and the lock protects
// the *update* of the last-accepted timestamp on the way out.

use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::Instant;

/// A per-credential minimum-interval gate.
///
/// Identity-by-credential is the caller's responsibility: construct
/// one `RateBucket` per credential id and route every flow that uses
/// that credential through it. The supervisor maintains the
/// `BTreeMap<CredentialId, Arc<RateBucket>>` keyed lookup.
#[derive(Debug)]
pub struct RateBucket {
    min_interval: Duration,
    last: Mutex<Option<Instant>>,
}

impl RateBucket {
    /// Construct a new bucket with the given minimum spacing between
    /// successful `acquire` calls.
    pub fn new(min_interval: Duration) -> Self {
        Self {
            min_interval,
            last: Mutex::new(None),
        }
    }

    /// The minimum-interval the bucket was constructed with.
    pub fn min_interval(&self) -> Duration {
        self.min_interval
    }

    /// Block until enough time has passed since the last successful
    /// acquire. The first call always returns immediately. Subsequent
    /// calls sleep for the remainder of `min_interval` measured from
    /// the previous acquire.
    ///
    /// Concurrency contract: the slot is RESERVED inside the critical
    /// section before any sleep happens. A second
    /// caller that races into `acquire()` while the first is still
    /// sleeping observes the reserved future timestamp under the
    /// lock and computes a target that is at least `min_interval`
    /// AFTER the first caller's wake — not after `now`. Without this
    /// reservation, N concurrent callers could all read the same
    /// last-acquire, all compute the same sleep_until, and all wake
    /// simultaneously, defeating the gate.
    ///
    /// On wake, the slot is updated to `max(reserved, Instant::now())`
    /// so a long sleep does not over-tighten the next caller's
    /// window: if the runtime overshoots its sleep deadline, the
    /// recorded last-acquire is the actual wake time and the next
    /// caller's target is `wake + min_interval`, not
    /// `reserved + min_interval` (which could be in the past).
    pub async fn acquire(&self) {
        let reserved_at = {
            let mut guard = self.last.lock().await;
            let now = Instant::now();
            let target = match *guard {
                None => now,
                Some(prev) => {
                    // `prev` may be in the future when a previous
                    // caller has reserved a slot but not yet woken.
                    // The target is the later of (prev + interval, now).
                    let earliest = prev + self.min_interval;
                    if earliest > now {
                        earliest
                    } else {
                        now
                    }
                }
            };
            *guard = Some(target);
            target
        };
        // Sleep WITHOUT holding the lock so other callers can queue.
        // The reserved timestamp keeps them honest — each one sees
        // the latest reservation and chains off it.
        tokio::time::sleep_until(reserved_at).await;
        let mut guard = self.last.lock().await;
        let now = Instant::now();
        // If the runtime overshot the sleep deadline, the actual wake
        // time is later than `reserved_at`. Bump the slot to the max
        // so the next caller's target is anchored at the real wake
        // time. If we wake on or before the reservation, leave the
        // slot at `reserved_at` — subsequent callers already chained
        // off it.
        if let Some(prev) = *guard {
            if now > prev {
                *guard = Some(now);
            }
        }
    }

    /// Synchronous variant: returns `Ok(())` immediately when the
    /// bucket's window has opened, or `Err(retry_after: Duration)`
    /// when caller would have to wait.
    ///
    /// Useful for non-blocking decision sites (e.g., `gcit trigger
    /// --dry-run` deciding whether to rate-bucket the synthetic
    /// request).
    pub async fn try_acquire(&self) -> Result<(), Duration> {
        let mut guard = self.last.lock().await;
        let now = Instant::now();
        match *guard {
            None => {
                *guard = Some(now);
                Ok(())
            }
            Some(prev) => {
                let target = prev + self.min_interval;
                if target > now {
                    Err(target.duration_since(now))
                } else {
                    *guard = Some(now);
                    Ok(())
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    async fn try_acquire_first_succeeds() {
        let b = RateBucket::new(Duration::from_secs(1));
        b.try_acquire().await.expect("first try_acquire");
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
            "retry_after in (0,1s]: {err:?}",
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn try_acquire_after_window_succeeds() {
        let b = RateBucket::new(Duration::from_secs(1));
        b.try_acquire().await.unwrap();
        tokio::time::advance(Duration::from_millis(1100)).await;
        b.try_acquire()
            .await
            .expect("after window, try_acquire must succeed");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn buckets_isolated_per_credential() {
        // Exhausted bucket on credential A does not affect credential
        // B. Two distinct buckets share no state.
        let a = RateBucket::new(Duration::from_secs(60));
        let b = RateBucket::new(Duration::from_secs(60));
        a.try_acquire().await.unwrap();
        // Bucket A is now in its window; bucket B is independent.
        b.try_acquire()
            .await
            .expect("bucket B must not see A's state");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn concurrent_acquires_serialize() {
        // A regression test for the lock-held-then-dropped bypass.
        // Three callers race into acquire() at t=0. Without the
        // in-lock reservation, all three would see last=None, compute
        // sleep_until=now, return immediately, and complete at t=0.
        //
        // With the fix, caller 1 reserves t=0, caller 2 reserves t=1s,
        // caller 3 reserves t=2s. All three serialize.
        use std::sync::Arc;
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
        // First caller wakes immediately; second after >= 1s; third
        // after >= 2s. Use >= so jitter / runtime overshoot doesn't
        // flake the assertion.
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
}

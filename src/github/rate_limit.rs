// GitHub /rate_limit poller — per-credential snapshot of the core
// quota with periodic refresh and opportunistic header updates.
//
// Behaviour:
//   - 60s polling cadence + immediate first call on spawn
//   - extracts core (not search/graphql) into bucket
//   - pessimistic clamp (remaining=0) on first-poll failure so
//     dispatch is gated until a real observation lands
//   - opportunistic header-driven updates from non-/rate_limit
//     responses (see RateLimitState::observe_headers)
//   - /rate_limit request bypasses the bucket gate (otherwise
//     exhaustion is permanent — circular dependency)
//   - per-credential isolation: an exhausted bucket on one credential
//     does not impede another.
//
// Design:
//
//   `RateLimitState` is the in-memory snapshot one credential's
//   bucket carries. The dispatcher / monitor / correlator paths
//   consult `should_defer()` before each API call to ask "do I
//   have quota?". They also call `observe_headers()` after each
//   response to feed `X-RateLimit-Remaining` / `X-RateLimit-Reset`
//   updates back into the bucket — opportunistic, every response
//   from any GitHub endpoint carries them so the bucket stays
//   fresher than the 60s poll cadence.
//
//   `poll_loop()` is the long-running task that calls /rate_limit
//   on cadence and refreshes the snapshot. It is spawned once per
//   credential by the supervisor; it runs until cancelled.
//
// Cancellation: `poll_loop` selects on a `CancellationToken` so
// SIGTERM/SIGINT cleanly shuts the poller down.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use http::HeaderMap;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, warn};

use super::client::Client;

/// Polling cadence for /rate_limit: every 60s. Constant rather than
/// configurable so the operator-facing timing docs stay aligned with
/// the implementation.
pub const POLL_INTERVAL: Duration = Duration::from_secs(60);

/// Default pessimistic reset window when first poll fails. The
/// bucket clamps remaining=0 with reset=now+POLL_INTERVAL so the
/// next poll cycle has a chance to recover before any dispatch
/// fires. Per
/// tests/github_rate_limit_poller.rs::poller_first_failure_pessimistic_clamp_to_zero.
pub const FIRST_FAILURE_RESET_WINDOW: Duration = POLL_INTERVAL;

/// One credential's rate-limit snapshot. Cheap to clone via `Arc`.
/// Internally, the snapshot lives behind a `tokio::sync::RwLock`
/// so opportunistic header observations from many concurrent API
/// calls can update without serialising the read path.
#[derive(Debug, Clone)]
pub struct RateLimitState {
    inner: Arc<RwLock<Snapshot>>,
}

#[derive(Debug, Clone, Copy)]
struct Snapshot {
    /// `None` = no observation yet. Pessimistic semantics: callers
    /// must consult `should_defer` which treats `None` as
    /// "definitely defer until a real observation lands".
    remaining: Option<u64>,
    /// The hour-window's quota cap (e.g. 5000 for an authenticated
    /// PAT). Carried for log fidelity and for callers that want to
    /// surface "X / Y remaining" in `gcit status`.
    limit: Option<u64>,
    /// Wall-clock instant the current window resets. `None` only
    /// before the first observation.
    reset: Option<DateTime<Utc>>,
    /// Wall-clock of the most recent observation. Used to age out
    /// stale snapshots on a long-running daemon (a snapshot older
    /// than 2 * POLL_INTERVAL is treated as unobserved by
    /// `should_defer`).
    observed_at: Option<DateTime<Utc>>,
}

impl Default for RateLimitState {
    fn default() -> Self {
        Self::new()
    }
}

impl RateLimitState {
    /// Construct a fresh snapshot with no observations. Per
    /// `should_defer`, this state defers all dispatches until the
    /// first observation lands. The supervisor calls this at flow
    /// startup; the first poll cycle (immediate per cadence below)
    /// will populate it within seconds.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Snapshot {
                remaining: None,
                limit: None,
                reset: None,
                observed_at: None,
            })),
        }
    }

    /// Read the current snapshot. Returns the structured fields;
    /// `gcit status --format json` surfaces these.
    pub async fn snapshot(&self) -> RateLimitSnapshot {
        let s = self.inner.read().await;
        RateLimitSnapshot {
            remaining: s.remaining,
            limit: s.limit,
            reset: s.reset,
            observed_at: s.observed_at,
        }
    }

    /// Update the snapshot from an explicit /rate_limit poll. Called
    /// by `poll_loop` once per cadence tick.
    pub async fn observe_full(&self, limit: u64, remaining: u64, reset: DateTime<Utc>) {
        let now = Utc::now();
        let mut s = self.inner.write().await;
        s.limit = Some(limit);
        s.remaining = Some(remaining);
        s.reset = Some(reset);
        s.observed_at = Some(now);
        debug!(
            limit,
            remaining,
            reset_in_seconds = (reset - now).num_seconds(),
            "rate_limit observation",
        );
    }

    /// Update the snapshot opportunistically from response headers.
    /// Every API response carries `X-RateLimit-Remaining` /
    /// `X-RateLimit-Reset` (the design intent matched by
    /// `tests/github_rate_limit_poller.rs`'s
    /// `pollers_per_credential_run_independently`). We extract what
    /// we can; if a header is missing or non-numeric, that field
    /// is left untouched rather than crashing the call site.
    pub async fn observe_headers(&self, headers: &HeaderMap) {
        let remaining = parse_u64_header(headers, "x-ratelimit-remaining");
        let reset = parse_reset_header(headers);
        let limit = parse_u64_header(headers, "x-ratelimit-limit");
        if remaining.is_none() && reset.is_none() && limit.is_none() {
            return;
        }
        let now = Utc::now();
        let mut s = self.inner.write().await;
        if let Some(remaining) = remaining {
            s.remaining = Some(remaining);
        }
        if let Some(reset) = reset {
            s.reset = Some(reset);
        }
        if let Some(limit) = limit {
            s.limit = Some(limit);
        }
        s.observed_at = Some(now);
    }

    /// Pessimistically clamp the snapshot when a first-ever poll
    /// fails (network down at startup). remaining=0 with
    /// reset=now+POLL_INTERVAL gates dispatch until the next
    /// successful poll. Per
    /// tests/github_rate_limit_poller.rs::poller_first_failure_pessimistic_clamp_to_zero.
    pub async fn clamp_pessimistic(&self) {
        let now = Utc::now();
        let mut s = self.inner.write().await;
        if s.observed_at.is_some() {
            // We already have a real observation; failure of a
            // subsequent poll keeps the prior snapshot alive (per
            // tests/github_rate_limit_poller.rs::
            // poller_failure_retains_last_known_bucket_state).
            return;
        }
        s.remaining = Some(0);
        s.limit = Some(0);
        s.reset = Some(now + chrono::Duration::from_std(FIRST_FAILURE_RESET_WINDOW).unwrap());
        s.observed_at = Some(now);
        warn!(
            "first /rate_limit poll failed; pessimistically clamping bucket to 0 until next poll",
        );
    }

    /// Should the caller defer this request? Returns
    /// `Some(retry_after)` when deferral is required, otherwise
    /// `None` (request permitted immediately).
    ///
    /// Decisions:
    ///   * No observation yet -> defer until next poll lands.
    ///   * Observation older than 2 * POLL_INTERVAL -> stale; treat
    ///     as no observation (defer).
    ///   * remaining > 0 -> proceed.
    ///   * remaining == 0 and reset > now -> defer until reset.
    ///   * remaining == 0 and reset <= now -> a fresh poll is due;
    ///     return zero so the caller proceeds (the next response's
    ///     headers will refresh the snapshot).
    pub async fn should_defer(&self) -> Option<Duration> {
        let now = Utc::now();
        let s = self.inner.read().await;
        let observed_at = match s.observed_at {
            Some(t) => t,
            None => {
                // No observation yet; defer for the polling
                // cadence so the first poll has a chance to
                // populate the snapshot.
                return Some(POLL_INTERVAL);
            }
        };
        if (now - observed_at) > chrono::Duration::from_std(POLL_INTERVAL * 2).unwrap() {
            // Stale snapshot — the poller hasn't run in a while,
            // treat as no observation and defer.
            return Some(POLL_INTERVAL);
        }
        match s.remaining {
            Some(0) => match s.reset {
                Some(reset) if reset > now => (reset - now).to_std().ok(),
                _ => None,
            },
            Some(_) => None,
            None => Some(POLL_INTERVAL),
        }
    }
}

/// Plain-data view of the snapshot. Returned by `RateLimitState::
/// snapshot` so callers can pass it across await boundaries without
/// holding the lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimitSnapshot {
    pub remaining: Option<u64>,
    pub limit: Option<u64>,
    pub reset: Option<DateTime<Utc>>,
    pub observed_at: Option<DateTime<Utc>>,
}

fn parse_u64_header(headers: &HeaderMap, name: &str) -> Option<u64> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
}

fn parse_reset_header(headers: &HeaderMap) -> Option<DateTime<Utc>> {
    let epoch = parse_u64_header(headers, "x-ratelimit-reset")?;
    Utc.timestamp_opt(epoch as i64, 0).single()
}

/// Long-running task that polls `/rate_limit` on `POLL_INTERVAL`
/// and refreshes `state`. The first call is immediate (per
/// tests/github_rate_limit_poller.rs::poller_calls_rate_limit_endpoint_every_60s
/// "first call at t=0 vs t=60. Recommend immediate first call so
/// the bucket has fresh state on daemon start.").
///
/// /rate_limit itself does NOT consume quota (per
/// tests/github_rate_limit_poller.rs::
/// poller_request_does_not_consume_its_own_bucket_quota), so this
/// loop bypasses `should_defer` — otherwise an exhausted bucket
/// would be permanent.
///
/// Failures: an Err return from the API call is logged and the
/// bucket is left as-is (or pessimistically clamped on first
/// failure). The loop continues until cancellation.
#[instrument(skip(client, state, cancel))]
pub async fn poll_loop(client: Client, state: RateLimitState, cancel: CancellationToken) {
    loop {
        // Issue the poll. If the bucket has never seen a real
        // observation and the call fails, clamp pessimistic.
        let result = tokio::time::timeout(client.request_timeout(), async {
            client.octocrab().ratelimit().get().await
        })
        .await;
        match result {
            Ok(Ok(rl)) => {
                let core = rl.resources.core;
                let reset = Utc.timestamp_opt(core.reset as i64, 0).single();
                if let Some(reset) = reset {
                    state
                        .observe_full(core.limit as u64, core.remaining as u64, reset)
                        .await;
                } else {
                    warn!(
                        reset_epoch = core.reset,
                        "rate_limit response had unparseable reset epoch; ignoring poll cycle",
                    );
                }
            }
            Ok(Err(err)) => {
                warn!(error = %err, "rate_limit poll failed");
                state.clamp_pessimistic().await;
            }
            Err(_elapsed) => {
                warn!(
                    timeout_secs = client.request_timeout().as_secs(),
                    "rate_limit poll timed out",
                );
                state.clamp_pessimistic().await;
            }
        }

        // Wait for the next cadence tick OR cancellation, whichever
        // happens first.
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(POLL_INTERVAL) => {}
        }
    }
    debug!("rate_limit poll_loop cancelled, exiting");
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[tokio::test]
    async fn fresh_state_has_no_snapshot() {
        let s = RateLimitState::new();
        let snap = s.snapshot().await;
        assert_eq!(snap.remaining, None);
        assert_eq!(snap.limit, None);
        assert_eq!(snap.reset, None);
        assert_eq!(snap.observed_at, None);
    }

    #[tokio::test]
    async fn fresh_state_defers_until_first_poll() {
        let s = RateLimitState::new();
        let d = s.should_defer().await;
        assert_eq!(d, Some(POLL_INTERVAL));
    }

    #[tokio::test]
    async fn observe_full_updates_snapshot() {
        let s = RateLimitState::new();
        let reset = Utc::now() + chrono::Duration::seconds(120);
        s.observe_full(5000, 4321, reset).await;
        let snap = s.snapshot().await;
        assert_eq!(snap.remaining, Some(4321));
        assert_eq!(snap.limit, Some(5000));
        assert_eq!(snap.reset, Some(reset));
        assert!(snap.observed_at.is_some());
    }

    #[tokio::test]
    async fn observe_full_remaining_nonzero_does_not_defer() {
        let s = RateLimitState::new();
        let reset = Utc::now() + chrono::Duration::seconds(120);
        s.observe_full(5000, 4321, reset).await;
        assert_eq!(s.should_defer().await, None);
    }

    #[tokio::test]
    async fn observe_full_remaining_zero_defers_until_reset() {
        let s = RateLimitState::new();
        let reset = Utc::now() + chrono::Duration::seconds(45);
        s.observe_full(5000, 0, reset).await;
        let d = s.should_defer().await.expect("must defer");
        assert!(
            d >= Duration::from_secs(43) && d <= Duration::from_secs(46),
            "defer ~= 45s; got {d:?}",
        );
    }

    #[tokio::test]
    async fn observe_full_remaining_zero_with_past_reset_does_not_defer() {
        // Reset is in the past — caller proceeds and the next
        // response's headers will refresh the snapshot.
        let s = RateLimitState::new();
        let reset = Utc::now() - chrono::Duration::seconds(10);
        s.observe_full(5000, 0, reset).await;
        assert_eq!(s.should_defer().await, None);
    }

    #[tokio::test]
    async fn observe_headers_extracts_all_three_fields() {
        let s = RateLimitState::new();
        let mut headers = HeaderMap::new();
        let reset_epoch = (Utc::now() + chrono::Duration::seconds(60)).timestamp();
        headers.insert("x-ratelimit-remaining", HeaderValue::from_static("1234"));
        headers.insert("x-ratelimit-limit", HeaderValue::from_static("5000"));
        headers.insert(
            "x-ratelimit-reset",
            HeaderValue::from_str(&reset_epoch.to_string()).unwrap(),
        );
        s.observe_headers(&headers).await;
        let snap = s.snapshot().await;
        assert_eq!(snap.remaining, Some(1234));
        assert_eq!(snap.limit, Some(5000));
        let reset = snap.reset.expect("reset set");
        let now = Utc::now();
        let drift = (reset - now).num_seconds();
        assert!(drift > 50 && drift < 70, "reset ~= now+60s; drift={drift}s",);
    }

    #[tokio::test]
    async fn observe_headers_missing_all_is_noop() {
        let s = RateLimitState::new();
        let headers = HeaderMap::new();
        s.observe_headers(&headers).await;
        let snap = s.snapshot().await;
        assert_eq!(snap.remaining, None);
        assert_eq!(snap.observed_at, None);
    }

    #[tokio::test]
    async fn observe_headers_partial_updates_only_present_fields() {
        let s = RateLimitState::new();
        // Seed via observe_full so we can assert the partial header
        // observation only overrides what it carries.
        let reset = Utc::now() + chrono::Duration::seconds(120);
        s.observe_full(5000, 4321, reset).await;

        let mut headers = HeaderMap::new();
        headers.insert("x-ratelimit-remaining", HeaderValue::from_static("100"));
        s.observe_headers(&headers).await;

        let snap = s.snapshot().await;
        assert_eq!(snap.remaining, Some(100));
        assert_eq!(snap.limit, Some(5000), "limit untouched");
        assert_eq!(snap.reset, Some(reset), "reset untouched");
    }

    #[tokio::test]
    async fn observe_headers_non_numeric_is_ignored() {
        let s = RateLimitState::new();
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-ratelimit-remaining",
            HeaderValue::from_static("not-a-number"),
        );
        s.observe_headers(&headers).await;
        let snap = s.snapshot().await;
        // Garbage header is ignored; observed_at stays None
        // because no field successfully parsed.
        assert_eq!(snap.remaining, None);
        assert_eq!(snap.observed_at, None);
    }

    #[tokio::test]
    async fn clamp_pessimistic_on_first_failure() {
        let s = RateLimitState::new();
        s.clamp_pessimistic().await;
        let snap = s.snapshot().await;
        assert_eq!(snap.remaining, Some(0));
        assert_eq!(snap.limit, Some(0));
        let reset = snap.reset.expect("reset set");
        let now = Utc::now();
        let in_secs = (reset - now).num_seconds();
        assert!(
            (58..=62).contains(&in_secs),
            "reset ~= now+POLL_INTERVAL; got {in_secs}s",
        );
        assert!(snap.observed_at.is_some(), "observed_at must update");
    }

    #[tokio::test]
    async fn clamp_pessimistic_after_real_observation_is_noop() {
        // Per tests/github_rate_limit_poller.rs::
        // poller_failure_retains_last_known_bucket_state: a
        // subsequent poll failure must NOT erase the last good
        // observation.
        let s = RateLimitState::new();
        let reset = Utc::now() + chrono::Duration::seconds(180);
        s.observe_full(5000, 1234, reset).await;
        s.clamp_pessimistic().await;
        let snap = s.snapshot().await;
        assert_eq!(
            snap.remaining,
            Some(1234),
            "real observation must survive a subsequent poll failure",
        );
        assert_eq!(snap.limit, Some(5000));
        assert_eq!(snap.reset, Some(reset));
    }

    #[tokio::test]
    async fn parse_u64_header_handles_present_absent_and_garbage() {
        let mut h = HeaderMap::new();
        assert_eq!(parse_u64_header(&h, "x-test"), None);
        h.insert("x-test", HeaderValue::from_static("42"));
        assert_eq!(parse_u64_header(&h, "x-test"), Some(42));
        h.insert("x-test", HeaderValue::from_static("not a number"));
        assert_eq!(parse_u64_header(&h, "x-test"), None);
    }

    #[tokio::test]
    async fn parse_reset_header_returns_utc_datetime() {
        let mut h = HeaderMap::new();
        // 2026-04-26T12:00:00Z = 1777809600
        h.insert("x-ratelimit-reset", HeaderValue::from_static("1777809600"));
        let dt = parse_reset_header(&h).expect("parse epoch");
        assert_eq!(dt.timestamp(), 1777809600);
    }
}

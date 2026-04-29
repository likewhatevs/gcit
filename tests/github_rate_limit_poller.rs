// Rate-limit poller (/rate_limit endpoint, per-credential bucket).
// Spawn rate-limit poller per credential (octocrab /rate_limit every
// 60s).
// HTTP errors are scoped per-credential (RateBucket key); exhausted
// bucket on credential A does not affect credential B.
//
// GitHub /rate_limit endpoint response shape:
//   { "resources": {
//       "core":         { "limit": 5000, "remaining": 4321, "reset": <epoch>, "used": 679 },
//       "search":       { "limit": 30,   "remaining": 30,   "reset": <epoch>, "used": 0 },
//       "graphql":      { "limit": 5000, "remaining": 5000, "reset": <epoch>, "used": 0 },
//       "actions_runner_registration": ...,
//       ...
//     }, "rate": { ... legacy alias for core ... } }
//
// gcit cares about "core" (workflow_dispatch, get_run, list_jobs all use
// the core quota). search and graphql are unused.
//
// Cross-references tests/poll_rate_bucket.rs (the bucket consumer side
// of the same code path). This file covers the POLLER side.

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore = "requires gcit::github::rate_limit::poll (not yet implemented)"]
async fn poller_calls_rate_limit_endpoint_every_60s() {
    // Mock GET /rate_limit returns the standard shape above.
    // Spawn poller with credential_id="github_pat".
    //
    // Under start_paused, advance:
    //   t=0:  poller starts; immediate first call (assert wiremock saw 1 call)
    //   t=60: assert 2 calls
    //   t=120: assert 3 calls
    //
    // SPEC GAP: spec doesn't pin "first call at t=0 vs t=60". Recommend
    // immediate first call so the bucket has fresh state on daemon
    // start. flag.
    //
    // Mutation target: the 60s constant. Pin via assert_eq alongside
    // other backoff constants.
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore = "requires GitHub rate-limit poller implementation"]
async fn poller_extracts_core_quota_into_bucket() {
    // /rate_limit response: resources.core.{limit: 5000, remaining: 4321, reset: t+30}.
    //
    // After one poll cycle, bucket.remaining() == 4321 and bucket.reset() ==
    // current_time + 30s. Pin the field-level extraction.
    //
    // Mutation target: reading from "resources.search" instead of
    // "resources.core" — would silently use a much smaller quota.
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore = "requires GitHub rate-limit poller implementation"]
async fn poller_failure_retains_last_known_bucket_state() {
    // /rate_limit returns 5xx on poll #2 (poll #1 succeeded).
    // The bucket retains its observation from poll #1 (remaining=4321).
    // After 60s for poll #3, the poller tries again; if successful,
    // bucket gets the new value.
    //
    // The bucket's observe() must handle "no fresh observation"
    // gracefully.
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore = "requires GitHub rate-limit poller implementation"]
async fn poller_first_failure_pessimistic_clamp_to_zero() {
    // First-ever observation fails (network down at startup). Bucket has
    // no prior state. Recommend: clamp remaining=0 with reset=now+60s
    // (try again at next poll cycle). This blocks ALL workflow_dispatch /
    // get_run / list_jobs calls until the bucket gets a real observation,
    // which is the SAFE default (vs. fail-open and immediately exhaust
    // the unknown quota).
    //
    // SPEC GAP: spec doesn't pin first-failure behavior. The above
    // recommendation favors safety over availability. Alternative: assume
    // 5000/hour default and proceed. flag.
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore = "requires GitHub rate-limit poller implementation"]
async fn pollers_per_credential_run_independently() {
    // Two credentials: github_pat_a (quota poller A) and github_pat_b
    // (quota poller B). Each has its own /rate_limit poll (since the
    // /rate_limit response is scoped to the requesting PAT).
    //
    // Wiremock: distinguish the two PATs via bearer_token matchers.
    //   GET /rate_limit with Bearer github_pat_a -> {core: remaining: 4000}
    //   GET /rate_limit with Bearer github_pat_b -> {core: remaining: 1000}
    //
    // After both pollers run, bucket("a").remaining == 4000, bucket("b")
    // .remaining == 1000.
    //
    // Mutation target: a single shared bucket regardless of PAT. Cross-
    // references tests/poll_rate_bucket.rs::buckets_isolated_per_credential.
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore = "requires GitHub rate-limit poller implementation"]
async fn poller_request_does_not_consume_its_own_bucket_quota() {
    // GitHub's /rate_limit endpoint is documented as NOT counting against
    // the rate limit it reports. The poller should still issue the
    // request even when bucket.remaining == 0 (otherwise the bucket
    // could never recover after exhaustion).
    //
    // Bucket: remaining=0, reset=t+60s. Poller spawn. Wiremock asserts
    // GET /rate_limit was called within 1s (the immediate first call),
    // proving the poller bypassed the bucket guard for this specific
    // endpoint.
    //
    // SPEC GAP: spec doesn't address this circular dependency. Pin via
    // test. flag.
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore = "requires GitHub rate-limit poller implementation"]
async fn poller_log_observation_at_debug_level() {
    // Each successful poll cycle emits a tracing event with structured
    // fields {credential_id, remaining, limit, reset_in_seconds}.
    //
    // Recommend: DEBUG level (operators don't need every poll in INFO).
    // Operators can RUST_LOG=gcit::github::rate_limit=debug to see them.
    //
    // SPEC GAP: spec doesn't pin log level for poll observations. flag.
}

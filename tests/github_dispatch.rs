// workflow_dispatch via octocrab.
//
// Behavior contract (verified against src/github/dispatcher.rs::dispatch):
//   - POST /repos/{o}/{r}/actions/workflows/{wf}/dispatches via
//     octocrab's low-level `_post` so response headers stay accessible
//     for `observe_headers` (rate-limit snapshot refresh on every
//     response, success or error).
//   - personal_token wraps as Bearer (octocrab handles this).
//   - Body shape: {"ref": <ref_name>, "inputs": {<rendered>, gcit_run_id}}.
//     `rendered_inputs` is BTreeMap<String,String> — the supervisor
//     pre-renders handlebars templates BEFORE constructing
//     DispatchParams; dispatch() passes the values verbatim to the wire.
//   - 204 No Content is the documented success response (any 2xx is
//     accepted via octocrab's map_github_error).
//   - dispatch() returns DispatchOutcome on success — it does NOT
//     emit StateUpdate; that lives in flow::dispatcher.
//   - Quota gate: `RateLimitState::should_defer().await` is consulted
//     before any request fires. When it returns Some(wait), dispatch
//     races sleep(wait) against `cancel.cancelled()` and surfaces
//     `GithubErrorKind::Cancelled` if the cancel arm wins.
//
// Each test installs the rustls ring CryptoProvider once via
// std::sync::Once — octocrab's hyper-rustls client requires a process
// provider, and integration tests run without main().

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use secrecy::SecretString;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use gcit::config::CredentialId;
use gcit::git::rate_bucket::RateBucket;
use gcit::github::client::Client;
use gcit::github::dispatcher::{dispatch, dispatch_with_retry, DispatchParams};
use gcit::github::error::GithubErrorKind;
use gcit::github::rate_limit::RateLimitState;

use common::{build_dispatch_deps, ensure_crypto_provider, DISPATCH_PATH, PAT};

#[tokio::test]
async fn dispatch_posts_workflow_dispatch_endpoint() {
    // Happy path: dispatch() POSTs to the documented
    //   /repos/{owner}/{repo}/actions/workflows/{workflow}/dispatches
    // route with body {"ref": ..., "inputs": {gcit_run_id, ...}}, and
    // GitHub's documented 204 No Content surfaces as Ok(DispatchOutcome).
    //
    // Mutation targets:
    //   - dispatcher writes to a different route shape (would miss
    //     wiremock and fall through to no-mock 404).
    //   - dispatcher swallows 204 and returns Err.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let (client, rate_bucket, rate_limit) = build_dispatch_deps(&mock.uri()).await;

    let gcit_run_id = Uuid::new_v4();
    let params = DispatchParams {
        repo: "myorg/linux-builder".to_string(),
        workflow: "ci.yml".to_string(),
        ref_name: "refs/heads/main".to_string(),
        gcit_run_id,
        rendered_inputs: BTreeMap::new(),
    };
    let cancel = CancellationToken::new();
    dispatch(&client, &rate_bucket, &rate_limit, &params, &cancel)
        .await
        .expect("204 No Content surfaces as Ok");

    // Wiremock's `expect(1)` is checked at MockServer drop. Dropping
    // here surfaces the count-mismatch panic at this assertion's source
    // line if the dispatcher misroutes the POST.
    drop(mock);
}

#[tokio::test]
async fn dispatch_injects_gcit_run_id_into_inputs() {
    // dispatch() MUST inject the caller-supplied gcit_run_id into the
    // workflow_dispatch payload's `inputs.gcit_run_id` so the run-name
    // correlator can match the resulting Run. User-supplied inputs are
    // preserved alongside the injection — neither key is dropped, no
    // extra keys are added.
    //
    // Strategy: the wiremock matcher gates on body_partial_json so the
    // request is only accepted when both keys carry the configured
    // values; the test additionally inspects received_requests to
    // assert the `inputs` object has EXACTLY the two expected keys
    // (defends against a regression that injects extra keys or copies
    // the run-id under a different name).
    ensure_crypto_provider();
    let mock = MockServer::start().await;

    let gcit_run_id = Uuid::new_v4();
    let upstream_sha = "deadbeefcafe1234567890abcdef1234567890ab";

    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .and(body_partial_json(json!({
            "ref": "refs/heads/main",
            "inputs": {
                "upstream_sha": upstream_sha,
                "gcit_run_id": gcit_run_id.to_string(),
            }
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let (client, rate_bucket, rate_limit) = build_dispatch_deps(&mock.uri()).await;
    let mut rendered_inputs = BTreeMap::new();
    rendered_inputs.insert("upstream_sha".to_string(), upstream_sha.to_string());
    let params = DispatchParams {
        repo: "myorg/linux-builder".to_string(),
        workflow: "ci.yml".to_string(),
        ref_name: "refs/heads/main".to_string(),
        gcit_run_id,
        rendered_inputs,
    };
    let cancel = CancellationToken::new();
    dispatch(&client, &rate_bucket, &rate_limit, &params, &cancel)
        .await
        .expect("204 No Content surfaces as Ok");

    let received = mock.received_requests().await.expect("recording enabled");
    assert_eq!(received.len(), 1, "exactly one POST should have fired");
    let parsed: Value = serde_json::from_slice(&received[0].body).expect("valid JSON body");
    let inputs = parsed
        .get("inputs")
        .and_then(Value::as_object)
        .expect("body has `inputs` object");
    assert_eq!(
        inputs.len(),
        2,
        "inputs must contain exactly upstream_sha + gcit_run_id; got {inputs:?}",
    );
    assert_eq!(
        inputs.get("upstream_sha").and_then(Value::as_str),
        Some(upstream_sha),
        "user-supplied input must survive injection unchanged",
    );
    assert_eq!(
        inputs.get("gcit_run_id").and_then(Value::as_str),
        Some(gcit_run_id.to_string().as_str()),
        "injected gcit_run_id must match DispatchParams exactly",
    );
}

#[tokio::test]
async fn dispatch_passes_rendered_inputs_verbatim() {
    // dispatch() does NOT render handlebars templates — it takes a
    // pre-rendered BTreeMap<String,String> and passes every value
    // byte-for-byte to the wire. Template rendering is the supervisor's
    // responsibility (src/flow/dispatcher.rs calls
    // gh_dispatcher::render_inputs BEFORE building DispatchParams).
    //
    // This test pins the verbatim contract: a literal "{{source.sha}}"
    // in rendered_inputs would arrive at GitHub unchanged because
    // dispatch() does no further processing. The mutation target is
    // any well-meaning attempt to "double-render" or transform values
    // inside dispatch() — that would change the wire body and the
    // exact-string assertion below would fail.
    //
    // We use a non-template-looking literal to keep the test focused
    // on the verbatim guarantee rather than escaping/rendering
    // semantics. Pair with the body inspection that pulls the actual
    // wire bytes and asserts the stored value reaches GitHub exactly
    // as configured, alongside the injected gcit_run_id.
    ensure_crypto_provider();
    let mock = MockServer::start().await;

    let gcit_run_id = Uuid::new_v4();
    let upstream_sha = "deadbeefcafe1234567890abcdef1234567890ab";

    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .and(body_partial_json(json!({
            "ref": "refs/heads/main",
            "inputs": {
                "upstream_sha": upstream_sha,
                "gcit_run_id": gcit_run_id.to_string(),
            }
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let (client, rate_bucket, rate_limit) = build_dispatch_deps(&mock.uri()).await;
    let mut rendered_inputs = BTreeMap::new();
    rendered_inputs.insert("upstream_sha".to_string(), upstream_sha.to_string());
    let params = DispatchParams {
        repo: "myorg/linux-builder".to_string(),
        workflow: "ci.yml".to_string(),
        ref_name: "refs/heads/main".to_string(),
        gcit_run_id,
        rendered_inputs,
    };
    let cancel = CancellationToken::new();
    dispatch(&client, &rate_bucket, &rate_limit, &params, &cancel)
        .await
        .expect("204 No Content surfaces as Ok");

    let received = mock.received_requests().await.expect("recording enabled");
    assert_eq!(received.len(), 1);
    let parsed: Value = serde_json::from_slice(&received[0].body).expect("valid JSON body");
    let upstream_sha_on_wire = parsed
        .get("inputs")
        .and_then(|v| v.get("upstream_sha"))
        .and_then(Value::as_str)
        .expect("body.inputs.upstream_sha is a string");
    assert_eq!(
        upstream_sha_on_wire, upstream_sha,
        "wire body must carry the rendered_inputs value byte-for-byte",
    );
    let gcit_run_id_on_wire = parsed
        .get("inputs")
        .and_then(|v| v.get("gcit_run_id"))
        .and_then(Value::as_str)
        .expect("body.inputs.gcit_run_id is a string");
    assert_eq!(
        gcit_run_id_on_wire,
        gcit_run_id.to_string(),
        "gcit_run_id must accompany the verbatim user-supplied input",
    );
}

#[tokio::test]
async fn dispatch_204_returns_dispatch_outcome_with_correct_fields() {
    // dispatch() returns Ok(DispatchOutcome) on a successful 2xx
    // response. The outcome carries the caller's gcit_run_id, the
    // params' repo/workflow/ref_name unchanged, and a `dispatched_at`
    // timestamp captured at request-fire time (Utc::now() right
    // before the POST).
    //
    // This test does NOT exercise StateUpdate — that lives in
    // flow::dispatcher's pipeline (dispatch -> correlator -> emit
    // RunStarted). github::dispatcher::dispatch returns the Outcome
    // and stops; the supervisor consumes Outcome and threads it into
    // the correlator + state writer.
    //
    // Mutation targets:
    //   - DispatchOutcome carries different repo/workflow/ref_name
    //     than the params (e.g. swapped fields).
    //   - dispatched_at populated from a stale or future clock source
    //     instead of Utc::now() at the call site.
    //   - gcit_run_id rewritten or generated inside dispatch() rather
    //     than threaded through from params.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let (client, rate_bucket, rate_limit) = build_dispatch_deps(&mock.uri()).await;

    let gcit_run_id = Uuid::new_v4();
    let params = DispatchParams {
        repo: "myorg/linux-builder".to_string(),
        workflow: "ci.yml".to_string(),
        ref_name: "refs/heads/main".to_string(),
        gcit_run_id,
        rendered_inputs: BTreeMap::new(),
    };
    let cancel = CancellationToken::new();
    let before = Utc::now();
    let outcome = dispatch(&client, &rate_bucket, &rate_limit, &params, &cancel)
        .await
        .expect("204 No Content surfaces as Ok(DispatchOutcome)");
    let after = Utc::now();

    assert_eq!(
        outcome.gcit_run_id, gcit_run_id,
        "outcome must carry the caller-supplied UUID",
    );
    assert_eq!(outcome.repo, "myorg/linux-builder");
    assert_eq!(outcome.workflow, "ci.yml");
    assert_eq!(outcome.ref_name, "refs/heads/main");
    assert!(
        outcome.dispatched_at >= before && outcome.dispatched_at <= after,
        "dispatched_at must lie in [before, after]; got {ts} not in [{before}, {after}]",
        ts = outcome.dispatched_at,
    );
}

#[tokio::test]
async fn dispatch_defers_when_rate_limited_and_cancels_cleanly() {
    // When the credential's rate-limit snapshot says quota is
    // exhausted with a future reset, dispatch()'s pre-fire check
    // (rate_limit.should_defer().await) returns Some(~remaining)
    // and the dispatcher enters a tokio::select! racing
    // sleep(wait) against cancel.cancelled(). A SIGTERM (or per-flow
    // reload) during that sleep MUST surface as
    // GithubErrorKind::Cancelled WITHOUT firing the POST — otherwise
    // a 60-min quota reset would block shutdown for 60 minutes.
    //
    // Setup: seed observe_full(limit=5000, remaining=0, reset=now+5min)
    // so should_defer returns Some(~300s). Spawn a task that fires
    // cancel after 50ms — well before the 5min sleep would naturally
    // wake — and assert the dispatch returns Cancelled with no POST
    // landed (wiremock expect(0)).
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(ResponseTemplate::new(204))
        .expect(0)
        .mount(&mock)
        .await;

    let client = Client::builder()
        .credential(CredentialId::new("github_pat").expect("valid id"))
        .token(SecretString::from(PAT.to_string()))
        .request_timeout(Duration::from_secs(5))
        .base_uri(mock.uri())
        .build()
        .expect("client build");
    let rate_bucket = RateBucket::new(Duration::from_millis(0));
    let rate_limit = RateLimitState::new();
    rate_limit
        .observe_full(5000, 0, Utc::now() + chrono::Duration::seconds(300))
        .await;
    // Sanity: confirm should_defer returned Some — without this gate
    // a regression that re-orders the select! could let the POST fire
    // even though the snapshot says quota is exhausted, and the
    // expect(0) below would catch the mistake but with a less
    // diagnostic message than this precondition assertion.
    let defer = rate_limit.should_defer().await;
    assert!(
        defer.is_some_and(|d| d > Duration::from_secs(60)),
        "seeded snapshot should defer for ~5min; got {defer:?}",
    );

    let cancel = CancellationToken::new();
    let cancel_handle = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel_handle.cancel();
    });

    let params = DispatchParams {
        repo: "myorg/linux-builder".to_string(),
        workflow: "ci.yml".to_string(),
        ref_name: "refs/heads/main".to_string(),
        gcit_run_id: Uuid::new_v4(),
        rendered_inputs: BTreeMap::new(),
    };
    let err = dispatch(&client, &rate_bucket, &rate_limit, &params, &cancel)
        .await
        .expect_err("rate-limit defer + cancel must surface as Err");
    assert!(
        matches!(err, GithubErrorKind::Cancelled),
        "expected Cancelled when defer-wait races a fired cancel; got {err:?}",
    );

    // wiremock's expect(0) is checked on Drop. Force the check now to
    // surface a clean panic message at the right line if the dispatcher
    // mistakenly fired despite the defer gate.
    drop(mock);
}

#[tokio::test]
async fn dispatch_204_updates_rate_limit_snapshot_from_response_headers() {
    // dispatch() calls `rate_limit.observe_headers(response.headers())`
    // BEFORE checking status (src/github/dispatcher.rs::dispatch step
    // e). Even on a 2xx response, the X-RateLimit-* headers must
    // refresh the credential's snapshot so the dispatcher's next call
    // sees a fresh quota observation rather than the stale poll-cycle
    // value (60s cadence on /rate_limit can lag fast back-to-back
    // dispatches).
    //
    // Mutation target: a dispatcher that skips observe_headers on the
    // success path (e.g. by inlining map_github_error before
    // observe_headers, since the success case enters via a different
    // arm) would leave the snapshot frozen on the seeded value
    // (5000/5000) instead of the response's 4999.
    ensure_crypto_provider();
    let mock = MockServer::start().await;

    // Future reset epoch — make it 7200s out so the snapshot's reset
    // field clearly differs from the seed (3600s) and we can check
    // the response's value won the merge.
    let reset_epoch = (Utc::now() + chrono::Duration::seconds(7200)).timestamp();
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(
            ResponseTemplate::new(204)
                .insert_header("X-RateLimit-Limit", "5000")
                .insert_header("X-RateLimit-Remaining", "4999")
                .insert_header("X-RateLimit-Reset", reset_epoch.to_string().as_str()),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let (client, rate_bucket, rate_limit) = build_dispatch_deps(&mock.uri()).await;
    // Pre-dispatch snapshot is the seed: remaining=5000.
    let pre = rate_limit.snapshot().await;
    assert_eq!(
        pre.remaining,
        Some(5000),
        "seed should be 5000; got {pre:?}",
    );

    let params = DispatchParams {
        repo: "myorg/linux-builder".to_string(),
        workflow: "ci.yml".to_string(),
        ref_name: "refs/heads/main".to_string(),
        gcit_run_id: Uuid::new_v4(),
        rendered_inputs: BTreeMap::new(),
    };
    let cancel = CancellationToken::new();
    dispatch(&client, &rate_bucket, &rate_limit, &params, &cancel)
        .await
        .expect("204 No Content surfaces as Ok");

    let post = rate_limit.snapshot().await;
    assert_eq!(
        post.remaining,
        Some(4999),
        "observe_headers must refresh remaining from response on success path",
    );
    assert_eq!(post.limit, Some(5000));
    let reset = post.reset.expect("reset populated from header");
    assert_eq!(
        reset.timestamp(),
        reset_epoch,
        "observe_headers must parse X-RateLimit-Reset as an epoch second",
    );
}

#[tokio::test]
async fn dispatch_timeout_returns_timeout_error() {
    // dispatch() wraps the octocrab `_post` call in
    // `tokio::time::timeout(client.request_timeout(), ...)`. When
    // the server delays its response past the per-request deadline,
    // the timeout arm fires and dispatch must surface
    // GithubErrorKind::Timeout carrying the configured Duration —
    // not the underlying tokio Elapsed, and not a Transport error
    // from a half-closed socket.
    //
    // Setup: wiremock holds the response for 10s; client deadline
    // is 1s. Real-time test (the deadline must elapse on the wall
    // clock so reqwest's send_timeout actually fires).
    //
    // Mutation targets:
    //   - dispatcher swallows the timeout and returns Ok / a
    //     different variant (Transport, Unknown).
    //   - dispatcher passes a different Duration into the Timeout
    //     variant than client.request_timeout() (e.g. hardcoded 30s).
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(ResponseTemplate::new(204).set_delay(Duration::from_secs(10)))
        .expect(1)
        .mount(&mock)
        .await;

    // Build the client directly — build_dispatch_deps hardcodes a
    // 5s request_timeout, but this test needs 1s so the timeout
    // fires inside the test's own budget. We still pre-seed
    // RateLimitState the same way build_dispatch_deps does so
    // should_defer returns None and the dispatcher proceeds to
    // fire the request immediately.
    let client = Client::builder()
        .credential(CredentialId::new("github_pat").expect("valid id"))
        .token(SecretString::from(PAT.to_string()))
        .request_timeout(Duration::from_secs(1))
        .base_uri(mock.uri())
        .build()
        .expect("client build");
    let rate_bucket = RateBucket::new(Duration::from_millis(0));
    let rate_limit = RateLimitState::new();
    rate_limit
        .observe_full(5000, 5000, Utc::now() + chrono::Duration::seconds(3600))
        .await;

    let params = DispatchParams {
        repo: "myorg/linux-builder".to_string(),
        workflow: "ci.yml".to_string(),
        ref_name: "refs/heads/main".to_string(),
        gcit_run_id: Uuid::new_v4(),
        rendered_inputs: BTreeMap::new(),
    };
    let cancel = CancellationToken::new();
    let err = dispatch(&client, &rate_bucket, &rate_limit, &params, &cancel)
        .await
        .expect_err("response delayed past 1s deadline must surface as Err");
    match err {
        GithubErrorKind::Timeout { timeout } => {
            assert_eq!(
                timeout,
                Duration::from_secs(1),
                "Timeout variant must carry the configured request_timeout, not a hardcoded value",
            );
        }
        other => panic!("expected GithubErrorKind::Timeout, got {other:?}"),
    }
}

#[tokio::test]
async fn dispatch_with_retry_retries_transient_then_succeeds() {
    // dispatch_with_retry wraps dispatch() in backon's
    // ExponentialBuilder schedule (min_delay=5s, factor=2.0,
    // jitter, max_times=N). Transient errors trigger backon's
    // retry; on a successful attempt the call resolves
    // Ok(DispatchOutcome).
    //
    // Two-layer retry contract that this test must respect:
    //   1. octocrab's internal `RetryConfig::Simple(3)` (default,
    //      verified at octocrab-0.49.8/src/lib.rs:932) retries on
    //      5xx and transport errors UP TO 3 TIMES INSIDE A SINGLE
    //      `dispatch()` CALL — i.e. 4 wire requests per dispatch
    //      before octocrab gives up and returns the error to gcit.
    //   2. Once octocrab returns the error, gcit's classifier maps
    //      5xx to ServerError (Transient), backon's `.when()`
    //      predicate fires, and `dispatch_with_retry` invokes
    //      `dispatch()` again after the backoff sleep.
    //
    // To exercise BACKON specifically (not just octocrab's
    // internal retry), wiremock must return >= 4 transient
    // failures before the success — otherwise octocrab swallows
    // the failures and backon never sees an error to retry on.
    //
    // Setup:
    //   * Mock A (priority 1, up_to_n_times(4), expect(4)): 503
    //     on the first 4 wire hits. octocrab consumes its 3
    //     retries here and surfaces ServerError on the 4th.
    //   * Mock B (priority 2, expect(1)): 204 on the 5th wire
    //     hit. backon's first retry of `dispatch()` fires this.
    //
    // Expected counts: octocrab 1st dispatch = 4 wire hits (all
    // 503). gcit backon retries → 2nd dispatch = 1 wire hit
    // (204). Total wire = 5. expect(4) + expect(1) = 5 hits
    // pinned exactly.
    //
    // Real-time cost: backon's min_delay is 5s and the test
    // hits backon exactly once, so wall-clock is ~5-10s
    // (with_jitter adds up to factor*delay on top). The test
    // runs on the real tokio clock — start_paused=true would
    // make tokio auto-advance fire dispatch()'s 5s
    // request_timeout before wiremock's real-IO future polls to
    // completion, masking the retry semantics with a Timeout.
    //
    // Mutation targets:
    //   - backon's .when() predicate flipped (e.g. !is_transient)
    //     — backon never retries, dispatch_with_retry surfaces
    //     ServerError after octocrab's first 4 hits, expect(1)
    //     on Mock B fails.
    //   - max_attempts cap accidentally consumed by octocrab's
    //     internal retries (not just backon's) — the cap could
    //     terminate before reaching Mock B; expect(1) fails.
    //   - dispatch_with_retry swallows transient errors and
    //     returns a synthetic Ok before retrying — Mock B never
    //     hit; expect(1) fails AND outcome fields could be wrong.
    ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(4)
        .with_priority(1)
        .expect(4)
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(ResponseTemplate::new(204))
        .with_priority(2)
        .expect(1)
        .mount(&mock)
        .await;

    let (client, rate_bucket, rate_limit) = build_dispatch_deps(&mock.uri()).await;
    let rate_bucket = Arc::new(rate_bucket);
    let rate_limit = Arc::new(rate_limit);

    let gcit_run_id = Uuid::new_v4();
    let params = DispatchParams {
        repo: "myorg/linux-builder".to_string(),
        workflow: "ci.yml".to_string(),
        ref_name: "refs/heads/main".to_string(),
        gcit_run_id,
        rendered_inputs: BTreeMap::new(),
    };
    let cancel = CancellationToken::new();
    // max_attempts = 5: backon counts dispatch attempts (NOT wire
    // requests; octocrab's internal retries are invisible to
    // backon). 1 failed dispatch + 1 successful dispatch = 2
    // attempts. The 5 cap leaves comfortable headroom so a
    // regression that mis-counts (e.g. consuming 2 attempts per
    // dispatch via a backon misuse) surfaces via the expected
    // wire-count mismatch rather than a generic max-attempts
    // exhaustion.
    let outcome = dispatch_with_retry(
        &client,
        Arc::clone(&rate_bucket),
        Arc::clone(&rate_limit),
        params,
        5,
        cancel,
    )
    .await
    .expect("transient 503 storm should retry through backon to the 204 success");
    assert_eq!(outcome.gcit_run_id, gcit_run_id);
    assert_eq!(outcome.repo, "myorg/linux-builder");
    assert_eq!(outcome.workflow, "ci.yml");
}

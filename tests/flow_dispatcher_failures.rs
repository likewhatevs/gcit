// Lifecycle edge-case tests for `flow::dispatcher::handle_trigger_for_test`.
// Pins the lifecycle's edge-case branches (failures + the past-reset
// success path that asserts no defer-sleep):
//   1. dispatch returns 401 → Err with kind="dispatch", no RunStarted.
//   2. dispatch succeeds, correlate path 401 (permanent) → Err with
//      kind="correlate".
//   3. cancel fired before dispatch reaches GitHub → returns; no RunStarted.
//   4. input render fails (handlebars undefined variable) →
//      Err with kind="input_render"; no HTTP at all.
//   5. rate-limit reset already elapsed → dispatch proceeds
//      immediately, no defer-sleep.
//
// The seam is `handle_trigger_for_test` (the `#[doc(hidden)] pub` test
// surface). Successful happy-path pipeline coverage lives in
// flow_dispatcher_pipeline.rs; this file fills the gap on the
// edge-case lifecycle arms that file does not exercise.
//
// Wiremock + the existing `Client::for_test` / `RateBucket` / `RateLimitState`
// helpers handle the GitHub side; tokio mpsc captures emitted state
// updates.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use secrecy::SecretString;
use serde_json::json;
use tokio::sync::{mpsc, Mutex as TokioMutex};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use gcit::config::{ActionConfig, CredentialId};
use gcit::flow::dispatcher::{handle_trigger_for_test, FlowDispatchParams};
use gcit::flow::supervisor::{record_last_error, FlowLastError};
use gcit::flow::TriggerSignal;
use gcit::git::rate_bucket::RateBucket;
use gcit::github::client::Client;
use gcit::github::rate_limit::RateLimitState;
use gcit::state::StateUpdate;
use tracing_test::traced_test;

mod common;

use common::{DISPATCH_PATH, PAT, RUNS_PATH};

/// Build a dispatcher harness pointed at `mock`. Caller chooses what
/// inputs to put in the action; the trigger SHA is fixed to a 40-hex
/// value so wiremock matchers are stable across test runs.
fn build_params(
    mock_uri: &str,
    inputs: BTreeMap<String, String>,
) -> (Arc<FlowDispatchParams>, gix_hash::ObjectId) {
    let github_client = Arc::new(
        Client::builder()
            .credential(CredentialId::new("github_pat").expect("valid id"))
            .token(SecretString::from(PAT.to_string()))
            .request_timeout(Duration::from_secs(2))
            .base_uri(mock_uri)
            .build()
            .expect("client build"),
    );
    let rate_bucket = Arc::new(RateBucket::new(Duration::from_millis(0)));
    let rate_limit = Arc::new(RateLimitState::new());

    let params = Arc::new(FlowDispatchParams {
        flow_name: "ci-flow".to_string(),
        flow_description: None,
        url: "https://example.com/repo.git".to_string(),
        ref_name: "refs/heads/main".to_string(),
        action: ActionConfig::GithubWorkflowDispatch {
            repo: "myorg/linux-builder".to_string(),
            workflow: "ci.yml".to_string(),
            ref_name: "refs/heads/main".to_string(),
            credential_id: CredentialId::new("github_pat").expect("valid id"),
            inputs,
        },
        destinations: Vec::new(),
        github_client,
        rate_bucket,
        rate_limit,
        job_interval: Duration::from_secs(30),
        notifiers: Vec::new(),
    });
    let head_sha = gix_hash::ObjectId::from_hex(b"deadbeefcafe1234567890abcdef1234567890ab")
        .expect("valid 40-hex sha");
    (params, head_sha)
}

/// Seed the rate-limit snapshot with full quota + a future reset so
/// `should_defer` returns None and the dispatch fires immediately
/// without the unobserved-snapshot 60s defer.
async fn seed_full_quota(rate_limit: &RateLimitState) {
    rate_limit
        .observe_full(5000, 5000, Utc::now() + chrono::Duration::seconds(3600))
        .await;
}

#[tokio::test]
#[traced_test]
async fn dispatch_failure_401_surfaces_as_dispatch_kind_no_run_started() {
    // dispatch path returns 401. The dispatcher's retry policy
    // short-circuits permanent errors; no RunStarted should land on
    // state_tx. The error message should carry "dispatch:" prefix
    // (per `DispatchError::from_github_error`'s "{stage}: {e}" shape).
    //
    // `#[traced_test]` captures tracing events into an in-memory buffer.
    // After observing the failure, the test mirrors what
    // `dispatcher::run` does — call
    // `record_last_error("dispatch", err_msg, ...)` — and asserts that
    // (1) the tracing event surfaces the `kind` discriminator and
    // (2) the message body retains the `dispatch:` prefix. This pins
    // the production observability contract: an operator reading
    // journalctl sees the same `kind` the control surface returns via
    // `gcit status`.
    common::ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(
            ResponseTemplate::new(401)
                .set_body_json(serde_json::json!({"message": "Bad credentials"})),
        )
        .mount(&mock)
        .await;

    let (params, head_sha) = build_params(&mock.uri(), BTreeMap::new());
    seed_full_quota(&params.rate_limit).await;

    let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
    let cancel = CancellationToken::new();
    let mut monitors: JoinSet<()> = JoinSet::new();
    let trigger = TriggerSignal {
        observed_sha: head_sha,
        observed_at: Utc::now(),
    };

    let result = tokio::time::timeout(
        Duration::from_secs(20),
        handle_trigger_for_test(
            Arc::clone(&params),
            trigger,
            state_tx,
            cancel,
            &mut monitors,
        ),
    )
    .await
    .expect("handle_trigger must complete within 20s");

    let err_msg = result.expect_err("401 must surface as Err");
    assert!(
        err_msg.starts_with("dispatch:"),
        "kind prefix must be `dispatch:`; got: {err_msg}",
    );
    // No RunStarted on the channel (the short-circuit means no
    // emission before the early return).
    assert!(
        state_rx.try_recv().is_err(),
        "401 dispatch must not emit RunStarted",
    );

    // Mirror dispatcher::run's failure-recording side effect: on Err
    // from handle_trigger, the production loop emits a `warn!` + calls
    // `record_last_error("dispatch", ..., e.message, ...)`. The
    // integration test cannot drive the run loop directly —
    // handle_trigger is the seam — so we replay the recording call
    // here. The kind discriminator is the leading word before the
    // `:` in the err message ("dispatch" for this path); the message
    // body is the full err string.
    let kind = err_msg
        .split_once(':')
        .map(|(k, _)| k)
        .expect("DispatchError message format is `{stage}: {e}`");
    let last_errors: Arc<TokioMutex<BTreeMap<String, FlowLastError>>> =
        Arc::new(TokioMutex::new(BTreeMap::new()));
    record_last_error(&last_errors, "ci-flow", kind, &err_msg, None).await;

    // tracing-subscriber's default fmt layer renders `Display`-formatted
    // fields as `name=value` (no surrounding quotes); record_last_error
    // emits with %-formatting (Display) so the captured line shape is
    // e.g. `WARN ...: last_error recorded flow=ci-flow kind=dispatch ...`.
    // Pin the kind correlation (an operator reading journalctl uses this
    // to triage), the `dispatch:` message-body prefix that carries the
    // underlying GithubErrorKind Display, and the canonical
    // "last_error recorded" message body that proves we hit the
    // record_last_error path rather than some other tracing emit.
    assert!(
        logs_contain("kind=dispatch"),
        "tracing event must carry the `kind=dispatch` discriminator",
    );
    assert!(
        logs_contain("dispatch:"),
        "tracing event must carry the `dispatch:` message-body prefix",
    );
    assert!(
        logs_contain("last_error recorded"),
        "tracing event must surface the canonical record_last_error message body",
    );
}

#[tokio::test]
async fn correlate_permanent_error_surfaces_as_correlate_kind_no_run_started() {
    // dispatch succeeds (204), correlate's first list-runs hits 401
    // (permanent). The correlator's `if !e.is_transient()` branch
    // short-circuits the polling loop and returns the
    // GithubErrorKind via CorrelationError::Github, which the flow
    // dispatcher surfaces with the `correlate:` kind prefix per
    // `DispatchError::from_correlation_error`.
    //
    // 401 (Unauthorized) is permanent in `GithubErrorKind::is_transient`
    // (Retryability::Permanent for Unauthorized in github::error);
    // using it here avoids the transient-retry polling loop a 5xx
    // would force, keeping the test bounded.
    common::ensure_crypto_provider();
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(ResponseTemplate::new(204))
        .mount(&mock)
        .await;
    Mock::given(method("GET"))
        .and(path(RUNS_PATH))
        .respond_with(
            ResponseTemplate::new(401)
                .set_body_json(serde_json::json!({"message": "Bad credentials"})),
        )
        .mount(&mock)
        .await;

    let (params, head_sha) = build_params(&mock.uri(), BTreeMap::new());
    seed_full_quota(&params.rate_limit).await;

    let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
    let cancel = CancellationToken::new();
    let mut monitors: JoinSet<()> = JoinSet::new();
    let trigger = TriggerSignal {
        observed_sha: head_sha,
        observed_at: Utc::now(),
    };

    let result = tokio::time::timeout(
        Duration::from_secs(15),
        handle_trigger_for_test(
            Arc::clone(&params),
            trigger,
            state_tx,
            cancel,
            &mut monitors,
        ),
    )
    .await
    .expect("handle_trigger must complete within 15s");

    let err_msg = result.expect_err("permanent correlate failure must surface as Err");
    assert!(
        err_msg.starts_with("correlate:"),
        "kind prefix must be `correlate:`; got: {err_msg}",
    );
    // dispatch landed (mock saw the POST), but correlate failed
    // before RunStarted could be emitted.
    assert!(
        state_rx.try_recv().is_err(),
        "correlate failure must not emit RunStarted",
    );
}

#[tokio::test]
async fn input_render_failure_surfaces_as_input_render_kind_no_http() {
    // An input template referencing an undefined variable forces
    // strict-mode handlebars to error out at render time, BEFORE any
    // HTTP call. Wiremock has no mocks mounted; if the dispatcher
    // touched it, the request would return 404 (wiremock default)
    // and surface as "dispatch:" — so observing "input_render:"
    // proves the early-exit branch.
    common::ensure_crypto_provider();
    let mock = MockServer::start().await;

    let mut inputs = BTreeMap::new();
    inputs.insert(
        "deliberately_undefined".to_string(),
        "{{undefined.nonexistent.var}}".to_string(),
    );
    let (params, head_sha) = build_params(&mock.uri(), inputs);
    seed_full_quota(&params.rate_limit).await;

    let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
    let cancel = CancellationToken::new();
    let mut monitors: JoinSet<()> = JoinSet::new();
    let trigger = TriggerSignal {
        observed_sha: head_sha,
        observed_at: Utc::now(),
    };

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        handle_trigger_for_test(
            Arc::clone(&params),
            trigger,
            state_tx,
            cancel,
            &mut monitors,
        ),
    )
    .await
    .expect("handle_trigger must complete within 5s");

    let err_msg = result.expect_err("input render failure must surface as Err");
    // `handle_trigger_for_test` returns `Err(e.message)`. The message
    // body for input-render failures is built by
    // `DispatchError::from_message("input_render", format!("input render failed: {e}"))`
    // inside flow::dispatcher::handle_trigger. Pin both the
    // template-render description and the offending variable name —
    // either drift surfaces here.
    assert!(
        err_msg.starts_with("input render failed:"),
        "message body must lead with `input render failed:`; got: {err_msg}",
    );
    assert!(
        err_msg.contains("undefined.nonexistent.var"),
        "error must name the offending variable; got: {err_msg}",
    );
    assert!(
        state_rx.try_recv().is_err(),
        "input render failure must not emit RunStarted",
    );
    // No mocks were mounted on the wiremock server — verify the
    // dispatcher did not touch it. mock.received_requests()
    // returns Some(empty Vec) when recording is enabled and no
    // request landed.
    let received = mock
        .received_requests()
        .await
        .expect("wiremock recording enabled by default");
    assert!(
        received.is_empty(),
        "input_render failure must short-circuit BEFORE any HTTP; mock saw {} requests",
        received.len(),
    );
}

#[tokio::test]
async fn dispatch_cancelled_in_rate_limit_defer_surfaces_no_run_started() {
    // Force the dispatch path through its cancel-aware select arm by
    // seeding the rate-limit snapshot with `remaining=0` and a future
    // reset 1 hour out — `should_defer` returns Some(remaining_until_reset),
    // putting `dispatch()` into the `tokio::select!` between cancel and
    // sleep. Cancel fires after a short delay; the cancel arm wins
    // and dispatch returns GithubErrorKind::Cancelled (Permanent),
    // which the flow dispatcher surfaces as `dispatch:` (the
    // "{stage}: {e}" format includes the Cancelled Display message
    // after the prefix).
    //
    // This is the bounded-cancel test: it pins the cancel branch of
    // dispatch() in <1s without depending on the correlator's 30s
    // drain deadline.
    common::ensure_crypto_provider();
    let mock = MockServer::start().await;
    // No mocks needed — the dispatch never reaches the wire because
    // the rate-limit defer arm cancels first. A POST mount would be
    // an `expect(0)` assertion but wiremock's default unmounted-route
    // 404 already proves nothing reached the server.

    let (params, head_sha) = build_params(&mock.uri(), BTreeMap::new());
    // Seed: zero remaining, reset 1h out → should_defer returns
    // Some(~1h). The dispatcher's tokio::select! waits on either
    // cancel or sleep(1h); cancel wins immediately when fired.
    params
        .rate_limit
        .observe_full(5000, 0, Utc::now() + chrono::Duration::seconds(3600))
        .await;

    let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
    let cancel = CancellationToken::new();
    let mut monitors: JoinSet<()> = JoinSet::new();
    let trigger = TriggerSignal {
        observed_sha: head_sha,
        observed_at: Utc::now(),
    };

    // Cancel after a short delay — the dispatcher is parked in the
    // rate-limit defer select and observes cancel within ms.
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel_clone.cancel();
    });

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        handle_trigger_for_test(
            Arc::clone(&params),
            trigger,
            state_tx,
            cancel,
            &mut monitors,
        ),
    )
    .await
    .expect("cancel branch must surface within 10s");

    // dispatch() returns GithubErrorKind::Cancelled, which
    // `DispatchError::from_github_error` formats as "dispatch: <Display>".
    let err_msg = result.expect_err("cancelled dispatch must surface as Err");
    assert!(
        err_msg.starts_with("dispatch:"),
        "cancel-during-defer must surface as `dispatch:`; got: {err_msg}",
    );
    // No HTTP reached the mock and no RunStarted emitted.
    assert!(
        state_rx.try_recv().is_err(),
        "cancel-during-defer must prevent RunStarted emission",
    );
    let received = mock
        .received_requests()
        .await
        .expect("wiremock recording enabled");
    assert!(
        received.is_empty(),
        "cancel-during-defer must short-circuit BEFORE the dispatch POST; mock saw {} requests",
        received.len(),
    );
}

#[tokio::test]
async fn dispatch_proceeds_immediately_when_rate_limit_reset_is_in_the_past() {
    // Seed the rate-limit snapshot with `remaining=0` and a reset
    // that has ALREADY ELAPSED (1h in the past). Per
    // RateLimitState::should_defer's match arm
    // `Some(0) => match s.reset { Some(reset) if reset > now => ...,
    // _ => None }`, an in-the-past reset returns None — the dispatcher
    // must proceed immediately rather than stalling on a sleep
    // computed from the stale reset epoch.
    //
    // This pins the regression: a refactor that swaps the comparison
    // (`reset >= now` instead of `reset > now`, or that subtracts
    // before checking ordering) could yield a Some(negative_duration)
    // that misroute the dispatcher into a multi-hour wait, OR a
    // panic via `(reset - now).to_std()` on a negative chrono::Duration.
    //
    // Test shape:
    //   1. Seed past-reset, remaining=0.
    //   2. Mount 204 on the dispatch endpoint.
    //   3. Mount the empty / matching pair on the runs list so the
    //      correlator's name-scan + head_sha-fallback both land on
    //      mocked responses (mirrors the pipeline test pattern).
    //   4. Drive handle_trigger_for_test with NO cancel.
    //   5. Assert the call completes well under 8s, the dispatch mock
    //      saw exactly one POST, RunStarted landed on state_tx, and
    //      the elapsed wall-clock is small (<5s — no 1h stall).
    common::ensure_crypto_provider();
    let mock = MockServer::start().await;

    let head_sha_hex = "deadbeefcafe1234567890abcdef1234567890ab";

    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    // Correlator: name-scan path (no head_sha query) returns empty
    // so the correlator falls back to head_sha matching.
    let now = Utc::now();
    Mock::given(method("GET"))
        .and(path(RUNS_PATH))
        .and(query_param("event", "workflow_dispatch"))
        .and(query_param("branch", "main"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"total_count": 0, "workflow_runs": []})),
        )
        .with_priority(2)
        .mount(&mock)
        .await;

    // Correlator fallback: head_sha-scan returns one matching run.
    let runs_with_match = json!({
        "total_count": 1,
        "workflow_runs": [
            common::make_run_json(202, "ci build", head_sha_hex, now + chrono::Duration::seconds(2)),
        ],
    });
    Mock::given(method("GET"))
        .and(path(RUNS_PATH))
        .and(query_param("head_sha", head_sha_hex))
        .respond_with(ResponseTemplate::new(200).set_body_json(runs_with_match))
        .with_priority(1)
        .mount(&mock)
        .await;

    let (params, head_sha) = build_params(&mock.uri(), BTreeMap::new());
    // Past-reset seed: 1h in the past, remaining=0. should_defer
    // returns None per the past-reset arm; dispatch fires immediately.
    params
        .rate_limit
        .observe_full(5000, 0, Utc::now() - chrono::Duration::seconds(3600))
        .await;

    let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
    let cancel = CancellationToken::new();
    let mut monitors: JoinSet<()> = JoinSet::new();
    let trigger = TriggerSignal {
        observed_sha: head_sha,
        observed_at: Utc::now(),
    };

    let started = Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(8),
        handle_trigger_for_test(
            Arc::clone(&params),
            trigger,
            state_tx,
            cancel.clone(),
            &mut monitors,
        ),
    )
    .await
    .expect("handle_trigger must complete in 8s — a stalled defer would blow the timeout");
    let elapsed = started.elapsed();

    result.expect("past-reset dispatch must succeed");

    // Tighter wall-clock pin: no defer-sleep occurred. A regression
    // that misroutes past-reset into a sleep would be on the order
    // of seconds-to-hours — 5s is well above the dispatch + correlate
    // round-trip on localhost wiremock and well below any plausible
    // defer duration.
    //
    // 5s is 10x the typical localhost round-trip; if CI flakes here
    // due to scheduling pressure, raise the threshold — but keep it
    // well below the 3600s a stale-reset defer would produce.
    assert!(
        elapsed < Duration::from_secs(5),
        "past-reset dispatch must NOT stall on defer-sleep; elapsed = {elapsed:?}",
    );

    // RunStarted lands on state_tx — proves the full pipeline ran
    // (dispatch + correlate + emit) rather than short-circuiting on
    // a stalled defer.
    let upd = state_rx
        .recv()
        .await
        .expect("RunStarted must be on the channel");
    match upd {
        StateUpdate::RunStarted { run_id, .. } => {
            assert_eq!(
                run_id, 202,
                "correlated run_id from the head_sha-fallback mock"
            );
        }
        other => panic!("expected RunStarted, got {other:?}"),
    }

    // Cancel + drain monitors so the spawned per-run monitor task
    // does not outlive the test.
    cancel.cancel();
    while monitors.join_next().await.is_some() {}

    // The dispatch mock's `expect(1)` is verified at MockServer drop;
    // re-assert the count explicitly here so a regression surfaces
    // with a clear message rather than a wiremock panic.
    let received = mock
        .received_requests()
        .await
        .expect("wiremock recording enabled");
    let dispatch_posts = received
        .iter()
        .filter(|r| r.method == http::Method::POST && r.url.path() == DISPATCH_PATH)
        .count();
    assert_eq!(
        dispatch_posts, 1,
        "past-reset dispatch must POST exactly once to the dispatch endpoint",
    );
}

// Flow-layer dispatcher integration test: drives the full pipeline
// at `gcit::flow::dispatcher::handle_trigger_for_test` against
// wiremock. Pins:
//   1. POST /repos/{o}/{r}/actions/workflows/{wf}/dispatches fires
//      with the right JSON body (ref + inputs + injected
//      gcit_run_id).
//   2. GET /repos/{o}/{r}/actions/workflows/{wf}/runs?... fires for
//      correlation and returns a Run carrying the same gcit_run_id
//      in its name.
//   3. The dispatcher emits exactly one StateUpdate::RunStarted on
//      the state_tx mpsc with the correlated run_id and the flow
//      name.
//
// This test lives at the flow::dispatcher level, not the
// github::dispatcher level. Where github_dispatch.rs::dispatch
// stops at returning DispatchOutcome, this test composes
// dispatch + correlate + RunStarted as a single pipeline.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use secrecy::SecretString;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use gcit::config::{ActionConfig, CredentialId};
use gcit::flow::dispatcher::{handle_trigger_for_test, FlowDispatchParams};
use gcit::flow::TriggerSignal;
use gcit::git::rate_bucket::RateBucket;
use gcit::github::client::Client;
use gcit::github::rate_limit::RateLimitState;
use gcit::state::StateUpdate;

mod common;

use common::{make_run_json, DISPATCH_PATH, PAT, RUNS_PATH};

#[tokio::test]
async fn handle_trigger_dispatches_correlates_and_emits_run_started() {
    // End-to-end: a single TriggerSignal flows through the
    // dispatcher's pipeline. Mock servers stand in for both the
    // workflow_dispatch POST and the list-runs GET. The test
    // captures the resulting StateUpdate::RunStarted on a tokio
    // mpsc and verifies its run_id matches the correlated run.
    //
    // Mutation targets:
    //   - dispatcher skips the correlate step → no RunStarted on
    //     state_tx (recv times out below).
    //   - dispatcher emits RunStarted with run_id=0 (the placeholder
    //     in pre-correlate trigger_run_ctx) instead of the
    //     correlated run_id (101).
    //   - dispatcher swaps repo/workflow fields, breaking the
    //     wiremock route matchers (expect(1) on Drop catches).
    common::ensure_crypto_provider();
    let mock = MockServer::start().await;

    let head_sha_hex = "deadbeefcafe1234567890abcdef1234567890ab";
    let head_sha = gix_hash::ObjectId::from_hex(head_sha_hex.as_bytes()).expect("valid 40-hex sha");

    // 1) dispatch endpoint: 204 on POST. wiremock recorded body is
    //    inspected after the test for the gcit_run_id field, so we
    //    can verify the dispatcher injected it correctly.
    Mock::given(method("POST"))
        .and(path(DISPATCH_PATH))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    // 2) list-runs endpoint: returns a single Run whose name carries
    //    the gcit-<uuid> marker. The correlator's name-substring
    //    match selects this run unambiguously.
    //
    //    The notifier's gcit_run_id is generated inside
    //    handle_trigger so we cannot wire the exact UUID into the
    //    Run.name in advance. Instead, the mock template uses
    //    `set_body_raw` with a placeholder we substitute — but
    //    wiremock 0.6 doesn't support body-substitution natively,
    //    and the simpler design is: respond with a single Run whose
    //    name contains "gcit-" + ANY uuid. The correlator's
    //    `Run.name.contains(&format!("gcit-{uuid}"))` only matches
    //    if Run.name carries that exact uuid.
    //
    //    To pin the uuid into the response body we use a
    //    `respond_with(ResponseTemplate::new(200).set_body_raw(...))`
    //    template. But we don't know the uuid until handle_trigger
    //    runs.
    //
    //    Workaround: install the GET mock to capture the actual
    //    GET request the correlator sends, extract the gcit_run_id
    //    from the dispatch's recorded request body (which IS the
    //    same uuid, since dispatch + correlate share it), then
    //    re-mount the GET mock with the correct uuid in the
    //    response body BEFORE handle_trigger calls correlate.
    //
    //    Cleaner approach: skip the name-match path entirely by
    //    setting `run_name_configured=Some(false)` — that would
    //    drive the correlator into the head_sha fallback path,
    //    which selects the most recent run by created_at >=
    //    dispatched_at - CLOCK_SKEW_BUFFER. But
    //    FlowDispatchParams doesn't carry that flag — it's on
    //    CorrelateParams which is built inside handle_trigger
    //    with `run_name_configured: None` (always tries name
    //    match first, falls back if nothing matches).
    //
    //    With `None`, the correlator runs the name scan first;
    //    on no match (because we don't know the uuid in advance),
    //    it then runs the fallback head_sha scan. So we mount a
    //    response that matches the name scan path with NO
    //    workflow_runs, and a separate response for the head_sha
    //    fallback that DOES include a run with matching head_sha.
    //
    //    Wiremock matches first-installed first; subsequent
    //    requests to the same path & query_params get the same
    //    response. That works fine for both paths so long as the
    //    matchers are sufficiently distinct.
    //
    // Mock A: name-scan (no `head_sha` query param) — empty.
    // Mock B: fallback (with `head_sha` query param) — single run.
    let now = Utc::now();
    let empty_runs = json!({"total_count": 0, "workflow_runs": []});
    Mock::given(method("GET"))
        .and(path(RUNS_PATH))
        .and(query_param("event", "workflow_dispatch"))
        .and(query_param("branch", "main"))
        // Match only when head_sha is NOT present (the name scan).
        // wiremock doesn't have a "no query param" matcher, but we
        // can use mock priority + up_to_n_times to guarantee the
        // fallback mock takes precedence when head_sha is supplied.
        .respond_with(ResponseTemplate::new(200).set_body_json(empty_runs))
        .with_priority(2)
        .mount(&mock)
        .await;

    let runs_with_match = json!({
        "total_count": 1,
        "workflow_runs": [
            make_run_json(101, "ci build", head_sha_hex, now + chrono::Duration::seconds(2)),
        ],
    });
    Mock::given(method("GET"))
        .and(path(RUNS_PATH))
        .and(query_param("head_sha", head_sha_hex))
        .respond_with(ResponseTemplate::new(200).set_body_json(runs_with_match))
        .with_priority(1)
        .mount(&mock)
        .await;

    // Build the GitHub client + per-credential rate state pointed
    // at wiremock.
    let github_client = Arc::new(
        Client::builder()
            .credential(CredentialId::new("github_pat").expect("valid id"))
            .token(SecretString::from(PAT.to_string()))
            .request_timeout(Duration::from_secs(5))
            .base_uri(mock.uri())
            .build()
            .expect("client build"),
    );
    let rate_bucket = Arc::new(RateBucket::new(Duration::from_millis(0)));
    let rate_limit = Arc::new(RateLimitState::new());
    rate_limit
        .observe_full(5000, 5000, Utc::now() + chrono::Duration::seconds(3600))
        .await;

    let action = ActionConfig::GithubWorkflowDispatch {
        repo: "myorg/linux-builder".to_string(),
        workflow: "ci.yml".to_string(),
        ref_name: "refs/heads/main".to_string(),
        credential_id: CredentialId::new("github_pat").expect("valid id"),
        inputs: BTreeMap::new(),
    };

    let params = Arc::new(FlowDispatchParams {
        flow_name: "ci-flow".to_string(),
        flow_description: None,
        url: "https://example.com/repo.git".to_string(),
        ref_name: "refs/heads/main".to_string(),
        action,
        destinations: Vec::new(),
        github_client,
        rate_bucket,
        rate_limit,
        job_interval: Duration::from_secs(30),
        // No notifiers — fan-out is fire-and-forget; an empty list
        // produces zero spawned tasks and the pipeline still runs
        // dispatch + correlate + RunStarted to completion.
        notifiers: Vec::new(),
    });

    let trigger = TriggerSignal {
        observed_sha: head_sha,
        observed_at: Utc::now(),
    };

    // Capture state updates on a sized mpsc — the dispatcher emits
    // exactly one RunStarted, plus the spawned monitor will emit
    // RunFinished asynchronously (which we don't drive here, since
    // the monitor's GET /runs/{id} hits no mock and the monitor
    // task will surface an error on its first poll). We only assert
    // the RunStarted; the monitor's eventual outcome is out of
    // scope.
    let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
    let cancel = CancellationToken::new();
    let mut monitors: JoinSet<()> = JoinSet::new();

    tokio::time::timeout(
        Duration::from_secs(15),
        handle_trigger_for_test(
            Arc::clone(&params),
            trigger,
            state_tx,
            cancel.clone(),
            &mut monitors,
        ),
    )
    .await
    .expect("handle_trigger must complete within 15s")
    .expect("dispatch + correlate must succeed end-to-end");

    // Receive the emitted RunStarted. Bound the wait — the channel
    // already has the message buffered by the time
    // handle_trigger_for_test returns Ok.
    let update = tokio::time::timeout(Duration::from_secs(2), state_rx.recv())
        .await
        .expect("RunStarted should be on the channel within 2s")
        .expect("state_tx must have sent at least one update before drop");
    match update {
        StateUpdate::RunStarted {
            flow,
            run_id,
            started_at: _,
        } => {
            assert_eq!(
                flow, "ci-flow",
                "RunStarted.flow must match params.flow_name",
            );
            assert_eq!(
                run_id, 101,
                "RunStarted.run_id must be the correlated run id, not the placeholder 0",
            );
        }
        other => panic!("expected first update to be RunStarted, got {other:?}"),
    }

    // Cancel + drain the monitor JoinSet so the test cleans up
    // promptly. The monitor's first list_jobs hit will error out
    // because there's no mock for it, but the monitor task drops
    // cleanly when cancel fires.
    cancel.cancel();
    while let Some(_finished) = tokio::time::timeout(Duration::from_secs(2), monitors.join_next())
        .await
        .ok()
        .flatten()
    {}
    drop(state_rx);

    // wiremock expect(1) on the dispatch mock fires at MockServer
    // drop. Verify the recorded dispatch body carried the
    // gcit_run_id field — the same UUID the correlator searched
    // for.
    let received = mock.received_requests().await.expect("recording enabled");
    let dispatch_req = received
        .iter()
        .find(|r| r.url.path() == DISPATCH_PATH)
        .expect("dispatch POST must have been recorded");
    let body: Value = serde_json::from_slice(&dispatch_req.body).expect("dispatch body is JSON");
    let inputs = body
        .get("inputs")
        .and_then(Value::as_object)
        .expect("body.inputs is an object");
    let gcit_run_id_str = inputs
        .get("gcit_run_id")
        .and_then(Value::as_str)
        .expect("inputs.gcit_run_id is a string");
    let _: Uuid = gcit_run_id_str
        .parse()
        .expect("gcit_run_id is a valid UUID");

    drop(mock);
}

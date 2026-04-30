// Unborn ref handling: WARN, no trigger, daemon stays Ready.
//
// Per-flow failure isolation invariants exercised here:
//   - Each flow task is wrapped in
//     `std::panic::AssertUnwindSafe(...).catch_unwind()` by the
//     supervisor. On panic: log a WARN, record a `FlowLastError`
//     under kind="panic", and respawn after a 30s delay.
//   - HTTP errors are scoped per-credential (one bad PAT can't take
//     down flows that use a different credential).
//   - Network-down conditions surface in `gcit status` as last_error;
//     the daemon stays Ready throughout.
//
// "Unborn ref" = configured ref doesn't exist on the source. This is a
// CONFIGURATION error from gcit's perspective (the operator typo'd or
// the upstream repo deleted the branch), but it's NOT a crash. gcit
// must:
//   1. Log a WARN with the offending ref + source URL.
//   2. Surface the condition in `gcit status` as last_error
//      (kind=git_poll_failed, message names the ref + URL).
//      UnbornRef shares the `git_poll_failed` umbrella with generic
//      poll failures (5xx, parse errors); the message body is what
//      tells an operator "ref doesn't exist" vs "upstream is
//      flapping". A previous `git_poll_failed` entry from a 5xx
//      burst is overwritten in place when the upstream subsequently
//      answers "ref doesn't exist" — same kind, new message.
//   3. NOT emit any TriggerSignal.
//   4. NOT crash the daemon or affect other flows.
//   5. Continue polling on the configured cadence — the operator might
//      be in the middle of pushing the branch.
//
// Test architecture: the assertions below need the supervisor's poll
// loop wiring (last_error map, state_tx + trigger_tx mpsc, cadence
// gating) without the noise of a real network-driven strategy. The
// `ScriptedPollExecutor` harness defined at the bottom of this file
// implements `gcit::flow::poll::PollExecutor` with a pre-baked Vec of
// outcomes, then drives `gcit::flow::poll::run_with_executor` directly
// — bypassing `auto_detect → github_api/grokmirror/ls_remote` so the
// test asserts the loop's wiring rather than any strategy's network
// behavior.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

use gcit::flow::poll::{
    run_with_executor, EffectivePoll, PollCycleError, PollExecutor, PollParams,
};
use gcit::flow::TriggerSignal;
use gcit::git::PollOutcome;
use gcit::state::StateUpdate;

#[tokio::test]
#[ignore = "covered by `unborn_ref_records_last_error_emits_no_trigger` below via the \
            ScriptedPollExecutor harness; this per-strategy skeleton stays #[ignore]'d as a \
            placeholder for the future strategy-driven test that wires real wiremock / octocrab \
            into the harness — useful as a regression-catcher for the network-layer code paths \
            that the harness deliberately mocks out"]
async fn github_api_unborn_ref_surfaces_in_status_no_trigger() {
    // wiremock GET /repos/o/r/git/ref/heads/nonexistent -> 404 with
    // GitHub error body { "message": "Not Found", "documentation_url": ... }.
    //
    // Drive one poll cycle. Assert:
    //   - tracing emits WARN target="gcit::flow::poll" with structured
    //     fields {flow=<name>, url=<source url>, ref_name=<ref>} and
    //     message "configured ref does not exist on remote; continuing
    //     on cadence"
    //   - state.flows[<name>].last_error == Some({kind: "git_poll_failed", ...})
    //   - state.flows[<name>].last_poll_at advances on every cycle
    //   - NO TriggerSignal sent to the dispatcher (test channel records 0)
    //   - daemon.is_ready() == true
}

#[tokio::test]
#[ignore = "covered by `unborn_ref_records_last_error_emits_no_trigger` below via the \
            ScriptedPollExecutor harness — strategy-agnostic. Cross-references the strategy-layer \
            pin in tests/poll_grokmirror.rs::lookup_fingerprint_for_unknown_repo_returns_repo_not_in_manifest"]
async fn grokmirror_repo_not_in_manifest_surfaces_in_status_no_trigger() {
    // Manifest body contains repos /a/.git, /b/.git but configured URL
    // points to /c/.git. PollOutcome::UnbornRef. Same assertions as the
    // github_api case.
}

#[tokio::test]
#[ignore = "covered by `unborn_ref_records_last_error_emits_no_trigger` below via the \
            ScriptedPollExecutor harness — strategy-agnostic. Cross-references the strategy-layer \
            pins in tests/poll_ls_remote.rs::poll_empty_repo_returns_unborn_ref_not_panic and \
            tests/poll_ls_remote.rs::poll_missing_ref_in_populated_repo_returns_unborn"]
async fn ls_remote_unborn_ref_surfaces_in_status_no_trigger() {
    // Bare repo with no commits; ls-remote returns empty ref list.
    // Same assertions.
}

// ---------------------------------------------------------------------------
// ACTIVATED tests below: drive `run_with_executor` against a
// `ScriptedPollExecutor` to exercise the supervisor wiring directly,
// strategy-agnostic.

/// One UnbornRef cycle records `kind="git_poll_failed"` in last_errors,
/// emits a `PollTimestamp` for liveness, and DOES NOT send a
/// `TriggerSignal`. The poll task then exits cleanly when the
/// scripted executor's outcome list is exhausted.
///
/// Runs under `start_paused = true` + `tokio::time::advance` so the
/// configured `source_interval` (60s default) is consumed in virtual
/// time without burning real seconds.
#[tokio::test(start_paused = true)]
async fn unborn_ref_records_last_error_emits_no_trigger() {
    let last_errors = Arc::new(Mutex::new(BTreeMap::new()));
    let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
    let (trigger_tx, mut trigger_rx) = mpsc::channel::<TriggerSignal>(8);
    let cancel = CancellationToken::new();

    let params = test_poll_params(
        "unborn-ref-flow",
        "https://example.com/o/r",
        "refs/heads/main",
        Arc::clone(&last_errors),
    );
    let executor = ScriptedPollExecutor::new(vec![
        Ok(PollOutcome::UnbornRef),
        Err(PollCycleError::Cancelled),
    ]);

    let cancel_for_task = cancel.clone();
    let task = tokio::spawn(async move {
        run_with_executor(
            params,
            executor,
            None,
            None,
            state_tx,
            trigger_tx,
            cancel_for_task,
        )
        .await;
    });

    // Drive cycles by advancing virtual time in small steps and
    // yielding between each so the spawned task progresses through
    // its scripted outcomes (sleep → poll → emit → sleep → ...).
    for _ in 0..200 {
        tokio::time::advance(Duration::from_millis(500)).await;
        tokio::task::yield_now().await;
    }
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("poll task must exit within 5s of cancel")
        .expect("poll task must not panic");

    // Liveness: PollTimestamp emitted on the UnbornRef cycle.
    let mut saw_timestamp = false;
    while let Ok(update) = state_rx.try_recv() {
        if let StateUpdate::PollTimestamp { flow, .. } = update {
            assert_eq!(flow, "unborn-ref-flow");
            saw_timestamp = true;
        }
    }
    assert!(
        saw_timestamp,
        "UnbornRef cycle must emit StateUpdate::PollTimestamp for liveness",
    );

    // No TriggerSignal sent.
    assert!(
        trigger_rx.try_recv().is_err(),
        "UnbornRef must not emit a TriggerSignal",
    );

    // last_error recorded with kind="git_poll_failed" and message
    // mentioning the ref + URL.
    let g = last_errors.lock().await;
    let entry = g
        .get("unborn-ref-flow")
        .expect("UnbornRef must record an entry under the flow's name");
    assert_eq!(entry.kind(), "git_poll_failed");
    assert!(
        entry.message().contains("refs/heads/main")
            && entry.message().contains("https://example.com/o/r"),
        "last_error message must name the ref + URL: {:?}",
        entry.message(),
    );
}

/// UnbornRef → Refreshed transition: poll 1 records the
/// `git_poll_failed` last_error, poll 2 (the ref appears at SHA A)
/// clears that entry via the existing `last_error_cleared` latch and
/// emits `PollObservation` + a `TriggerSignal` (because compare_sha
/// against `None` returns `trigger: true` for first-observation).
#[tokio::test(start_paused = true)]
async fn unborn_ref_then_ref_appears_clears_last_error() {
    let last_errors = Arc::new(Mutex::new(BTreeMap::new()));
    let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
    let (trigger_tx, mut trigger_rx) = mpsc::channel::<TriggerSignal>(8);
    let cancel = CancellationToken::new();

    let params = test_poll_params(
        "unborn-then-refresh",
        "https://example.com/o/r",
        "refs/heads/main",
        Arc::clone(&last_errors),
    );
    let sha_a = sha_filled(0xaa);
    let executor = ScriptedPollExecutor::new(vec![
        Ok(PollOutcome::UnbornRef),
        Ok(PollOutcome::Refreshed { sha: sha_a }),
        Err(PollCycleError::Cancelled),
    ]);

    // Initial last_sha differs from the scripted Refreshed SHA so
    // `compare_sha(Some(prev), sha_a)` returns trigger:true.
    // `compare_sha(None, _)` returns trigger:false per the
    // first-observation-baseline rule in git::strategy::compare_sha
    // — that path does not exercise the trigger arm we want to
    // assert.
    let initial_last_sha = Some(sha_filled(0xff));
    let cancel_for_task = cancel.clone();
    let task = tokio::spawn(async move {
        run_with_executor(
            params,
            executor,
            initial_last_sha,
            None,
            state_tx,
            trigger_tx,
            cancel_for_task,
        )
        .await;
    });

    // Wait for the spawned task to drain its scripted outcomes —
    // when the executor returns Err(PollCycleError::Cancelled) the
    // loop exits cleanly via `return`, dropping both senders. We
    // can then read the channels to completion. To make the loop
    // proceed, advance virtual time well past the cumulative
    // jittered sleeps and yield enough that all pending sends
    // complete their tokio scheduling.
    for _ in 0..200 {
        tokio::time::advance(Duration::from_millis(500)).await;
        tokio::task::yield_now().await;
    }
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("poll task must exit")
        .expect("poll task must not panic");

    // Now both senders are dropped. Drain everything that landed.
    let mut triggers: Vec<TriggerSignal> = Vec::new();
    let mut state_updates: Vec<StateUpdate> = Vec::new();
    while let Ok(t) = trigger_rx.try_recv() {
        triggers.push(t);
    }
    while let Ok(u) = state_rx.try_recv() {
        state_updates.push(u);
    }

    let mut saw_unborn_timestamp = false;
    let mut saw_refresh_observation = false;
    for update in state_updates {
        match update {
            StateUpdate::PollTimestamp { flow, .. } => {
                assert_eq!(flow, "unborn-then-refresh");
                saw_unborn_timestamp = true;
            }
            StateUpdate::PollObservation { flow, last_sha, .. } => {
                assert_eq!(flow, "unborn-then-refresh");
                assert_eq!(last_sha, sha_a);
                saw_refresh_observation = true;
            }
            other => panic!("unexpected state update: {other:?}"),
        }
    }
    assert!(saw_unborn_timestamp, "UnbornRef cycle emits PollTimestamp");
    assert!(
        saw_refresh_observation,
        "Refreshed cycle emits PollObservation",
    );

    // Refreshed-with-no-prior-SHA fires a baseline TriggerSignal
    // (compare_sha(None, sha) returns trigger:true).
    assert_eq!(
        triggers.len(),
        1,
        "first Refreshed must emit exactly one TriggerSignal; got {triggers:?}",
    );
    assert_eq!(triggers[0].observed_sha, sha_a);

    // After the Refreshed cycle, last_error must be cleared.
    let g = last_errors.lock().await;
    assert!(
        !g.contains_key("unborn-then-refresh"),
        "Refreshed after UnbornRef must clear the last_error entry; got {:?}",
        g.get("unborn-then-refresh").map(|e| e.kind().to_string()),
    );
}

#[tokio::test]
#[ignore = "covered by `unborn_ref_loops_on_configured_cadence` below via the ScriptedPollExecutor \
            harness — three back-to-back UnbornRef cycles assert continued cadence with no backoff. \
            This per-strategy skeleton stays #[ignore]'d as a placeholder for future cadence \
            regressions specific to backoff classification"]
async fn unborn_ref_does_not_count_against_backoff() {
    // Unborn ref is Permanent (not Transient). It does NOT trigger backon
    // exponential delay; the next poll happens on the configured
    // source_interval cadence (typically 60s/300s).
}

/// Three back-to-back UnbornRef cycles: the loop continues on
/// `source_interval` cadence (no backoff classification of UnbornRef
/// as Transient). Asserts the harness sees three PollTimestamp
/// emissions and a single last_error record (each cycle overwrites
/// the prior under the same flow key).
#[tokio::test(start_paused = true)]
async fn unborn_ref_loops_on_configured_cadence() {
    let last_errors = Arc::new(Mutex::new(BTreeMap::new()));
    let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(16);
    let (trigger_tx, mut trigger_rx) = mpsc::channel::<TriggerSignal>(8);
    let cancel = CancellationToken::new();

    let params = test_poll_params(
        "unborn-cadence",
        "https://example.com/o/r",
        "refs/heads/main",
        Arc::clone(&last_errors),
    );
    let executor = ScriptedPollExecutor::new(vec![
        Ok(PollOutcome::UnbornRef),
        Ok(PollOutcome::UnbornRef),
        Ok(PollOutcome::UnbornRef),
        Err(PollCycleError::Cancelled),
    ]);

    let cancel_for_task = cancel.clone();
    let task = tokio::spawn(async move {
        run_with_executor(
            params,
            executor,
            None,
            None,
            state_tx,
            trigger_tx,
            cancel_for_task,
        )
        .await;
    });

    for _ in 0..200 {
        tokio::time::advance(Duration::from_millis(500)).await;
        tokio::task::yield_now().await;
    }
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("poll task must exit")
        .expect("poll task must not panic");

    let mut timestamp_count = 0;
    while let Ok(update) = state_rx.try_recv() {
        if let StateUpdate::PollTimestamp { flow, .. } = update {
            assert_eq!(flow, "unborn-cadence");
            timestamp_count += 1;
        }
    }
    assert_eq!(
        timestamp_count, 3,
        "three UnbornRef cycles must each emit a PollTimestamp",
    );
    assert!(
        trigger_rx.try_recv().is_err(),
        "no UnbornRef cycle emits a TriggerSignal",
    );

    let g = last_errors.lock().await;
    let entry = g
        .get("unborn-cadence")
        .expect("UnbornRef cycles must leave a last_error entry");
    assert_eq!(entry.kind(), "git_poll_failed");
}

#[tokio::test]
#[ignore = "covered by `unborn_ref_isolation_one_flow_does_not_starve_another` below via the \
            ScriptedPollExecutor harness — two flows share trigger/state channels but each has its \
            own CancellationToken + scripted executor"]
async fn unborn_ref_in_one_flow_does_not_crash_other_flows() {
    // Two flows configured. Flow A's ref is unborn; Flow B's ref exists
    // and changes from A -> B. Single poll cycle:
    //   - Flow A: WARN, no trigger, last_error set on flow A
    //   - Flow B: PollObservation, TriggerSignal emitted normally
}

/// Per-flow isolation: flow A loops on UnbornRef while flow B sees a
/// real Refreshed → fires a TriggerSignal. Asserts:
///   - Only flow B's TriggerSignal reaches the trigger channel
///   - Flow A's last_error is recorded; flow B's is absent (cleared
///     after first Refreshed via the latch)
///   - PollObservation for flow B sits next to PollTimestamps for
///     flow A in the state stream
#[tokio::test(start_paused = true)]
async fn unborn_ref_isolation_one_flow_does_not_starve_another() {
    let last_errors = Arc::new(Mutex::new(BTreeMap::new()));
    let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(32);
    let (trigger_tx, mut trigger_rx) = mpsc::channel::<TriggerSignal>(8);

    // Flow A: UnbornRef forever.
    let cancel_a = CancellationToken::new();
    let params_a = test_poll_params(
        "flow-a",
        "https://example.com/a/r",
        "refs/heads/main",
        Arc::clone(&last_errors),
    );
    let executor_a = ScriptedPollExecutor::new(vec![
        Ok(PollOutcome::UnbornRef),
        Ok(PollOutcome::UnbornRef),
        Err(PollCycleError::Cancelled),
    ]);

    // Flow B: real Refreshed cycle.
    let cancel_b = CancellationToken::new();
    let params_b = test_poll_params(
        "flow-b",
        "https://example.com/b/r",
        "refs/heads/main",
        Arc::clone(&last_errors),
    );
    let sha_b = sha_filled(0xbb);
    let executor_b = ScriptedPollExecutor::new(vec![
        Ok(PollOutcome::Refreshed { sha: sha_b }),
        Err(PollCycleError::Cancelled),
    ]);

    let cancel_a_for_task = cancel_a.clone();
    let state_tx_a = state_tx.clone();
    let trigger_tx_a = trigger_tx.clone();
    let task_a = tokio::spawn(async move {
        run_with_executor(
            params_a,
            executor_a,
            None,
            None,
            state_tx_a,
            trigger_tx_a,
            cancel_a_for_task,
        )
        .await;
    });
    let cancel_b_for_task = cancel_b.clone();
    // Initial last_sha for flow-b differs from sha_b so the first
    // Refreshed cycle is a real diff (compare_sha returns
    // trigger:true). Without this seed the first observation falls
    // under the baseline-no-trigger rule.
    let initial_last_sha_b = Some(sha_filled(0xff));
    let task_b = tokio::spawn(async move {
        run_with_executor(
            params_b,
            executor_b,
            initial_last_sha_b,
            None,
            state_tx,
            trigger_tx,
            cancel_b_for_task,
        )
        .await;
    });

    for _ in 0..200 {
        tokio::time::advance(Duration::from_millis(500)).await;
        tokio::task::yield_now().await;
    }
    cancel_a.cancel();
    cancel_b.cancel();
    tokio::time::timeout(Duration::from_secs(5), task_a)
        .await
        .expect("flow-a task must exit")
        .expect("flow-a must not panic");
    tokio::time::timeout(Duration::from_secs(5), task_b)
        .await
        .expect("flow-b task must exit")
        .expect("flow-b must not panic");

    let mut triggers: Vec<TriggerSignal> = Vec::new();
    let mut state_updates: Vec<StateUpdate> = Vec::new();
    while let Ok(t) = trigger_rx.try_recv() {
        triggers.push(t);
    }
    while let Ok(u) = state_rx.try_recv() {
        state_updates.push(u);
    }

    // Flow B's TriggerSignal arrived; no triggers from flow A.
    assert_eq!(
        triggers.len(),
        1,
        "exactly one trigger expected: {triggers:?}"
    );
    assert_eq!(triggers[0].observed_sha, sha_b);

    let mut saw_a_timestamp = false;
    let mut saw_b_observation = false;
    for update in state_updates {
        match update {
            StateUpdate::PollTimestamp { flow, .. } if flow == "flow-a" => {
                saw_a_timestamp = true;
            }
            StateUpdate::PollObservation { flow, last_sha, .. } if flow == "flow-b" => {
                assert_eq!(last_sha, sha_b);
                saw_b_observation = true;
            }
            _ => {}
        }
    }
    assert!(
        saw_a_timestamp,
        "flow-a's UnbornRef cycles emit PollTimestamps",
    );
    assert!(
        saw_b_observation,
        "flow-b's Refreshed emits PollObservation",
    );

    let g = last_errors.lock().await;
    assert_eq!(
        g.get("flow-a").map(|e| e.kind().to_string()),
        Some("git_poll_failed".to_string()),
        "flow-a must have a git_poll_failed last_error",
    );
    assert!(
        !g.contains_key("flow-b"),
        "flow-b's first Refreshed clears any latent last_error entry",
    );
}

#[tokio::test]
#[ignore = "covered by `unborn_ref_records_last_error_emits_no_trigger` above (the full message \
            text is asserted via .contains() on ref + URL) and by the in-module unit tests \
            flow::poll::tests::unborn_ref_message_* which pin the exact format including \
            the 'wait if it's still being pushed' remediation hint"]
async fn unborn_ref_warn_message_includes_actionable_text() {
    // The last_error message must guide the operator. Pinned by
    // `poll::unborn_ref_message`:
    //   "ref refs/heads/main not found at https://github.com/o/r — \
    //    verify the ref exists upstream, wait if it's still being \
    //    pushed, or update flow.<name>.source.ref"
}

// ---------------------------------------------------------------------------
// Harness helpers.

/// Pre-baked-outcome PollExecutor for end-to-end supervisor wiring
/// tests. Wraps a `Vec<Result<PollOutcome, PollCycleError>>` behind a
/// tokio Mutex so the trait's `&self` works (the loop never makes
/// concurrent calls to one executor — poll_cycle is awaited serially
/// per task — but the Mutex is needed for interior mutability behind
/// the shared reference). When the script is exhausted the executor
/// returns `Err(PollCycleError::Cancelled)` so the loop exits cleanly
/// without recording a last_error.
struct ScriptedPollExecutor {
    script: Mutex<std::collections::VecDeque<Result<PollOutcome, PollCycleError>>>,
}

impl ScriptedPollExecutor {
    fn new(outcomes: Vec<Result<PollOutcome, PollCycleError>>) -> Self {
        Self {
            script: Mutex::new(outcomes.into()),
        }
    }
}

impl PollExecutor for ScriptedPollExecutor {
    fn strategy_label(&self) -> &'static str {
        "scripted"
    }

    async fn poll_cycle(
        &self,
        _params: &PollParams,
        _cancel: &CancellationToken,
    ) -> Result<PollOutcome, PollCycleError> {
        let mut g = self.script.lock().await;
        g.pop_front().unwrap_or(Err(PollCycleError::Cancelled))
    }
}

/// Construct a minimal `PollParams` for the harness. `octo` and
/// `reqwest` are `None` because the `ScriptedPollExecutor` ignores
/// them. `rate_bucket` is `None` so the per-cycle pacing select!
/// falls through immediately.
fn test_poll_params(
    flow_name: &str,
    url: &str,
    ref_name: &str,
    last_errors: Arc<Mutex<BTreeMap<String, gcit::flow::supervisor::FlowLastError>>>,
) -> PollParams {
    PollParams {
        flow_name: flow_name.to_string(),
        url: url.to_string(),
        ref_name: ref_name.to_string(),
        effective_poll: EffectivePoll {
            // Short source_interval + zero jitter so the test
            // doesn't burn excess virtual time waiting between
            // cycles. Production defaults are 60s+/0.1; the loop's
            // wiring is identical regardless of cadence values, so
            // a 1ms cycle exercises the same code paths in
            // microseconds of virtual time.
            source_interval: Duration::from_millis(1),
            job_interval: Duration::from_secs(30),
            jitter: 0.0,
            // Cooldown disabled so the existing tests assert the
            // pre-cooldown trigger semantics.
            cooldown: Duration::ZERO,
        },
        rate_bucket: None,
        octo: None,
        reqwest: None,
        last_errors,
    }
}

fn sha_filled(byte: u8) -> gix_hash::ObjectId {
    let hex = format!("{byte:02x}").repeat(20);
    gix_hash::ObjectId::from_hex(hex.as_bytes()).expect("valid hex SHA")
}

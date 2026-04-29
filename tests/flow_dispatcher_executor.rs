// flow::dispatcher::handle_trigger_with_executor lifecycle tests.
//
// Drives the per-flow dispatch lifecycle against a scripted
// `DispatchExecutor` so the post-execute branches (RunStarted,
// run_start fan-out, monitor spawn, error mapping) can be exercised
// without wiremock + octocrab. Mirrors the
// `flow::poll::run_with_executor` and `flow::monitor::run_monitor_with_source`
// seams.
//
// These tests are FAST (single-digit milliseconds each) because the
// scripted executor returns by value — no network, no rate limiter,
// no retry loop.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use common::recording_notifier::{NotifyHook, RecordingNotifier};

use gcit::config::{ActionConfig, CredentialId};
use gcit::flow::dispatcher::{
    handle_trigger_with_executor, DispatchExecutor, ExecuteOutcome, FlowDispatchParams,
};
use gcit::flow::TriggerSignal;
use gcit::git::rate_bucket::RateBucket;
use gcit::github::client::Client;
use gcit::github::correlator::CorrelationOutcome;
use gcit::github::dispatcher::{DispatchOutcome, DispatchParams};
use gcit::github::monitor::empty_run_summary;
use gcit::github::rate_limit::RateLimitState;
use gcit::state::StateUpdate;
use secrecy::SecretString;

/// Pre-baked `DispatchExecutor` — returns one canned `ExecuteOutcome`
/// per call. Caller pre-loads the queue; calls beyond the queue
/// length panic so a forgotten outcome surfaces loudly rather than
/// silently producing an unrelated default.
struct ScriptedDispatchExecutor {
    outcomes: tokio::sync::Mutex<Vec<ExecuteOutcome>>,
    last_dispatch_params: tokio::sync::Mutex<Vec<DispatchParams>>,
}

impl ScriptedDispatchExecutor {
    fn new(outcomes: Vec<ExecuteOutcome>) -> Self {
        Self {
            outcomes: tokio::sync::Mutex::new(outcomes),
            last_dispatch_params: tokio::sync::Mutex::new(Vec::new()),
        }
    }
}

impl DispatchExecutor for ScriptedDispatchExecutor {
    async fn execute(
        &self,
        dispatch_params: DispatchParams,
        _branch: String,
        _head_sha: String,
        _cancel: CancellationToken,
    ) -> ExecuteOutcome {
        self.last_dispatch_params.lock().await.push(dispatch_params);
        self.outcomes
            .lock()
            .await
            .pop()
            .expect("ScriptedDispatchExecutor: outcome queue exhausted")
    }
}

fn build_params(
    notifiers: Vec<Arc<dyn gcit::flow::dispatcher::DynNotifier>>,
) -> Arc<FlowDispatchParams> {
    build_params_with_inputs(notifiers, BTreeMap::new())
}

/// `build_params` extended with caller-supplied action.inputs so the
/// input-render-failure test can inject a handlebars template that
/// references an undefined variable. Every other field matches
/// `build_params`'s defaults — the centralized crypto-provider call,
/// the unreachable base_uri (so an accidental production-path
/// dispatch fails loudly), and the unused-but-required GitHub client
/// and rate-limit shapes.
fn build_params_with_inputs(
    notifiers: Vec<Arc<dyn gcit::flow::dispatcher::DynNotifier>>,
    inputs: BTreeMap<String, String>,
) -> Arc<FlowDispatchParams> {
    common::ensure_crypto_provider();
    // Real client/rate_limit are required by FlowDispatchParams's
    // shape but the scripted executor never reads them. Point at an
    // unreachable URL so any accidental production-path call would
    // fail loudly.
    let github_client = Arc::new(
        Client::builder()
            .credential(CredentialId::new("github_pat").expect("valid id"))
            .token(SecretString::from(
                "github_pat_unreachable_test_should_not_call".to_string(),
            ))
            .request_timeout(Duration::from_secs(1))
            .base_uri("http://127.0.0.1:1")
            .build()
            .expect("client build"),
    );
    Arc::new(FlowDispatchParams {
        flow_name: "ci-flow".into(),
        flow_description: Some("ci flow desc".into()),
        url: "https://example.com/repo.git".into(),
        ref_name: "refs/heads/main".into(),
        action: ActionConfig::GithubWorkflowDispatch {
            repo: "myorg/linux-builder".into(),
            workflow: "ci.yml".into(),
            ref_name: "refs/heads/main".into(),
            credential_id: CredentialId::new("github_pat").expect("valid id"),
            inputs,
        },
        destinations: Vec::new(),
        github_client,
        rate_bucket: Arc::new(RateBucket::new(Duration::from_millis(0))),
        rate_limit: Arc::new(RateLimitState::new()),
        job_interval: Duration::from_secs(30),
        notifiers,
    })
}

fn make_dispatch_outcome(gcit_run_id: Uuid) -> DispatchOutcome {
    DispatchOutcome {
        gcit_run_id,
        dispatched_at: Utc::now(),
        repo: "myorg/linux-builder".into(),
        workflow: "ci.yml".into(),
        ref_name: "refs/heads/main".into(),
    }
}

fn make_correlation_outcome(run_id: u64) -> CorrelationOutcome {
    CorrelationOutcome {
        run_id,
        summary: empty_run_summary(run_id),
    }
}

fn make_trigger() -> TriggerSignal {
    let head_sha =
        gix_hash::ObjectId::from_hex(b"deadbeefcafe1234567890abcdef1234567890ab").expect("sha");
    TriggerSignal {
        observed_sha: head_sha,
        observed_at: Utc::now(),
    }
}

#[tokio::test]
async fn success_path_emits_run_started_and_fires_run_start_notifier() {
    // Scripted executor returns Success → RunStarted lands on
    // state_tx, on_run_start fires on every notifier exactly once.
    let recorder = Arc::new(RecordingNotifier::new("disc-1"));
    let notifiers: Vec<Arc<dyn gcit::flow::dispatcher::DynNotifier>> =
        vec![Arc::clone(&recorder) as _];
    let params = build_params(notifiers);

    let executor = ScriptedDispatchExecutor::new(vec![ExecuteOutcome::Success {
        dispatch: make_dispatch_outcome(Uuid::nil()),
        correlation: make_correlation_outcome(101),
    }]);

    let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
    let cancel = CancellationToken::new();
    let mut monitors: JoinSet<()> = JoinSet::new();

    tokio::time::timeout(
        Duration::from_secs(5),
        handle_trigger_with_executor(
            Arc::clone(&params),
            make_trigger(),
            state_tx,
            cancel.clone(),
            &mut monitors,
            &executor,
        ),
    )
    .await
    .expect("must complete in 5s")
    .expect("scripted Success must drive lifecycle to completion");

    // RunStarted emitted with the correlated run_id.
    let upd = state_rx
        .recv()
        .await
        .expect("RunStarted must be on the channel");
    match upd {
        StateUpdate::RunStarted { flow, run_id, .. } => {
            assert_eq!(flow, "ci-flow");
            assert_eq!(run_id, 101);
        }
        other => panic!("expected RunStarted, got {other:?}"),
    }

    // The on_run_start fan-out is fire-and-forget — handle_trigger
    // drops the JoinHandles. Sleep briefly so the spawned task has
    // run its next_outcome body before we assert on the count;
    // 100ms is well over the recorder's mutex-locked record path
    // (microseconds) and bounded so the test stays fast. Same
    // pattern as the failure-isolation test below.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // on_run_start fired once. on_run_complete is the monitor task's
    // responsibility — the scripted executor doesn't emit a Done
    // event, so the spawned monitor task hits the unreachable URL,
    // observes errors, and exits. Cancel + drain to be tidy.
    cancel.cancel();
    while monitors.join_next().await.is_some() {}

    assert_eq!(
        recorder.count(NotifyHook::RunStart),
        1,
        "on_run_start must fire exactly once for the successful trigger",
    );
}

#[tokio::test]
async fn dispatch_failed_path_returns_dispatch_kind_no_run_started() {
    // Scripted executor returns DispatchFailed(Unauthorized).
    // handle_trigger surfaces "dispatch:" message, no RunStarted on
    // state_tx, no notifier hooks fired.
    use gcit::config::CredentialId as Cid;
    use gcit::github::error::GithubErrorKind;

    let recorder = Arc::new(RecordingNotifier::new("disc-1"));
    let notifiers: Vec<Arc<dyn gcit::flow::dispatcher::DynNotifier>> =
        vec![Arc::clone(&recorder) as _];
    let params = build_params(notifiers);

    let executor = ScriptedDispatchExecutor::new(vec![ExecuteOutcome::DispatchFailed(
        GithubErrorKind::Unauthorized {
            credential: Cid::new("github_pat").expect("valid id"),
        },
    )]);

    let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
    let cancel = CancellationToken::new();
    let mut monitors: JoinSet<()> = JoinSet::new();

    let err = tokio::time::timeout(
        Duration::from_secs(5),
        handle_trigger_with_executor(
            Arc::clone(&params),
            make_trigger(),
            state_tx,
            cancel,
            &mut monitors,
            &executor,
        ),
    )
    .await
    .expect("5s")
    .expect_err("DispatchFailed must surface as Err");

    assert!(
        err.starts_with("dispatch:"),
        "kind must be `dispatch:`; got: {err}",
    );
    assert!(
        err.contains("Unauthorized") || err.contains("auth failed"),
        "error must include the underlying GithubErrorKind Display; got: {err}",
    );
    assert!(state_rx.try_recv().is_err());
    assert_eq!(recorder.count(NotifyHook::RunStart), 0);
}

#[tokio::test]
async fn correlate_failed_path_returns_correlate_kind_no_run_started() {
    use gcit::github::correlator::CorrelationError;

    let recorder = Arc::new(RecordingNotifier::new("disc-1"));
    let notifiers: Vec<Arc<dyn gcit::flow::dispatcher::DynNotifier>> =
        vec![Arc::clone(&recorder) as _];
    let params = build_params(notifiers);

    let executor = ScriptedDispatchExecutor::new(vec![ExecuteOutcome::CorrelateFailed(
        CorrelationError::DuplicateMatch {
            repo: "myorg/linux-builder".into(),
            workflow: "ci.yml".into(),
            gcit_run_id: Uuid::nil(),
            run_ids: vec![201, 202],
        },
    )]);

    let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
    let cancel = CancellationToken::new();
    let mut monitors: JoinSet<()> = JoinSet::new();

    let err = tokio::time::timeout(
        Duration::from_secs(5),
        handle_trigger_with_executor(
            Arc::clone(&params),
            make_trigger(),
            state_tx,
            cancel,
            &mut monitors,
            &executor,
        ),
    )
    .await
    .expect("5s")
    .expect_err("CorrelateFailed must surface as Err");

    assert!(err.starts_with("correlate:"), "got: {err}");
    assert!(
        err.contains("201") && err.contains("202"),
        "DuplicateMatch must list both run ids in the error message; got: {err}",
    );
    assert!(state_rx.try_recv().is_err());
    assert_eq!(recorder.count(NotifyHook::RunStart), 0);
}

#[tokio::test]
async fn dispatch_params_carries_injected_gcit_run_id() {
    // Verifies the dispatcher generates a UUID and threads it into
    // DispatchParams.gcit_run_id BEFORE calling the executor — pin
    // the contract that scripted tests can rely on.
    let recorder = Arc::new(RecordingNotifier::new("disc-1"));
    let notifiers: Vec<Arc<dyn gcit::flow::dispatcher::DynNotifier>> =
        vec![Arc::clone(&recorder) as _];
    let params = build_params(notifiers);

    let executor = ScriptedDispatchExecutor::new(vec![ExecuteOutcome::Success {
        dispatch: make_dispatch_outcome(Uuid::nil()),
        correlation: make_correlation_outcome(101),
    }]);

    let (state_tx, _state_rx) = mpsc::channel::<StateUpdate>(8);
    let cancel = CancellationToken::new();
    let mut monitors: JoinSet<()> = JoinSet::new();

    tokio::time::timeout(
        Duration::from_secs(5),
        handle_trigger_with_executor(
            Arc::clone(&params),
            make_trigger(),
            state_tx,
            cancel.clone(),
            &mut monitors,
            &executor,
        ),
    )
    .await
    .expect("5s")
    .expect("scripted Success");

    cancel.cancel();
    while monitors.join_next().await.is_some() {}

    let captured = executor.last_dispatch_params.lock().await;
    assert_eq!(captured.len(), 1, "executor invoked exactly once");
    let dp = &captured[0];
    assert_ne!(
        dp.gcit_run_id,
        Uuid::nil(),
        "handle_trigger must generate a fresh UUID for each trigger",
    );
    assert_eq!(dp.repo, "myorg/linux-builder");
    assert_eq!(dp.workflow, "ci.yml");
    assert_eq!(dp.ref_name, "refs/heads/main");
}

#[tokio::test]
async fn input_render_failure_skips_executor_entirely() {
    // Force a strict-mode handlebars failure via an undefined
    // variable in the action.inputs map. handle_trigger short-
    // circuits BEFORE calling the executor — verified by checking
    // last_dispatch_params is empty post-call.
    let recorder = Arc::new(RecordingNotifier::new("disc-1"));
    let notifiers: Vec<Arc<dyn gcit::flow::dispatcher::DynNotifier>> =
        vec![Arc::clone(&recorder) as _];

    let mut inputs = BTreeMap::new();
    inputs.insert("broken".to_string(), "{{undefined.var.path}}".to_string());
    // Build params with the broken inputs map. Goes through the
    // shared helper so the crypto-provider init and the unreachable
    // client shape stay aligned with the other tests.
    let params = build_params_with_inputs(notifiers, inputs);

    // Executor with NO outcomes — if handle_trigger calls execute,
    // the panic ("queue exhausted") surfaces as a test failure.
    let executor = ScriptedDispatchExecutor::new(Vec::new());

    let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
    let cancel = CancellationToken::new();
    let mut monitors: JoinSet<()> = JoinSet::new();

    let err = tokio::time::timeout(
        Duration::from_secs(5),
        handle_trigger_with_executor(
            Arc::clone(&params),
            make_trigger(),
            state_tx,
            cancel,
            &mut monitors,
            &executor,
        ),
    )
    .await
    .expect("5s")
    .expect_err("input render failure must surface as Err");

    assert!(err.starts_with("input render failed:"), "got: {err}",);
    assert!(
        executor.last_dispatch_params.lock().await.is_empty(),
        "executor must NOT be called when input render fails"
    );
    assert!(state_rx.try_recv().is_err());
    assert_eq!(recorder.count(NotifyHook::RunStart), 0);
}

/// Cancel-observing scripted executor — awaits `cancel.cancelled()`
/// and then returns `DispatchFailed(Cancelled)`. Pins the contract
/// that `handle_trigger_with_executor` threads its own `cancel`
/// argument through to `DispatchExecutor::execute`'s 4th parameter,
/// so the production `RealDispatchExecutor` can react to SIGHUP /
/// SIGTERM-driven cancellation mid-(dispatch + correlate).
///
/// `entered_cancel_wait` fires the moment the executor reaches its
/// `cancel.cancelled().await` point so the test body can race the
/// cancel signal against an exact synchronization edge instead of a
/// wall-clock sleep. The Sender is taken on the first execute call
/// (the test only invokes once); a second execute would observe
/// `None` and skip the signal.
struct CancelObservingDispatchExecutor {
    entered_cancel_wait: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl CancelObservingDispatchExecutor {
    fn new(entered_cancel_wait: tokio::sync::oneshot::Sender<()>) -> Self {
        Self {
            entered_cancel_wait: tokio::sync::Mutex::new(Some(entered_cancel_wait)),
        }
    }
}

impl DispatchExecutor for CancelObservingDispatchExecutor {
    async fn execute(
        &self,
        _dispatch_params: DispatchParams,
        _branch: String,
        _head_sha: String,
        cancel: CancellationToken,
    ) -> ExecuteOutcome {
        // Signal that we have reached the cancel-wait point.
        // `take()` ensures a second execute call (which the test
        // does not perform) would not panic on a doubly-consumed
        // Sender.
        if let Some(tx) = self.entered_cancel_wait.lock().await.take() {
            let _ = tx.send(());
        }
        cancel.cancelled().await;
        ExecuteOutcome::DispatchFailed(gcit::github::error::GithubErrorKind::Cancelled)
    }
}

#[tokio::test]
async fn cancel_during_execute_propagates_through_executor_seam() {
    // handle_trigger_with_executor must thread its `cancel` argument
    // into `DispatchExecutor::execute(..)`. A cancel-observing executor
    // confirms the wiring: cancel fires → executor returns
    // DispatchFailed(Cancelled) → handle_trigger surfaces "dispatch:"
    // with the Cancelled Display body. No RunStarted, no notifier
    // hook, no monitor spawn.
    let recorder = Arc::new(RecordingNotifier::new("disc-1"));
    let notifiers: Vec<Arc<dyn gcit::flow::dispatcher::DynNotifier>> =
        vec![Arc::clone(&recorder) as _];
    let params = build_params(notifiers);

    // The executor signals via `entered_tx` the moment it reaches
    // `cancel.cancelled().await`. Replacing the previous 50ms sleep
    // with this oneshot pins the exact race: cancel fires only after
    // the executor is provably parked on the cancel future, so the
    // test outcome does not depend on wall-clock timing across loaded
    // CI runners.
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel::<()>();
    let executor = CancelObservingDispatchExecutor::new(entered_tx);

    let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
    let cancel = CancellationToken::new();
    let mut monitors: JoinSet<()> = JoinSet::new();

    // Drive cancel only after the executor has reached its
    // cancel.cancelled().await point. The receiver task awaits the
    // oneshot signal; this completes the moment the executor is
    // confirmed parked, then fires cancel. Bounded by handle_trigger's
    // 5s outer timeout so a botched executor that never reaches the
    // signal point still surfaces as a timeout panic rather than a
    // hang.
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        // entered_rx returns Err only if the Sender is dropped without
        // sending (the executor never ran or panicked). In that case
        // the outer timeout still bounds the test.
        if entered_rx.await.is_ok() {
            cancel_clone.cancel();
        }
    });

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        handle_trigger_with_executor(
            Arc::clone(&params),
            make_trigger(),
            state_tx,
            cancel,
            &mut monitors,
            &executor,
        ),
    )
    .await
    .expect("handle_trigger must complete in 5s");

    let err = result.expect_err("cancelled execute must surface as Err");
    assert!(
        err.starts_with("dispatch:"),
        "kind prefix must be `dispatch:`; got: {err}",
    );
    // GithubErrorKind::Cancelled's Display body opens with
    // "dispatch cancelled by supervisor" (src/github/error.rs:142).
    // Pin the leading phrase so a wording drift surfaces here.
    assert!(
        err.contains("cancelled by supervisor"),
        "Cancelled Display must surface in the error body; got: {err}",
    );
    // No RunStarted on the channel — the cancelled execute
    // short-circuited before the post-execute lifecycle.
    assert!(
        state_rx.try_recv().is_err(),
        "cancel-during-execute must not emit RunStarted",
    );
    assert_eq!(
        recorder.count(NotifyHook::RunStart),
        0,
        "cancel-during-execute must not fire on_run_start",
    );
    assert!(
        monitors.is_empty(),
        "cancel-during-execute must not spawn a monitor",
    );
}

#[tokio::test]
async fn run_start_fan_out_isolates_failing_notifier_from_succeeding_one() {
    // The dispatcher's on_run_start fan-out (call site at
    // src/flow/dispatcher.rs:781) spawns one independent tokio::task
    // per notifier and drops the JoinHandles (fire-and-forget). One
    // notifier returning Err must NOT prevent the others from being
    // invoked: the supervisor's failure-isolation invariant says "a
    // flaky Discord webhook does not silence the mbox notifier on the
    // same flow."
    //
    // Setup: two RecordingNotifiers. The first returns
    // NotifyError::Permanent on its single RunStart invocation. The
    // second is left at the default (Ok). Drive a scripted Success
    // outcome through handle_trigger_with_executor and assert BOTH
    // recorders saw RunStart exactly once.
    let failing = Arc::new(RecordingNotifier::with_scripted(
        "failing-notifier",
        vec![Err(gcit::notify::NotifyError::Permanent {
            source: anyhow::anyhow!("scripted permanent failure for isolation test"),
        })],
    ));
    let succeeding = Arc::new(RecordingNotifier::new("succeeding-notifier"));
    let notifiers: Vec<Arc<dyn gcit::flow::dispatcher::DynNotifier>> =
        vec![Arc::clone(&failing) as _, Arc::clone(&succeeding) as _];
    let params = build_params(notifiers);

    let executor = ScriptedDispatchExecutor::new(vec![ExecuteOutcome::Success {
        dispatch: make_dispatch_outcome(Uuid::nil()),
        correlation: make_correlation_outcome(202),
    }]);

    let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
    let cancel = CancellationToken::new();
    let mut monitors: JoinSet<()> = JoinSet::new();

    tokio::time::timeout(
        Duration::from_secs(5),
        handle_trigger_with_executor(
            Arc::clone(&params),
            make_trigger(),
            state_tx,
            cancel.clone(),
            &mut monitors,
            &executor,
        ),
    )
    .await
    .expect("must complete in 5s")
    .expect("scripted Success must drive lifecycle to completion");

    // RunStarted lands on state_rx — proves the dispatcher reached
    // the post-execute path before the fan-out spawn.
    let upd = state_rx
        .recv()
        .await
        .expect("RunStarted must be on the channel");
    match upd {
        StateUpdate::RunStarted { run_id, .. } => assert_eq!(run_id, 202),
        other => panic!("expected RunStarted, got {other:?}"),
    }

    // The fan-out tasks are fire-and-forget — handle_trigger drops the
    // JoinHandles. Yield + brief sleep so both spawned tasks definitely
    // ran their next_outcome bodies before we assert on their counts.
    // 100ms is well over what the recorder's mutex-locked next_outcome
    // needs (microseconds) and bounded so the test stays fast.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cancel + drain the monitor (the scripted Success spawns one;
    // it'll hit the unreachable URL and exit promptly).
    cancel.cancel();
    while monitors.join_next().await.is_some() {}

    // Both notifiers must have received exactly one RunStart — the
    // failing one's Err did not abort the spawn loop, and the order
    // was independent (each notifier got its own task).
    assert_eq!(
        failing.count(NotifyHook::RunStart),
        1,
        "failing notifier must still observe RunStart (its hook ran; \
         it just returned Err afterwards)",
    );
    assert_eq!(
        succeeding.count(NotifyHook::RunStart),
        1,
        "succeeding notifier must observe RunStart even when a sibling \
         notifier failed; failure-isolation invariant",
    );
}

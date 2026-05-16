// Per-trigger pipeline: render inputs, drive the dispatch+correlate
// executor, emit RunStarted, fan out on_run_start, spawn the monitor.

use std::sync::Arc;

use chrono::Utc;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument};
use uuid::Uuid;

use crate::config::ActionConfig;
use crate::github::correlator;
use crate::github::dispatcher::{self as gh_dispatcher, DispatchParams};
use crate::notify::{self, ActionInfo, RunContext, SourceInfo};
use crate::state::StateUpdate;

use super::executor::{DispatchError, DispatchExecutor, ExecuteOutcome, RealDispatchExecutor};
use super::{FlowDispatchParams, DISPATCH_MAX_ATTEMPTS};
use crate::flow::monitor::{spawn_monitor, MonitorParams};
use crate::flow::TriggerSignal;

/// Test seam: drive the per-trigger pipeline against a real executor
/// from outside the loop. `#[doc(hidden)] pub` so integration tests
/// can call it.
#[doc(hidden)]
pub async fn handle_trigger_for_test(
    params: Arc<FlowDispatchParams>,
    trigger: TriggerSignal,
    state_tx: Sender<StateUpdate>,
    cancel: CancellationToken,
    monitors: &mut JoinSet<()>,
) -> Result<(), String> {
    let executor = RealDispatchExecutor {
        github_client: Arc::clone(&params.github_client),
        rate_bucket: Arc::clone(&params.rate_bucket),
        rate_limit: Arc::clone(&params.rate_limit),
        max_attempts: DISPATCH_MAX_ATTEMPTS,
    };
    handle_trigger(params, trigger, state_tx, cancel, monitors, &executor)
        .await
        .map_err(|e| e.message)
}

/// Test seam parametric on the executor — lets integration tests
/// script the (dispatch + correlate) pair without wiremock + octocrab.
#[doc(hidden)]
pub async fn handle_trigger_with_executor<E: DispatchExecutor>(
    params: Arc<FlowDispatchParams>,
    trigger: TriggerSignal,
    state_tx: Sender<StateUpdate>,
    cancel: CancellationToken,
    monitors: &mut JoinSet<()>,
    executor: &E,
) -> Result<(), String> {
    handle_trigger(params, trigger, state_tx, cancel, monitors, executor)
        .await
        .map_err(|e| e.message)
}

/// Process one trigger end-to-end.
///
/// `gcit_run_id` is generated up front so the same UUID surfaces in
/// (a) the input-render data context's `{{gcit.run_id}}`, (b) the
/// dispatch payload's injected `inputs.gcit_run_id`, and (c) the
/// correlator's name-substring search needle. User-supplied
/// `gcit_run_id` values are rejected at config-load time.
#[instrument(level = "debug", skip(params, state_tx, cancel, monitors, executor), fields(
    flow = %params.flow_name,
    sha = %trigger.observed_sha,
))]
pub(super) async fn handle_trigger<E: DispatchExecutor>(
    params: Arc<FlowDispatchParams>,
    trigger: TriggerSignal,
    state_tx: Sender<StateUpdate>,
    cancel: CancellationToken,
    monitors: &mut JoinSet<()>,
    executor: &E,
) -> Result<(), DispatchError> {
    // ActionConfig is currently only GithubWorkflowDispatch but match
    // exhaustively so a new variant surfaces here.
    let (repo, workflow, ref_name, inputs) = match &params.action {
        ActionConfig::GithubWorkflowDispatch {
            repo,
            workflow,
            ref_name,
            inputs,
            ..
        } => (
            repo.clone(),
            workflow.clone(),
            ref_name.clone(),
            inputs.clone(),
        ),
    };

    let gcit_run_id = Uuid::new_v4();
    // `trigger_dispatched_at` is the wall-clock at input-render — the
    // closest stand-in for dispatch time we have before
    // `dispatch_with_retry` returns its own `DispatchOutcome`.
    let trigger_dispatched_at = Utc::now();
    let trigger_run_ctx = RunContext {
        flow_name: params.flow_name.clone(),
        flow_description: params.flow_description.clone(),
        source: SourceInfo {
            url: params.url.clone(),
            ref_name: params.ref_name.clone(),
            sha: trigger.observed_sha,
            sha_short: short_sha(&trigger.observed_sha),
        },
        // ActionInfo's run_id + run_url are populated after
        // correlation; for input rendering we only need source +
        // flow + gcit.run_id.
        action: ActionInfo {
            repo: repo.clone(),
            workflow: workflow.clone(),
            run_id: 0,
            run_url: String::new(),
            dispatched_at: trigger_dispatched_at,
        },
        gcit_run_id,
    };
    let trigger_data = notify::render_context(&trigger_run_ctx, &empty_summary());

    let hb = notify::strict_handlebars();
    let rendered_inputs = match gh_dispatcher::render_inputs(&inputs, &trigger_data, &hb) {
        Ok(r) => r,
        Err(e) => {
            return Err(DispatchError::from_message(
                "input_render",
                format!("input render failed: {e}"),
            ));
        }
    };

    let dispatch_params = DispatchParams {
        repo: repo.clone(),
        workflow: workflow.clone(),
        ref_name: ref_name.clone(),
        gcit_run_id,
        rendered_inputs,
    };

    let branch = correlator::ref_to_branch(&ref_name).to_string();
    let head_sha = trigger.observed_sha.to_string();
    let (outcome, correlation) = match executor
        .execute(dispatch_params, branch, head_sha, cancel.clone())
        .await
    {
        ExecuteOutcome::Success {
            dispatch,
            correlation,
        } => (dispatch, correlation),
        ExecuteOutcome::DispatchFailed(e) => {
            return Err(DispatchError::from_github_error("dispatch", &e));
        }
        ExecuteOutcome::CorrelateFailed(e) => {
            return Err(DispatchError::from_correlation_error("correlate", &e));
        }
    };
    debug!(
        gcit_run_id = %outcome.gcit_run_id,
        "dispatch succeeded; correlating",
    );

    if state_tx
        .send(StateUpdate::RunStarted {
            flow: params.flow_name.clone(),
            run_id: correlation.run_id,
            started_at: Utc::now(),
        })
        .await
        .is_err()
    {
        return Err(DispatchError::from_message(
            "state_writer",
            "state writer dropped".into(),
        ));
    }

    let final_run_ctx = RunContext {
        flow_name: params.flow_name.clone(),
        flow_description: params.flow_description.clone(),
        source: SourceInfo {
            url: params.url.clone(),
            ref_name: params.ref_name.clone(),
            sha: trigger.observed_sha,
            sha_short: short_sha(&trigger.observed_sha),
        },
        action: ActionInfo {
            repo: outcome.repo.clone(),
            workflow: outcome.workflow.clone(),
            run_id: correlation.run_id,
            run_url: correlation.summary.run_url.clone(),
            dispatched_at: outcome.dispatched_at,
        },
        gcit_run_id: outcome.gcit_run_id,
    };

    // Fan out `on_run_start` fire-and-forget so a slow notifier does
    // not delay monitor spawn. Each per-notifier task logs its own
    // outcome; failures are isolated. Cancel propagates so an SIGTERM
    // mid-fan-out unblocks any blocking notifier syscall.
    {
        let ctx = final_run_ctx.clone();
        let cancel_for_fanout = cancel.clone();
        let _ = crate::flow::spawn_fan_out(&params.notifiers, "run-start", None, move |n| {
            let c = ctx.clone();
            let cancel = cancel_for_fanout.clone();
            async move { n.on_run_start(&c, &cancel).await }
        });
    }

    let mparams = MonitorParams {
        flow_name: params.flow_name.clone(),
        repo: outcome.repo.clone(),
        workflow: outcome.workflow.clone(),
        run_id: correlation.run_id,
        job_interval: params.job_interval,
        github_client: Arc::clone(&params.github_client),
        rate_limit: Arc::clone(&params.rate_limit),
        notifiers: params.notifiers.clone(),
        run_context: final_run_ctx,
    };
    spawn_monitor(monitors, mparams, state_tx.clone(), cancel.clone());
    Ok(())
}

/// First 12 hex chars of an `ObjectId` for `{{source.sha_short}}`.
/// The template-variable namespace pins this at "first 12 chars of
/// sha"; use the same width here for stable notifier output.
fn short_sha(sha: &gix_hash::ObjectId) -> String {
    let s = sha.to_string();
    let n = s.chars().count().min(12);
    s.chars().take(n).collect()
}

/// Empty `RunSummary` for input rendering against the trigger
/// context. The dispatch hasn't happened yet so no real run summary
/// exists; templates probing `run.status` see "queued".
fn empty_summary() -> crate::github::RunSummary {
    crate::github::monitor::empty_run_summary(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CredentialId;
    use crate::git::rate_bucket::RateBucket;
    use crate::github::client::Client as GithubClient;
    use crate::github::correlator::CorrelationError;
    use crate::github::dispatcher::DispatchOutcome;
    use crate::github::error::GithubErrorKind;
    use crate::github::rate_limit::RateLimitState;
    use crate::util::ensure_crypto_provider;
    use std::collections::BTreeMap;
    use std::time::Duration;
    use tokio::sync::mpsc;

    #[test]
    fn short_sha_pads_to_12_chars() {
        let sha = gix_hash::ObjectId::null(gix_hash::Kind::Sha1);
        let s = short_sha(&sha);
        assert_eq!(s.chars().count(), 12);
    }

    #[test]
    fn short_sha_truncates_real_hex_to_12_chars() {
        let sha = gix_hash::ObjectId::from_hex(b"deadbeefcafe1234567890abcdef1234567890ab")
            .expect("valid 40-hex sha");
        let s = short_sha(&sha);
        assert_eq!(s, "deadbeefcafe");
        assert_eq!(s.chars().count(), 12);
    }

    #[test]
    fn empty_summary_carries_zero_run_id() {
        let s = empty_summary();
        assert_eq!(s.run_id, 0);
    }

    /// Pre-baked executor for unit tests. Pops one outcome per call;
    /// an exhausted queue panics so a forgotten outcome surfaces.
    struct UnitScriptedExecutor {
        outcomes: tokio::sync::Mutex<Vec<ExecuteOutcome>>,
    }

    impl UnitScriptedExecutor {
        fn new(outcomes: Vec<ExecuteOutcome>) -> Self {
            Self {
                outcomes: tokio::sync::Mutex::new(outcomes),
            }
        }
    }

    impl DispatchExecutor for UnitScriptedExecutor {
        async fn execute(
            &self,
            _dispatch_params: DispatchParams,
            _branch: String,
            _head_sha: String,
            _cancel: CancellationToken,
        ) -> ExecuteOutcome {
            self.outcomes
                .lock()
                .await
                .pop()
                .expect("UnitScriptedExecutor: outcome queue exhausted")
        }
    }

    /// Minimal-viable `FlowDispatchParams` for scripted-executor
    /// tests. `github_client` points at an unreachable URL so any
    /// accidental production-path call fails loudly.
    pub(super) fn build_test_params(inputs: BTreeMap<String, String>) -> Arc<FlowDispatchParams> {
        ensure_crypto_provider();
        let github_client = Arc::new(
            GithubClient::builder()
                .credential(CredentialId::new("github_pat").expect("valid id"))
                .token(secrecy::SecretString::from(
                    "github_pat_FFFF0000unit_test_unreachableFFFF".to_string(),
                ))
                .request_timeout(Duration::from_secs(1))
                .base_uri("http://127.0.0.1:1")
                .build()
                .expect("client build"),
        );
        Arc::new(FlowDispatchParams {
            flow_name: "unit-flow".into(),
            flow_description: Some("unit test flow".into()),
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
            notifiers: Vec::new(),
        })
    }

    pub(super) fn unit_test_trigger() -> TriggerSignal {
        let head_sha = gix_hash::ObjectId::from_hex(b"deadbeefcafe1234567890abcdef1234567890ab")
            .expect("valid 40-hex sha");
        TriggerSignal {
            observed_sha: head_sha,
            observed_at: Utc::now(),
        }
    }

    fn success_outcome(run_id: u64) -> ExecuteOutcome {
        ExecuteOutcome::Success {
            dispatch: DispatchOutcome {
                gcit_run_id: Uuid::new_v4(),
                dispatched_at: Utc::now(),
                repo: "myorg/linux-builder".to_string(),
                workflow: "ci.yml".to_string(),
                ref_name: "refs/heads/main".to_string(),
            },
            correlation: correlator::CorrelationOutcome {
                run_id,
                summary: crate::github::monitor::empty_run_summary(run_id),
            },
        }
    }

    fn dispatch_failed_unauthorized() -> ExecuteOutcome {
        ExecuteOutcome::DispatchFailed(GithubErrorKind::Unauthorized {
            credential: CredentialId::new("github_pat").expect("valid id"),
        })
    }

    #[tokio::test]
    async fn handle_trigger_with_executor_dispatch_failed_unauthorized_surfaces_dispatch_prefix() {
        let executor = UnitScriptedExecutor::new(vec![dispatch_failed_unauthorized()]);
        let params = build_test_params(BTreeMap::new());
        let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
        let cancel = CancellationToken::new();
        let mut monitors: JoinSet<()> = JoinSet::new();
        let err = handle_trigger_with_executor(
            params,
            unit_test_trigger(),
            state_tx,
            cancel,
            &mut monitors,
            &executor,
        )
        .await
        .expect_err("DispatchFailed must surface as Err");
        assert!(
            err.starts_with("dispatch:"),
            "expected dispatch: prefix; got: {err}"
        );
        assert!(
            state_rx.try_recv().is_err(),
            "DispatchFailed must NOT emit RunStarted"
        );
        assert!(
            monitors.is_empty(),
            "DispatchFailed must NOT spawn a monitor"
        );
    }

    #[tokio::test]
    async fn handle_trigger_with_executor_correlate_failed_timeout_surfaces_correlate_prefix() {
        let executor = UnitScriptedExecutor::new(vec![ExecuteOutcome::CorrelateFailed(
            CorrelationError::Timeout {
                repo: "myorg/linux-builder".into(),
                workflow: "ci.yml".into(),
                gcit_run_id: Uuid::nil(),
                timeout: Duration::from_secs(30),
            },
        )]);
        let params = build_test_params(BTreeMap::new());
        let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
        let cancel = CancellationToken::new();
        let mut monitors: JoinSet<()> = JoinSet::new();
        let err = handle_trigger_with_executor(
            params,
            unit_test_trigger(),
            state_tx,
            cancel,
            &mut monitors,
            &executor,
        )
        .await
        .expect_err("CorrelateFailed must surface as Err");
        assert!(
            err.starts_with("correlate:"),
            "expected correlate: prefix; got: {err}"
        );
        assert!(state_rx.try_recv().is_err());
        assert!(monitors.is_empty());
    }

    #[tokio::test]
    async fn handle_trigger_with_executor_input_render_failure_short_circuits_before_executor() {
        // Undefined-var template makes `render_inputs` return Err; the
        // executor must NOT be invoked (empty queue would panic).
        let mut inputs = BTreeMap::new();
        inputs.insert("broken".to_string(), "{{undefined.var.path}}".to_string());
        let params = build_test_params(inputs);
        let executor = UnitScriptedExecutor::new(Vec::new());
        let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
        let cancel = CancellationToken::new();
        let mut monitors: JoinSet<()> = JoinSet::new();
        let err = handle_trigger_with_executor(
            params,
            unit_test_trigger(),
            state_tx,
            cancel,
            &mut monitors,
            &executor,
        )
        .await
        .expect_err("input render failure must surface as Err");
        assert!(err.starts_with("input render failed:"), "got: {err}");
        assert!(state_rx.try_recv().is_err());
        assert!(monitors.is_empty());
    }

    #[tokio::test]
    async fn handle_trigger_with_executor_success_emits_run_started_and_spawns_monitor() {
        // Pre-cancel BEFORE handle_trigger spawns the monitor so the
        // monitor's first poll observes the cancelled token and
        // unwinds without contacting the unreachable test base_uri.
        let executor = UnitScriptedExecutor::new(vec![success_outcome(4242)]);
        let params = build_test_params(BTreeMap::new());
        let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut monitors: JoinSet<()> = JoinSet::new();
        handle_trigger_with_executor(
            params,
            unit_test_trigger(),
            state_tx,
            cancel,
            &mut monitors,
            &executor,
        )
        .await
        .expect("Success must NOT surface as Err");

        match state_rx
            .try_recv()
            .expect("Success path must emit RunStarted")
        {
            StateUpdate::RunStarted { flow, run_id, .. } => {
                assert_eq!(flow, "unit-flow");
                assert_eq!(run_id, 4242);
            }
            other => panic!("expected RunStarted, got {other:?}"),
        }
        assert_eq!(monitors.len(), 1, "Success must spawn exactly one monitor");
        while monitors.join_next().await.is_some() {}
    }

    #[tokio::test]
    async fn handle_trigger_with_executor_success_state_writer_dropped_returns_state_writer_error()
    {
        let executor = UnitScriptedExecutor::new(vec![success_outcome(123)]);
        let params = build_test_params(BTreeMap::new());
        let (state_tx, state_rx) = mpsc::channel::<StateUpdate>(8);
        drop(state_rx);
        let cancel = CancellationToken::new();
        let mut monitors: JoinSet<()> = JoinSet::new();
        let err = handle_trigger_with_executor(
            params,
            unit_test_trigger(),
            state_tx,
            cancel,
            &mut monitors,
            &executor,
        )
        .await
        .expect_err("dropped state_rx must surface as Err");
        assert_eq!(err, "state writer dropped");
        // Monitor MUST NOT spawn when RunStarted send fails — the
        // function returns before the spawn_monitor call.
        assert!(monitors.is_empty());
    }

    #[tokio::test]
    async fn handle_trigger_with_executor_success_with_inputs_renders_handlebars_template() {
        // Non-empty inputs exercise the render-success branch.
        let mut inputs = BTreeMap::new();
        inputs.insert("flow_name".to_string(), "{{flow.name}}".to_string());
        inputs.insert("ref_name".to_string(), "{{source.ref_name}}".to_string());
        inputs.insert(
            "static_value".to_string(),
            "literal text no template".to_string(),
        );
        let executor = UnitScriptedExecutor::new(vec![success_outcome(555)]);
        let params = build_test_params(inputs);
        let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut monitors: JoinSet<()> = JoinSet::new();
        handle_trigger_with_executor(
            params,
            unit_test_trigger(),
            state_tx,
            cancel,
            &mut monitors,
            &executor,
        )
        .await
        .expect("rendered inputs must succeed");

        match state_rx.try_recv() {
            Ok(StateUpdate::RunStarted { run_id, flow, .. }) => {
                assert_eq!(run_id, 555);
                assert_eq!(flow, "unit-flow");
            }
            other => panic!("expected RunStarted, got {other:?}"),
        }
        assert_eq!(monitors.len(), 1);
        while monitors.join_next().await.is_some() {}
    }

    #[tokio::test]
    async fn handle_trigger_with_executor_dispatch_failed_rate_limited_propagates_retry_at() {
        let reset = chrono::Utc::now() + chrono::Duration::minutes(15);
        let executor = UnitScriptedExecutor::new(vec![ExecuteOutcome::DispatchFailed(
            GithubErrorKind::RateLimited {
                credential: CredentialId::new("github_pat").expect("valid id"),
                reset,
            },
        )]);
        let params = build_test_params(BTreeMap::new());
        let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
        let cancel = CancellationToken::new();
        let mut monitors: JoinSet<()> = JoinSet::new();
        let err = handle_trigger_with_executor(
            params,
            unit_test_trigger(),
            state_tx,
            cancel,
            &mut monitors,
            &executor,
        )
        .await
        .expect_err("DispatchFailed(RateLimited) must surface as Err");
        assert!(err.starts_with("dispatch:"));
        assert!(state_rx.try_recv().is_err());
        assert!(monitors.is_empty());
    }

    #[tokio::test]
    async fn handle_trigger_with_executor_correlate_failed_duplicate_match_lists_run_ids() {
        let executor = UnitScriptedExecutor::new(vec![ExecuteOutcome::CorrelateFailed(
            CorrelationError::DuplicateMatch {
                repo: "myorg/linux-builder".into(),
                workflow: "ci.yml".into(),
                gcit_run_id: Uuid::nil(),
                run_ids: vec![1001, 1002, 1003],
            },
        )]);
        let params = build_test_params(BTreeMap::new());
        let (state_tx, _state_rx) = mpsc::channel::<StateUpdate>(8);
        let cancel = CancellationToken::new();
        let mut monitors: JoinSet<()> = JoinSet::new();
        let err = handle_trigger_with_executor(
            params,
            unit_test_trigger(),
            state_tx,
            cancel,
            &mut monitors,
            &executor,
        )
        .await
        .expect_err("DuplicateMatch must surface as Err");
        assert!(err.starts_with("correlate:"));
        for id in [1001, 1002, 1003] {
            assert!(
                err.contains(&id.to_string()),
                "duplicate-match error must list run id {id}; got: {err}",
            );
        }
    }

    /// Hand-rolled recording notifier. The `tests/common/recording_notifier.rs`
    /// helper is gated behind the integration-test crate boundary, so
    /// in-file unit tests cannot import it.
    struct UnitRecordingNotifier {
        kind: &'static str,
        id: String,
        on_run_start_calls: tokio::sync::Mutex<usize>,
    }

    impl UnitRecordingNotifier {
        fn new(id: impl Into<String>) -> Self {
            Self {
                kind: "recording",
                id: id.into(),
                on_run_start_calls: tokio::sync::Mutex::new(0),
            }
        }
    }

    impl super::super::DynNotifier for UnitRecordingNotifier {
        fn kind(&self) -> &'static str {
            self.kind
        }
        fn id(&self) -> &str {
            &self.id
        }
        fn on_run_start<'a>(
            &'a self,
            _ctx: &'a crate::notify::RunContext,
            _cancel: &'a CancellationToken,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<crate::notify::NotifyOutcome, crate::notify::NotifyError>,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                *self.on_run_start_calls.lock().await += 1;
                Ok(crate::notify::NotifyOutcome::Sent {
                    receipt: "unit-recorded".to_string(),
                })
            })
        }
        fn on_job_complete<'a>(
            &'a self,
            _ctx: &'a crate::notify::RunContext,
            _job: &'a crate::github::JobResult,
            _cancel: &'a CancellationToken,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<crate::notify::NotifyOutcome, crate::notify::NotifyError>,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                Ok(crate::notify::NotifyOutcome::Sent {
                    receipt: "unit-recorded".to_string(),
                })
            })
        }
        fn on_run_complete<'a>(
            &'a self,
            _ctx: &'a crate::notify::RunContext,
            _summary: &'a crate::github::RunSummary,
            _cancel: &'a CancellationToken,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<crate::notify::NotifyOutcome, crate::notify::NotifyError>,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                Ok(crate::notify::NotifyOutcome::Sent {
                    receipt: "unit-recorded".to_string(),
                })
            })
        }
    }

    #[tokio::test]
    async fn handle_trigger_with_executor_success_fires_on_run_start_via_spawn_fan_out() {
        let recorder = Arc::new(UnitRecordingNotifier::new("notifier-1"));
        let mut params_inner = (*build_test_params(BTreeMap::new())).clone();
        params_inner.notifiers = vec![Arc::clone(&recorder) as Arc<dyn super::super::DynNotifier>];
        let params = Arc::new(params_inner);

        let executor = UnitScriptedExecutor::new(vec![success_outcome(7777)]);
        let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
        let cancel = CancellationToken::new();
        let mut monitors: JoinSet<()> = JoinSet::new();
        handle_trigger_with_executor(
            params,
            unit_test_trigger(),
            state_tx,
            cancel.clone(),
            &mut monitors,
            &executor,
        )
        .await
        .expect("Success must NOT surface as Err");

        match state_rx.try_recv() {
            Ok(StateUpdate::RunStarted { run_id, .. }) => assert_eq!(run_id, 7777),
            other => panic!("expected RunStarted, got {other:?}"),
        }

        // Fire-and-forget fan-out needs a moment to run before we
        // assert on its side-effect counter.
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel.cancel();
        while monitors.join_next().await.is_some() {}

        let count = *recorder.on_run_start_calls.lock().await;
        assert_eq!(count, 1, "on_run_start must fire exactly once");
    }
}

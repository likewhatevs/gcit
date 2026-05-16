// Top-level dispatcher loop: receive triggers, hand each to
// `handle_trigger`, drain on cancel.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Bound on the cancel-path monitor drain. Matches the supervisor
/// reload drain timeout so a wedged monitor (e.g. blocked in an
/// uncancellable octocrab HTTP retry) does not block shutdown.
const MONITOR_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

use crate::flow::supervisor::{record_last_error, FlowLastError};
use crate::flow::TriggerSignal;
use crate::state::StateUpdate;

use super::executor::DispatchExecutor;
use super::handle::handle_trigger;
use super::FlowDispatchParams;

/// Generic dispatch-loop driver. Production goes through
/// `super::run`, which constructs a `RealDispatchExecutor`; the
/// supervisor end-to-end test harness wires a scripted executor.
#[doc(hidden)]
pub async fn run_with_executor<E: DispatchExecutor>(
    params: FlowDispatchParams,
    executor: E,
    mut trigger_rx: Receiver<TriggerSignal>,
    state_tx: Sender<StateUpdate>,
    last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>>,
    cancel: CancellationToken,
) {
    info!(
        target: "gcit::flow::dispatcher",
        flow = %params.flow_name,
        "dispatcher loop starting",
    );
    let mut monitors: JoinSet<()> = JoinSet::new();
    let params = Arc::new(params);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                // Drain triggers the poll task enqueued before
                // observing cancel itself, so they aren't lost
                // forever (state.json already advanced because
                // poll's observation send is unraced after a
                // successful trigger send). Each drained trigger
                // propagates the same cancel token, so a wedged
                // GitHub round-trip short-circuits quickly.
                let drained = drain_pending_triggers(
                    &params,
                    &mut trigger_rx,
                    &state_tx,
                    &last_errors,
                    &cancel,
                    &mut monitors,
                    &executor,
                ).await;
                info!(
                    target: "gcit::flow::dispatcher",
                    flow = %params.flow_name,
                    in_flight_monitors = monitors.len(),
                    drained_triggers = drained,
                    "dispatcher cancelled; draining monitors",
                );
                drain_monitors_with_timeout(&mut monitors, &params.flow_name).await;
                return;
            }
            Some(_finished) = monitors.join_next(), if !monitors.is_empty() => {
                // Monitor task completed; it logs its own outcome.
            }
            recv = trigger_rx.recv() => {
                let Some(trigger) = recv else {
                    info!(
                        target: "gcit::flow::dispatcher",
                        flow = %params.flow_name,
                        "trigger channel closed; dispatcher exiting",
                    );
                    drain_monitors_with_timeout(&mut monitors, &params.flow_name).await;
                    return;
                };
                if let Err(e) = handle_trigger(
                    Arc::clone(&params),
                    trigger,
                    state_tx.clone(),
                    cancel.clone(),
                    &mut monitors,
                    &executor,
                )
                .await
                {
                    warn!(
                        target: "gcit::flow::dispatcher",
                        flow = %params.flow_name,
                        error = %e,
                        "dispatch failed",
                    );
                    record_last_error(
                        &last_errors,
                        &params.flow_name,
                        e.kind,
                        &e.message,
                        e.retry_at,
                    )
                    .await;
                }
            }
        }
    }
}

/// Drain the per-run monitors with a timeout. If a monitor wedges
/// (e.g. blocked in an uncancellable HTTP retry), proceed after
/// `MONITOR_DRAIN_TIMEOUT` and log so the leftover task does not
/// block daemon shutdown.
async fn drain_monitors_with_timeout(monitors: &mut JoinSet<()>, flow_name: &str) {
    let drain = async { while monitors.join_next().await.is_some() {} };
    if tokio::time::timeout(MONITOR_DRAIN_TIMEOUT, drain)
        .await
        .is_err()
    {
        warn!(
            target: "gcit::flow::dispatcher",
            flow = %flow_name,
            remaining_monitors = monitors.len(),
            "monitor drain timed out after 30s; proceeding without waiting on leftover tasks",
        );
    }
}

/// Drain triggers enqueued before cancel was observed. Each is
/// processed via `handle_trigger`; the propagated cancel token
/// short-circuits the GitHub round-trip when the network path can
/// observe cancellation. Returns the drained count for log fidelity.
async fn drain_pending_triggers<E: DispatchExecutor>(
    params: &Arc<FlowDispatchParams>,
    trigger_rx: &mut Receiver<TriggerSignal>,
    state_tx: &Sender<StateUpdate>,
    last_errors: &Arc<Mutex<BTreeMap<String, FlowLastError>>>,
    cancel: &CancellationToken,
    monitors: &mut JoinSet<()>,
    executor: &E,
) -> usize {
    let mut count = 0usize;
    loop {
        match trigger_rx.try_recv() {
            Ok(trigger) => {
                count += 1;
                if let Err(e) = handle_trigger(
                    Arc::clone(params),
                    trigger,
                    state_tx.clone(),
                    cancel.clone(),
                    monitors,
                    executor,
                )
                .await
                {
                    warn!(
                        target: "gcit::flow::dispatcher",
                        flow = %params.flow_name,
                        error = %e,
                        "dispatch failed during cancel drain",
                    );
                    record_last_error(
                        last_errors,
                        &params.flow_name,
                        e.kind,
                        &e.message,
                        e.retry_at,
                    )
                    .await;
                }
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            | Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => return count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ActionConfig, CredentialId};
    use crate::git::rate_bucket::RateBucket;
    use crate::github::client::Client as GithubClient;
    use crate::github::dispatcher::DispatchParams;
    use crate::github::error::GithubErrorKind;
    use crate::github::rate_limit::RateLimitState;
    use crate::util::ensure_crypto_provider;
    use chrono::Utc;
    use std::time::Duration;
    use tokio::sync::mpsc;

    /// Pre-baked executor for the drain tests.
    struct UnitScriptedExecutor {
        outcomes: tokio::sync::Mutex<Vec<super::super::executor::ExecuteOutcome>>,
    }

    impl UnitScriptedExecutor {
        fn new(outcomes: Vec<super::super::executor::ExecuteOutcome>) -> Self {
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
        ) -> super::super::executor::ExecuteOutcome {
            self.outcomes
                .lock()
                .await
                .pop()
                .expect("UnitScriptedExecutor: outcome queue exhausted")
        }
    }

    fn build_test_params() -> Arc<FlowDispatchParams> {
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
                inputs: BTreeMap::new(),
            },
            destinations: Vec::new(),
            github_client,
            rate_bucket: Arc::new(RateBucket::new(Duration::from_millis(0))),
            rate_limit: Arc::new(RateLimitState::new()),
            job_interval: Duration::from_secs(30),
            notifiers: Vec::new(),
        })
    }

    fn unit_test_trigger() -> TriggerSignal {
        let head_sha = gix_hash::ObjectId::from_hex(b"deadbeefcafe1234567890abcdef1234567890ab")
            .expect("valid 40-hex sha");
        TriggerSignal {
            observed_sha: head_sha,
            observed_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn drain_pending_triggers_processes_each_buffered_trigger_and_records_last_error_per_failed_dispatch(
    ) {
        let params = build_test_params();
        let (trigger_tx, mut trigger_rx) =
            mpsc::channel::<TriggerSignal>(crate::flow::TRIGGER_QUEUE);
        for _ in 0..3 {
            trigger_tx.try_send(unit_test_trigger()).expect("buffer ok");
        }
        drop(trigger_tx);

        let executor = UnitScriptedExecutor::new(vec![
            super::super::executor::ExecuteOutcome::DispatchFailed(GithubErrorKind::Unauthorized {
                credential: CredentialId::new("github_pat").expect("valid id"),
            }),
            super::super::executor::ExecuteOutcome::DispatchFailed(GithubErrorKind::Unauthorized {
                credential: CredentialId::new("github_pat").expect("valid id"),
            }),
            super::super::executor::ExecuteOutcome::DispatchFailed(GithubErrorKind::Unauthorized {
                credential: CredentialId::new("github_pat").expect("valid id"),
            }),
        ]);
        let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(8);
        let last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>> =
            Arc::new(Mutex::new(BTreeMap::new()));
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut monitors: JoinSet<()> = JoinSet::new();

        let count = drain_pending_triggers(
            &params,
            &mut trigger_rx,
            &state_tx,
            &last_errors,
            &cancel,
            &mut monitors,
            &executor,
        )
        .await;

        assert_eq!(count, 3, "drain must consume every buffered trigger");
        assert!(
            matches!(
                trigger_rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected),
            ),
            "drain leaves the receiver empty + disconnected",
        );
        // record_last_error uses BTreeMap::insert; the last drained
        // failure overwrites earlier entries.
        let errs = last_errors.lock().await;
        let entry = errs.get(&params.flow_name).expect("last_error recorded");
        assert_eq!(entry.kind(), "dispatch");
        assert!(entry.message().starts_with("dispatch:"));
        assert!(
            state_rx.try_recv().is_err(),
            "drain failures emit no RunStarted"
        );
        assert!(monitors.is_empty());
    }

    #[tokio::test]
    async fn drain_pending_triggers_returns_zero_count_on_empty_channel() {
        let params = build_test_params();
        let (trigger_tx, mut trigger_rx) =
            mpsc::channel::<TriggerSignal>(crate::flow::TRIGGER_QUEUE);
        drop(trigger_tx);

        // Empty queue — handle_trigger would panic if invoked.
        let executor = UnitScriptedExecutor::new(Vec::new());
        let (state_tx, _state_rx) = mpsc::channel::<StateUpdate>(8);
        let last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>> =
            Arc::new(Mutex::new(BTreeMap::new()));
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut monitors: JoinSet<()> = JoinSet::new();

        let count = drain_pending_triggers(
            &params,
            &mut trigger_rx,
            &state_tx,
            &last_errors,
            &cancel,
            &mut monitors,
            &executor,
        )
        .await;

        assert_eq!(count, 0);
        assert!(last_errors.lock().await.is_empty());
        assert!(monitors.is_empty());
    }
}

// Respawn request driver: consumes `RespawnRequest`s from the
// panic-watcher mpsc and either re-enqueues (old-gen pair not yet
// drained) or spawns the new generation via `flows::spawn_flow`.

use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::config::Config;

use super::super::control::ControlHandler;
use super::super::flows::{spawn_flow, SpawnContext};
use super::super::types::{FlowExit, FlowRegistry};

use super::{RespawnRequest, RESPAWN_MAX_ATTEMPTS, RESPAWN_RETRY_INTERVAL};

/// Process a `RespawnRequest` enqueued by the panic-watcher task.
/// Reads the current config from `config_watch`; if the flow is
/// still present + enabled, spawns it. Otherwise logs and skips —
/// the operator's reload wins over a panic-respawn.
///
/// Refreshes `control_handler.flow_names` after a successful spawn
/// so the respawned flow stops showing "not running" via
/// `gcit status`.
pub(crate) async fn handle_respawn_request(
    req: RespawnRequest,
    config_watch: Arc<watch::Sender<Arc<Config>>>,
    ctx: &SpawnContext,
    respawn_tx: mpsc::Sender<RespawnRequest>,
    join_set: &mut JoinSet<FlowExit>,
    registry: &mut FlowRegistry,
    control_handler: &Arc<ControlHandler>,
) {
    // Drain gate: if old-gen pair has not fully exited, defer via a
    // short timer so old-gen + new-gen don't briefly share the
    // writer mpsc. Bounded by RESPAWN_MAX_ATTEMPTS so a wedged
    // old-gen doesn't block forever.
    if registry.pending_exits.contains_key(&req.flow) {
        if req.attempts >= RESPAWN_MAX_ATTEMPTS {
            warn!(
                target: "gcit::supervisor",
                flow = %req.flow,
                attempts = req.attempts,
                "respawn drain budget exhausted; old-gen pair still has unobserved exits — proceeding anyway (writer LWW handles brief overlap)",
            );
            // Fall through to spawn_flow.
        } else {
            let next = RespawnRequest {
                flow: req.flow.clone(),
                attempts: req.attempts + 1,
            };
            let tx = respawn_tx.clone();
            let cancel = ctx.root_cancel.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(RESPAWN_RETRY_INTERVAL) => {}
                }
                let _ = tx.send(next).await;
            });
            // Do NOT release respawning_flows here — the re-enqueue
            // still represents the same panic-respawn cycle.
            return;
        }
    }

    // Release the slot: the panic-respawn lifecycle ends when the
    // request triggers a spawn (or is dropped). A subsequent panic
    // schedules a fresh cycle.
    registry.respawning_flows.remove(&req.flow);

    if registry.handles.contains_key(&req.flow) {
        // Operator respawned via SIGHUP — freshly-loaded entry wins.
        info!(
            target: "gcit::supervisor",
            flow = %req.flow,
            "respawn-request ignored: flow already running (likely respawned via reload)",
        );
        return;
    }
    let cfg = config_watch.borrow().clone();
    let Some(flow_cfg) = cfg.flow.iter().find(|f| f.name == req.flow) else {
        info!(
            target: "gcit::supervisor",
            flow = %req.flow,
            "flow removed from config during respawn delay; skipping",
        );
        return;
    };
    if !flow_cfg.enabled {
        info!(
            target: "gcit::supervisor",
            flow = %req.flow,
            "flow disabled in config during respawn delay; skipping",
        );
        return;
    }
    info!(
        target: "gcit::supervisor",
        flow = %req.flow,
        "respawning panicked flow",
    );
    spawn_flow(flow_cfg, cfg.as_ref(), ctx, join_set, registry).await;
    *control_handler.flow_names.write().await = registry.handles.keys().cloned().collect();
}

#[cfg(test)]
mod tests {
    use super::super::super::control::ControlCommand;
    use super::super::super::credentials::CredentialPool;
    use super::super::super::flows::{DispatchTaskFactory, PollTaskFactory};
    use super::super::super::types::{FlowHandle, FlowLastError};
    use super::*;
    use crate::config::{
        ActionConfig, CredentialId, FlowConfig, HttpConfig, LogConfig, PollDefaults, PollOverride,
        SourceConfig,
    };
    use crate::flow::TRIGGER_QUEUE;
    use crate::state::State;
    use std::collections::BTreeMap;
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;
    use tokio::sync::{Mutex, RwLock};
    use tokio_util::sync::CancellationToken;
    use tracing_test::traced_test;

    fn test_flow_config(name: &str, enabled: bool) -> FlowConfig {
        FlowConfig {
            name: name.to_string(),
            enabled,
            description: None,
            source: SourceConfig {
                url: format!("https://example.com/{name}.git"),
                ref_name: "refs/heads/main".to_string(),
                credential_id: None,
            },
            action: ActionConfig::GithubWorkflowDispatch {
                repo: "owner/repo".to_string(),
                workflow: "ci.yml".to_string(),
                ref_name: "refs/heads/main".to_string(),
                credential_id: CredentialId::new("gh").expect("valid id"),
                inputs: BTreeMap::new(),
            },
            destination: Vec::new(),
            poll: PollOverride::default(),
        }
    }

    fn test_config(flows: Vec<FlowConfig>) -> Config {
        Config {
            source_path: std::path::PathBuf::new(),
            poll: PollDefaults::default(),
            log: LogConfig::default(),
            http: HttpConfig::default(),
            flow: flows,
            credential_lines: BTreeMap::new(),
        }
    }

    fn no_op_poll_factory() -> PollTaskFactory {
        Arc::new(|_, _, _, _, _, _| {
            panic!("poll factory invoked unexpectedly: test exercises an early-return arm")
        })
    }

    fn no_op_dispatch_factory() -> DispatchTaskFactory {
        Arc::new(|_, _, _, _, _| {
            panic!("dispatch factory invoked unexpectedly: test exercises an early-return arm")
        })
    }

    fn test_flow_handle() -> (FlowHandle, CancellationToken) {
        let cancel = CancellationToken::new();
        let (trigger_tx, _trigger_rx) = mpsc::channel(TRIGGER_QUEUE);
        let handle = FlowHandle {
            cancel: cancel.clone(),
            trigger_tx,
        };
        (handle, cancel)
    }

    fn test_spawn_context(
        last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>>,
        state_mirror: Arc<StdMutex<State>>,
    ) -> SpawnContext {
        let (state_tx, _state_rx) = mpsc::channel(8);
        SpawnContext {
            credential_pool: Arc::new(RwLock::new(CredentialPool::default())),
            shared_reqwest: Arc::new(reqwest::Client::new()),
            hostname: Arc::new("ci-host".to_string()),
            state_tx,
            state_mirror,
            root_cancel: CancellationToken::new(),
            last_errors,
            poll_task_factory: no_op_poll_factory(),
            dispatch_task_factory: no_op_dispatch_factory(),
        }
    }

    fn test_control_handler(
        last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>>,
        state_mirror: Arc<StdMutex<State>>,
        flow_names: Vec<String>,
    ) -> Arc<ControlHandler> {
        let (cmd_tx, _cmd_rx) = mpsc::channel::<ControlCommand>(8);
        Arc::new(ControlHandler {
            cmd_tx,
            state_mirror,
            last_errors,
            flow_names: Arc::new(RwLock::new(flow_names)),
        })
    }

    async fn advance_past(delta: Duration) {
        tokio::time::sleep(Duration::from_millis(1)).await;
        tokio::time::advance(delta).await;
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn handle_respawn_request_drain_defer_re_enqueues_with_attempts_incremented_when_pending_exits_outstanding(
    ) {
        // Drain-defer: pending_exits non-empty + attempts < MAX.
        // Spawn a timer that re-enqueues with attempts+1; do NOT
        // release the respawning_flows slot.
        let mut registry = FlowRegistry::new();
        registry.respawning_flows.insert("flow1".to_string());
        registry.pending_exits.insert("flow1".to_string(), 2);

        let last_errors = Arc::new(Mutex::new(BTreeMap::new()));
        let state_mirror = Arc::new(StdMutex::new(State::default()));
        let ctx = test_spawn_context(Arc::clone(&last_errors), Arc::clone(&state_mirror));
        let cfg = Arc::new(test_config(vec![test_flow_config("flow1", true)]));
        let (config_watch_tx, _config_watch_rx) = watch::channel(Arc::clone(&cfg));
        let config_watch_tx = Arc::new(config_watch_tx);
        let control_handler = test_control_handler(Arc::clone(&last_errors), state_mirror, vec![]);
        let (respawn_tx, mut respawn_rx) = mpsc::channel::<RespawnRequest>(4);
        let mut join_set: JoinSet<FlowExit> = JoinSet::new();

        let req = RespawnRequest {
            flow: "flow1".to_string(),
            attempts: 5,
        };
        handle_respawn_request(
            req,
            Arc::clone(&config_watch_tx),
            &ctx,
            respawn_tx.clone(),
            &mut join_set,
            &mut registry,
            &control_handler,
        )
        .await;

        assert!(registry.respawning_flows.contains("flow1"));
        assert_eq!(join_set.len(), 0);

        advance_past(Duration::from_secs(2)).await;
        let next = respawn_rx
            .try_recv()
            .expect("drain-defer must re-enqueue after RESPAWN_RETRY_INTERVAL");
        assert_eq!(next.flow, "flow1");
        assert_eq!(next.attempts, 6);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[traced_test]
    async fn handle_respawn_request_drain_budget_exhausted_logs_warn_and_falls_through() {
        // At RESPAWN_MAX_ATTEMPTS, pending_exits non-empty BUT the
        // function falls through. Empty config sends us to the
        // removed-from-config arm.
        let mut registry = FlowRegistry::new();
        registry.respawning_flows.insert("flow1".to_string());
        registry.pending_exits.insert("flow1".to_string(), 1);

        let last_errors = Arc::new(Mutex::new(BTreeMap::new()));
        let state_mirror = Arc::new(StdMutex::new(State::default()));
        let ctx = test_spawn_context(Arc::clone(&last_errors), Arc::clone(&state_mirror));
        let cfg = Arc::new(test_config(vec![]));
        let (config_watch_tx, _config_watch_rx) = watch::channel(Arc::clone(&cfg));
        let config_watch_tx = Arc::new(config_watch_tx);
        let control_handler = test_control_handler(Arc::clone(&last_errors), state_mirror, vec![]);
        let (respawn_tx, mut respawn_rx) = mpsc::channel::<RespawnRequest>(4);
        let mut join_set: JoinSet<FlowExit> = JoinSet::new();

        let req = RespawnRequest {
            flow: "flow1".to_string(),
            attempts: RESPAWN_MAX_ATTEMPTS,
        };
        handle_respawn_request(
            req,
            Arc::clone(&config_watch_tx),
            &ctx,
            respawn_tx.clone(),
            &mut join_set,
            &mut registry,
            &control_handler,
        )
        .await;

        assert!(logs_contain("respawn drain budget exhausted"));
        assert!(!registry.respawning_flows.contains("flow1"));
        assert!(respawn_rx.try_recv().is_err());
        assert_eq!(join_set.len(), 0);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[traced_test]
    async fn handle_respawn_request_already_running_skips_with_tracing_event() {
        // Operator respawned via SIGHUP before panic-watcher fired.
        let mut registry = FlowRegistry::new();
        registry.respawning_flows.insert("flow1".to_string());
        let (handle, _cancel) = test_flow_handle();
        registry.handles.insert("flow1".to_string(), handle);

        let last_errors = Arc::new(Mutex::new(BTreeMap::new()));
        let state_mirror = Arc::new(StdMutex::new(State::default()));
        let ctx = test_spawn_context(Arc::clone(&last_errors), Arc::clone(&state_mirror));
        let cfg = Arc::new(test_config(vec![test_flow_config("flow1", true)]));
        let (config_watch_tx, _config_watch_rx) = watch::channel(Arc::clone(&cfg));
        let config_watch_tx = Arc::new(config_watch_tx);
        let control_handler = test_control_handler(Arc::clone(&last_errors), state_mirror, vec![]);
        let (respawn_tx, _respawn_rx) = mpsc::channel::<RespawnRequest>(4);
        let mut join_set: JoinSet<FlowExit> = JoinSet::new();

        handle_respawn_request(
            RespawnRequest {
                flow: "flow1".to_string(),
                attempts: 0,
            },
            Arc::clone(&config_watch_tx),
            &ctx,
            respawn_tx,
            &mut join_set,
            &mut registry,
            &control_handler,
        )
        .await;

        assert!(!registry.respawning_flows.contains("flow1"));
        assert!(registry.handles.contains_key("flow1"));
        assert_eq!(join_set.len(), 0);
        assert!(logs_contain(
            "respawn-request ignored: flow already running"
        ));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[traced_test]
    async fn handle_respawn_request_removed_from_config_skips_with_tracing_event() {
        // Flow gone from config after a reload.
        let mut registry = FlowRegistry::new();
        registry.respawning_flows.insert("flow1".to_string());

        let last_errors = Arc::new(Mutex::new(BTreeMap::new()));
        let state_mirror = Arc::new(StdMutex::new(State::default()));
        let ctx = test_spawn_context(Arc::clone(&last_errors), Arc::clone(&state_mirror));
        let cfg = Arc::new(test_config(vec![]));
        let (config_watch_tx, _config_watch_rx) = watch::channel(Arc::clone(&cfg));
        let config_watch_tx = Arc::new(config_watch_tx);
        let control_handler = test_control_handler(Arc::clone(&last_errors), state_mirror, vec![]);
        let (respawn_tx, _respawn_rx) = mpsc::channel::<RespawnRequest>(4);
        let mut join_set: JoinSet<FlowExit> = JoinSet::new();

        handle_respawn_request(
            RespawnRequest {
                flow: "flow1".to_string(),
                attempts: 0,
            },
            Arc::clone(&config_watch_tx),
            &ctx,
            respawn_tx,
            &mut join_set,
            &mut registry,
            &control_handler,
        )
        .await;

        assert!(!registry.respawning_flows.contains("flow1"));
        assert_eq!(join_set.len(), 0);
        assert!(logs_contain(
            "flow removed from config during respawn delay; skipping"
        ));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[traced_test]
    async fn handle_respawn_request_disabled_during_respawn_skips_with_tracing_event() {
        // Flow disabled after a reload.
        let mut registry = FlowRegistry::new();
        registry.respawning_flows.insert("flow1".to_string());

        let last_errors = Arc::new(Mutex::new(BTreeMap::new()));
        let state_mirror = Arc::new(StdMutex::new(State::default()));
        let ctx = test_spawn_context(Arc::clone(&last_errors), Arc::clone(&state_mirror));
        let cfg = Arc::new(test_config(vec![test_flow_config("flow1", false)]));
        let (config_watch_tx, _config_watch_rx) = watch::channel(Arc::clone(&cfg));
        let config_watch_tx = Arc::new(config_watch_tx);
        let control_handler = test_control_handler(Arc::clone(&last_errors), state_mirror, vec![]);
        let (respawn_tx, _respawn_rx) = mpsc::channel::<RespawnRequest>(4);
        let mut join_set: JoinSet<FlowExit> = JoinSet::new();

        handle_respawn_request(
            RespawnRequest {
                flow: "flow1".to_string(),
                attempts: 0,
            },
            Arc::clone(&config_watch_tx),
            &ctx,
            respawn_tx,
            &mut join_set,
            &mut registry,
            &control_handler,
        )
        .await;

        assert!(!registry.respawning_flows.contains("flow1"));
        assert_eq!(join_set.len(), 0);
        assert!(logs_contain(
            "flow disabled in config during respawn delay; skipping"
        ));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn handle_respawn_request_no_pending_exits_proceeds_to_post_drain_arms() {
        // pending_exits empty → skip drain-defer → release slot →
        // proceed to already-running / removed / disabled checks.
        let mut registry = FlowRegistry::new();
        registry.respawning_flows.insert("flow1".to_string());
        assert!(registry.pending_exits.is_empty());

        let last_errors = Arc::new(Mutex::new(BTreeMap::new()));
        let state_mirror = Arc::new(StdMutex::new(State::default()));
        let ctx = test_spawn_context(Arc::clone(&last_errors), Arc::clone(&state_mirror));
        let cfg = Arc::new(test_config(vec![]));
        let (config_watch_tx, _config_watch_rx) = watch::channel(Arc::clone(&cfg));
        let config_watch_tx = Arc::new(config_watch_tx);
        let control_handler = test_control_handler(Arc::clone(&last_errors), state_mirror, vec![]);
        let (respawn_tx, mut respawn_rx) = mpsc::channel::<RespawnRequest>(4);
        let mut join_set: JoinSet<FlowExit> = JoinSet::new();

        handle_respawn_request(
            RespawnRequest {
                flow: "flow1".to_string(),
                attempts: 0,
            },
            Arc::clone(&config_watch_tx),
            &ctx,
            respawn_tx.clone(),
            &mut join_set,
            &mut registry,
            &control_handler,
        )
        .await;

        assert!(!registry.respawning_flows.contains("flow1"));
        advance_past(Duration::from_secs(2)).await;
        assert!(respawn_rx.try_recv().is_err());
    }
}

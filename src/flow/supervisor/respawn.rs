// Per-flow panic-respawn pipeline: classifies JoinSet exits and drives
// the supervisor's respawn-after-RESPAWN_DELAY path.
//
// `decide_respawn` is the pure classifier (no side effects, unit-
// testable without standing up a JoinSet) consumed by `handle_flow_exit`.
// `handle_flow_exit` performs the side effects (touch FlowRegistry,
// record last_error, arm the panic-watcher task). `handle_respawn_request`
// drains a `RespawnRequest` from the supervisor's mpsc and either
// re-enqueues (old-gen pair not yet drained) or spawns the new
// generation via `flows::spawn_flow`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::Config;

use super::control::ControlHandler;
use super::flows::{spawn_flow, SpawnContext};
use super::types::{record_last_error, FlowExit, FlowLastError, FlowRegistry};
use crate::flow::RESPAWN_DELAY;

/// Cap on `RespawnRequest::attempts`. With `RESPAWN_RETRY_INTERVAL ==
/// 1s` and `RESPAWN_MAX_ATTEMPTS == 30`, the maximum drain-defer wait
/// is 30 seconds — symmetric with the 30s timeout in `run_reload`'s
/// drain block.
const RESPAWN_MAX_ATTEMPTS: u32 = 30;

/// Polling interval between drain checks when a wedged old-gen pair
/// has not yet exited. Each retry re-enqueues the request after this
/// delay; the supervisor's select! loop processes the request
/// (decoupling the wait from the loop body).
const RESPAWN_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Per-flow respawn request. Carries the flow name plus an `attempts`
/// counter that bounds drain-deferral retries when the old-gen
/// poll/dispatcher pair has not yet fully exited the JoinSet.
///
/// Drain deferral: the panic-watcher task sleeps RESPAWN_DELAY and
/// enqueues a request, but a wedged old-gen sibling (cancelled and
/// stuck in HTTP retry past the cancel deadline) may still be holding
/// a `state_tx.clone()` when the request reaches the supervisor.
/// `handle_respawn_request` checks `pending_exits[flow]` and, if the
/// old-gen pair is not fully drained, re-enqueues the request via a
/// short timer. Each re-enqueue increments `attempts`; once
/// `RESPAWN_MAX_ATTEMPTS` is hit the supervisor proceeds anyway and
/// logs a warning — same trade-off as the run_reload drain timeout
/// (the writer's per-variant LWW handles the brief overlap).
///
/// Supervisor select! responsiveness is preserved: each retry's timer
/// runs in its own spawned task, the supervisor only sees the
/// resulting `respawn_rx.recv()`.
pub(super) struct RespawnRequest {
    flow: String,
    attempts: u32,
}

/// Pure decision: given the exit shape and whether the flow is already
/// in the respawn pipeline, what should the supervisor do?
///
/// Extracted so the panic-race scenarios — particularly clean-exit-then-
/// panic, where the order in which the supervisor observes the two
/// FlowExits matters — can be unit-tested without standing up a JoinSet.
/// The caller (`handle_flow_exit`) translates each decision into the
/// concrete side effects (touching `handles`, `respawning_flows`, the
/// last_errors map, and the respawn-watcher task).
#[derive(Debug, PartialEq, Eq)]
enum RespawnDecision {
    /// Tokio reported a non-panic, non-cancelled JoinError. Should not
    /// happen given the catch_unwind wrapping, but log and continue.
    UnexpectedJoinError,
    /// The supervisor cancelled the task (reload or shutdown). No
    /// action required — handles is already being drained by the
    /// caller (run_reload).
    Cancelled,
    /// Inner future returned cleanly (trigger channel closed, dispatcher
    /// drained naturally, etc.). Drop the handle entry; do NOT touch
    /// `respawning_flows` because the OTHER role of this flow may still
    /// be in the panic-respawn pipeline and its respawn must proceed.
    CleanExit,
    /// Inner future panicked but the sibling role of this flow is
    /// already in the respawn pipeline (its panic-watcher will fire a
    /// RespawnRequest). Skip the duplicate.
    PanicDuplicate,
    /// Inner future panicked and this is the first observation. Cancel
    /// the surviving sibling, mark the flow as respawning, and arm the
    /// panic-watcher task.
    PanicFirst,
}

/// Pure classifier for a JoinSet result. Inspects the exit shape and
/// the current `respawning_flows` membership and returns the next
/// action without performing any side effect. The caller mutates state
/// based on the decision.
///
/// `already_respawning` reflects the snapshot BEFORE this exit is
/// observed — the side-effect step (insert into `respawning_flows`)
/// happens in the caller after a `PanicFirst` decision.
fn decide_respawn(
    joined: &Result<FlowExit, tokio::task::JoinError>,
    already_respawning: bool,
) -> RespawnDecision {
    let exit = match joined {
        Ok(e) => e,
        Err(join_err) if join_err.is_cancelled() => return RespawnDecision::Cancelled,
        Err(_) => return RespawnDecision::UnexpectedJoinError,
    };
    if exit.panic.is_none() {
        return RespawnDecision::CleanExit;
    }
    if already_respawning {
        RespawnDecision::PanicDuplicate
    } else {
        RespawnDecision::PanicFirst
    }
}

/// Handle a flow task's exit. Drives the panic-respawn path: on
/// panic, schedule a respawn via the `respawn_tx` channel after
/// `RESPAWN_DELAY`. The actual `spawn_flow` call happens back in the
/// supervisor's select! arm so the loop stays responsive to
/// SIGTERM/SIGHUP/control commands during the 30-second window.
///
/// `respawning_flows` is the authoritative source of truth for "this
/// flow is between panic-observation and respawn". We dedup duplicate
/// respawns from this set rather than from `handles` membership: if
/// the poll role panics while the dispatcher role exits cleanly, the
/// clean exit removes the handle entry, and a check against
/// `handles` membership would then incorrectly classify the still-
/// pending panic as "already respawning" and skip the respawn,
/// killing the flow forever.
pub(super) async fn handle_flow_exit(
    joined: Result<FlowExit, tokio::task::JoinError>,
    last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>>,
    respawn_tx: mpsc::Sender<RespawnRequest>,
    registry: &mut FlowRegistry,
    root_cancel: &CancellationToken,
) {
    let exit_flow_role = joined.as_ref().ok().map(|e| (e.flow.clone(), e.role));
    // Decrement pending_exits on every observed exit. The Ok(FlowExit)
    // case carries the flow name; Err(JoinError::is_cancelled) does
    // not, but in practice the catch_unwind wrapper around each
    // spawned future converts cancellation into Ok(FlowExit{panic:
    // None}) so this branch is rare. The decrement happens BEFORE
    // any other side effect so a respawn-request that arrives in the
    // same select! tick observes the updated count.
    if let Some((flow, _)) = &exit_flow_role {
        if let Some(count) = registry.pending_exits.get_mut(flow) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                registry.pending_exits.remove(flow);
            }
        }
    }
    // Decision is taken from the snapshot BEFORE any mutation; the
    // panic-first arm performs the `respawning_flows.insert` itself.
    let decision = {
        let already = exit_flow_role
            .as_ref()
            .is_some_and(|(name, _)| registry.respawning_flows.contains(name));
        decide_respawn(&joined, already)
    };
    match decision {
        RespawnDecision::Cancelled => return,
        RespawnDecision::UnexpectedJoinError => {
            // The spawned future wraps in catch_unwind so panics
            // surface as Ok(FlowExit{panic:Some}). A non-cancelled
            // JoinError here means tokio reported a panic that escaped
            // catch_unwind — genuinely a bug. Log and continue.
            if let Err(join_err) = joined {
                warn!(
                    target: "gcit::supervisor",
                    error = %join_err,
                    "flow task join failed (not panic, not cancelled — bug?)",
                );
            }
            return;
        }
        RespawnDecision::CleanExit => {
            // Cancellation or natural completion of the inner future.
            // Drop the FlowHandle entry; the cancel token is already
            // spent. Do NOT touch `respawning_flows`: a clean exit of
            // one role does not affect the panic-respawn state of the
            // other role; if the other role is panicking, its respawn
            // must still proceed.
            if let Some((flow, role)) = &exit_flow_role {
                info!(
                    target: "gcit::supervisor",
                    flow = %flow,
                    role = ?role,
                    "flow task exited normally",
                );
                registry.handles.remove(flow);
            }
            return;
        }
        RespawnDecision::PanicDuplicate | RespawnDecision::PanicFirst => {}
    }

    // Panic arms below: extract the exit's identity for logging and
    // last_error recording.
    let exit = match joined {
        Ok(e) => e,
        // UnexpectedJoinError + Cancelled both returned above; the
        // panic decisions can only originate from Ok(FlowExit{...}).
        Err(_) => unreachable!("decide_respawn returned panic arm for non-Ok join"),
    };
    let panic_message = match exit.panic.as_ref() {
        Some(m) => m.clone(),
        // CleanExit returned above; panic arms imply Some.
        None => unreachable!("decide_respawn returned panic arm for clean exit"),
    };
    warn!(
        target: "gcit::supervisor",
        flow = %exit.flow,
        role = ?exit.role,
        panic = %panic_message,
        "flow task PANICKED; respawn scheduled after RESPAWN_DELAY",
    );
    record_last_error(&last_errors, &exit.flow, "panic", &panic_message, None).await;

    if matches!(decision, RespawnDecision::PanicDuplicate) {
        // The OTHER role of this flow already saw the panic and is
        // already in the respawn pipeline; skip the duplicate.
        info!(
            target: "gcit::supervisor",
            flow = %exit.flow,
            role = ?exit.role,
            "sibling role already respawning; skipping duplicate",
        );
        return;
    }

    // PanicFirst: insert into respawning_flows, drop the FlowHandle (if
    // any), cancel the surviving sibling so the respawn brings up a
    // fresh pair. The handle may already be gone (clean exit of the
    // sibling raced ahead of this panic); `cancel.cancel()` only runs
    // when a handle actually exists.
    registry.respawning_flows.insert(exit.flow.clone());
    if let Some(h) = registry.handles.remove(&exit.flow) {
        h.cancel.cancel();
    }

    // Spawn a small task that sleeps RESPAWN_DELAY then enqueues a
    // RespawnRequest. The supervisor's select! loop receives the
    // request and calls spawn_flow synchronously — the sleep happens
    // here, off the select! path, so SIGTERM/SIGHUP/control commands
    // are not delayed. The sleep races against `root_cancel` so a
    // daemon shutdown during the 30s window collapses the watcher
    // immediately rather than holding the spawned task open.
    let flow = exit.flow.clone();
    let tx = respawn_tx.clone();
    let cancel = root_cancel.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(RESPAWN_DELAY) => {}
        }
        let _ = tx.send(RespawnRequest { flow, attempts: 0 }).await;
    });
}

/// Process a `RespawnRequest` enqueued by the panic-watcher task.
/// Reads the current config from `config_watch`; if the flow is still
/// present + enabled, spawn it. Otherwise log and skip — the
/// operator's reload wins over a panic-respawn.
///
/// Refreshes `control_handler.flow_names` after a successful spawn
/// so a respawned flow stops showing "not running" via `gcit status`.
pub(super) async fn handle_respawn_request(
    req: RespawnRequest,
    config_watch: Arc<watch::Sender<Arc<Config>>>,
    ctx: &SpawnContext,
    respawn_tx: mpsc::Sender<RespawnRequest>,
    join_set: &mut JoinSet<FlowExit>,
    registry: &mut FlowRegistry,
    control_handler: &Arc<ControlHandler>,
) {
    // Drain gate: if the old-gen poll/dispatcher pair has not fully
    // exited the JoinSet, both old-gen and new-gen would briefly
    // share the writer mpsc and race on PollObservation/RunStarted/
    // RunFinished. Defer the respawn until pending_exits[flow] is
    // empty — re-enqueue via a short timer so the supervisor's
    // select! loop stays responsive during the wait. Bounded by
    // RESPAWN_MAX_ATTEMPTS so a wedged old-gen task doesn't block
    // the respawn forever (after the budget the supervisor proceeds
    // and logs a warning, mirroring the run_reload drain timeout).
    if registry.pending_exits.contains_key(&req.flow) {
        if req.attempts >= RESPAWN_MAX_ATTEMPTS {
            warn!(
                target: "gcit::supervisor",
                flow = %req.flow,
                attempts = req.attempts,
                "respawn drain budget exhausted; old-gen pair still has unobserved exits — proceeding anyway (writer LWW handles brief overlap)",
            );
            // Fall through to spawn_flow. respawning_flows slot is
            // released below.
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
            // Do NOT release the respawning_flows slot here — the
            // re-enqueued request still represents the same panic-
            // respawn cycle. Releasing now would let a fresh panic
            // on the same flow schedule a duplicate respawn before
            // the original one completes.
            return;
        }
    }

    // Release the slot: the panic-respawn lifecycle ends when the
    // request actually triggers a spawn (or is dropped because the
    // flow is no longer in config / disabled / already running). A
    // subsequent panic on the same flow schedules a fresh respawn
    // cycle without being deduped against this slot.
    registry.respawning_flows.remove(&req.flow);

    if registry.handles.contains_key(&req.flow) {
        // The operator must have respawned the flow already (e.g.
        // via SIGHUP after the panic). Skip — the freshly-loaded
        // entry takes precedence.
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
    // Refresh the control handler's flow-name list so the respawned
    // flow appears in `gcit status` output.
    *control_handler.flow_names.write().await = registry.handles.keys().cloned().collect();
}

#[cfg(test)]
mod tests {
    use super::super::types::FlowRole;
    use super::*;

    fn poll_clean(name: &str) -> Result<FlowExit, tokio::task::JoinError> {
        Ok(FlowExit {
            flow: name.to_string(),
            role: FlowRole::Poll,
            panic: None,
        })
    }

    fn dispatcher_clean(name: &str) -> Result<FlowExit, tokio::task::JoinError> {
        Ok(FlowExit {
            flow: name.to_string(),
            role: FlowRole::Dispatcher,
            panic: None,
        })
    }

    fn poll_panic(name: &str, msg: &str) -> Result<FlowExit, tokio::task::JoinError> {
        Ok(FlowExit {
            flow: name.to_string(),
            role: FlowRole::Poll,
            panic: Some(msg.to_string()),
        })
    }

    fn dispatcher_panic(name: &str, msg: &str) -> Result<FlowExit, tokio::task::JoinError> {
        Ok(FlowExit {
            flow: name.to_string(),
            role: FlowRole::Dispatcher,
            panic: Some(msg.to_string()),
        })
    }

    #[test]
    fn decide_respawn_clean_exit_returns_clean_exit() {
        let r = poll_clean("flow1");
        assert_eq!(decide_respawn(&r, false), RespawnDecision::CleanExit);
    }

    #[test]
    fn decide_respawn_clean_exit_with_sibling_already_respawning_returns_clean_exit() {
        // Even when respawning_flows already lists the flow (because
        // the sibling panicked first), the clean exit of the OTHER
        // role is still classified as CleanExit. The sibling's
        // panic-watcher carries the respawn forward; this clean exit
        // just drops its FlowHandle.
        let r = dispatcher_clean("flow1");
        assert_eq!(decide_respawn(&r, true), RespawnDecision::CleanExit);
    }

    #[test]
    fn decide_respawn_first_panic_returns_panic_first() {
        let r = poll_panic("flow1", "boom");
        assert_eq!(decide_respawn(&r, false), RespawnDecision::PanicFirst);
    }

    #[test]
    fn decide_respawn_duplicate_panic_returns_panic_duplicate() {
        // Sibling already in respawn pipeline — second panic is a
        // duplicate, deduped to PanicDuplicate.
        let r = dispatcher_panic("flow1", "boom");
        assert_eq!(decide_respawn(&r, true), RespawnDecision::PanicDuplicate);
    }

    #[test]
    fn decide_respawn_panic_race_clean_exit_observed_first_then_panic_returns_panic_first() {
        // The panic-race scenario the explicit `respawning_flows` set
        // exists to handle: poll cleanly returns first; supervisor
        // calls handle_flow_exit -> CleanExit, removes the FlowHandle.
        // Then dispatcher panics; supervisor calls handle_flow_exit
        // again. Because `handles` no longer contains the flow (the
        // clean exit dropped it), an inference based on handle
        // membership would incorrectly skip the respawn. The pure
        // decide_respawn looks ONLY at the snapshot of
        // respawning_flows, which is still empty (clean exits do not
        // touch it). Decision: PanicFirst — the respawn proceeds.
        let clean = poll_clean("flow1");
        assert_eq!(decide_respawn(&clean, false), RespawnDecision::CleanExit);
        // Caller observes CleanExit; it removes the handle but does
        // NOT insert into respawning_flows. The next exit (the panic)
        // arrives with `already_respawning = false`.
        let panicked = dispatcher_panic("flow1", "boom");
        assert_eq!(
            decide_respawn(&panicked, false),
            RespawnDecision::PanicFirst
        );
    }

    #[test]
    fn decide_respawn_panic_then_clean_exit_classifies_clean_exit_with_already_respawning() {
        // Reverse ordering: dispatcher panics first (PanicFirst,
        // caller inserts into respawning_flows). Then poll cleanly
        // returns. Even though `already_respawning = true` for the
        // clean exit, the decision is still CleanExit — clean exits
        // are independent of panic state.
        let panicked = dispatcher_panic("flow1", "boom");
        assert_eq!(
            decide_respawn(&panicked, false),
            RespawnDecision::PanicFirst
        );
        // Caller now has flow1 in respawning_flows.
        let clean = poll_clean("flow1");
        assert_eq!(decide_respawn(&clean, true), RespawnDecision::CleanExit);
    }

    #[test]
    fn decide_respawn_join_cancelled_returns_cancelled() {
        // The supervisor cancelled the task via reload or shutdown.
        // tokio reports JoinError::is_cancelled = true.
        let cancelled: Result<FlowExit, tokio::task::JoinError> = {
            // Construct a JoinError representing cancellation: spawn a
            // task and abort it before it can run, then await the
            // join handle to recover the JoinError. Wrap in a
            // current-thread runtime since this is a sync test.
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("rt build");
            rt.block_on(async {
                let h = tokio::spawn(async {
                    // Sleep forever; the abort below cancels.
                    futures_util::future::pending::<FlowExit>().await
                });
                h.abort();
                h.await
            })
        };
        assert!(
            cancelled.as_ref().map(|_| ()).err().unwrap().is_cancelled(),
            "must be a cancellation JoinError",
        );
        assert_eq!(
            decide_respawn(&cancelled, false),
            RespawnDecision::Cancelled
        );
    }

    #[test]
    fn decide_respawn_uncaught_panic_returns_unexpected_join_error() {
        // The production catch_unwind wrapper around each spawned
        // future converts panics into Ok(FlowExit{panic:Some}) so
        // tokio's JoinHandle resolves with Ok. UnexpectedJoinError
        // is the defensive arm for a hypothetical bug where a panic
        // escapes catch_unwind and tokio reports it as a non-
        // cancelled JoinError. Construct that shape directly: spawn
        // a task that panics WITHOUT a catch_unwind wrapper, join
        // it, and verify the resulting JoinError is_panic = true /
        // is_cancelled = false. The classifier returns
        // UnexpectedJoinError.
        let panicked: Result<FlowExit, tokio::task::JoinError> = {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("rt build");
            rt.block_on(async {
                let h: tokio::task::JoinHandle<FlowExit> = tokio::spawn(async {
                    // Panic directly. Tokio's runtime catches the
                    // panic at the JoinHandle boundary and reports
                    // it via JoinError::is_panic. The diverging
                    // `panic!` produces `!`, which coerces to the
                    // annotated FlowExit return type so the type
                    // checker is happy.
                    panic!("test panic that escapes catch_unwind");
                });
                h.await
            })
        };
        let err = panicked.as_ref().expect_err("must be an Err");
        assert!(err.is_panic(), "must be a panic JoinError; got {err:?}",);
        assert!(
            !err.is_cancelled(),
            "panic JoinError must not be is_cancelled",
        );
        assert_eq!(
            decide_respawn(&panicked, false),
            RespawnDecision::UnexpectedJoinError,
        );
    }

    // -----------------------------------------------------------------
    // handle_flow_exit + handle_respawn_request side-effect tests.
    // The pure decide_respawn classifier is covered above; these tests
    // exercise the side-effect paths in handle_flow_exit (registry
    // mutation, last_error recording, watcher spawn, respawn_tx send)
    // and the early-return arms of handle_respawn_request (drain-defer,
    // already-running, removed-from-config, disabled-during-respawn,
    // drain budget exhausted).
    //
    // Tests live inside the same `mod tests` so they can construct
    // the pub(super) types (`FlowRegistry`, `FlowHandle`,
    // `RespawnRequest`, `SpawnContext`, `ControlHandler`) directly.
    // The pre-spawn_flow logic and early-return arms do NOT call into
    // the heavyweight credential/HTTP machinery, so the SpawnContext
    // built here uses default `CredentialPool`, an empty hostname, and
    // factories that panic if invoked (they must not be — see the
    // per-test assertions on the early-return arms).
    // -----------------------------------------------------------------

    use std::sync::Mutex as StdMutex;

    use tokio::sync::RwLock;
    use tracing_test::traced_test;

    use super::super::control::ControlCommand;
    use super::super::credentials::CredentialPool;
    use super::super::flows::{DispatchTaskFactory, PollTaskFactory};
    use super::super::types::FlowHandle;
    use crate::config::{
        ActionConfig, Config, CredentialId, FlowConfig, HttpConfig, LogConfig, PollDefaults,
        PollOverride, SourceConfig,
    };
    use crate::flow::TRIGGER_QUEUE;
    use crate::state::State;

    /// Build a minimal `FlowConfig` matching the shape `spawn_flow`
    /// would receive from a real config load. The action's
    /// `credential_id` is unused on the early-return paths
    /// (`handle_respawn_request` returns before calling `acquire_github`)
    /// but a valid id is required to satisfy `CredentialId::new`.
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

    /// Build a `Config` containing the supplied flows. Used by the
    /// `handle_respawn_request` early-return tests; the function reads
    /// `cfg.flow.iter().find(|f| f.name == req.flow)` to decide
    /// between the removed/disabled/spawn arms.
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

    /// Build a no-op `PollTaskFactory` that panics if invoked. Used in
    /// `SpawnContext`s for tests that exercise the early-return arms of
    /// `handle_respawn_request`. The factory is never reached on those
    /// arms — a panic here means the test wired the wrong arm.
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

    /// Build a `FlowHandle` whose cancel token + trigger_tx the test
    /// can observe. Returns the cancel token clone so the caller can
    /// poll `cancel.is_cancelled()` after the function under test
    /// runs.
    fn test_flow_handle() -> (FlowHandle, CancellationToken) {
        let cancel = CancellationToken::new();
        let (trigger_tx, _trigger_rx) = mpsc::channel(TRIGGER_QUEUE);
        let handle = FlowHandle {
            cancel: cancel.clone(),
            trigger_tx,
        };
        (handle, cancel)
    }

    /// Build a SpawnContext suitable for the early-return arms of
    /// `handle_respawn_request`. The factories panic if invoked; pass
    /// real factories via `with_factories` if a test needs to exercise
    /// the spawn fall-through.
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

    /// Build a ControlHandler suitable for the early-return arms of
    /// `handle_respawn_request`. The flow_names list is mutated by the
    /// successful-spawn arm; early-return arms leave it untouched.
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

    // -----------------------------------------------------------------
    // handle_flow_exit
    // -----------------------------------------------------------------

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[traced_test]
    async fn handle_flow_exit_panic_first_records_last_error_inserts_respawning_cancels_handle_and_enqueues_request() {
        // The PanicFirst arm of `handle_flow_exit` performs five
        // observable side effects that this test pins:
        //   1. record_last_error with kind="panic" and the panic message
        //   2. registry.respawning_flows.insert(flow)
        //   3. registry.handles[flow] removed (and its cancel token fired)
        //   4. tokio::spawn of the panic-watcher that sleeps RESPAWN_DELAY
        //      then sends a RespawnRequest with attempts=0
        //   5. pending_exits decrements by 1 (saturating_sub)
        let mut registry = FlowRegistry::new();
        let (handle, cancel) = test_flow_handle();
        registry.handles.insert("flow1".to_string(), handle);
        registry.pending_exits.insert("flow1".to_string(), 2);

        let last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>> =
            Arc::new(Mutex::new(BTreeMap::new()));
        let (respawn_tx, mut respawn_rx) = mpsc::channel::<RespawnRequest>(4);
        let root_cancel = CancellationToken::new();

        handle_flow_exit(
            poll_panic("flow1", "boom in poll loop"),
            Arc::clone(&last_errors),
            respawn_tx,
            &mut registry,
            &root_cancel,
        )
        .await;

        // (1) last_error recorded with kind="panic" and the original message body.
        let errs = last_errors.lock().await;
        let entry = errs.get("flow1").expect("last_error must be inserted");
        assert_eq!(entry.kind(), "panic");
        assert_eq!(entry.message(), "boom in poll loop");
        drop(errs);

        // (2) respawning_flows now contains the flow.
        assert!(
            registry.respawning_flows.contains("flow1"),
            "PanicFirst must insert flow into respawning_flows; got: {:?}",
            registry.respawning_flows,
        );

        // (3) The FlowHandle entry is removed from `handles` AND its
        // cancel token was fired so the surviving sibling task (which
        // shares this token) drains.
        assert!(
            !registry.handles.contains_key("flow1"),
            "PanicFirst must remove the FlowHandle entry",
        );
        assert!(
            cancel.is_cancelled(),
            "PanicFirst must fire the cancel token so the sibling task observes shutdown",
        );

        // (5) pending_exits decrements by 1 (was 2, now 1).
        assert_eq!(
            registry.pending_exits.get("flow1").copied(),
            Some(1),
            "pending_exits must decrement by 1 (was 2 -> 1); got: {:?}",
            registry.pending_exits,
        );

        // (4) The panic-watcher task is detached on the runtime; it
        // sleeps RESPAWN_DELAY then sends. Advance virtual time past
        // the delay and drain the receiver.
        //
        // tokio::time::sleep records its deadline as `now + D` at
        // FIRST POLL. The watcher task is queued but not yet polled
        // when handle_flow_exit returns; it polls only after the test
        // parks. Yield FIRST so the watcher's sleep starts at the
        // current virtual instant — otherwise an `advance` issued
        // before the watcher's first poll is consumed by nothing and
        // the sleep deadline lands at `(advance_target) + RESPAWN_DELAY`
        // (i.e. far in the future, past the test's budget).
        tokio::time::sleep(Duration::from_millis(1)).await;
        tokio::time::advance(RESPAWN_DELAY + Duration::from_millis(100)).await;
        // Yield again so the now-ready watcher gets polled.
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let req = respawn_rx
            .try_recv()
            .expect("panic-watcher must enqueue a RespawnRequest after RESPAWN_DELAY");
        assert_eq!(req.flow, "flow1");
        assert_eq!(
            req.attempts, 0,
            "fresh RespawnRequest must start with attempts=0; got {}",
            req.attempts,
        );

        // Tracing event from the panic-respawn `warn!` fires.
        assert!(
            logs_contain("flow task PANICKED; respawn scheduled after RESPAWN_DELAY"),
            "PanicFirst must emit the canonical respawn-scheduled tracing event",
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[traced_test]
    async fn handle_flow_exit_panic_duplicate_records_last_error_but_does_not_spawn_second_watcher() {
        // PanicDuplicate arm of `handle_flow_exit`: respawning_flows
        // already contains the flow (set by the first panic via the
        // sibling role's PanicFirst). The second panic still records
        // last_error (overwriting the first), but does NOT touch
        // respawning_flows, does NOT cancel any handle (the sibling
        // already cancelled it), and does NOT spawn a second
        // panic-watcher. Pin both the no-second-watcher invariant
        // (respawn_tx receives nothing across the full RESPAWN_DELAY
        // window) AND the dedup tracing event.
        let mut registry = FlowRegistry::new();
        registry.respawning_flows.insert("flow1".to_string());
        // No FlowHandle in `handles` — the sibling's PanicFirst already
        // removed it (see PanicFirst test above).

        let last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>> =
            Arc::new(Mutex::new(BTreeMap::new()));
        // Pre-seed last_error with the first panic's record so we can
        // assert the second panic OVERWRITES it (`record_last_error`
        // uses `BTreeMap::insert`, which replaces).
        record_last_error(&last_errors, "flow1", "panic", "first panic", None).await;

        let (respawn_tx, mut respawn_rx) = mpsc::channel::<RespawnRequest>(4);
        let root_cancel = CancellationToken::new();

        handle_flow_exit(
            dispatcher_panic("flow1", "second panic from sibling"),
            Arc::clone(&last_errors),
            respawn_tx,
            &mut registry,
            &root_cancel,
        )
        .await;

        // last_error overwritten with the second panic's message.
        let errs = last_errors.lock().await;
        let entry = errs.get("flow1").expect("last_error must be present");
        assert_eq!(entry.kind(), "panic");
        assert_eq!(
            entry.message(),
            "second panic from sibling",
            "PanicDuplicate must overwrite last_error with the second panic's body \
             (record_last_error uses insert, not entry().or_insert)",
        );
        drop(errs);

        // respawning_flows is unchanged (still contains the flow); the
        // first-panic's slot is the authoritative one.
        assert!(
            registry.respawning_flows.contains("flow1"),
            "PanicDuplicate must NOT touch respawning_flows",
        );

        // No second watcher: advance well past RESPAWN_DELAY, then
        // confirm the channel never produces a request. Yield first
        // so any erroneously-spawned watcher gets a chance to poll
        // (its sleep deadline would be set BEFORE the advance below);
        // without the pre-advance yield, a regression that DID spawn
        // the watcher would have its sleep start AFTER the advance
        // and the test would falsely pass.
        tokio::time::sleep(Duration::from_millis(1)).await;
        tokio::time::advance(RESPAWN_DELAY + Duration::from_secs(1)).await;
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            respawn_rx.try_recv().is_err(),
            "PanicDuplicate must NOT spawn a second panic-watcher; \
             respawn_rx must remain empty after RESPAWN_DELAY+1s",
        );

        // Tracing event from the dedup `info!`.
        assert!(
            logs_contain("sibling role already respawning; skipping duplicate"),
            "PanicDuplicate must emit the dedup tracing event",
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[traced_test]
    async fn handle_flow_exit_clean_exit_removes_handle_without_touching_respawning_or_last_error() {
        // CleanExit arm of `handle_flow_exit`: the inner future
        // returned cleanly (cancellation or natural completion). The
        // supervisor drops the FlowHandle entry but does NOT touch
        // `respawning_flows` (a clean exit of one role does not affect
        // the panic-respawn state of the other role) and does NOT
        // record a last_error.
        let mut registry = FlowRegistry::new();
        let (handle, cancel) = test_flow_handle();
        registry.handles.insert("flow1".to_string(), handle);
        registry.pending_exits.insert("flow1".to_string(), 1);

        let last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>> =
            Arc::new(Mutex::new(BTreeMap::new()));
        let (respawn_tx, mut respawn_rx) = mpsc::channel::<RespawnRequest>(4);
        let root_cancel = CancellationToken::new();

        handle_flow_exit(
            poll_clean("flow1"),
            Arc::clone(&last_errors),
            respawn_tx,
            &mut registry,
            &root_cancel,
        )
        .await;

        assert!(
            !registry.handles.contains_key("flow1"),
            "CleanExit must remove the FlowHandle entry",
        );
        assert!(
            !registry.respawning_flows.contains("flow1"),
            "CleanExit must NOT insert into respawning_flows",
        );
        assert!(
            !cancel.is_cancelled(),
            "CleanExit must NOT fire the cancel token (the inner future already finished)",
        );
        let errs = last_errors.lock().await;
        assert!(
            errs.is_empty(),
            "CleanExit must NOT record a last_error; got: {errs:?}",
        );
        drop(errs);
        // Decrement pending_exits 1 -> 0 -> remove.
        assert!(
            !registry.pending_exits.contains_key("flow1"),
            "pending_exits[flow] must be removed when its count reaches 0",
        );
        assert!(
            respawn_rx.try_recv().is_err(),
            "CleanExit must NOT spawn a panic-watcher",
        );
        assert!(
            logs_contain("flow task exited normally"),
            "CleanExit must emit the canonical clean-exit tracing event",
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn handle_flow_exit_cancelled_join_error_returns_without_mutation_or_recording() {
        // The Cancelled arm of `handle_flow_exit` returns immediately
        // with no side effects — the supervisor cancelled the task
        // itself (reload or shutdown), and run_reload's drain has
        // already accounted for the handle drop.
        let mut registry = FlowRegistry::new();
        let (handle, _cancel) = test_flow_handle();
        registry.handles.insert("flow1".to_string(), handle);
        // No pending_exits seed: the cancelled JoinError carries no
        // FlowExit (handle_flow_exit's pending_exits decrement is
        // gated by `Some(...)`, which is None for an Err join), so
        // the registry's pending_exits stays empty. Pinning this
        // avoids the saturating_sub branch firing.
        let last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>> =
            Arc::new(Mutex::new(BTreeMap::new()));
        let (respawn_tx, mut respawn_rx) = mpsc::channel::<RespawnRequest>(4);
        let root_cancel = CancellationToken::new();

        // Build a JoinError representing cancellation. Same shape as
        // decide_respawn_join_cancelled_returns_cancelled above.
        let cancelled_join: Result<FlowExit, tokio::task::JoinError> = {
            let h: tokio::task::JoinHandle<FlowExit> =
                tokio::spawn(async { futures_util::future::pending::<FlowExit>().await });
            h.abort();
            h.await
        };

        handle_flow_exit(
            cancelled_join,
            Arc::clone(&last_errors),
            respawn_tx,
            &mut registry,
            &root_cancel,
        )
        .await;

        // No side effects.
        assert!(
            registry.handles.contains_key("flow1"),
            "Cancelled arm must NOT remove the FlowHandle (run_reload's drain owns that)",
        );
        assert!(
            registry.respawning_flows.is_empty(),
            "Cancelled arm must NOT touch respawning_flows",
        );
        assert!(
            last_errors.lock().await.is_empty(),
            "Cancelled arm must NOT record a last_error",
        );
        assert!(
            respawn_rx.try_recv().is_err(),
            "Cancelled arm must NOT spawn a panic-watcher",
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[traced_test]
    async fn handle_flow_exit_unexpected_join_error_logs_warn_without_mutation() {
        // UnexpectedJoinError arm of `handle_flow_exit`: defensive
        // arm for a hypothetical bug where a panic escapes catch_unwind.
        // The supervisor logs a warn but does NOT mutate state nor
        // record a last_error (the panic's flow identity is unknown
        // because the JoinError carries no FlowExit).
        let mut registry = FlowRegistry::new();
        let (handle, _cancel) = test_flow_handle();
        registry.handles.insert("flow1".to_string(), handle);
        let last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>> =
            Arc::new(Mutex::new(BTreeMap::new()));
        let (respawn_tx, mut respawn_rx) = mpsc::channel::<RespawnRequest>(4);
        let root_cancel = CancellationToken::new();

        // JoinError::is_panic = true, is_cancelled = false: spawn a
        // panicking task without catch_unwind and join it.
        let panicked: Result<FlowExit, tokio::task::JoinError> = {
            let h: tokio::task::JoinHandle<FlowExit> = tokio::spawn(async {
                panic!("escaped catch_unwind");
            });
            h.await
        };
        assert!(panicked.as_ref().is_err());

        handle_flow_exit(
            panicked,
            Arc::clone(&last_errors),
            respawn_tx,
            &mut registry,
            &root_cancel,
        )
        .await;

        assert!(
            registry.handles.contains_key("flow1"),
            "UnexpectedJoinError arm must NOT mutate registry",
        );
        assert!(last_errors.lock().await.is_empty());
        assert!(respawn_rx.try_recv().is_err());
        assert!(
            logs_contain("flow task join failed (not panic, not cancelled — bug?)"),
            "UnexpectedJoinError arm must emit the canonical bug-tracker warn",
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn handle_flow_exit_clean_then_panic_race_classifies_panic_as_panic_first() {
        // The race the explicit `respawning_flows` set was added to
        // handle: dispatcher exits cleanly first (CleanExit removes
        // handle, leaves respawning_flows empty), then poll panics
        // (decide_respawn returns PanicFirst because already_respawning
        // is still false — clean exits don't insert into the set).
        // The panic-respawn proceeds as if the panic were the first
        // observation.
        let mut registry = FlowRegistry::new();
        let (handle, cancel) = test_flow_handle();
        registry.handles.insert("flow1".to_string(), handle);
        registry.pending_exits.insert("flow1".to_string(), 2);
        let last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>> =
            Arc::new(Mutex::new(BTreeMap::new()));
        let (respawn_tx, mut respawn_rx) = mpsc::channel::<RespawnRequest>(4);
        let root_cancel = CancellationToken::new();

        // First exit: dispatcher clean.
        handle_flow_exit(
            dispatcher_clean("flow1"),
            Arc::clone(&last_errors),
            respawn_tx.clone(),
            &mut registry,
            &root_cancel,
        )
        .await;

        assert!(
            !registry.handles.contains_key("flow1"),
            "clean dispatcher exit drops the handle",
        );
        assert!(
            registry.respawning_flows.is_empty(),
            "clean exit must not insert into respawning_flows",
        );
        assert!(
            !cancel.is_cancelled(),
            "clean exit must NOT fire the cancel token",
        );

        // Second exit: poll panic. With handles now empty, an
        // implementation that inferred respawn state from handle
        // membership would skip — but decide_respawn looks ONLY at
        // respawning_flows, which is still empty.
        handle_flow_exit(
            poll_panic("flow1", "second-observed panic"),
            Arc::clone(&last_errors),
            respawn_tx,
            &mut registry,
            &root_cancel,
        )
        .await;

        assert!(
            registry.respawning_flows.contains("flow1"),
            "PanicFirst must fire on the second exit despite the prior clean exit",
        );
        // pending_exits 2 -> 1 (clean) -> 0 (panic) -> removed.
        assert!(
            !registry.pending_exits.contains_key("flow1"),
            "pending_exits must reach 0 after both exits observed; got: {:?}",
            registry.pending_exits,
        );
        let errs = last_errors.lock().await;
        let entry = errs.get("flow1").expect("panic recorded");
        assert_eq!(entry.kind(), "panic");
        assert_eq!(entry.message(), "second-observed panic");
        drop(errs);
        // Watcher fires after RESPAWN_DELAY. Yield first so its
        // sleep deadline is anchored at the current virtual instant
        // (see PanicFirst test for the sleep-deadline-vs-advance
        // ordering rationale).
        tokio::time::sleep(Duration::from_millis(1)).await;
        tokio::time::advance(RESPAWN_DELAY + Duration::from_millis(100)).await;
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let req = respawn_rx
            .try_recv()
            .expect("panic-watcher must enqueue a respawn request");
        assert_eq!(req.flow, "flow1");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn handle_flow_exit_panic_first_with_no_handle_present_skips_cancel_but_still_inserts_respawning() {
        // The handle may already be gone when the panic lands (clean
        // exit of the sibling raced ahead, or the supervisor cancelled
        // the flow during reload and the cancel-driven exit's
        // observation arrived first). The PanicFirst arm wraps the
        // cancel call in `if let Some(h)` so the missing handle is
        // not a panic — the rest of the PanicFirst side effects still
        // fire.
        let mut registry = FlowRegistry::new();
        // No handle inserted — directly simulate "handle already
        // dropped before the panic was observed".
        let last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>> =
            Arc::new(Mutex::new(BTreeMap::new()));
        let (respawn_tx, mut respawn_rx) = mpsc::channel::<RespawnRequest>(4);
        let root_cancel = CancellationToken::new();

        handle_flow_exit(
            poll_panic("flow1", "boom"),
            Arc::clone(&last_errors),
            respawn_tx,
            &mut registry,
            &root_cancel,
        )
        .await;

        assert!(
            registry.respawning_flows.contains("flow1"),
            "PanicFirst must still insert into respawning_flows when no handle exists",
        );
        let errs = last_errors.lock().await;
        assert_eq!(errs.get("flow1").map(|e| e.kind()), Some("panic"));
        drop(errs);
        // The watcher still fires — there's no handle but the respawn
        // path is still scheduled. Yield first so the watcher's sleep
        // deadline anchors at the current virtual instant.
        tokio::time::sleep(Duration::from_millis(1)).await;
        tokio::time::advance(RESPAWN_DELAY + Duration::from_millis(100)).await;
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let req = respawn_rx
            .try_recv()
            .expect("watcher must enqueue request even when no handle was present at panic time");
        assert_eq!(req.flow, "flow1");
    }

    // -----------------------------------------------------------------
    // handle_respawn_request
    // -----------------------------------------------------------------

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn handle_respawn_request_drain_defer_re_enqueues_with_attempts_incremented_when_pending_exits_outstanding() {
        // The drain-defer path of `handle_respawn_request`:
        // pending_exits has an outstanding count for the flow AND
        // attempts < MAX_ATTEMPTS.
        // The function spawns a task that sleeps RESPAWN_RETRY_INTERVAL
        // then re-enqueues the request with attempts+1. The
        // respawning_flows slot is intentionally NOT released (the
        // re-enqueued request still represents the same panic-respawn
        // cycle).
        let mut registry = FlowRegistry::new();
        registry.respawning_flows.insert("flow1".to_string());
        registry.pending_exits.insert("flow1".to_string(), 2);

        let last_errors = Arc::new(Mutex::new(BTreeMap::new()));
        let state_mirror = Arc::new(StdMutex::new(State::default()));
        let ctx = test_spawn_context(Arc::clone(&last_errors), Arc::clone(&state_mirror));
        let cfg = Arc::new(test_config(vec![test_flow_config("flow1", true)]));
        let (config_watch_tx, _config_watch_rx) = watch::channel(Arc::clone(&cfg));
        let config_watch_tx = Arc::new(config_watch_tx);
        let control_handler =
            test_control_handler(Arc::clone(&last_errors), state_mirror, vec![]);
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

        // respawning_flows still contains the flow — the cycle is not
        // yet released.
        assert!(
            registry.respawning_flows.contains("flow1"),
            "drain-defer must NOT release the respawning_flows slot",
        );
        // No spawn happened (join_set still empty).
        assert_eq!(
            join_set.len(),
            0,
            "drain-defer must NOT call spawn_flow",
        );

        // Advance past RESPAWN_RETRY_INTERVAL so the re-enqueue task
        // wakes and sends. Yield first so the re-enqueue task's sleep
        // anchors at the current virtual instant — see PanicFirst test
        // for the sleep-deadline-vs-advance ordering rationale.
        tokio::time::sleep(Duration::from_millis(1)).await;
        tokio::time::advance(Duration::from_secs(2)).await;
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let next = respawn_rx
            .try_recv()
            .expect("drain-defer must re-enqueue a RespawnRequest after RESPAWN_RETRY_INTERVAL");
        assert_eq!(next.flow, "flow1");
        assert_eq!(
            next.attempts, 6,
            "re-enqueue must increment attempts by 1 (was 5 -> 6); got {}",
            next.attempts,
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[traced_test]
    async fn handle_respawn_request_drain_budget_exhausted_logs_warn_and_falls_through() {
        // At RESPAWN_MAX_ATTEMPTS, pending_exits is still non-empty
        // BUT the function falls through to the post-pending_exits
        // logic. Pin BOTH the warn tracing event AND a fall-through
        // outcome (in this test, the flow is also missing from config
        // so the "removed-from-config" arm fires — this avoids needing
        // spawn_flow's heavyweight machinery while still proving the
        // budget-exhausted path crosses the fall-through boundary).
        let mut registry = FlowRegistry::new();
        registry.respawning_flows.insert("flow1".to_string());
        registry.pending_exits.insert("flow1".to_string(), 1);

        let last_errors = Arc::new(Mutex::new(BTreeMap::new()));
        let state_mirror = Arc::new(StdMutex::new(State::default()));
        let ctx = test_spawn_context(Arc::clone(&last_errors), Arc::clone(&state_mirror));
        // EMPTY config — flow1 is no longer present, so after the
        // budget-exhausted fall-through the function will hit the
        // removed-from-config arm rather than spawn_flow.
        let cfg = Arc::new(test_config(vec![]));
        let (config_watch_tx, _config_watch_rx) = watch::channel(Arc::clone(&cfg));
        let config_watch_tx = Arc::new(config_watch_tx);
        let control_handler =
            test_control_handler(Arc::clone(&last_errors), state_mirror, vec![]);
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

        assert!(
            logs_contain("respawn drain budget exhausted"),
            "drain-budget-exhausted must emit the canonical warn",
        );
        // After fall-through, the slot is released regardless of which
        // post-fall-through arm fires.
        assert!(
            !registry.respawning_flows.contains("flow1"),
            "fall-through past drain budget must release respawning_flows",
        );
        // No re-enqueue happened (the budget-exhausted branch falls
        // through, NOT into the timer-spawn branch).
        assert!(
            respawn_rx.try_recv().is_err(),
            "budget-exhausted path must NOT re-enqueue another RespawnRequest",
        );
        // No spawn (config has no entry for the flow).
        assert_eq!(join_set.len(), 0);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[traced_test]
    async fn handle_respawn_request_already_running_skips_with_tracing_event() {
        // The already-running arm fires when registry.handles already
        // contains the flow (operator respawned it via SIGHUP after
        // the panic). Skip the spawn — the freshly-loaded entry takes
        // precedence over the panic-respawn that's now stale.
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
        let control_handler =
            test_control_handler(Arc::clone(&last_errors), state_mirror, vec![]);
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

        assert!(
            !registry.respawning_flows.contains("flow1"),
            "already-running arm must release the respawning_flows slot",
        );
        assert!(
            registry.handles.contains_key("flow1"),
            "already-running arm must NOT remove the existing handle",
        );
        assert_eq!(join_set.len(), 0, "already-running arm must NOT spawn");
        assert!(
            logs_contain("respawn-request ignored: flow already running"),
            "already-running arm must emit the canonical tracing event",
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[traced_test]
    async fn handle_respawn_request_removed_from_config_skips_with_tracing_event() {
        // The removed-from-config arm fires when the operator's
        // reload removed the flow before the panic-watcher's enqueue
        // arrived. Skip the spawn; the operator's intent wins.
        let mut registry = FlowRegistry::new();
        registry.respawning_flows.insert("flow1".to_string());

        let last_errors = Arc::new(Mutex::new(BTreeMap::new()));
        let state_mirror = Arc::new(StdMutex::new(State::default()));
        let ctx = test_spawn_context(Arc::clone(&last_errors), Arc::clone(&state_mirror));
        // Config has NO entry for flow1 — gone after a reload.
        let cfg = Arc::new(test_config(vec![]));
        let (config_watch_tx, _config_watch_rx) = watch::channel(Arc::clone(&cfg));
        let config_watch_tx = Arc::new(config_watch_tx);
        let control_handler =
            test_control_handler(Arc::clone(&last_errors), state_mirror, vec![]);
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

        assert!(
            !registry.respawning_flows.contains("flow1"),
            "removed-from-config arm must release the respawning_flows slot",
        );
        assert_eq!(join_set.len(), 0);
        assert!(
            logs_contain("flow removed from config during respawn delay; skipping"),
            "removed-from-config arm must emit the canonical tracing event",
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[traced_test]
    async fn handle_respawn_request_disabled_during_respawn_skips_with_tracing_event() {
        // The disabled-during-respawn arm fires when the operator's
        // reload disabled the flow (set enabled=false) before the
        // panic-watcher's enqueue arrived. Same skip semantics as
        // removed-from-config but the tracing message distinguishes
        // the two paths.
        let mut registry = FlowRegistry::new();
        registry.respawning_flows.insert("flow1".to_string());

        let last_errors = Arc::new(Mutex::new(BTreeMap::new()));
        let state_mirror = Arc::new(StdMutex::new(State::default()));
        let ctx = test_spawn_context(Arc::clone(&last_errors), Arc::clone(&state_mirror));
        // Flow present but disabled.
        let cfg = Arc::new(test_config(vec![test_flow_config("flow1", false)]));
        let (config_watch_tx, _config_watch_rx) = watch::channel(Arc::clone(&cfg));
        let config_watch_tx = Arc::new(config_watch_tx);
        let control_handler =
            test_control_handler(Arc::clone(&last_errors), state_mirror, vec![]);
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

        assert!(
            !registry.respawning_flows.contains("flow1"),
            "disabled-during-respawn arm must release the respawning_flows slot",
        );
        assert_eq!(join_set.len(), 0);
        assert!(
            logs_contain("flow disabled in config during respawn delay; skipping"),
            "disabled-during-respawn arm must emit the canonical tracing event",
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn handle_respawn_request_no_pending_exits_proceeds_to_post_drain_arms() {
        // When pending_exits[flow] is absent (the old-gen pair has
        // fully drained), the function skips the drain-defer block
        // entirely and proceeds to release the slot + check for
        // already-running / removed / disabled. Pin that the
        // pending_exits.contains_key check is the gate (a regression
        // that fired the drain-defer arm even with no pending exits
        // would surface here).
        let mut registry = FlowRegistry::new();
        registry.respawning_flows.insert("flow1".to_string());
        // Empty pending_exits: no contains_key hit.
        assert!(registry.pending_exits.is_empty());

        let last_errors = Arc::new(Mutex::new(BTreeMap::new()));
        let state_mirror = Arc::new(StdMutex::new(State::default()));
        let ctx = test_spawn_context(Arc::clone(&last_errors), Arc::clone(&state_mirror));
        let cfg = Arc::new(test_config(vec![]));
        let (config_watch_tx, _config_watch_rx) = watch::channel(Arc::clone(&cfg));
        let config_watch_tx = Arc::new(config_watch_tx);
        let control_handler =
            test_control_handler(Arc::clone(&last_errors), state_mirror, vec![]);
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

        // Slot released (post-drain block ran).
        assert!(
            !registry.respawning_flows.contains("flow1"),
            "no-pending-exits path must release the respawning_flows slot",
        );
        // No re-enqueue (drain-defer arm not taken). Yield first so
        // any erroneously-spawned re-enqueue timer would have its
        // sleep anchored before the advance — without this, the
        // advance would be consumed by nothing and the test would
        // falsely pass even if the drain-defer branch had fired.
        tokio::time::sleep(Duration::from_millis(1)).await;
        tokio::time::advance(Duration::from_secs(2)).await;
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            respawn_rx.try_recv().is_err(),
            "no-pending-exits path must NOT re-enqueue (drain-defer arm not taken)",
        );
    }

}

// Side-effect handler: translates a `RespawnDecision` into mutations
// on `FlowRegistry`, `last_errors`, and the panic-watcher task.

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::super::types::{record_last_error, FlowExit, FlowLastError, FlowRegistry};
use super::decision::{decide_respawn, RespawnDecision};
use super::RespawnRequest;
use crate::flow::RESPAWN_DELAY;

/// Handle a flow task's exit. On panic, schedules a respawn via
/// `respawn_tx` after `RESPAWN_DELAY`. The actual `spawn_flow` call
/// happens back in the supervisor's select! arm so the loop stays
/// responsive during the 30s window.
///
/// `respawning_flows` is the authoritative source of truth for "this
/// flow is between panic-observation and respawn". Dedup is gated
/// on this set (not on `handles` membership): a clean exit of the
/// sibling role could drop the handle entry, and a check against
/// `handles` would then misclassify a still-pending panic as
/// "already respawning" and kill the flow forever.
pub(crate) async fn handle_flow_exit(
    joined: Result<FlowExit, tokio::task::JoinError>,
    last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>>,
    respawn_tx: mpsc::Sender<RespawnRequest>,
    registry: &mut FlowRegistry,
    root_cancel: &CancellationToken,
) {
    let exit_flow_role = joined.as_ref().ok().map(|e| (e.flow.clone(), e.role));
    // Decrement pending_exits before any other side effect so a
    // respawn-request arriving in the same select! tick observes
    // the updated count.
    if let Some((flow, _)) = &exit_flow_role {
        if let Some(count) = registry.pending_exits.get_mut(flow) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                registry.pending_exits.remove(flow);
            }
        }
    }
    // Decision is taken from the snapshot BEFORE any mutation.
    let decision = {
        let already = exit_flow_role
            .as_ref()
            .is_some_and(|(name, _)| registry.respawning_flows.contains(name));
        decide_respawn(&joined, already)
    };
    match decision {
        RespawnDecision::Cancelled => return,
        RespawnDecision::UnexpectedJoinError => {
            // Genuine bug: a panic escaped catch_unwind.
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
            // Drop the handle; do NOT touch respawning_flows.
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

    // Panic arms: extract the exit's identity for logging and
    // last_error recording.
    let exit = match joined {
        Ok(e) => e,
        Err(_) => unreachable!("decide_respawn returned panic arm for non-Ok join"),
    };
    let panic_message = match exit.panic.as_ref() {
        Some(m) => m.clone(),
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
        info!(
            target: "gcit::supervisor",
            flow = %exit.flow,
            role = ?exit.role,
            "sibling role already respawning; skipping duplicate",
        );
        return;
    }

    // PanicFirst: mark respawning + cancel the sibling.
    registry.respawning_flows.insert(exit.flow.clone());
    if let Some(h) = registry.handles.remove(&exit.flow) {
        h.cancel.cancel();
    }

    // Detached watcher: sleeps RESPAWN_DELAY, then enqueues a
    // request. The sleep runs off the select! path so
    // SIGTERM/SIGHUP/control commands are not delayed.
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

#[cfg(test)]
mod tests {
    use super::super::super::types::{FlowHandle, FlowRole};
    use super::*;
    use crate::flow::TRIGGER_QUEUE;
    use std::time::Duration;
    use tracing_test::traced_test;

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

    fn test_flow_handle() -> (FlowHandle, CancellationToken) {
        let cancel = CancellationToken::new();
        let (trigger_tx, _trigger_rx) = mpsc::channel(TRIGGER_QUEUE);
        let handle = FlowHandle {
            cancel: cancel.clone(),
            trigger_tx,
        };
        (handle, cancel)
    }

    /// Drive virtual time forward by `delta` after a small initial
    /// yield so the watcher task gets its first poll (anchoring its
    /// `tokio::time::sleep` at the current virtual instant). Then
    /// yield a few more times so the now-ready watcher actually
    /// completes its send.
    async fn advance_past(delta: Duration) {
        tokio::time::sleep(Duration::from_millis(1)).await;
        tokio::time::advance(delta).await;
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[traced_test]
    async fn handle_flow_exit_panic_first_records_last_error_inserts_respawning_cancels_handle_and_enqueues_request(
    ) {
        // PanicFirst arm: five observable side effects:
        //   1. record_last_error kind="panic" + message
        //   2. registry.respawning_flows.insert(flow)
        //   3. handles[flow] removed (cancel token fires)
        //   4. panic-watcher spawned -> RespawnRequest{attempts:0}
        //   5. pending_exits decrements by 1
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

        let errs = last_errors.lock().await;
        let entry = errs.get("flow1").expect("last_error must be inserted");
        assert_eq!(entry.kind(), "panic");
        assert_eq!(entry.message(), "boom in poll loop");
        drop(errs);

        assert!(registry.respawning_flows.contains("flow1"));
        assert!(!registry.handles.contains_key("flow1"));
        assert!(cancel.is_cancelled());
        assert_eq!(registry.pending_exits.get("flow1").copied(), Some(1));

        advance_past(RESPAWN_DELAY + Duration::from_millis(100)).await;
        let req = respawn_rx
            .try_recv()
            .expect("panic-watcher must enqueue a RespawnRequest after RESPAWN_DELAY");
        assert_eq!(req.flow, "flow1");
        assert_eq!(req.attempts, 0);

        assert!(logs_contain(
            "flow task PANICKED; respawn scheduled after RESPAWN_DELAY"
        ));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[traced_test]
    async fn handle_flow_exit_panic_duplicate_records_last_error_but_does_not_spawn_second_watcher()
    {
        // PanicDuplicate: respawning_flows already contains the flow.
        // last_error overwrites (insert), no second watcher.
        let mut registry = FlowRegistry::new();
        registry.respawning_flows.insert("flow1".to_string());

        let last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>> =
            Arc::new(Mutex::new(BTreeMap::new()));
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

        let errs = last_errors.lock().await;
        let entry = errs.get("flow1").expect("last_error must be present");
        assert_eq!(entry.kind(), "panic");
        assert_eq!(entry.message(), "second panic from sibling");
        drop(errs);

        assert!(registry.respawning_flows.contains("flow1"));

        advance_past(RESPAWN_DELAY + Duration::from_secs(1)).await;
        assert!(respawn_rx.try_recv().is_err(), "no second watcher");

        assert!(logs_contain(
            "sibling role already respawning; skipping duplicate"
        ));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[traced_test]
    async fn handle_flow_exit_clean_exit_removes_handle_without_touching_respawning_or_last_error()
    {
        // CleanExit: drop the handle, leave respawning_flows alone,
        // record no last_error.
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

        assert!(!registry.handles.contains_key("flow1"));
        assert!(!registry.respawning_flows.contains("flow1"));
        assert!(!cancel.is_cancelled());
        assert!(last_errors.lock().await.is_empty());
        assert!(!registry.pending_exits.contains_key("flow1"));
        assert!(respawn_rx.try_recv().is_err());
        assert!(logs_contain("flow task exited normally"));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn handle_flow_exit_cancelled_join_error_returns_without_mutation_or_recording() {
        // Cancelled arm returns immediately — supervisor cancelled
        // the task itself; run_reload's drain owns the handle drop.
        let mut registry = FlowRegistry::new();
        let (handle, _cancel) = test_flow_handle();
        registry.handles.insert("flow1".to_string(), handle);
        let last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>> =
            Arc::new(Mutex::new(BTreeMap::new()));
        let (respawn_tx, mut respawn_rx) = mpsc::channel::<RespawnRequest>(4);
        let root_cancel = CancellationToken::new();

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

        assert!(registry.handles.contains_key("flow1"));
        assert!(registry.respawning_flows.is_empty());
        assert!(last_errors.lock().await.is_empty());
        assert!(respawn_rx.try_recv().is_err());
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[traced_test]
    async fn handle_flow_exit_unexpected_join_error_logs_warn_without_mutation() {
        // Defensive arm: panic that escaped catch_unwind. Log warn,
        // no state mutation.
        let mut registry = FlowRegistry::new();
        let (handle, _cancel) = test_flow_handle();
        registry.handles.insert("flow1".to_string(), handle);
        let last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>> =
            Arc::new(Mutex::new(BTreeMap::new()));
        let (respawn_tx, mut respawn_rx) = mpsc::channel::<RespawnRequest>(4);
        let root_cancel = CancellationToken::new();

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

        assert!(registry.handles.contains_key("flow1"));
        assert!(last_errors.lock().await.is_empty());
        assert!(respawn_rx.try_recv().is_err());
        assert!(logs_contain(
            "flow task join failed (not panic, not cancelled — bug?)"
        ));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn handle_flow_exit_clean_then_panic_race_classifies_panic_as_panic_first() {
        // Race that respawning_flows exists for: dispatcher clean
        // exits first (CleanExit drops handle, leaves respawning_flows
        // empty), then poll panics (PanicFirst because
        // already_respawning is still false — clean exits don't
        // insert into respawning_flows).
        let mut registry = FlowRegistry::new();
        let (handle, cancel) = test_flow_handle();
        registry.handles.insert("flow1".to_string(), handle);
        registry.pending_exits.insert("flow1".to_string(), 2);
        let last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>> =
            Arc::new(Mutex::new(BTreeMap::new()));
        let (respawn_tx, mut respawn_rx) = mpsc::channel::<RespawnRequest>(4);
        let root_cancel = CancellationToken::new();

        handle_flow_exit(
            dispatcher_clean("flow1"),
            Arc::clone(&last_errors),
            respawn_tx.clone(),
            &mut registry,
            &root_cancel,
        )
        .await;

        assert!(!registry.handles.contains_key("flow1"));
        assert!(registry.respawning_flows.is_empty());
        assert!(!cancel.is_cancelled());

        handle_flow_exit(
            poll_panic("flow1", "second-observed panic"),
            Arc::clone(&last_errors),
            respawn_tx,
            &mut registry,
            &root_cancel,
        )
        .await;

        assert!(registry.respawning_flows.contains("flow1"));
        assert!(!registry.pending_exits.contains_key("flow1"));
        let errs = last_errors.lock().await;
        let entry = errs.get("flow1").expect("panic recorded");
        assert_eq!(entry.kind(), "panic");
        assert_eq!(entry.message(), "second-observed panic");
        drop(errs);

        advance_past(RESPAWN_DELAY + Duration::from_millis(100)).await;
        let req = respawn_rx.try_recv().expect("watcher must enqueue");
        assert_eq!(req.flow, "flow1");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn handle_flow_exit_panic_first_with_no_handle_present_skips_cancel_but_still_inserts_respawning(
    ) {
        // Handle may already be gone when panic lands (sibling clean
        // exit raced ahead). PanicFirst wraps the cancel in
        // `if let Some(h)` so the missing handle is not a panic.
        let mut registry = FlowRegistry::new();
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

        assert!(registry.respawning_flows.contains("flow1"));
        let errs = last_errors.lock().await;
        assert_eq!(errs.get("flow1").map(|e| e.kind()), Some("panic"));
        drop(errs);

        advance_past(RESPAWN_DELAY + Duration::from_millis(100)).await;
        let req = respawn_rx
            .try_recv()
            .expect("watcher must enqueue request even when no handle was present");
        assert_eq!(req.flow, "flow1");
    }
}

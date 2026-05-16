// Control-channel handler + command dispatch.
//
// `control::Handler` impl on `ControlHandler` lives here. Trigger
// and Reload requests ship a `ControlCommand` + reply oneshot to the
// supervisor's main select! loop via the `cmd_tx` mpsc; Status reads
// the in-memory state mirror + last_errors map directly without
// round-tripping through the supervisor (read-only operations).
//
// `handle_control_command` is the supervisor-side dispatch fn: it
// matches the `ControlCommand` and routes to `reload::run_reload`
// (Reload) or `trigger::run_trigger` (Trigger).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::{mpsc, oneshot, watch, Mutex, RwLock};
use tokio::task::JoinSet;

use crate::config::Config;
use crate::control;
use crate::state::{FlowState, State};

use super::flows::SpawnContext;
use super::reload::run_reload;
use super::types::{FlowExit, FlowLastError, FlowRegistry};

mod trigger;

use trigger::run_trigger;

/// Control-channel command routed from the `ControlHandler` to the
/// supervisor's main select! loop. The handler does NOT run
/// reload/trigger inline — the supervisor owns the per-flow state
/// and the actual mutations.
pub(super) enum ControlCommand {
    Reload {
        reply: oneshot::Sender<Result<serde_json::Value, String>>,
    },
    Trigger {
        flow: String,
        dry_run: bool,
        reply: oneshot::Sender<Result<serde_json::Value, String>>,
    },
}

/// Control-channel handler. Forwards Reload + Trigger to the
/// supervisor via `cmd_tx`; Status reads the in-memory state mirror
/// and last_errors map directly (these are read-only operations).
pub(super) struct ControlHandler {
    pub(super) cmd_tx: mpsc::Sender<ControlCommand>,
    pub(super) state_mirror: Arc<StdMutex<State>>,
    pub(super) last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>>,
    pub(super) flow_names: Arc<RwLock<Vec<String>>>,
}

impl ControlHandler {
    /// Send a command to the supervisor and await its reply. Centralizes
    /// the `cmd_tx.send + reply_rx.await + map_err` triple shared by
    /// `trigger` and `reload` — both differ only in the variant they
    /// construct, so the caller passes a builder closure that takes
    /// the reply sender and returns the variant.
    async fn dispatch_cmd<F>(&self, build: F) -> Result<serde_json::Value, String>
    where
        F: FnOnce(oneshot::Sender<Result<serde_json::Value, String>>) -> ControlCommand,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(build(reply_tx))
            .await
            .map_err(|_| "supervisor command channel closed".to_string())?;
        reply_rx
            .await
            .map_err(|_| "supervisor reply dropped".to_string())?
    }
}

impl control::Handler for ControlHandler {
    async fn trigger(&self, flow: &str, dry_run: bool) -> Result<serde_json::Value, String> {
        self.dispatch_cmd(|reply| ControlCommand::Trigger {
            flow: flow.to_string(),
            dry_run,
            reply,
        })
        .await
    }

    async fn status(&self, flow: Option<&str>) -> Result<serde_json::Value, String> {
        let names = self.flow_names.read().await.clone();
        let errors = self.last_errors.lock().await.clone();
        // Snapshot the state mirror under the stdlib mutex. Clone the
        // per-flow entries before building JSON so the lock is dropped
        // before any subsequent `.await`.
        let flows_state: BTreeMap<String, FlowState> = match self.state_mirror.lock() {
            Ok(g) => g.flows.clone(),
            Err(_) => return Err("state mirror mutex poisoned".to_string()),
        };

        match flow {
            Some(f) => {
                if !names.iter().any(|n| n == f) {
                    return Err(format!("flow `{f}` not running"));
                }
                let mut out = serde_json::Map::new();
                out.insert(f.to_string(), render_one(f, &flows_state, &errors));
                Ok(serde_json::Value::Object(out))
            }
            None => {
                let mut out = serde_json::Map::new();
                for name in &names {
                    out.insert(name.clone(), render_one(name, &flows_state, &errors));
                }
                // Synthetic last_errors keys (currently only `(reload)`,
                // recorded by run_reload on a failed parse) are not
                // flow names but operators still need to see them.
                // Surface them alongside real flows so `gcit status`
                // reports daemon-level reload failures.
                //
                // Convention: synthetic key names are wrapped in
                // parentheses. The wrapping is the namespace — a flow
                // id cannot match because `flow.name` is validated
                // against `[a-zA-Z0-9_-]+` at config load and parens
                // are outside that character class. The text renderer
                // (`cli/status.rs::render_text`) detects the wrapping
                // and prefixes the header with `[daemon]`.
                for key in errors.keys() {
                    if !out.contains_key(key) {
                        out.insert(key.clone(), render_one(key, &flows_state, &errors));
                    }
                }
                Ok(serde_json::Value::Object(out))
            }
        }
    }

    async fn reload(&self) -> Result<serde_json::Value, String> {
        self.dispatch_cmd(|reply| ControlCommand::Reload { reply })
            .await
    }

    async fn version(&self) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"),
        }))
    }
}

/// Render one flow (or synthetic daemon key) into the per-flow JSON
/// shape `gcit status` consumes. Extracted from the `status` impl so
/// the all-flows path and the single-flow path share one source of
/// truth.
///
/// JSON shape per cli/status.rs's renderer:
///   `state` (running | starting | errored),
///   `last_sha`, `last_poll_at`, `last_dispatched_at`, `cooldown_until`,
///   `active_runs` (count), `notified_runs` (count),
///   `last_error` (object with `at`/`kind`/`message`/`retry_at` or null).
fn render_one(
    name: &str,
    flows_state: &BTreeMap<String, FlowState>,
    errors: &BTreeMap<String, FlowLastError>,
) -> serde_json::Value {
    let st = flows_state.get(name);
    let last_sha = st
        .and_then(|s| s.last_sha.as_deref())
        .map(serde_json::Value::from)
        .unwrap_or(serde_json::Value::Null);
    let last_poll_at = opt_rfc3339(st.and_then(|s| s.last_poll_at));
    let last_dispatched_at = opt_rfc3339(st.and_then(|s| s.last_dispatched_at));
    let cooldown_until = opt_rfc3339(st.and_then(|s| s.cooldown_until));
    let active_runs = st.map(|s| s.active_runs.len()).unwrap_or(0);
    let notified_runs = st.map(|s| s.notified_runs.len()).unwrap_or(0);
    // "running" iff state is present AND no recent error;
    // "errored" iff last_errors carries an entry;
    // "starting" otherwise.
    let label = match (errors.get(name), st) {
        (Some(_), _) => "errored",
        (None, Some(_)) => "running",
        (None, None) => "starting",
    };
    let last_error = errors.get(name).map(|e| {
        serde_json::json!({
            "at": e.at().to_rfc3339(),
            "kind": e.kind(),
            "message": e.message(),
            "retry_at": e.retry_at().map(|t| t.to_rfc3339()),
        })
    });
    serde_json::json!({
        "state": label,
        "last_sha": last_sha,
        "last_poll_at": last_poll_at,
        "last_dispatched_at": last_dispatched_at,
        "cooldown_until": cooldown_until,
        "active_runs": active_runs,
        "notified_runs": notified_runs,
        "last_error": last_error,
    })
}

/// Map an `Option<DateTime<Utc>>` to JSON: `None` -> `Value::Null`,
/// `Some(t)` -> `Value::String(rfc3339)`. Centralizes the chain
/// repeated for `last_poll_at`, `last_dispatched_at`, and
/// `cooldown_until` in `render_one`.
pub(super) fn opt_rfc3339(t: Option<chrono::DateTime<chrono::Utc>>) -> serde_json::Value {
    t.map(|t| t.to_rfc3339())
        .map(serde_json::Value::from)
        .unwrap_or(serde_json::Value::Null)
}

/// Dispatch a control-channel command on the supervisor's main
/// select! loop. Reload routes to `run_reload`; Trigger looks up the
/// flow in `registry.handles` and either renders a dry-run payload OR
/// injects a synthetic TriggerSignal into the flow's dispatcher mpsc.
pub(super) async fn handle_control_command(
    cmd: ControlCommand,
    config_path: &std::path::Path,
    config_watch: Arc<watch::Sender<Arc<Config>>>,
    ctx: &SpawnContext,
    join_set: &mut JoinSet<FlowExit>,
    registry: &mut FlowRegistry,
    control_handler: &Arc<ControlHandler>,
) {
    match cmd {
        ControlCommand::Reload { reply } => {
            run_reload(
                config_path,
                config_watch,
                ctx,
                join_set,
                registry,
                control_handler,
            )
            .await;
            let body = serde_json::json!({
                "flows": registry.handles.len(),
                "result": "reload completed",
            });
            let _ = reply.send(Ok(body));
        }
        ControlCommand::Trigger {
            flow,
            dry_run,
            reply,
        } => {
            let result = run_trigger(
                &flow,
                dry_run,
                &registry.handles,
                config_watch,
                &ctx.state_mirror,
            )
            .await;
            let _ = reply.send(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CredentialId, Destination, DiscordTemplateConfig, DiscordWebhookConfig, FireEvent,
        LocalMailConfig, LocalMailTemplateConfig,
    };
    use crate::control::Handler as _;
    use crate::flow::supervisor::FlowLastError;
    use crate::state::FlowState;

    /// Pin the Destination::DiscordWebhook + Destination::LocalMail
    /// shapes used elsewhere in the supervisor — a clear compile-time
    /// failure if either variant's required fields drift.
    #[test]
    fn destination_variants_compile_with_expected_fields() {
        let _disc = Destination::DiscordWebhook(DiscordWebhookConfig {
            credential_id: CredentialId::new("disc-cred").expect("valid id"),
            fire_on: vec![FireEvent::RunStart, FireEvent::RunComplete],
            template: DiscordTemplateConfig::default(),
        });
        let _mail = Destination::LocalMail(LocalMailConfig {
            user: "ci".to_string(),
            fire_on: vec![FireEvent::RunComplete],
            template: LocalMailTemplateConfig::default(),
        });
    }

    /// Build a ControlHandler with the supplied flow_names, last_errors,
    /// and state. The cmd_tx mpsc is constructed but never sent into;
    /// status / version paths are read-only and do not exercise it.
    fn handler_with(
        flow_names: Vec<String>,
        last_errors: BTreeMap<String, FlowLastError>,
        state: State,
    ) -> Arc<ControlHandler> {
        let (cmd_tx, _cmd_rx) = mpsc::channel(1);
        Arc::new(ControlHandler {
            cmd_tx,
            state_mirror: Arc::new(StdMutex::new(state)),
            last_errors: Arc::new(Mutex::new(last_errors)),
            flow_names: Arc::new(RwLock::new(flow_names)),
        })
    }

    #[tokio::test]
    async fn status_returns_empty_object_when_no_flows_and_no_errors() {
        let h = handler_with(Vec::new(), BTreeMap::new(), State::default());
        let v = h.status(None).await.expect("status must succeed");
        let obj = v.as_object().expect("object");
        assert!(obj.is_empty(), "no flows + no errors must yield empty map");
    }

    #[tokio::test]
    async fn status_renders_starting_label_for_flow_with_no_state_or_error() {
        // Freshly-spawned flow that has not yet recorded its first poll
        // or panic surfaces as `state: starting`. Pin so a regression
        // defaulting to e.g. "running" doesn't hide initial-cycle
        // failures.
        let h = handler_with(vec!["ci".to_string()], BTreeMap::new(), State::default());
        let v = h.status(None).await.expect("status must succeed");
        assert_eq!(v["ci"]["state"], "starting");
        assert_eq!(v["ci"]["last_sha"], serde_json::Value::Null);
        assert_eq!(v["ci"]["last_poll_at"], serde_json::Value::Null);
        assert_eq!(v["ci"]["active_runs"], 0);
        assert_eq!(v["ci"]["notified_runs"], 0);
        assert_eq!(v["ci"]["last_error"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn status_renders_running_label_when_state_present_and_no_error() {
        // Flow with a state.json entry but no recent error renders as
        // "running" — pinned by the (None, Some(_)) match arm.
        let mut s = State::default();
        s.flows.insert(
            "ci".to_string(),
            FlowState {
                last_sha: Some("deadbeefcafe1234567890abcdef1234567890ab".to_string()),
                ..Default::default()
            },
        );
        let h = handler_with(vec!["ci".to_string()], BTreeMap::new(), s);
        let v = h.status(None).await.expect("status must succeed");
        assert_eq!(v["ci"]["state"], "running");
        assert_eq!(
            v["ci"]["last_sha"],
            "deadbeefcafe1234567890abcdef1234567890ab",
        );
    }

    #[tokio::test]
    async fn status_renders_errored_label_when_last_error_present() {
        // Flow with an entry in last_errors renders as "errored" even
        // when state mirror has a healthy snapshot. (Some, _) wins.
        let mut errors = BTreeMap::new();
        let last_errors_map = Arc::new(Mutex::new(BTreeMap::new()));
        crate::flow::supervisor::record_last_error(
            &last_errors_map,
            "ci",
            "git_poll_failed",
            "boom",
            None,
        )
        .await;
        errors.insert(
            "ci".to_string(),
            last_errors_map
                .lock()
                .await
                .get("ci")
                .cloned()
                .expect("entry present"),
        );
        let h = handler_with(vec!["ci".to_string()], errors, State::default());
        let v = h.status(None).await.expect("status must succeed");
        assert_eq!(v["ci"]["state"], "errored");
        assert_eq!(v["ci"]["last_error"]["kind"], "git_poll_failed");
        assert_eq!(v["ci"]["last_error"]["message"], "boom");
        assert_eq!(v["ci"]["last_error"]["retry_at"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn status_surfaces_synthetic_reload_key_alongside_real_flows() {
        // run_reload records parse failures under `(reload)`. The
        // status handler includes synthetic keys in the all-flows
        // response even though they aren't in flow_names.
        let last_errors_map = Arc::new(Mutex::new(BTreeMap::new()));
        crate::flow::supervisor::record_last_error(
            &last_errors_map,
            "(reload)",
            "config_reload",
            "parse error",
            None,
        )
        .await;
        let errors = last_errors_map.lock().await.clone();
        let h = handler_with(vec!["ci".to_string()], errors, State::default());
        let v = h.status(None).await.expect("status must succeed");
        let obj = v.as_object().expect("object");
        assert!(obj.contains_key("ci"));
        assert!(
            obj.contains_key("(reload)"),
            "synthetic daemon key must surface alongside real flows; got keys {:?}",
            obj.keys().collect::<Vec<_>>(),
        );
        assert_eq!(obj["(reload)"]["state"], "errored");
        assert_eq!(obj["(reload)"]["last_error"]["kind"], "config_reload");
    }

    #[tokio::test]
    async fn status_filtered_to_specific_flow_returns_only_that_entry() {
        let h = handler_with(
            vec!["ci".to_string(), "release".to_string()],
            BTreeMap::new(),
            State::default(),
        );
        let v = h.status(Some("ci")).await.expect("status must succeed");
        let obj = v.as_object().expect("object");
        assert_eq!(obj.len(), 1);
        assert!(obj.contains_key("ci"));
        assert!(!obj.contains_key("release"));
    }

    #[tokio::test]
    async fn status_filtered_to_unknown_flow_returns_error() {
        let h = handler_with(vec!["ci".to_string()], BTreeMap::new(), State::default());
        let err = h
            .status(Some("missing"))
            .await
            .expect_err("unknown flow must error");
        assert!(
            err.contains("missing") && err.contains("not running"),
            "error must name the flow and the reason; got: {err}",
        );
    }

    #[tokio::test]
    async fn version_returns_cargo_pkg_version_field() {
        let h = handler_with(Vec::new(), BTreeMap::new(), State::default());
        let v = h.version().await.expect("version must succeed");
        assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
    }

    #[tokio::test]
    async fn status_includes_active_and_notified_run_counts() {
        // active_runs + notified_runs are surfaced as counts (not the
        // full RunState entries) so operators can spot a flow with
        // many in-flight runs without reading state.json.
        use crate::state::apply::RunState;
        let mut s = State::default();
        s.flows.insert(
            "ci".to_string(),
            FlowState {
                active_runs: vec![RunState {
                    run_id: 1,
                    started_at: chrono::Utc::now(),
                    conclusion: None,
                    completed_at: None,
                }],
                notified_runs: vec![
                    RunState {
                        run_id: 2,
                        started_at: chrono::Utc::now(),
                        conclusion: Some("success".to_string()),
                        completed_at: Some(chrono::Utc::now()),
                    },
                    RunState {
                        run_id: 3,
                        started_at: chrono::Utc::now(),
                        conclusion: Some("success".to_string()),
                        completed_at: Some(chrono::Utc::now()),
                    },
                ],
                ..Default::default()
            },
        );
        let h = handler_with(vec!["ci".to_string()], BTreeMap::new(), s);
        let v = h.status(None).await.expect("status must succeed");
        assert_eq!(v["ci"]["active_runs"], 1);
        assert_eq!(v["ci"]["notified_runs"], 2);
    }

    /// Build a ControlHandler whose cmd_rx receiver has been dropped
    /// before the test calls `trigger`/`reload`. The supervisor's
    /// command channel appears closed from the handler's perspective.
    fn handler_with_closed_cmd_channel() -> Arc<ControlHandler> {
        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        drop(cmd_rx);
        Arc::new(ControlHandler {
            cmd_tx,
            state_mirror: Arc::new(StdMutex::new(State::default())),
            last_errors: Arc::new(Mutex::new(BTreeMap::new())),
            flow_names: Arc::new(RwLock::new(Vec::new())),
        })
    }

    #[tokio::test]
    async fn trigger_reports_supervisor_command_channel_closed_when_cmd_rx_dropped() {
        let h = handler_with_closed_cmd_channel();
        let err = h
            .trigger("ci", false)
            .await
            .expect_err("must error when cmd channel closed");
        assert!(
            err.contains("supervisor command channel closed"),
            "trigger must surface 'supervisor command channel closed'; got: {err}",
        );
    }

    #[tokio::test]
    async fn reload_reports_supervisor_command_channel_closed_when_cmd_rx_dropped() {
        let h = handler_with_closed_cmd_channel();
        let err = h
            .reload()
            .await
            .expect_err("must error when cmd channel closed");
        assert!(
            err.contains("supervisor command channel closed"),
            "reload must surface 'supervisor command channel closed'; got: {err}",
        );
    }

    /// Stand up a stub supervisor that recv's the cmd then drops the
    /// reply oneshot without sending. Both `trigger` and `reload`
    /// surface `"supervisor reply dropped"` in that case.
    async fn assert_reply_dropped_surfaces<F, Fut>(call: F)
    where
        F: FnOnce(Arc<ControlHandler>) -> Fut,
        Fut: std::future::Future<Output = Result<serde_json::Value, String>>,
    {
        let (cmd_tx, mut cmd_rx) = mpsc::channel(1);
        let h = Arc::new(ControlHandler {
            cmd_tx,
            state_mirror: Arc::new(StdMutex::new(State::default())),
            last_errors: Arc::new(Mutex::new(BTreeMap::new())),
            flow_names: Arc::new(RwLock::new(Vec::new())),
        });
        let supervisor_stub = tokio::spawn(async move {
            match cmd_rx.recv().await {
                Some(ControlCommand::Trigger { reply, .. }) => drop(reply),
                Some(ControlCommand::Reload { reply }) => drop(reply),
                None => panic!("cmd channel closed unexpectedly"),
            }
        });
        let err = call(h)
            .await
            .expect_err("must error when reply oneshot dropped");
        assert!(
            err.contains("supervisor reply dropped"),
            "must surface 'supervisor reply dropped'; got: {err}",
        );
        supervisor_stub
            .await
            .expect("supervisor stub task panicked");
    }

    #[tokio::test]
    async fn trigger_reports_supervisor_reply_dropped_when_reply_oneshot_dropped() {
        assert_reply_dropped_surfaces(|h| async move { h.trigger("ci", false).await }).await;
    }

    #[tokio::test]
    async fn reload_reports_supervisor_reply_dropped_when_reply_oneshot_dropped() {
        assert_reply_dropped_surfaces(|h| async move { h.reload().await }).await;
    }

    #[tokio::test]
    async fn status_renders_cooldown_window_fields_when_state_present() {
        // last_dispatched_at + cooldown_until must surface as RFC3339
        // strings on a flow whose state has them set. Pin so a
        // regression that drops the chain in render_one's opt_rfc3339
        // wiring is caught.
        let mut s = State::default();
        let dispatched = chrono::DateTime::parse_from_rfc3339("2026-05-01T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let until = chrono::DateTime::parse_from_rfc3339("2026-05-01T12:05:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        s.flows.insert(
            "ci".to_string(),
            FlowState {
                last_dispatched_at: Some(dispatched),
                cooldown_until: Some(until),
                ..Default::default()
            },
        );
        let h = handler_with(vec!["ci".to_string()], BTreeMap::new(), s);
        let v = h.status(None).await.expect("status must succeed");
        assert_eq!(v["ci"]["last_dispatched_at"], "2026-05-01T12:00:00+00:00");
        assert_eq!(v["ci"]["cooldown_until"], "2026-05-01T12:05:00+00:00");
    }
}

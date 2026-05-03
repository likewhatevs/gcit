// Control-channel handler + command dispatch.
//
// The `control::Handler` impl on `ControlHandler` lives here. Trigger
// + Reload requests ship a `ControlCommand` + reply oneshot to the
// supervisor's main select! loop via the `cmd_tx` mpsc; Status reads
// the in-memory state mirror + last_errors map directly without
// round-tripping through the supervisor (read-only operations).
//
// `handle_control_command` is the supervisor-side dispatch fn: it
// matches the `ControlCommand` and routes to either `reload::run_reload`
// (Reload) or `run_trigger` (Trigger).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::{mpsc, oneshot, watch, Mutex, RwLock};
use tokio::task::JoinSet;

use crate::config::{ActionConfig, Config, FlowConfig};
use crate::control;
use crate::state::{FlowState, State};

use super::flows::SpawnContext;
use super::reload::run_reload;
use super::types::{FlowExit, FlowHandle, FlowLastError, FlowRegistry};

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

impl control::Handler for ControlHandler {
    async fn trigger(&self, flow: &str, dry_run: bool) -> Result<serde_json::Value, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(ControlCommand::Trigger {
                flow: flow.to_string(),
                dry_run,
                reply: reply_tx,
            })
            .await
            .map_err(|_| "supervisor command channel closed".to_string())?;
        reply_rx
            .await
            .map_err(|_| "supervisor reply dropped".to_string())?
    }

    async fn status(&self, flow: Option<&str>) -> Result<serde_json::Value, String> {
        let names = self.flow_names.read().await.clone();
        let errors = self.last_errors.lock().await.clone();
        // Snapshot the state mirror under the stdlib mutex. Clone
        // the per-flow entries we need so the lock is released
        // before we build JSON.
        let flows_state: BTreeMap<String, FlowState> = match self.state_mirror.lock() {
            Ok(g) => g.flows.clone(),
            Err(_) => return Err("state mirror mutex poisoned".to_string()),
        };

        // Build per-flow JSON values. cli/status.rs expects a
        // top-level Object keyed by flow name; each value carries
        // `state`, `last_sha`, `last_poll_at`, `active_runs` (count),
        // `notified_runs` (count), and `last_error` (object with
        // `at`/`kind`/`message`).
        let render_one = |name: &str| -> serde_json::Value {
            let st = flows_state.get(name);
            let last_sha = st
                .and_then(|s| s.last_sha.as_deref())
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null);
            let last_poll_at = st
                .and_then(|s| s.last_poll_at)
                .map(|t| t.to_rfc3339())
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null);
            let last_dispatched_at = st
                .and_then(|s| s.last_dispatched_at)
                .map(|t| t.to_rfc3339())
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null);
            let cooldown_until = st
                .and_then(|s| s.cooldown_until)
                .map(|t| t.to_rfc3339())
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null);
            let active_runs = st.map(|s| s.active_runs.len()).unwrap_or(0);
            let notified_runs = st.map(|s| s.notified_runs.len()).unwrap_or(0);
            // `state` field: a coarse human label so cli/status.rs's
            // text renderer prints something useful. "running" if we
            // have an entry in registry.handles AND no recent panic;
            // "errored" if the last_errors map carries an entry;
            // "starting" if neither.
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
        };

        match flow {
            Some(f) => {
                if !names.contains(&f.to_string()) {
                    return Err(format!("flow `{f}` not running"));
                }
                let mut out = serde_json::Map::new();
                out.insert(f.to_string(), render_one(f));
                Ok(serde_json::Value::Object(out))
            }
            None => {
                let mut out = serde_json::Map::new();
                for name in &names {
                    out.insert(name.clone(), render_one(name));
                }
                // Synthetic last_errors keys (e.g. "(reload)" recorded
                // by run_reload on a failed parse) are not flow names
                // but operators still need to see them — surface them
                // alongside real flows so `gcit status` reports daemon-
                // level reload failures.
                //
                // Convention: synthetic key names are wrapped in
                // parentheses (currently only `(reload)`; future
                // daemon-scoped sentinels follow the same shape). The
                // wrapping is the namespace — a flow id cannot match
                // because `flow.name` is validated against
                // `[a-zA-Z0-9_-]+` at config load and parens are
                // outside that character class. The text renderer
                // (`cli/status.rs::render_text`) detects the wrapping
                // and prefixes the header with `[daemon]`; JSON
                // consumers see the parenthesised key directly.
                for key in errors.keys() {
                    if !out.contains_key(key) {
                        out.insert(key.clone(), render_one(key));
                    }
                }
                Ok(serde_json::Value::Object(out))
            }
        }
    }

    async fn reload(&self) -> Result<serde_json::Value, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(ControlCommand::Reload { reply: reply_tx })
            .await
            .map_err(|_| "supervisor command channel closed".to_string())?;
        reply_rx
            .await
            .map_err(|_| "supervisor reply dropped".to_string())?
    }

    async fn version(&self) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"),
        }))
    }
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

/// Implement the `gcit trigger <flow> [--dry-run]` control command.
///
/// Real path (`dry_run = false`): look up the flow's dispatcher
/// trigger_tx; load the most recent observed sha from the state
/// mirror; send a synthetic TriggerSignal into the dispatcher's
/// mpsc. The dispatcher does the rest (dispatch + correlate + monitor
/// spawn) on its own.
///
/// Dry-run path (`dry_run = true`): render the dispatch payload
/// (rendered_inputs + injected gcit_run_id) without contacting GitHub
/// or any notifier. The rendered map + the would-be ref/repo/workflow
/// are returned to the caller as JSON.
async fn run_trigger(
    flow_name: &str,
    dry_run: bool,
    handles: &BTreeMap<String, FlowHandle>,
    config_watch: Arc<watch::Sender<Arc<Config>>>,
    state_mirror: &Arc<StdMutex<State>>,
) -> Result<serde_json::Value, String> {
    let cfg = config_watch.borrow().clone();
    let flow_cfg = cfg
        .flow
        .iter()
        .find(|f| f.name == flow_name)
        .ok_or_else(|| format!("flow '{flow_name}' not found in current config"))?;
    if !flow_cfg.enabled {
        return Err(format!("flow '{flow_name}' is disabled in current config",));
    }

    if dry_run {
        return render_dry_run_payload(flow_cfg);
    }

    // Real trigger path: pull the most recently observed sha from the
    // state mirror so the dispatcher's RunContext.source.sha is the
    // real one. If no observation exists yet (first poll hasn't
    // landed), refuse — we can't fabricate a sha.
    let sha = read_last_sha(state_mirror, flow_name)?;
    let handle = handles
        .get(flow_name)
        .ok_or_else(|| format!("flow '{flow_name}' is not currently running"))?;
    let signal = crate::flow::TriggerSignal {
        observed_sha: sha,
        observed_at: chrono::Utc::now(),
    };
    handle
        .trigger_tx
        .send(signal)
        .await
        .map_err(|e| format!("failed to enqueue trigger: {e}"))?;
    Ok(serde_json::json!({
        "flow": flow_name,
        "result": "trigger enqueued",
        "observed_sha": sha.to_string(),
    }))
}

/// Render the dry-run payload for `gcit trigger --dry-run`. Mirrors
/// the dispatcher's render path (`render_inputs` + `build_inputs_payload`)
/// so an operator sees exactly what would land in the
/// `workflow_dispatch` body.
///
/// Uses `Uuid::new_v4()` for the gcit_run_id so the operator sees a
/// realistic preview (the real dispatch path also calls `new_v4`).
/// A nil UUID would be misleading because the workflow's run-name
/// template would render `gcit-00000000...` instead of a value the
/// operator can correlate against.
fn render_dry_run_payload(flow: &FlowConfig) -> Result<serde_json::Value, String> {
    let ActionConfig::GithubWorkflowDispatch {
        repo,
        workflow,
        ref_name,
        inputs,
        ..
    } = &flow.action;
    let gcit_run_id = uuid::Uuid::new_v4();
    // Probe SHA — dry-run renders against a synthetic context because
    // no real trigger has fired. The rendered output is for operator
    // inspection only; it never travels over the wire. Use the same
    // probe-context shape that `config::validate::probe_context`
    // employs at config-load.
    let probe_run_ctx = crate::notify::RunContext {
        flow_name: flow.name.clone(),
        flow_description: flow.description.clone(),
        source: crate::notify::SourceInfo {
            url: flow.source.url.clone(),
            ref_name: flow.source.ref_name.clone(),
            sha: gix_hash::ObjectId::null(gix_hash::Kind::Sha1),
            sha_short: "0".repeat(12),
        },
        action: crate::notify::ActionInfo {
            repo: repo.clone(),
            workflow: workflow.clone(),
            run_id: 0,
            run_url: String::new(),
            dispatched_at: chrono::Utc::now(),
        },
        gcit_run_id,
    };
    let summary = crate::github::monitor::empty_run_summary(0);
    let data = crate::notify::render_context(&probe_run_ctx, &summary);
    let hb = crate::notify::strict_handlebars();
    let rendered_inputs = crate::github::dispatcher::render_inputs(inputs, &data, &hb)
        .map_err(|e| format!("render inputs (dry-run): {e}"))?;
    let payload = crate::github::dispatcher::build_inputs_payload(&rendered_inputs, gcit_run_id);
    Ok(serde_json::json!({
        "flow": flow.name,
        "dry_run": true,
        "repo": repo,
        "workflow": workflow,
        "ref": ref_name,
        "gcit_run_id": gcit_run_id.to_string(),
        "rendered_inputs": payload,
    }))
}

/// Read the most recent observed sha for `flow` from the state
/// mirror. Returns an error message naming the flow if no
/// observation has ever landed: a manual trigger requires an
/// observed SHA so the dispatcher's correlator can populate its
/// head_sha filter.
fn read_last_sha(
    state_mirror: &Arc<StdMutex<State>>,
    flow: &str,
) -> Result<gix_hash::ObjectId, String> {
    let state = state_mirror
        .lock()
        .map_err(|_| "state mirror mutex poisoned".to_string())?;
    let entry = state
        .flows
        .get(flow)
        .ok_or_else(|| format!("flow '{flow}' has no recorded state yet"))?;
    let sha_hex = entry
        .last_sha
        .as_ref()
        .ok_or_else(|| {
            format!(
                "flow '{flow}' has no observed sha yet (manual trigger requires at least one poll cycle)",
            )
        })?;
    gix_hash::ObjectId::from_hex(sha_hex.as_bytes())
        .map_err(|e| format!("flow '{flow}' last_sha is not valid hex: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CredentialId, Destination, DiscordTemplateConfig, DiscordWebhookConfig, FireEvent,
        LocalMailConfig, LocalMailTemplateConfig, PollOverride, SourceConfig,
    };
    use crate::state::FlowState;
    use std::collections::BTreeMap;

    fn flow_with_action(name: &str, inputs: BTreeMap<String, String>) -> FlowConfig {
        FlowConfig {
            name: name.to_string(),
            enabled: true,
            description: Some("ci".to_string()),
            source: SourceConfig {
                url: "https://github.com/myorg/linux-builder.git".to_string(),
                ref_name: "refs/heads/main".to_string(),
                credential_id: None,
            },
            action: ActionConfig::GithubWorkflowDispatch {
                repo: "myorg/linux-builder".to_string(),
                workflow: "ci.yml".to_string(),
                ref_name: "refs/heads/main".to_string(),
                credential_id: CredentialId::new("github_pat").expect("valid id"),
                inputs,
            },
            destination: vec![],
            poll: PollOverride::default(),
        }
    }

    #[test]
    fn read_last_sha_returns_observed_sha_when_state_has_entry() {
        let mut s = State::default();
        s.flows.insert(
            "flow1".to_string(),
            FlowState {
                last_sha: Some("deadbeefcafe1234567890abcdef1234567890ab".to_string()),
                ..Default::default()
            },
        );
        let mirror = Arc::new(StdMutex::new(s));
        let sha = read_last_sha(&mirror, "flow1").expect("must succeed");
        assert_eq!(sha.to_string(), "deadbeefcafe1234567890abcdef1234567890ab");
    }

    #[test]
    fn read_last_sha_errors_when_flow_has_no_state_entry() {
        let mirror = Arc::new(StdMutex::new(State::default()));
        let err = read_last_sha(&mirror, "missing-flow").expect_err("must error");
        assert!(
            err.contains("missing-flow") && err.contains("no recorded state"),
            "error must name the flow and reason; got: {err}",
        );
    }

    #[test]
    fn read_last_sha_errors_when_flow_has_state_but_no_sha_yet() {
        // FlowState is present (e.g. PollTimestamp landed without a
        // matching SHA observation, or first poll cycle hasn't fired
        // yet). `last_sha` is None — manual trigger requires a sha so
        // this surfaces as a specific error.
        let mut s = State::default();
        s.flows.insert(
            "flow1".to_string(),
            FlowState {
                last_sha: None,
                ..Default::default()
            },
        );
        let mirror = Arc::new(StdMutex::new(s));
        let err = read_last_sha(&mirror, "flow1").expect_err("must error");
        assert!(
            err.contains("no observed sha yet"),
            "error must explain why; got: {err}",
        );
    }

    #[test]
    fn read_last_sha_errors_when_stored_hex_is_malformed() {
        let mut s = State::default();
        s.flows.insert(
            "flow1".to_string(),
            FlowState {
                last_sha: Some("not-valid-hex".to_string()),
                ..Default::default()
            },
        );
        let mirror = Arc::new(StdMutex::new(s));
        let err = read_last_sha(&mirror, "flow1").expect_err("must error");
        assert!(
            err.contains("not valid hex"),
            "error must name the parse failure; got: {err}",
        );
    }

    #[test]
    fn render_dry_run_payload_returns_json_with_repo_workflow_ref_and_run_id() {
        let mut inputs = BTreeMap::new();
        inputs.insert("static_key".to_string(), "static_value".to_string());
        let flow = flow_with_action("ci-flow", inputs);
        let v = render_dry_run_payload(&flow).expect("render must succeed");
        assert_eq!(v["flow"], "ci-flow");
        assert_eq!(v["dry_run"], true);
        assert_eq!(v["repo"], "myorg/linux-builder");
        assert_eq!(v["workflow"], "ci.yml");
        assert_eq!(v["ref"], "refs/heads/main");
        let run_id_str = v["gcit_run_id"].as_str().expect("string");
        // `Uuid::new_v4` produces a 36-char canonical v4 UUID with
        // hyphens — pin the structural shape rather than a literal
        // value (which would be non-deterministic).
        assert_eq!(run_id_str.len(), 36, "uuid v4 canonical length");
        assert!(
            run_id_str.contains('-'),
            "uuid v4 canonical must contain hyphens; got: {run_id_str}",
        );
        // Static input rendered as-is (no handlebars expansion).
        // `rendered_inputs` is a flat JSON object keyed by input
        // name (per `build_inputs_payload`'s shape — see
        // src/github/dispatcher.rs::build_inputs_payload).
        let inputs = v["rendered_inputs"]
            .as_object()
            .expect("rendered_inputs object present");
        assert_eq!(inputs["static_key"], "static_value");
    }

    #[test]
    fn render_dry_run_payload_renders_handlebars_templates() {
        let mut inputs = BTreeMap::new();
        inputs.insert("flow_name".to_string(), "{{flow.name}}".to_string());
        inputs.insert("repo".to_string(), "{{action.repo}}".to_string());
        let flow = flow_with_action("ci-flow", inputs);
        let v = render_dry_run_payload(&flow).expect("render must succeed");
        let inputs = v["rendered_inputs"]
            .as_object()
            .expect("rendered_inputs object present");
        assert_eq!(inputs["flow_name"], "ci-flow");
        assert_eq!(inputs["repo"], "myorg/linux-builder");
    }

    #[test]
    fn render_dry_run_payload_errors_on_undefined_handlebars_variable() {
        // strict_handlebars must reject undefined variables — the dry-
        // run path mirrors the production render path so an operator
        // running `gcit trigger --dry-run` after editing inputs sees
        // the same error the daemon would surface at dispatch time.
        let mut inputs = BTreeMap::new();
        inputs.insert(
            "broken".to_string(),
            "{{undefined.variable.path}}".to_string(),
        );
        let flow = flow_with_action("ci-flow", inputs);
        let err = render_dry_run_payload(&flow).expect_err("must error");
        assert!(
            err.starts_with("render inputs (dry-run):"),
            "error must lead with the render-stage prefix; got: {err}",
        );
    }

    #[test]
    fn render_dry_run_payload_injects_gcit_run_id_into_inputs() {
        // The dispatcher's build_inputs_payload always injects
        // `gcit_run_id` into the rendered_inputs object so the
        // downstream workflow has access to the same UUID. Pin that
        // contract on the dry-run path so an operator's preview shows
        // the same shape the wire payload would carry.
        let flow = flow_with_action("ci-flow", BTreeMap::new());
        let v = render_dry_run_payload(&flow).expect("render must succeed");
        let inputs = v["rendered_inputs"]
            .as_object()
            .expect("rendered_inputs object present");
        let injected = inputs["gcit_run_id"]
            .as_str()
            .expect("gcit_run_id must be present in rendered_inputs");
        // The injected value matches the top-level `gcit_run_id`
        // (same generation point — render_dry_run_payload calls
        // Uuid::new_v4 once and threads it through both fields).
        assert_eq!(injected, v["gcit_run_id"].as_str().expect("string"));
    }

    /// Pins the Destination::DiscordWebhook + Destination::LocalMail
    /// shapes used elsewhere in the supervisor — surfaces a clear
    /// compile-time failure if either variant's required fields drift.
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

    use crate::control::Handler as _;
    use crate::flow::supervisor::FlowLastError;

    /// Build a ControlHandler with the supplied flow_names, last_errors,
    /// and state. The cmd_tx mpsc is constructed but never sent into;
    /// status / version paths are read-only and do not exercise it.
    fn handler_with(
        flow_names: Vec<String>,
        last_errors: BTreeMap<String, FlowLastError>,
        state: State,
    ) -> Arc<ControlHandler> {
        // 1-slot mpsc is enough — status/version don't enqueue.
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
        // A freshly-spawned flow that has not yet recorded its first
        // poll or panic surfaces as `state: starting`. Pin so a
        // regression that defaulted to e.g. "running" doesn't hide
        // initial-cycle failures.
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
        // A flow with a state.json entry but no recent error renders
        // as "running" — pinned by the (None, Some(_)) match arm in
        // the status renderer.
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
        // A flow with an entry in last_errors renders as "errored"
        // even when the state mirror also has a healthy snapshot.
        // The (Some, _) branch wins regardless of state presence.
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
        // Move the recorded entry into a fresh map for the handler.
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
        // run_reload records a parse-failure under the `(reload)`
        // synthetic key (per types.rs::RELOAD_SYNTHETIC_KEY). The
        // status handler must include synthetic keys in the all-flows
        // response even though they aren't in flow_names — operators
        // running `gcit status` after a typo'd SIGHUP must see the
        // failure.
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
        // `gcit status <flow>` filters to a single flow. The handler
        // returns a 1-key object even when other flows exist.
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
        // The status handler refuses to render a flow that's not in
        // flow_names — operators get a clear error rather than an
        // empty/synthetic placeholder. Pin both the `Err` shape and
        // the message body so the CLI's error path stays accurate.
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
        // `version` is the only handler arm that doesn't consult any
        // shared state — it returns a constant from the package
        // manifest so operators can confirm the daemon binary they're
        // talking to. Pin the field name + the value matches the
        // build-time CARGO_PKG_VERSION env var.
        let h = handler_with(Vec::new(), BTreeMap::new(), State::default());
        let v = h.version().await.expect("version must succeed");
        assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
    }

    #[tokio::test]
    async fn status_includes_active_and_notified_run_counts() {
        // active_runs + notified_runs are surfaced as counts (not the
        // full RunState entries) so operators can spot a flow that's
        // accumulated many in-flight runs without reading state.json.
        // Pin that the count matches the Vec length, including the
        // mixed case where active_runs and notified_runs are both
        // populated.
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
    /// command channel appears closed from the handler's perspective —
    /// `cmd_tx.send` returns SendError, the handler's `.map_err`
    /// converts it to "supervisor command channel closed".
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
        // The supervisor exited (or the cmd mpsc was never wired up).
        // The `.map_err(|_| "supervisor command channel closed"...)`
        // arm in `trigger` must surface that exact text rather than
        // panicking on the SendError.
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
        // Companion to the trigger arm above. `reload` shares the same
        // map_err shape — pin both so a regression on one doesn't
        // slide past on the back of the other.
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

    #[tokio::test]
    async fn trigger_reports_supervisor_reply_dropped_when_reply_oneshot_dropped() {
        // The supervisor accepted the cmd onto cmd_rx but exited before
        // sending a reply on the oneshot. The handler awaits
        // `reply_rx`; when the supervisor drops the oneshot Sender
        // half (per the `ControlCommand::Trigger { reply, .. }` move),
        // `reply_rx.await` returns Err and the `.map_err` arm in
        // `trigger` surfaces "supervisor reply dropped".
        let (cmd_tx, mut cmd_rx) = mpsc::channel(1);
        let h = Arc::new(ControlHandler {
            cmd_tx,
            state_mirror: Arc::new(StdMutex::new(State::default())),
            last_errors: Arc::new(Mutex::new(BTreeMap::new())),
            flow_names: Arc::new(RwLock::new(Vec::new())),
        });
        let supervisor_stub = tokio::spawn(async move {
            // Recv the cmd, drop the reply oneshot Sender without
            // sending. The handler's reply_rx.await observes the drop.
            match cmd_rx.recv().await {
                Some(ControlCommand::Trigger { reply, .. }) => drop(reply),
                Some(ControlCommand::Reload { .. }) => {
                    panic!("expected Trigger; got Reload — handler routed the wrong arm");
                }
                None => panic!("cmd channel closed unexpectedly"),
            }
        });
        let err = h
            .trigger("ci", false)
            .await
            .expect_err("must error when reply oneshot dropped");
        assert!(
            err.contains("supervisor reply dropped"),
            "trigger must surface 'supervisor reply dropped'; got: {err}",
        );
        supervisor_stub
            .await
            .expect("supervisor stub task panicked");
    }

    #[tokio::test]
    async fn reload_reports_supervisor_reply_dropped_when_reply_oneshot_dropped() {
        // Companion to the trigger arm above. `reload` shares the same
        // `reply_rx.await.map_err(|_| "supervisor reply dropped"...)`
        // shape. The supervisor accepts the cmd onto cmd_rx but exits
        // before sending a reply on the oneshot; reply_rx.await
        // observes the drop and the handler surfaces the canonical
        // message. Pin both arms (Trigger + Reload) so a regression
        // on the Reload arm doesn't slide past on the back of the
        // Trigger one.
        let (cmd_tx, mut cmd_rx) = mpsc::channel(1);
        let h = Arc::new(ControlHandler {
            cmd_tx,
            state_mirror: Arc::new(StdMutex::new(State::default())),
            last_errors: Arc::new(Mutex::new(BTreeMap::new())),
            flow_names: Arc::new(RwLock::new(Vec::new())),
        });
        let supervisor_stub = tokio::spawn(async move {
            // Recv the cmd, drop the reply oneshot Sender without
            // sending. The handler's reply_rx.await observes the drop.
            match cmd_rx.recv().await {
                Some(ControlCommand::Reload { reply }) => drop(reply),
                Some(ControlCommand::Trigger { .. }) => {
                    panic!("expected Reload; got Trigger — handler routed the wrong arm");
                }
                None => panic!("cmd channel closed unexpectedly"),
            }
        });
        let err = h
            .reload()
            .await
            .expect_err("must error when reply oneshot dropped");
        assert!(
            err.contains("supervisor reply dropped"),
            "reload must surface 'supervisor reply dropped'; got: {err}",
        );
        supervisor_stub
            .await
            .expect("supervisor stub task panicked");
    }

    /// Construct a Config carrying a single FlowConfig. `enabled`
    /// is taken from the caller so disabled-flow tests share the
    /// helper. The other Config fields are set to their defaults so
    /// run_trigger only consults `flow`.
    fn config_with_flow(flow: FlowConfig) -> Config {
        Config {
            source_path: std::path::PathBuf::from("test.toml"),
            poll: crate::config::PollDefaults::default(),
            log: crate::config::LogConfig::default(),
            http: crate::config::HttpConfig::default(),
            flow: vec![flow],
            credential_lines: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn run_trigger_errors_when_flow_is_disabled_in_current_config() {
        // run_trigger short-circuits on `!flow_cfg.enabled` before
        // consulting the state mirror or the handles map. The
        // operator-facing message names the flow and surfaces the
        // disabled state so `gcit trigger <flow>` against a flow that
        // was disabled in TOML doesn't silently inject a synthetic
        // signal into a quiesced dispatcher.
        let mut flow = flow_with_action("ci-flow", BTreeMap::new());
        flow.enabled = false;
        let cfg = Arc::new(config_with_flow(flow));
        let (config_watch, _rx) = watch::channel(cfg);
        let config_watch = Arc::new(config_watch);
        let mirror = Arc::new(StdMutex::new(State::default()));
        let handles: BTreeMap<String, FlowHandle> = BTreeMap::new();

        let err = run_trigger("ci-flow", false, &handles, config_watch, &mirror)
            .await
            .expect_err("disabled flow must reject trigger");
        assert!(
            err.contains("'ci-flow'") && err.contains("is disabled"),
            "error must name the flow in single quotes and the disabled state; got: {err}",
        );
    }

    #[tokio::test]
    async fn run_trigger_errors_when_flow_is_not_in_current_config() {
        // The first lookup in run_trigger is `cfg.flow.iter().find(|f|
        // f.name == flow_name).ok_or_else(...)`. When the operator
        // runs `gcit trigger <flow>` against a name that the daemon's
        // current config does not know about (e.g. typo, or the flow
        // was removed via SIGHUP reload), the handler must surface
        // the canonical "not found in current config" message rather
        // than panicking or returning a misleading "not running" /
        // "disabled" body.
        let flow = flow_with_action("ci-flow", BTreeMap::new());
        let cfg = Arc::new(config_with_flow(flow));
        let (config_watch, _rx) = watch::channel(cfg);
        let config_watch = Arc::new(config_watch);
        let mirror = Arc::new(StdMutex::new(State::default()));
        let handles: BTreeMap<String, FlowHandle> = BTreeMap::new();

        let err = run_trigger("missing-flow", false, &handles, config_watch, &mirror)
            .await
            .expect_err("flow absent from config must reject trigger");
        assert!(
            err.contains("'missing-flow'") && err.contains("not found in current config"),
            "error must name the flow in single quotes and the missing-from-config reason; got: {err}",
        );
    }

    #[tokio::test]
    async fn run_trigger_errors_when_flow_is_not_currently_running() {
        // The flow is enabled in config and the state mirror has a
        // recorded last_sha — but the supervisor's handles map is
        // empty (no FlowHandle registered for this flow). This
        // exercises the `handles.get(flow_name).ok_or_else(...)` arm
        // which fires when a flow is in the active config but its
        // dispatcher pair has not yet been spawned (e.g. after a
        // notifier_setup failure or while a respawn is in flight).
        // The error is non-suppressible — operators get an explicit
        // "not currently running" rather than a silent dispatch into
        // the void.
        let flow = flow_with_action("ci-flow", BTreeMap::new());
        let cfg = Arc::new(config_with_flow(flow));
        let (config_watch, _rx) = watch::channel(cfg);
        let config_watch = Arc::new(config_watch);
        let mut state = State::default();
        state.flows.insert(
            "ci-flow".to_string(),
            FlowState {
                last_sha: Some("deadbeefcafe1234567890abcdef1234567890ab".to_string()),
                ..Default::default()
            },
        );
        let mirror = Arc::new(StdMutex::new(state));
        let handles: BTreeMap<String, FlowHandle> = BTreeMap::new();

        let err = run_trigger("ci-flow", false, &handles, config_watch, &mirror)
            .await
            .expect_err("missing handle must reject trigger");
        assert!(
            err.contains("'ci-flow'") && err.contains("is not currently running"),
            "error must name the flow in single quotes and the running-state; got: {err}",
        );
    }

    #[tokio::test]
    async fn run_trigger_real_path_enqueues_trigger_signal_with_observed_sha() {
        // Happy-path through the real (non-dry-run) trigger arm. With
        // the flow enabled in config, the state mirror carrying a hex
        // last_sha, and a FlowHandle registered in the handles map,
        // run_trigger:
        //   1. resolves the flow_cfg, finds it enabled,
        //   2. reads the persisted last_sha out of the state mirror,
        //   3. looks up the handle in the BTreeMap,
        //   4. sends a TriggerSignal to handle.trigger_tx.
        //
        // The receiver end (kept alive on the test side) observes the
        // signal with the same sha; the JSON return body carries
        // "trigger enqueued" and the sha hex. Pin both sides — a
        // regression that reorders the steps or drops the send is
        // caught by the missing recv. A regression that mangles the
        // JSON shape is caught by the body-field assertions.
        let flow = flow_with_action("ci-flow", BTreeMap::new());
        let cfg = Arc::new(config_with_flow(flow));
        let (config_watch, _rx) = watch::channel(cfg);
        let config_watch = Arc::new(config_watch);
        let expected_sha_hex = "deadbeefcafe1234567890abcdef1234567890ab";
        let mut state = State::default();
        state.flows.insert(
            "ci-flow".to_string(),
            FlowState {
                last_sha: Some(expected_sha_hex.to_string()),
                ..Default::default()
            },
        );
        let mirror = Arc::new(StdMutex::new(state));
        // Build a FlowHandle whose trigger_tx is wired to a test
        // receiver. crate::flow::TRIGGER_QUEUE (= 8) is the production
        // capacity; mirror it so the test sends behave like the real
        // dispatcher mpsc.
        let (trigger_tx, mut trigger_rx) =
            mpsc::channel::<crate::flow::TriggerSignal>(crate::flow::TRIGGER_QUEUE);
        let mut handles: BTreeMap<String, FlowHandle> = BTreeMap::new();
        handles.insert(
            "ci-flow".to_string(),
            FlowHandle {
                cancel: tokio_util::sync::CancellationToken::new(),
                trigger_tx,
            },
        );

        let body = run_trigger("ci-flow", false, &handles, config_watch, &mirror)
            .await
            .expect("real-path trigger must succeed");
        assert_eq!(body["flow"], "ci-flow");
        assert_eq!(body["result"], "trigger enqueued");
        assert_eq!(body["observed_sha"], expected_sha_hex);

        // The signal must have arrived at the dispatcher's recv-end
        // with the same sha threaded out of the state mirror.
        let signal = trigger_rx
            .recv()
            .await
            .expect("trigger signal must arrive on dispatcher channel");
        assert_eq!(signal.observed_sha.to_string(), expected_sha_hex);
    }
}

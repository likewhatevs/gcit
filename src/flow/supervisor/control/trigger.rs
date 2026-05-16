// `gcit trigger <flow> [--dry-run]` implementation. Real path
// injects a synthetic `TriggerSignal` into the dispatcher's mpsc;
// dry-run renders the dispatch payload without contacting GitHub.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::watch;

use crate::config::{ActionConfig, Config, FlowConfig};
use crate::state::State;

use super::super::types::FlowHandle;

/// Implement `gcit trigger <flow> [--dry-run]`.
///
/// Real path (`dry_run = false`): look up the flow's dispatcher
/// trigger_tx; load the most recent observed sha from the state
/// mirror; send a synthetic TriggerSignal into the dispatcher's
/// mpsc. The dispatcher does the rest.
///
/// Dry-run path (`dry_run = true`): render the dispatch payload
/// (rendered_inputs + injected gcit_run_id) without contacting GitHub
/// or any notifier. The rendered map + the would-be ref/repo/workflow
/// are returned as JSON.
pub(super) async fn run_trigger(
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
        return Err(format!("flow '{flow_name}' is disabled in current config"));
    }

    if dry_run {
        return render_dry_run_payload(flow_cfg);
    }

    // Real path: pull the most recent observed sha so the dispatcher's
    // RunContext.source.sha is real. If no observation has landed yet,
    // refuse — we can't fabricate a sha.
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
/// the dispatcher's render path (`render_inputs` +
/// `build_inputs_payload`) so an operator sees what would land in
/// the `workflow_dispatch` body.
///
/// `Uuid::new_v4()` is used for the gcit_run_id so the preview is
/// realistic. A nil UUID would render the workflow's `run-name` as
/// `gcit-00000000...` which the operator could not correlate against.
fn render_dry_run_payload(flow: &FlowConfig) -> Result<serde_json::Value, String> {
    let ActionConfig::GithubWorkflowDispatch {
        repo,
        workflow,
        ref_name,
        inputs,
        ..
    } = &flow.action;
    let gcit_run_id = uuid::Uuid::new_v4();
    // Probe context — dry-run renders against a synthetic context;
    // the rendered output is for operator inspection only, never over
    // the wire. Shape matches `config::validate::probe_context`.
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
    use crate::config::{CredentialId, PollOverride, SourceConfig};
    use crate::flow::TRIGGER_QUEUE;
    use crate::state::FlowState;
    use tokio::sync::mpsc;

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
        // FlowState present but `last_sha: None` (PollTimestamp landed
        // without a matching SHA observation, or first poll cycle
        // hasn't fired yet). Manual trigger requires a sha.
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
        // hyphens — pin the structural shape, not a literal value.
        assert_eq!(run_id_str.len(), 36, "uuid v4 canonical length");
        assert!(
            run_id_str.contains('-'),
            "uuid v4 canonical must contain hyphens; got: {run_id_str}",
        );
        // Static input rendered as-is (no handlebars expansion).
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
        // strict_handlebars rejects undefined variables — the dry-run
        // path mirrors the production render so an operator running
        // `gcit trigger --dry-run` sees the same error.
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
        // build_inputs_payload always injects gcit_run_id into the
        // rendered_inputs object so the workflow has access to the
        // same UUID. Pin that contract on the dry-run path.
        let flow = flow_with_action("ci-flow", BTreeMap::new());
        let v = render_dry_run_payload(&flow).expect("render must succeed");
        let inputs = v["rendered_inputs"]
            .as_object()
            .expect("rendered_inputs object present");
        let injected = inputs["gcit_run_id"]
            .as_str()
            .expect("gcit_run_id must be present in rendered_inputs");
        // Injected value matches the top-level gcit_run_id (same
        // generation point — render_dry_run_payload calls Uuid::new_v4
        // once and threads it through both fields).
        assert_eq!(injected, v["gcit_run_id"].as_str().expect("string"));
    }

    #[tokio::test]
    async fn run_trigger_errors_when_flow_is_disabled_in_current_config() {
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
            "error must name the flow and the missing-from-config reason; got: {err}",
        );
    }

    #[tokio::test]
    async fn run_trigger_errors_when_flow_is_not_currently_running() {
        // Flow is enabled in config + state has last_sha, but
        // handles is empty (notifier_setup failure or mid-respawn).
        // Operators get an explicit "not currently running".
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
            "error must name the flow and running-state; got: {err}",
        );
    }

    #[tokio::test]
    async fn run_trigger_real_path_enqueues_trigger_signal_with_observed_sha() {
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
        let (trigger_tx, mut trigger_rx) =
            mpsc::channel::<crate::flow::TriggerSignal>(TRIGGER_QUEUE);
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

        // Signal arrives at the dispatcher's recv-end with the same
        // sha threaded out of the state mirror.
        let signal = trigger_rx
            .recv()
            .await
            .expect("trigger signal must arrive on dispatcher channel");
        assert_eq!(signal.observed_sha.to_string(), expected_sha_hex);
    }
}

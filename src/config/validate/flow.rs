// Per-flow validation: orchestrator + source/action validators.
// Destinations live in `destination.rs`; cadence in `cadence.rs`.

use std::collections::BTreeMap;
use std::path::Path;

use toml::Spanned;

use super::super::credential::CredentialId;
use super::super::error::ConfigError;
use super::super::parse::{
    ActionConfig, FlowConfig, RawActionConfig, RawFlowConfig, RawSourceConfig, SourceConfig,
};
use super::cadence::validate_poll_override;
use super::credential::validate_credential_id;
use super::destination::validate_destinations;
use super::{span_line, validate_err, MAX_FLOW_NAME_LEN};

/// Validate flow.name charset + length. Returns `true` iff the name
/// passes every check; only valid names participate in
/// duplicate-name detection.
pub(super) fn validate_flow_name(
    spanned: &Spanned<String>,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
) -> bool {
    let name = spanned.get_ref();
    let line = span_line(source, spanned);
    if name.is_empty() {
        errors.push(validate_err(
            path,
            vec![line],
            None,
            "flow.name",
            name.clone(),
            "flow.name must be non-empty",
            "use a descriptive name like 'linux-mainline-ci'",
        ));
        return false;
    }
    if name.len() > MAX_FLOW_NAME_LEN {
        errors.push(validate_err(
            path,
            vec![line],
            Some(name),
            "flow.name",
            name.clone(),
            format!(
                "flow.name is {} chars; max is {}",
                name.len(),
                MAX_FLOW_NAME_LEN
            ),
            format!("shorten to {} chars or fewer", MAX_FLOW_NAME_LEN),
        ));
        return false;
    }
    for ch in name.chars() {
        let ok = ch.is_ascii_alphanumeric() || ch == '_' || ch == '-';
        if !ok {
            errors.push(validate_err(
                path,
                vec![line],
                Some(name),
                "flow.name",
                name.clone(),
                format!(
                    "flow.name contains invalid character {:?}; allowed: A-Z a-z 0-9 _ -",
                    ch
                ),
                "use only A-Z, a-z, 0-9, '_', and '-'",
            ));
            return false;
        }
    }
    true
}

#[allow(clippy::too_many_arguments)]
pub(super) fn validate_flow(
    raw: &RawFlowConfig,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
    env_var_index: &mut BTreeMap<String, Vec<(String, usize, String)>>,
    credential_lines: &mut BTreeMap<CredentialId, Vec<usize>>,
    flow_name: &str,
) -> FlowConfig {
    let source_cfg = validate_source(
        &raw.source,
        source,
        path,
        errors,
        env_var_index,
        credential_lines,
        flow_name,
    );
    let action_cfg = validate_action(
        &raw.action,
        source,
        path,
        errors,
        env_var_index,
        credential_lines,
        flow_name,
    );
    let destinations = validate_destinations(
        &raw.destination,
        source,
        path,
        errors,
        env_var_index,
        credential_lines,
        flow_name,
    );
    let poll_override = validate_poll_override(&raw.poll, source, path, errors, flow_name);
    FlowConfig {
        name: flow_name.to_string(),
        enabled: raw.enabled,
        description: raw.description.clone(),
        source: source_cfg,
        action: action_cfg,
        destination: destinations,
        poll: poll_override,
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_source(
    raw: &RawSourceConfig,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
    env_var_index: &mut BTreeMap<String, Vec<(String, usize, String)>>,
    credential_lines: &mut BTreeMap<CredentialId, Vec<usize>>,
    flow_name: &str,
) -> SourceConfig {
    let url = raw.url.get_ref().clone();
    let url_line = span_line(source, &raw.url);
    let ref_name = raw.ref_name.get_ref().clone();
    let ref_line = span_line(source, &raw.ref_name);

    match url::Url::parse(&url) {
        Err(e) => {
            errors.push(validate_err(
                path,
                vec![url_line],
                Some(flow_name),
                "source.url",
                url.clone(),
                format!("source.url is not a valid URL: {}", e),
                "use a full URL like 'https://github.com/owner/repo.git'",
            ));
        }
        Ok(parsed) => {
            const ALLOWED_SCHEMES: &[&str] = &["http", "https", "ssh", "git", "file"];
            let scheme = parsed.scheme();
            if !ALLOWED_SCHEMES.contains(&scheme) {
                errors.push(validate_err(
                    path,
                    vec![url_line],
                    Some(flow_name),
                    "source.url",
                    url.clone(),
                    format!(
                        "source.url has unsupported scheme '{}'; allowed: {}",
                        scheme,
                        ALLOWED_SCHEMES.join(", ")
                    ),
                    format!(
                        "use one of: {}",
                        ALLOWED_SCHEMES
                            .iter()
                            .map(|s| format!("{}://", s))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                ));
            }
        }
    }

    if !ref_name.starts_with("refs/") {
        errors.push(validate_err(
            path,
            vec![ref_line],
            Some(flow_name),
            "source.ref",
            ref_name.clone(),
            "source.ref must start with 'refs/'",
            format!("use 'refs/heads/{}' for a branch", ref_name),
        ));
    }

    let credential_id = match &raw.credential_id {
        Some(spanned) => validate_credential_id(
            spanned,
            "source.credential_id",
            flow_name,
            source,
            path,
            errors,
            env_var_index,
            credential_lines,
        ),
        None => None,
    };

    SourceConfig {
        url,
        ref_name,
        credential_id,
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_action(
    raw: &RawActionConfig,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
    env_var_index: &mut BTreeMap<String, Vec<(String, usize, String)>>,
    credential_lines: &mut BTreeMap<CredentialId, Vec<usize>>,
    flow_name: &str,
) -> ActionConfig {
    let kind_str = raw.kind.get_ref().as_str();
    let kind_line = span_line(source, &raw.kind);

    match kind_str {
        "github_workflow_dispatch" => {
            let repo_str =
                require_field(&raw.repo, "action.repo", flow_name, kind_line, path, errors);
            let workflow_str = require_field(
                &raw.workflow,
                "action.workflow",
                flow_name,
                kind_line,
                path,
                errors,
            );
            let ref_str = require_field(
                &raw.ref_name,
                "action.ref",
                flow_name,
                kind_line,
                path,
                errors,
            );

            // repo: exactly one '/', neither side empty.
            if let Some(repo_spanned) = &raw.repo {
                let s = repo_spanned.get_ref();
                let line = span_line(source, repo_spanned);
                let parts: Vec<&str> = s.split('/').collect();
                let ok = parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty();
                if !ok {
                    errors.push(validate_err(
                        path,
                        vec![line],
                        Some(flow_name),
                        "action.repo",
                        s.clone(),
                        "action.repo must match owner/repo (exactly one '/', both sides non-empty)",
                        "use the form 'owner/repo' (for example, 'octocat/hello-world')",
                    ));
                }
            }

            // workflow: non-empty, no '/', '\\', '..'.
            if let Some(workflow_spanned) = &raw.workflow {
                let s = workflow_spanned.get_ref();
                let line = span_line(source, workflow_spanned);
                if s.is_empty() {
                    errors.push(validate_err(
                        path,
                        vec![line],
                        Some(flow_name),
                        "action.workflow",
                        s.clone(),
                        "action.workflow must be non-empty",
                        "use the workflow file name like 'ci.yml'",
                    ));
                } else if s.contains('/') || s.contains('\\') || s.contains("..") {
                    errors.push(validate_err(
                        path,
                        vec![line],
                        Some(flow_name),
                        "action.workflow",
                        s.clone(),
                        "action.workflow must not contain '/', '\\', or '..'",
                        "use a bare file name like 'ci.yml' (no directory components)",
                    ));
                }
            }

            // ref_name: must start with "refs/".
            if let Some(ref_spanned) = &raw.ref_name {
                let s = ref_spanned.get_ref();
                let line = span_line(source, ref_spanned);
                if !s.starts_with("refs/") {
                    errors.push(validate_err(
                        path,
                        vec![line],
                        Some(flow_name),
                        "action.ref",
                        s.clone(),
                        "action.ref must start with 'refs/'",
                        format!("use 'refs/heads/{}' for a branch", s),
                    ));
                }
            }

            let cid = match &raw.credential_id {
                Some(spanned) => validate_credential_id(
                    spanned,
                    "action.credential_id",
                    flow_name,
                    source,
                    path,
                    errors,
                    env_var_index,
                    credential_lines,
                ),
                None => {
                    errors.push(validate_err(
                        path,
                        vec![kind_line],
                        Some(flow_name),
                        "action.credential_id",
                        "<missing>",
                        "action.credential_id is required for github_workflow_dispatch",
                        "add credential_id = \"<id>\" under [flow.action]",
                    ));
                    None
                }
            };

            // Reject a user-supplied `gcit_run_id` input. The
            // dispatcher injects this key at dispatch time so the
            // workflow's run-name directive can correlate the run.
            // A user-supplied value would be silently overwritten.
            // The key string is sourced from the dispatcher so a rename
            // can't drift between the validator and the injector.
            let gcit_run_id_key = crate::github::dispatcher::GCIT_RUN_ID_INPUT;
            if let Some(operator_value) = raw.inputs.get(gcit_run_id_key) {
                errors.push(validate_err(
                    path,
                    vec![kind_line],
                    Some(flow_name),
                    format!("action.inputs.{gcit_run_id_key}"),
                    operator_value.clone(),
                    format!(
                        "action.inputs may not contain `{gcit_run_id_key}`; gcit injects this key automatically at dispatch time (the value you supplied would be silently overwritten)",
                    ),
                    format!(
                        "remove the `{gcit_run_id_key}` entry from action.inputs (gcit supplies the value automatically)",
                    ),
                ));
            }

            ActionConfig::GithubWorkflowDispatch {
                repo: repo_str,
                workflow: workflow_str,
                ref_name: ref_str,
                // unreachable at runtime: `validate` returns Err when
                // errors is non-empty, so the missing-credential
                // branch never reaches the typed Config consumer.
                credential_id: cid.unwrap_or_else(|| {
                    CredentialId::new("placeholder").expect("placeholder is a valid id")
                }),
                inputs: raw.inputs.clone(),
            }
        }
        other => {
            errors.push(validate_err(
                path,
                vec![kind_line],
                Some(flow_name),
                "action.kind",
                other,
                format!(
                    "unknown action kind {:?}; valid kinds: github_workflow_dispatch",
                    other
                ),
                "set kind = \"github_workflow_dispatch\"",
            ));
            ActionConfig::GithubWorkflowDispatch {
                repo: String::new(),
                workflow: String::new(),
                ref_name: String::new(),
                credential_id: CredentialId::new("placeholder").expect("placeholder is a valid id"),
                inputs: BTreeMap::new(),
            }
        }
    }
}

pub(super) fn require_field(
    spanned: &Option<Spanned<String>>,
    field: &'static str,
    flow_name: &str,
    fallback_line: usize,
    path: &Path,
    errors: &mut Vec<ConfigError>,
) -> String {
    match spanned {
        Some(s) => s.get_ref().clone(),
        None => {
            errors.push(validate_err(
                path,
                vec![fallback_line],
                Some(flow_name),
                field,
                "<missing>",
                format!("{} is required", field),
                format!("add {} = \"...\" to the relevant table", field),
            ));
            String::new()
        }
    }
}

// Credential-id validation, env-var collision tracking, and the
// fire_on list collector (which also detects duplicates).

use std::collections::BTreeMap;
use std::path::Path;

use toml::Spanned;

use super::super::credential::{CredentialId, IdError};
use super::super::error::ConfigError;
use super::super::parse::FireEvent;
use super::{span_line, validate_err};

/// Validate a credential id and record its env-var occurrence for
/// later collision detection. Returns `Some` only when the id passes
/// every charset/length/path-traversal check.
///
/// Invalid ids are NOT recorded in the env-var index — reporting the
/// same id again as part of a collision would be noise atop the
/// per-id validation error.
#[allow(clippy::too_many_arguments)]
pub(super) fn validate_credential_id(
    spanned: &Spanned<String>,
    field: &'static str,
    flow_name: &str,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
    env_var_index: &mut BTreeMap<String, Vec<(String, usize, String)>>,
    credential_lines: &mut BTreeMap<CredentialId, Vec<usize>>,
) -> Option<CredentialId> {
    let id = spanned.get_ref().clone();
    let line = span_line(source, spanned);
    match CredentialId::new(id.clone()) {
        Ok(cid) => {
            let env = cid.to_env_var();
            env_var_index
                .entry(env)
                .or_default()
                .push((id, line, flow_name.to_string()));
            credential_lines.entry(cid.clone()).or_default().push(line);
            Some(cid)
        }
        Err(e) => {
            errors.push(map_id_error(&e, &id, field, flow_name, line, path));
            None
        }
    }
}

fn map_id_error(
    err: &IdError,
    id: &str,
    field: &'static str,
    flow_name: &str,
    line: usize,
    path: &Path,
) -> ConfigError {
    validate_err(
        path,
        vec![line],
        Some(flow_name),
        field,
        id,
        err.message(),
        err.suggestion(),
    )
}

/// Collect a `fire_on` array. Duplicate events surface as a
/// `ConfigError::Validate` (one per dup) rather than silent dedup; the
/// dedup'd vector is still returned so the validator's later passes
/// operate on a clean list.
pub(super) fn collect_fire_on(
    events: &[Spanned<FireEvent>],
    field: &'static str,
    flow_name: &str,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
) -> Vec<FireEvent> {
    let mut out: Vec<FireEvent> = Vec::with_capacity(events.len());
    for s in events {
        let ev = *s.get_ref();
        if out.contains(&ev) {
            let line = span_line(source, s);
            let label = match ev {
                FireEvent::RunStart => "run_start",
                FireEvent::JobComplete => "job_complete",
                FireEvent::RunComplete => "run_complete",
            };
            errors.push(validate_err(
                path,
                vec![line],
                Some(flow_name),
                field,
                label,
                format!("fire_on contains duplicate event '{}'", label),
                format!(
                    "remove the duplicate '{}'; events fire once per occurrence",
                    label
                ),
            ));
        } else {
            out.push(ev);
        }
    }
    out
}

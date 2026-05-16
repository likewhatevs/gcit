// Validation pass: convert `RawConfig` to `Config`, accumulating every
// rule violation as a `ConfigError::Validate` (or other variant).
//
// Rules:
//   - MIN_INTERVAL=15s, MAX_INTERVAL=24h, INCLUSIVE both ends.
//   - jitter 0.0..=0.5 INCLUSIVE.
//   - http.request_timeout bounded to [1s, 300s].
//   - http.max_concurrent: deprecated (silently dropped with a WARN).
//   - flow.name unique, [a-zA-Z0-9_-]+, 1..=64 chars.
//   - source.url must parse via url::Url with scheme in
//     {http, https, ssh, git, file}.
//   - source.ref / action.ref must start with "refs/".
//   - action.repo must match owner/repo (exactly one '/').
//   - action.workflow non-empty, no '/', '\', or '..'.
//   - credential_id validated by CredentialId::new; collisions detected
//     post-parse by mapping to env var name.
//   - local_mail user [a-zA-Z0-9_-]+, 1..=32 chars.
//   - At least one flow required.
//   - fire_on duplicates surface as ConfigError::Validate (one per dup),
//     not silent dedup.
//   - Every Discord/local_mail template field must compile under
//     handlebars strict_mode AND render successfully against a probe
//     context. Bare names like {{flow}} or {{gcit_run_id}} are rejected
//     via AST inspection so they fail at config load instead of runtime.
//
// All errors are collected into `Vec<ConfigError>`; the validator never
// short-circuits. This is the contract `gcit check` uses to print every
// problem at once.
//
// Module layout (refactored from the prior single-file `validate.rs`):
//   - `cadence` — duration/jitter parsing + http + poll defaults/override
//   - `credential` — `validate_credential_id` + fire_on collection
//   - `destination` — discord_webhook + local_mail validation
//   - `flow` — per-flow orchestrator + source/action validation
//   - `spool` — `validate_spool_writability` host-state probe
//   - `template` — handlebars compile + bare-name AST check + probe ctx

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use toml::Spanned;

use super::credential::CredentialId;
use super::error::ConfigError;
use super::parse::{Config, RawConfig};

mod cadence;
mod credential;
mod destination;
mod flow;
mod spool;
mod template;

// External callers reference these via `gcit::config::validate::*`.
pub use spool::validate_spool_writability;
pub use template::{find_bare_name, probe_context};

// Internal references for the `validate()` entry below.
use cadence::{validate_http, validate_poll_defaults};
use flow::validate_flow;

/// Inclusive lower bound on poll intervals.
pub const MIN_INTERVAL: Duration = Duration::from_secs(15);
/// Inclusive upper bound on poll intervals.
pub const MAX_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
/// Inclusive lower bound on poll jitter.
pub const MIN_JITTER: f64 = 0.0;
/// Inclusive upper bound on poll jitter.
pub const MAX_JITTER: f64 = 0.5;
/// Maximum length for `local_mail` user names.
pub const MAX_LOCAL_MAIL_USER_LEN: usize = 32;
/// Maximum length for flow names.
pub const MAX_FLOW_NAME_LEN: usize = 64;
/// Inclusive lower bound for `http.request_timeout`. Without an upper
/// bound, "5M" parses (humantime months) as a multi-month timeout —
/// silently disabling the timeout.
pub const MIN_HTTP_TIMEOUT: Duration = Duration::from_secs(1);
/// Inclusive upper bound for `http.request_timeout` (5 minutes).
pub const MAX_HTTP_TIMEOUT: Duration = Duration::from_secs(300);

/// Convert a byte offset within `source` to a 1-based line number.
/// Bytes beyond the end of the source clamp to the last line.
pub fn byte_offset_to_line(source: &str, offset: usize) -> usize {
    let bytes = source.as_bytes();
    let limit = offset.min(bytes.len());
    let mut line = 1usize;
    for &b in &bytes[..limit] {
        if b == b'\n' {
            line += 1;
        }
    }
    line
}

pub(super) fn span_line(source: &str, span: &Spanned<impl Sized>) -> usize {
    byte_offset_to_line(source, span.span().start)
}

/// Construct a `ConfigError::Validate`. Centralizes the 6-field shape
/// so a future variant tweak updates one place. Empty `lines` is
/// normalized to `vec![1]` so the rendered `path:lines: ...` still
/// parses as an editor jump target.
pub(super) fn validate_err(
    path: &Path,
    lines: Vec<usize>,
    flow: Option<&str>,
    field: impl Into<String>,
    value: impl Into<String>,
    message: impl Into<String>,
    suggestion: impl Into<String>,
) -> ConfigError {
    let lines = if lines.is_empty() { vec![1] } else { lines };
    ConfigError::Validate {
        path: path.to_path_buf(),
        lines,
        flow: flow.map(|s| s.to_string()),
        field: field.into(),
        value: value.into(),
        message: message.into(),
        suggestion: suggestion.into(),
    }
}

/// Validate a parsed `RawConfig` and produce a fully typed `Config`.
///
/// Errors are collected into `Vec<ConfigError>` so the operator sees
/// every problem in one pass. On success: every duration is bounded,
/// every credential id is validated, every template compiles, and the
/// flow list is non-empty with unique names.
///
/// This pass is purely structural — it does not touch the filesystem.
/// Spool writability for `local_mail` destinations is a host-state
/// check; production callers follow this pass with
/// `validate_spool_writability` against the resolved Config.
pub(crate) fn validate(
    raw: RawConfig,
    source: &str,
    path: &Path,
) -> Result<Config, Vec<ConfigError>> {
    let mut errors: Vec<ConfigError> = Vec::new();

    let poll = validate_poll_defaults(&raw.poll, source, path, &mut errors);
    let log = super::parse::LogConfig {
        filter: raw.log.filter.clone(),
    };
    let http = validate_http(&raw.http, source, path, &mut errors);

    if raw.flow.is_empty() {
        errors.push(validate_err(
            path,
            vec![],
            None,
            "flow",
            "[]",
            "at least one flow is required",
            "add a [[flow]] block with name, source, and action",
        ));
    }

    let mut flows: Vec<super::parse::FlowConfig> = Vec::with_capacity(raw.flow.len());
    // (raw env var name) -> (id string, line, flow name) for collisions.
    let mut env_var_index: BTreeMap<String, Vec<(String, usize, String)>> = BTreeMap::new();
    // (CredentialId) -> Vec<line>. Surfaced through
    // `Config::credential_lines` so `gcit check` can include source
    // lines in CredentialNotFound errors.
    let mut credential_lines: BTreeMap<CredentialId, Vec<usize>> = BTreeMap::new();
    // (flow name) -> Vec<line>. Names failing per-flow validation are
    // skipped so the operator does not see a "duplicate empty name"
    // pair-up alongside the per-empty validation errors.
    let mut name_index: BTreeMap<String, Vec<usize>> = BTreeMap::new();

    for raw_flow in &raw.flow {
        let flow_line = span_line(source, &raw_flow.name);
        let flow_name = raw_flow.name.get_ref().clone();
        let name_ok = flow::validate_flow_name(&raw_flow.name, source, path, &mut errors);
        if name_ok {
            name_index
                .entry(flow_name.clone())
                .or_default()
                .push(flow_line);
        }

        let flow = validate_flow(
            raw_flow,
            source,
            path,
            &mut errors,
            &mut env_var_index,
            &mut credential_lines,
            &flow_name,
        );
        flows.push(flow);
    }

    // Duplicate flow names: one error per name with >1 occurrence.
    for (name, lines) in &name_index {
        if lines.len() > 1 {
            errors.push(validate_err(
                path,
                lines.clone(),
                Some(name),
                "flow.name",
                name.clone(),
                format!("duplicate flow name; {} occurrences", lines.len()),
                "rename each occurrence so flow names are unique",
            ));
        }
    }

    // Credential-id env-var collisions: two distinct ids that map to
    // the same GCIT_CREDENTIAL_* env var name are rejected.
    for (env, occurrences) in &env_var_index {
        let mut distinct_ids: Vec<&String> = occurrences.iter().map(|(id, _, _)| id).collect();
        distinct_ids.sort();
        distinct_ids.dedup();
        if distinct_ids.len() > 1 {
            let mut lines: Vec<usize> = occurrences.iter().map(|(_, l, _)| *l).collect();
            lines.sort();
            lines.dedup();
            let value = distinct_ids
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            errors.push(validate_err(
                path,
                lines,
                None,
                "credential_id",
                value,
                format!(
                    "credential_id collision: ids map to the same env var {} (rule: uppercase + s/-/_/)",
                    env
                ),
                "rename so each id maps to a unique env var (current rule: id.uppercase().replace('-','_'))",
            ));
        }
    }

    // Sort + dedup for deterministic output (and so CredentialNotFound
    // doesn't repeat lines when an id is referenced from one flow's
    // source AND action).
    for v in credential_lines.values_mut() {
        v.sort();
        v.dedup();
    }

    if errors.is_empty() {
        Ok(Config {
            source_path: path.to_path_buf(),
            poll,
            log,
            http,
            flow: flows,
            credential_lines,
        })
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_offset_to_line_zero_offset_is_line_one() {
        assert_eq!(byte_offset_to_line("first\nsecond\n", 0), 1);
    }

    #[test]
    fn byte_offset_to_line_offset_just_before_newline_is_same_line() {
        assert_eq!(byte_offset_to_line("first\nsecond\n", 5), 1);
    }

    #[test]
    fn byte_offset_to_line_offset_at_newline_advances_to_next_line() {
        assert_eq!(byte_offset_to_line("first\nsecond\n", 6), 2);
    }

    #[test]
    fn byte_offset_to_line_clamps_offset_beyond_source_length() {
        // Out-of-range offset must clamp to the last byte, not panic.
        let s = "a\nb\nc";
        assert_eq!(byte_offset_to_line(s, 999), 3);
    }

    #[test]
    fn byte_offset_to_line_empty_source_returns_one_for_any_offset() {
        assert_eq!(byte_offset_to_line("", 0), 1);
        assert_eq!(byte_offset_to_line("", 100), 1);
    }

    #[test]
    fn byte_offset_to_line_multiple_consecutive_newlines_advance_per_newline() {
        assert_eq!(byte_offset_to_line("\n\n\n", 3), 4);
    }

    #[test]
    fn validate_err_empty_lines_vec_falls_back_to_line_one() {
        // Empty lines vec must normalize to [1] so the rendered
        // `path:lines: ...` parses as an editor jump target.
        let err = validate_err(
            std::path::Path::new("inline"),
            vec![],
            None,
            "anything",
            "value",
            "msg",
            "fix it",
        );
        match err {
            ConfigError::Validate { lines, .. } => {
                assert_eq!(lines, vec![1]);
            }
            _ => panic!("validate_err must produce Validate variant"),
        }
    }

    #[test]
    fn validate_err_non_empty_lines_vec_passes_through() {
        let err = validate_err(
            std::path::Path::new("inline"),
            vec![5, 7, 11],
            Some("flow"),
            "field",
            "v",
            "m",
            "s",
        );
        match err {
            ConfigError::Validate { lines, flow, .. } => {
                assert_eq!(lines, vec![5, 7, 11]);
                assert_eq!(flow.as_deref(), Some("flow"));
            }
            _ => panic!("validate_err must produce Validate variant"),
        }
    }
}

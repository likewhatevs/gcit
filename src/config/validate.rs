// Validation pass: convert RawConfig to Config, accumulating every
// rule violation as ConfigError::Validate (or other variants).
//
// Rules:
//   - MIN_INTERVAL=15s, MAX_INTERVAL=24h, INCLUSIVE both ends.
//   - jitter 0.0..=0.5 INCLUSIVE.
//   - http.request_timeout bounded to [1s, 300s].
//   - http.max_concurrent: deprecated. Field is accepted for
//     back-compat but ignored — concurrency is bounded by
//     octocrab's per-credential rate-limiter plus the tokio
//     runtime. validate_http emits a deprecation warning and
//     drops the value.
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
// All errors are collected into Vec<ConfigError>; the validator never
// short-circuits. This is the contract gcit check uses to print every
// problem at once.

use std::collections::BTreeMap;
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use toml::Spanned;

use super::credential::{CredentialId, IdError};
use super::error::ConfigError;
use super::parse::{
    ActionConfig, Config, Destination, DiscordTemplateConfig, DiscordWebhookConfig, FireEvent,
    FlowConfig, HttpConfig, LocalMailConfig, LocalMailTemplateConfig, LogConfig, PollDefaults,
    PollOverride, RawActionConfig, RawConfig, RawDestination, RawDestinationTemplateConfig,
    RawFlowConfig, RawHttpConfig, RawPollDefaults, RawPollOverride, RawSourceConfig, SourceConfig,
};
use crate::mail::DEFAULT_SPOOL_DIR;

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
/// silently disabling the timeout. The range constrains the value to a
/// sane operator-facing window.
pub const MIN_HTTP_TIMEOUT: Duration = Duration::from_secs(1);
/// Inclusive upper bound for `http.request_timeout` (5 minutes).
pub const MAX_HTTP_TIMEOUT: Duration = Duration::from_secs(300);
// `http.max_concurrent` was previously bounded to [1, 4096] when
// the field was wired into a Semaphore. The field is now
// deprecated: octocrab's per-credential rate-limiter plus the
// tokio runtime's scheduler bound concurrency, and the operator-
// facing value has no effect. The bounds constants were removed
// alongside the field; `validate_http` emits a deprecation
// warning if the field is set in the parsed TOML.

/// Top-level template namespaces. Bare expressions matching one of
/// these names (e.g. `{{flow}}`) are rejected because the namespace
/// itself has no string representation; only its dotted leaves are
/// renderable values. Single-name expressions that do not match a
/// namespace (e.g. `{{gcit_run_id}}`) are also rejected — the
/// template variable contract requires the `namespace.field` form.
const TEMPLATE_NAMESPACES: &[&str] = &["flow", "source", "action", "run", "gcit"];

/// Convert a byte offset within `source` to a 1-based line number.
///
/// Counts `\n` bytes in `source[..offset]` and adds 1. Bytes beyond the
/// end of the source clamp to the last line. Used by every error site
/// that surfaces a span back to the operator.
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

fn span_line(source: &str, span: &Spanned<impl Sized>) -> usize {
    byte_offset_to_line(source, span.span().start)
}

/// Outcome of probing `<spool_root>/<user>` for write access via
/// `access(2)`. Maps the four operator-relevant kernel errno cases to
/// distinct error messages so each carries a remediation tailored to
/// the failure mode.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SpoolProbe {
    /// `access(W_OK)` returned 0 — the daemon's effective uid can
    /// write the spool file. Validator emits no error.
    Writable,
    /// `access(W_OK)` returned ENOENT and the parent directory
    /// (`<spool_root>`) also does not exist. Operator needs to install
    /// a mail package or create the directory itself.
    ParentMissing,
    /// `access(W_OK)` returned ENOENT but the parent directory
    /// exists. The per-user spool file is missing — operator creates
    /// it via `useradd`/`mailx`/`touch`.
    SpoolMissing,
    /// `access(W_OK)` returned EACCES. Either the mode bits exclude
    /// write for the daemon's effective uid+gid, or systemd's
    /// `ProtectSystem=strict` plus a missing
    /// `ReadWritePaths=/var/mail` is blocking the write.
    NotWritable,
    /// Any other errno (EROFS, EIO, ...). Surfaces verbatim with the
    /// errno number so operators can look it up.
    OtherError(i32),
}

/// Probe whether `path` is writable via `access(2)` with the
/// `W_OK` mode. Avoids `open(2)`+`close(2)` (which would update atime
/// on some filesystems and bumps `mtime` indirectly via the open
/// path on read-write opens) and avoids `std::fs::metadata` (which
/// only inspects the inode mode and would lie under POSIX ACLs or
/// CAP_DAC_OVERRIDE). The kernel does the full effective-uid +
/// effective-gid + supplementary-groups + ACL + capability check.
///
/// Returns `SpoolProbe::Writable` on success; on error, the errno
/// is mapped to the appropriate remediation variant.
fn probe_spool_writability(path: &Path) -> SpoolProbe {
    // libc::access takes a NUL-terminated C string. `Path` may
    // contain NUL bytes (the validator already rejects user values
    // containing NUL via `LocalMailNotifier` defense-in-depth, but
    // a malformed override path could still surface here). A NUL in
    // the path makes `CString::new` fail; treat that as a generic
    // error so the validator surfaces a stable message rather than
    // panicking.
    let cstr = match CString::new(path.as_os_str().as_bytes()) {
        Ok(c) => c,
        Err(_) => return SpoolProbe::OtherError(libc::EINVAL),
    };
    // SAFETY: `cstr.as_ptr()` is a valid NUL-terminated pointer for
    // the duration of the call. `access` does not retain the pointer
    // and has no thread-state side effects.
    let rc = unsafe { libc::access(cstr.as_ptr(), libc::W_OK) };
    if rc == 0 {
        return SpoolProbe::Writable;
    }
    let err = std::io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::ENOENT) => {
            // Distinguish parent-missing from spool-missing by a
            // second probe on the parent. The parent probe uses
            // F_OK (existence) — operator may not have W_OK there
            // yet (e.g. `/var/mail` owned by root with
            // `SupplementaryGroups=mail`), but existence is what
            // we need.
            let parent = path.parent().unwrap_or_else(|| Path::new("/"));
            let pcstr = match CString::new(parent.as_os_str().as_bytes()) {
                Ok(c) => c,
                Err(_) => return SpoolProbe::OtherError(libc::EINVAL),
            };
            // SAFETY: same as above.
            let prc = unsafe { libc::access(pcstr.as_ptr(), libc::F_OK) };
            if prc == 0 {
                SpoolProbe::SpoolMissing
            } else {
                SpoolProbe::ParentMissing
            }
        }
        Some(libc::EACCES) => SpoolProbe::NotWritable,
        Some(errno) => SpoolProbe::OtherError(errno),
        None => SpoolProbe::OtherError(0),
    }
}

/// Construct a `ConfigError::Validate`. Centralizes the 6-field shape so
/// a future schema change to the variant updates one place, and the
/// per-rule validators stay readable.
fn validate_err(
    path: &Path,
    lines: Vec<usize>,
    flow: Option<&str>,
    field: impl Into<String>,
    value: impl Into<String>,
    message: impl Into<String>,
    suggestion: impl Into<String>,
) -> ConfigError {
    // Every validate error displays as `path:lines: ...`. An empty
    // `lines` field would render as `path: ...`, which most editors
    // do not parse as a jump target. Fall back to line 1 (the start
    // of the file) for "missing content" errors that have no
    // specific source span.
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
/// Errors are collected into a `Vec<ConfigError>` so the operator sees
/// every problem in one pass. On success, every duration is bounded,
/// every credential id is validated, every template compiles, and the
/// flow list is non-empty with unique names.
///
/// This pass is purely structural — it does not touch the host
/// filesystem. Spool writability for `local_mail` destinations is a
/// runtime/host-state check; production callers (`cli::check`,
/// daemon startup) follow this pass with `validate_spool_writability`
/// against the resolved Config.
pub(crate) fn validate(
    raw: RawConfig,
    source: &str,
    path: &Path,
) -> Result<Config, Vec<ConfigError>> {
    let mut errors: Vec<ConfigError> = Vec::new();

    // ----- top-level poll defaults -----
    let poll = validate_poll_defaults(&raw.poll, source, path, &mut errors);

    // ----- top-level log -----
    let log = LogConfig {
        filter: raw.log.filter.clone(),
    };

    // ----- top-level http -----
    let http = validate_http(&raw.http, source, path, &mut errors);

    // ----- flows -----
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

    let mut flows: Vec<FlowConfig> = Vec::with_capacity(raw.flow.len());
    // (raw env var name) -> (id string, line, flow name) for collision detection.
    let mut env_var_index: BTreeMap<String, Vec<(String, usize, String)>> = BTreeMap::new();
    // (CredentialId) -> Vec<line>. Populated as `validate_credential_id`
    // accepts each id; surfaced through `Config::credential_lines` so
    // `gcit check` can include source lines in CredentialNotFound
    // errors.
    let mut credential_lines: BTreeMap<CredentialId, Vec<usize>> = BTreeMap::new();
    // (flow name) -> Vec<line> for duplicate-name detection. Names that
    // fail `validate_flow_name` are skipped so the operator does not see
    // a spurious "duplicate empty name" pair-up alongside the per-empty
    // validation errors.
    let mut name_index: BTreeMap<String, Vec<usize>> = BTreeMap::new();

    for raw_flow in &raw.flow {
        let flow_line = span_line(source, &raw_flow.name);
        let flow_name = raw_flow.name.get_ref().clone();

        // Validate flow.name charset + length. Returns true iff the name
        // passed every check; only valid names participate in
        // duplicate-name detection.
        let name_ok = validate_flow_name(&raw_flow.name, source, path, &mut errors);
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

    // Duplicate flow name detection: every name with >1 occurrence
    // surfaces a single ConfigError::Validate carrying every line.
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

    // Credential id env-var collision detection. Two ids that map
    // to the same GCIT_CREDENTIAL_* env var name are rejected,
    // naming both ids and the env-var rule.
    for (env, occurrences) in &env_var_index {
        // Distinct id strings sharing the same env var name.
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

    // Sort + dedup each line list for deterministic output and so the
    // CredentialNotFound display doesn't repeat lines when an id is
    // referenced from the same flow's source + action.
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

/// Probe spool-writability for every `local_mail` destination in
/// `cfg`. Decoupled from `validate` because writability is a
/// host-state check, not a schema/structural check: bundling it
/// into `validate` would force every test fixture that builds a
/// `local_mail` config (including the in-tree `systemd::unit`
/// tests) to maintain a `/var/mail/<user>` fixture on the runner.
///
/// Production callers run `validate` first (structural), then
/// `validate_spool_writability` against the resolved `Config`:
///   * `cli::check` collects the host-state errors alongside its
///     credential probes — gcit check and daemon startup share the
///     same validator.
///   * Daemon startup runs the same probe with soft (warn-only)
///     semantics per `ValidateContext::DaemonStart`.
///
/// `spool_root = None` resolves to `mail::DEFAULT_SPOOL_DIR`
/// (`/var/mail` in production). Tests pass `Some(<tempdir>)` so
/// the `access(2)` probe targets a fixture path the test owns.
///
/// Returns a `Vec<ConfigError>` so the caller can splice it into
/// its own error accumulator and surface every problem in one pass.
///
/// `pub` (not `pub(crate)`) so integration test crates can drive
/// the probe directly with a tempdir override. `#[doc(hidden)]`
/// keeps it out of rustdoc.
#[doc(hidden)]
pub fn validate_spool_writability(cfg: &Config, spool_root: Option<&Path>) -> Vec<ConfigError> {
    let mut errors: Vec<ConfigError> = Vec::new();
    let resolved_spool_root: PathBuf = spool_root
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from(DEFAULT_SPOOL_DIR));
    for flow in &cfg.flow {
        for dest in &flow.destination {
            if let Destination::LocalMail(lm) = dest {
                let spool_path = resolved_spool_root.join(&lm.user);
                match probe_spool_writability(&spool_path) {
                    SpoolProbe::Writable => { /* passes */ }
                    SpoolProbe::ParentMissing => {
                        errors.push(validate_err(
                            cfg.source_path.as_path(),
                            // No source-line span for a host-state
                            // failure — fall back to line 1 via the
                            // empty-vec convention in `validate_err`.
                            vec![],
                            Some(&flow.name),
                            "destination.local_mail.user",
                            lm.user.clone(),
                            format!(
                                "spool parent directory {} does not exist; install a mail package or create it manually",
                                resolved_spool_root.display(),
                            ),
                            format!(
                                "install mailutils or postfix, or `sudo mkdir -m 0755 {}`",
                                resolved_spool_root.display(),
                            ),
                        ));
                    }
                    SpoolProbe::SpoolMissing => {
                        errors.push(validate_err(
                            cfg.source_path.as_path(),
                            vec![],
                            Some(&flow.name),
                            "destination.local_mail.user",
                            lm.user.clone(),
                            format!(
                                "spool file {} does not exist; gcit does not auto-create it",
                                spool_path.display(),
                            ),
                            format!(
                                "create the user via `useradd` or `mailx`, or `sudo touch {p} && sudo chown {u}:mail {p} && sudo chmod 0660 {p}`",
                                p = spool_path.display(),
                                u = lm.user,
                            ),
                        ));
                    }
                    SpoolProbe::NotWritable => {
                        errors.push(validate_err(
                            cfg.source_path.as_path(),
                            vec![],
                            Some(&flow.name),
                            "destination.local_mail.user",
                            lm.user.clone(),
                            format!(
                                "spool file {} is not writable for the current effective uid",
                                spool_path.display(),
                            ),
                            format!(
                                "ensure the daemon is in the mail group and `chmod 0660 {p}`, OR add `ReadWritePaths={d}` to gcit.service",
                                p = spool_path.display(),
                                d = resolved_spool_root.display(),
                            ),
                        ));
                    }
                    SpoolProbe::OtherError(errno) => {
                        errors.push(validate_err(
                            cfg.source_path.as_path(),
                            vec![],
                            Some(&flow.name),
                            "destination.local_mail.user",
                            lm.user.clone(),
                            format!(
                                "spool file {} probe failed with errno {} ({})",
                                spool_path.display(),
                                errno,
                                std::io::Error::from_raw_os_error(errno),
                            ),
                            "investigate the underlying filesystem condition (mount state, EROFS, EIO, etc.)",
                        ));
                    }
                }
            }
        }
    }
    errors
}

fn validate_poll_defaults(
    raw: &RawPollDefaults,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
) -> PollDefaults {
    let default_poll = PollDefaults::default();

    let source_interval = match &raw.source_interval {
        Some(spanned) => {
            parse_interval(spanned, "poll.source_interval", None, source, path, errors)
        }
        None => None,
    };
    let job_interval = match &raw.job_interval {
        Some(spanned) => parse_interval(spanned, "poll.job_interval", None, source, path, errors)
            .unwrap_or(default_poll.job_interval),
        None => default_poll.job_interval,
    };
    let jitter = match &raw.jitter {
        Some(spanned) => parse_jitter(spanned, "poll.jitter", None, source, path, errors)
            .unwrap_or(default_poll.jitter),
        None => default_poll.jitter,
    };

    PollDefaults {
        source_interval,
        job_interval,
        jitter,
    }
}

fn validate_http(
    raw: &RawHttpConfig,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
) -> HttpConfig {
    let defaults = HttpConfig::default();
    let request_timeout = match &raw.request_timeout {
        Some(spanned) => parse_bounded_duration(
            spanned,
            "http.request_timeout",
            None,
            MIN_HTTP_TIMEOUT,
            MAX_HTTP_TIMEOUT,
            source,
            path,
            errors,
        )
        .unwrap_or(defaults.request_timeout),
        None => defaults.request_timeout,
    };
    if let Some(spanned) = &raw.max_concurrent {
        // Deprecated. Field is accepted but ignored — the runtime
        // no longer wires it into a Semaphore. Surface a WARN so
        // an operator using the documented field sees the
        // deprecation rather than wondering why their value has no
        // effect, and remove it from their config on the next
        // edit.
        let line = span_line(source, spanned);
        let value = *spanned.get_ref();
        tracing::warn!(
            target: "gcit::config",
            config = %path.display(),
            line,
            value,
            "http.max_concurrent is deprecated and ignored; remove it from the config",
        );
    }
    HttpConfig { request_timeout }
}

/// Returns true when the name passes every check (so the caller can
/// gate duplicate-detection on a clean name and avoid emitting noisy
/// "duplicate empty-name" errors when several flows omit/blank the
/// field).
fn validate_flow_name(
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
fn validate_flow(
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

    // source.url must parse via url::Url AND use a scheme in the
    // allowlist {http, https, ssh, git, file}. Other schemes (data:,
    // javascript:, exotica) are rejected — git transports do not use
    // them, and accepting them would silently disable poll-strategy
    // detection.
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

            // repo: exactly one '/', neither side empty
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

            // workflow: non-empty, no '/', '\\', '..'
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

            // ref_name: must start with "refs/"
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

            // Reject a user-supplied `gcit_run_id` input. The dispatcher
            // injects this key automatically at dispatch time so the
            // workflow's run-name directive can correlate the run to
            // its dispatch. A user-supplied value would be silently
            // overwritten by the injection and is almost certainly a
            // misunderstanding of the contract.
            if let Some(operator_value) = raw.inputs.get("gcit_run_id") {
                errors.push(validate_err(
                    path,
                    vec![kind_line],
                    Some(flow_name),
                    "action.inputs.gcit_run_id",
                    operator_value.clone(),
                    "action.inputs may not contain `gcit_run_id`; gcit injects this key automatically at dispatch time (the value you supplied would be silently overwritten)",
                    "remove the `gcit_run_id` entry from action.inputs (gcit supplies the value automatically)",
                ));
            }

            ActionConfig::GithubWorkflowDispatch {
                repo: repo_str,
                workflow: workflow_str,
                ref_name: ref_str,
                // unreachable at runtime: validate() returns Err when
                // errors is non-empty, so the missing-credential branch
                // above never reaches the typed Config consumer.
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
                // unreachable at runtime: validate() returns Err when
                // errors is non-empty.
                credential_id: CredentialId::new("placeholder").expect("placeholder is a valid id"),
                inputs: BTreeMap::new(),
            }
        }
    }
}

fn require_field(
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

#[allow(clippy::too_many_arguments)]
fn validate_destinations(
    raw: &[RawDestination],
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
    env_var_index: &mut BTreeMap<String, Vec<(String, usize, String)>>,
    credential_lines: &mut BTreeMap<CredentialId, Vec<usize>>,
    flow_name: &str,
) -> Vec<Destination> {
    let mut out = Vec::with_capacity(raw.len());
    for dest in raw {
        let kind_str = dest.kind.get_ref().as_str();
        let kind_line = span_line(source, &dest.kind);
        match kind_str {
            "discord_webhook" => {
                out.push(Destination::DiscordWebhook(validate_discord(
                    dest,
                    kind_line,
                    source,
                    path,
                    errors,
                    env_var_index,
                    credential_lines,
                    flow_name,
                )));
            }
            "local_mail" => {
                out.push(Destination::LocalMail(validate_local_mail(
                    dest, kind_line, source, path, errors, flow_name,
                )));
            }
            other => {
                errors.push(validate_err(
                    path,
                    vec![kind_line],
                    Some(flow_name),
                    "destination.kind",
                    other,
                    format!(
                        "unknown destination kind {:?}; valid kinds: discord_webhook, local_mail",
                        other
                    ),
                    "set kind = \"discord_webhook\" or \"local_mail\"",
                ));
            }
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn validate_discord(
    raw: &RawDestination,
    kind_line: usize,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
    env_var_index: &mut BTreeMap<String, Vec<(String, usize, String)>>,
    credential_lines: &mut BTreeMap<CredentialId, Vec<usize>>,
    flow_name: &str,
) -> DiscordWebhookConfig {
    // Reject local_mail-only fields used with discord_webhook kind.
    if let Some(user) = &raw.user {
        let line = span_line(source, user);
        errors.push(validate_err(
            path,
            vec![line],
            Some(flow_name),
            "destination.user",
            user.get_ref().clone(),
            "destination.user belongs to local_mail; not valid on discord_webhook",
            "remove `user` or change kind to local_mail",
        ));
    }
    let credential_id = match &raw.credential_id {
        Some(spanned) => validate_credential_id(
            spanned,
            "destination.discord_webhook.credential_id",
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
                "destination.credential_id",
                "",
                "destination.credential_id is required for discord_webhook",
                "add credential_id = \"<id>\" under [[flow.destination]]",
            ));
            None
        }
    };
    let fire_on = match &raw.fire_on {
        Some(events) => collect_fire_on(
            events,
            "destination.discord_webhook.fire_on",
            flow_name,
            source,
            path,
            errors,
        ),
        None => vec![FireEvent::RunComplete],
    };
    let template = validate_discord_template(&raw.template, source, path, errors, flow_name);

    DiscordWebhookConfig {
        // unreachable at runtime: validate() returns Err when
        // errors is non-empty.
        credential_id: credential_id
            .unwrap_or_else(|| CredentialId::new("placeholder").expect("placeholder valid")),
        fire_on,
        template,
    }
}

fn validate_discord_template(
    raw: &RawDestinationTemplateConfig,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
    flow_name: &str,
) -> DiscordTemplateConfig {
    // Reject local_mail-only template fields used inside discord_webhook
    // destinations.
    for (field, value) in [("subject", &raw.subject), ("body", &raw.body)] {
        if let Some(spanned) = value {
            errors.push(validate_err(
                path,
                vec![span_line(source, spanned)],
                Some(flow_name),
                format!("destination.template.{}", field),
                spanned.get_ref().clone(),
                format!(
                    "destination.template.{} belongs to local_mail; not valid on discord_webhook",
                    field
                ),
                format!("remove `{}` or change kind to local_mail", field),
            ));
        }
    }
    let title = compile_template_field(
        &raw.title,
        "destination.template.title",
        source,
        path,
        errors,
        flow_name,
    );
    let description = compile_template_field(
        &raw.description,
        "destination.template.description",
        source,
        path,
        errors,
        flow_name,
    );
    let field_name = compile_template_field(
        &raw.field_name,
        "destination.template.field_name",
        source,
        path,
        errors,
        flow_name,
    );
    let field_value = compile_template_field(
        &raw.field_value,
        "destination.template.field_value",
        source,
        path,
        errors,
        flow_name,
    );
    let collapsed_summary = compile_template_field(
        &raw.collapsed_summary,
        "destination.template.collapsed_summary",
        source,
        path,
        errors,
        flow_name,
    );
    DiscordTemplateConfig {
        title,
        description,
        field_name,
        field_value,
        collapsed_summary,
    }
}

fn validate_local_mail(
    raw: &RawDestination,
    kind_line: usize,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
    flow_name: &str,
) -> LocalMailConfig {
    // Reject discord_webhook-only fields used with local_mail kind.
    if let Some(cid) = &raw.credential_id {
        let line = span_line(source, cid);
        errors.push(validate_err(
            path,
            vec![line],
            Some(flow_name),
            "destination.credential_id",
            cid.get_ref().clone(),
            "destination.credential_id belongs to discord_webhook; not valid on local_mail",
            "remove `credential_id` or change kind to discord_webhook",
        ));
    }
    let (user_str, user_line) = match &raw.user {
        Some(spanned) => (spanned.get_ref().clone(), span_line(source, spanned)),
        None => {
            errors.push(validate_err(
                path,
                vec![kind_line],
                Some(flow_name),
                "destination.user",
                "",
                "destination.user is required for local_mail",
                "add user = \"<unix_user>\" under [[flow.destination]]",
            ));
            (String::new(), kind_line)
        }
    };
    if user_str.is_empty() {
        errors.push(validate_err(
            path,
            vec![user_line],
            Some(flow_name),
            "destination.local_mail.user",
            user_str.clone(),
            "local_mail.user must be non-empty",
            "use a Unix username like 'ops'",
        ));
    } else if user_str.len() > MAX_LOCAL_MAIL_USER_LEN {
        errors.push(validate_err(
            path,
            vec![user_line],
            Some(flow_name),
            "destination.local_mail.user",
            user_str.clone(),
            format!(
                "local_mail.user is {} chars; max is {}",
                user_str.len(),
                MAX_LOCAL_MAIL_USER_LEN
            ),
            format!("shorten to {} chars or fewer", MAX_LOCAL_MAIL_USER_LEN),
        ));
    } else {
        for ch in user_str.chars() {
            if !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '-') {
                errors.push(validate_err(
                    path,
                    vec![user_line],
                    Some(flow_name),
                    "destination.local_mail.user",
                    user_str.clone(),
                    format!(
                        "local_mail.user contains invalid character {:?}; allowed: A-Z a-z 0-9 _ -",
                        ch
                    ),
                    "use only A-Z, a-z, 0-9, '_', and '-'",
                ));
                break;
            }
        }
    }

    let fire_on = match &raw.fire_on {
        Some(events) => collect_fire_on(
            events,
            "destination.local_mail.fire_on",
            flow_name,
            source,
            path,
            errors,
        ),
        None => vec![FireEvent::RunComplete],
    };
    let template = validate_local_mail_template(&raw.template, source, path, errors, flow_name);

    LocalMailConfig {
        user: user_str,
        fire_on,
        template,
    }
}

fn validate_local_mail_template(
    raw: &RawDestinationTemplateConfig,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
    flow_name: &str,
) -> LocalMailTemplateConfig {
    // Reject discord_webhook-only template fields used inside
    // local_mail destinations.
    for (field, value) in [
        ("title", &raw.title),
        ("description", &raw.description),
        ("field_name", &raw.field_name),
        ("field_value", &raw.field_value),
        ("collapsed_summary", &raw.collapsed_summary),
    ] {
        if let Some(spanned) = value {
            errors.push(validate_err(
                path,
                vec![span_line(source, spanned)],
                Some(flow_name),
                format!("destination.template.{}", field),
                spanned.get_ref().clone(),
                format!(
                    "destination.template.{} belongs to discord_webhook; not valid on local_mail",
                    field
                ),
                format!("remove `{}` or change kind to discord_webhook", field),
            ));
        }
    }
    let subject = compile_template_field(
        &raw.subject,
        "destination.template.subject",
        source,
        path,
        errors,
        flow_name,
    );
    let body = compile_template_field(
        &raw.body,
        "destination.template.body",
        source,
        path,
        errors,
        flow_name,
    );
    LocalMailTemplateConfig { subject, body }
}

fn validate_poll_override(
    raw: &RawPollOverride,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
    flow_name: &str,
) -> PollOverride {
    let source_interval = raw.source_interval.as_ref().and_then(|s| {
        parse_interval(
            s,
            "flow.poll.source_interval",
            Some(flow_name),
            source,
            path,
            errors,
        )
    });
    let job_interval = raw.job_interval.as_ref().and_then(|s| {
        parse_interval(
            s,
            "flow.poll.job_interval",
            Some(flow_name),
            source,
            path,
            errors,
        )
    });
    let jitter = raw
        .jitter
        .as_ref()
        .and_then(|s| parse_jitter(s, "flow.poll.jitter", Some(flow_name), source, path, errors));
    PollOverride {
        source_interval,
        job_interval,
        jitter,
    }
}

/// Parse and bound a humantime duration.
///
/// Two error categories: humantime parse failure (bad syntax) -> Parse,
/// and out-of-bounds (under MIN, over MAX) -> Validate. The distinction
/// matters because the operator's fix differs.
fn parse_interval(
    spanned: &Spanned<String>,
    field: &'static str,
    flow: Option<&str>,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
) -> Option<Duration> {
    parse_bounded_duration(
        spanned,
        field,
        flow,
        MIN_INTERVAL,
        MAX_INTERVAL,
        source,
        path,
        errors,
    )
}

/// Parse a humantime duration string and require it to fall within
/// `[min, max]` (inclusive). Out-of-range surfaces as
/// `ConfigError::Validate`; malformed input surfaces as
/// `ConfigError::Parse`. The distinction matters because the operator's
/// fix differs (range = pick a different number; parse = fix the
/// syntax).
///
/// Both branches append a case-confusable hint when the raw input
/// contains uppercase 'M' or 'W' — humantime treats these as months
/// and weeks (uppercase 'W' is *not* a valid unit at all), and the
/// glyphs are visually indistinguishable from lowercase 'm'/'w' in
/// many fonts, so a typo there is a common operator mistake.
#[allow(clippy::too_many_arguments)]
fn parse_bounded_duration(
    spanned: &Spanned<String>,
    field: &'static str,
    flow: Option<&str>,
    min: Duration,
    max: Duration,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
) -> Option<Duration> {
    let raw = spanned.get_ref().clone();
    let line = span_line(source, spanned);
    let hint = case_confusable_hint(&raw);
    match humantime::parse_duration(&raw) {
        Err(e) => {
            errors.push(ConfigError::Parse {
                path: path.to_path_buf(),
                line,
                message: format!("{} = {:?}: invalid duration: {}{}", field, raw, e, hint),
            });
            None
        }
        Ok(d) => {
            if d < min || d > max {
                errors.push(validate_err(
                    path,
                    vec![line],
                    flow,
                    field,
                    raw.clone(),
                    format!(
                        "{} must be between {}s and {}s; got {}s{}",
                        field,
                        min.as_secs(),
                        max.as_secs(),
                        d.as_secs(),
                        hint,
                    ),
                    format!(
                        "use a value in the range [{}s, {}s]",
                        min.as_secs(),
                        max.as_secs()
                    ),
                ));
                None
            } else {
                Some(d)
            }
        }
    }
}

/// Return a hint string ready to append to a duration error when the
/// input contains a case-confusable unit. Empty string when no hint
/// applies, so it can be unconditionally inserted into format strings.
fn case_confusable_hint(raw: &str) -> &'static str {
    if raw.contains('M') {
        " (note: 'M' means months in humantime; use lowercase 'm' for minutes)"
    } else if raw.contains('W') {
        " (note: 'W' is not a valid humantime unit; use lowercase 'w' for weeks)"
    } else {
        ""
    }
}

/// Collect a `fire_on` array, surfacing each duplicate as a
/// `ConfigError::Validate`. Duplicate events in `fire_on` are a
/// config mistake — `gcit check` exists to catch them, not silently
/// hide them. The deduplicated event list is still returned so the
/// validator's later passes operate on a clean vector; the load()
/// result still carries the error so the operator has to fix it.
fn collect_fire_on(
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

fn parse_jitter(
    spanned: &Spanned<f64>,
    field: &'static str,
    flow: Option<&str>,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
) -> Option<f64> {
    let value = *spanned.get_ref();
    let line = span_line(source, spanned);
    if !(MIN_JITTER..=MAX_JITTER).contains(&value) {
        errors.push(validate_err(
            path,
            vec![line],
            flow,
            field,
            format!("{}", value),
            format!(
                "{} must be in the range [{}, {}]; got {}",
                field, MIN_JITTER, MAX_JITTER, value
            ),
            format!(
                "use a value in [{}, {}], for example 0.1",
                MIN_JITTER, MAX_JITTER
            ),
        ));
        None
    } else {
        Some(value)
    }
}

/// Validate a credential id and record its env-var occurrence for
/// later collision detection. Returns `Some` only when the id passes
/// all charset/length/path-traversal checks.
///
/// Invalid ids are NOT recorded in the env-var index. The operator
/// already sees a per-id validation error; reporting the same id again
/// as part of a collision would be noise. Once the operator fixes the
/// invalid id, the validator is re-run and any genuine collision among
/// the resulting valid ids surfaces normally.
#[allow(clippy::too_many_arguments)]
fn validate_credential_id(
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

/// Compile a single template field and verify it renders against a
/// probe context that supplies every documented namespaced variable.
/// Returns `None` when compilation OR the bare-name AST check fails so
/// the typed `Config` never carries a template that would explode at
/// notification time.
///
/// Bare expressions like `{{flow}}` (a top-level namespace) or
/// `{{gcit_run_id}}` (a single name not in `namespace.field` form) are
/// rejected here: the template variable contract requires the dotted
/// form so an accidental shorthand surfaces at config load instead of
/// at runtime as an opaque "Cannot find data" error.
fn compile_template_field(
    template: &Option<Spanned<String>>,
    field: &'static str,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
    flow_name: &str,
) -> Option<String> {
    let spanned = template.as_ref()?;
    let raw = spanned.get_ref().clone();
    let line = span_line(source, spanned);

    let mut hb = crate::notify::strict_handlebars();
    if let Err(e) = hb.register_template_string(field, &raw) {
        errors.push(ConfigError::TemplateCompile {
            path: path.to_path_buf(),
            line,
            flow: flow_name.to_string(),
            field: field.to_string(),
            source: e,
        });
        return None;
    }

    // AST-level check: reject any expression whose path is a single
    // segment matching a top-level namespace OR is not in dotted form.
    // The render below cannot catch these because handlebars renders
    // a sub-object as its serialized form rather than raising.
    if let Some(tpl) = hb.get_template(field) {
        if let Some(bare) = find_bare_name(tpl) {
            errors.push(validate_err(
                path,
                vec![line],
                Some(flow_name),
                field,
                raw.clone(),
                format!(
                    "template references bare name '{{{{{}}}}}'; only namespaced variables ({}) are accepted",
                    bare,
                    namespace_form_examples(),
                ),
                format!(
                    "use a namespaced reference like '{{{{flow.name}}}}', '{{{{source.sha_short}}}}' (got {{{{{}}}}})",
                    bare,
                ),
            ));
            return None;
        }
    }

    // Render against the probe context. Strict-mode catches typos in
    // dotted leaves (e.g. `{{flow.naem}}`) and missing nested fields
    // (e.g. `{{action.runn_id}}`).
    let ctx = probe_context();
    if let Err(e) = hb.render(field, &ctx) {
        errors.push(validate_err(
            path,
            vec![line],
            Some(flow_name),
            field,
            raw.clone(),
            format!("template fails to render against the probe context: {}", e),
            "use only documented variables (flow.name, source.sha, source.sha_short, action.repo, action.workflow, action.run_id, action.run_url, action.dispatched_at, run.status, run.conclusion, gcit.run_id, flow.description)",
        ));
        return None;
    }

    Some(raw)
}

/// Inspect a parsed handlebars `Template` for any expression whose
/// reference is a single-segment name (i.e. not in the dotted
/// `namespace.field` form). Returns the offending raw name when one
/// is found; `None` when every reference is a dotted path.
///
/// `pub` so `cli/validate_template.rs` can apply the same AST check
/// the daemon uses at config load — `gcit validate-template` must
/// agree with `gcit check` on what templates are accepted.
pub fn find_bare_name(tpl: &handlebars::Template) -> Option<String> {
    use handlebars::template::TemplateElement;
    for element in &tpl.elements {
        let helper = match element {
            TemplateElement::Expression(h) => h,
            TemplateElement::HtmlExpression(h) => h,
            TemplateElement::HelperBlock(h) => h,
            _ => continue,
        };
        // Parameter::as_name returns the raw text for Name(_) and
        // Path(_) variants. Other variants (Literal, Subexpression)
        // never reach the helper-name slot for plain expressions
        // produced by parsing user template strings.
        let name = match helper.name.as_name() {
            Some(n) => n.to_string(),
            None => continue,
        };
        // Dotted (e.g. flow.name) or slashed (e.g. flow/name from
        // alternate path syntax) paths are accepted; anything single-
        // segment is rejected. A namespace name on its own (`{{flow}}`)
        // is also single-segment and falls through here.
        if !name.contains('.') && !name.contains('/') {
            return Some(name);
        }
        // Recurse into helper-block bodies so nested bare references
        // are caught even when buried under a block helper (which
        // itself was rejected above, but we continue scanning the body
        // for clarity in error messages).
        if let Some(inner) = &helper.template {
            if let Some(inner_bare) = find_bare_name(inner) {
                return Some(inner_bare);
            }
        }
        if let Some(inner) = &helper.inverse {
            if let Some(inner_bare) = find_bare_name(inner) {
                return Some(inner_bare);
            }
        }
    }
    None
}

fn namespace_form_examples() -> String {
    TEMPLATE_NAMESPACES
        .iter()
        .map(|n| format!("{}.<field>", n))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Probe context supplying type-faithful stub values for every
/// documented namespaced template variable. Used by
/// `compile_template_field` to drive a render pass that exercises
/// strict-mode missing-variable detection. `pub` so
/// `cli/validate_template.rs` can drive the same probe context against
/// a standalone template file — `gcit validate-template <FILE>`
/// shares the daemon's strict-mode validation behaviour.
///
/// The stub values are shaped after the actual runtime values:
///   * Strings carry plausible content (sha = 40 hex chars, sha_short
///     = 12 hex, run_url = full https URL, dispatched_at = RFC3339
///     timestamp, gcit.run_id = UUID-shaped string). Empty strings
///     would render but obscure type mismatches; type-faithful values
///     surface a wider class of template bugs at validation time
///     (e.g. handlebars helper that expects a number applied to a
///     string-rendering URL).
///   * Numeric fields use real numbers (action.run_id is u64 at
///     runtime, so the stub uses an integer JSON value rather than a
///     string).
pub fn probe_context() -> serde_json::Value {
    serde_json::json!({
        "flow": {
            "name": "linux-mainline-ci",
            "description": "Watch torvalds/linux master and dispatch our CI builder.",
        },
        "source": {
            "url": "https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git",
            "ref_name": "refs/heads/master",
            "sha": "a".repeat(40),
            "sha_short": "a".repeat(12),
        },
        "action": {
            "repo": "myorg/linux-builder",
            "workflow": "ci.yml",
            "run_id": 1234567890u64,
            "run_url": "https://github.com/myorg/linux-builder/actions/runs/1234567890",
            "dispatched_at": "2026-04-26T12:34:56Z",
        },
        "run": {
            "status": "completed",
            "conclusion": "success",
        },
        "gcit": {
            "run_id": "00000000-0000-0000-0000-000000000000",
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------
    // byte_offset_to_line
    // -----------------------------------------------------------------

    #[test]
    fn byte_offset_to_line_zero_offset_is_line_one() {
        // Per src/config/validate.rs:93-103: offsets 0..first_newline
        // map to line 1. The function counts `\n` bytes in [0..offset).
        // An offset of 0 has no bytes to count, so it returns 1.
        assert_eq!(byte_offset_to_line("first\nsecond\n", 0), 1);
    }

    #[test]
    fn byte_offset_to_line_offset_just_before_newline_is_same_line() {
        // The newline at index 5 (`first\n` is bytes 0..6) belongs to
        // line 1. Offsets 0..=5 all map to line 1 because the loop
        // iterates `bytes[..offset]` and offset=5 skips the newline at
        // index 5.
        assert_eq!(byte_offset_to_line("first\nsecond\n", 5), 1);
    }

    #[test]
    fn byte_offset_to_line_offset_at_newline_advances_to_next_line() {
        // The newline at index 5 is included when offset=6 (since the
        // slice is `bytes[..6]` which contains the `\n`). Once that
        // newline is counted, line goes from 1 to 2.
        assert_eq!(byte_offset_to_line("first\nsecond\n", 6), 2);
    }

    #[test]
    fn byte_offset_to_line_clamps_offset_beyond_source_length() {
        // src/config/validate.rs:95: `let limit = offset.min(bytes.len());`
        // ensures an out-of-range offset clamps to the last byte rather
        // than panicking. Three lines = two `\n` bytes; an offset past
        // the end still counts both newlines and returns line 3.
        let s = "a\nb\nc";
        assert_eq!(byte_offset_to_line(s, 999), 3);
    }

    #[test]
    fn byte_offset_to_line_empty_source_returns_one_for_any_offset() {
        // Empty input has no newlines and the clamp turns any offset
        // into 0. The loop runs zero iterations; the function returns
        // the initial `line = 1`.
        assert_eq!(byte_offset_to_line("", 0), 1);
        assert_eq!(byte_offset_to_line("", 100), 1);
    }

    #[test]
    fn byte_offset_to_line_multiple_consecutive_newlines_advance_per_newline() {
        // Each `\n` advances the counter by 1, so an offset past three
        // consecutive newlines lands on line 4.
        assert_eq!(byte_offset_to_line("\n\n\n", 3), 4);
    }

    // -----------------------------------------------------------------
    // case_confusable_hint
    // -----------------------------------------------------------------

    #[test]
    fn case_confusable_hint_uppercase_m_returns_months_warning() {
        // src/config/validate.rs:1450-1458 emits a months hint when the
        // input contains uppercase 'M'. Operators frequently confuse
        // 'M' (months) with 'm' (minutes) because the glyphs are
        // visually indistinguishable in many monospace fonts.
        let hint = case_confusable_hint("5M");
        assert!(
            hint.contains("'M' means months"),
            "uppercase M must surface the months hint; got: {hint}",
        );
        assert!(
            hint.contains("lowercase 'm'"),
            "hint must point at the correct lowercase form; got: {hint}",
        );
    }

    #[test]
    fn case_confusable_hint_uppercase_w_returns_weeks_warning() {
        // 'W' is NOT a valid humantime unit at all (only 'w' for weeks),
        // so the hint must surface the lowercase form rather than
        // pretending uppercase has a different meaning.
        let hint = case_confusable_hint("3W");
        assert!(
            hint.contains("'W' is not a valid humantime unit"),
            "uppercase W must surface the not-a-valid-unit hint; got: {hint}",
        );
        assert!(
            hint.contains("lowercase 'w'"),
            "hint must point at lowercase 'w' for weeks; got: {hint}",
        );
    }

    #[test]
    fn case_confusable_hint_lowercase_only_returns_empty() {
        // No confusable uppercase character → no hint. The function
        // returns the empty string so callers can unconditionally
        // append it to format strings (line 1407 of validate.rs).
        assert_eq!(case_confusable_hint("15s"), "");
        assert_eq!(case_confusable_hint("5m"), "");
        assert_eq!(case_confusable_hint("2w"), "");
        assert_eq!(case_confusable_hint(""), "");
    }

    #[test]
    fn case_confusable_hint_uppercase_m_takes_precedence_over_uppercase_w() {
        // The function checks 'M' before 'W' (line 1451 vs 1453). An
        // input containing both surfaces the months hint, not the
        // weeks-not-a-unit one. Pinned so a future swap of the order
        // would surface as a test failure rather than silently
        // changing operator-facing output.
        let hint = case_confusable_hint("5M3W");
        assert!(
            hint.contains("'M' means months"),
            "M must take precedence over W when both appear; got: {hint}",
        );
    }

    // -----------------------------------------------------------------
    // probe_context
    // -----------------------------------------------------------------

    #[test]
    fn probe_context_carries_every_documented_namespace() {
        // src/config/validate.rs:80-86 names the five top-level
        // namespaces (flow, source, action, run, gcit). probe_context
        // must populate every one of them so that compile_template_field
        // can render any documented `namespace.field` reference. A
        // missing namespace would silently break a strict-mode render
        // pass — surfaceable here as a top-level key absence.
        let ctx = probe_context();
        let obj = ctx.as_object().expect("probe context must be a JSON object");
        for ns in TEMPLATE_NAMESPACES {
            assert!(
                obj.contains_key(*ns),
                "probe context must carry namespace '{}'; got keys: {:?}",
                ns,
                obj.keys().collect::<Vec<_>>(),
            );
        }
    }

    #[test]
    fn probe_context_source_sha_is_40_hex_chars() {
        // src/config/validate.rs:1764: source.sha is "a".repeat(40)
        // because runtime SHAs are 40 hex chars. A regression that
        // emitted a short value would slip past strict-mode rendering
        // but would NOT exercise template helpers that assume full sha
        // length.
        let ctx = probe_context();
        let sha = ctx
            .get("source")
            .and_then(|s| s.get("sha"))
            .and_then(|v| v.as_str())
            .expect("probe context source.sha must be a string");
        assert_eq!(sha.len(), 40, "source.sha must be 40 chars; got: {sha}");
    }

    #[test]
    fn probe_context_action_run_id_is_unsigned_integer() {
        // Per src/config/validate.rs:1770 + the comment at 1752-1754,
        // action.run_id is u64 at runtime. The probe value must be a
        // JSON integer so handlebars helpers expecting a number still
        // see one (a string-typed probe value would surface a
        // false-positive helper failure at render time).
        let ctx = probe_context();
        let run_id = ctx
            .get("action")
            .and_then(|a| a.get("run_id"))
            .expect("probe context action.run_id must exist");
        assert!(
            run_id.is_u64(),
            "action.run_id must be a u64; got: {run_id}",
        );
    }

    // -----------------------------------------------------------------
    // find_bare_name
    // -----------------------------------------------------------------

    fn compile_template(s: &str) -> handlebars::Template {
        handlebars::Template::compile(s).expect("template must compile")
    }

    #[test]
    fn find_bare_name_returns_none_for_dotted_path() {
        // Dotted paths (containing '.') pass — find_bare_name returns
        // None. The `flow.name` form is the canonical accepted shape.
        let tpl = compile_template("hello {{flow.name}}");
        assert_eq!(find_bare_name(&tpl), None);
    }

    #[test]
    fn find_bare_name_returns_none_for_slashed_path() {
        // Line 1707 also accepts paths containing '/' because handlebars
        // supports slashed path syntax as an alias for dotted (rare but
        // handled). A slashed `flow/name` is treated as multi-segment
        // and passes the bare-name check.
        let tpl = compile_template("{{flow/name}}");
        assert_eq!(find_bare_name(&tpl), None);
    }

    #[test]
    fn find_bare_name_returns_some_for_single_segment_namespace() {
        // `{{flow}}` is a single-segment reference matching one of the
        // top-level namespaces. find_bare_name returns Some("flow") so
        // compile_template_field can surface a Validate error pointing
        // at the bare reference.
        let tpl = compile_template("hello {{flow}}");
        assert_eq!(find_bare_name(&tpl), Some("flow".to_string()));
    }

    #[test]
    fn find_bare_name_returns_some_for_single_segment_unknown_name() {
        // `{{gcit_run_id}}` (vs the correct `{{gcit.run_id}}`) is a
        // single-segment reference that does NOT match any namespace.
        // It still must be rejected (line 1707 only checks for '.'/
        // '/' presence — single-segment names of any kind fall through
        // to the rejection arm).
        let tpl = compile_template("{{gcit_run_id}}");
        assert_eq!(find_bare_name(&tpl), Some("gcit_run_id".to_string()));
    }

    #[test]
    fn find_bare_name_returns_none_for_plain_string_with_no_expressions() {
        // A template carrying only literal text has no expressions, so
        // the elements iterator never enters the helper-bearing arm.
        // find_bare_name returns None.
        let tpl = compile_template("plain text with no template variables");
        assert_eq!(find_bare_name(&tpl), None);
    }

    #[test]
    fn find_bare_name_returns_first_match_when_template_has_multiple_bare_references() {
        // The function scans elements in source order and returns
        // Some(name) on the first hit (line 1708 `return Some(name)`).
        // Pin first-match semantics so a regression that returns the
        // last match (or all matches) would fail.
        let tpl = compile_template("{{flow}} and {{source}}");
        // flow is first in order; the function must surface it before
        // even considering source.
        assert_eq!(find_bare_name(&tpl), Some("flow".to_string()));
    }

    #[test]
    fn find_bare_name_block_helper_with_single_segment_name_returns_helper_name() {
        // src/config/validate.rs:1707-1708 — the single-segment guard
        // fires for the OUTER helper name first, so a `{{#if ...}}`
        // block surfaces "if" itself before any recursion into the
        // body. Pin the first-match-wins semantic: even though the
        // body contains `{{flow}}` (also bare), the outer "if" wins.
        // This matches the function's contract: any single-segment
        // reference, helper or otherwise, is rejected at the first
        // hit.
        let tpl = compile_template("{{#if action.repo}}{{flow}}{{/if}}");
        // "if" is a single-segment helper name; find_bare_name returns
        // it before walking the body.
        assert_eq!(find_bare_name(&tpl), Some("if".to_string()));
    }

    // -----------------------------------------------------------------
    // namespace_form_examples
    // -----------------------------------------------------------------

    #[test]
    fn namespace_form_examples_lists_every_documented_namespace() {
        // src/config/validate.rs:1728-1734 joins every namespace with
        // the ".<field>" suffix. The function is the operator-facing
        // suggestion in compile_template_field's bare-name error so
        // every documented namespace must appear; pin so a refactor
        // that drops a namespace silently surfaces here.
        let s = namespace_form_examples();
        for ns in TEMPLATE_NAMESPACES {
            let needle = format!("{}.<field>", ns);
            assert!(
                s.contains(&needle),
                "namespace_form_examples must mention {needle:?}; got: {s}",
            );
        }
        // Comma-delimited per the join(", ") at line 1733.
        assert!(s.contains(", "), "must use \", \" separator; got: {s}");
    }

    // -----------------------------------------------------------------
    // validate_err empty-lines fallback
    // -----------------------------------------------------------------

    #[test]
    fn validate_err_empty_lines_vec_falls_back_to_line_one() {
        // src/config/validate.rs:210 — empty `lines` vec produces
        // `vec![1]`. Pin the editor-jump-target invariant: a
        // ConfigError::Validate with an empty lines vec would render
        // as `path: ...` which most editors do not parse as a jump
        // target. The validator uses empty Vec for "missing content"
        // errors that have no specific source span.
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
                assert_eq!(
                    lines,
                    vec![1],
                    "empty input lines must be normalized to [1]",
                );
            }
            _ => panic!("validate_err must produce Validate variant"),
        }
    }

    #[test]
    fn validate_err_non_empty_lines_vec_passes_through() {
        // Symmetric guard against a regression that always overwrites
        // the lines vec with [1] regardless of input.
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

    // -----------------------------------------------------------------
    // probe_context: every namespace's leaf shape
    // -----------------------------------------------------------------

    #[test]
    fn probe_context_run_status_and_conclusion_are_strings() {
        // src/config/validate.rs:1774-1777 — `run.status` and
        // `run.conclusion` are JSON strings ("completed" / "success").
        // Templates that apply a string-only operation (truncate,
        // case-fold) must see a string at probe time so a runtime
        // type mismatch surfaces at config load.
        let ctx = probe_context();
        let status = ctx.get("run").and_then(|r| r.get("status")).unwrap();
        assert!(status.is_string(), "run.status must be a string");
        let conclusion = ctx.get("run").and_then(|r| r.get("conclusion")).unwrap();
        assert!(conclusion.is_string(), "run.conclusion must be a string");
    }

    #[test]
    fn probe_context_action_dispatched_at_is_rfc3339_string() {
        // src/config/validate.rs:1772 — `action.dispatched_at` is a
        // JSON string in RFC3339 form. Templates that use it inside a
        // helper expecting a date-shaped string must see one at probe
        // time. The doc comment at validate.rs:1746-1751 commits to
        // type-faithful values; pin the RFC3339 'T' separator.
        let ctx = probe_context();
        let v = ctx
            .get("action")
            .and_then(|a| a.get("dispatched_at"))
            .and_then(|v| v.as_str())
            .expect("action.dispatched_at must exist as a string");
        assert!(
            v.contains('T') && v.ends_with('Z'),
            "action.dispatched_at must be RFC3339 with 'T' and 'Z'; got: {v}",
        );
    }

    #[test]
    fn probe_context_gcit_run_id_is_uuid_shaped() {
        // src/config/validate.rs:1779 — `gcit.run_id` is the nil UUID
        // literal "00000000-0000-0000-0000-000000000000". Pin the
        // 36-char + four-hyphen shape so a regression to a non-UUID
        // string would fail to drive the runtime check.
        let ctx = probe_context();
        let v = ctx
            .get("gcit")
            .and_then(|g| g.get("run_id"))
            .and_then(|v| v.as_str())
            .expect("gcit.run_id must exist as a string");
        assert_eq!(v.len(), 36, "uuid string must be 36 chars; got: {v}");
        assert_eq!(v.matches('-').count(), 4, "uuid must have 4 hyphens; got: {v}");
    }
}

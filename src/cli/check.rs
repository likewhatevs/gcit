// `gcit check` — validate config + report every error, then check that
// every credential id can be resolved (or surface an INFO note when
// the daemon's systemd-supplied environment is what will resolve it).
//
// State-3 detection: "is $CREDENTIALS_DIRECTORY env var set AND points
// at a real directory". If yes, credentials may be runtime-resolvable
// — exit 0 with INFO note. No unit-file parsing.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use crate::cli::exit;
use crate::config::credential_file::{self, InvariantError, Probe};
use crate::config::validate::validate_spool_writability;
use crate::config::{self, ConfigError, CredentialId};

/// Run `gcit check`. Reads the config at `config_path`, validates it,
/// and prints every problem (or every credential lookup result) to
/// stderr / stdout. Returns the corresponding exit code.
pub fn run(config_path: &Path) -> ExitCode {
    let cfg = match config::load(config_path) {
        Ok(c) => c,
        Err(errors) => {
            for e in &errors {
                eprintln!("{}", e);
            }
            return ExitCode::from(exit::CONFIG);
        }
    };

    // Collect every credential id referenced by the config along with
    // the flow that consumes it. The map drives both the lookup pass
    // and any CredentialNotFound error we surface for unresolved ids.
    let consumers_map = collect_consumers(&cfg);

    // Canonicalize the config path before deriving the credentials/
    // search root. Relative invocations like `gcit --config gcit.toml
    // check` produce a config_path whose `.parent()` is `Some("")` —
    // useless as a directory base.
    let canon_config = config_path
        .canonicalize()
        .unwrap_or_else(|_| config_path.to_path_buf());
    let config_dir = canon_config.parent().map(|p| p.to_path_buf());

    // $CREDENTIALS_DIRECTORY only counts as state-3 evidence when the
    // env var is set AND the path resolves to a real directory. A
    // stale or wrong value is worse than no value: it pushes the user
    // toward the silent-success state and hides a misconfiguration.
    let credentials_dir = match std::env::var_os("CREDENTIALS_DIRECTORY").map(PathBuf::from) {
        Some(p) if p.is_dir() => Some(p),
        _ => None,
    };

    let mut not_found: Vec<ConfigError> = Vec::new();
    let mut info_notes: Vec<String> = Vec::new();

    // Host-state probe: every local_mail destination's spool must be
    // writable for the daemon's effective uid. Runs after structural
    // validation succeeds; failures collect into the same `not_found`
    // bucket so a config with both unresolved credentials and a
    // missing spool surfaces both at once. Production passes None
    // for the spool-root override so the probe targets the global
    // default (`/var/mail`).
    not_found.extend(validate_spool_writability(&cfg, None));

    for (id, owners) in &consumers_map {
        let mut resolved = false;
        let mut resolution_error: Option<ConfigError> = None;

        // Resolution path 1: $CREDENTIALS_DIRECTORY/<id>.
        if let Some(dir) = &credentials_dir {
            let p = dir.join(id.as_str());
            match credential_file::probe(&p) {
                Probe::Ok => {
                    resolved = true;
                }
                Probe::NotPresent => {
                    // Fall through to step 2.
                }
                Probe::Invariant(err) => {
                    resolution_error = Some(invariant_to_config_error(&p, id, owners, &err));
                }
                Probe::StatError(err) => {
                    // Any stat failure (EACCES, EIO, etc.) is a hard
                    // error: the pre-flight's purpose is to verify the
                    // daemon will succeed, and an unverifiable file
                    // could mask a real misconfiguration (e.g.
                    // mode-0644 file under a 0700 parent — operator
                    // sees EACCES, daemon traverses and rejects).
                    resolution_error = Some(stat_error_to_config_error(&p, id, owners, &err));
                }
            }
        }

        // Resolution path 2: GCIT_CREDENTIAL_<UPPER_SNAKE_ID>.
        if !resolved && resolution_error.is_none() && std::env::var_os(id.to_env_var()).is_some() {
            resolved = true;
        }

        // Resolution path 3: <config_dir>/credentials/<id> (chmod 0600).
        if !resolved && resolution_error.is_none() {
            if let Some(dir) = &config_dir {
                let p = dir.join("credentials").join(id.as_str());
                match credential_file::probe(&p) {
                    Probe::Ok => {
                        resolved = true;
                    }
                    Probe::NotPresent => {
                        // Fall through.
                    }
                    Probe::Invariant(err) => {
                        resolution_error = Some(invariant_to_config_error(&p, id, owners, &err));
                    }
                    Probe::StatError(err) => {
                        // See step 1: EACCES is a hard error so the
                        // pre-flight cannot certify a path it cannot
                        // examine.
                        resolution_error = Some(stat_error_to_config_error(&p, id, owners, &err));
                    }
                }
            }
        }

        if let Some(e) = resolution_error {
            not_found.push(e);
            continue;
        }

        if resolved {
            continue;
        }

        // Build the full searched-paths list for the error / note even
        // when only a subset of paths were actually checked. The
        // operator needs to know every place gcit looks so they can
        // place the credential where they prefer.
        let searched = build_searched_paths(id, credentials_dir.as_deref(), config_dir.as_deref());

        // Reach here: not resolvable in the current shell. If
        // $CREDENTIALS_DIRECTORY is set AND a real directory the
        // daemon is running under a systemd unit — gcit check trusts
        // the unit and exits 0 with an INFO note. Otherwise, fail with
        // EX_CONFIG.
        if credentials_dir.is_some() {
            info_notes.push(format!(
                "credential '{}' not present in $CREDENTIALS_DIRECTORY at check time; the systemd unit's LoadCredential= line will provide it at daemon start (consumed by: {})",
                id,
                owners.join(", ")
            ));
        } else {
            // Threading source-line info through the typed Config gives
            // operators editor-friendly jump targets in the error
            // message (e.g. "config.toml:42: credential 'x' not
            // found"). The validator records every reference line; if
            // the id is referenced from N places, the error names them
            // all so a search-and-replace plan is obvious.
            let lines = cfg.credential_lines.get(id).cloned().unwrap_or_default();
            not_found.push(ConfigError::CredentialNotFound {
                path: config_path.to_path_buf(),
                lines,
                id: id.as_str().to_string(),
                searched,
                consumers: owners.clone(),
            });
        }
    }

    // Print info notes regardless of resolution outcome. The previous
    // version dropped them when not_found was non-empty, hiding context
    // the operator might need to understand the failure.
    for note in &info_notes {
        println!("INFO: {}", note);
    }

    if !not_found.is_empty() {
        for e in &not_found {
            eprintln!("{}", e);
        }
        return ExitCode::from(exit::CONFIG);
    }

    ExitCode::from(exit::OK)
}

/// Wrap an `InvariantError` from the shared probe into a
/// `ConfigError::CredentialInvariant`. The variant's `reason` field
/// carries the operator-facing render of the invariant (mode/owner/
/// symlink/non-regular-file) so the error message names the failure
/// reason and the suggested chmod/chown command; `consumers` carries
/// flow names only — no longer overloaded with the failure description.
fn invariant_to_config_error(
    path: &Path,
    id: &CredentialId,
    owners: &[String],
    err: &InvariantError,
) -> ConfigError {
    ConfigError::CredentialInvariant {
        path: path.to_path_buf(),
        reason: err.render(id, path),
        consumers: owners.to_vec(),
    }
}

/// Wrap an `io::Error` from `Probe::StatError` into a
/// `ConfigError::CredentialInvariant`. Any stat failure — EACCES, EIO,
/// ELOOP on intermediate components, etc. — is a hard error: the
/// pre-flight's value is "verify the daemon will succeed", and a
/// path the operator's shell cannot examine could mask a real
/// misconfiguration the daemon would later reject. `reason` carries
/// the rendered stat-error message (which already names the parent-
/// dir hint and `sudo gcit check` recovery action under
/// `Context::PreFlight`).
fn stat_error_to_config_error(
    path: &Path,
    id: &CredentialId,
    owners: &[String],
    err: &std::io::Error,
) -> ConfigError {
    ConfigError::CredentialInvariant {
        path: path.to_path_buf(),
        reason: credential_file::render_stat_error(
            err,
            id,
            path,
            credential_file::Context::PreFlight,
        ),
        consumers: owners.to_vec(),
    }
}

fn build_searched_paths(
    id: &CredentialId,
    credentials_dir: Option<&Path>,
    config_dir: Option<&Path>,
) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::with_capacity(3);
    if let Some(dir) = credentials_dir {
        out.push(dir.join(id.as_str()));
    }
    out.push(PathBuf::from(format!("$env::{}", id.to_env_var())));
    if let Some(dir) = config_dir {
        out.push(dir.join("credentials").join(id.as_str()));
    }
    out
}

fn collect_consumers(cfg: &config::Config) -> BTreeMap<CredentialId, Vec<String>> {
    let mut out: BTreeMap<CredentialId, Vec<String>> = BTreeMap::new();
    for r in config::walk_credentials(cfg) {
        out.entry(r.id).or_default().push(r.flow);
    }
    // Dedup consumer lists per id while preserving first-occurrence
    // order. walk_credentials emits one entry per reference site, so a
    // flow that references the same id from source AND action shows
    // up twice in the raw list before this dedup pass.
    for v in out.values_mut() {
        let mut seen = std::collections::BTreeSet::new();
        v.retain(|name| seen.insert(name.clone()));
    }
    out
}

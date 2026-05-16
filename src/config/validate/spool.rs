// Spool-writability probe for `local_mail` destinations. This is a
// host-state check (separate from the structural `validate` pass)
// because writability depends on filesystem state, mount options,
// effective uid + supplementary groups, and POSIX ACLs.

use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use super::super::error::ConfigError;
use super::super::parse::{Config, Destination};
use super::validate_err;
use crate::mail::DEFAULT_SPOOL_DIR;

/// Outcome of probing `<spool_root>/<user>` via `access(2)`. Maps the
/// operator-relevant errno cases to distinct error messages so each
/// carries remediation tailored to the failure mode.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SpoolProbe {
    Writable,
    ParentMissing,
    SpoolMissing,
    NotWritable,
    OtherError(i32),
}

/// Probe whether `path` is writable via `access(2)` with `W_OK`.
/// `access` lets the kernel apply the full effective-uid, gid,
/// supplementary-groups, ACL, and capability check; `metadata` would
/// lie under POSIX ACLs or `CAP_DAC_OVERRIDE`.
fn probe_spool_writability(path: &Path) -> SpoolProbe {
    // `libc::access` takes a NUL-terminated C string; a NUL byte in
    // the path fails `CString::new`. Surface as a generic error so
    // the validator stays stable rather than panicking.
    let cstr = match CString::new(path.as_os_str().as_bytes()) {
        Ok(c) => c,
        Err(_) => return SpoolProbe::OtherError(libc::EINVAL),
    };
    // SAFETY: `cstr.as_ptr()` is a valid NUL-terminated pointer for
    // the duration of the call; access does not retain the pointer.
    let rc = unsafe { libc::access(cstr.as_ptr(), libc::W_OK) };
    if rc == 0 {
        return SpoolProbe::Writable;
    }
    let err = std::io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::ENOENT) => {
            // Distinguish parent-missing from spool-missing with a
            // second probe on the parent. Use F_OK (existence) — we
            // don't need W_OK on the parent.
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

/// Probe spool writability for every `local_mail` destination in
/// `cfg`. Returns one `ConfigError` per failing destination.
///
/// Decoupled from `validate` because writability is a host-state
/// check, not structural: bundling it in would force every test
/// fixture with a `local_mail` config to maintain a
/// `/var/mail/<user>` file. Production callers run `validate` first
/// (structural), then this. `cli::check` collects host-state errors
/// alongside its credential probes — operators run `gcit check`
/// before bringing up the daemon.
///
/// `spool_root = None` resolves to `mail::DEFAULT_SPOOL_DIR`
/// (`/var/mail`). Tests pass `Some(<tempdir>)` so the `access` probe
/// targets a fixture path they own.
///
/// `pub` (not `pub(crate)`) so integration test crates can drive the
/// probe directly with a tempdir override.
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
                if let Some(err) = probe_to_error(
                    &probe_spool_writability(&spool_path),
                    cfg,
                    flow,
                    lm,
                    &spool_path,
                    &resolved_spool_root,
                ) {
                    errors.push(err);
                }
            }
        }
    }
    errors
}

/// Map a `SpoolProbe` outcome to an operator-facing `ConfigError`.
/// `Writable` returns `None` (no error to surface).
fn probe_to_error(
    probe: &SpoolProbe,
    cfg: &Config,
    flow: &super::super::parse::FlowConfig,
    lm: &super::super::parse::LocalMailConfig,
    spool_path: &Path,
    resolved_spool_root: &Path,
) -> Option<ConfigError> {
    let path = cfg.source_path.as_path();
    let flow_name = Some(flow.name.as_str());
    let field = "destination.local_mail.user";
    match probe {
        SpoolProbe::Writable => None,
        SpoolProbe::ParentMissing => Some(validate_err(
            path,
            vec![],
            flow_name,
            field,
            lm.user.clone(),
            format!(
                "spool parent directory {} does not exist; install a mail package or create it manually",
                resolved_spool_root.display(),
            ),
            format!(
                "install mailutils or postfix, or `sudo mkdir -m 0755 {}`",
                resolved_spool_root.display(),
            ),
        )),
        SpoolProbe::SpoolMissing => Some(validate_err(
            path,
            vec![],
            flow_name,
            field,
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
        )),
        SpoolProbe::NotWritable => Some(validate_err(
            path,
            vec![],
            flow_name,
            field,
            lm.user.clone(),
            format!(
                "spool file {} is not writable for the current effective uid",
                spool_path.display(),
            ),
            format!(
                "ensure the daemon is in the mail group and `chmod 0660 {p}`, OR add `BindPaths={d}` to gcit.service",
                p = spool_path.display(),
                d = resolved_spool_root.display(),
            ),
        )),
        SpoolProbe::OtherError(errno) => Some(validate_err(
            path,
            vec![],
            flow_name,
            field,
            lm.user.clone(),
            format!(
                "spool file {} probe failed with errno {} ({})",
                spool_path.display(),
                errno,
                std::io::Error::from_raw_os_error(*errno),
            ),
            "investigate the underlying filesystem condition (mount state, EROFS, EIO, etc.)",
        )),
    }
}

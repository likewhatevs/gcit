// Shared credential-file invariant probe.
//
// Three surfaces look up credential files on disk and enforce the same
// on-disk rules: regular file (not symlink, not fifo/socket/dir/etc.),
// mode bitmask `mode & 0o077 == 0` (no group/other access bits — so
// 0o400 / 0o500 / 0o600 / 0o700 all qualify), and owner uid matching
// the resolving process's effective uid OR root (uid 0).
//   1. `gcit check` — pre-flight resolution chain ($CREDENTIALS_DIRECTORY,
//      env var, <config_dir>/credentials).
//   2. The daemon supervisor — boot-time and reload-time secret
//      resolution via `CredentialPool::resolve_secret`.
//   3. `gcit install` — walkthrough output decides whether to print the
//      "this credential is already configured" line.
//
// Centralizing the probe here keeps the three callers from drifting on
// invariant rules, error wording, ENOENT-vs-EACCES semantics, or the
// symlink/fifo/regular-file gate. The probe itself is synchronous; the
// supervisor's read-the-bytes layer (tokio::task::spawn_blocking with a
// 5-second timeout) sits on top of `Probe::Ok` and is not the shared
// module's concern. All three callers treat `Probe::StatError` (EACCES,
// EIO, ELOOP on intermediate components, etc.) as a hard error — the
// pre-flight cannot certify a path it cannot examine, and the daemon
// fails on its own resolution path when the same condition occurs at
// boot.

use std::io;
use std::path::Path;

use super::credential::CredentialId;

/// Outcome of probing a candidate credential file path.
///
/// Drives the resolution-chain control flow:
///   - `Ok`: caller may now read the file and use the contents as the
///     resolved credential.
///   - `NotPresent`: the file does not exist (ENOENT). Caller proceeds
///     to the next resolution step (env var, fallback path).
///   - `Invariant`: the file exists but fails an invariant. Caller
///     surfaces the error and stops walking the resolution chain — a
///     misconfigured credential must not be silently overridden by a
///     later step (an operator chmoded 0644 because they meant to use
///     this file; falling through hides the misconfiguration behind a
///     "not found" surfaced from a downstream step).
///   - `StatError`: existence could not be determined. The file may
///     exist but a permission / IO error blocked the stat call (EACCES
///     on the parent dir is the common case). Caller surfaces the
///     system error so the operator sees "Permission denied" rather
///     than a misleading "not found".
#[derive(Debug)]
#[must_use]
pub enum Probe {
    Ok,
    NotPresent,
    Invariant(InvariantError),
    StatError(io::Error),
}

/// Reason a credential file failed an on-disk invariant check.
///
/// Variants carry the data needed to render the operator-facing
/// message (mode for a permissions error, both uids for an owner
/// error) so the rendering layer never re-derives them from the
/// path.
#[derive(Debug)]
pub enum InvariantError {
    /// Path exists as a symlink. Refused so a 0600 symlink cannot
    /// pivot the resolution to a world-readable target.
    Symlink,
    /// Path exists but is not a regular file (fifo, socket, block/char
    /// device, directory). Refused explicitly — `is_file()` would
    /// return false and silently skip such paths in the cli/check
    /// chain, but the supervisor would error at runtime, so the two
    /// surfaces would diverge. Erroring here keeps every caller in
    /// sync.
    NonRegularFile,
    /// File mode contains bits outside the 0600 set (any group or
    /// other read/write/execute bit). Carries the observed mode for
    /// the error message.
    PermissiveMode { mode: u32 },
    /// Owner uid is neither the resolving process's effective uid nor
    /// root (uid 0). Carries both uids so the error message can name
    /// the chown command.
    UnexpectedOwner { owner_uid: u32, euid: u32 },
}

/// Probe a candidate credential file path. Synchronous; uses
/// `symlink_metadata` so a symlink dressed up as 0600 cannot pivot the
/// resolution.
///
/// `NotPresent` is returned only on a definitive ENOENT. Any other
/// stat failure (EACCES, EIO, ELOOP on intermediate path components,
/// etc.) returns `StatError` so the caller does not falsely report
/// "not found" when the file may exist but is unreadable.
pub fn probe(path: &Path) -> Probe {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let meta = match std::fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Probe::NotPresent;
        }
        Err(e) => {
            return Probe::StatError(e);
        }
    };
    let ft = meta.file_type();
    if ft.is_symlink() {
        return Probe::Invariant(InvariantError::Symlink);
    }
    if !ft.is_file() {
        return Probe::Invariant(InvariantError::NonRegularFile);
    }
    let mode = meta.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Probe::Invariant(InvariantError::PermissiveMode { mode });
    }
    // geteuid(2) has no error return per POSIX.
    let euid = unsafe { libc::geteuid() };
    let owner_uid = meta.uid();
    if !owner_uid_accepted(owner_uid, euid) {
        return Probe::Invariant(InvariantError::UnexpectedOwner { owner_uid, euid });
    }
    Probe::Ok
}

/// Pure owner-uid acceptance predicate. Returns true when a credential
/// file owned by `owner_uid` is acceptable for a process running as
/// effective uid `euid`. Two branches:
///   - `owner_uid == euid`: same-uid case (the operator-installed
///     credential under their own home / `gcit run --user`).
///   - `owner_uid == 0`: root-owned case (the system-installed
///     credential under /etc/gcit/credentials, resolved by a daemon
///     running as DynamicUser=yes — euid is a non-zero ephemeral uid).
///
/// Extracted from `probe`'s body so the asymmetric `owner_uid == 0 &&
/// euid != 0` branch is unit-testable without a real chown — the
/// production DynamicUser scenario cannot be reproduced from a
/// privileged test (chowning the file to 0 doesn't change the test
/// process's euid). The predicate has no side effects and no I/O.
fn owner_uid_accepted(owner_uid: u32, euid: u32) -> bool {
    owner_uid == euid || owner_uid == 0
}

impl InvariantError {
    /// Render this invariant failure as a single-line operator-facing
    /// message. The message names the credential id, the path, the
    /// failure reason, and a concrete fix command where applicable. No
    /// consumer / flow info — that is caller-specific (cli/check
    /// appends a `(consumed by: ...)` suffix; the supervisor and
    /// install do not).
    pub fn render(&self, id: &CredentialId, path: &Path) -> String {
        let display = path.display();
        match self {
            InvariantError::Symlink => format!(
                "credential `{id}` at {display} is a symlink; credential files must be regular files (place the credential file directly at this path)",
            ),
            InvariantError::NonRegularFile => format!(
                "credential `{id}` at {display} is not a regular file (remove any existing inode at this path and place a regular file)",
            ),
            InvariantError::PermissiveMode { mode } => format!(
                "credential `{id}` at {display} has mode 0{mode:o}; must be 0600 (no group/other access); sudo chmod 0600 {display}",
            ),
            InvariantError::UnexpectedOwner { owner_uid, euid } => format!(
                "credential `{id}` at {display} is owned by uid {owner_uid} but the resolving process runs as uid {euid}; only the resolver's uid or root (uid 0) is accepted; sudo chown {euid} {display} (or sudo chown root {display})",
            ),
        }
    }
}

/// Caller context for `render_stat_error`. The pre-flight (`gcit
/// check`) and the daemon hit the same probe failure but the
/// actionable hint differs: an operator running `gcit check` from a
/// shell can re-invoke under sudo, while the daemon already has a
/// fixed effective uid (DynamicUser=yes / User=gcit) and the
/// operator's only lever is the parent-dir mode/owner.
#[derive(Debug, Clone, Copy)]
pub enum Context {
    /// Operator running `gcit check` from a shell. Suggest re-running
    /// as the credential owner or via `sudo gcit check`.
    PreFlight,
    /// Daemon-side resolution at boot or reload. The hint should not
    /// suggest `sudo gcit check` — the daemon's effective uid is
    /// fixed (DynamicUser=yes / User=gcit) so the operator's lever is
    /// the parent-dir mode/owner, not the resolver's identity.
    Daemon,
}

/// Render an `io::Error` from `Probe::StatError` as a single-line
/// operator-facing message. Free function (not a method) because
/// `io::Error` is an external type. Adds the credential id, the path,
/// a hint about the most-common cause (an unreadable parent
/// directory), and the recovery action — context-specific because the
/// pre-flight and daemon paths have different operator levers (see
/// `Context` doc).
pub fn render_stat_error(err: &io::Error, id: &CredentialId, path: &Path, ctx: Context) -> String {
    let display = path.display();
    let hint = match ctx {
        Context::PreFlight => {
            "the parent directory must be readable by the resolving process; \
             re-run as the credential owner or via `sudo gcit check` to validate"
        }
        Context::Daemon => "check parent directory permissions for the daemon's effective uid",
    };
    format!("credential `{id}` at {display} could not be checked: {err} ({hint})")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn id() -> CredentialId {
        CredentialId::new("test_cred").expect("valid id")
    }

    fn write_mode(dir: &Path, name: &str, mode: u32) -> PathBuf {
        let p = dir.join(name);
        fs::write(&p, "secret").unwrap();
        fs::set_permissions(&p, fs::Permissions::from_mode(mode)).unwrap();
        p
    }

    /// Drop guard that restores a directory's mode on scope exit. Used
    /// by tests that drop search permission on a parent dir to provoke
    /// EACCES on inner stat calls; without the guard, an early panic
    /// (or future code change) would leave the TempDir uncleanable.
    struct ModeGuard<'a> {
        path: &'a Path,
        restore: u32,
    }

    impl Drop for ModeGuard<'_> {
        fn drop(&mut self) {
            let _ = fs::set_permissions(self.path, fs::Permissions::from_mode(self.restore));
        }
    }

    #[test]
    fn missing_path_returns_not_present() {
        let d = TempDir::new().unwrap();
        let p = d.path().join("nope");
        match probe(&p) {
            Probe::NotPresent => {}
            _ => panic!("expected NotPresent for ENOENT"),
        }
    }

    #[test]
    fn valid_0600_regular_file_returns_ok() {
        let d = TempDir::new().unwrap();
        let p = write_mode(d.path(), "ok", 0o600);
        match probe(&p) {
            Probe::Ok => {}
            _ => panic!("expected Ok for a 0600 regular file"),
        }
    }

    #[test]
    fn valid_0400_regular_file_returns_ok() {
        // mode & 0o077 == 0 — no group/other bits — so 0400 is also
        // accepted. This is intentional: the rule guards against
        // group/other access, not against the owner having read-only
        // access.
        let d = TempDir::new().unwrap();
        let p = write_mode(d.path(), "readonly", 0o400);
        match probe(&p) {
            Probe::Ok => {}
            _ => panic!("expected Ok for a 0400 regular file"),
        }
    }

    #[test]
    fn mode_0644_returns_permissive_mode() {
        let d = TempDir::new().unwrap();
        let p = write_mode(d.path(), "loose", 0o644);
        match probe(&p) {
            Probe::Invariant(InvariantError::PermissiveMode { mode }) => {
                assert_eq!(mode, 0o644);
            }
            _ => panic!("expected PermissiveMode for 0644"),
        }
    }

    #[test]
    fn symlink_returns_symlink() {
        let d = TempDir::new().unwrap();
        let target = write_mode(d.path(), "target", 0o600);
        let link = d.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        match probe(&link) {
            Probe::Invariant(InvariantError::Symlink) => {}
            _ => panic!("expected Symlink for a symlink"),
        }
    }

    #[test]
    fn directory_returns_non_regular_file() {
        let d = TempDir::new().unwrap();
        let sub = d.path().join("subdir");
        fs::create_dir(&sub).unwrap();
        match probe(&sub) {
            Probe::Invariant(InvariantError::NonRegularFile) => {}
            _ => panic!("expected NonRegularFile for a directory"),
        }
    }

    #[test]
    fn fifo_returns_non_regular_file() {
        let d = TempDir::new().unwrap();
        let fifo = d.path().join("fifo");
        let cs = std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes()).unwrap();
        let rc = unsafe { libc::mkfifo(cs.as_ptr(), 0o600) };
        assert_eq!(rc, 0, "mkfifo should succeed");
        match probe(&fifo) {
            Probe::Invariant(InvariantError::NonRegularFile) => {}
            _ => panic!("expected NonRegularFile for a fifo"),
        }
    }

    #[test]
    fn eacces_on_parent_dir_returns_stat_error() {
        // Skip when running as root — root traverses 0o000 dirs and
        // would not see EACCES, so the test cannot reproduce the
        // operator's common-case outcome.
        let euid = unsafe { libc::geteuid() };
        if euid == 0 {
            eprintln!(
                "eacces_on_parent_dir_returns_stat_error: skipped — \
                 test requires non-root euid (root traverses 0o000 dirs and would not see EACCES)",
            );
            return;
        }
        let d = TempDir::new().unwrap();
        let parent = d.path().join("locked");
        fs::create_dir(&parent).unwrap();
        let inside = parent.join("cred");
        fs::write(&inside, "secret").unwrap();
        fs::set_permissions(&inside, fs::Permissions::from_mode(0o600)).unwrap();
        // Strip search permission so symlink_metadata fails with
        // EACCES rather than ENOENT. Guard restores 0o755 on scope
        // exit (incl. panic) so TempDir can still clean up.
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o000)).unwrap();
        let _restore = ModeGuard {
            path: &parent,
            restore: 0o755,
        };
        match probe(&inside) {
            Probe::StatError(e) => {
                assert_eq!(e.kind(), io::ErrorKind::PermissionDenied);
            }
            other => panic!(
                "expected StatError(PermissionDenied) for EACCES, got {}",
                match other {
                    Probe::Ok => "Ok",
                    Probe::NotPresent => "NotPresent",
                    Probe::Invariant(_) => "Invariant",
                    Probe::StatError(_) => unreachable!(),
                }
            ),
        }
    }

    #[test]
    fn render_permissive_mode_is_actionable() {
        let path = PathBuf::from("/etc/gcit/credentials/github_pat");
        let err = InvariantError::PermissiveMode { mode: 0o644 };
        let msg = err.render(&id(), &path);
        assert!(msg.contains("test_cred"));
        assert!(msg.contains("0644"));
        assert!(msg.contains("0600"));
        assert!(msg.contains("chmod 0600"));
    }

    #[test]
    fn render_unexpected_owner_includes_chown() {
        let path = PathBuf::from("/etc/gcit/credentials/cred");
        let err = InvariantError::UnexpectedOwner {
            owner_uid: 1234,
            euid: 5678,
        };
        let msg = err.render(&id(), &path);
        assert!(msg.contains("1234"));
        assert!(msg.contains("5678"));
        assert!(msg.contains("chown 5678"));
        assert!(msg.contains("chown root"));
    }

    #[test]
    fn render_symlink_names_id_path_and_keyword() {
        let path = PathBuf::from("/etc/gcit/credentials/github_pat");
        let msg = InvariantError::Symlink.render(&id(), &path);
        assert!(msg.contains("test_cred"), "id missing: {msg}");
        assert!(
            msg.contains("/etc/gcit/credentials/github_pat"),
            "path missing: {msg}"
        );
        assert!(msg.contains("symlink"), "keyword missing: {msg}");
    }

    #[test]
    fn render_non_regular_file_names_id_path_and_keyword() {
        let path = PathBuf::from("/etc/gcit/credentials/cred");
        let msg = InvariantError::NonRegularFile.render(&id(), &path);
        assert!(msg.contains("test_cred"), "id missing: {msg}");
        assert!(
            msg.contains("/etc/gcit/credentials/cred"),
            "path missing: {msg}"
        );
        assert!(msg.contains("regular file"), "keyword missing: {msg}");
    }

    #[test]
    fn render_stat_error_pre_flight_includes_sudo_hint() {
        let path = PathBuf::from("/etc/gcit/credentials/cred");
        let err = io::Error::from(io::ErrorKind::PermissionDenied);
        let msg = render_stat_error(&err, &id(), &path, Context::PreFlight);
        assert!(msg.contains("test_cred"));
        assert!(msg.contains("/etc/gcit/credentials/cred"));
        assert!(msg.contains("permission denied") || msg.contains("Permission denied"));
        assert!(
            msg.contains("sudo gcit check"),
            "expected sudo recovery hint, got: {msg}"
        );
        assert!(
            msg.contains("credential owner"),
            "expected credential-owner hint, got: {msg}"
        );
    }

    #[test]
    fn render_stat_error_daemon_omits_sudo_hint() {
        // The daemon's effective uid is fixed by the unit
        // (DynamicUser=yes / User=gcit); `sudo gcit check` is the
        // wrong recovery action — the daemon will still resolve under
        // the same uid that produced the EACCES. Daemon-context
        // messages name the parent-dir lever instead.
        let path = PathBuf::from("/etc/gcit/credentials/cred");
        let err = io::Error::from(io::ErrorKind::PermissionDenied);
        let msg = render_stat_error(&err, &id(), &path, Context::Daemon);
        assert!(msg.contains("test_cred"));
        assert!(msg.contains("/etc/gcit/credentials/cred"));
        assert!(
            !msg.contains("sudo gcit check"),
            "daemon hint must not suggest `sudo gcit check`, got: {msg}",
        );
        assert!(
            msg.contains("parent directory permissions"),
            "expected parent-dir hint, got: {msg}",
        );
        assert!(
            msg.contains("daemon's effective uid"),
            "expected effective-uid hint, got: {msg}",
        );
    }

    // -----------------------------------------------------------------
    // owner_uid_accepted: the asymmetric-branch tests. The integration
    // test in tests/cli_check_3state.rs only exercises the same-uid
    // branch (owner_uid == euid). The production scenario for
    // DynamicUser=yes — root-owned credential resolved by an ephemeral
    // non-zero uid — cannot be reproduced from a privileged test
    // (chowning to 0 doesn't change the test process's euid). These
    // unit tests cover all four (owner_uid, euid) quadrants on the
    // pure predicate.
    // -----------------------------------------------------------------
    #[test]
    fn owner_uid_accepted_same_nonzero_uid() {
        // Operator-installed credential: file owned by the operator's
        // uid, gcit runs as that same uid (--user scope or
        // `gcit check` from the operator's shell).
        assert!(owner_uid_accepted(1000, 1000));
        assert!(owner_uid_accepted(65534, 65534));
    }

    #[test]
    fn owner_uid_accepted_root_owner_root_euid() {
        // Operator running `sudo gcit check` against a root-owned
        // credential. Both branches of the predicate (== euid AND ==
        // 0) hold; the predicate accepts.
        assert!(owner_uid_accepted(0, 0));
    }

    #[test]
    fn owner_uid_accepted_root_owner_nonzero_euid_dynamicuser_case() {
        // The asymmetric branch: file owned by root, process running
        // as a non-zero ephemeral uid. This is the production
        // DynamicUser=yes scenario — the daemon's euid is assigned
        // by systemd and is unrelated to the credential's owner. The
        // predicate must accept via owner_uid == 0 when owner_uid !=
        // euid.
        assert!(owner_uid_accepted(0, 1000));
        assert!(owner_uid_accepted(0, 65534));
        assert!(owner_uid_accepted(0, u32::MAX - 1));
    }

    #[test]
    fn owner_uid_accepted_nonzero_owner_root_euid() {
        // Operator running `sudo gcit check` against an
        // operator-owned credential. The predicate accepts because
        // `owner_uid == 0` is false but the inverse — `owner_uid !=
        // euid` is also false (euid is 0 here, but the operator's uid
        // is not 0)... wait, this case actually fails the predicate:
        // owner_uid (1000) != euid (0) AND owner_uid (1000) != 0, so
        // the original `if` is true and the probe rejects. The
        // predicate returns false. Pin that semantics — running
        // `sudo gcit check` against your-own-uid credentials is
        // rejected because the resolving process's euid (root) does
        // not match, and the credential isn't root-owned either.
        assert!(!owner_uid_accepted(1000, 0));
    }

    #[test]
    fn owner_uid_accepted_rejects_unrelated_uids() {
        // File owned by uid X, process running as uid Y where neither
        // is 0 and X != Y. Classic "wrong owner" rejection.
        assert!(!owner_uid_accepted(1000, 1001));
        assert!(!owner_uid_accepted(65534, 1000));
        assert!(!owner_uid_accepted(1, 2));
    }

    #[test]
    fn owner_uid_accepted_min_max_boundaries() {
        // Boundary checks: u32 extrema. Ensures no integer-overflow
        // surprise and that 0 stays "root" for both arguments.
        assert!(owner_uid_accepted(0, u32::MAX));
        assert!(owner_uid_accepted(u32::MAX, u32::MAX));
        assert!(!owner_uid_accepted(u32::MAX, 0));
        assert!(!owner_uid_accepted(u32::MAX, u32::MAX - 1));
    }
}

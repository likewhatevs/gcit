// `gcit check` 3-state exit semantics.
//
// State 1 (clean): config parses, validates, every credential
//                  resolvable in the current shell -> exit 0.
// State 2 (malformed): TOML / schema error -> exit 78 (EX_CONFIG).
// State 3 (declared-but-unresolvable-from-shell): config valid,
//                  credential declared, env var unset and credentials/
//                  file missing, but $CREDENTIALS_DIRECTORY is set ->
//                  exit 0 with INFO note (the systemd unit's
//                  LoadCredential= line will resolve at daemon start).
//
// Mutation testing tries to collapse states 2 and 3 into the same
// exit code; this test catches that.

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;

mod common;

const FULL_CONFIG: &str = r#"
[[flow]]
name = "flow1"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "github_pat"
[[flow.destination]]
kind = "discord_webhook"
credential_id = "discord_webhook"
"#;

fn write_config(dir: &TempDir, body: &str) -> std::path::PathBuf {
    let path = dir.path().join("gcit.toml");
    std::fs::write(&path, body).expect("write tempdir config");
    path
}

#[test]
fn state1_clean_config_with_resolvable_credentials_exits_zero() {
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir, FULL_CONFIG);
    Command::cargo_bin("gcit")
        .unwrap()
        .env_remove("CREDENTIALS_DIRECTORY")
        .env("GCIT_CREDENTIAL_GITHUB_PAT", "github_pat_dummy_value")
        .env(
            "GCIT_CREDENTIAL_DISCORD_WEBHOOK",
            "https://discord.com/api/webhooks/1/abc",
        )
        .arg("--config")
        .arg(&path)
        .arg("check")
        .assert()
        .code(0);
}

#[test]
fn state2_malformed_toml_exits_78() {
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir, "this is not valid TOML [");

    Command::cargo_bin("gcit")
        .unwrap()
        .env_remove("CREDENTIALS_DIRECTORY")
        .arg("--config")
        .arg(&path)
        .arg("check")
        .assert()
        .code(78);
}

#[test]
fn state2_invalid_schema_exits_78() {
    let dir = TempDir::new().unwrap();
    let raw = r#"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
"#;
    let path = write_config(&dir, raw);
    Command::cargo_bin("gcit")
        .unwrap()
        .env_remove("CREDENTIALS_DIRECTORY")
        .arg("--config")
        .arg(&path)
        .arg("check")
        .assert()
        .code(78);
}

#[test]
fn state2_credential_collision_exits_78() {
    let dir = TempDir::new().unwrap();
    let raw = r#"
[[flow]]
name = "a"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "foo-bar"
[[flow]]
name = "b"
[flow.source]
url = "https://git.kernel.org/y.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/s"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "foo_bar"
"#;
    let path = write_config(&dir, raw);
    Command::cargo_bin("gcit")
        .unwrap()
        .env_remove("CREDENTIALS_DIRECTORY")
        .arg("--config")
        .arg(&path)
        .arg("check")
        .assert()
        .code(78);
}

#[test]
fn state2_credential_id_undeclared_anywhere_exits_78() {
    // Config valid, credential referenced. NOT in env, no
    // $CREDENTIALS_DIRECTORY, no <config_dir>/credentials/.
    // Exit 78 with output naming the id.
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir, FULL_CONFIG);
    Command::cargo_bin("gcit")
        .unwrap()
        .env_remove("CREDENTIALS_DIRECTORY")
        .env_remove("GCIT_CREDENTIAL_GITHUB_PAT")
        .env_remove("GCIT_CREDENTIAL_DISCORD_WEBHOOK")
        .arg("--config")
        .arg(&path)
        .arg("check")
        .assert()
        .code(78)
        .stderr(predicate::str::contains("github_pat"));
}

#[test]
fn state3_declared_in_unit_but_not_currently_resolvable_exits_zero_with_note() {
    // If $CREDENTIALS_DIRECTORY is set but the credential is not
    // currently resolvable, exit 0 with an INFO note. The systemd
    // unit's LoadCredential= line will resolve it at daemon start.
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir, FULL_CONFIG);
    let creds_dir = dir.path().join("creds");
    std::fs::create_dir_all(&creds_dir).unwrap();
    Command::cargo_bin("gcit")
        .unwrap()
        .env("CREDENTIALS_DIRECTORY", &creds_dir)
        .env_remove("GCIT_CREDENTIAL_GITHUB_PAT")
        .env_remove("GCIT_CREDENTIAL_DISCORD_WEBHOOK")
        .arg("--config")
        .arg(&path)
        .arg("check")
        .assert()
        .code(0)
        .stdout(predicate::str::contains("INFO"));
}

#[test]
fn check_emits_all_errors_at_once_not_first_only() {
    // Config with TWO independent errors must produce TWO error lines,
    // not bail on the first.
    let dir = TempDir::new().unwrap();
    let raw = r#"
[poll]
jitter = 0.7
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
"#;
    let path = write_config(&dir, raw);
    Command::cargo_bin("gcit")
        .unwrap()
        .env_remove("CREDENTIALS_DIRECTORY")
        .arg("--config")
        .arg(&path)
        .arg("check")
        .assert()
        .code(78)
        .stderr(predicate::str::contains("jitter"))
        .stderr(predicate::str::contains("source.ref"));
}

// ---------------------------------------------------------------------
// Credential files must be mode 0600. A file with looser bits is
// rejected so credentials never leak via shared-host access.
// ---------------------------------------------------------------------
#[test]
fn credential_file_mode_0600_accepted() {
    use std::os::unix::fs::PermissionsExt;
    let dir = TempDir::new().unwrap();
    let cfg_path = write_config(&dir, FULL_CONFIG);
    let creds_dir = dir.path().join("credentials");
    std::fs::create_dir_all(&creds_dir).unwrap();
    for id in ["github_pat", "discord_webhook"] {
        let f = creds_dir.join(id);
        std::fs::write(&f, "secret-value").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    Command::cargo_bin("gcit")
        .unwrap()
        .env_remove("CREDENTIALS_DIRECTORY")
        .env_remove("GCIT_CREDENTIAL_GITHUB_PAT")
        .env_remove("GCIT_CREDENTIAL_DISCORD_WEBHOOK")
        .arg("--config")
        .arg(&cfg_path)
        .arg("check")
        .assert()
        .code(0);
}

#[test]
fn credential_file_mode_0644_rejected() {
    use std::os::unix::fs::PermissionsExt;
    let dir = TempDir::new().unwrap();
    let cfg_path = write_config(&dir, FULL_CONFIG);
    let creds_dir = dir.path().join("credentials");
    std::fs::create_dir_all(&creds_dir).unwrap();
    for id in ["github_pat", "discord_webhook"] {
        let f = creds_dir.join(id);
        std::fs::write(&f, "secret-value").unwrap();
        // 0o644 = world-readable; this must be rejected
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o644)).unwrap();
    }
    Command::cargo_bin("gcit")
        .unwrap()
        .env_remove("CREDENTIALS_DIRECTORY")
        .env_remove("GCIT_CREDENTIAL_GITHUB_PAT")
        .env_remove("GCIT_CREDENTIAL_DISCORD_WEBHOOK")
        .arg("--config")
        .arg(&cfg_path)
        .arg("check")
        .assert()
        .code(78)
        .stderr(predicate::str::contains("must be 0600"))
        .stderr(predicate::str::contains("chmod 0600"));
}

#[test]
fn credentials_directory_must_be_real_directory() {
    // A stale $CREDENTIALS_DIRECTORY pointing at a missing path must
    // NOT trigger state-3 (the "trust the systemd unit" path). Treat
    // as if the env var were unset.
    let dir = TempDir::new().unwrap();
    let path = write_config(&dir, FULL_CONFIG);
    let stale_dir = dir.path().join("does-not-exist");
    Command::cargo_bin("gcit")
        .unwrap()
        .env("CREDENTIALS_DIRECTORY", &stale_dir)
        .env_remove("GCIT_CREDENTIAL_GITHUB_PAT")
        .env_remove("GCIT_CREDENTIAL_DISCORD_WEBHOOK")
        .arg("--config")
        .arg(&path)
        .arg("check")
        .assert()
        // not 0 — the stale env var no longer rescues the missing
        // credential lookup
        .code(78);
}

// ---------------------------------------------------------------------
// EACCES-on-parent-dir hard error: cli/check must not certify a path
// it cannot examine. Operator running gcit check from a shell that
// cannot traverse the credential parent (system-install scenario:
// /etc/gcit/credentials is 0700 root:root, operator runs as themselves)
// will see exit 78 with the "could not be checked" wording plus the
// recovery hint pointing at sudo or the credential owner. The
// alternative — an INFO-note exit 0 — was rejected because the
// operator's shell EACCES does not predict the daemon's outcome:
// the daemon, running as a different uid, may traverse and reject
// the file (e.g. mode 0644), masking a real misconfiguration behind
// a green pre-flight.
// ---------------------------------------------------------------------
/// Drop guard that restores a directory's mode on scope exit. Used
/// by tests that drop search permission on a parent dir to provoke
/// EACCES on inner stat calls; without the guard, an early panic
/// (or future code change) would leave the TempDir uncleanable.
struct ModeGuard<'a> {
    path: &'a std::path::Path,
    restore: u32,
}

impl Drop for ModeGuard<'_> {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(self.path, std::fs::Permissions::from_mode(self.restore));
    }
}

#[test]
fn step3_eacces_on_parent_dir_exits_78_with_sudo_hint() {
    use std::os::unix::fs::PermissionsExt;
    // Skip when running as root — root traverses 0o000 dirs and would
    // not see EACCES, so the test cannot reproduce the operator's
    // common-case outcome.
    if common::euid_is_root() {
        eprintln!(
            "step3_eacces_on_parent_dir_exits_78_with_sudo_hint: skipped — \
             test requires non-root euid (root traverses 0o000 dirs and would not see EACCES)",
        );
        return;
    }
    let dir = TempDir::new().unwrap();
    let cfg_path = write_config(&dir, FULL_CONFIG);
    let creds_dir = dir.path().join("credentials");
    std::fs::create_dir_all(&creds_dir).unwrap();
    for id in ["github_pat", "discord_webhook"] {
        let f = creds_dir.join(id);
        std::fs::write(&f, "secret-value").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    // Strip search permission on the parent so symlink_metadata fails
    // with EACCES rather than ENOENT. Guard restores 0o755 on scope
    // exit (incl. panic) so TempDir can still clean up.
    std::fs::set_permissions(&creds_dir, std::fs::Permissions::from_mode(0o000)).unwrap();
    let _restore = ModeGuard {
        path: &creds_dir,
        restore: 0o755,
    };
    let output = Command::cargo_bin("gcit")
        .unwrap()
        .env_remove("CREDENTIALS_DIRECTORY")
        .env_remove("GCIT_CREDENTIAL_GITHUB_PAT")
        .env_remove("GCIT_CREDENTIAL_DISCORD_WEBHOOK")
        .arg("--config")
        .arg(&cfg_path)
        .arg("check")
        .output()
        .expect("spawning gcit check should succeed even on non-zero exit");
    assert_eq!(
        output.status.code(),
        Some(78),
        "expected exit 78 (EX_CONFIG), got {:?}",
        output.status.code()
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("could not be checked"),
        "expected 'could not be checked' wording in stderr: {stderr}"
    );
    assert!(
        stderr.contains("sudo gcit check") || stderr.contains("credential owner"),
        "expected sudo / owner recovery hint in stderr: {stderr}"
    );
}

// ---------------------------------------------------------------------
// Root-uid acceptance: a credential file owned by root (uid 0) must be
// accepted by `gcit check` regardless of the resolving process's euid.
// The probe accepts owner-uid == euid OR owner-uid == 0 — both branches
// must pass. This mirrors the daemon supervisor's invariant: under
// DynamicUser=yes, /etc/gcit/credentials/<id> is typically root-owned at
// install time and remains root-owned across daemon restarts (no chown
// hook fires when the dynamic uid rotates).
//
// Gated on running as root because chown(2) requires CAP_CHOWN to set
// uid 0 on a file. Skip otherwise — the check cannot be exercised
// without that capability.
//
// LIMITATION: this integration test only exercises the trivial
// same-uid branch (test process is root, files are chowned to 0, so
// `owner_uid == euid == 0` already satisfies the predicate). The
// production scenario this is meant to model — a non-root daemon
// (DynamicUser=yes, ephemeral non-zero euid) resolving a root-owned
// credential — cannot be reproduced from a privileged test because
// chown(2) does not change the test process's euid. A regression
// that collapsed the predicate to single-condition `if owner_uid !=
// euid` would still pass this test.
//
// The asymmetric `owner_uid == 0 && euid != 0` branch is pinned by
// pure-predicate unit tests on `owner_uid_accepted` in
// src/config/credential_file.rs (see
// owner_uid_accepted_root_owner_nonzero_euid_dynamicuser_case et al.)
// — those run on every `cargo nextest run` regardless of test-process
// euid and catch the regression mentioned above.
// ---------------------------------------------------------------------
#[test]
fn credential_file_owned_by_root_accepted() {
    use std::os::unix::fs::PermissionsExt;
    if !common::euid_is_root() {
        eprintln!(
            "credential_file_owned_by_root_accepted: skipped — \
             test requires root euid (chown to uid 0)",
        );
        return;
    }
    let dir = TempDir::new().unwrap();
    let cfg_path = write_config(&dir, FULL_CONFIG);
    let creds_dir = dir.path().join("credentials");
    std::fs::create_dir_all(&creds_dir).unwrap();
    for id in ["github_pat", "discord_webhook"] {
        let f = creds_dir.join(id);
        std::fs::write(&f, "secret-value").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600)).unwrap();
        // chown to uid 0; we are already root, so this either sets the
        // owner or is a noop (the test process is itself uid 0).
        let cs = std::ffi::CString::new(f.as_os_str().as_encoded_bytes()).unwrap();
        let rc = unsafe { libc::chown(cs.as_ptr(), 0, libc::gid_t::MAX) };
        assert_eq!(rc, 0, "chown to root must succeed when running as root");
    }
    Command::cargo_bin("gcit")
        .unwrap()
        .env_remove("CREDENTIALS_DIRECTORY")
        .env_remove("GCIT_CREDENTIAL_GITHUB_PAT")
        .env_remove("GCIT_CREDENTIAL_DISCORD_WEBHOOK")
        .arg("--config")
        .arg(&cfg_path)
        .arg("check")
        .assert()
        .code(0);
}

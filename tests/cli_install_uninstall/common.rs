// Shared helpers for the `gcit install` / `gcit uninstall` submodules.
//
// Every sub-test imports `super::common::*` so the per-theme test
// files stay focused on assertions, not fixture plumbing. `pub` is
// per the Rust module-private convention — siblings cannot see
// `pub(super)` items, so widen to `pub` and let the `mod tests`
// boundary of the integration-test binary contain the surface.

#![allow(dead_code)]

use std::io::Write;
use std::path::Path;

use assert_cmd::Command;
use tempfile::TempDir;

/// Minimal valid gcit config — Discord-only flow so the install path
/// stays on the DynamicUser=yes branch and never invokes useradd. Used
/// by the install/uninstall success-path + non-interactive tests where
/// we want config::load to succeed without dragging in local_mail
/// machinery.
pub const MINIMAL_CONFIG_TOML: &str = "[[flow]]\n\
    name = \"minimal\"\n\
    \n\
    [flow.source]\n\
    url = \"https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git\"\n\
    ref = \"refs/heads/master\"\n\
    \n\
    [flow.action]\n\
    kind          = \"github_workflow_dispatch\"\n\
    repo          = \"owner/repo\"\n\
    workflow      = \"ci.yml\"\n\
    ref           = \"refs/heads/main\"\n\
    credential_id = \"github_pat\"\n\
    \n\
    [[flow.destination]]\n\
    kind          = \"discord_webhook\"\n\
    credential_id = \"discord_webhook\"\n";

/// local_mail-only flow config; drives the `gcit install --user`
/// rejection arm and the `--system` static-user path.
pub const LOCAL_MAIL_CONFIG_TOML: &str = "[[flow]]\n\
    name = \"localmail-flow\"\n\
    \n\
    [flow.source]\n\
    url = \"https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git\"\n\
    ref = \"refs/heads/master\"\n\
    \n\
    [flow.action]\n\
    kind          = \"github_workflow_dispatch\"\n\
    repo          = \"owner/repo\"\n\
    workflow      = \"ci.yml\"\n\
    ref           = \"refs/heads/main\"\n\
    credential_id = \"github_pat\"\n\
    \n\
    [[flow.destination]]\n\
    kind = \"local_mail\"\n\
    user = \"ops\"\n";

/// Config with a source-side credential — drives the
/// `CredentialKindHint::SourceFetch` walkthrough arm.
pub const SOURCE_FETCH_CREDENTIAL_CONFIG_TOML: &str = "[[flow]]\n\
    name = \"source-fetch-test\"\n\
    \n\
    [flow.source]\n\
    url = \"https://git.example.com/repo.git\"\n\
    ref = \"refs/heads/master\"\n\
    credential_id = \"source_only_cred\"\n\
    \n\
    [flow.action]\n\
    kind          = \"github_workflow_dispatch\"\n\
    repo          = \"owner/repo\"\n\
    workflow      = \"ci.yml\"\n\
    ref           = \"refs/heads/main\"\n\
    credential_id = \"github_pat\"\n\
    \n\
    [[flow.destination]]\n\
    kind          = \"discord_webhook\"\n\
    credential_id = \"discord_webhook\"\n";

/// Write `MINIMAL_CONFIG_TOML` into `<dir>/config.toml` and return the
/// full path. Caller passes the result to `--config` so the install
/// path's `config::load` succeeds against a tempdir-rooted file.
pub fn write_minimal_config(dir: &Path) -> std::path::PathBuf {
    let p = dir.join("config.toml");
    let mut f = std::fs::File::create(&p).expect("create minimal config");
    f.write_all(MINIMAL_CONFIG_TOML.as_bytes())
        .expect("write minimal config");
    f.sync_all().expect("sync minimal config");
    p
}

/// Write `LOCAL_MAIL_CONFIG_TOML` to `<dir>/config.toml`.
pub fn write_local_mail_config(dir: &Path) -> std::path::PathBuf {
    let p = dir.join("config.toml");
    std::fs::write(&p, LOCAL_MAIL_CONFIG_TOML).expect("write local_mail config");
    p
}

/// Drive a `gcit install` invocation against an isolated, empty
/// tempdir-rooted XDG layout. All env vars that influence path
/// resolution are pinned to the tempdir so a stray system install
/// never affects the test, and an interrupted test cannot leave
/// crumbs in the operator's real `~/.config` / `~/.local` tree.
pub fn isolated_command(home: &Path) -> Command {
    let mut cmd = Command::cargo_bin("gcit").expect("cargo-built gcit binary");
    cmd.env("HOME", home);
    cmd.env("XDG_CONFIG_HOME", home.join(".config"));
    cmd.env("XDG_DATA_HOME", home.join(".local").join("share"));
    cmd.env("XDG_STATE_HOME", home.join(".local").join("state"));
    cmd.env("XDG_RUNTIME_DIR", home.join("run"));
    // The systemd / journald layers should not be exercised for these
    // CLI-surface tests; explicitly remove any inherited values that
    // could change the early-init path.
    cmd.env_remove("CREDENTIALS_DIRECTORY");
    cmd.env_remove("STATE_DIRECTORY");
    cmd.env_remove("RUNTIME_DIRECTORY");
    cmd.env_remove("INVOCATION_ID");
    cmd.env_remove("JOURNAL_STREAM");
    cmd
}

/// Resolve the user-scope managed paths the install wizard writes to
/// under `home_dir`. Mirrors `install_paths(InstallScope::User, &home)`
/// in systemd::unit with $XDG_* envs from `isolated_command`.
pub fn user_scope_paths(home_dir: &Path) -> UserScopePaths {
    let xdg_config = home_dir.join(".config");
    let xdg_state = home_dir.join(".local").join("state");
    UserScopePaths {
        service_unit: xdg_config.join("systemd").join("user").join("gcit.service"),
        socket_unit: xdg_config.join("systemd").join("user").join("gcit.socket"),
        config: xdg_config.join("gcit").join("config.toml"),
        credentials_dir: xdg_config.join("gcit").join("credentials"),
        manifest: xdg_state.join("gcit").join(".install-manifest.json"),
    }
}

pub struct UserScopePaths {
    pub service_unit: std::path::PathBuf,
    pub socket_unit: std::path::PathBuf,
    pub config: std::path::PathBuf,
    pub credentials_dir: std::path::PathBuf,
    pub manifest: std::path::PathBuf,
}

/// Run a fresh `gcit install --user --non-interactive --force` against
/// the supplied isolated home dir + config path. Used by the uninstall
/// happy-path tests so each one starts from a known-installed state.
/// Asserts exit 0 so a regression in the install path surfaces here
/// rather than masking as an uninstall failure.
pub fn run_user_install(home_dir: &Path, config_path: &Path) {
    isolated_command(home_dir)
        .arg("--config")
        .arg(config_path)
        .arg("install")
        .arg("--user")
        .arg("--non-interactive")
        .arg("--force")
        .assert()
        .code(0);
}

/// SHA-256 hex of `bytes`, lower-case. Uses the sha2 crate that the
/// production install code already depends on (see cli::install).
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    format!("{:x}", h.finalize())
}

/// Convenience: build a temp home directory + write a minimal config
/// inside it. Returns the TempDir (must outlive the test) and the
/// config path.
pub fn home_with_minimal_config() -> (TempDir, std::path::PathBuf) {
    let td = TempDir::new().expect("tempdir");
    let cfg = write_minimal_config(td.path());
    (td, cfg)
}

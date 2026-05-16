// Credential walkthrough: the pre-configured short-circuit (✓ marker)
// and the source-side credential kind hint surfaced when a credential
// is referenced only from `[flow.source]`.

use std::os::unix::fs::PermissionsExt;
use tempfile::TempDir;

use super::common::{
    isolated_command, user_scope_paths, write_minimal_config, SOURCE_FETCH_CREDENTIAL_CONFIG_TOML,
};

#[test]
fn install_user_with_pre_configured_credential_file_emits_check_marker_in_walkthrough() {
    // In cli::install's credential walkthrough, when a credential file
    // exists at the canonical path with mode bitmask `mode & 0o077 == 0`
    // and is owned by the invoking euid OR root, the wizard prints a
    // one-line confirmation instead of the full instructions.
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let paths = user_scope_paths(td.path());

    // Place a 0600 credential file at the destination the walkthrough
    // would print. The minimal config references credential_id values
    // "github_pat" and "discord_webhook" (per write_minimal_config /
    // MINIMAL_CONFIG_TOML in `super::common`).
    std::fs::create_dir_all(&paths.credentials_dir).unwrap();
    for id in ["github_pat", "discord_webhook"] {
        let f = paths.credentials_dir.join(id);
        std::fs::write(&f, "secret-value-for-test").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    let output = isolated_command(td.path())
        .arg("--config")
        .arg(&cfg)
        .arg("install")
        .arg("--user")
        .arg("--non-interactive")
        .arg("--force")
        .output()
        .expect("install command must run");
    assert_eq!(output.status.code(), Some(0), "install must succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    // The pre-configured credential lines surface with the canonical
    // ✓ marker emitted by the credential walkthrough's short-circuit
    // arm. A regression that dropped the short-circuit would re-print
    // the full "obtain at:" / "chmod" instructions instead.
    assert!(
        stdout.contains("✓ github_pat"),
        "stdout must surface the ✓ short-circuit for github_pat; got: {stdout}",
    );
    assert!(
        stdout.contains("✓ discord_webhook"),
        "stdout must surface the ✓ short-circuit for discord_webhook; got: {stdout}",
    );
    // The full-instructions block emits "obtain at:" — the short-
    // circuit must NOT print this for either credential. (A different
    // credential might still emit it in another config; here every
    // referenced credential is pre-configured.)
    assert!(
        !stdout.contains("obtain at:"),
        "short-circuited credential walkthrough must NOT print full instructions; got: {stdout}",
    );
}

#[test]
fn install_user_walkthrough_surfaces_source_fetch_kind_hint_for_source_only_credential() {
    // The cli::install walkthrough prints a per-credential block;
    // for a credential id referenced ONLY from [flow.source] (and not
    // also from [flow.action] / [[flow.destination]]) the kind hint
    // stays at SourceFetch and the line "kind: source-side fetch
    // credential" surfaces.
    //
    // Pin: with `source_only_cred` referenced only from [flow.source],
    // the install stdout includes the SourceFetch line for that id.
    // The other two credentials in the config (`github_pat` for the
    // action, `discord_webhook` for the destination) get their own
    // upgrade-paths and emit GitHub/Discord-specific lines instead.
    let td = TempDir::new().unwrap();
    let cfg_path = td.path().join("config.toml");
    std::fs::write(&cfg_path, SOURCE_FETCH_CREDENTIAL_CONFIG_TOML).unwrap();
    let output = isolated_command(td.path())
        .arg("--config")
        .arg(&cfg_path)
        .arg("install")
        .arg("--user")
        .arg("--non-interactive")
        .arg("--force")
        .output()
        .expect("install command must run");
    assert_eq!(output.status.code(), Some(0), "install must succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    // The SourceFetch arm is the only kind hint that prints
    // "source-side fetch credential" — an upgrade to GithubPat or
    // DiscordWebhook would print different text.
    assert!(
        stdout.contains("source-side fetch credential"),
        "stdout must surface SourceFetch kind hint for source_only_cred; got: {stdout}",
    );
    // The id name itself must surface in the credential walkthrough
    // header.
    assert!(
        stdout.contains("source_only_cred"),
        "stdout must surface the credential id; got: {stdout}",
    );
}

#[test]
fn install_user_with_source_credential_id_completes_and_walkthrough_includes_id() {
    // Install --user --non-interactive against a config whose
    // [flow.source] carries a credential_id. Pin: install proceeds,
    // walkthrough mentions the source credential id in the printed
    // walkthrough block, and the post-install state matches the
    // no-source-credential path (manifest + units present).
    let td = TempDir::new().unwrap();
    let cfg_path = td.path().join("config.toml");
    std::fs::write(&cfg_path, SOURCE_FETCH_CREDENTIAL_CONFIG_TOML).unwrap();
    let paths = user_scope_paths(td.path());

    let output = isolated_command(td.path())
        .arg("--config")
        .arg(&cfg_path)
        .arg("install")
        .arg("--user")
        .arg("--non-interactive")
        .arg("--force")
        .output()
        .expect("install command must run");
    assert_eq!(output.status.code(), Some(0), "install must succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Walkthrough surfaces the source-only id name.
    assert!(
        stdout.contains("source_only_cred"),
        "walkthrough must surface source_only_cred; got: {stdout}",
    );
    // The path-preview line for gcit.service surfaces too — pin so a
    // regression that swallows the walkthrough text would surface.
    assert!(
        stdout.contains("gcit.service"),
        "path preview must mention gcit.service; got: {stdout}",
    );
    // Post-install state mirrors the minimal-config path.
    assert!(paths.service_unit.exists());
    assert!(paths.socket_unit.exists());
    assert!(paths.config.exists());
    assert!(paths.manifest.exists());
}

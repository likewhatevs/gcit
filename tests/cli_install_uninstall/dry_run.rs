// `gcit install --dry-run` — short-circuits after config + scope
// validation, renders the systemd service unit to stdout, exits 0,
// writes nothing. Tests pin: stdout shape (only unit text), exit code,
// zero side effects on disk, and that pre-dry-run validation gates
// still fire (config errors, --user + local_mail rejection, scope-
// specific LoadCredential paths).

use tempfile::TempDir;

use super::common::{
    isolated_command, user_scope_paths, write_local_mail_config, write_minimal_config,
};

#[test]
fn install_user_dry_run_prints_only_service_unit_text_and_exits_zero() {
    // Drives `gcit install --user --dry-run` against a Discord-only
    // minimal config. cli::install::run renders the service unit via
    // render_service_unit() and prints it to stdout, then exits OK.
    // No credential walkthrough, no path preview, no prompt, no
    // daemon-reload, no post-install banner — stdout is the unit text
    // only, suitable for piping to `systemd-analyze security`.
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let output = isolated_command(td.path())
        .arg("--config")
        .arg(&cfg)
        .arg("install")
        .arg("--user")
        .arg("--dry-run")
        .output()
        .expect("install --dry-run must run");
    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    // First line is the [Unit] header from render_service_unit().
    assert!(
        stdout.starts_with("[Unit]\n"),
        "stdout must start with `[Unit]\\n`; got: {stdout}",
    );
    // Last non-empty content from render_service_unit() ends with a
    // trailing newline after WantedBy=default.target.
    assert!(
        stdout.ends_with("WantedBy=default.target\n"),
        "stdout must end with `WantedBy=default.target\\n`; got tail: {:?}",
        &stdout.as_bytes()[stdout.len().saturating_sub(40)..],
    );
    // None of the install-side print markers leak into the dry-run
    // pipe. Each marker corresponds to a distinct print site
    // (credential walkthrough, path preview, prompt, post-install
    // banner) that must be skipped by the early-exit branch.
    for marker in [
        "# Credentials",
        "# Files gcit will write",
        "# Next steps",
        "Proceed?",
        "install cancelled",
        "Wrote ",
        "daemon-reload",
    ] {
        assert!(
            !stdout.contains(marker),
            "dry-run stdout must not contain `{marker}`; got: {stdout}",
        );
    }
    // Discord-only config emits DynamicUser=yes (not the static
    // User=gcit / Group=mail pair). Pin so a regression that broke
    // the user-model branch surfaces here as well.
    assert!(
        stdout.contains("DynamicUser=yes"),
        "Discord-only dry-run must emit DynamicUser=yes; got: {stdout}",
    );
    assert!(
        !stdout.contains("User=gcit"),
        "Discord-only dry-run must NOT emit User=gcit; got: {stdout}",
    );
}

#[test]
fn install_user_dry_run_writes_no_files() {
    // Dry-run must not write any of the managed paths the normal
    // install path writes: gcit.service, gcit.socket, the config copy,
    // or the install manifest. Anchored to the same path-resolution
    // helper used by the success-path tests so a regression that
    // produced silent writes surfaces.
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let paths = user_scope_paths(td.path());
    isolated_command(td.path())
        .arg("--config")
        .arg(&cfg)
        .arg("install")
        .arg("--user")
        .arg("--dry-run")
        .assert()
        .code(0);
    assert!(!paths.service_unit.exists());
    assert!(!paths.socket_unit.exists());
    assert!(!paths.config.exists());
    assert!(!paths.manifest.exists());
}

#[test]
fn install_user_dry_run_with_local_mail_still_rejected_with_usage_64() {
    use predicates::prelude::*;
    // The --user + local_mail rejection runs BEFORE the dry-run
    // early-exit. Letting dry-run emit the rendered unit for that
    // combination would print `User=gcit / Group=mail` for a scope
    // that cannot grant the mail group — broken on its face. The
    // rejection therefore stays in dry-run too.
    let td = TempDir::new().unwrap();
    let cfg = write_local_mail_config(td.path());
    isolated_command(td.path())
        .arg("--config")
        .arg(&cfg)
        .arg("install")
        .arg("--user")
        .arg("--dry-run")
        .assert()
        .code(64)
        .stderr(predicate::str::contains("/var/mail"))
        .stderr(predicate::str::contains("--system"));
}

#[test]
fn install_user_dry_run_with_malformed_config_exits_config_78() {
    // config::load failure runs BEFORE the dry-run early-exit. A bad
    // config still maps to EX_CONFIG=78 in dry-run; we don't want a
    // CI pipeline to render a unit from a config that no real install
    // would accept.
    let td = TempDir::new().unwrap();
    let bad_config = td.path().join("malformed.toml");
    std::fs::write(&bad_config, "this is = not valid = toml = at all\n").unwrap();
    isolated_command(td.path())
        .arg("--config")
        .arg(&bad_config)
        .arg("install")
        .arg("--user")
        .arg("--dry-run")
        .assert()
        .code(78);
}

#[test]
fn install_user_dry_run_emits_user_scope_load_credential_paths() {
    // The LoadCredential lines reflect the chosen scope: user-scope
    // installs source credentials from `%E/gcit/credentials/<id>` (the
    // systemd specifier for $XDG_CONFIG_HOME). Pinned so a regression
    // that flipped the scope-driven path back to /etc/gcit/credentials
    // (the system-scope value) surfaces here.
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let output = isolated_command(td.path())
        .arg("--config")
        .arg(&cfg)
        .arg("install")
        .arg("--user")
        .arg("--dry-run")
        .output()
        .expect("install --dry-run must run");
    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Minimal config references credential ids `github_pat` and
    // `discord_webhook`; both surface as LoadCredential= lines under
    // `%E/gcit/credentials/` for user scope.
    assert!(
        stdout.contains("LoadCredential=github_pat:%E/gcit/credentials/github_pat"),
        "user-scope dry-run must surface user-scope LoadCredential path; got: {stdout}",
    );
    assert!(
        stdout.contains("LoadCredential=discord_webhook:%E/gcit/credentials/discord_webhook"),
        "user-scope dry-run must surface user-scope LoadCredential path; got: {stdout}",
    );
    // Sibling check: the system-scope path must NOT appear in a
    // user-scope dry-run, otherwise the operator would see a unit
    // that points at a directory their session cannot read.
    assert!(
        !stdout.contains("/etc/gcit/credentials/"),
        "user-scope dry-run must not surface the system-scope credentials path; got: {stdout}",
    );
}

#[test]
fn install_system_dry_run_emits_system_scope_load_credential_paths() {
    // Gap A: --system scope LoadCredential paths.
    // User-scope dry-run pins %E/gcit/credentials/. System-scope must
    // use the FHS path /etc/gcit/credentials/ instead (per
    // render_service_unit branch in src/systemd/unit.rs).
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let output = isolated_command(td.path())
        .arg("--config")
        .arg(&cfg)
        .arg("install")
        .arg("--system")
        .arg("--dry-run")
        .output()
        .expect("install --dry-run must run");
    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("LoadCredential=github_pat:/etc/gcit/credentials/github_pat"),
        "system-scope dry-run must source credentials from /etc/gcit; got: {stdout}",
    );
    assert!(
        !stdout.contains("%E/gcit/credentials"),
        "system-scope dry-run must not emit the user-scope %E/ token; got: {stdout}",
    );
}

#[test]
fn install_system_dry_run_with_local_mail_emits_static_user_and_var_mail_path() {
    // Gap B: --system + local_mail emits the static
    // User=gcit/Group=mail form (not DynamicUser=yes) and the
    // BindPaths=/var/mail directive. The --user + local_mail
    // combination is rejected before dry-run runs, so the only path
    // that exercises this branch is --system. Pinned: dry-run renders
    // the static-user variant verbatim.
    let td = TempDir::new().unwrap();
    let cfg = write_local_mail_config(td.path());
    let output = isolated_command(td.path())
        .arg("--config")
        .arg(&cfg)
        .arg("install")
        .arg("--system")
        .arg("--dry-run")
        .output()
        .expect("install --dry-run must run");
    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("User=gcit"),
        "system-scope local_mail dry-run must emit User=gcit; got: {stdout}",
    );
    assert!(
        stdout.contains("Group=mail"),
        "system-scope local_mail dry-run must emit Group=mail; got: {stdout}",
    );
    assert!(
        stdout.contains("SupplementaryGroups=mail"),
        "system-scope local_mail dry-run must emit SupplementaryGroups=mail; got: {stdout}",
    );
    assert!(
        !stdout.contains("DynamicUser=yes"),
        "system-scope local_mail dry-run must NOT emit DynamicUser=yes; got: {stdout}",
    );
    assert!(
        stdout.contains("BindPaths=/var/mail"),
        "system-scope local_mail dry-run must emit BindPaths=/var/mail; got: {stdout}",
    );
}

#[test]
fn install_user_dry_run_with_pre_existing_files_does_not_overwrite() {
    // Gap C: pre-existing managed files survive dry-run. The
    // silent-overwrite gate at install.rs is BYPASSED by the dry-run
    // short-circuit. With a sentinel pre-written into gcit.service,
    // dry-run must exit 0 AND leave the sentinel byte-for-byte.
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let paths = user_scope_paths(td.path());
    std::fs::create_dir_all(paths.service_unit.parent().unwrap()).unwrap();
    let sentinel = b"DRY_RUN_MUST_NOT_OVERWRITE_THIS_SENTINEL";
    std::fs::write(&paths.service_unit, sentinel).unwrap();
    isolated_command(td.path())
        .arg("--config")
        .arg(&cfg)
        .arg("install")
        .arg("--user")
        .arg("--dry-run")
        .assert()
        .code(0);
    let after = std::fs::read(&paths.service_unit).expect("file still exists");
    assert_eq!(
        after, sentinel,
        "dry-run must not overwrite pre-existing files"
    );
}

#[test]
fn install_user_dry_run_with_force_flag_behaves_identically() {
    // Gap D: --dry-run + --force is a no-op for --force. The early
    // short-circuit returns before the silent-overwrite gate fires,
    // so --force changes nothing. Pinned: same exit + same stdout
    // shape as without --force.
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let without_force = isolated_command(td.path())
        .arg("--config")
        .arg(&cfg)
        .arg("install")
        .arg("--user")
        .arg("--dry-run")
        .output()
        .expect("install --dry-run must run");
    assert_eq!(without_force.status.code(), Some(0));
    let with_force = isolated_command(td.path())
        .arg("--config")
        .arg(&cfg)
        .arg("install")
        .arg("--user")
        .arg("--dry-run")
        .arg("--force")
        .output()
        .expect("install --dry-run --force must run");
    assert_eq!(with_force.status.code(), Some(0));
    assert_eq!(
        without_force.stdout, with_force.stdout,
        "--force must not change dry-run stdout",
    );
}

#[test]
fn install_user_dry_run_without_non_interactive_exits_zero() {
    use predicates::prelude::*;
    // Gap E: dry-run without --non-interactive is still
    // non-interactive because the early-return fires before the
    // prompt. assert_cmd's default closed-stdin would otherwise
    // trigger the prompt's read failure. Pinned: dry-run sans
    // --non-interactive exits 0 cleanly.
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    isolated_command(td.path())
        .arg("--config")
        .arg(&cfg)
        .arg("install")
        .arg("--user")
        .arg("--dry-run")
        .assert()
        .code(0)
        .stdout(predicate::str::contains("[Unit]"))
        .stdout(predicate::str::contains("WantedBy=default.target"));
}

#[test]
fn install_user_dry_run_with_schema_invalid_config_exits_config_78() {
    // Gap F: schema-invalid config. The malformed-TOML test exercises
    // the parse-stage Err arm; this exercises the schema-validation
    // Err arm. Both should map to EX_CONFIG=78 in dry-run.
    let cfg = std::path::Path::new("tests/resources/config/invalid/duplicate_flow_name.toml");
    let td = TempDir::new().unwrap();
    isolated_command(td.path())
        .arg("--config")
        .arg(cfg)
        .arg("install")
        .arg("--user")
        .arg("--dry-run")
        .assert()
        .code(78);
}

#[test]
fn install_user_dry_run_stdout_omits_every_install_print_marker() {
    // Gap G: expanded stdout-discipline markers. The original test
    // pins 7 markers; the tester surfaced 6 more sites in
    // cli/install.rs that must not leak. Group both old + new markers
    // into one assertion list so a regression at any of the print
    // sites surfaces.
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let output = isolated_command(td.path())
        .arg("--config")
        .arg(&cfg)
        .arg("install")
        .arg("--user")
        .arg("--dry-run")
        .output()
        .expect("install --dry-run must run");
    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    for marker in [
        "# Credentials",
        "obtain at:",
        "chmod 0600",
        "# Files gcit will write",
        "# Manifest",
        "# Next steps",
        "# Service user model",
        "Proceed?",
        "install cancelled",
        "Wrote ",
        "daemon-reload",
        "Creating system user",
        "[exists]",
        "[new]",
    ] {
        assert!(
            !stdout.contains(marker),
            "dry-run stdout must not leak install marker `{marker}`; got: {stdout}",
        );
    }
}

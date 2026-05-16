// Install happy-path lifecycle: non-interactive success, interactive
// cancel, refuse-silent-overwrite without --force, --force override,
// and idempotent double-run. Each test isolates an XDG-rooted home
// directory and validates the on-disk side-effects produced by
// `cli::install::run`.

use predicates::prelude::*;
use tempfile::TempDir;

use super::common::{isolated_command, run_user_install, user_scope_paths, write_minimal_config};

#[test]
fn install_user_non_interactive_with_valid_config_writes_files_and_exits_zero() {
    // End-to-end success path. `gcit install --user --non-interactive
    // --force` against a Discord-only minimal config:
    //   * config::load succeeds
    //   * has_local_mail=false, so the --user + local_mail rejection
    //     does NOT fire
    //   * has_local_mail=false, so ensure_static_user is NOT called
    //     — no useradd, no mail-group requirement
    //   * --non-interactive skips the [y/N] prompt
    //   * --force overrides any pre-existing-file refusal; a fresh
    //     tempdir has nothing pre-existing so it doesn't matter here,
    //     but we pass it for robustness against parallel test runs
    //   * write_outputs writes the three managed files into the
    //     tempdir-rooted XDG layout (resolved via install_paths) and
    //     the manifest under XDG_STATE_HOME
    //   * trigger_daemon_reload on the user session bus may fail in CI
    //     (no session bus); the install path only emits a `warning:`
    //     and continues — exit code stays OK
    //
    // Pin that all three managed files exist on disk after the run +
    // the manifest is present at the expected path.
    let td = TempDir::new().unwrap();
    let config = write_minimal_config(td.path());
    isolated_command(td.path())
        .arg("--config")
        .arg(&config)
        .arg("install")
        .arg("--user")
        .arg("--non-interactive")
        .arg("--force")
        .assert()
        .code(0);

    // Verify the wizard wrote every file the manifest tracks. Paths
    // mirror `install_paths(InstallScope::User, $HOME)` from
    // systemd::unit with tempdir-rooted XDG.
    let xdg_config = td.path().join(".config");
    let xdg_state = td.path().join(".local").join("state");
    let units_dir = xdg_config.join("systemd").join("user");
    assert!(
        units_dir.join("gcit.service").exists(),
        "install must write gcit.service",
    );
    assert!(
        units_dir.join("gcit.socket").exists(),
        "install must write gcit.socket",
    );
    assert!(
        xdg_config.join("gcit").join("config.toml").exists(),
        "install must write the config copy",
    );
    assert!(
        xdg_state
            .join("gcit")
            .join(".install-manifest.json")
            .exists(),
        "install must write the manifest",
    );
}

#[test]
fn install_user_interactive_with_closed_stdin_cancels_and_exits_zero() {
    // Without `--non-interactive`, the wizard prints "Proceed? [y/N] "
    // and reads stdin (cli::install's interactive prompt). assert_cmd's
    // default `.assert()` provides a closed stdin (EOF on first read);
    // `io::stdin().read_line(&mut answer)` returns Ok(0), leaving
    // `answer` empty. The empty string does not match
    // `"y"|"Y"|"yes"|"YES"|"Yes"` → "install cancelled; nothing written."
    // → exit::OK. Pin that the interactive cancel arm exits cleanly
    // without writing files (operator changing their mind is not a
    // failure).
    let td = TempDir::new().unwrap();
    let config = write_minimal_config(td.path());
    isolated_command(td.path())
        .arg("--config")
        .arg(&config)
        .arg("install")
        .arg("--user")
        .assert()
        .code(0)
        .stdout(predicate::str::contains("install cancelled"));

    // Cancel arm wrote nothing. Pin one of the managed paths to catch a
    // regression that would (incorrectly) commit writes before reading
    // stdin.
    let units_dir = td.path().join(".config").join("systemd").join("user");
    assert!(
        !units_dir.join("gcit.service").exists(),
        "cancelled install must NOT write gcit.service",
    );
}

#[test]
fn install_user_refuses_silent_overwrite_without_force_exits_config_78() {
    // Pre-create one of the managed paths (the service unit) under the
    // tempdir-rooted XDG layout BEFORE running install. With no --force,
    // the existing-file detection in cli::install::run fires and the
    // wizard exits 78 without overwriting anything.
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let paths = user_scope_paths(td.path());

    // Pre-create the service unit with content that must SURVIVE the
    // refused install. A regression that wrote anyway would change the
    // contents — assert byte-for-byte equality after the run.
    std::fs::create_dir_all(paths.service_unit.parent().unwrap()).unwrap();
    let pre_existing = b"PRE-EXISTING SENTINEL CONTENT";
    std::fs::write(&paths.service_unit, pre_existing).unwrap();

    isolated_command(td.path())
        .arg("--config")
        .arg(&cfg)
        .arg("install")
        .arg("--user")
        .arg("--non-interactive")
        .assert()
        .code(78)
        .stderr(predicate::str::contains("refusing silent overwrite"))
        .stderr(predicate::str::contains("--force"));

    // The pre-existing file must NOT have been overwritten.
    let after = std::fs::read(&paths.service_unit).expect("file must still exist");
    assert_eq!(
        after, pre_existing,
        "refused install must NOT modify existing managed files",
    );
}

#[test]
fn install_user_force_overwrites_pre_existing_managed_files_and_exits_zero() {
    // Same setup as above but with `--force`. The `if !force` block
    // in cli::install::run is skipped entirely, so existing-file
    // detection does not fire and write_outputs proceeds.
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let paths = user_scope_paths(td.path());

    std::fs::create_dir_all(paths.service_unit.parent().unwrap()).unwrap();
    let pre_existing = b"PRE-EXISTING SENTINEL CONTENT";
    std::fs::write(&paths.service_unit, pre_existing).unwrap();

    isolated_command(td.path())
        .arg("--config")
        .arg(&cfg)
        .arg("install")
        .arg("--user")
        .arg("--non-interactive")
        .arg("--force")
        .assert()
        .code(0);

    // The pre-existing content must be GONE — install rewrote the file.
    let after = std::fs::read(&paths.service_unit).expect("file exists after force install");
    assert_ne!(
        after, pre_existing,
        "--force must overwrite the pre-existing sentinel content",
    );
    // The replacement content is a real systemd unit; pin a known
    // marker that render_service_unit always emits in systemd::unit
    // (Description= header).
    let after_str = String::from_utf8_lossy(&after);
    assert!(
        after_str.contains("[Unit]") && after_str.contains("[Service]"),
        "rewritten unit must be a real systemd unit; got: {}",
        after_str,
    );
}

#[test]
fn install_user_idempotent_double_run_with_force_exits_zero_both_times() {
    // Run install --force twice in a row. The first run writes fresh
    // files; the second run encounters them, but --force keeps
    // exit 0 per cli::install::run's `if !force` guard. Pin the
    // manifest still resolves to a parseable JSON after the second
    // run (a regression that left the manifest half-written would
    // break uninstall).
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let paths = user_scope_paths(td.path());

    run_user_install(td.path(), &cfg);
    // Manifest exists after first run.
    let first_manifest =
        std::fs::read_to_string(&paths.manifest).expect("manifest written by first install");
    assert!(
        first_manifest.contains("schema_version"),
        "first manifest must carry schema_version: {}",
        first_manifest,
    );

    run_user_install(td.path(), &cfg);
    let second_manifest =
        std::fs::read_to_string(&paths.manifest).expect("manifest written by second install");
    assert!(
        second_manifest.contains("schema_version"),
        "second manifest must still parse cleanly: {}",
        second_manifest,
    );
    // Both runs hash the same input bytes, so the manifest contents
    // (modulo non-deterministic content like timestamps which the
    // current Manifest schema does not carry) round-trip identically.
    assert_eq!(
        first_manifest, second_manifest,
        "idempotent re-run must produce byte-identical manifests",
    );
}

#[test]
fn install_user_force_with_pre_existing_managed_file_prints_exists_tag_in_path_preview() {
    // Pre-create the service unit so the path preview annotates it
    // [exists] rather than [new]. Pin the stdout-visible tag — a
    // regression that changed the tag spelling would not surface in
    // the overwrite test above.
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let paths = user_scope_paths(td.path());

    std::fs::create_dir_all(paths.service_unit.parent().unwrap()).unwrap();
    std::fs::write(&paths.service_unit, b"PRE-EXISTING SENTINEL CONTENT").unwrap();

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
    // The "[exists]" tag must appear in the path preview block. Even
    // a single match is enough — the manifest path is also annotated
    // with [new] (since the manifest doesn't exist on a fresh install)
    // so we cannot blindly check the [new] count.
    assert!(
        stdout.contains("[exists]"),
        "path preview must annotate pre-existing service unit with [exists]; got: {stdout}",
    );
}

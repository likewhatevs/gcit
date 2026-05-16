// Early-exit error branches in `cli::install::run` and
// `cli::uninstall::run` that fire BEFORE any filesystem side-effect:
// missing config (EX_CONFIG=78), missing manifest (EX_CONFIG=78),
// malformed config (EX_CONFIG=78), --user + local_mail rejection
// (EX_USAGE=64).

use predicates::prelude::*;
use tempfile::TempDir;

use super::common::{isolated_command, write_local_mail_config};

#[test]
fn install_user_without_existing_config_exits_config_78() {
    // `gcit install --user --non-interactive` reaches
    // `cli::install::run` which calls `config::load(config_path)`.
    // With no config present at the resolved path, load returns Err
    // and run exits with EX_CONFIG=78.
    let td = TempDir::new().unwrap();
    isolated_command(td.path())
        .arg("install")
        .arg("--user")
        .arg("--non-interactive")
        .assert()
        .code(78);
}

#[test]
fn install_system_without_existing_config_exits_config_78() {
    // --system path. The config-load failure happens BEFORE any
    // scope-specific branching, so this path mirrors the --user one.
    // Pin both so a refactor that adds an early scope check would
    // still surface the right exit code.
    //
    // Pass --config explicitly: the system-scope default
    // `/etc/gcit/config.toml` may exist on a dev box (e.g. an
    // operator who has installed gcit system-wide and is running
    // tests against the same checkout). A guaranteed-nonexistent
    // path under the tempdir is the only way to drive the
    // config-load Err arm reliably across hosts.
    let td = TempDir::new().unwrap();
    let nonexistent_config = td.path().join("nonexistent.toml");
    isolated_command(td.path())
        .arg("--config")
        .arg(&nonexistent_config)
        .arg("install")
        .arg("--system")
        .arg("--non-interactive")
        .assert()
        .code(78);
}

#[test]
fn uninstall_user_without_existing_manifest_exits_config_78() {
    // `gcit uninstall --user` reaches `cli::uninstall::run` which
    // calls `install::read_manifest(&paths.manifest)`. With no
    // manifest at the resolved XDG path, read_manifest returns Err
    // and run exits with EX_CONFIG=78. No filesystem mutation
    // because the schema/path-traversal/sha checks all run AFTER the
    // manifest read.
    let td = TempDir::new().unwrap();
    isolated_command(td.path())
        .arg("uninstall")
        .arg("--user")
        .assert()
        .code(78);
}

#[test]
fn uninstall_user_force_without_existing_manifest_still_exits_config_78() {
    // --force overrides the operator-modified-files gate but does
    // NOT bypass manifest read. With no manifest on disk, read fails
    // before --force can even be consulted.
    let td = TempDir::new().unwrap();
    isolated_command(td.path())
        .arg("uninstall")
        .arg("--user")
        .arg("--force")
        .assert()
        .code(78);
}

#[test]
fn install_user_with_malformed_config_toml_exits_config_78() {
    // `gcit install --user --config <malformed-toml>` fails inside
    // `cli::install::run` at the `config::load(config_path)` call.
    // The loader returns Err with one or more parse diagnostics;
    // `run` prints each to stderr and exits EX_CONFIG=78. The existing
    // tests only cover the missing-file case (config-load returns Err
    // with NotFound shape); this test exercises the parse-failure
    // shape, which is a different `errors` payload.
    let td = TempDir::new().unwrap();
    let bad_config = td.path().join("malformed.toml");
    // A TOML key with a missing value is a parse error rather than a
    // schema violation — drives the loader's parse-stage Err arm.
    std::fs::write(&bad_config, "this is = not valid = toml = at all\n").unwrap();
    isolated_command(td.path())
        .arg("--config")
        .arg(&bad_config)
        .arg("install")
        .arg("--user")
        .arg("--non-interactive")
        .assert()
        .code(78);
}

#[test]
fn install_user_with_local_mail_destination_rejected_with_usage_64() {
    // cli::install::run rejects --user + local_mail with a clear
    // error pointing at /var/mail's group requirement, mapping to
    // EX_USAGE=64. Pinned: stderr includes "/var/mail" and "Re-run
    // with --system" so an operator's first read of the message
    // names the fix.
    let td = TempDir::new().unwrap();
    let cfg = write_local_mail_config(td.path());
    isolated_command(td.path())
        .arg("--config")
        .arg(&cfg)
        .arg("install")
        .arg("--user")
        .arg("--non-interactive")
        .assert()
        .code(64)
        .stderr(predicate::str::contains("/var/mail"))
        .stderr(predicate::str::contains("--system"));
}

#[test]
fn uninstall_system_force_without_existing_manifest_exits_config_78() {
    // Mirrors `uninstall_user_force_without_existing_manifest_still_exits_config_78`
    // for the system scope. `gcit uninstall --system --force` reads the
    // manifest at the system-scope path (`/var/lib/gcit/.install-manifest.json`
    // per InstallPaths::System) which does not exist for any test
    // environment. read_manifest returns Err → exit 78. --force
    // overrides operator-modified-files gating but does NOT bypass
    // manifest read; without a manifest there's nothing to override.
    // The test environment runs as a non-root user, so the manifest
    // read attempt against /var/lib/gcit/.install-manifest.json
    // returns NotFound (or PermissionDenied if /var/lib is not
    // accessible) — both surface as Err and route through the same
    // EX_CONFIG=78 arm in cli::uninstall::run.
    let td = TempDir::new().unwrap();
    isolated_command(td.path())
        .arg("uninstall")
        .arg("--system")
        .arg("--force")
        .assert()
        .code(78);
}

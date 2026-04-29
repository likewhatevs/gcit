// Black-box CLI tests for `gcit install` and `gcit uninstall` argument
// parsing + early-exit error paths.
//
// These tests drive the cargo-built `gcit` binary via assert_cmd and
// pin the clap-derived argument-parser surface (`--user`/`--system`
// mutex, `--non-interactive`, `--force`) plus the early CONFIG /
// USAGE / OSERR exit branches in `cli::install::run` and
// `cli::uninstall::run` that fire before any side effect against the
// filesystem.
//
// Coverage focus is the CLI surface — the deeper install/uninstall
// pipelines (manifest writing, systemd reload, useradd) live behind
// in-module unit tests in src/cli/install.rs and src/cli/uninstall.rs
// and behind the install_*.rs sibling integration files. This file
// covers the clap-parser layer + the two early-exit branches that
// don't write any state: missing config, manifest not found.
//
// Exit codes (per src/cli/exit.rs / sysexits):
//   * 0   = OK
//   * 64  = USAGE       (EX_USAGE; clap parse error, missing arg group)
//   * 71  = OSERR       (EX_OSERR; HOME unresolvable, manifest schema
//                        mismatch, file-removal failure)
//   * 78  = CONFIG      (EX_CONFIG; config load failure on install,
//                        manifest read failure on uninstall)

use std::io::Write;
use std::path::Path;

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;

/// Minimal valid gcit config — Discord-only flow so the install path
/// stays on the DynamicUser=yes branch and never invokes useradd. Used
/// by the install/uninstall success-path + non-interactive tests where
/// we want config::load to succeed without dragging in local_mail
/// machinery.
const MINIMAL_CONFIG_TOML: &str = "[[flow]]\n\
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

/// Write `MINIMAL_CONFIG_TOML` into `<dir>/config.toml` and return the
/// full path. Caller passes the result to `--config` so the install
/// path's `config::load` succeeds against a tempdir-rooted file.
fn write_minimal_config(dir: &Path) -> std::path::PathBuf {
    let p = dir.join("config.toml");
    let mut f = std::fs::File::create(&p).expect("create minimal config");
    f.write_all(MINIMAL_CONFIG_TOML.as_bytes())
        .expect("write minimal config");
    f.sync_all().expect("sync minimal config");
    p
}

/// Drive a `gcit install` invocation against an isolated, empty
/// tempdir-rooted XDG layout. All env vars that influence path
/// resolution are pinned to the tempdir so a stray system install
/// never affects the test, and an interrupted test cannot leave
/// crumbs in the operator's real `~/.config` / `~/.local` tree.
///
/// `home` becomes `$HOME` (and is also where `XDG_CONFIG_HOME` /
/// `XDG_DATA_HOME` / `XDG_STATE_HOME` / `XDG_RUNTIME_DIR` live).
fn isolated_command(home: &Path) -> Command {
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

#[test]
fn install_help_prints_user_system_non_interactive_force_flags() {
    // The clap-derived help output must surface every flag the
    // operator can pass to `gcit install`. A refactor that drops a
    // flag's `#[arg(long)]` would be caught here.
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("install")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("--user"))
        .stdout(predicate::str::contains("--system"))
        .stdout(predicate::str::contains("--non-interactive"))
        .stdout(predicate::str::contains("--force"));
}

#[test]
fn uninstall_help_prints_user_system_force_flags() {
    // Uninstall has fewer flags than install (no walkthrough, so no
    // --non-interactive). Pin --user, --system, --force are all
    // surfaced.
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("uninstall")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("--user"))
        .stdout(predicate::str::contains("--system"))
        .stdout(predicate::str::contains("--force"));
}

#[test]
fn install_with_user_and_system_simultaneously_is_rejected_by_arg_group_mutex() {
    // The clap ArgGroup `install_scope` is `required(true)` AND its
    // members `[user, system]` are mutex-exclusive. Passing both
    // simultaneously is an ArgGroup violation; clap rejects with a
    // parse error which `bin/gcit.rs` maps to EX_USAGE=64.
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("install")
        .arg("--user")
        .arg("--system")
        .assert()
        .failure()
        .code(64)
        .stderr(predicate::str::contains("cannot be used with"));
}

#[test]
fn uninstall_with_user_and_system_simultaneously_is_rejected_by_arg_group_mutex() {
    // Same mutex semantics on the uninstall ArgGroup `uninstall_scope`.
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("uninstall")
        .arg("--user")
        .arg("--system")
        .assert()
        .failure()
        .code(64)
        .stderr(predicate::str::contains("cannot be used with"));
}

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
fn install_unknown_flag_is_rejected() {
    // A flag not in the InstallArgs struct is rejected by clap as a
    // parse error before any cli::install::run code runs. Pinned so
    // a refactor that adds a typoed flag spelling (e.g. `--users`)
    // surfaces immediately. Clap parse errors map to EX_USAGE=64.
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("install")
        .arg("--user")
        .arg("--definitely-not-a-real-flag")
        .assert()
        .failure()
        .code(64);
}

#[test]
fn uninstall_unknown_flag_is_rejected() {
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("uninstall")
        .arg("--user")
        .arg("--definitely-not-a-real-flag")
        .assert()
        .failure()
        .code(64);
}

#[test]
fn install_help_documents_user_install_path_hint() {
    // The InstallArgs --user doc comment in src/bin/gcit.rs says
    // "Install for the current user (writes under XDG paths)." Pin
    // that the doc surfaces in the help so an operator who runs
    // `gcit install --help` understands the scope semantics without
    // reading source.
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("install")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("XDG paths"));
}

#[test]
fn install_help_documents_system_install_path_hint() {
    // The InstallArgs --system doc comment in src/bin/gcit.rs says
    // "Install system-wide (writes under /etc/systemd/system + /etc/gcit)."
    // Pin that the operator-facing path surfaces in help text.
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("install")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("/etc/systemd/system"))
        .stdout(predicate::str::contains("/etc/gcit"));
}

#[test]
fn install_without_scope_flag_is_rejected_by_required_arg_group_with_usage_64() {
    // The clap ArgGroup `install_scope` (src/bin/gcit.rs:137) is
    // `required(true)`. Bare `gcit install` with no scope flag fails
    // ArgGroup validation; clap emits a parse error which
    // `bin/gcit.rs::async_main` maps to EX_USAGE=64. Pin that the
    // required-mutex semantics survive any future refactor that might
    // accidentally make the group optional.
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("install")
        .assert()
        .failure()
        .code(64)
        .stderr(predicate::str::contains("required"));
}

#[test]
fn uninstall_without_scope_flag_is_rejected_by_required_arg_group_with_usage_64() {
    // Same shape as install — UninstallArgs at src/bin/gcit.rs:154 has
    // its own `uninstall_scope` ArgGroup `required(true)`. Bare
    // `gcit uninstall` is a parse error → EX_USAGE=64.
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("uninstall")
        .assert()
        .failure()
        .code(64)
        .stderr(predicate::str::contains("required"));
}

#[test]
fn install_user_with_malformed_config_toml_exits_config_78() {
    // `gcit install --user --config <malformed-toml>` fails inside
    // `cli::install::run` at the `config::load(config_path)` call
    // (src/cli/install.rs:120-128). The loader returns Err with one or
    // more parse diagnostics; `run` prints each to stderr and exits
    // EX_CONFIG=78. The existing tests only cover the missing-file case
    // (config-load returns Err with NotFound shape); this test exercises
    // the parse-failure shape, which is a different `errors` payload.
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
fn install_user_non_interactive_with_valid_config_writes_files_and_exits_zero() {
    // End-to-end success path. `gcit install --user --non-interactive
    // --force` against a Discord-only minimal config:
    //   * config::load succeeds (line 120-128)
    //   * has_local_mail=false, so the --user + local_mail rejection
    //     at line 139-147 does NOT fire
    //   * has_local_mail=false, so ensure_static_user is NOT called
    //     (line 242-252) — no useradd, no mail-group requirement
    //   * --non-interactive skips the [y/N] prompt (line 218-235)
    //   * --force overrides any pre-existing-file refusal (line 200-216);
    //     a fresh tempdir has nothing pre-existing so it doesn't matter
    //     here, but we pass it for robustness against parallel test runs
    //   * write_outputs writes the three managed files into the
    //     tempdir-rooted XDG layout (resolved at install.rs:156 via
    //     install_paths) and the manifest under XDG_STATE_HOME
    //   * trigger_daemon_reload on the user session bus may fail in CI
    //     (no session bus); install.rs:284-289 only emits a `warning:`
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
    // src/systemd/unit.rs:54-72 with tempdir-rooted XDG.
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
    // and reads stdin (src/cli/install.rs:218-228). assert_cmd's
    // default `.assert()` provides a closed stdin (EOF on first read);
    // `io::stdin().read_line(&mut answer)` returns Ok(0), leaving
    // `answer` empty. The empty string does not match
    // `"y"|"Y"|"yes"|"YES"|"Yes"` → "install cancelled; nothing written."
    // → exit::OK. Pin that the interactive cancel arm at line 230-234
    // exits cleanly without writing files (operator changing their
    // mind is not a failure).
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
fn uninstall_system_force_without_existing_manifest_exits_config_78() {
    // Mirrors `uninstall_user_force_without_existing_manifest_still_exits_config_78`
    // for the system scope. `gcit uninstall --system --force` reads the
    // manifest at the system-scope path (`/var/lib/gcit/.install-manifest.json`
    // per InstallPaths::System at src/systemd/unit.rs:79) which does not
    // exist for any test environment. read_manifest returns Err → exit 78.
    // --force overrides operator-modified-files gating but does NOT
    // bypass manifest read; without a manifest there's nothing to
    // override. The test environment runs as a non-root user, so the
    // manifest read attempt against /var/lib/gcit/.install-manifest.json
    // returns NotFound (or PermissionDenied if /var/lib is not
    // accessible) — both surface as Err and route through the same
    // EX_CONFIG=78 arm at src/cli/uninstall.rs:46-53.
    let td = TempDir::new().unwrap();
    isolated_command(td.path())
        .arg("uninstall")
        .arg("--system")
        .arg("--force")
        .assert()
        .code(78);
}

// ---------------------------------------------------------------------
// Helpers for the deeper install/uninstall tests below.
// ---------------------------------------------------------------------

/// Resolve the user-scope managed paths the install wizard writes to
/// under `home_dir`. Mirrors `install_paths(InstallScope::User, &home)`
/// at src/systemd/unit.rs:54-72 with $XDG_* envs from `isolated_command`.
fn user_scope_paths(home_dir: &Path) -> UserScopePaths {
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

struct UserScopePaths {
    service_unit: std::path::PathBuf,
    socket_unit: std::path::PathBuf,
    config: std::path::PathBuf,
    credentials_dir: std::path::PathBuf,
    manifest: std::path::PathBuf,
}

/// Run a fresh `gcit install --user --non-interactive --force` against
/// the supplied isolated home dir + config path. Used by the uninstall
/// happy-path tests so each one starts from a known-installed state.
/// Asserts exit 0 so a regression in the install path surfaces here
/// rather than masking as an uninstall failure.
fn run_user_install(home_dir: &Path, config_path: &Path) {
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

// ---------------------------------------------------------------------
// install --user + local_mail rejection arm.
// src/cli/install.rs:139-147 — --user scope plus a local_mail
// destination is rejected up-front because /var/mail/<user> requires
// the static `mail` group and the per-user systemd manager cannot
// useradd into it. Pinned at this level so a regression that drops
// the early-return surfaces.
// ---------------------------------------------------------------------

const LOCAL_MAIL_CONFIG_TOML: &str = "[[flow]]\n\
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

fn write_local_mail_config(dir: &Path) -> std::path::PathBuf {
    let p = dir.join("config.toml");
    std::fs::write(&p, LOCAL_MAIL_CONFIG_TOML).expect("write local_mail config");
    p
}

#[test]
fn install_user_with_local_mail_destination_rejected_with_usage_64() {
    // src/cli/install.rs:139-147 rejects --user + local_mail with a
    // clear error pointing at /var/mail's group requirement, mapping
    // to EX_USAGE=64. Pinned: stderr includes "/var/mail" and
    // "Re-run with --system" so an operator's first read of the
    // message names the fix.
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

// ---------------------------------------------------------------------
// install refuse-silent-overwrite arm.
// src/cli/install.rs:200-216 — without `--force`, an existing managed
// file at any output path causes an early exit 78 with the file
// listed in stderr. Pinned: exit code AND the canonical "refusing
// silent overwrite" wording.
// ---------------------------------------------------------------------

#[test]
fn install_user_refuses_silent_overwrite_without_force_exits_config_78() {
    // Pre-create one of the managed paths (the service unit) under the
    // tempdir-rooted XDG layout BEFORE running install. With no --force,
    // the existing-file detection at install.rs:200-216 fires and the
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
    // Same setup as above but with `--force`. Per src/cli/install.rs:200,
    // the `if !force` block is skipped entirely, so existing-file
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
    // marker that render_service_unit always emits per
    // src/systemd/unit.rs (Description= header).
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
    // exit 0 per src/cli/install.rs:200's `if !force` guard. Pin
    // the manifest still resolves to a parseable JSON after the
    // second run (a regression that left the manifest half-written
    // would break uninstall).
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

// ---------------------------------------------------------------------
// install credential walkthrough "✓ already configured" short-circuit.
// src/cli/install.rs:383-397 — when a credential file exists at the
// canonical path with mode bitmask `mode & 0o077 == 0` and is owned
// by the invoking euid OR root, the wizard prints a one-line
// confirmation instead of the full instructions.
// ---------------------------------------------------------------------

#[test]
fn install_user_with_pre_configured_credential_file_emits_check_marker_in_walkthrough() {
    use std::os::unix::fs::PermissionsExt;
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let paths = user_scope_paths(td.path());

    // Place a 0600 credential file at the destination the walkthrough
    // would print. The minimal config references credential_id values
    // "github_pat" and "discord_webhook" (per write_minimal_config /
    // MINIMAL_CONFIG_TOML at the top of this file).
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
    // ✓ marker per src/cli/install.rs:388-394. A regression that
    // dropped the short-circuit would re-print the full "obtain at:" /
    // "chmod" instructions instead.
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

// ---------------------------------------------------------------------
// uninstall happy-path: run install + uninstall, assert every
// managed file is gone, the manifest is gone, and the state directory
// is preserved (state directory intentionally kept for re-install).
// ---------------------------------------------------------------------

#[test]
fn uninstall_user_after_successful_install_removes_all_managed_files_and_exits_zero() {
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let paths = user_scope_paths(td.path());

    run_user_install(td.path(), &cfg);
    // Every managed file is on disk after install.
    assert!(paths.service_unit.exists());
    assert!(paths.socket_unit.exists());
    assert!(paths.config.exists());
    assert!(paths.manifest.exists());

    isolated_command(td.path())
        .arg("uninstall")
        .arg("--user")
        .assert()
        .code(0)
        .stdout(predicate::str::contains("removed"));

    // Every managed file must be gone after uninstall.
    assert!(
        !paths.service_unit.exists(),
        "uninstall must remove the service unit",
    );
    assert!(
        !paths.socket_unit.exists(),
        "uninstall must remove the socket unit",
    );
    assert!(
        !paths.config.exists(),
        "uninstall must remove the config copy",
    );
    assert!(
        !paths.manifest.exists(),
        "uninstall must remove the manifest itself last",
    );
    // State directory is preserved intentionally: the parent dir
    // still exists even after the manifest file is removed.
    assert!(
        paths.manifest.parent().expect("manifest parent").exists(),
        "uninstall must preserve the state directory parent",
    );
}

#[test]
fn uninstall_user_after_install_with_modified_file_without_force_refuses_with_oserr_71() {
    // src/cli/uninstall.rs:109-120 refuses to remove an
    // operator-modified file without --force, exiting 71 (EX_OSERR).
    // The manifest sha mismatch surfaces in stderr with both the
    // recorded and on-disk shas.
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let paths = user_scope_paths(td.path());

    run_user_install(td.path(), &cfg);

    // Tamper with the service unit so its on-disk sha drifts from the
    // manifest's recorded sha.
    let original = std::fs::read(&paths.service_unit).expect("service unit exists");
    let tampered = [original.as_slice(), b"\n# operator edit\n"].concat();
    std::fs::write(&paths.service_unit, &tampered).expect("write tampered file");

    isolated_command(td.path())
        .arg("uninstall")
        .arg("--user")
        .assert()
        .code(71)
        .stderr(predicate::str::contains("operator-modified files"))
        .stderr(predicate::str::contains("--force"));

    // The tampered file must still be on disk — uninstall refused to
    // remove it.
    assert!(
        paths.service_unit.exists(),
        "refused uninstall must NOT remove the tampered file",
    );
    // The manifest must also still exist for the operator's --force
    // retry.
    assert!(
        paths.manifest.exists(),
        "refused uninstall must NOT remove the manifest before completion",
    );
}

#[test]
fn uninstall_user_after_install_with_modified_file_force_overrides_and_exits_zero() {
    // Same setup as above but with --force. Per the comment at
    // src/cli/uninstall.rs:111, --force overrides the sha-mismatch
    // gate; uninstall completes and removes the tampered file.
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let paths = user_scope_paths(td.path());

    run_user_install(td.path(), &cfg);

    let original = std::fs::read(&paths.service_unit).expect("service unit exists");
    let tampered = [original.as_slice(), b"\n# operator edit\n"].concat();
    std::fs::write(&paths.service_unit, &tampered).expect("write tampered file");

    isolated_command(td.path())
        .arg("uninstall")
        .arg("--user")
        .arg("--force")
        .assert()
        .code(0);

    assert!(
        !paths.service_unit.exists(),
        "--force uninstall must remove even tampered files",
    );
    assert!(!paths.manifest.exists());
}

#[test]
fn uninstall_user_with_schema_version_mismatch_exits_oserr_71() {
    // src/cli/uninstall.rs:55-60 rejects a manifest whose
    // schema_version differs from MANIFEST_SCHEMA_VERSION (currently 1
    // per src/cli/install.rs:55). EX_OSERR=71 distinguishes this from
    // the manifest-not-found case (EX_CONFIG=78).
    let td = TempDir::new().unwrap();
    let paths = user_scope_paths(td.path());

    // Hand-write a manifest with schema_version=999 — guaranteed not
    // to match. Empty `files` keeps the path-traversal scan trivial.
    std::fs::create_dir_all(paths.manifest.parent().unwrap()).unwrap();
    let bad_manifest = r#"{"schema_version":999,"files":[]}"#;
    std::fs::write(&paths.manifest, bad_manifest).unwrap();

    isolated_command(td.path())
        .arg("uninstall")
        .arg("--user")
        .assert()
        .code(71)
        .stderr(predicate::str::contains("schema_version"))
        .stderr(predicate::str::contains("999"));
}

#[test]
fn uninstall_user_with_path_outside_install_roots_exits_oserr_71() {
    // src/cli/uninstall.rs:73-82 refuses any manifest entry that
    // canonicalizes outside the expected_roots derived from
    // install_paths. Hand-write a manifest pointing at a tempdir-rooted
    // file OUTSIDE the user-scope managed dirs (e.g. directly under
    // $HOME) so canonicalize succeeds + the prefix check fails.
    use std::os::unix::fs::PermissionsExt;
    let td = TempDir::new().unwrap();
    let paths = user_scope_paths(td.path());

    // Place a file the manifest will reference, somewhere INSIDE the
    // tempdir but OUTSIDE every user-scope managed dir
    // ($HOME/<not-managed>/sentinel.txt).
    let outside = td.path().join("sentinel.txt");
    std::fs::write(&outside, b"sentinel content").unwrap();

    // SHA-256 of "sentinel content" so the sha gate doesn't fire
    // first. canonicalize on `outside` resolves; the resulting path
    // does not start with any of the expected_roots — uninstall.rs's
    // for loop in validate_manifest_path runs through every root and
    // returns Err.
    let sha = sha256_hex(b"sentinel content");
    std::fs::create_dir_all(paths.manifest.parent().unwrap()).unwrap();
    let manifest_json = format!(
        r#"{{"schema_version":1,"files":[{{"path":{:?},"sha256":{:?},"mode":420}}]}}"#,
        outside.to_string_lossy(),
        sha,
    );
    std::fs::write(&paths.manifest, manifest_json).unwrap();
    // Permissions don't matter for path validation but stay readable.
    std::fs::set_permissions(&paths.manifest, std::fs::Permissions::from_mode(0o600)).unwrap();

    isolated_command(td.path())
        .arg("uninstall")
        .arg("--user")
        .assert()
        .code(71)
        .stderr(predicate::str::contains("manifest entry rejected"))
        .stderr(predicate::str::contains("outside the install directories"));

    // Path traversal defense MUST NOT remove the offending file even
    // though `--force` was not passed.
    assert!(
        outside.exists(),
        "path-traversal rejection must NOT remove anything",
    );
}

/// SHA-256 hex of `bytes`, lower-case. Uses the sha2 crate that the
/// production install code already depends on (see src/cli/install.rs).
fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    format!("{:x}", h.finalize())
}

// ---------------------------------------------------------------------
// Source-side credential walkthrough emits the SourceFetch hint when
// no other credential reference upgrades the kind.
// src/cli/install.rs:413-415 — CredentialKindHint::SourceFetch prints
// "kind: source-side fetch credential". This arm fires only when
// collect_credential_uses leaves a SourceFetch reference unupgraded
// (no Action/Discord reference for the same id, per validate.rs:603-609).
// ---------------------------------------------------------------------

const SOURCE_FETCH_CREDENTIAL_CONFIG_TOML: &str = "[[flow]]\n\
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

#[test]
fn install_user_walkthrough_surfaces_source_fetch_kind_hint_for_source_only_credential() {
    // The walkthrough at src/cli/install.rs:399-419 prints a
    // per-credential block; for a credential id referenced ONLY from
    // [flow.source] (and not also from [flow.action] /
    // [[flow.destination]]) the kind hint stays at SourceFetch and
    // the line "kind: source-side fetch credential" surfaces.
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
    // header (src/cli/install.rs:399).
    assert!(
        stdout.contains("source_only_cred"),
        "stdout must surface the credential id; got: {stdout}",
    );
}

// ---------------------------------------------------------------------
// Path preview "[exists]" tag arm at src/cli/install.rs:521. Existing
// install_user_force_overwrites_pre_existing_managed_files_and_exits_zero
// covers the [exists] -> overwrite pipeline; this test pins the
// stdout-visible "[exists]" tag specifically (a regression that
// changed the tag spelling would not surface there).
// ---------------------------------------------------------------------

#[test]
fn install_user_force_with_pre_existing_managed_file_prints_exists_tag_in_path_preview() {
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let paths = user_scope_paths(td.path());

    // Pre-create the service unit so the path preview annotates it
    // [exists] rather than [new].
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

// ---------------------------------------------------------------------
// Uninstall when a managed file was already deleted by the operator.
// src/cli/uninstall.rs:88-95 + 126-130 — "File already gone — nothing
// to remove. Not an error". The remaining managed files are removed
// normally, the manifest is removed last, and exit is 0.
// ---------------------------------------------------------------------

#[test]
fn uninstall_user_with_pre_deleted_managed_file_succeeds_and_removes_remaining() {
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let paths = user_scope_paths(td.path());

    run_user_install(td.path(), &cfg);
    // Operator manually removes the socket unit BEFORE running
    // uninstall. The manifest still references it; the sha-mismatch
    // gate at line 88-107 sees `path.exists() == false` and skips
    // the file via `continue`, so no "operator-modified" error fires.
    assert!(paths.socket_unit.exists());
    std::fs::remove_file(&paths.socket_unit).expect("remove pre-uninstall");
    assert!(!paths.socket_unit.exists(), "pre-deletion staged");

    isolated_command(td.path())
        .arg("uninstall")
        .arg("--user")
        .assert()
        .code(0);

    // Remaining managed files are gone too — the loop at uninstall.rs:
    // 126-135 removes every entry whose path still exists.
    assert!(
        !paths.service_unit.exists(),
        "uninstall must remove the still-present service unit",
    );
    assert!(
        !paths.config.exists(),
        "uninstall must remove the config copy",
    );
    assert!(
        !paths.manifest.exists(),
        "uninstall must remove the manifest itself",
    );
}

// ---------------------------------------------------------------------
// Uninstall when a managed file has been replaced by a symlink. The
// symlink-defense arm at src/cli/uninstall.rs:206-216 inside
// validate_manifest_path rejects any manifest entry where
// `symlink_metadata` reports a symlink, EVEN when --force is passed.
// The recorded sha256 was over the original file content — following
// the symlink would let an attacker substitute arbitrary content for
// the manifest's intended target.
// ---------------------------------------------------------------------

#[test]
fn uninstall_user_with_managed_file_replaced_by_symlink_rejected_with_oserr_71() {
    use std::os::unix::fs::symlink;
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let paths = user_scope_paths(td.path());

    run_user_install(td.path(), &cfg);
    // Replace the service unit with a symlink. The manifest still
    // points at the path. validate_manifest_path's
    // symlink_metadata branch fires and emits the
    // "manifest entry rejected: ...: symlink ..." stderr.
    assert!(paths.service_unit.exists());
    std::fs::remove_file(&paths.service_unit).unwrap();
    let target = td.path().join("symlink-target.txt");
    std::fs::write(&target, b"attacker-controlled content").unwrap();
    symlink(&target, &paths.service_unit).expect("create symlink");

    // --force is OFF so a sha mismatch would fire ANYWAY; the
    // symlink defense fires FIRST per the order at uninstall.rs:73-82
    // (path validation runs before sha verification).
    isolated_command(td.path())
        .arg("uninstall")
        .arg("--user")
        .assert()
        .code(71)
        .stderr(predicate::str::contains("manifest entry rejected"))
        .stderr(predicate::str::contains("symlink"));

    // Symlink defense MUST NOT remove the symlink itself or the
    // attacker-controlled target.
    assert!(
        paths.service_unit.exists(),
        "symlink-rejection must not delete the symlink",
    );
    assert!(
        target.exists(),
        "symlink-rejection must not delete the symlink target",
    );
    // The manifest must still be present so a follow-up uninstall
    // (e.g. after the operator restores the file) can complete.
    assert!(
        paths.manifest.exists(),
        "symlink-rejection must not remove the manifest before completion",
    );
}

// ---------------------------------------------------------------------
// Uninstall with manifest entries pointing INSIDE expected_roots but
// where the file is missing. The path-validation branch at
// src/cli/uninstall.rs:226 (`Err(_) if !path.exists() => return Ok(())`)
// returns Ok for paths that don't canonicalize but also don't exist.
// The sha-verify loop at line 88-95 then skips the entry via
// `if !entry.path.exists() { continue; }`. Net behavior: missing
// files inside expected_roots are no-ops and the uninstall succeeds.
// ---------------------------------------------------------------------

#[test]
fn uninstall_user_with_manifest_entries_inside_roots_but_missing_succeeds() {
    use std::os::unix::fs::PermissionsExt;
    let td = TempDir::new().unwrap();
    let paths = user_scope_paths(td.path());

    // Build a manifest by hand. Every entry's path canonicalizes to
    // somewhere INSIDE the user-scope managed dirs (units_dir, config
    // dir), but the files themselves do not exist on disk. The
    // validate_manifest_path arm at uninstall.rs:226 returns Ok for
    // missing paths inside the roots (they can't canonicalize but
    // they don't exist so removal will no-op). The sha-verify loop at
    // line 88-95 skips them via `!entry.path.exists()`. The remove
    // loop at 126-135 also skips them via the same predicate.
    std::fs::create_dir_all(paths.manifest.parent().unwrap()).unwrap();
    // We need expected_roots to canonicalize successfully. Pre-create
    // the parent dirs of the managed files so canonicalize on them
    // returns Ok inside expected_roots() at uninstall.rs:189-197.
    std::fs::create_dir_all(paths.service_unit.parent().unwrap()).unwrap();
    std::fs::create_dir_all(paths.config.parent().unwrap()).unwrap();

    let mock_sha = sha256_hex(b"ignored");
    let manifest_json = format!(
        r#"{{"schema_version":1,"files":[{{"path":{:?},"sha256":{:?},"mode":420}},{{"path":{:?},"sha256":{:?},"mode":420}}]}}"#,
        paths.service_unit.to_string_lossy(),
        mock_sha,
        paths.config.to_string_lossy(),
        mock_sha,
    );
    std::fs::write(&paths.manifest, manifest_json).unwrap();
    std::fs::set_permissions(&paths.manifest, std::fs::Permissions::from_mode(0o600)).unwrap();

    isolated_command(td.path())
        .arg("uninstall")
        .arg("--user")
        .assert()
        .code(0);

    // Manifest itself is removed at the end (uninstall.rs:147-157).
    assert!(
        !paths.manifest.exists(),
        "uninstall must remove the manifest after a missing-files run",
    );
}

// ---------------------------------------------------------------------
// Install --user --non-interactive against a config whose [flow.source]
// carries a credential_id. Pin: install proceeds, walkthrough mentions
// the source credential id in the printed walkthrough block, and the
// post-install state matches the no-source-credential path (manifest
// + units present). The source-credential code path differs from the
// destination-only path because walk_credentials emits a SourceFetch
// reference (config/mod.rs:67-72) before any other reference; this test
// pins that the install completes cleanly when that branch fires.
// ---------------------------------------------------------------------

#[test]
fn install_user_with_source_credential_id_completes_and_walkthrough_includes_id() {
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

// Clap-derived help output, flag-mutex enforcement, and unknown-flag
// rejection. Black-box CLI tests against the cargo-built `gcit`
// binary so the parser surface is exercised end-to-end.

use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn install_help_prints_every_install_flag() {
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
        .stdout(predicate::str::contains("--force"))
        .stdout(predicate::str::contains("--dry-run"));
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
    // The clap ArgGroup `install_scope` on InstallArgs is
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
    // Same shape as install — UninstallArgs has its own
    // `uninstall_scope` ArgGroup `required(true)`. Bare `gcit
    // uninstall` is a parse error → EX_USAGE=64.
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("uninstall")
        .assert()
        .failure()
        .code(64)
        .stderr(predicate::str::contains("required"));
}

#[test]
fn install_dry_run_help_text_present() {
    // The clap-derived help output for `gcit install` must surface
    // the `--dry-run` flag with its first-line description so an
    // operator can discover the feature without reading source.
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("install")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("--dry-run"))
        .stdout(predicate::str::contains("Render the systemd service unit"));
}

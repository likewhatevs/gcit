// Black-box CLI tests for clap-handled paths that don't reach the
// daemon: `gcit --help`, `gcit <subcommand> --help`, `gcit` with no
// subcommand, and the missing/unreadable config error path on `gcit
// check`. These exercise the binary's argument parser + the early
// config-load branch in `gcit check` without needing a control socket.

use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn help_top_level_lists_all_subcommands() {
    // `gcit --help` is clap-derived. Pin that the help output names
    // every subcommand the operator can reach.
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("run"))
        .stdout(predicate::str::contains("check"))
        .stdout(predicate::str::contains("install"))
        .stdout(predicate::str::contains("uninstall"))
        .stdout(predicate::str::contains("reload"))
        .stdout(predicate::str::contains("status"))
        .stdout(predicate::str::contains("trigger"))
        .stdout(predicate::str::contains("validate-template"))
        .stdout(predicate::str::contains("completions"));
}

#[test]
fn help_short_form_succeeds_too() {
    // Both `-h` and `--help` succeed; the short form prints a
    // clap-default summary, the long form prints the full doc.
    // Pin only that the short form exits 0 and produces non-empty
    // stdout.
    let out = Command::cargo_bin("gcit")
        .unwrap()
        .arg("-h")
        .output()
        .unwrap();
    assert!(out.status.success(), "exit code: {:?}", out.status);
    assert!(!out.stdout.is_empty(), "stdout empty");
}

#[test]
fn no_subcommand_exits_usage_with_no_subcommand_message() {
    // Bare `gcit` (no subcommand) is intercepted by `bin/gcit.rs`'s
    // no-subcommand handler (clap's default would print the help
    // message and exit 0; gcit overrides this to refuse with
    // EX_USAGE=64 + a hint pointing at `--help`). Pin the override
    // is in place — a clap upgrade or a refactor that drops the
    // override would otherwise silently change the exit code.
    Command::cargo_bin("gcit")
        .unwrap()
        .assert()
        .failure()
        .stderr(predicate::str::contains("no subcommand"))
        .stderr(predicate::str::contains("--help"));
}

#[test]
fn unknown_subcommand_exits_non_zero() {
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("definitely-not-a-subcommand")
        .assert()
        .failure();
}

#[test]
fn check_help_documents_config_flag() {
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("check")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("--config"));
}

#[test]
fn install_without_scope_exits_non_zero() {
    // --user and --system are required (ArgGroup `required(true)`);
    // omitting both is a parse error, not a TEMPFAIL.
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("install")
        .assert()
        .failure();
}

#[test]
fn uninstall_without_scope_exits_non_zero() {
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("uninstall")
        .assert()
        .failure();
}

#[test]
fn check_with_nonexistent_config_path_exits_non_zero() {
    // Passing a config path that does not exist. `gcit check`
    // surfaces a config-load error and exits 78 (EX_CONFIG).
    // The path is under /tmp so test isolation is clean.
    Command::cargo_bin("gcit")
        .unwrap()
        .env_remove("CREDENTIALS_DIRECTORY")
        .arg("--config")
        .arg("/tmp/gcit-cli-help-test-definitely-does-not-exist-9f3a2b.toml")
        .arg("check")
        .assert()
        .failure();
}

#[test]
fn trigger_with_empty_flow_exits_usage() {
    // `gcit trigger ""` reaches `cli::trigger::run` with an empty
    // FLOW, which is rejected with EX_USAGE=64 BEFORE any control-
    // socket connect attempt. Pins the early-validation branch.
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("trigger")
        .arg("")
        .assert()
        .code(64);
}

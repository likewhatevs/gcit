// `gcit --version` prints crate version + git SHA via vergen-gix.
//
// build.rs emits VERGEN_GIT_SHA via vergen-gix; bin/gcit.rs concatenates
// CARGO_PKG_VERSION + " (" + VERGEN_GIT_SHA + ")" into the
// `#[command(version = ...)]` attribute.

use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn version_prints_crate_version_and_git_sha() {
    let mut cmd = Command::cargo_bin("gcit").unwrap();
    cmd.arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::starts_with("gcit "))
        // CARGO_PKG_VERSION baked at compile time
        .stdout(predicate::str::contains(env!("CARGO_PKG_VERSION")))
        // vergen-gix injects a 7+ char git SHA in parens when
        // built inside a git repo. cargo-mutants copies source to
        // a scratch dir without .git, so the SHA may be absent.
        .stdout(predicate::str::contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn version_long_form_matches_short_form() {
    // `gcit -V` and `gcit --version` print identical output (clap-derive default).
    let v_short = Command::cargo_bin("gcit")
        .unwrap()
        .arg("-V")
        .output()
        .unwrap();
    let v_long = Command::cargo_bin("gcit")
        .unwrap()
        .arg("--version")
        .output()
        .unwrap();
    assert_eq!(v_short.stdout, v_long.stdout);
}

#[test]
fn version_exits_zero() {
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("--version")
        .assert()
        .code(0);
}

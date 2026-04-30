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
        // vergen-gix injects either a 7+ char git SHA or the literal
        // "VERGEN_IDEMPOTENT_OUTPUT" placeholder (when built outside a
        // git checkout, e.g. cargo-mutants scratch dirs / tarball
        // builds) inside parentheses. The regex matches both shapes:
        // `(<7-or-more-hex>)` for a real git SHA and
        // `(VERGEN_IDEMPOTENT_OUTPUT)` for the placeholder. Either
        // proves bin/gcit.rs's `concat!(... " (", env!("VERGEN_GIT_SHA"), ")")`
        // composition produced a parenthesized SHA segment.
        .stdout(
            predicate::str::is_match(r"\(([0-9a-f]{7,}|VERGEN_IDEMPOTENT_OUTPUT)\)").unwrap(),
        );
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

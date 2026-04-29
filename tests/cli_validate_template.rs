// `gcit validate-template <FILE>` — compile template against sample
// data, exit 0 / EX_DATAERR=65.
//
// These tests drive the binary via `assert_cmd::Command::cargo_bin`
// because the subcommand is wired in `bin/gcit.rs` and exercises the
// full clap argument-parsing path. The library-side behaviour is
// covered by `cli/validate_template.rs::run` directly invoking
// `notify::strict_handlebars()` against `config::validate::probe_context()`.

use std::io::Write;

use assert_cmd::Command;
use predicates::prelude::*;

/// EX_DATAERR per src/cli/exit.rs::DATAERR — kept as a literal here
/// so a drift in the constant trips the test rather than silently
/// passing.
const EX_DATAERR: i32 = 65;

fn write_template_file(contents: &str) -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().expect("tempfile");
    f.write_all(contents.as_bytes()).expect("write");
    f.flush().expect("flush");
    f
}

#[test]
fn renders_namespaced_template_to_stdout_and_exits_zero() {
    // Probe context (config::validate::probe_context) supplies
    // type-faithful stub values: flow.name is a real flow-id-shaped
    // string and source.sha is 40 hex chars. A leaf-only substitution
    // renders those stubs verbatim to stdout and exits 0.
    let f = write_template_file("flow:{{flow.name}};sha:{{source.sha}}");
    let expected_sha = "a".repeat(40);
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("validate-template")
        .arg(f.path())
        .assert()
        .success()
        .stdout(format!("flow:linux-mainline-ci;sha:{expected_sha}"));
}

#[test]
fn rejects_template_with_undefined_namespaced_leaf_exit_65() {
    // A typo in a documented namespace (e.g. `{{flow.naem}}` instead
    // of `{{flow.name}}`) gets through the bare-name AST check —
    // `flow.naem` is dotted — but fails strict-mode rendering
    // because `flow.naem` is not in the probe context. The failure
    // exits EX_DATAERR=65 with the underlying render error on stderr.
    let f = write_template_file("hello {{flow.naem}}");
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("validate-template")
        .arg(f.path())
        .assert()
        .code(EX_DATAERR)
        .stderr(predicate::str::contains("failed to render"));
}

#[test]
fn rejects_template_with_block_helper_exit_65() {
    // notify::strict_handlebars() deregisters every block helper
    // (each, with, if, unless, lookup, log, raw). A template that
    // references one of them must fail compile/render and exit 65.
    let f = write_template_file("{{#if x}}y{{/if}}");
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("validate-template")
        .arg(f.path())
        .assert()
        .code(EX_DATAERR);
}

#[test]
fn rejects_missing_file_exit_65() {
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("validate-template")
        .arg("/nonexistent/gcit/template.path")
        .assert()
        .code(EX_DATAERR)
        .stderr(predicate::str::contains("cannot read"));
}

#[test]
fn empty_template_is_valid_and_renders_empty() {
    let f = write_template_file("");
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("validate-template")
        .arg(f.path())
        .assert()
        .success()
        .stdout("");
}

#[test]
fn validate_template_rejects_bare_name_exit_65() {
    // `gcit validate-template` must agree with `gcit check` on what
    // templates are accepted. `{{flow}}` is a bare-name reference (no
    // `.field`) which the daemon rejects at config load via
    // `config::validate::find_bare_name`. validate-template runs the
    // same AST check before rendering so operators don't get a green
    // light here only to fail later in `gcit check`.
    let f = write_template_file("{{flow}}");
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("validate-template")
        .arg(f.path())
        .assert()
        .code(EX_DATAERR)
        .stderr(predicate::str::contains("bare name"));
}

#[test]
fn validate_template_rejects_bare_gcit_run_id_exit_65() {
    // `{{gcit_run_id}}` (bare, not `{{gcit.run_id}}`) is a rejected
    // shorthand.
    let f = write_template_file("{{gcit_run_id}}");
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("validate-template")
        .arg(f.path())
        .assert()
        .code(EX_DATAERR)
        .stderr(predicate::str::contains("bare name"));
}

#[test]
fn validate_template_with_kind_discord_emits_surface_header_to_stderr() {
    // The `--kind` flag is wired in src/cli/validate_template.rs:85-92:
    // when set, the run prints a one-line stderr header "validating
    // <path> against the <surface> surface" before the compile/render
    // pipeline. clap's kebab-case rename in src/cli/validate_template.rs:45
    // maps `--kind discord` to `Kind::Discord` whose label is "discord"
    // (line 58-61). Pin that the surface label appears verbatim in
    // stderr so operators reading CI logs see which surface they
    // validated. The template itself is a passing one so the test
    // isolates the header behaviour from the render result.
    let f = write_template_file("flow:{{flow.name}}");
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("validate-template")
        .arg(f.path())
        .arg("--kind")
        .arg("discord")
        .assert()
        .success()
        .stderr(predicate::str::contains("against the discord surface"));
}

#[test]
fn validate_template_with_kind_local_mail_emits_surface_header_to_stderr() {
    // Mirrors the discord case for the local-mail surface. The
    // kebab-case rename at src/cli/validate_template.rs:45 maps
    // `--kind local-mail` (CLI form) to `Kind::LocalMail` (Rust form);
    // the label() at line 60 returns "local_mail" (snake_case — chosen
    // to match the destination kind in config.toml so the header line
    // operators see in CI matches the kind they typed in their config).
    let f = write_template_file("flow:{{flow.name}}");
    Command::cargo_bin("gcit")
        .unwrap()
        .arg("validate-template")
        .arg(f.path())
        .arg("--kind")
        .arg("local-mail")
        .assert()
        .success()
        .stderr(predicate::str::contains("against the local_mail surface"));
}

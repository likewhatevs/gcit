// Black-box CLI tests for `--log-filter` validation handled by
// `gcit::log::init`. The binary's `init_log` closure (src/bin/gcit.rs)
// calls `log::init(filter, foreground)` for every subcommand; an
// invalid filter string is mapped to EX_CONFIG=78. Pinning these
// paths covers the EnvFilter::try_new error arm at src/log/mod.rs:48-49
// without needing to hold the global tracing subscriber state across
// in-process tests (try_init only succeeds once per process — a
// subprocess-per-case via assert_cmd is the only correct way to
// exercise `init` repeatedly).

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;

/// EX_CONFIG per src/cli/exit.rs::CONFIG. Kept as a literal `i32` so
/// it slots into `assert_cmd::Assert::code` (which expects an `i32`)
/// without an explicit cast at every callsite.
const CONFIG: i32 = 78;

/// Minimal valid TOML so `gcit check` reaches log init before failing
/// on anything else. The filter is consumed before config-load (see
/// src/bin/gcit.rs init_log invocation pattern), so a filter rejection
/// must surface as exit 78 even with a perfectly-valid config.
const FULL_CONFIG: &str = r#"
[[flow]]
name = "log-filter-test"
[flow.source]
url = "https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "owner/repo"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "github_pat"
[[flow.destination]]
kind = "discord_webhook"
credential_id = "discord_webhook"
"#;

fn write_config(td: &TempDir) -> std::path::PathBuf {
    let path = td.path().join("gcit.toml");
    std::fs::write(&path, FULL_CONFIG).expect("write minimal config");
    path
}

#[test]
fn invalid_log_filter_leading_equals_exits_config_78() {
    // src/log/mod.rs:48-49 calls EnvFilter::try_new(raw) and maps any
    // error to InvalidInput. The binary's init_log closure prints
    // "gcit: log init failed: ..." and returns ExitCode::from(CONFIG).
    // Per tracing-subscriber's directive grammar (verified at
    // tracing-subscriber-0.3.23/src/filter/env/directive.rs:146), the
    // first char must be `[`, `-`, `:`, `_`, or alphanumeric — `=` is
    // NOT in that set, so a directive starting with `=` fails parse
    // with ParseError. Pin that the resulting exit code is 78 with
    // the canonical operator-facing "log init failed" lead.
    let td = TempDir::new().unwrap();
    let cfg = write_config(&td);
    Command::cargo_bin("gcit")
        .unwrap()
        .env_remove("CREDENTIALS_DIRECTORY")
        .arg("--log-filter")
        .arg("=invalid")
        .arg("--config")
        .arg(&cfg)
        .arg("check")
        .assert()
        .code(CONFIG)
        .stderr(predicate::str::contains("log init failed"));
}

#[test]
fn valid_log_filter_string_does_not_block_check_subcommand() {
    // Sentinel direction: a valid `--log-filter` must NOT cause the
    // log-init early-exit. Drive a known-good filter ("warn") and
    // verify the subcommand reaches the post-init logic. We use a
    // bogus config path so check exits 78 from config-load, NOT from
    // log-init — distinguishing the two error paths via the stderr
    // marker. A regression that incorrectly rejected a valid filter
    // would fail with "log init failed" in stderr instead.
    let td = TempDir::new().unwrap();
    let bogus = td.path().join("nonexistent.toml");
    Command::cargo_bin("gcit")
        .unwrap()
        .env_remove("CREDENTIALS_DIRECTORY")
        .arg("--log-filter")
        .arg("warn")
        .arg("--config")
        .arg(&bogus)
        .arg("check")
        .assert()
        .code(CONFIG)
        // The error must NOT be a log-init failure — it must come from
        // config-load. A regression that broke valid-filter parsing
        // would surface here as a "log init failed" message.
        .stderr(predicate::str::contains("log init failed").not());
}

#[test]
fn invalid_log_filter_target_level_exits_config_78() {
    // EnvFilter rejects "gcit=foobar" as a directive — "foobar" is
    // not a recognized log level. Pin that this specific shape is
    // also caught (rather than only the obvious "::::" malformed
    // case).
    let td = TempDir::new().unwrap();
    let cfg = write_config(&td);
    Command::cargo_bin("gcit")
        .unwrap()
        .env_remove("CREDENTIALS_DIRECTORY")
        .arg("--log-filter")
        .arg("gcit=foobar")
        .arg("--config")
        .arg(&cfg)
        .arg("check")
        .assert()
        .code(CONFIG)
        .stderr(predicate::str::contains("log init failed"));
}

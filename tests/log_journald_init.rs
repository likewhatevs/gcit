// tracing-subscriber + tracing-journald init.
// Deps: tracing, tracing-subscriber, tracing-journald.
// `log/` module.
// `gcit run [--foreground]` daemon entry; --foreground logs to stderr
// (main(): if foreground: bind own socket + stderr; else: booted() +
// listen_fds()).
//
// In daemon mode (no --foreground), gcit logs to journald. The daemon's
// stdout/stderr is captured by systemd via journald regardless, but
// tracing-journald lets us emit STRUCTURED events (key-value fields)
// rather than just stringified lines. Critical for `journalctl -u gcit
// --output=json | jq` workflows that operators expect.

use tempfile::TempDir;

#[test]
#[ignore = "requires logging subscriber implementation"]
fn journald_layer_attached_when_not_foreground() {
    let _dir = TempDir::new().unwrap();
    // Implementer hook: gcit::log::init() returns the assembled subscriber
    // OR a marker indicating which layers are attached.
    //
    // Assert when foreground=false:
    //   - tracing_journald::Layer is attached
    //   - tracing_subscriber::fmt::Layer (stderr) is NOT attached, OR
    //     attached but inert (no-op writer) — depending on impl choice
    //
    // SPEC GAP: spec doesn't define whether stderr layer coexists with
    // journald in daemon mode. Recommend journald-only in daemon to
    // avoid double-logging (systemd captures stderr separately and
    // would double-record if both are on). Flag for review.
}

#[test]
#[ignore = "requires logging subscriber implementation"]
fn fmt_stderr_layer_attached_when_foreground() {
    let _dir = TempDir::new().unwrap();
    // Assert when foreground=true:
    //   - tracing_subscriber::fmt::Layer (stderr) IS attached
    //   - tracing_journald::Layer is NOT attached
    //
    // Foreground mode is for development; readable colored stderr beats
    // structured journald entries an operator can't easily inspect from
    // a tty.
}

#[test]
#[ignore = "requires logging subscriber implementation"]
fn rust_log_overrides_config_filter() {
    let _dir = TempDir::new().unwrap();
    // Precedence: CLI flag > env var > config file (applied
    // uniformly to every option that can be set in more than one
    // place).
    //
    // For log filter: --log-filter > RUST_LOG > config.log.filter > "info".
    // Pin all four levels of precedence:
    //
    // 1. only "info" default — assert subscriber EnvFilter is "info"
    // 2. config has filter="warn" — assert "warn"
    // 3. RUST_LOG=trace + config="warn" — assert "trace"
    // 4. --log-filter=debug + RUST_LOG=trace + config="warn" — assert "debug"
    //
    // SPEC GAP: spec line 121 lists `filter = "info,gcit=debug"` example;
    // line 603 documents precedence; but spec doesn't pin the literal
    // "info" string as the bottom default. Recommend "info,gcit=debug"
    // (matches the example) OR plain "info". Flag for review.
}

#[test]
#[ignore = "requires logging subscriber implementation"]
fn tracing_event_includes_structured_fields_in_journald() {
    let _dir = TempDir::new().unwrap();
    // Smoke test that tracing-journald is wired such that an event with
    // structured fields (e.g., tracing::info!(flow = "x", "polled"))
    // produces journald entries with PRIORITY, MESSAGE, FLOW=x.
    //
    // This is HARD to assert from inside cargo nextest because journald
    // is process-global. Realistic approach: invoke the gcit binary as
    // a subprocess under a separate journald namespace
    // (systemd-run --user --pty --service-type=notify ...) and read back
    // via `journalctl --user -t gcit --output=json` after the subprocess
    // exits.
    //
    // Recommend gating this test behind SYSTEMD_TESTS=1 and running
    // it in journey/journald.sh, not as a normal Rust test. The
    // unit-test side covers wiring; the journey side covers actual
    // emission.
    //
    // SPEC GAP: spec doesn't pin which fields gcit must emit (FLOW, RUN_ID,
    // CONCLUSION, etc.). Without a concrete contract, snapshot tests
    // can't pin them. Recommend a documented field list in the spec.
    // Flag for review.
}

#[test]
#[ignore = "requires logging subscriber implementation"]
fn credential_fields_skipped_in_instrument_attribute() {
    let _dir = TempDir::new().unwrap();
    // tracing instrumentation on functions that touch
    // credentials uses #[instrument(skip(credential, secret, token,
    // password))] to keep secrets out of structured log fields.
    //
    // Build a function with #[instrument] over a SecretString-typed arg
    // named one of those four; emit an event inside; capture via
    // tracing-subscriber test layer; assert NO field with the secret
    // value or its repr appears in any captured event.
    //
    // Mutation target: removing the skip() list. Test catches by
    // looking for any field whose value matches the SecretString's
    // wrapped content.
    //
    // This test pins the JOURNALD/TRACING vector specifically;
    // SecretString redaction in Debug/Display paths is a separate
    // concern.
}

// Logging initialization.
//
// Logging is initialized as the first step of `run()`. We use
// `tracing-subscriber` for the registry/filter and `tracing-journald`
// for the structured-journal layer; the fallback to a stderr fmt
// layer is the canonical pattern documented in `tracing-journald`'s
// own README.
//
// Filter precedence: CLI flag > env var > config file. The caller
// decides which value wins; `init()` accepts the resolved filter
// string and applies a built-in fallback only when every source is
// empty.
//
// Layer choice: in daemon (non-foreground) mode the layer is
// journald-only — failing to reach journald is a fatal startup error
// so structured logs never silently downgrade to stderr in
// production. In `--foreground` mode the fmt layer is always used
// (no journald, even when reachable, since the operator is reading
// stderr directly).

use std::io;
use std::path::Path;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Default env-filter when no source supplies one. Bottom-default
/// is `info,gcit=debug`. The wider crate graph stays at INFO;
/// gcit's own targets emit DEBUG so the daemon's structured events
/// (control reload, dispatch summary, notifier outcomes) reach
/// journald without an explicit operator opt-in.
pub const DEFAULT_FILTER: &str = "info,gcit=debug";

/// Initialize the global tracing subscriber.
///
/// `foreground = true`: stderr fmt layer (human-readable). Used by
/// `gcit run --foreground` for development and testing.
/// `foreground = false`: journald-only layer with the `gcit` syslog
/// identifier. A journald connect failure here is a fatal startup
/// error — the daemon must not silently downgrade to stderr because
/// nothing reads stderr under systemd.
///
/// Returns `Err` when the resolved filter string is invalid (caller
/// should treat as `EX_CONFIG=78`) or when journald is unreachable
/// in daemon mode.
pub fn init(filter: Option<&str>, foreground: bool) -> io::Result<()> {
    let raw = filter.unwrap_or(DEFAULT_FILTER);
    let env_filter = EnvFilter::try_new(raw)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

    if foreground {
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_writer(io::stderr)
            .with_target(true);
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .try_init()
            .map_err(|e| io::Error::other(e.to_string()))?;
    } else {
        let journald = tracing_journald::Layer::new()
            .map_err(|e| {
                io::Error::other(format!(
                    "journald layer unavailable in daemon mode: {} (try --foreground for stderr logs)",
                    e
                ))
            })?
            .with_syslog_identifier("gcit".into());
        tracing_subscriber::registry()
            .with(env_filter)
            .with(journald)
            .try_init()
            .map_err(|e| io::Error::other(e.to_string()))?;
    }
    Ok(())
}

/// Best-effort peek at `[log].filter` from a config file. Reads the
/// file as TOML through a minimal serde struct so an unrelated
/// config error (a misconfigured `[[flow]]`, a credential typo) does
/// NOT block log init — the binary needs to log at startup to
/// surface the actual error. Any read or parse failure returns
/// `None` and the binary falls back to the CLI flag or the built-in
/// default. Subcommands that load the full config later surface the
/// underlying error with the default filter applied.
pub fn peek_filter_from_config(config_path: &Path) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct LogPeek {
        log: Option<LogFilterPeek>,
    }
    #[derive(serde::Deserialize)]
    struct LogFilterPeek {
        filter: Option<String>,
    }
    let raw = std::fs::read_to_string(config_path).ok()?;
    let parsed: LogPeek = toml::from_str(&raw).ok()?;
    parsed.log?.filter
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peek_returns_filter_when_log_table_present() {
        // Pin the happy path: a valid TOML config with `[log] filter`
        // returns the filter string verbatim. Mutations that drop the
        // peek (e.g. returning None on any nested Some chain) would
        // surface here as a fall-through to the default filter.
        let td = tempfile::tempdir().expect("tempdir");
        let path = td.path().join("config.toml");
        std::fs::write(&path, "[log]\nfilter = \"warn,gcit=trace\"\n").expect("write");
        assert_eq!(
            peek_filter_from_config(&path).as_deref(),
            Some("warn,gcit=trace"),
        );
    }

    #[test]
    fn peek_returns_none_when_log_table_absent() {
        // No `[log]` table -> Option::None on the outer `log:` field
        // -> the `?` operator short-circuits cleanly to None. Pin so
        // a regression that defaulted to an empty-string filter
        // (rather than None) would surface here — empty filter
        // strings make EnvFilter reject everything, silently
        // suppressing all logs.
        let td = tempfile::tempdir().expect("tempdir");
        let path = td.path().join("config.toml");
        std::fs::write(&path, "# config without [log]\n").expect("write");
        assert!(peek_filter_from_config(&path).is_none());
    }

    #[test]
    fn peek_returns_none_when_log_filter_absent() {
        // `[log]` table present but `filter = ` omitted. The outer
        // table parses to Some(LogFilterPeek { filter: None }), and
        // the inner None propagates out.
        let td = tempfile::tempdir().expect("tempdir");
        let path = td.path().join("config.toml");
        std::fs::write(&path, "[log]\n").expect("write");
        assert!(peek_filter_from_config(&path).is_none());
    }

    #[test]
    fn peek_returns_none_for_missing_file() {
        // The peek is best-effort — a missing config path returns
        // None silently so log init falls back to the default and
        // the actual config-load step surfaces the real error.
        let td = tempfile::tempdir().expect("tempdir");
        let path = td.path().join("does-not-exist.toml");
        assert!(peek_filter_from_config(&path).is_none());
    }

    #[test]
    fn peek_returns_none_for_invalid_toml() {
        // Malformed TOML produces None silently. The subsequent
        // full config load surfaces the parse error with the default
        // filter applied to the error event.
        let td = tempfile::tempdir().expect("tempdir");
        let path = td.path().join("config.toml");
        std::fs::write(&path, "this is = = not valid toml = []]\n").expect("write");
        assert!(peek_filter_from_config(&path).is_none());
    }

    #[test]
    fn peek_returns_none_when_log_filter_is_null_string() {
        // Defensive: `filter = ""` parses successfully to
        // Some("".to_string()). The current implementation returns
        // that empty string. Pin to the actual behavior — operators
        // who set an empty filter get an empty filter (and likely
        // suppress all logs); they can fix the config. A regression
        // that normalised "" -> None would NOT surface here unless
        // this expectation changes.
        let td = tempfile::tempdir().expect("tempdir");
        let path = td.path().join("config.toml");
        std::fs::write(&path, "[log]\nfilter = \"\"\n").expect("write");
        assert_eq!(peek_filter_from_config(&path).as_deref(), Some(""));
    }
}

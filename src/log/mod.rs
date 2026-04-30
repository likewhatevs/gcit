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

// Poll defaults / overrides, HTTP config, and the duration/jitter
// parsers that gate them.

use std::path::Path;
use std::time::Duration;

use toml::Spanned;

use super::super::error::ConfigError;
use super::super::parse::{
    HttpConfig, PollDefaults, PollOverride, RawHttpConfig, RawPollDefaults, RawPollOverride,
};
use super::{
    span_line, validate_err, MAX_HTTP_TIMEOUT, MAX_INTERVAL, MAX_JITTER, MIN_HTTP_TIMEOUT,
    MIN_INTERVAL, MIN_JITTER,
};

pub(super) fn validate_poll_defaults(
    raw: &RawPollDefaults,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
) -> PollDefaults {
    let default_poll = PollDefaults::default();

    let source_interval = raw
        .source_interval
        .as_ref()
        .and_then(|s| parse_interval(s, "poll.source_interval", None, source, path, errors));
    let job_interval = raw
        .job_interval
        .as_ref()
        .and_then(|s| parse_interval(s, "poll.job_interval", None, source, path, errors))
        .unwrap_or(default_poll.job_interval);
    let jitter = raw
        .jitter
        .as_ref()
        .and_then(|s| parse_jitter(s, "poll.jitter", None, source, path, errors))
        .unwrap_or(default_poll.jitter);
    let cooldown = raw
        .cooldown
        .as_ref()
        .and_then(|s| parse_cooldown(s, "poll.cooldown", None, source, path, errors))
        .unwrap_or(default_poll.cooldown);

    PollDefaults {
        source_interval,
        job_interval,
        jitter,
        cooldown,
    }
}

pub(super) fn validate_http(
    raw: &RawHttpConfig,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
) -> HttpConfig {
    let defaults = HttpConfig::default();
    let request_timeout = raw
        .request_timeout
        .as_ref()
        .and_then(|s| {
            parse_bounded_duration(
                s,
                "http.request_timeout",
                None,
                MIN_HTTP_TIMEOUT,
                MAX_HTTP_TIMEOUT,
                source,
                path,
                errors,
            )
        })
        .unwrap_or(defaults.request_timeout);
    if let Some(spanned) = &raw.max_concurrent {
        // Deprecated. Field is accepted but ignored — the runtime no
        // longer wires it into a Semaphore. Surface a WARN so the
        // operator removes it on the next edit.
        let line = span_line(source, spanned);
        let value = *spanned.get_ref();
        tracing::warn!(
            target: "gcit::config",
            config = %path.display(),
            line,
            value,
            "http.max_concurrent is deprecated and ignored; remove it from the config",
        );
    }
    HttpConfig { request_timeout }
}

pub(super) fn validate_poll_override(
    raw: &RawPollOverride,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
    flow_name: &str,
) -> PollOverride {
    let source_interval = raw.source_interval.as_ref().and_then(|s| {
        parse_interval(
            s,
            "flow.poll.source_interval",
            Some(flow_name),
            source,
            path,
            errors,
        )
    });
    let job_interval = raw.job_interval.as_ref().and_then(|s| {
        parse_interval(
            s,
            "flow.poll.job_interval",
            Some(flow_name),
            source,
            path,
            errors,
        )
    });
    let jitter = raw
        .jitter
        .as_ref()
        .and_then(|s| parse_jitter(s, "flow.poll.jitter", Some(flow_name), source, path, errors));
    let cooldown = raw.cooldown.as_ref().and_then(|s| {
        parse_cooldown(
            s,
            "flow.poll.cooldown",
            Some(flow_name),
            source,
            path,
            errors,
        )
    });
    PollOverride {
        source_interval,
        job_interval,
        jitter,
        cooldown,
    }
}

/// Parse a humantime cooldown string. `0s` is explicitly accepted and
/// disables throttling. Non-zero values are bounded to
/// `[MIN_INTERVAL, MAX_INTERVAL]`.
pub(super) fn parse_cooldown(
    spanned: &Spanned<String>,
    field: &'static str,
    flow: Option<&str>,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
) -> Option<Duration> {
    let raw = spanned.get_ref().clone();
    let line = span_line(source, spanned);
    let hint = case_confusable_hint(&raw);
    match humantime::parse_duration(&raw) {
        Err(e) => {
            errors.push(ConfigError::Parse {
                path: path.to_path_buf(),
                line,
                message: format!("{} = {:?}: invalid duration: {}{}", field, raw, e, hint),
            });
            None
        }
        Ok(d) => {
            if d.is_zero() {
                Some(Duration::ZERO)
            } else if d < MIN_INTERVAL || d > MAX_INTERVAL {
                errors.push(validate_err(
                    path,
                    vec![line],
                    flow,
                    field,
                    raw.clone(),
                    format!(
                        "{} must be 0s (disables throttling) or between {}s and {}s; got {}s{}",
                        field,
                        MIN_INTERVAL.as_secs(),
                        MAX_INTERVAL.as_secs(),
                        d.as_secs(),
                        hint,
                    ),
                    format!(
                        "use 0s to disable, or a value in the range [{}s, {}s]",
                        MIN_INTERVAL.as_secs(),
                        MAX_INTERVAL.as_secs()
                    ),
                ));
                None
            } else {
                Some(d)
            }
        }
    }
}

/// Parse a humantime duration string, bounded to `[MIN_INTERVAL,
/// MAX_INTERVAL]`. Thin wrapper over `parse_bounded_duration`.
pub(super) fn parse_interval(
    spanned: &Spanned<String>,
    field: &'static str,
    flow: Option<&str>,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
) -> Option<Duration> {
    parse_bounded_duration(
        spanned,
        field,
        flow,
        MIN_INTERVAL,
        MAX_INTERVAL,
        source,
        path,
        errors,
    )
}

/// Parse a humantime duration and require it within `[min, max]`.
///
/// Two error categories: humantime parse failure -> `Parse`,
/// out-of-bounds -> `Validate`. The distinction matters because the
/// operator's fix differs (range = pick a number; parse = fix syntax).
/// Both branches append a case-confusable hint for uppercase 'M'/'W'
/// because the glyphs are indistinguishable from 'm'/'w' in many fonts.
#[allow(clippy::too_many_arguments)]
fn parse_bounded_duration(
    spanned: &Spanned<String>,
    field: &'static str,
    flow: Option<&str>,
    min: Duration,
    max: Duration,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
) -> Option<Duration> {
    let raw = spanned.get_ref().clone();
    let line = span_line(source, spanned);
    let hint = case_confusable_hint(&raw);
    match humantime::parse_duration(&raw) {
        Err(e) => {
            errors.push(ConfigError::Parse {
                path: path.to_path_buf(),
                line,
                message: format!("{} = {:?}: invalid duration: {}{}", field, raw, e, hint),
            });
            None
        }
        Ok(d) => {
            if d < min || d > max {
                errors.push(validate_err(
                    path,
                    vec![line],
                    flow,
                    field,
                    raw.clone(),
                    format!(
                        "{} must be between {}s and {}s; got {}s{}",
                        field,
                        min.as_secs(),
                        max.as_secs(),
                        d.as_secs(),
                        hint,
                    ),
                    format!(
                        "use a value in the range [{}s, {}s]",
                        min.as_secs(),
                        max.as_secs()
                    ),
                ));
                None
            } else {
                Some(d)
            }
        }
    }
}

/// Hint string appended to a duration error when the input contains a
/// case-confusable unit. Empty string when no hint applies, so callers
/// can unconditionally splice it into format strings.
fn case_confusable_hint(raw: &str) -> &'static str {
    if raw.contains('M') {
        " (note: 'M' means months in humantime; use lowercase 'm' for minutes)"
    } else if raw.contains('W') {
        " (note: 'W' is not a valid humantime unit; use lowercase 'w' for weeks)"
    } else {
        ""
    }
}

fn parse_jitter(
    spanned: &Spanned<f64>,
    field: &'static str,
    flow: Option<&str>,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
) -> Option<f64> {
    let value = *spanned.get_ref();
    let line = span_line(source, spanned);
    if !(MIN_JITTER..=MAX_JITTER).contains(&value) {
        errors.push(validate_err(
            path,
            vec![line],
            flow,
            field,
            format!("{}", value),
            format!(
                "{} must be in the range [{}, {}]; got {}",
                field, MIN_JITTER, MAX_JITTER, value
            ),
            format!(
                "use a value in [{}, {}], for example 0.1",
                MIN_JITTER, MAX_JITTER
            ),
        ));
        None
    } else {
        Some(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn case_confusable_hint_uppercase_m_returns_months_warning() {
        let hint = case_confusable_hint("5M");
        assert!(
            hint.contains("'M' means months"),
            "uppercase M must surface the months hint; got: {hint}",
        );
        assert!(
            hint.contains("lowercase 'm'"),
            "hint must point at the correct lowercase form; got: {hint}",
        );
    }

    #[test]
    fn case_confusable_hint_uppercase_w_returns_weeks_warning() {
        let hint = case_confusable_hint("3W");
        assert!(
            hint.contains("'W' is not a valid humantime unit"),
            "uppercase W must surface the not-a-valid-unit hint; got: {hint}",
        );
        assert!(
            hint.contains("lowercase 'w'"),
            "hint must point at lowercase 'w' for weeks; got: {hint}",
        );
    }

    #[test]
    fn case_confusable_hint_lowercase_only_returns_empty() {
        assert_eq!(case_confusable_hint("15s"), "");
        assert_eq!(case_confusable_hint("5m"), "");
        assert_eq!(case_confusable_hint("2w"), "");
        assert_eq!(case_confusable_hint(""), "");
    }

    #[test]
    fn case_confusable_hint_uppercase_m_takes_precedence_over_uppercase_w() {
        // 'M' is checked before 'W' inside case_confusable_hint;
        // an input containing both surfaces the months hint.
        let hint = case_confusable_hint("5M3W");
        assert!(hint.contains("'M' means months"));
    }
}

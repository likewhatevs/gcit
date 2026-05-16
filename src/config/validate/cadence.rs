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
        // operator removes it on the next edit. The warning fires at
        // most once per process so a SIGHUP reload doesn't re-spam
        // operator logs every cycle; the AtomicBool latch is reset
        // never (the warning is per-process by design).
        static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        if !WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
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

    /// Build a `parse_cooldown` invocation with the given value.
    /// Returns the parsed `Option<Duration>` plus any `ConfigError`s
    /// the validator collected — handy for asserting both the
    /// happy-path return and the lack of errors in one call.
    fn parse_cooldown_test(value: &str) -> (Option<Duration>, Vec<ConfigError>) {
        use toml::Spanned;
        let source = format!("v = {:?}\n", value);
        // Build a Spanned<String> by parsing the same TOML the
        // production validator sees; `Spanned` is non-trivially
        // constructible by hand because its `span` field is private.
        #[derive(serde::Deserialize)]
        struct Doc {
            v: Spanned<String>,
        }
        let doc: Doc = toml::from_str(&source).expect("test toml must parse");
        let mut errors: Vec<ConfigError> = Vec::new();
        let parsed = parse_cooldown(
            &doc.v,
            "flow.poll.cooldown",
            Some("flow-x"),
            &source,
            std::path::Path::new("inline"),
            &mut errors,
        );
        (parsed, errors)
    }

    #[test]
    fn parse_cooldown_zero_seconds_returns_duration_zero_without_errors() {
        // `cooldown = "0s"` is the documented opt-out for throttling.
        // The function short-circuits on `d.is_zero()` and must NOT
        // apply the MIN_INTERVAL bound (which would reject 0s as
        // below-minimum). Pin the special-case so a regression that
        // dropped the short-circuit and treated 0s as below-min
        // surfaces here.
        let (parsed, errors) = parse_cooldown_test("0s");
        assert_eq!(parsed, Some(Duration::ZERO));
        assert!(
            errors.is_empty(),
            "0s must not surface any error; got: {errors:?}",
        );
    }

    #[test]
    fn parse_cooldown_one_second_rejected_below_min_interval() {
        // Above-zero values fall through to the bounds check.
        // `1s < MIN_INTERVAL` (15s), so the validator rejects with a
        // Validate variant naming the field. Mirrors the same bound
        // `parse_interval` enforces.
        let (parsed, errors) = parse_cooldown_test("1s");
        assert!(parsed.is_none(), "below-min must return None");
        assert!(
            errors.iter().any(|e| matches!(
                e,
                ConfigError::Validate { field, .. } if field == "flow.poll.cooldown"
            )),
            "below-min must surface a Validate error on the cooldown field; got: {errors:?}",
        );
    }

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

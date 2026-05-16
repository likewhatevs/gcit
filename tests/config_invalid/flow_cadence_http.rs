// Flow-name validation + top-level/per-flow cadence bounds + http
// timeout bounds + case-confusable-M hint. Empty/disabled flow lists
// also live here since they're top-level config-shape concerns.

use rstest::rstest;

use super::common::{
    build_flow_with_name, build_with_http_request_timeout, build_with_jitter,
    build_with_max_concurrent,
};

#[test]
fn empty_config_rejects() {
    let errors = gcit::config::load_str("", std::path::Path::new("inline"))
        .expect_err("empty config must reject");
    let has_flow_required = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, message, .. } => {
            field == "flow" && message.contains("at least one flow is required")
        }
        _ => false,
    });
    assert!(
        has_flow_required,
        "expected 'at least one flow is required' Validate, got: {:#?}",
        errors,
    );
}

// flow.name boundary lengths and charset.

#[test]
fn flow_name_zero_chars_rejected() {
    let toml = build_flow_with_name("");
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err("empty name must reject");
    assert!(errors.iter().any(|e| matches!(
        e,
        gcit::config::ConfigError::Validate { field, .. } if field == "flow.name"
    )));
}

#[test]
fn flow_name_one_char_accepted() {
    let toml = build_flow_with_name("a");
    gcit::config::load_str(&toml, std::path::Path::new("inline")).expect("1-char name is valid");
}

#[test]
fn flow_name_64_chars_accepted() {
    let name: String = "a".repeat(64);
    let toml = build_flow_with_name(&name);
    gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect("64-char name is valid (boundary)");
}

#[test]
fn flow_name_65_chars_rejected() {
    let name: String = "a".repeat(65);
    let toml = build_flow_with_name(&name);
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err("65-char name must reject (over boundary)");
    assert!(errors.iter().any(|e| matches!(
        e,
        gcit::config::ConfigError::Validate { field, message, .. }
        if field == "flow.name" && message.contains("max is 64")
    )));
}

#[rstest]
#[case::dot("foo.bar")]
#[case::space("foo bar")]
#[case::slash("foo/bar")]
#[case::tilde("~foo")]
#[case::dollar("$foo")]
fn flow_name_invalid_charset_rejected(#[case] name: &str) {
    let toml = build_flow_with_name(name);
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err(&format!("{:?} must reject as a flow.name", name));
    assert!(
        errors.iter().any(|e| matches!(
            e,
            gcit::config::ConfigError::Validate { field, .. } if field == "flow.name"
        )),
        "expected flow.name Validate, got {:#?}",
        errors,
    );
}

#[test]
fn flow_name_empty_string_emits_non_empty_validate_error() {
    let toml = build_flow_with_name("");
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err("empty flow.name must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, message, .. } => {
            field == "flow.name" && message.contains("non-empty")
        }
        _ => false,
    });
    assert!(any, "expected flow.name non-empty Validate: {:#?}", errors);
}

// jitter boundary values (top-level + per-flow).

#[rstest]
#[case::zero(0.0)]
#[case::point_one(0.1)]
#[case::half(0.5)]
fn jitter_in_range_accepted(#[case] j: f64) {
    let toml = build_with_jitter(j);
    gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .unwrap_or_else(|e| panic!("expected jitter={} to be accepted: {:#?}", j, e));
}

#[rstest]
#[case::just_below_zero(-0.001)]
#[case::just_above_max(0.5001)]
#[case::well_above(0.7)]
fn jitter_out_of_range_rejected(#[case] j: f64) {
    let toml = build_with_jitter(j);
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err(&format!("jitter={} must reject", j));
    assert!(
        errors.iter().any(|e| matches!(
            e,
            gcit::config::ConfigError::Validate { field, .. } if field == "poll.jitter"
        )),
        "expected jitter Validate, got {:#?}",
        errors,
    );
}

#[test]
fn poll_jitter_at_lower_boundary_zero_accepted() {
    let raw = r#"
[poll]
jitter = 0.0
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
"#;
    gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect("jitter=0.0 must be accepted at lower boundary");
}

#[test]
fn poll_jitter_at_upper_boundary_half_accepted() {
    let raw = r#"
[poll]
jitter = 0.5
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
"#;
    gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect("jitter=0.5 must be accepted at upper boundary");
}

#[test]
fn poll_default_jitter_negative_rejected() {
    let raw = r#"
[poll]
jitter = -0.1
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("poll.jitter=-0.1 must reject");
    assert!(
        errors.iter().any(|e| matches!(
            e,
            gcit::config::ConfigError::Validate { field, .. }
            if field == "poll.jitter"
        )),
        "expected poll.jitter Validate: {:#?}",
        errors,
    );
}

// per-flow PollOverride bounds.

#[test]
fn flow_poll_override_jitter_out_of_range_rejected() {
    let raw = r#"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
[flow.poll]
jitter = 0.99
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("flow.poll.jitter=0.99 must reject");
    assert!(errors.iter().any(|e| matches!(
        e,
        gcit::config::ConfigError::Validate { field, .. }
        if field == "flow.poll.jitter"
    )));
}

#[test]
fn flow_poll_override_source_interval_below_min_rejected() {
    let raw = r#"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
[flow.poll]
source_interval = "5s"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("flow.poll.source_interval=5s must reject");
    assert!(errors.iter().any(|e| matches!(
        e,
        gcit::config::ConfigError::Validate { field, .. }
        if field == "flow.poll.source_interval"
    )));
}

#[test]
fn flow_poll_override_job_interval_below_min_rejected() {
    let raw = r#"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
[flow.poll]
job_interval = "1s"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("flow.poll.job_interval=1s must reject");
    assert!(
        errors.iter().any(|e| matches!(
            e,
            gcit::config::ConfigError::Validate { field, .. }
            if field == "flow.poll.job_interval"
        )),
        "expected flow.poll.job_interval Validate: {:#?}",
        errors,
    );
}

#[test]
fn poll_default_job_interval_above_max_rejected() {
    let raw = r#"
[poll]
job_interval = "25h"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("poll.job_interval=25h must reject");
    assert!(
        errors.iter().any(|e| matches!(
            e,
            gcit::config::ConfigError::Validate { field, .. }
            if field == "poll.job_interval"
        )),
        "expected poll.job_interval Validate: {:#?}",
        errors,
    );
}

#[test]
fn poll_source_interval_above_max_rejected() {
    let raw = r#"
[poll]
source_interval = "25h"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("poll.source_interval=25h must reject");
    assert!(
        errors.iter().any(|e| matches!(
            e,
            gcit::config::ConfigError::Validate { field, .. }
            if field == "poll.source_interval"
        )),
        "expected poll.source_interval Validate: {:#?}",
        errors,
    );
}

// http.* validation.

#[test]
fn http_max_concurrent_zero_accepted_as_deprecated() {
    let toml = build_with_max_concurrent(0);
    gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect("max_concurrent=0 must be accepted under deprecation");
}

#[test]
fn http_max_concurrent_large_accepted_as_deprecated() {
    let toml = build_with_max_concurrent(99_999);
    gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect("oversize max_concurrent must be accepted under deprecation");
}

#[test]
fn http_max_concurrent_typical_accepted() {
    let toml = build_with_max_concurrent(16);
    gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect("max_concurrent=16 (former default) must be accepted");
}

#[test]
fn http_request_timeout_below_min_rejected() {
    let toml = build_with_http_request_timeout("500ms");
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err("request_timeout=500ms must reject");
    assert!(
        errors.iter().any(|e| matches!(
            e,
            gcit::config::ConfigError::Validate { field, .. }
            if field == "http.request_timeout"
        )),
        "expected http.request_timeout Validate: {:#?}",
        errors,
    );
}

#[test]
fn http_request_timeout_above_max_rejected() {
    let toml = build_with_http_request_timeout("10m");
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err("request_timeout=10m must reject");
    assert!(
        errors.iter().any(|e| matches!(
            e,
            gcit::config::ConfigError::Validate { field, .. }
            if field == "http.request_timeout"
        )),
        "expected http.request_timeout Validate: {:#?}",
        errors,
    );
}

#[test]
fn http_request_timeout_at_min_boundary_accepted() {
    let toml = build_with_http_request_timeout("1s");
    gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect("request_timeout=1s must be accepted at lower boundary");
}

#[test]
fn http_request_timeout_at_max_boundary_accepted() {
    let toml = build_with_http_request_timeout("5m");
    gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect("request_timeout=5m must be accepted at upper boundary");
}

#[test]
fn duration_uppercase_m_surfaces_months_hint_in_error_message() {
    let raw = r#"
[poll]
source_interval = "5M"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("poll.source_interval=5M must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, message, .. } => {
            field == "poll.source_interval" && message.contains("'M' means months")
        }
        _ => false,
    });
    assert!(
        any,
        "expected case-confusable months hint in source_interval error: {:#?}",
        errors,
    );
}

#[test]
fn malformed_humantime_duration_emits_parse_error_not_validate() {
    let raw = r#"
[poll]
source_interval = "xyz"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("malformed duration must reject");
    let parse_count = errors
        .iter()
        .filter(|e| match e {
            gcit::config::ConfigError::Parse { message, .. } => {
                message.contains("poll.source_interval")
                    && message.contains("invalid duration")
                    && message.contains("\"xyz\"")
            }
            _ => false,
        })
        .count();
    assert_eq!(
        parse_count, 1,
        "malformed humantime must surface as a Parse variant naming the field, value, and 'invalid duration'; got: {:#?}",
        errors,
    );
    let validate_count = errors
        .iter()
        .filter(|e| match e {
            gcit::config::ConfigError::Validate { field, .. } => field == "poll.source_interval",
            _ => false,
        })
        .count();
    assert_eq!(
        validate_count, 0,
        "Parse failure must NOT also emit a Validate (range check is short-circuited): {:#?}",
        errors,
    );
}

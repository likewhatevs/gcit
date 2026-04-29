// humantime duration parsing edge cases.
// source_interval/job_interval are humantime; jitter is f64.
// MIN_INTERVAL=15s, MAX_INTERVAL=24h enforced at parse time,
// INCLUSIVE on both ends.
//
// humantime accepts a wide range of forms; gcit must accept the
// canonical ones documented in the README example and reject malformed
// inputs with a helpful Parse error pointing at the line.

use std::time::Duration;

use rstest::rstest;

fn build_with_source_interval(s: &str) -> String {
    format!(
        r#"
[poll]
source_interval = "{}"
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
"#,
        s
    )
}

#[rstest]
#[case::seconds("60s", 60)]
#[case::minutes("5m", 300)]
#[case::hours("1h", 3600)]
#[case::compound("1m 30s", 90)]
#[case::min_boundary("15s", 15)]
#[case::max_boundary("24h", 86_400)]
fn humantime_accepted_durations(#[case] s: &str, #[case] expect_seconds: u64) {
    let toml = build_with_source_interval(s);
    let cfg = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .unwrap_or_else(|e| panic!("expected {} to parse: {:#?}", s, e));
    assert_eq!(
        cfg.poll.source_interval.unwrap(),
        Duration::from_secs(expect_seconds)
    );
}

#[rstest]
#[case::below_min_one_second("1s")]
#[case::below_min_fourteen("14s")]
#[case::above_max_25h("25h")]
#[case::above_max_2d("2d")]
#[case::zero("0s")]
#[case::ambiguous_5_months("5M")]
fn humantime_rejected_durations_out_of_range(#[case] s: &str) {
    let toml = build_with_source_interval(s);
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err("must reject out-of-range duration");
    let any_validate = errors
        .iter()
        .any(|e| matches!(e, gcit::config::ConfigError::Validate { .. }));
    assert!(
        any_validate,
        "expected a Validate error for {:?}, got {:#?}",
        s, errors
    );
}

#[rstest]
#[case::garbage("not a duration")]
#[case::missing_unit("60")]
#[case::scientific("1e3s")]
#[case::uppercase_w_unsupported("5W")]
fn humantime_rejected_durations_malformed(#[case] s: &str) {
    let toml = build_with_source_interval(s);
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err("must reject malformed duration");
    let any_parse = errors
        .iter()
        .any(|e| matches!(e, gcit::config::ConfigError::Parse { .. }));
    assert!(
        any_parse,
        "expected a Parse error for {:?}, got {:#?}",
        s, errors
    );
}

#[test]
fn http_request_timeout_uses_humantime_too() {
    // HttpConfig.request_timeout uses humantime.
    let raw = r#"
[http]
request_timeout = "45s"
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
    let cfg =
        gcit::config::load_str(raw, std::path::Path::new("inline")).expect("valid http config");
    assert_eq!(cfg.http.request_timeout, Duration::from_secs(45));
}

#[rstest]
#[case::accepted_short("1s")]
#[case::accepted_default("30s")]
#[case::accepted_long("5m")]
fn http_request_timeout_in_range_accepted(#[case] s: &str) {
    let raw = format!(
        r#"
[http]
request_timeout = "{}"
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
"#,
        s
    );
    gcit::config::load_str(&raw, std::path::Path::new("inline"))
        .unwrap_or_else(|e| panic!("expected {} to be accepted: {:#?}", s, e));
}

#[rstest]
#[case::below_min("0s")]
#[case::above_max_6m("6m")]
#[case::ambiguous_humantime_months("5M")]
fn http_request_timeout_out_of_range_rejected(#[case] s: &str) {
    // "5M" is humantime months. Without an upper bound the daemon
    // would accept a months-long HTTP timeout, silently disabling
    // the timeout for security purposes.
    let raw = format!(
        r#"
[http]
request_timeout = "{}"
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
"#,
        s
    );
    let errors = gcit::config::load_str(&raw, std::path::Path::new("inline"))
        .expect_err("expected out-of-range http timeout to reject");
    let any_validate = errors
        .iter()
        .any(|e| matches!(e, gcit::config::ConfigError::Validate { .. }));
    assert!(
        any_validate,
        "expected Validate error for {:?}, got {:#?}",
        s, errors
    );
}

#[test]
fn humantime_uppercase_m_rejected_with_minutes_hint() {
    // "5M" looks like "5 minutes" at a glance but humantime parses
    // it as "5 months". The validator must reject the value AND
    // surface a hint pointing at the case mismatch so the operator
    // notices and fixes the typo.
    let raw = build_with_source_interval("5M");
    let errors =
        gcit::config::load_str(&raw, std::path::Path::new("inline")).expect_err("'5M' must reject");
    let combined = errors
        .iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        combined.contains("'M' means months") || combined.contains("means months"),
        "expected months/minutes hint in error output, got: {}",
        combined,
    );
}

#[rstest]
#[case::negative_seconds("-10s")]
#[case::negative_minutes("-5m")]
fn humantime_negative_durations_rejected(#[case] s: &str) {
    // humantime does not parse negative durations; gcit must surface
    // these as Parse errors (malformed syntax), not Validate.
    let toml = build_with_source_interval(s);
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err("negative duration must reject");
    let any_parse = errors
        .iter()
        .any(|e| matches!(e, gcit::config::ConfigError::Parse { .. }));
    assert!(
        any_parse,
        "expected Parse error for {:?}, got {:#?}",
        s, errors,
    );
}

#[rstest]
#[case::sub_second_close("14999ms")]
#[case::just_over_max("86401s")]
fn humantime_close_to_boundary_rejected(#[case] s: &str) {
    // Boundary-adjacent values: 14999ms (1ms below MIN_INTERVAL=15s)
    // and 86401s (1s above MAX_INTERVAL=24h). Both must be rejected as
    // Validate errors so an operator who tries to set "just under" or
    // "just over" the limits gets a clear range error.
    let toml = build_with_source_interval(s);
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err("boundary-adjacent duration must reject");
    let any_validate = errors
        .iter()
        .any(|e| matches!(e, gcit::config::ConfigError::Validate { .. }));
    assert!(
        any_validate,
        "expected Validate for {:?}, got {:#?}",
        s, errors,
    );
}

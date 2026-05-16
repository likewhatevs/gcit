// Table-driven test for invalid-config fixture files under
// `tests/resources/config/invalid/`. Each row asserts that loading
// the fixture produces a SPECIFIC ConfigError variant — not just
// "any error". This is the difference between a useful validator
// and a frustrating one.

use rstest::rstest;

use super::common::fixture;

#[rstest]
#[case::unknown_top_key(
    "resources/config/invalid/unknown_top_key.toml",
    "Parse",
    Some("polll"),
    Some(5)
)]
#[case::below_min_interval(
    "resources/config/invalid/below_min_interval.toml",
    "Validate",
    Some("source_interval"),
    Some(6)
)]
#[case::jitter_out_of_range(
    "resources/config/invalid/jitter_out_of_range.toml",
    "Validate",
    Some("jitter"),
    Some(7)
)]
#[case::ref_missing_prefix(
    "resources/config/invalid/ref_missing_prefix.toml",
    "Validate",
    Some("source.ref"),
    Some(11)
)]
#[case::repo_not_owner_slash_repo(
    "resources/config/invalid/repo_not_owner_slash_repo.toml",
    "Validate",
    Some("repo"),
    Some(14)
)]
#[case::duplicate_flow_name(
    "resources/config/invalid/duplicate_flow_name.toml",
    "Validate",
    Some("flow.name"),
    None
)]
#[case::credential_id_collision(
    "resources/config/invalid/credential_id_collision.toml",
    "Validate",
    Some("GCIT_CREDENTIAL_FOO_BAR"),
    None
)]
fn invalid_fixture_rejected_with_named_error(
    #[case] fixture_path: &str,
    #[case] expected_variant: &str,
    #[case] expected_substr: Option<&str>,
    #[case] expected_line: Option<usize>,
) {
    let path = fixture(fixture_path);
    let errors = gcit::config::load(&path).expect_err("fixture must reject");
    assert!(!errors.is_empty(), "must produce at least one error");
    let matched = errors.iter().find(|e| match (expected_variant, e) {
        ("Parse", gcit::config::ConfigError::Parse { line, message, .. }) => {
            let line_ok = expected_line.is_none_or(|w| *line == w);
            let substr_ok = expected_substr.is_none_or(|w| message.contains(w));
            line_ok && substr_ok
        }
        (
            "Validate",
            gcit::config::ConfigError::Validate {
                lines,
                field,
                value,
                message,
                suggestion,
                ..
            },
        ) => {
            let line_ok = expected_line.is_none_or(|w| lines.contains(&w));
            let combined = format!("{} {} {} {}", field, value, message, suggestion);
            let substr_ok = expected_substr.is_none_or(|w| combined.contains(w));
            line_ok && substr_ok && !suggestion.is_empty()
        }
        _ => false,
    });
    assert!(
        matched.is_some(),
        "expected variant {} matching substr={:?} line={:?} for {}, got: {:#?}",
        expected_variant,
        expected_substr,
        expected_line,
        fixture_path,
        errors
    );
}

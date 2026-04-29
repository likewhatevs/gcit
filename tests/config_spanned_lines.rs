// Every config error must include line number, value, and fix
// (via toml::Spanned<T>).
//
// `toml::Spanned<T>` wraps a deserialized value with byte-offset spans
// into the source. The validator converts those spans to 1-based line
// numbers for the ConfigError variants.
//
// Mutation target: off-by-one in the byte-offset → line conversion.

use std::path::PathBuf;

fn fixture(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join(rel)
}

#[test]
fn validate_error_for_jitter_points_at_correct_line() {
    // jitter_out_of_range.toml has `jitter = 0.7` on line 7 (after a
    // 2-line comment header, blank line, [poll] header, and two
    // sibling fields).
    let path = fixture("resources/config/invalid/jitter_out_of_range.toml");
    let errors = gcit::config::load(&path).expect_err("must reject");
    let matched = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { lines, field, .. } => {
            field == "poll.jitter" && lines == &vec![7]
        }
        _ => false,
    });
    assert!(matched, "expected jitter Validate@7, got: {:#?}", errors);
}

#[test]
fn validate_error_carries_offending_value() {
    // Each error MUST include the actual value that failed validation,
    // not just "value rejected". Operator must see what gcit saw.
    let cases = [
        ("resources/config/invalid/jitter_out_of_range.toml", "0.7"),
        ("resources/config/invalid/ref_missing_prefix.toml", "master"),
        ("resources/config/invalid/below_min_interval.toml", "5s"),
    ];
    for (path_rel, expected_value_substr) in cases {
        let path = fixture(path_rel);
        let errors = gcit::config::load(&path).expect_err("must reject");
        let any = errors.iter().any(|e| match e {
            gcit::config::ConfigError::Validate { value, .. } => {
                value.contains(expected_value_substr)
            }
            _ => false,
        });
        assert!(
            any,
            "expected value containing {:?} in {}: {:#?}",
            expected_value_substr, path_rel, errors
        );
    }
}

#[test]
fn validate_error_carries_concrete_fix_suggestion() {
    // Validate variant has a `suggestion` field. The fix suggestion
    // must be CONCRETE (a literal value or command), not generic
    // ("use a valid value").
    let cases = [
        (
            "resources/config/invalid/below_min_interval.toml",
            // "use a value in the range [15s, ..."
            "15s",
        ),
        (
            "resources/config/invalid/ref_missing_prefix.toml",
            "refs/heads",
        ),
        (
            "resources/config/invalid/repo_not_owner_slash_repo.toml",
            "owner/repo",
        ),
    ];
    for (path_rel, needle) in cases {
        let path = fixture(path_rel);
        let errors = gcit::config::load(&path).expect_err("must reject");
        let any = errors.iter().any(|e| match e {
            gcit::config::ConfigError::Validate { suggestion, .. } => suggestion.contains(needle),
            _ => false,
        });
        assert!(
            any,
            "expected suggestion containing {:?} for {}: {:#?}",
            needle, path_rel, errors
        );
    }
}

#[test]
fn parse_error_for_unknown_top_key_points_at_correct_line() {
    // unknown_top_key.toml has `polll = ...` on line 5 (after a 3-line
    // comment header and a blank line).
    let path = fixture("resources/config/invalid/unknown_top_key.toml");
    let errors = gcit::config::load(&path).expect_err("must reject");
    let matched = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Parse { line, message, .. } => {
            *line == 5 && message.contains("polll")
        }
        _ => false,
    });
    assert!(
        matched,
        "expected Parse@5 mentioning 'polll', got: {:#?}",
        errors
    );
}

#[test]
fn duplicate_flow_name_points_at_both_occurrences() {
    // Validate carries `lines: Vec<usize>`. For duplicate-name
    // detection, the validator emits a single error whose `lines`
    // field carries every occurrence so the operator can find every
    // definition that needs renaming.
    let path = fixture("resources/config/invalid/duplicate_flow_name.toml");
    let errors = gcit::config::load(&path).expect_err("must reject");
    let matched = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate {
            lines,
            field,
            value,
            ..
        } => field == "flow.name" && value == "linux" && lines.len() >= 2,
        _ => false,
    });
    assert!(
        matched,
        "expected Validate(field=flow.name) with >=2 lines: {:#?}",
        errors
    );
}

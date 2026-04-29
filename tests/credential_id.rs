// credential_id charset + collision-detection.
// `credential_id` must match `^[a-zA-Z0-9_-]+$`, max 64 chars. Path
// traversal (`..`), separators (`/`, `\`), `~`, and NUL bytes are
// explicitly rejected.
// id-to-env-var collision: `foo-bar` and `foo_bar` both map to
// GCIT_CREDENTIAL_FOO_BAR; rejected at config load.
//
// Mutation target: the regex check, the length check, the collision
// detector.

use rstest::rstest;

#[rstest]
#[case::lowercase("github_pat", true)]
#[case::with_hyphens("discord-ci-webhook", true)]
#[case::mixed_case("MyCred-123", true)]
#[case::single_char("a", true)]
#[case::digits_only("12345", true)]
#[case::empty("", false)]
#[case::dot("foo.bar", false)]
#[case::space("foo bar", false)]
#[case::slash("foo/bar", false)]
#[case::backslash("foo\\bar", false)]
#[case::dot_dot("..", false)]
#[case::dot_dot_path("foo/../bar", false)]
#[case::null_byte("foo\0bar", false)]
#[case::leading_hyphen("-foo", false)]
#[case::tilde("~foo", false)]
#[case::dollar("$foo", false)]
#[case::unicode_homoglyph("foo\u{0430}bar", false)] // cyrillic 'a'
fn credential_id_charset(#[case] id: &str, #[case] expect_valid: bool) {
    let result = gcit::config::credential::validate_id(id);
    if expect_valid {
        assert!(
            result.is_ok(),
            "expected {:?} to be accepted, got {:?}",
            id,
            result
        );
    } else {
        assert!(result.is_err(), "expected {:?} to be rejected, got Ok", id);
    }
}

#[test]
fn credential_id_max_length_accepted() {
    let id = "a".repeat(64);
    assert!(gcit::config::credential::validate_id(&id).is_ok());
}

#[test]
fn credential_id_too_long_rejected() {
    let id = "a".repeat(65);
    assert!(gcit::config::credential::validate_id(&id).is_err());
}

#[test]
fn credential_id_to_env_var_mapping() {
    // uppercase, hyphens to underscores, prefix.
    assert_eq!(
        gcit::config::credential::id_to_env("github_pat"),
        "GCIT_CREDENTIAL_GITHUB_PAT"
    );
    assert_eq!(
        gcit::config::credential::id_to_env("discord-ci-webhook"),
        "GCIT_CREDENTIAL_DISCORD_CI_WEBHOOK"
    );
    assert_eq!(
        gcit::config::credential::id_to_env("MyCred-123"),
        "GCIT_CREDENTIAL_MYCRED_123"
    );
}

fn build_config_with_credential_ids(ids: &[&str]) -> String {
    let mut s = String::new();
    for (i, id) in ids.iter().enumerate() {
        s.push_str(&format!(
            r#"
[[flow]]
name = "flow{}"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "{}"
"#,
            i, id
        ));
    }
    s
}

#[rstest]
#[case::hyphen_vs_underscore(&["foo-bar", "foo_bar"])]
#[case::case_insensitive_uppercase(&["foo", "FOO"])]
#[case::three_way(&["a-b-c", "a_b_c", "a-b_c"])]
fn credential_id_collision_detected(#[case] colliding_ids: &[&str]) {
    let toml = build_config_with_credential_ids(colliding_ids);
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err("collision must be detected");
    let any_validate = errors
        .iter()
        .any(|e| matches!(e, gcit::config::ConfigError::Validate { .. }));
    assert!(any_validate, "expected Validate, got: {:#?}", errors);
    let message = errors
        .iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    for id in colliding_ids {
        assert!(
            message.contains(id),
            "collision error must name id {:?}, got: {}",
            id,
            message
        );
    }
}

#[test]
fn credential_id_unique_no_collision() {
    let toml = build_config_with_credential_ids(&["github_pat", "discord_webhook"]);
    let cfg = gcit::config::load_str(&toml, std::path::Path::new("inline")).expect("no collision");
    assert_eq!(cfg.flow.len(), 2);
}

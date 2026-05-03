// Table-driven test for every invalid config fixture.
//
// Each row asserts that loading the fixture produces a SPECIFIC
// ConfigError variant — not just "any error". This is the difference
// between a useful validator and a frustrating one: the operator must
// see the field, the value, the line number, and a concrete fix
// suggestion.
//
// Mutation target: the validator's `match` arms in
// src/config/validate.rs that classify a single bad input into the
// specific Validate variant. Mutation testing flips the arm; this
// table catches it.

use std::path::PathBuf;

use rstest::rstest;

fn fixture(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join(rel)
}

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
    None, // multi-line — duplicate detection emits Vec<usize>
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
            // Every Validate error MUST carry a non-empty suggestion.
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

#[test]
fn fire_on_duplicate_event_rejected() {
    // Duplicate fire_on entries are config errors, not slop. `gcit
    // check` exists to surface mistakes — silent dedup would hide them.
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
[[flow.destination]]
kind = "discord_webhook"
credential_id = "c"
fire_on = ["run_complete", "run_complete"]
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("dup fire_on entries must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, message, .. } => {
            field.contains("fire_on") && message.contains("duplicate event 'run_complete'")
        }
        _ => false,
    });
    assert!(
        any,
        "expected fire_on duplicate-event error, got: {:#?}",
        errors
    );
}

#[test]
fn source_url_unsupported_scheme_rejected() {
    // source.url scheme must be one of {http, https, ssh, git, file}.
    // Other schemes (data:, javascript:, exotica) are rejected because
    // git transports do not use them.
    let raw = r#"
[[flow]]
name = "x"
[flow.source]
url = "data:text/plain,whatever"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("unsupported scheme must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, message, .. } => {
            field == "source.url" && message.contains("unsupported scheme 'data'")
        }
        _ => false,
    });
    assert!(any, "expected source.url scheme error: {:#?}", errors);
}

#[test]
fn source_url_must_parse_as_url() {
    // source.url is validated via url::Url::parse at config load.
    // Allowed schemes: http, https, ssh, git, file. Bare strings
    // without a scheme are rejected.
    let raw = r#"
[[flow]]
name = "x"
[flow.source]
url = "this is not a url"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("invalid url must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, .. } => field == "source.url",
        _ => false,
    });
    assert!(any, "expected source.url validate error: {:#?}", errors);
}

#[test]
fn empty_fire_on_array_is_accepted_and_destination_carries_empty_vec() {
    // fire_on=[] means destination fires on no events.
    // Loading must succeed; the runtime check silently skips the
    // destination.
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
[[flow.destination]]
kind = "discord_webhook"
credential_id = "c"
fire_on = []
"#;
    let cfg =
        gcit::config::load_str(raw, std::path::Path::new("inline")).expect("fire_on=[] is valid");
    let dest = &cfg.flow[0].destination[0];
    if let gcit::config::Destination::DiscordWebhook(d) = dest {
        assert!(d.fire_on.is_empty());
    } else {
        panic!("expected discord_webhook destination");
    }
}

#[test]
fn action_inputs_user_supplied_gcit_run_id_rejected() {
    // gcit injects `gcit_run_id` at dispatch time so the workflow's
    // run-name directive can correlate the run to its dispatch. A
    // user-supplied value would be silently overwritten, so config
    // validation must reject it loudly with an actionable error
    // pointing at action.inputs.
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
inputs = { gcit_run_id = "user-supplied" }
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("user-supplied gcit_run_id must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate {
            field,
            message,
            suggestion,
            ..
        } => {
            field.contains("action.inputs")
                && message.contains("gcit_run_id")
                && !suggestion.is_empty()
        }
        _ => false,
    });
    assert!(
        any,
        "expected action.inputs.gcit_run_id rejection: {:#?}",
        errors,
    );
}

// ---------------------------------------------------------------------
// Empty config (zero flows) must reject.
// ---------------------------------------------------------------------
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

// ---------------------------------------------------------------------
// B4: flow.name boundary lengths.
// ---------------------------------------------------------------------
fn build_flow_with_name(name: &str) -> String {
    format!(
        r#"
[[flow]]
name = "{}"
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
        name,
    )
}

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

// ---------------------------------------------------------------------
// B5: flow.name charset rejects punctuation, spaces, dots.
// ---------------------------------------------------------------------
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

// ---------------------------------------------------------------------
// B6: jitter boundary values.
// ---------------------------------------------------------------------
fn build_with_jitter(j: f64) -> String {
    format!(
        r#"
[poll]
jitter = {}
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
        j,
    )
}

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

// ---------------------------------------------------------------------
// B12: local_mail user validation.
// ---------------------------------------------------------------------
fn build_local_mail_user(user: &str) -> String {
    format!(
        r#"
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
[[flow.destination]]
kind = "local_mail"
user = "{}"
"#,
        user,
    )
}

#[test]
fn local_mail_user_empty_rejected() {
    let toml = build_local_mail_user("");
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err("empty user must reject");
    assert!(errors.iter().any(|e| matches!(
        e,
        gcit::config::ConfigError::Validate { field, .. }
        if field.contains("local_mail.user")
    )));
}

#[test]
fn local_mail_user_32_chars_accepted() {
    let user: String = "u".repeat(32);
    let toml = build_local_mail_user(&user);
    gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect("32-char user is valid (boundary)");
}

#[test]
fn local_mail_user_33_chars_rejected() {
    let user: String = "u".repeat(33);
    let toml = build_local_mail_user(&user);
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err("33-char user must reject");
    assert!(errors.iter().any(|e| matches!(
        e,
        gcit::config::ConfigError::Validate { field, message, .. }
        if field.contains("local_mail.user") && message.contains("max is 32")
    )));
}

#[rstest]
#[case::dot("foo.bar")]
#[case::slash("foo/bar")]
fn local_mail_user_invalid_charset_rejected(#[case] user: &str) {
    let toml = build_local_mail_user(user);
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err(&format!("{:?} must reject", user));
    assert!(
        errors.iter().any(|e| matches!(
            e,
            gcit::config::ConfigError::Validate { field, .. }
            if field.contains("local_mail.user") || field.contains("destination.user")
        )),
        "expected local_mail.user Validate, got {:#?}",
        errors,
    );
}

// ---------------------------------------------------------------------
// B14: PollOverride out-of-range values.
// ---------------------------------------------------------------------
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

// ---------------------------------------------------------------------
// B15: action.repo edge cases.
// ---------------------------------------------------------------------
fn build_with_repo(repo: &str) -> String {
    format!(
        r#"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "{}"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
"#,
        repo,
    )
}

#[rstest]
#[case::three_parts("a/b/c")]
#[case::leading_slash("/repo")]
#[case::trailing_slash("owner/")]
#[case::double_slash("owner//repo")]
#[case::just_slash("/")]
fn action_repo_invalid_forms_rejected(#[case] repo: &str) {
    let toml = build_with_repo(repo);
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err(&format!("repo={:?} must reject", repo));
    assert!(
        errors.iter().any(|e| matches!(
            e,
            gcit::config::ConfigError::Validate { field, .. } if field == "action.repo"
        )),
        "expected action.repo Validate for {:?}, got {:#?}",
        repo,
        errors,
    );
}

// ---------------------------------------------------------------------
// B16: action.workflow edge cases.
// ---------------------------------------------------------------------
//
// Workflow values are wrapped in TOML literal-string (single-quote)
// quoting because some valid test values contain backslashes; TOML
// basic-string (double-quote) syntax interprets backslash as an escape
// character, so `workflow = "ci\extra.yml"` is a Parse error rather
// than a workflow with a literal backslash.
fn build_with_workflow(workflow: &str) -> String {
    format!(
        r#"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = '{}'
ref = "refs/heads/main"
credential_id = "c"
"#,
        workflow,
    )
}

#[rstest]
#[case::empty("")]
#[case::dot_dot_path("../etc/passwd")]
#[case::nested("ci/extra.yml")]
#[case::backslash("ci\\extra.yml")]
fn action_workflow_invalid_forms_rejected(#[case] workflow: &str) {
    let toml = build_with_workflow(workflow);
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err(&format!("workflow={:?} must reject", workflow));
    assert!(
        errors.iter().any(|e| matches!(
            e,
            gcit::config::ConfigError::Validate { field, .. } if field == "action.workflow"
        )),
        "expected action.workflow Validate for {:?}, got {:#?}",
        workflow,
        errors,
    );
}

// ---------------------------------------------------------------------
// http.max_concurrent (deprecated). Field accepted for back-compat,
// no longer wired into a Semaphore. validate_http emits a deprecation
// warning and drops the value. No bounds-checking — any usize is
// accepted.
// ---------------------------------------------------------------------
fn build_with_max_concurrent(n: usize) -> String {
    format!(
        r#"
[http]
max_concurrent = {}
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
        n,
    )
}

#[test]
fn http_max_concurrent_zero_accepted_as_deprecated() {
    // Pre-deprecation, max_concurrent=0 was rejected because it
    // would have stalled the Semaphore. Post-deprecation, the
    // field is no longer wired into a Semaphore, so any value
    // (including 0) is accepted with a deprecation warning.
    let toml = build_with_max_concurrent(0);
    gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect("max_concurrent=0 must be accepted under deprecation");
}

#[test]
fn http_max_concurrent_large_accepted_as_deprecated() {
    // Same logic: pre-deprecation upper bound was 4096; post-
    // deprecation any usize is accepted.
    let toml = build_with_max_concurrent(99_999);
    gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect("oversize max_concurrent must be accepted under deprecation");
}

#[test]
fn http_max_concurrent_typical_accepted() {
    // The common operator value (16, the documented default
    // before deprecation) keeps loading without error.
    let toml = build_with_max_concurrent(16);
    gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect("max_concurrent=16 (former default) must be accepted");
}

// ---------------------------------------------------------------------
// Bare template variables rejected at config load.
// ---------------------------------------------------------------------
fn build_with_discord_title(title: &str) -> String {
    format!(
        r#"
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
[[flow.destination]]
kind = "discord_webhook"
credential_id = "c"
[flow.destination.template]
title = "{}"
"#,
        title,
    )
}

#[rstest]
#[case::bare_namespace("{{flow}}")]
#[case::bare_unknown("{{gcit_run_id}}")]
#[case::bare_at_start("{{flow}}: prefix")]
fn template_bare_name_rejected(#[case] title: &str) {
    let toml = build_with_discord_title(title);
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err(&format!("title={:?} must reject", title));
    let any_validate = errors.iter().any(|e| {
        matches!(
            e,
            gcit::config::ConfigError::Validate { field, .. }
            if field == "destination.template.title"
        )
    });
    assert!(
        any_validate,
        "expected template.title Validate, got {:#?}",
        errors,
    );
}

#[test]
fn template_namespaced_variable_accepted() {
    let toml = build_with_discord_title("flow {{flow.name}} done at {{action.dispatched_at}}");
    gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect("namespaced template is valid");
}

#[test]
fn template_unknown_dotted_leaf_rejected() {
    // {{flow.naem}} (typo of name) is dotted and so passes the bare-
    // name check, but the probe-context render in strict mode catches
    // it as a missing variable.
    let toml = build_with_discord_title("{{flow.naem}}");
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err("typo in dotted leaf must reject");
    assert!(
        errors.iter().any(|e| matches!(
            e,
            gcit::config::ConfigError::Validate { field, .. }
            if field == "destination.template.title"
        )),
        "expected template Validate for typo'd dotted leaf, got {:#?}",
        errors,
    );
}

// ---------------------------------------------------------------------
// Cross-kind field rejection. Each destination kind has its own field
// shape; mixing fields across kinds is rejected at config load so a
// silent typo doesn't survive into runtime.
// ---------------------------------------------------------------------

#[test]
fn discord_destination_with_user_field_rejected() {
    // validate_discord rejects `user` on a discord_webhook destination
    // because `user` belongs to local_mail. The error names
    // destination.user so the operator's fix is unambiguous (either
    // remove `user` or change `kind`).
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
[[flow.destination]]
kind = "discord_webhook"
credential_id = "c"
user = "ops"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("user on discord_webhook must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, message, .. } => {
            field == "destination.user"
                && message.contains("local_mail")
                && message.contains("not valid on discord_webhook")
        }
        _ => false,
    });
    assert!(
        any,
        "expected destination.user rejection on discord_webhook: {:#?}",
        errors,
    );
}

#[test]
fn local_mail_destination_with_credential_id_rejected() {
    // validate_local_mail rejects `credential_id` on a local_mail
    // destination — local mail authenticates via the unix user, not via
    // a credential.
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
[[flow.destination]]
kind = "local_mail"
user = "ops"
credential_id = "extra-id"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("credential_id on local_mail must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, message, .. } => {
            field == "destination.credential_id"
                && message.contains("discord_webhook")
                && message.contains("not valid on local_mail")
        }
        _ => false,
    });
    assert!(
        any,
        "expected destination.credential_id rejection on local_mail: {:#?}",
        errors,
    );
}

#[test]
fn discord_template_with_subject_field_rejected() {
    // validate_discord_template rejects `subject` (and `body`) inside a
    // discord_webhook destination's template — those fields belong to
    // local_mail's mboxrd format, not Discord's embed.
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
[[flow.destination]]
kind = "discord_webhook"
credential_id = "c"
[flow.destination.template]
subject = "should not be here"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("template.subject on discord_webhook must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, message, .. } => {
            field == "destination.template.subject"
                && message.contains("local_mail")
                && message.contains("not valid on discord_webhook")
        }
        _ => false,
    });
    assert!(
        any,
        "expected template.subject rejection on discord_webhook: {:#?}",
        errors,
    );
}

#[test]
fn local_mail_template_with_discord_title_field_rejected() {
    // validate_local_mail_template rejects discord-specific template
    // fields (`title`, `description`, `field_name`, `field_value`,
    // `collapsed_summary`) on a local_mail destination.
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
[[flow.destination]]
kind = "local_mail"
user = "ops"
[flow.destination.template]
title = "should not be here"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("template.title on local_mail must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, message, .. } => {
            field == "destination.template.title"
                && message.contains("discord_webhook")
                && message.contains("not valid on local_mail")
        }
        _ => false,
    });
    assert!(
        any,
        "expected template.title rejection on local_mail: {:#?}",
        errors,
    );
}

#[test]
fn unknown_destination_kind_rejected() {
    // validate_destinations rejects any destination.kind outside the
    // {discord_webhook, local_mail} set with an actionable error naming
    // both supported variants.
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
[[flow.destination]]
kind = "matrix"
credential_id = "c"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("unknown destination.kind must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate {
            field,
            message,
            suggestion,
            ..
        } => {
            field == "destination.kind"
                && message.contains("\"matrix\"")
                && message.contains("discord_webhook")
                && message.contains("local_mail")
                && suggestion.contains("discord_webhook")
        }
        _ => false,
    });
    assert!(
        any,
        "expected destination.kind rejection naming valid variants: {:#?}",
        errors,
    );
}

#[test]
fn unknown_action_kind_rejected() {
    // validate_action rejects any action.kind outside the
    // {github_workflow_dispatch} set. The error must name the supplied
    // kind (with quotes) AND the only currently-valid kind so a future
    // expansion doesn't break operator-facing diagnostics.
    let raw = r#"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "gitlab_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("unknown action.kind must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, message, .. } => {
            field == "action.kind"
                && message.contains("\"gitlab_workflow_dispatch\"")
                && message.contains("github_workflow_dispatch")
        }
        _ => false,
    });
    assert!(
        any,
        "expected action.kind rejection naming valid kinds: {:#?}",
        errors,
    );
}

// ---------------------------------------------------------------------
// action.workflow empty vs path-traversal: distinct error messages
// for the two arms of validate_action's workflow check.
// ---------------------------------------------------------------------

#[test]
fn action_workflow_empty_string_emits_non_empty_error() {
    // The empty-string arm of validate_action emits a different message
    // from the path-traversal arm. The existing
    // action_workflow_invalid_forms_rejected test covers both arms
    // under one assertion; pin the empty-string message specifically
    // so a regression that swaps the two messages surfaces.
    let toml = build_with_workflow("");
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err("empty workflow must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, message, .. } => {
            field == "action.workflow" && message.contains("must be non-empty")
        }
        _ => false,
    });
    assert!(
        any,
        "expected workflow=\"\" to surface the non-empty message: {:#?}",
        errors,
    );
}

// ---------------------------------------------------------------------
// action.ref must start with "refs/" (validate_action arm). Existing
// source.ref test does NOT cover the action.ref arm.
// ---------------------------------------------------------------------

#[test]
fn action_ref_missing_refs_prefix_rejected() {
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
ref = "main"
credential_id = "c"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("action.ref without refs/ prefix must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate {
            field,
            message,
            suggestion,
            ..
        } => {
            field == "action.ref"
                && message.contains("must start with 'refs/'")
                && suggestion.contains("refs/heads/main")
        }
        _ => false,
    });
    assert!(
        any,
        "expected action.ref Validate with concrete suggestion: {:#?}",
        errors,
    );
}

// ---------------------------------------------------------------------
// action.credential_id missing rejected (validate_action's missing-
// credential_id arm).
// ---------------------------------------------------------------------

#[test]
fn action_missing_credential_id_rejected() {
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
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("missing action.credential_id must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, message, .. } => {
            field == "action.credential_id"
                && message.contains("required for github_workflow_dispatch")
        }
        _ => false,
    });
    assert!(
        any,
        "expected action.credential_id required Validate: {:#?}",
        errors,
    );
}

// ---------------------------------------------------------------------
// http.request_timeout bounds (validate_http via parse_bounded_duration).
// Existing tests cover other intervals but not the http-specific bounds
// [MIN_HTTP_TIMEOUT=1s, MAX_HTTP_TIMEOUT=300s].
// ---------------------------------------------------------------------

fn build_with_http_request_timeout(timeout: &str) -> String {
    format!(
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
        timeout,
    )
}

#[test]
fn http_request_timeout_below_min_rejected() {
    // MIN_HTTP_TIMEOUT = 1s. 500ms is below the bound;
    // parse_bounded_duration's `d < min` arm fires.
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
    // MAX_HTTP_TIMEOUT = 300s. 10m exceeds the bound.
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
    // The lower bound is INCLUSIVE per the MIN_HTTP_TIMEOUT doc
    // comment. 1s must load.
    let toml = build_with_http_request_timeout("1s");
    gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect("request_timeout=1s must be accepted at lower boundary");
}

#[test]
fn http_request_timeout_at_max_boundary_accepted() {
    // Upper bound is INCLUSIVE per the MAX_HTTP_TIMEOUT doc comment.
    // 300s/5min must load.
    let toml = build_with_http_request_timeout("5m");
    gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect("request_timeout=5m must be accepted at upper boundary");
}

// ---------------------------------------------------------------------
// poll.source_interval at MAX_INTERVAL=24h. The existing
// below_min_interval fixture covers MIN; pin MAX.
// ---------------------------------------------------------------------

#[test]
fn poll_source_interval_above_max_rejected() {
    // 25h > MAX_INTERVAL=24h. Drives parse_bounded_duration's `d > max`
    // arm.
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

// ---------------------------------------------------------------------
// case-confusable hint surfaces in the operator-facing message via
// case_confusable_hint inside parse_bounded_duration. A duration string
// containing uppercase 'M' parses successfully under humantime (months)
// but almost always indicates an operator typo for minutes. The hint
// surfaces in the bounded-out-of-range arm OR the parse-failure arm
// depending on the magnitude; an interval of "5M" parses to ~5 months
// which is way over MAX_INTERVAL=24h, so the bounded-out-of-range arm
// fires.
// ---------------------------------------------------------------------

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

// ---------------------------------------------------------------------
// fire_on with multiple distinct duplicates emits one Validate per
// duplicate occurrence (collect_fire_on). The existing
// fire_on_duplicate_event_rejected test only proves "one duplicate"
// surfaces; pin that two distinct duplicates surface as two errors.
// ---------------------------------------------------------------------

#[test]
fn fire_on_two_distinct_duplicates_each_emit_one_error() {
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
[[flow.destination]]
kind = "discord_webhook"
credential_id = "c"
fire_on = ["run_complete", "run_complete", "job_complete", "job_complete"]
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("multiple distinct fire_on duplicates must reject");
    let dup_run_complete = errors
        .iter()
        .filter(|e| match e {
            gcit::config::ConfigError::Validate { field, message, .. } => {
                field.contains("fire_on") && message.contains("duplicate event 'run_complete'")
            }
            _ => false,
        })
        .count();
    let dup_job_complete = errors
        .iter()
        .filter(|e| match e {
            gcit::config::ConfigError::Validate { field, message, .. } => {
                field.contains("fire_on") && message.contains("duplicate event 'job_complete'")
            }
            _ => false,
        })
        .count();
    assert_eq!(
        dup_run_complete, 1,
        "expected exactly one error per distinct duplicate; run_complete: {:#?}",
        errors,
    );
    assert_eq!(
        dup_job_complete, 1,
        "expected exactly one error per distinct duplicate; job_complete: {:#?}",
        errors,
    );
}

// ---------------------------------------------------------------------
// validate_spool_writability — host-state probe of /var/mail/<user>.
//
// `validate_spool_writability` is `pub` so integration tests can drive
// it with a tempdir-rooted spool root override. Each test pre-stages
// the tempdir to provoke a specific SpoolProbe variant inside
// `probe_spool_writability`, then asserts the resulting
// `Vec<ConfigError>` carries the right per-variant message + suggestion.
// Each SpoolProbe variant emits a distinct ConfigError shape —
// covering all four reachable variants here exercises validate.rs paths
// that no other test hits today.
// ---------------------------------------------------------------------

fn local_mail_only_config(user: &str) -> String {
    format!(
        r#"
[[flow]]
name = "spool-test-flow"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
[[flow.destination]]
kind = "local_mail"
user = "{}"
"#,
        user,
    )
}

#[test]
fn validate_spool_writability_writable_path_emits_no_error() {
    use std::os::unix::fs::PermissionsExt;
    // Build a tempdir spool root, place a writable file at <root>/<user>,
    // and call validate_spool_writability with that root. probe_spool_writability
    // returns SpoolProbe::Writable → no error pushed.
    let td = tempfile::TempDir::new().unwrap();
    let user = "testuser";
    let spool = td.path().join(user);
    std::fs::write(&spool, b"existing mbox").unwrap();
    // Mode 0o600 — owner-writable. probe_spool_writability uses access(W_OK)
    // which the kernel evaluates with the test process's effective uid;
    // the test process owns the file so the write check passes.
    std::fs::set_permissions(&spool, std::fs::Permissions::from_mode(0o600)).unwrap();

    let cfg = gcit::config::load_str(
        &local_mail_only_config(user),
        std::path::Path::new("inline"),
    )
    .expect("config must parse");
    let errors = gcit::config::validate::validate_spool_writability(&cfg, Some(td.path()));
    assert!(
        errors.is_empty(),
        "writable spool must emit no error; got: {:#?}",
        errors,
    );
}

#[test]
fn validate_spool_writability_parent_missing_emits_parent_missing_error() {
    // Spool root that does NOT exist on disk (a sub-path inside the
    // tempdir we never created). probe_spool_writability gets ENOENT
    // on the spool path AND ENOENT on the parent (the access(F_OK)
    // probe returns non-zero), so it returns
    // SpoolProbe::ParentMissing. validate_spool_writability emits the
    // "spool parent directory ... does not exist" message.
    let td = tempfile::TempDir::new().unwrap();
    let user = "anyuser";
    // Use a sub-dir of the tempdir as the "spool root" — the sub-dir
    // doesn't exist, so its parent (the spool path's parent) doesn't
    // exist either.
    let bogus_root = td.path().join("does-not-exist-parent");
    let cfg = gcit::config::load_str(
        &local_mail_only_config(user),
        std::path::Path::new("inline"),
    )
    .expect("config must parse");
    let errors = gcit::config::validate::validate_spool_writability(&cfg, Some(&bogus_root));
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate {
            field,
            message,
            suggestion,
            ..
        } => {
            field == "destination.local_mail.user"
                && message.contains("spool parent directory")
                && message.contains("does not exist")
                && (suggestion.contains("mailutils")
                    || suggestion.contains("postfix")
                    || suggestion.contains("mkdir"))
        }
        _ => false,
    });
    assert!(
        any,
        "expected ParentMissing error with mailutils/postfix/mkdir suggestion; got: {:#?}",
        errors,
    );
}

#[test]
fn validate_spool_writability_spool_missing_emits_spool_missing_error() {
    // Spool root EXISTS but the per-user spool file does NOT.
    // access(W_OK) on <root>/<user> returns ENOENT; access(F_OK) on
    // the parent (the tempdir itself) returns 0 → SpoolProbe::SpoolMissing.
    // validate_spool_writability emits the "spool file ... does not exist"
    // message.
    let td = tempfile::TempDir::new().unwrap();
    let user = "missing-user";
    // Don't create <td>/<user> — let access(W_OK) hit ENOENT on it.
    let cfg = gcit::config::load_str(
        &local_mail_only_config(user),
        std::path::Path::new("inline"),
    )
    .expect("config must parse");
    let errors = gcit::config::validate::validate_spool_writability(&cfg, Some(td.path()));
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate {
            field,
            message,
            suggestion,
            value,
            ..
        } => {
            field == "destination.local_mail.user"
                && value == user
                && message.contains("spool file")
                && message.contains("does not exist")
                && message.contains("does not auto-create")
                && (suggestion.contains("touch") || suggestion.contains("useradd"))
        }
        _ => false,
    });
    assert!(
        any,
        "expected SpoolMissing error naming the user with touch/useradd suggestion; got: {:#?}",
        errors,
    );
}

#[test]
fn validate_spool_writability_not_writable_emits_not_writable_error() {
    use std::os::unix::fs::PermissionsExt;
    // Skip when running as root — root traverses 0o400 and access(W_OK)
    // would still succeed because root bypasses the DAC write check.
    // Without that bypass the kernel returns EACCES; we need EACCES to
    // drive SpoolProbe::NotWritable.
    if euid_is_root() {
        eprintln!(
            "validate_spool_writability_not_writable_emits_not_writable_error: skipped — \
             test requires non-root euid (root bypasses DAC W_OK check)",
        );
        return;
    }
    // Spool file exists but is mode 0o400 (owner-readable, NOT writable).
    // access(W_OK) returns EACCES → SpoolProbe::NotWritable.
    let td = tempfile::TempDir::new().unwrap();
    let user = "readonly-user";
    let spool = td.path().join(user);
    std::fs::write(&spool, b"readonly mbox").unwrap();
    std::fs::set_permissions(&spool, std::fs::Permissions::from_mode(0o400)).unwrap();

    let cfg = gcit::config::load_str(
        &local_mail_only_config(user),
        std::path::Path::new("inline"),
    )
    .expect("config must parse");
    let errors = gcit::config::validate::validate_spool_writability(&cfg, Some(td.path()));
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate {
            field,
            message,
            suggestion,
            ..
        } => {
            field == "destination.local_mail.user"
                && message.contains("not writable")
                && (suggestion.contains("ReadWritePaths") || suggestion.contains("chmod 0660"))
        }
        _ => false,
    });
    assert!(
        any,
        "expected NotWritable error with chmod/ReadWritePaths suggestion; got: {:#?}",
        errors,
    );
}

#[test]
fn validate_spool_writability_no_local_mail_destinations_emits_no_error() {
    // Discord-only config has no LocalMail destinations — the inner
    // `if let Destination::LocalMail(lm) = dest` guard inside
    // validate_spool_writability fails for every destination, so
    // probe_spool_writability is never called and the error vec stays
    // empty.
    let raw = r#"
[[flow]]
name = "discord-only"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
[[flow.destination]]
kind = "discord_webhook"
credential_id = "c"
"#;
    let cfg = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect("discord-only config must parse");
    let td = tempfile::TempDir::new().unwrap();
    let errors = gcit::config::validate::validate_spool_writability(&cfg, Some(td.path()));
    assert!(
        errors.is_empty(),
        "discord-only config must emit zero spool errors regardless of spool root; got: {:#?}",
        errors,
    );
}

#[test]
fn validate_spool_writability_default_spool_root_uses_var_mail() {
    // spool_root = None resolves to mail::DEFAULT_SPOOL_DIR
    // (`/var/mail`). For a config with
    // local_mail destination naming a user that almost certainly
    // doesn't have a /var/mail/<random> spool, we expect SOME error
    // (Writable for an unlikely-existing user would be a coincidence).
    // Pin: error vec is non-empty AND every error names the user.
    //
    // Exception: skip when running as root because root may bypass
    // access checks on /var/mail. The test is fundamentally about
    // routing through the default-path branch; the user value is
    // randomized so a host with that user pre-configured is
    // statistically impossible.
    // 32-char max for local_mail.user (MAX_LOCAL_MAIL_USER_LEN).
    // Stay under 32 chars while keeping a low collision risk on dev hosts.
    let user = "gcit-spool-test-9f3a2b-no-host";
    let cfg = gcit::config::load_str(
        &local_mail_only_config(user),
        std::path::Path::new("inline"),
    )
    .expect("config must parse");
    // spool_root = None → DEFAULT_SPOOL_DIR (/var/mail) is used.
    let errors = gcit::config::validate::validate_spool_writability(&cfg, None);
    // Either ParentMissing (no /var/mail), SpoolMissing
    // (/var/mail exists but no /var/mail/gcit-spool-...), or
    // NotWritable — but NEVER Writable for a randomly-named user. At
    // least one error must surface.
    assert!(
        !errors.is_empty(),
        "default spool root with bogus user must emit at least one error: {:#?}",
        errors,
    );
    let any_names_user = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { value, .. } => value == user,
        _ => false,
    });
    assert!(
        any_names_user,
        "every error from default-spool-root path must name the offending user; got: {:#?}",
        errors,
    );
}

#[test]
fn validate_spool_writability_two_local_mail_destinations_emit_one_error_each() {
    // Two local_mail destinations with distinct users; both spool
    // files missing → two separate SpoolMissing errors. Pins the
    // per-destination iteration in validate_spool_writability (the
    // outer `for flow in &cfg.flow` and inner `for dest in
    // &flow.destination` loops both fire). Catches a regression where
    // the loop short-circuits after the first error.
    let raw = r#"
[[flow]]
name = "multi-dest"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
[[flow.destination]]
kind = "local_mail"
user = "alice"
[[flow.destination]]
kind = "local_mail"
user = "bob"
"#;
    let cfg = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect("multi-local_mail config must parse");
    let td = tempfile::TempDir::new().unwrap();
    let errors = gcit::config::validate::validate_spool_writability(&cfg, Some(td.path()));
    let alice_err = errors
        .iter()
        .filter(|e| match e {
            gcit::config::ConfigError::Validate { value, .. } => value == "alice",
            _ => false,
        })
        .count();
    let bob_err = errors
        .iter()
        .filter(|e| match e {
            gcit::config::ConfigError::Validate { value, .. } => value == "bob",
            _ => false,
        })
        .count();
    assert_eq!(
        alice_err, 1,
        "expected exactly one error naming 'alice'; got: {:#?}",
        errors,
    );
    assert_eq!(
        bob_err, 1,
        "expected exactly one error naming 'bob'; got: {:#?}",
        errors,
    );
}

/// True if the running process's effective uid is 0. Used to skip
/// tests that depend on DAC behavior root bypasses.
fn euid_is_root() -> bool {
    // SAFETY: geteuid() is async-signal-safe and always succeeds.
    unsafe { libc::geteuid() == 0 }
}

// ---------------------------------------------------------------------
// parse_bounded_duration parse-failure path emits ConfigError::Parse
// (NOT Validate). The two error categories carry different fix-it
// shapes: Parse → "fix the syntax", Validate → "pick a different
// number". The existing below_min_interval fixture covers the
// Validate arm; this test covers the distinct Parse arm.
// ---------------------------------------------------------------------

#[test]
fn malformed_humantime_duration_emits_parse_error_not_validate() {
    // "xyz" is not a valid humantime duration — humantime::parse_duration
    // returns Err. parse_bounded_duration's Err arm pushes
    // ConfigError::Parse with the "{field} = {raw:?}: invalid duration: ..."
    // message. Pin: a Parse variant exists for the right field, NOT a
    // Validate variant.
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
    // Symmetrically: NO Validate variant for source_interval — the
    // parse failure short-circuits before the bounded check.
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

// ---------------------------------------------------------------------
// IdError variants surfaced via credential_id rejection. Each variant
// of `config::credential::IdError` (Empty, TooLong, InvalidChar,
// LeadingHyphen, PathTraversal) drives a different operator-facing
// message + suggestion via `validate.rs::map_id_error`. The existing
// credential_id_collision fixture covers env-var collision but NONE
// of the per-variant IdError paths.
// ---------------------------------------------------------------------

fn build_with_credential_id(id: &str) -> String {
    format!(
        r#"
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
credential_id = "{}"
"#,
        id,
    )
}

#[test]
fn credential_id_with_invalid_char_rejected_with_named_char() {
    // IdError::InvalidChar { ch } message names the offending character
    // (e.g. ' ' or '@'). Pin that the message names the specific char
    // so the operator sees what to remove.
    let toml = build_with_credential_id("foo@bar");
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err("invalid char in credential_id must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate {
            field,
            message,
            suggestion,
            ..
        } => {
            field == "action.credential_id"
                && message.contains("invalid character")
                && message.contains("'@'")
                && suggestion.contains("A-Z, a-z, 0-9, '_', and '-'")
        }
        _ => false,
    });
    assert!(
        any,
        "expected InvalidChar error naming '@' with charset suggestion; got: {:#?}",
        errors,
    );
}

#[test]
fn credential_id_starting_with_hyphen_rejected_with_cli_flag_warning() {
    // IdError::LeadingHyphen: leading '-' conflicts with CLI flags.
    // Pin the operator-facing wording.
    let toml = build_with_credential_id("-leading-hyphen");
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err("credential_id starting with - must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate {
            field,
            message,
            suggestion,
            ..
        } => {
            field == "action.credential_id"
                && message.contains("must not start with '-'")
                && message.contains("CLI flags")
                && (suggestion.contains("github_pat") || suggestion.contains("non-empty id"))
        }
        _ => false,
    });
    assert!(
        any,
        "expected LeadingHyphen error mentioning CLI flag conflict; got: {:#?}",
        errors,
    );
}

#[test]
fn credential_id_with_path_traversal_chars_rejected() {
    // IdError::PathTraversal covers '..', '/', '\', '~', or NUL. Try
    // '/' specifically because that's the most common operator typo
    // (using "discord/webhook" instead of "discord-webhook").
    let toml = build_with_credential_id("discord/webhook");
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err("credential_id with '/' must reject");
    // Could surface as either PathTraversal (the dedicated arm) OR
    // InvalidChar (since '/' is also outside [a-zA-Z0-9_-]). The
    // production code (CredentialId parser) must check path-traversal
    // BEFORE the charset; pin EITHER the path-traversal message OR
    // the invalid-char message so the test catches whichever the
    // production code emits without over-specifying the implementation
    // choice.
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, message, .. } => {
            field == "action.credential_id"
                && (message.contains("'..'") || message.contains("invalid character"))
        }
        _ => false,
    });
    assert!(
        any,
        "expected PathTraversal or InvalidChar error for '/'; got: {:#?}",
        errors,
    );
}

#[test]
fn credential_id_too_long_rejected_with_max_chars_message() {
    // IdError::TooLong names the supplied length and the max. The
    // MAX_LEN constant lives in credential.rs; we don't know its
    // exact value here but a 200-char id is definitely over the limit.
    let long_id = "a".repeat(200);
    let toml = build_with_credential_id(&long_id);
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err("200-char credential_id must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate {
            field,
            message,
            suggestion,
            ..
        } => {
            field == "action.credential_id"
                && message.contains("200 chars")
                && message.contains("max is")
                && suggestion.contains("shorten")
        }
        _ => false,
    });
    assert!(
        any,
        "expected TooLong error naming '200 chars' and 'shorten' suggestion; got: {:#?}",
        errors,
    );
}

// ---------------------------------------------------------------------
// source.credential_id surfaces through validate_credential_id with the
// "source.credential_id" field tag (set inside validate_source). The
// existing per-IdError tests above all drive action.credential_id; pin
// that the SAME map_id_error path produces a source.credential_id
// Validate when the bad id appears under [flow.source].
// ---------------------------------------------------------------------

#[test]
fn source_credential_id_invalid_char_emits_source_field_validate() {
    let raw = r#"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
credential_id = "with space"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("source.credential_id with space must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, message, .. } => {
            field == "source.credential_id" && message.contains("invalid character")
        }
        _ => false,
    });
    assert!(
        any,
        "expected source.credential_id Validate error; got: {:#?}",
        errors,
    );
}

#[test]
fn source_credential_id_empty_string_rejected_at_source_field() {
    // The Empty IdError variant fires here even though the value is a
    // present TOML string — the validator routes through
    // validate_credential_id::map_id_error with field=source.credential_id.
    let raw = r#"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
credential_id = ""
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("empty source.credential_id must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, .. } => field == "source.credential_id",
        _ => false,
    });
    assert!(
        any,
        "expected source.credential_id Validate; got: {:#?}",
        errors,
    );
}

// ---------------------------------------------------------------------
// destination.discord_webhook.credential_id rejection arm — symmetric
// to source.credential_id and action.credential_id but uses the longer
// field tag set inside validate_discord. Pin that bad ids under
// [[flow.destination]] produce that specific field tag.
// ---------------------------------------------------------------------

#[test]
fn destination_discord_credential_id_invalid_char_emits_destination_field_validate() {
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
credential_id = "ok"
[[flow.destination]]
kind = "discord_webhook"
credential_id = "bad space"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("destination.credential_id with space must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, .. } => {
            field == "destination.discord_webhook.credential_id"
        }
        _ => false,
    });
    assert!(
        any,
        "expected destination.discord_webhook.credential_id Validate; got: {:#?}",
        errors,
    );
}

// ---------------------------------------------------------------------
// destination.credential_id missing on discord_webhook surfaces a
// Validate at the kind line (validate_discord's missing-credential_id
// arm).
// ---------------------------------------------------------------------

#[test]
fn discord_webhook_destination_without_credential_id_rejected() {
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
[[flow.destination]]
kind = "discord_webhook"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("discord_webhook missing credential_id must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, message, .. } => {
            field == "destination.credential_id" && message.contains("required for discord_webhook")
        }
        _ => false,
    });
    assert!(
        any,
        "expected destination.credential_id required for discord_webhook; got: {:#?}",
        errors,
    );
}

// ---------------------------------------------------------------------
// local_mail destination missing user surfaces Validate at the kind
// line (validate_local_mail's missing-user arm).
// ---------------------------------------------------------------------

#[test]
fn local_mail_destination_without_user_rejected() {
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
[[flow.destination]]
kind = "local_mail"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("local_mail without user must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, message, .. } => {
            field == "destination.user" && message.contains("required for local_mail")
        }
        _ => false,
    });
    assert!(
        any,
        "expected destination.user required for local_mail; got: {:#?}",
        errors,
    );
}

// ---------------------------------------------------------------------
// flow.poll.job_interval bounded check (validate_poll_override's
// job_interval arm). Existing tests cover flow.poll.source_interval
// below_min and flow.poll.jitter out_of_range, but not the
// job_interval branch.
// ---------------------------------------------------------------------

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

// ---------------------------------------------------------------------
// poll.job_interval (top-level default, validate_poll_defaults's
// job_interval arm). Existing tests do not cover top-level
// job_interval out-of-range.
// ---------------------------------------------------------------------

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

// ---------------------------------------------------------------------
// poll.jitter (top-level default, validate_poll_defaults's jitter
// arm). Existing tests do not cover top-level jitter validation.
// ---------------------------------------------------------------------

#[test]
fn poll_default_jitter_negative_rejected() {
    // MIN_JITTER=0.0 inclusive; -0.1 fails parse_jitter's range check.
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

// ---------------------------------------------------------------------
// Template compilation failure surfaces as ConfigError::TemplateCompile,
// distinct from the AST-level Validate that find_bare_name produces.
// Unbalanced `{{` is a handlebars parse error that kicks in BEFORE the
// bare-name AST check — pinned because the two errors have different
// operator semantics ("syntax broken" vs "use a dotted form").
// ---------------------------------------------------------------------

#[test]
fn template_with_unbalanced_braces_emits_template_compile_error() {
    // Unbalanced `{{` — handlebars's parser rejects with a TemplateError.
    // validate_discord_template's compile_template_field pushes
    // ConfigError::TemplateCompile. Pin: the variant is TemplateCompile
    // (NOT Validate) — different display shape and different operator
    // fix-it.
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
[[flow.destination]]
kind = "discord_webhook"
credential_id = "c"
[flow.destination.template]
title = "{{ unterminated"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("unbalanced braces must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::TemplateCompile { field, flow, .. } => {
            field == "destination.template.title" && flow == "x"
        }
        _ => false,
    });
    assert!(
        any,
        "expected TemplateCompile for unbalanced braces; got: {:#?}",
        errors,
    );
}

// ---------------------------------------------------------------------
// All five Discord template fields under cross-kind rejection on
// local_mail (validate_local_mail_template's discord-only-fields loop).
// Existing tests only exercise the `title` row in that loop; pin every
// other field so the loop's per-field iteration cannot be partially
// short-circuited by a future refactor.
// ---------------------------------------------------------------------

#[test]
fn local_mail_template_with_description_field_rejected() {
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
[[flow.destination]]
kind = "local_mail"
user = "ops"
[flow.destination.template]
description = "should not be here"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("template.description on local_mail must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, message, .. } => {
            field == "destination.template.description"
                && message.contains("discord_webhook")
                && message.contains("not valid on local_mail")
        }
        _ => false,
    });
    assert!(
        any,
        "expected template.description rejection on local_mail: {:#?}",
        errors,
    );
}

#[test]
fn local_mail_template_with_field_name_rejected() {
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
[[flow.destination]]
kind = "local_mail"
user = "ops"
[flow.destination.template]
field_name = "should not be here"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("template.field_name on local_mail must reject");
    assert!(
        errors.iter().any(|e| matches!(
            e,
            gcit::config::ConfigError::Validate { field, .. }
            if field == "destination.template.field_name"
        )),
        "expected template.field_name rejection: {:#?}",
        errors,
    );
}

#[test]
fn discord_template_with_body_field_rejected() {
    // Mirror of discord_template_with_subject_field_rejected for the
    // `body` field (validate_discord_template's local-mail-only fields
    // loop iterates over both subject and body — pin the body row too).
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
[[flow.destination]]
kind = "discord_webhook"
credential_id = "c"
[flow.destination.template]
body = "should not be here"
"#;
    let errors = gcit::config::load_str(raw, std::path::Path::new("inline"))
        .expect_err("template.body on discord_webhook must reject");
    let any = errors.iter().any(|e| match e {
        gcit::config::ConfigError::Validate { field, message, .. } => {
            field == "destination.template.body"
                && message.contains("local_mail")
                && message.contains("not valid on discord_webhook")
        }
        _ => false,
    });
    assert!(
        any,
        "expected template.body rejection on discord_webhook: {:#?}",
        errors,
    );
}

// ---------------------------------------------------------------------
// Empty flow.name surfaces a Validate that names the non-empty rule.
// The validator's empty-name arm in validate_flow_name emits a
// distinct message from the length+charset arms; pin the wording so
// a regression that conflates the arms surfaces here.
// ---------------------------------------------------------------------

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

// ---------------------------------------------------------------------
// poll.jitter at exact boundaries 0.0 and 0.5 must be accepted
// (inclusive per parse_jitter's MIN_JITTER..=MAX_JITTER predicate).
// The existing flow.poll.jitter test only exercises out-of-range;
// pin acceptance at both ends so a regression to a strict-less-than
// predicate would surface.
// ---------------------------------------------------------------------

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

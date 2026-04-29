// Schema rejects unknown keys, never silently ignores.
// Every struct uses #[serde(deny_unknown_fields)].
//
// Each test injects an unknown key into a different struct and asserts
// the loader rejects it. Catches a regression where someone forgets the
// attribute on a new sub-struct.
//
// Mutation target: the `deny_unknown_fields` attribute on each struct.
// cargo-mutants can flip serde attributes; this test catches.

fn assert_loads_with_error_containing(toml: &str, needle: &str) {
    let errors = gcit::config::load_str(toml, std::path::Path::new("inline"))
        .expect_err("must fail to load");
    let any = errors.iter().any(|e| e.to_string().contains(needle));
    assert!(
        any,
        "expected error containing {:?} in: {:#?}",
        needle, errors
    );
}

#[test]
fn unknown_field_at_root_rejected() {
    let raw = r#"
unknown_root_key = 42
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
    assert_loads_with_error_containing(raw, "unknown_root_key");
}

#[test]
fn unknown_field_in_poll_rejected() {
    let raw = r#"
[poll]
nonsense = "x"
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
    assert_loads_with_error_containing(raw, "nonsense");
}

#[test]
fn unknown_field_in_http_rejected() {
    let raw = r#"
[http]
nonsense = 1
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
    assert_loads_with_error_containing(raw, "nonsense");
}

#[test]
fn unknown_field_in_flow_rejected() {
    let raw = r#"
[[flow]]
name = "x"
extra = true
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
    assert_loads_with_error_containing(raw, "extra");
}

#[test]
fn unknown_field_in_action_rejected() {
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
extra_action_key = "x"
"#;
    assert_loads_with_error_containing(raw, "extra_action_key");
}

#[test]
fn unknown_field_in_destination_template_rejected() {
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
extra_key = "x"
"#;
    assert_loads_with_error_containing(raw, "extra_key");
}

#[test]
fn unknown_kind_in_destination_rejected() {
    // The destination schema parses as a flat struct (RawDestination) —
    // serde's tagged-enum dispatch is incompatible with toml::Spanned,
    // so the validator reads `kind` explicitly and dispatches by hand.
    // An unknown kind therefore surfaces as ConfigError::Validate from
    // the validator, not as a serde "unknown variant" parse error. The
    // user-facing message must still name the unknown kind so the
    // operator can fix it.
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
kind = "matrix_room"
credential_id = "c"
"#;
    assert_loads_with_error_containing(raw, "matrix_room");
}

// action.* field validation: repo, workflow, ref, kind, and the
// missing-required-field paths. action.credential_id IdError variants
// live in `credentials.rs` since they share the per-variant message
// logic with the source.credential_id and destination.credential_id
// rejection arms.

use rstest::rstest;

use super::common::{build_with_repo, build_with_workflow};

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

#[test]
fn action_workflow_empty_string_emits_non_empty_error() {
    // The empty-string arm of validate_action emits a different message
    // from the path-traversal arm. Pin the empty-string message
    // specifically so a regression that swaps the two messages surfaces.
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

#[test]
fn unknown_action_kind_rejected() {
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

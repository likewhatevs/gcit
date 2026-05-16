// Template validation: bare-name rejection, namespaced acceptance,
// dotted-leaf typos, cross-kind template field rejection (every
// Discord field rejected on local_mail, and the inverse), and
// TemplateCompile errors from unbalanced braces.

use rstest::rstest;

use super::common::build_with_discord_title;

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

// Discord template rejected on local_mail destination.

#[test]
fn local_mail_template_with_discord_title_field_rejected() {
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

// local_mail template fields rejected on discord_webhook.

#[test]
fn discord_template_with_subject_field_rejected() {
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
fn discord_template_with_body_field_rejected() {
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

#[test]
fn template_with_unbalanced_braces_emits_template_compile_error() {
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

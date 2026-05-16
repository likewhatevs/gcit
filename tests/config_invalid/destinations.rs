// local_mail user validation, fire_on duplicates, cross-kind field
// rejection, source.url scheme/parse, and gcit_run_id input rejection.
// All destination-shape and source-shape validation that isn't about
// credentials or templates lives here.

use rstest::rstest;

use super::common::build_local_mail_user;

#[test]
fn fire_on_duplicate_event_rejected() {
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
    assert_eq!(dup_run_complete, 1);
    assert_eq!(dup_job_complete, 1);
}

#[test]
fn empty_fire_on_array_is_accepted_and_destination_carries_empty_vec() {
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
fn source_url_unsupported_scheme_rejected() {
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
fn action_inputs_user_supplied_gcit_run_id_rejected() {
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

// local_mail.user validation.

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

// Cross-kind field rejection.

#[test]
fn discord_destination_with_user_field_rejected() {
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
fn unknown_destination_kind_rejected() {
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
        "expected destination.user required for local_mail: {:#?}",
        errors,
    );
}

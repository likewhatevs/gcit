// credential_id IdError variants (Empty, TooLong, InvalidChar,
// LeadingHyphen, PathTraversal) surfaced via the action / source /
// destination paths. The validate_credential_id::map_id_error
// dispatch logic owns the per-variant message + suggestion shapes;
// every consumer (action.credential_id, source.credential_id,
// destination.discord_webhook.credential_id) routes through the
// same logic and the field tag differs only.

use super::common::build_with_credential_id;

#[test]
fn credential_id_with_invalid_char_rejected_with_named_char() {
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
    let toml = build_with_credential_id("discord/webhook");
    let errors = gcit::config::load_str(&toml, std::path::Path::new("inline"))
        .expect_err("credential_id with '/' must reject");
    // Could surface as either PathTraversal (the dedicated arm) OR
    // InvalidChar (since '/' is also outside [a-zA-Z0-9_-]). Pin
    // EITHER so the test catches whichever the production code
    // emits without over-specifying.
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

// source.credential_id surfaces through validate_credential_id with
// the "source.credential_id" field tag (set inside validate_source).

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

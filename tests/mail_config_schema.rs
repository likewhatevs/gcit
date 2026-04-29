// LocalMailConfig TOML round-trip + multi-destination + fire_on
// default.
//
// The pipeline under test is `gcit::config::load_str(source, path)`
// which:
//   1. Parses TOML via serde into the raw, span-preserving schema
//      (src/config/parse.rs::RawConfig + RawDestination). All raw
//      structs carry `#[serde(deny_unknown_fields)]`; the destination
//      template carries the union of Discord + local_mail field
//      names with the same deny.
//   2. Walks the raw schema in `src/config/validate.rs::validate`,
//      dispatching on `[[flow.destination]] kind` to either
//      `validate_local_mail` or `validate_discord_webhook`. Domain
//      rules (user charset, max length, required fields) fire as
//      `ConfigError::Validate` here.
//
// Tests in this file exercise both layers via the public load_str
// entry point. Parse-layer failures (unknown TOML fields) surface
// as `ConfigError::Parse`; validate-layer failures (bad user
// charset, missing required local_mail.user) surface as
// `ConfigError::Validate`.

use std::path::Path;

use rstest::rstest;

use gcit::config::{load_str, ConfigError, Destination, FireEvent, LocalMailTemplateConfig};

/// Common preamble: a minimal valid config skeleton up through the
/// flow's source + action blocks, with a placeholder destination
/// block left for the caller to append. Tests build a full document
/// by concatenating this with a destination snippet.
const PREAMBLE: &str = r#"
[[flow]]
name = "f"
description = "test flow"

[flow.source]
url = "https://example.com/repo.git"
ref = "refs/heads/main"

[flow.action]
kind          = "github_workflow_dispatch"
repo          = "owner/repo"
workflow      = "ci.yml"
ref           = "refs/heads/main"
credential_id = "github_pat"
inputs        = {}
"#;

fn parse_ok(source: &str) -> gcit::config::Config {
    load_str(source, Path::new("test.toml"))
        .unwrap_or_else(|errs| panic!("expected valid config; errors: {errs:#?}"))
}

fn parse_err(source: &str) -> Vec<ConfigError> {
    load_str(source, Path::new("test.toml")).expect_err("expected an error")
}

#[test]
fn local_mail_destination_toml_round_trip() {
    // Canonical example: kind = "local_mail", user, fire_on. The
    // parsed Config should carry one flow with one Destination::
    // LocalMail variant whose fields match the input.
    //
    // Mutation target: the serde tag string drifts (e.g., changing
    // `rename_all = "snake_case"` to "kebab-case"). The
    // canonical TOML examples in the repo would stop parsing.
    let src = format!(
        "{PREAMBLE}\n\
         [[flow.destination]]\n\
         kind    = \"local_mail\"\n\
         user    = \"ops\"\n\
         fire_on = [\"run_complete\"]\n",
    );
    let cfg = parse_ok(&src);
    assert_eq!(cfg.flow.len(), 1);
    assert_eq!(cfg.flow[0].destination.len(), 1);
    let Destination::LocalMail(local) = &cfg.flow[0].destination[0] else {
        panic!(
            "expected Destination::LocalMail; got {:?}",
            cfg.flow[0].destination[0],
        );
    };
    assert_eq!(local.user, "ops");
    assert_eq!(local.fire_on, vec![FireEvent::RunComplete]);
    // Template defaults to LocalMailTemplateConfig::default() (both
    // fields None) when no [flow.destination.template] block is
    // present. Pinned in detail by
    // local_mail_template_optional_fields_default_to_none below.
}

#[test]
fn local_mail_default_fire_on_is_run_complete() {
    // No `fire_on` key in the destination block → validator
    // populates `vec![FireEvent::RunComplete]` (validate.rs:1057).
    //
    // Mutation target: the default is changed to `vec![]` (would
    // silently disable the destination) or to the full set
    // `[RunStart, JobComplete, RunComplete]` (would surface
    // unwanted notifications for partial events).
    let src = format!(
        "{PREAMBLE}\n\
         [[flow.destination]]\n\
         kind = \"local_mail\"\n\
         user = \"ops\"\n",
    );
    let cfg = parse_ok(&src);
    let Destination::LocalMail(local) = &cfg.flow[0].destination[0] else {
        panic!("expected LocalMail variant");
    };
    assert_eq!(local.fire_on, vec![FireEvent::RunComplete]);
}

#[rstest]
#[case::valid_short("a", true)]
#[case::valid_typical("ops", true)]
#[case::valid_with_dash("alerts-team", true)]
#[case::valid_with_underscore("admin_42", true)]
// 32 chars: at the inclusive max; allowed.
#[case::valid_max_length("a234567890123456789012345678901a", true)]
// Empty string: caught by the "must be non-empty" branch
// (validate.rs:1004-1013).
#[case::invalid_empty("", false)]
// 33 chars: one byte over MAX_LOCAL_MAIL_USER_LEN.
#[case::invalid_too_long("a234567890123456789012345678901ab", false)]
// Space: not in the [A-Za-z0-9_-] charset (validate.rs:1029-1045).
#[case::invalid_space("ops user", false)]
// Slash: rejected by charset; also a path-traversal-shaped value.
#[case::invalid_slash("ops/user", false)]
// Period: rejected by charset (only `_` and `-` are allowed
// punctuation).
#[case::invalid_period("ops.team", false)]
// "@" sign: rejected by charset.
#[case::invalid_at_sign("user@example.com", false)]
// Non-ASCII codepoint: ascii_alphanumeric() returns false.
#[case::invalid_unicode("ops\u{00E9}", false)]
fn local_mail_user_validation_charset(#[case] user: &str, #[case] expect_valid: bool) {
    // Build a config whose only variable is the destination user.
    // The destination block's TOML quoting requires neutralizing any
    // double quotes the test fixture might contain — none of our
    // cases include `"`, so a literal embedding is fine. Empty user
    // is encoded as `user = ""`.
    let src = format!(
        "{PREAMBLE}\n\
         [[flow.destination]]\n\
         kind = \"local_mail\"\n\
         user = \"{user}\"\n",
    );
    let result = load_str(&src, Path::new("test.toml"));
    if expect_valid {
        result
            .unwrap_or_else(|errs| panic!("user {user:?} should be valid; got errors: {errs:#?}"));
    } else {
        let errs = result
            .err()
            .unwrap_or_else(|| panic!("user {user:?} should be invalid; parse succeeded"));
        // Every charset/length/empty failure surfaces as a
        // ConfigError::Validate naming the destination.local_mail.user
        // field (or destination.user for the missing-field case,
        // which isn't reached by this charset table).
        let saw_validate = errs.iter().any(|e| {
            matches!(e, ConfigError::Validate { field, .. }
                if field == "destination.local_mail.user")
        });
        assert!(
            saw_validate,
            "user {user:?} should produce ConfigError::Validate on destination.local_mail.user; got: {errs:#?}",
        );
    }
}

#[test]
fn flow_with_multiple_local_mail_destinations() {
    // Two local_mail destinations on the same flow with distinct
    // users. Each is a separate Destination entry in the validated
    // config.
    let src = format!(
        "{PREAMBLE}\n\
         [[flow.destination]]\n\
         kind = \"local_mail\"\n\
         user = \"ops\"\n\
         \n\
         [[flow.destination]]\n\
         kind = \"local_mail\"\n\
         user = \"alerts\"\n",
    );
    let cfg = parse_ok(&src);
    assert_eq!(cfg.flow[0].destination.len(), 2);
    let users: Vec<&str> = cfg.flow[0]
        .destination
        .iter()
        .map(|d| match d {
            Destination::LocalMail(l) => l.user.as_str(),
            other => panic!("expected LocalMail variant; got {other:?}"),
        })
        .collect();
    assert_eq!(users, vec!["ops", "alerts"]);
}

#[test]
fn flow_with_mixed_destinations() {
    // A flow with one discord_webhook + one local_mail. Pinned so a
    // schema change that breaks variant interleaving (e.g., a typo in
    // the kind dispatcher) surfaces here.
    let src = format!(
        "{PREAMBLE}\n\
         [[flow.destination]]\n\
         kind          = \"discord_webhook\"\n\
         credential_id = \"webhook\"\n\
         \n\
         [[flow.destination]]\n\
         kind = \"local_mail\"\n\
         user = \"ops\"\n",
    );
    let cfg = parse_ok(&src);
    assert_eq!(cfg.flow[0].destination.len(), 2);
    assert!(
        matches!(&cfg.flow[0].destination[0], Destination::DiscordWebhook(_)),
        "first destination must be DiscordWebhook; got {:?}",
        cfg.flow[0].destination[0],
    );
    assert!(
        matches!(&cfg.flow[0].destination[1], Destination::LocalMail(_)),
        "second destination must be LocalMail; got {:?}",
        cfg.flow[0].destination[1],
    );
}

#[test]
fn local_mail_template_optional_fields_default_to_none() {
    // No [flow.destination.template] block → both subject and body
    // default to None. Per src/config/parse.rs:157-161:
    //   pub struct LocalMailTemplateConfig {
    //     pub subject: Option<String>,
    //     pub body: Option<String>,
    //   }
    //
    // Mutation target: the validator default produces Some("")
    // instead of None; gcit then renders an empty Subject header
    // through handlebars/sanitize.
    let src = format!(
        "{PREAMBLE}\n\
         [[flow.destination]]\n\
         kind = \"local_mail\"\n\
         user = \"ops\"\n",
    );
    let cfg = parse_ok(&src);
    let Destination::LocalMail(local) = &cfg.flow[0].destination[0] else {
        panic!("expected LocalMail variant");
    };
    assert_eq!(local.template.subject, None);
    assert_eq!(local.template.body, None);
    // Sanity: this matches the Default impl too.
    assert_eq!(
        local.template.subject,
        LocalMailTemplateConfig::default().subject,
    );
    assert_eq!(local.template.body, LocalMailTemplateConfig::default().body,);
}

#[test]
fn local_mail_template_with_overrides() {
    // [flow.destination.template] with explicit subject + body.
    // Both arrive as Some(...) on the validated config.
    let src = format!(
        "{PREAMBLE}\n\
         [[flow.destination]]\n\
         kind = \"local_mail\"\n\
         user = \"ops\"\n\
         \n\
         [flow.destination.template]\n\
         subject = \"FAILED {{{{flow.name}}}}\"\n\
         body    = \"Custom body\"\n",
    );
    let cfg = parse_ok(&src);
    let Destination::LocalMail(local) = &cfg.flow[0].destination[0] else {
        panic!("expected LocalMail variant");
    };
    assert_eq!(
        local.template.subject.as_deref(),
        Some("FAILED {{flow.name}}"),
    );
    assert_eq!(local.template.body.as_deref(), Some("Custom body"));
}

#[test]
fn local_mail_kind_string_is_local_mail_snake_case() {
    // The serde tag for the LocalMail variant must be the literal
    // "local_mail" (snake_case). Operator config files include
    // `kind = "local_mail"`; renaming the variant or removing
    // rename_all would break every existing config.
    //
    // The validator dispatches on the literal kind string in
    // src/config/validate.rs:799 (`"local_mail" => ...`). This test
    // pins the input side: a TOML file with `kind = "local_mail"`
    // resolves to Destination::LocalMail. A drift-detector for the
    // kind string also lives at validate.rs:812-816 (the unknown-kind
    // error message lists "discord_webhook, local_mail" verbatim).
    let src = format!(
        "{PREAMBLE}\n\
         [[flow.destination]]\n\
         kind = \"local_mail\"\n\
         user = \"ops\"\n",
    );
    let cfg = parse_ok(&src);
    assert!(
        matches!(&cfg.flow[0].destination[0], Destination::LocalMail(_)),
        "kind = \"local_mail\" must dispatch to Destination::LocalMail",
    );
    // Also verify the negative case: a typo-style kind ("localmail")
    // produces an unknown-kind validation error rather than silently
    // skipping the destination.
    let bad_src = format!(
        "{PREAMBLE}\n\
         [[flow.destination]]\n\
         kind = \"localmail\"\n\
         user = \"ops\"\n",
    );
    let errs = parse_err(&bad_src);
    let saw_unknown = errs.iter().any(|e| match e {
        ConfigError::Validate { message, .. } => message.contains("local_mail"),
        _ => false,
    });
    assert!(
        saw_unknown,
        "unknown kind 'localmail' should surface a Validate error mentioning the valid kinds; got: {errs:#?}",
    );
}

#[test]
fn local_mail_unknown_field_rejected_by_deny_unknown_fields() {
    // Per src/config/parse.rs:290-305, RawDestination has
    // #[serde(deny_unknown_fields)] and lists exactly:
    //   kind, credential_id, user, fire_on, template
    // Any other key (e.g., `spool_path`) trips deny at parse time;
    // the error surfaces as ConfigError::Parse via map_toml_error.
    //
    // Mutation target: adding a new field to the daemon
    // but forgets to update the schema; a config carrying the field
    // would silently parse with the field ignored. deny_unknown_fields
    // is the seatbelt.
    let src = format!(
        "{PREAMBLE}\n\
         [[flow.destination]]\n\
         kind       = \"local_mail\"\n\
         user       = \"ops\"\n\
         spool_path = \"/var/mail/different\"\n",
    );
    let errs = parse_err(&src);
    // serde's TOML deserializer typically reports "unknown field
    // ... expected one of ..." with the offending field name. The
    // exact wording is a serde implementation detail; pin only the
    // variant + that the offending field name surfaces.
    let saw_parse_error_naming_field = errs.iter().any(|e| match e {
        ConfigError::Parse { message, .. } => message.contains("spool_path"),
        _ => false,
    });
    assert!(
        saw_parse_error_naming_field,
        "unknown destination field `spool_path` should surface a Parse error naming it; got: {errs:#?}",
    );
}

#[test]
fn local_mail_template_unknown_field_rejected() {
    // Per src/config/parse.rs:311-330, RawDestinationTemplateConfig
    // is the union of Discord + local_mail template fields with
    // #[serde(deny_unknown_fields)]. `format` is not in the list, so
    // it's rejected at parse time.
    let src = format!(
        "{PREAMBLE}\n\
         [[flow.destination]]\n\
         kind = \"local_mail\"\n\
         user = \"ops\"\n\
         \n\
         [flow.destination.template]\n\
         subject = \"...\"\n\
         format  = \"html\"\n",
    );
    let errs = parse_err(&src);
    let saw_parse_error_naming_field = errs.iter().any(|e| match e {
        ConfigError::Parse { message, .. } => message.contains("format"),
        _ => false,
    });
    assert!(
        saw_parse_error_naming_field,
        "unknown template field `format` should surface a Parse error naming it; got: {errs:#?}",
    );
}

#[test]
fn local_mail_missing_user_field_rejected() {
    // `user` is `Option<Spanned<String>>` in RawDestination
    // (src/config/parse.rs:298-299) — parse layer accepts the
    // missing field. The validator (src/config/validate.rs:991-1001)
    // turns the absence into a ConfigError::Validate naming
    // "destination.user is required for local_mail".
    //
    // Note this differs from the original stub which expected
    // ConfigError::Parse — the parse-layer Option means missing
    // user is structurally fine; the policy gate fires later.
    let src = format!(
        "{PREAMBLE}\n\
         [[flow.destination]]\n\
         kind = \"local_mail\"\n",
    );
    let errs = parse_err(&src);
    let saw_user_required = errs.iter().any(|e| match e {
        ConfigError::Validate { field, message, .. } => {
            field == "destination.user" && message.contains("required")
        }
        _ => false,
    });
    assert!(
        saw_user_required,
        "missing user must surface a Validate error on destination.user with 'required' in the message; got: {errs:#?}",
    );
}

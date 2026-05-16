// `validate_spool_writability` host-state probe. Each test pre-stages
// a tempdir to provoke a specific SpoolProbe variant inside
// `probe_spool_writability` (Writable, ParentMissing, SpoolMissing,
// NotWritable), then asserts the per-variant ConfigError shape.

use std::os::unix::fs::PermissionsExt;

use super::common::{euid_is_root, local_mail_only_config};

#[test]
fn validate_spool_writability_writable_path_emits_no_error() {
    let td = tempfile::TempDir::new().unwrap();
    let user = "testuser";
    let spool = td.path().join(user);
    std::fs::write(&spool, b"existing mbox").unwrap();
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
    let td = tempfile::TempDir::new().unwrap();
    let user = "anyuser";
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
    let td = tempfile::TempDir::new().unwrap();
    let user = "missing-user";
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
    // Skip when running as root — root traverses 0o400 and access(W_OK)
    // would still succeed because root bypasses the DAC write check.
    if euid_is_root() {
        eprintln!(
            "validate_spool_writability_not_writable_emits_not_writable_error: skipped — \
             test requires non-root euid (root bypasses DAC W_OK check)",
        );
        return;
    }
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
                && (suggestion.contains("BindPaths") || suggestion.contains("chmod 0660"))
        }
        _ => false,
    });
    assert!(
        any,
        "expected NotWritable error with chmod/BindPaths suggestion; got: {:#?}",
        errors,
    );
}

#[test]
fn validate_spool_writability_no_local_mail_destinations_emits_no_error() {
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
    // (`/var/mail`). For a config with local_mail destination naming a
    // user that almost certainly doesn't have a /var/mail/<random>
    // spool, expect SOME error.
    let user = "gcit-spool-test-9f3a2b-no-host";
    let cfg = gcit::config::load_str(
        &local_mail_only_config(user),
        std::path::Path::new("inline"),
    )
    .expect("config must parse");
    let errors = gcit::config::validate::validate_spool_writability(&cfg, None);
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
    assert_eq!(alice_err, 1);
    assert_eq!(bob_err, 1);
}

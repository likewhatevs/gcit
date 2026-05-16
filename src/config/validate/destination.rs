// Destination validators: `discord_webhook` + `local_mail` plus their
// templates. Each validator also rejects the OTHER kind's fields so
// an operator sees a clear "wrong-kind field" error at config time.

use std::collections::BTreeMap;
use std::path::Path;

use super::super::credential::CredentialId;
use super::super::error::ConfigError;
use super::super::parse::{
    Destination, DiscordTemplateConfig, DiscordWebhookConfig, FireEvent, LocalMailConfig,
    LocalMailTemplateConfig, RawDestination, RawDestinationTemplateConfig,
};
use super::credential::{collect_fire_on, validate_credential_id};
use super::template::compile_template_field;
use super::{span_line, validate_err, MAX_LOCAL_MAIL_USER_LEN};

#[allow(clippy::too_many_arguments)]
pub(super) fn validate_destinations(
    raw: &[RawDestination],
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
    env_var_index: &mut BTreeMap<String, Vec<(String, usize, String)>>,
    credential_lines: &mut BTreeMap<CredentialId, Vec<usize>>,
    flow_name: &str,
) -> Vec<Destination> {
    let mut out = Vec::with_capacity(raw.len());
    for dest in raw {
        let kind_str = dest.kind.get_ref().as_str();
        let kind_line = span_line(source, &dest.kind);
        match kind_str {
            "discord_webhook" => {
                out.push(Destination::DiscordWebhook(validate_discord(
                    dest,
                    kind_line,
                    source,
                    path,
                    errors,
                    env_var_index,
                    credential_lines,
                    flow_name,
                )));
            }
            "local_mail" => {
                out.push(Destination::LocalMail(validate_local_mail(
                    dest, kind_line, source, path, errors, flow_name,
                )));
            }
            other => {
                errors.push(validate_err(
                    path,
                    vec![kind_line],
                    Some(flow_name),
                    "destination.kind",
                    other,
                    format!(
                        "unknown destination kind {:?}; valid kinds: discord_webhook, local_mail",
                        other
                    ),
                    "set kind = \"discord_webhook\" or \"local_mail\"",
                ));
            }
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn validate_discord(
    raw: &RawDestination,
    kind_line: usize,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
    env_var_index: &mut BTreeMap<String, Vec<(String, usize, String)>>,
    credential_lines: &mut BTreeMap<CredentialId, Vec<usize>>,
    flow_name: &str,
) -> DiscordWebhookConfig {
    // Reject local_mail-only fields used with discord_webhook.
    if let Some(user) = &raw.user {
        let line = span_line(source, user);
        errors.push(validate_err(
            path,
            vec![line],
            Some(flow_name),
            "destination.user",
            user.get_ref().clone(),
            "destination.user belongs to local_mail; not valid on discord_webhook",
            "remove `user` or change kind to local_mail",
        ));
    }
    let credential_id = match &raw.credential_id {
        Some(spanned) => validate_credential_id(
            spanned,
            "destination.discord_webhook.credential_id",
            flow_name,
            source,
            path,
            errors,
            env_var_index,
            credential_lines,
        ),
        None => {
            errors.push(validate_err(
                path,
                vec![kind_line],
                Some(flow_name),
                "destination.credential_id",
                "",
                "destination.credential_id is required for discord_webhook",
                "add credential_id = \"<id>\" under [[flow.destination]]",
            ));
            None
        }
    };
    let fire_on = match &raw.fire_on {
        Some(events) => collect_fire_on(
            events,
            "destination.discord_webhook.fire_on",
            flow_name,
            source,
            path,
            errors,
        ),
        None => vec![FireEvent::RunComplete],
    };
    let template = validate_discord_template(&raw.template, source, path, errors, flow_name);

    DiscordWebhookConfig {
        // unreachable at runtime: `validate` returns Err when errors
        // is non-empty.
        credential_id: credential_id
            .unwrap_or_else(|| CredentialId::new("placeholder").expect("placeholder valid")),
        fire_on,
        template,
    }
}

fn validate_discord_template(
    raw: &RawDestinationTemplateConfig,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
    flow_name: &str,
) -> DiscordTemplateConfig {
    // Reject local_mail-only template fields used inside
    // discord_webhook destinations.
    for (field, value) in [("subject", &raw.subject), ("body", &raw.body)] {
        if let Some(spanned) = value {
            errors.push(validate_err(
                path,
                vec![span_line(source, spanned)],
                Some(flow_name),
                format!("destination.template.{}", field),
                spanned.get_ref().clone(),
                format!(
                    "destination.template.{} belongs to local_mail; not valid on discord_webhook",
                    field
                ),
                format!("remove `{}` or change kind to local_mail", field),
            ));
        }
    }
    DiscordTemplateConfig {
        title: compile_template_field(
            &raw.title,
            "destination.template.title",
            source,
            path,
            errors,
            flow_name,
        ),
        description: compile_template_field(
            &raw.description,
            "destination.template.description",
            source,
            path,
            errors,
            flow_name,
        ),
        field_name: compile_template_field(
            &raw.field_name,
            "destination.template.field_name",
            source,
            path,
            errors,
            flow_name,
        ),
        field_value: compile_template_field(
            &raw.field_value,
            "destination.template.field_value",
            source,
            path,
            errors,
            flow_name,
        ),
        collapsed_summary: compile_template_field(
            &raw.collapsed_summary,
            "destination.template.collapsed_summary",
            source,
            path,
            errors,
            flow_name,
        ),
    }
}

fn validate_local_mail(
    raw: &RawDestination,
    kind_line: usize,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
    flow_name: &str,
) -> LocalMailConfig {
    // Reject discord_webhook-only fields used with local_mail.
    if let Some(cid) = &raw.credential_id {
        let line = span_line(source, cid);
        errors.push(validate_err(
            path,
            vec![line],
            Some(flow_name),
            "destination.credential_id",
            cid.get_ref().clone(),
            "destination.credential_id belongs to discord_webhook; not valid on local_mail",
            "remove `credential_id` or change kind to discord_webhook",
        ));
    }
    let (user_str, user_line) = match &raw.user {
        Some(spanned) => (spanned.get_ref().clone(), span_line(source, spanned)),
        None => {
            errors.push(validate_err(
                path,
                vec![kind_line],
                Some(flow_name),
                "destination.user",
                "",
                "destination.user is required for local_mail",
                "add user = \"<unix_user>\" under [[flow.destination]]",
            ));
            (String::new(), kind_line)
        }
    };
    validate_local_mail_user(&user_str, user_line, flow_name, path, errors);

    let fire_on = match &raw.fire_on {
        Some(events) => collect_fire_on(
            events,
            "destination.local_mail.fire_on",
            flow_name,
            source,
            path,
            errors,
        ),
        None => vec![FireEvent::RunComplete],
    };
    let template = validate_local_mail_template(&raw.template, source, path, errors, flow_name);

    LocalMailConfig {
        user: user_str,
        fire_on,
        template,
    }
}

fn validate_local_mail_user(
    user_str: &str,
    user_line: usize,
    flow_name: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
) {
    if user_str.is_empty() {
        errors.push(validate_err(
            path,
            vec![user_line],
            Some(flow_name),
            "destination.local_mail.user",
            user_str.to_string(),
            "local_mail.user must be non-empty",
            "use a Unix username like 'ops'",
        ));
    } else if user_str.len() > MAX_LOCAL_MAIL_USER_LEN {
        errors.push(validate_err(
            path,
            vec![user_line],
            Some(flow_name),
            "destination.local_mail.user",
            user_str.to_string(),
            format!(
                "local_mail.user is {} chars; max is {}",
                user_str.len(),
                MAX_LOCAL_MAIL_USER_LEN
            ),
            format!("shorten to {} chars or fewer", MAX_LOCAL_MAIL_USER_LEN),
        ));
    } else {
        for ch in user_str.chars() {
            if !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '-') {
                errors.push(validate_err(
                    path,
                    vec![user_line],
                    Some(flow_name),
                    "destination.local_mail.user",
                    user_str.to_string(),
                    format!(
                        "local_mail.user contains invalid character {:?}; allowed: A-Z a-z 0-9 _ -",
                        ch
                    ),
                    "use only A-Z, a-z, 0-9, '_', and '-'",
                ));
                break;
            }
        }
    }
}

fn validate_local_mail_template(
    raw: &RawDestinationTemplateConfig,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
    flow_name: &str,
) -> LocalMailTemplateConfig {
    // Reject discord_webhook-only template fields used inside
    // local_mail destinations.
    for (field, value) in [
        ("title", &raw.title),
        ("description", &raw.description),
        ("field_name", &raw.field_name),
        ("field_value", &raw.field_value),
        ("collapsed_summary", &raw.collapsed_summary),
    ] {
        if let Some(spanned) = value {
            errors.push(validate_err(
                path,
                vec![span_line(source, spanned)],
                Some(flow_name),
                format!("destination.template.{}", field),
                spanned.get_ref().clone(),
                format!(
                    "destination.template.{} belongs to discord_webhook; not valid on local_mail",
                    field
                ),
                format!("remove `{}` or change kind to discord_webhook", field),
            ));
        }
    }
    LocalMailTemplateConfig {
        subject: compile_template_field(
            &raw.subject,
            "destination.template.subject",
            source,
            path,
            errors,
            flow_name,
        ),
        body: compile_template_field(
            &raw.body,
            "destination.template.body",
            source,
            path,
            errors,
            flow_name,
        ),
    }
}

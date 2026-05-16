// Build the per-flow notifier vector from the destination list.
// Discord destinations build a `DiscordNotifier`; local_mail builds
// a `LocalMailNotifier`. Errors bubble up as `String` for
// `last_error` reporting at the spawn-flow callsite.

use std::sync::Arc;
use std::time::Duration;

use secrecy::ExposeSecret;
use tokio::sync::RwLock;

use crate::config::{Destination, FlowConfig};
use crate::discord::{self, DiscordNotifier};
use crate::flow::dispatcher::DynNotifier;
use crate::mail::LocalMailNotifier;

use super::super::credentials::CredentialPool;

/// Translate `flow.destination` into a `Vec<Arc<dyn DynNotifier>>`
/// the dispatcher's fan-out path consumes. Discord destinations
/// resolve their webhook URL via the credential pool; local_mail
/// destinations construct directly. Any failure (credential
/// resolution, URL parse, user validation) bubbles up as a `String`
/// so the caller can record it in `last_error`.
///
/// `http_request_timeout` is threaded into `discord::Client::new`
/// so the Discord webhook honors `config.http.request_timeout`
/// (octocrab and grokmirror already do).
pub(super) async fn build_notifiers(
    flow: &FlowConfig,
    credential_pool: &Arc<RwLock<CredentialPool>>,
    hostname: &Arc<String>,
    http_request_timeout: Duration,
) -> Result<Vec<Arc<dyn DynNotifier>>, String> {
    let mut out: Vec<Arc<dyn DynNotifier>> = Vec::with_capacity(flow.destination.len());
    let hb = Arc::new(crate::notify::strict_handlebars());
    for (idx, dest) in flow.destination.iter().enumerate() {
        match dest {
            Destination::DiscordWebhook(w) => {
                let url_secret = credential_pool
                    .write()
                    .await
                    .resolve_secret(&w.credential_id)
                    .await
                    .map_err(|e| format!("discord credential: {e}"))?;
                let parsed = discord::parse_webhook_url(url_secret.expose_secret())
                    .map_err(|e| format!("discord webhook URL: {e}"))?;
                let client = discord::Client::new(http_request_timeout)
                    .map_err(|e| format!("discord client: {e}"))?;
                let n = DiscordNotifier::new(
                    format!("{}.dest{}", flow.name, idx),
                    client,
                    parsed,
                    w.fire_on.clone(),
                    w.template.clone(),
                    Arc::clone(&hb),
                );
                out.push(Arc::new(n));
            }
            Destination::LocalMail(m) => {
                let n = LocalMailNotifier::new(
                    format!("{}.dest{}", flow.name, idx),
                    m.user.clone(),
                    Arc::clone(hostname),
                    m.fire_on.clone(),
                    m.template.clone(),
                    Arc::clone(&hb),
                )
                .map_err(|e| format!("local_mail user: {e}"))?;
                out.push(Arc::new(n));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        ActionConfig, CredentialId, DiscordTemplateConfig, DiscordWebhookConfig, FireEvent,
        FlowConfig, LocalMailConfig, LocalMailTemplateConfig, PollOverride, SourceConfig,
    };
    use std::collections::BTreeMap;

    fn flow_no_destinations(name: &str) -> FlowConfig {
        FlowConfig {
            name: name.to_string(),
            enabled: true,
            description: None,
            source: SourceConfig {
                url: "https://github.com/myorg/linux-builder.git".to_string(),
                ref_name: "refs/heads/main".to_string(),
                credential_id: None,
            },
            action: ActionConfig::GithubWorkflowDispatch {
                repo: "myorg/linux-builder".to_string(),
                workflow: "ci.yml".to_string(),
                ref_name: "refs/heads/main".to_string(),
                credential_id: CredentialId::new("github_pat").expect("valid id"),
                inputs: BTreeMap::new(),
            },
            destination: vec![],
            poll: PollOverride::default(),
        }
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(30)
    }

    #[tokio::test]
    async fn build_notifiers_returns_empty_vec_for_no_destinations() {
        let flow = flow_no_destinations("flow1");
        let pool = Arc::new(RwLock::new(CredentialPool::default()));
        let hostname = Arc::new("ci-host".to_string());
        let n = build_notifiers(&flow, &pool, &hostname, default_timeout())
            .await
            .expect("build must succeed");
        assert!(
            n.is_empty(),
            "no destinations must yield empty notifier vec",
        );
    }

    #[tokio::test]
    async fn build_notifiers_constructs_local_mail_notifier_for_mail_destination() {
        let mut flow = flow_no_destinations("flow-mail");
        flow.destination
            .push(Destination::LocalMail(LocalMailConfig {
                user: "ci".to_string(),
                fire_on: vec![FireEvent::RunComplete],
                template: LocalMailTemplateConfig::default(),
            }));
        let pool = Arc::new(RwLock::new(CredentialPool::default()));
        let hostname = Arc::new("ci-host".to_string());
        let n = build_notifiers(&flow, &pool, &hostname, default_timeout())
            .await
            .expect("build must succeed for local_mail-only flow");
        assert_eq!(
            n.len(),
            1,
            "single local_mail destination must produce one notifier",
        );
    }

    #[tokio::test]
    async fn build_notifiers_constructs_two_notifiers_for_two_local_mail_destinations() {
        // Multiple local_mail destinations on the same flow are
        // allowed (fan-out to multiple system users). Each becomes
        // its own notifier with a distinct `flow.destN` id.
        let mut flow = flow_no_destinations("flow-multi");
        flow.destination
            .push(Destination::LocalMail(LocalMailConfig {
                user: "ci".to_string(),
                fire_on: vec![FireEvent::RunComplete],
                template: LocalMailTemplateConfig::default(),
            }));
        flow.destination
            .push(Destination::LocalMail(LocalMailConfig {
                user: "ops".to_string(),
                fire_on: vec![FireEvent::JobComplete],
                template: LocalMailTemplateConfig::default(),
            }));
        let pool = Arc::new(RwLock::new(CredentialPool::default()));
        let hostname = Arc::new("ci-host".to_string());
        let n = build_notifiers(&flow, &pool, &hostname, default_timeout())
            .await
            .expect("build must succeed");
        assert_eq!(n.len(), 2);
    }

    #[tokio::test]
    async fn build_notifiers_propagates_local_mail_user_validation_failure() {
        // LocalMailNotifier::new rejects users containing '/', '..',
        // or NUL. build_notifiers wraps with "local_mail user: " so
        // the operator-facing last_error names the failing path.
        let mut flow = flow_no_destinations("flow-bad-user");
        flow.destination
            .push(Destination::LocalMail(LocalMailConfig {
                user: "bad/user".to_string(),
                fire_on: vec![FireEvent::RunComplete],
                template: LocalMailTemplateConfig::default(),
            }));
        let pool = Arc::new(RwLock::new(CredentialPool::default()));
        let hostname = Arc::new("ci-host".to_string());
        // Result<Vec<Arc<dyn DynNotifier>>, _>::expect_err needs Debug
        // on Ok; DynNotifier is dyn-erased and not Debug. Match instead.
        let err = match build_notifiers(&flow, &pool, &hostname, default_timeout()).await {
            Ok(_) => panic!("invalid unix user must surface as Err"),
            Err(e) => e,
        };
        assert!(
            err.starts_with("local_mail user:"),
            "error must lead with the path-naming prefix; got: {err}",
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn build_notifiers_propagates_discord_credential_resolution_failure() {
        // The Discord arm calls credential_pool.resolve_secret and
        // wraps any failure with "discord credential: ". Build a pool
        // with no config_dir, scrub the env var, and reference the id
        // — resolve_secret falls through every step.
        let credential_id = CredentialId::new("nonexistent-discord-cred").expect("valid id");
        // SAFETY: serialized via #[serial]; single-threaded env mutation.
        unsafe {
            std::env::remove_var("CREDENTIALS_DIRECTORY");
            std::env::remove_var(credential_id.to_env_var());
        }
        let mut flow = flow_no_destinations("flow-bad-discord");
        flow.destination
            .push(Destination::DiscordWebhook(DiscordWebhookConfig {
                credential_id: credential_id.clone(),
                fire_on: vec![FireEvent::RunStart],
                template: DiscordTemplateConfig::default(),
            }));
        let pool = Arc::new(RwLock::new(CredentialPool::default()));
        let hostname = Arc::new("ci-host".to_string());
        let err = match build_notifiers(&flow, &pool, &hostname, default_timeout()).await {
            Ok(_) => panic!("missing discord credential must surface as Err"),
            Err(e) => e,
        };
        assert!(
            err.starts_with("discord credential:"),
            "error must lead with the discord-credential prefix; got: {err}",
        );
        // Pin the backtick-wrapped form `<id>`. resolve_secret's
        // not-found arm wraps the id in backticks; operators grep
        // for the wrapped form. A bare-id substring would still pass
        // even after a quote-style regression.
        let backticked = format!("`{}`", credential_id.as_str());
        assert!(
            err.contains(&backticked),
            "error must name the credential id wrapped as {backticked}; got: {err}",
        );
    }
}

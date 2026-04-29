// Public face of the config module: load(), Config, and the
// ConfigError enum.
//
// The gcit library is not a published API; the only consumers are
// the gcit binary and the integration test harness under tests/.
// Items are `pub` because `pub(crate)` does not reach integration
// tests. Treat every public name as crate-internal and unstable.

pub mod credential;
pub mod credential_file;
pub mod error;
pub mod parse;
pub mod validate;

pub use credential::CredentialId;
pub use error::ConfigError;
pub use parse::{
    ActionConfig, Config, Destination, DiscordTemplateConfig, DiscordWebhookConfig, FireEvent,
    FlowConfig, HttpConfig, LocalMailConfig, LocalMailTemplateConfig, LogConfig, PollDefaults,
    PollOverride, SourceConfig,
};

/// Where a credential id is referenced in the config. Drives the
/// install walkthrough's per-id messaging (URL hint, target repo,
/// chmod path) and lets the unit renderer / status check share the
/// same walk shape so a future kind addition only updates this enum
/// + walk_credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialKindHint {
    /// Source-side credential consumed by the git fetch path. The
    /// schema does not pin a kind here; treat as opaque.
    SourceFetch,
    /// `github_workflow_dispatch` action: GitHub PAT scoped to the
    /// named `owner/repo`.
    GithubPat { repo: String },
    /// `discord_webhook` destination: webhook URL.
    DiscordWebhook,
}

/// One occurrence of a credential id reference inside the config. A
/// credential consumed by N flow sites yields N entries; the caller
/// decides whether to dedup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialRef {
    pub id: CredentialId,
    pub flow: String,
    pub kind: CredentialKindHint,
}

/// Iterate every credential id referenced by `cfg`, paired with the
/// flow name and a kind hint derived from the consumer site
/// (source / action / destination). Deterministic order: outer loop
/// is `cfg.flow` order; within a flow the order is source ->
/// action -> destinations (config order).
///
/// This is the single canonical credential walk: cli/install.rs uses
/// it for the wizard's per-id messaging, cli/check.rs uses it to
/// build the consumers map, and systemd/unit.rs uses it to emit
/// `LoadCredential=` lines (deduped via BTreeSet).
pub fn walk_credentials(cfg: &Config) -> impl Iterator<Item = CredentialRef> + '_ {
    cfg.flow.iter().flat_map(|flow| {
        let mut refs: Vec<CredentialRef> = Vec::new();
        if let Some(cid) = &flow.source.credential_id {
            refs.push(CredentialRef {
                id: cid.clone(),
                flow: flow.name.clone(),
                kind: CredentialKindHint::SourceFetch,
            });
        }
        match &flow.action {
            ActionConfig::GithubWorkflowDispatch {
                credential_id,
                repo,
                ..
            } => {
                refs.push(CredentialRef {
                    id: credential_id.clone(),
                    flow: flow.name.clone(),
                    kind: CredentialKindHint::GithubPat { repo: repo.clone() },
                });
            }
        }
        for d in &flow.destination {
            match d {
                Destination::DiscordWebhook(w) => {
                    refs.push(CredentialRef {
                        id: w.credential_id.clone(),
                        flow: flow.name.clone(),
                        kind: CredentialKindHint::DiscordWebhook,
                    });
                }
                Destination::LocalMail(_) => {}
            }
        }
        refs.into_iter()
    })
}

use std::path::Path;

/// Load and validate a gcit config file.
///
/// On success returns a fully validated `Config`. On failure returns
/// every error the validator could collect — TOML parse failures stop
/// after the first because the document is structurally broken, but
/// every domain-validation failure is reported at once so the
/// operator can fix multiple issues per edit.
pub fn load(path: &Path) -> Result<Config, Vec<ConfigError>> {
    let source = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            return Err(vec![ConfigError::Parse {
                path: path.to_path_buf(),
                line: 0,
                message: format!("could not read config file: {}", e),
            }]);
        }
    };
    load_str(&source, path)
}

/// Load and validate a config from an in-memory source string.
///
/// Used by `load`, by `gcit check` when the source has already been
/// read, and by integration tests that build a config inline.
pub fn load_str(source: &str, path: &Path) -> Result<Config, Vec<ConfigError>> {
    let raw: parse::RawConfig = match toml::from_str(source) {
        Ok(r) => r,
        Err(e) => {
            return Err(vec![map_toml_error(e, source, path)]);
        }
    };
    validate::validate(raw, source, path)
}

/// Convert a `toml::de::Error` into a `ConfigError::Parse` with a
/// 1-based line number derived from the error's byte span.
fn map_toml_error(err: toml::de::Error, source: &str, path: &Path) -> ConfigError {
    let line = match err.span() {
        Some(span) => validate::byte_offset_to_line(source, span.start),
        None => 1,
    };
    ConfigError::Parse {
        path: path.to_path_buf(),
        line,
        message: err.message().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_offset_to_line_unit() {
        let s = "abc\ndef\nghi";
        assert_eq!(validate::byte_offset_to_line(s, 0), 1);
        assert_eq!(validate::byte_offset_to_line(s, 3), 1);
        assert_eq!(validate::byte_offset_to_line(s, 4), 2);
        assert_eq!(validate::byte_offset_to_line(s, 7), 2);
        assert_eq!(validate::byte_offset_to_line(s, 8), 3);
        assert_eq!(validate::byte_offset_to_line(s, 100), 3);
        assert_eq!(validate::byte_offset_to_line("", 0), 1);
        assert_eq!(validate::byte_offset_to_line("\n\n\n", 3), 4);
    }
}

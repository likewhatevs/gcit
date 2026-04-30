// Two-phase config parsing: a raw schema with `Spanned<String>` for
// every field that needs source-line attribution, then the validator
// converts to the typed `Config` exposed to the rest of the daemon.
//
// Why two-phase:
//   `Spanned<T>::Deserialize` calls `deserialize_struct` with a magic
//   field-name list, while `humantime_serde::Deserialize` calls
//   `deserialize_str`. Composing them on the same field is impossible.
//   By keeping every parse-attributed field as `Spanned<String>` in the
//   raw schema we (a) preserve the source span for error reporting and
//   (b) defer all real parsing (humantime, charset, range checks) to
//   the validator where errors can be aggregated.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use toml::Spanned;

use super::credential::CredentialId;

// ---------------------------------------------------------------------
// Final, validated Config (the type the rest of the daemon consumes).
// ---------------------------------------------------------------------

/// Fully validated daemon configuration.
///
/// All durations are typed `Duration`; all credential ids are typed
/// `CredentialId`. Fields are pub for integration-test reachability
/// (Rust's `pub(crate)` does not span the integration-test boundary);
/// treat as crate-internal and unstable.
#[derive(Debug, Clone, Serialize)]
pub struct Config {
    pub source_path: PathBuf,
    pub poll: PollDefaults,
    pub log: LogConfig,
    pub http: HttpConfig,
    pub flow: Vec<FlowConfig>,
    /// Each `CredentialId` referenced in the config carries its source
    /// line(s) so a runtime CredentialNotFound surfaces editor-friendly
    /// jump targets. The validator records every occurrence's line; an
    /// id referenced from N flows has N entries (sorted, deduped).
    pub credential_lines: std::collections::BTreeMap<crate::config::CredentialId, Vec<usize>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PollDefaults {
    pub source_interval: Option<Duration>,
    pub job_interval: Duration,
    pub jitter: f64,
    /// Per-flow dispatch throttle. Bounds the minimum elapsed wall
    /// time between two poll-originated dispatch acceptances on a
    /// flow. `Duration::ZERO` disables throttling. Default: 5
    /// minutes (see `default_cooldown`).
    pub cooldown: Duration,
}

impl Default for PollDefaults {
    fn default() -> Self {
        Self {
            source_interval: None,
            job_interval: Duration::from_secs(30),
            jitter: 0.1,
            cooldown: default_cooldown(),
        }
    }
}

/// Default value for `[poll] cooldown` and the bottom-fallback for
/// `[flow.poll] cooldown` overrides. Five minutes balances "don't
/// spam dispatches on a busy upstream" with "don't make operators
/// wait forever for a manual edit + push to fire". Operators who
/// want the pre-cooldown behaviour set `cooldown = "0s"`.
pub fn default_cooldown() -> Duration {
    Duration::from_secs(5 * 60)
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct LogConfig {
    pub filter: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HttpConfig {
    pub request_timeout: Duration,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct FlowConfig {
    pub name: String,
    pub enabled: bool,
    pub description: Option<String>,
    pub source: SourceConfig,
    pub action: ActionConfig,
    pub destination: Vec<Destination>,
    pub poll: PollOverride,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceConfig {
    pub url: String,
    #[serde(rename = "ref")]
    pub ref_name: String,
    pub credential_id: Option<CredentialId>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ActionConfig {
    GithubWorkflowDispatch {
        repo: String,
        workflow: String,
        // Match the input TOML's `ref` key on the Serialize side
        // too so any round-trip serializer renders the operator-
        // written key, not the Rust-side `ref_name` (which exists
        // only because `ref` is a Rust keyword).
        #[serde(rename = "ref")]
        ref_name: String,
        credential_id: CredentialId,
        inputs: BTreeMap<String, String>,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Destination {
    DiscordWebhook(DiscordWebhookConfig),
    LocalMail(LocalMailConfig),
}

#[derive(Debug, Clone, Serialize)]
pub struct DiscordWebhookConfig {
    pub credential_id: CredentialId,
    pub fire_on: Vec<FireEvent>,
    pub template: DiscordTemplateConfig,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DiscordTemplateConfig {
    pub title: Option<String>,
    pub description: Option<String>,
    pub field_name: Option<String>,
    pub field_value: Option<String>,
    pub collapsed_summary: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LocalMailConfig {
    pub user: String,
    pub fire_on: Vec<FireEvent>,
    pub template: LocalMailTemplateConfig,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct LocalMailTemplateConfig {
    pub subject: Option<String>,
    pub body: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FireEvent {
    RunStart,
    JobComplete,
    RunComplete,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PollOverride {
    pub source_interval: Option<Duration>,
    pub job_interval: Option<Duration>,
    pub jitter: Option<f64>,
    /// Per-flow override for the dispatch cooldown. When `Some`,
    /// replaces `PollDefaults.cooldown` for this flow.
    pub cooldown: Option<Duration>,
}

// ---------------------------------------------------------------------
// Raw, span-preserving schema (parse target).
// ---------------------------------------------------------------------
//
// Every field that participates in error messages is `Spanned<...>` so
// the validator can map a domain failure back to a 1-based line number.
// Fields that take typed values directly (bool, BTreeMap) do not need
// span attribution because their failure modes are caught by serde and
// surface as parse-time errors, not validation errors.

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawConfig {
    #[serde(default)]
    pub(crate) poll: RawPollDefaults,
    #[serde(default)]
    pub(crate) log: RawLogConfig,
    #[serde(default)]
    pub(crate) http: RawHttpConfig,
    #[serde(default)]
    pub(crate) flow: Vec<RawFlowConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawPollDefaults {
    #[serde(default)]
    pub(crate) source_interval: Option<Spanned<String>>,
    #[serde(default)]
    pub(crate) job_interval: Option<Spanned<String>>,
    #[serde(default)]
    pub(crate) jitter: Option<Spanned<f64>>,
    #[serde(default)]
    pub(crate) cooldown: Option<Spanned<String>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawLogConfig {
    #[serde(default)]
    pub(crate) filter: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawHttpConfig {
    #[serde(default)]
    pub(crate) request_timeout: Option<Spanned<String>>,
    // Deprecated. Kept on the raw struct so existing configs that
    // set `http.max_concurrent` deserialize successfully (without
    // tripping `deny_unknown_fields`). The field is no longer
    // wired into a Semaphore — concurrency is bounded by
    // octocrab's internal rate-limiter (per-credential) plus the
    // tokio runtime's task scheduler, so the value has no effect.
    // `validate_http` emits a deprecation warning if the field is
    // set, then drops the value. Spanned for source-line fidelity
    // in the warning.
    #[serde(default)]
    pub(crate) max_concurrent: Option<Spanned<usize>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawFlowConfig {
    pub(crate) name: Spanned<String>,
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) description: Option<String>,
    pub(crate) source: RawSourceConfig,
    pub(crate) action: RawActionConfig,
    #[serde(default)]
    pub(crate) destination: Vec<RawDestination>,
    #[serde(default)]
    pub(crate) poll: RawPollOverride,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawSourceConfig {
    pub(crate) url: Spanned<String>,
    #[serde(rename = "ref")]
    pub(crate) ref_name: Spanned<String>,
    #[serde(default)]
    pub(crate) credential_id: Option<Spanned<String>>,
}

/// Flat parse target for `[flow.action]`. The `kind` discriminator
/// is read as a spanned string and dispatched in the validator —
/// using a `#[serde(tag = "kind")]` enum is incompatible with
/// `toml::Spanned` because serde's internally-tagged enums buffer
/// values through `serde::__private::de::Content`, which discards
/// the magic `Spanned` struct-name dispatch. The discriminator-
/// dispatch is done explicitly in the validator (see
/// `validate_action`), and unknown kinds are surfaced as
/// `ConfigError::Validate` with a list of valid kinds.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawActionConfig {
    pub(crate) kind: Spanned<String>,
    #[serde(default)]
    pub(crate) repo: Option<Spanned<String>>,
    #[serde(default)]
    pub(crate) workflow: Option<Spanned<String>>,
    #[serde(default, rename = "ref")]
    pub(crate) ref_name: Option<Spanned<String>>,
    #[serde(default)]
    pub(crate) credential_id: Option<Spanned<String>>,
    #[serde(default)]
    pub(crate) inputs: BTreeMap<String, String>,
}

/// Flat parse target for `[[flow.destination]]`. Same rationale as
/// `RawActionConfig`: the `kind` discriminator drives validator
/// dispatch, all variant-specific fields are optional, and the
/// validator rejects ones that don't match the chosen kind.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawDestination {
    pub(crate) kind: Spanned<String>,
    // discord_webhook fields
    #[serde(default)]
    pub(crate) credential_id: Option<Spanned<String>>,
    // local_mail fields
    #[serde(default)]
    pub(crate) user: Option<Spanned<String>>,
    // shared
    #[serde(default)]
    pub(crate) fire_on: Option<Vec<Spanned<FireEvent>>>,
    #[serde(default)]
    pub(crate) template: RawDestinationTemplateConfig,
}

/// Union of every documented Discord and local_mail template field.
/// The validator dispatches on the destination's kind and collects
/// only the relevant subset; fields belonging to the wrong kind are
/// surfaced as Validate errors.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawDestinationTemplateConfig {
    // discord_webhook fields
    #[serde(default)]
    pub(crate) title: Option<Spanned<String>>,
    #[serde(default)]
    pub(crate) description: Option<Spanned<String>>,
    #[serde(default)]
    pub(crate) field_name: Option<Spanned<String>>,
    #[serde(default)]
    pub(crate) field_value: Option<Spanned<String>>,
    #[serde(default)]
    pub(crate) collapsed_summary: Option<Spanned<String>>,
    // local_mail fields
    #[serde(default)]
    pub(crate) subject: Option<Spanned<String>>,
    #[serde(default)]
    pub(crate) body: Option<Spanned<String>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawPollOverride {
    #[serde(default)]
    pub(crate) source_interval: Option<Spanned<String>>,
    #[serde(default)]
    pub(crate) job_interval: Option<Spanned<String>>,
    #[serde(default)]
    pub(crate) jitter: Option<Spanned<f64>>,
    #[serde(default)]
    pub(crate) cooldown: Option<Spanned<String>>,
}

// Shared notifier trait and supporting types.
//
// `RunContext` is the per-flow snapshot the supervisor builds at
// trigger time. The dispatcher uses it to render `inputs` templates;
// the notifiers use it to render Discord embed fields and mbox
// subject/body. By keeping this module above `discord` and `mail` in
// the dependency graph, both notifier crates pull from the same
// source of truth.
//
// Native `async fn` in traits is used (MSRV 1.91). No
// `#[async_trait::async_trait]` attribute.

use std::time::Duration;

use chrono::{DateTime, Utc};
use gix_hash::ObjectId;
use handlebars::Handlebars;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::github::{self, JobResult, RunSummary};

/// One run's full context: who triggered it, against what source,
/// targeting what action. Built by the supervisor and handed to every
/// notifier for the same run.
///
/// Field set mirrors `config::validate::probe_context` so any template
/// that compiles at config-load also renders at runtime.
#[derive(Debug, Clone)]
pub struct RunContext {
    pub flow_name: String,
    /// Optional `flow.description` per the operator's TOML. `None`
    /// renders as the empty string in templates.
    pub flow_description: Option<String>,
    pub source: SourceInfo,
    pub action: ActionInfo,
    /// `gcit_run_id` is the per-trigger UUID. The supervisor injects
    /// it into the `workflow_dispatch` inputs payload (so the
    /// resulting GitHub Actions run carries it on `Run.name` for
    /// correlation) and surfaces it via the `{{gcit.run_id}}` template
    /// namespace.
    pub gcit_run_id: Uuid,
}

/// The git side of a run — where the SHA came from.
#[derive(Debug, Clone)]
pub struct SourceInfo {
    pub url: String,
    pub ref_name: String,
    pub sha: ObjectId,
    /// 12-character hex prefix of `sha`. Pre-computed because every
    /// notifier renders it (Discord embeds, mbox bodies).
    pub sha_short: String,
}

/// The GitHub Actions side of a run — what gcit dispatched.
#[derive(Debug, Clone)]
pub struct ActionInfo {
    pub repo: String,
    pub workflow: String,
    pub run_id: u64,
    pub run_url: String,
    pub dispatched_at: DateTime<Utc>,
}

/// Result of a successful notifier invocation.
#[derive(Debug, Clone)]
pub enum NotifyOutcome {
    /// Notification was delivered. `receipt` is an opaque string the
    /// supervisor can log and surface in `gcit status`. For Discord:
    /// `"webhook:{webhook ID}"` formed from the parsed webhook URL's
    /// id segment. For mbox: `"file:{path}"` naming the spool path
    /// that was appended to.
    Sent { receipt: String },
    /// The notifier short-circuited without sending. The reason
    /// distinguishes "wasn't asked to fire on this event" from
    /// "wasn't configured at all" so operators reading logs can
    /// distinguish a no-op flow from a misconfigured flow.
    Skipped { reason: SkipReason },
}

/// Why a notifier returned `Skipped` instead of `Sent`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// The destination was constructed with a default (no-op)
    /// `on_run_start` / `on_job_complete` impl. Never returned by
    /// `on_run_complete` (which is `must_implement`).
    NotConfigured,
    /// The destination's `fire_on` array does not include the
    /// triggering event. Equivalent to "operator opted out" — not
    /// an error.
    FireOnMismatch,
    /// The notifier's per-credential rate bucket said defer. Caller
    /// can retry on the next supervisor cycle.
    RateLimited,
}

/// Error returned from a notifier. Distinct from `GithubErrorKind`
/// (which is GitHub-API-specific) — every notifier kind shares this
/// shape so the supervisor's retry logic is uniform.
#[derive(Debug, thiserror::Error)]
pub enum NotifyError {
    /// Retryable — caller should retry on backoff. `retry_after`
    /// optionally hints how long to wait before the next attempt
    /// (Discord's 429 Retry-After header, mbox EBUSY backoff, etc.).
    #[error("transient: {source}")]
    Transient {
        #[source]
        source: anyhow::Error,
        retry_after: Option<Duration>,
    },

    /// Non-retryable — operator action required. Includes config
    /// errors that escaped validation, unrecoverable I/O failures
    /// (ENOENT on a missing mbox spool), and permanent API errors
    /// (404 webhook gone).
    #[error("permanent: {source}")]
    Permanent {
        #[source]
        source: anyhow::Error,
    },
}

impl NotifyError {
    /// Whether `backon`'s retry loop should retry this error. Mirror
    /// of `GithubErrorKind::is_transient` so notifier dispatch and
    /// API dispatch use the same predicate.
    pub fn is_transient(&self) -> bool {
        matches!(self, NotifyError::Transient { .. })
    }

    /// Hint how long the caller should sleep before the next retry.
    /// Only `Transient` carries this; `Permanent` always returns
    /// `None`.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            NotifyError::Transient { retry_after, .. } => *retry_after,
            NotifyError::Permanent { .. } => None,
        }
    }
}

/// The notifier interface. Every destination kind (Discord webhook,
/// local mbox, future SMTP, etc.) implements this trait.
///
/// The default impls of `on_run_start` and `on_job_complete` return
/// `Skipped { NotConfigured }` so impls only need to override the
/// events they actually fire on. `on_run_complete` is required —
/// every meaningful destination delivers at least the terminal
/// summary.
///
/// Native `async fn` in traits per MSRV 1.91; no async-trait dep.
pub trait Notifier: Send + Sync {
    /// Static identifier for the notifier kind (`"discord_webhook"`,
    /// `"local_mail"`, ...). Logs and `gcit status` use this.
    fn kind(&self) -> &'static str;

    /// Per-instance identifier — the destination index within a flow
    /// or the human-readable name from config. Used to disambiguate
    /// when one flow has multiple destinations of the same kind.
    fn id(&self) -> &str;

    /// Fired after a `workflow_dispatch` succeeds AND the resulting
    /// `Run.id` has been correlated, before the per-run monitor is
    /// spawned. The `RunContext` carries the resolved `action.run_id`
    /// and `action.run_url` so notifier templates can link the
    /// operator straight to the run page. Default: skip with
    /// `NotConfigured`.
    ///
    /// `cancel` is the per-flow cancellation token. See
    /// `on_run_complete` doc comment for the contract; the default
    /// impl ignores it because it returns synchronously without I/O.
    #[allow(unused_variables)]
    fn on_run_start(
        &self,
        ctx: &RunContext,
        cancel: &CancellationToken,
    ) -> impl std::future::Future<Output = Result<NotifyOutcome, NotifyError>> + Send {
        async move {
            Ok(NotifyOutcome::Skipped {
                reason: SkipReason::NotConfigured,
            })
        }
    }

    /// Fired when an individual job inside a run terminates. The
    /// supervisor calls this once per job per run, in observation
    /// order (which matches GitHub's list_jobs ordering, not start
    /// time). Default: skip with `NotConfigured`.
    ///
    /// `cancel` is the per-flow cancellation token. See
    /// `on_run_complete` doc comment for the contract; the default
    /// impl ignores it because it returns synchronously without I/O.
    #[allow(unused_variables)]
    fn on_job_complete(
        &self,
        ctx: &RunContext,
        job: &JobResult,
        cancel: &CancellationToken,
    ) -> impl std::future::Future<Output = Result<NotifyOutcome, NotifyError>> + Send {
        async move {
            Ok(NotifyOutcome::Skipped {
                reason: SkipReason::NotConfigured,
            })
        }
    }

    /// Fired exactly once when the run reaches a terminal status.
    /// Required: every notifier must implement this — it carries the
    /// only summary the operator is guaranteed to see.
    ///
    /// `cancel` is the per-flow cancellation token. Impls that
    /// block on an unbounded wait (e.g. flock acquisition) SHOULD
    /// race that wait against `cancel.cancelled()` so a SIGTERM or
    /// per-flow reload unblocks the notifier within bounded time.
    /// Impls whose underlying transport already bounds its own
    /// wait (e.g. HTTP clients with a per-request timeout) MAY
    /// ignore `cancel` — see the kind-specific impl for the
    /// rationale.
    ///
    /// On cancel, impls SHOULD return `NotifyError::Transient`
    /// with a `source` describing the cancellation. The supervisor
    /// logs this under the `gcit::flow::notify` tracing target.
    /// Notifier errors surface in journald, not in `gcit status`
    /// last_error (which tracks dispatch/correlate failures).
    fn on_run_complete(
        &self,
        ctx: &RunContext,
        summary: &RunSummary,
        cancel: &CancellationToken,
    ) -> impl std::future::Future<Output = Result<NotifyOutcome, NotifyError>> + Send;
}

/// Names of every built-in handlebars helper gcit deregisters before
/// evaluating user-supplied templates. gcit templates are leaf-only
/// string substitutions. Block helpers
/// (each/with/if/unless/lookup/log) would let an operator's template
/// branch on internal state, evaluate sub-expressions, or emit log
/// lines from inside a template — surface area gcit doesn't accept.
/// Comparison helpers (eq/ne/gt/...) and `len` are left registered
/// because they are pure-data leaf operations and useful for
/// formatting (e.g. `{{#if (eq run.conclusion "success")}}` is
/// rejected by virtue of removing `if`, not by removing `eq`).
const DEREGISTERED_HELPERS: &[&str] = &["each", "with", "if", "unless", "lookup", "log", "raw"];

/// Build the canonical handlebars instance gcit uses everywhere a
/// template is rendered (config validation, dispatcher inputs,
/// Discord embed fields, mbox subject/body). Strict-mode is enabled
/// (every variable must resolve), the block helpers in
/// `DEREGISTERED_HELPERS` are removed to enforce the leaf-only rule,
/// and the escape function is replaced with `handlebars::no_escape`
/// so plain-text outputs (mbox bodies, Discord embed leaves) are NOT
/// html-entity-encoded. Discord embed fields render markdown (not
/// HTML); mbox bodies are plain text. Without the no_escape
/// registration, `<`/`>`/`&` in upstream values would render as
/// `&lt;`/`&gt;`/`&amp;` and operators would see literal entity
/// escapes in their notifications.
pub fn strict_handlebars() -> Handlebars<'static> {
    let mut hb = Handlebars::new();
    hb.set_strict_mode(true);
    hb.register_escape_fn(handlebars::no_escape);
    for name in DEREGISTERED_HELPERS {
        hb.unregister_helper(name);
    }
    hb
}

/// Build the JSON-shaped data context handlebars renders against.
/// Single source of truth for the runtime template namespace —
/// every notifier (and any future render path) MUST go through this
/// function so the runtime shape stays in lock-step with
/// `config::validate::probe_context`.
///
/// Field set is exactly what `probe_context` produces, so any
/// template that compiled at config-load also renders here. The
/// optional `job` namespace is layered in by `render_context_with_job`
/// for per-job templates (Discord field name / value).
pub fn render_context(ctx: &RunContext, summary: &RunSummary) -> serde_json::Value {
    let conclusion_label = summary
        .conclusion
        .map(github::label_for)
        .unwrap_or("(in progress)");
    let status_label = github::run_status_label(summary.status);
    serde_json::json!({
        "flow": {
            "name": ctx.flow_name,
            "description": ctx.flow_description.as_deref().unwrap_or(""),
        },
        "source": {
            "url": ctx.source.url,
            "ref_name": ctx.source.ref_name,
            "sha": ctx.source.sha.to_string(),
            "sha_short": ctx.source.sha_short,
        },
        "action": {
            "repo": ctx.action.repo,
            "workflow": ctx.action.workflow,
            "run_id": ctx.action.run_id,
            "run_url": ctx.action.run_url,
            "dispatched_at": ctx.action.dispatched_at.to_rfc3339(),
        },
        "run": {
            "status": status_label,
            "conclusion": conclusion_label,
        },
        "gcit": {
            "run_id": ctx.gcit_run_id.to_string(),
        },
    })
}

/// Layer a `job.*` namespace on top of `render_context`. Used by
/// Discord embed field-name and field-value templates (the only
/// per-job render sites). The `job` namespace mirrors the shape
/// surfaced for `JobResult` and is independent of probe_context
/// because per-job templates are configured under
/// `[flow.destination.template]` and are not pre-rendered against
/// probe_context (they only render at run-completion time, when
/// the job set is known).
pub fn render_context_with_job(
    ctx: &RunContext,
    summary: &RunSummary,
    job: &JobResult,
) -> serde_json::Value {
    let mut base = render_context(ctx, summary);
    let job_label = job
        .conclusion
        .map(github::label_for)
        .unwrap_or("in progress");
    if let serde_json::Value::Object(map) = &mut base {
        map.insert(
            "job".to_string(),
            serde_json::json!({
                "id": job.job_id,
                "name": job.name,
                "url": job.html_url,
                "conclusion": job_label,
                "attempt": job.run_attempt,
            }),
        );
    }
    base
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notify_error_transient_retry_after() {
        let e = NotifyError::Transient {
            source: anyhow::anyhow!("boom"),
            retry_after: Some(Duration::from_secs(5)),
        };
        assert!(e.is_transient());
        assert_eq!(e.retry_after(), Some(Duration::from_secs(5)));
    }

    #[test]
    fn notify_error_permanent_is_not_transient() {
        let e = NotifyError::Permanent {
            source: anyhow::anyhow!("config wrong"),
        };
        assert!(!e.is_transient());
        assert_eq!(e.retry_after(), None);
    }

    #[test]
    fn notify_outcome_sent_carries_receipt() {
        let o = NotifyOutcome::Sent {
            receipt: "msg-123".into(),
        };
        match o {
            NotifyOutcome::Sent { receipt } => assert_eq!(receipt, "msg-123"),
            _ => panic!("expected Sent"),
        }
    }

    #[test]
    fn notify_outcome_skipped_carries_reason() {
        for reason in [
            SkipReason::NotConfigured,
            SkipReason::FireOnMismatch,
            SkipReason::RateLimited,
        ] {
            let o = NotifyOutcome::Skipped { reason };
            match o {
                NotifyOutcome::Skipped { reason: r } => assert_eq!(r, reason),
                _ => panic!("expected Skipped"),
            }
        }
    }

    fn ctx() -> RunContext {
        RunContext {
            flow_name: "ci-flow".into(),
            flow_description: Some("desc".into()),
            source: SourceInfo {
                url: "https://example.com/r.git".into(),
                ref_name: "refs/heads/main".into(),
                sha: ObjectId::null(gix_hash::Kind::Sha1),
                sha_short: "0000000".into(),
            },
            action: ActionInfo {
                repo: "owner/repo".into(),
                workflow: "ci.yml".into(),
                run_id: 7,
                run_url: "https://github.com/owner/repo/actions/runs/7".into(),
                dispatched_at: "2026-04-26T12:00:00Z"
                    .parse::<chrono::DateTime<Utc>>()
                    .unwrap(),
            },
            gcit_run_id: Uuid::nil(),
        }
    }

    fn summary_for_test() -> RunSummary {
        use crate::github::{Conclusion, RunStatus};
        RunSummary {
            run_id: 7,
            run_url: "https://github.com/owner/repo/actions/runs/7".into(),
            run_number: 1,
            run_attempt: 1,
            status: RunStatus::Completed,
            conclusion: Some(Conclusion::Success),
            started_at: None,
            completed_at: None,
            jobs: Vec::new(),
        }
    }

    #[test]
    fn render_context_includes_every_probe_namespace() {
        // Pin the exact namespace shape that
        // `config::validate::probe_context` exercises so a future
        // probe addition forces a runtime addition (and vice versa).
        let v = render_context(&ctx(), &summary_for_test());
        let obj = v.as_object().expect("object");
        for ns in ["flow", "source", "action", "run", "gcit"] {
            assert!(obj.contains_key(ns), "missing namespace {ns}");
        }
        assert_eq!(obj["flow"]["name"], "ci-flow");
        assert_eq!(obj["flow"]["description"], "desc");
        assert_eq!(obj["source"]["ref_name"], "refs/heads/main");
        assert_eq!(obj["source"]["sha_short"], "0000000");
        assert_eq!(obj["action"]["repo"], "owner/repo");
        assert_eq!(obj["action"]["workflow"], "ci.yml");
        assert_eq!(obj["action"]["run_id"], 7);
        assert!(obj["action"]["dispatched_at"].is_string());
        assert_eq!(obj["run"]["status"], "completed");
        assert_eq!(obj["run"]["conclusion"], "success");
        assert_eq!(obj["gcit"]["run_id"], Uuid::nil().to_string());
    }

    #[test]
    fn render_context_flow_description_defaults_to_empty_string() {
        let mut c = ctx();
        c.flow_description = None;
        let v = render_context(&c, &summary_for_test());
        assert_eq!(v["flow"]["description"], "");
    }

    #[test]
    fn render_context_with_job_layers_job_namespace() {
        use crate::github::{Conclusion, JobResult};
        let job = JobResult {
            job_id: 99,
            name: "build".into(),
            html_url: "https://example.com/jobs/99".into(),
            conclusion: Some(Conclusion::Failure),
            started_at: None,
            completed_at: None,
            steps: Vec::new(),
            run_attempt: 2,
        };
        let v = render_context_with_job(&ctx(), &summary_for_test(), &job);
        assert_eq!(v["job"]["id"], 99);
        assert_eq!(v["job"]["name"], "build");
        assert_eq!(v["job"]["url"], "https://example.com/jobs/99");
        assert_eq!(v["job"]["conclusion"], "failure");
        assert_eq!(v["job"]["attempt"], 2);
        // Outer namespaces still present.
        assert_eq!(v["flow"]["name"], "ci-flow");
    }

    #[test]
    fn strict_handlebars_deregisters_block_helpers() {
        let hb = strict_handlebars();
        assert!(hb.strict_mode());
        // Try to use `each` — should fail because the helper is gone.
        let err = hb
            .render_template("{{#each xs}}{{/each}}", &serde_json::json!({"xs":[1,2]}))
            .expect_err("each must be deregistered");
        let msg = format!("{err}");
        assert!(msg.contains("each"), "expected 'each' in error: {msg}");

        // `if` similarly.
        let err = hb
            .render_template("{{#if x}}y{{/if}}", &serde_json::json!({"x":true}))
            .expect_err("if must be deregistered");
        let msg = format!("{err}");
        assert!(msg.contains("if"), "expected 'if' in error: {msg}");
    }

    #[test]
    fn strict_handlebars_renders_leaf_substitution() {
        // Plain leaf substitution still works after deregistering
        // block helpers.
        let hb = strict_handlebars();
        let s = hb
            .render_template("{{name}}", &serde_json::json!({"name":"alice"}))
            .unwrap();
        assert_eq!(s, "alice");
    }

    #[test]
    fn strict_handlebars_does_not_html_escape_special_chars() {
        // mbox bodies and Discord embed leaves are plain text, not
        // HTML. The default handlebars escape function HTML-encodes
        // `<`, `>`, `&`, `"`, `'`. strict_handlebars() must register
        // no_escape so operators do not see literal `&lt;` in their
        // notifications.
        let hb = strict_handlebars();
        let s = hb
            .render_template(
                "{{name}}",
                &serde_json::json!({"name":"<b>Alice & Bob</b>"}),
            )
            .unwrap();
        assert_eq!(s, "<b>Alice & Bob</b>");
    }

    #[test]
    fn strict_handlebars_data_values_are_not_re_interpreted_as_template() {
        // Property: handlebars renders a TEMPLATE STRING (trusted, from
        // config) against DATA (untrusted, from network responses
        // populating RunContext / RunSummary). Data values must never
        // re-enter the parser — a malicious upstream value containing
        // `{{evil}}`, `{{#each ...}}`, or `{{> partial}}` must surface
        // verbatim in the rendered output, not execute as a template
        // fragment. This is the load-bearing security property gcit
        // relies on for handlebars-injection defense (see DESIGN.md
        // §3 "Handlebars / template security"). The deregistration
        // tests above prove block helpers fail when present in the
        // TEMPLATE; this test proves they pass through inert when
        // present in DATA, which is a different property.
        let hb = strict_handlebars();

        // (1) Bare mustache literal in data must render as text. The
        // template references `flow.name`; the data value contains
        // `{{evil}}` plus a block-helper opener plus a closing tag.
        // None of those are re-parsed — handlebars renders the value
        // as a single string.
        let data = serde_json::json!({
            "flow": {"name": "{{evil}}{{#each items}}x{{/each}}"},
        });
        let out = hb.render_template("{{flow.name}}", &data).unwrap();
        assert_eq!(
            out, "{{evil}}{{#each items}}x{{/each}}",
            "data containing handlebars syntax must render as literal text",
        );

        // (2) Partial-include syntax in data must also pass through
        // inert. Partials are not registered on `strict_handlebars` —
        // an attempt to render a template containing `{{> partial}}`
        // would fail at template-compile time. This proves the same
        // syntax is safe inside DATA: it is treated as an opaque
        // string segment regardless of partial-registration state.
        let data = serde_json::json!({
            "flow": {"name": "{{> partial}}"},
        });
        let out = hb.render_template("{{flow.name}}", &data).unwrap();
        assert_eq!(
            out, "{{> partial}}",
            "data containing partial-include syntax must render as literal text",
        );

        // (3) Block-helper closing tag alone (e.g. orphaned `{{/if}}`)
        // would be a parse error if it appeared in the template. Inside
        // data it is just bytes. A regression that re-rendered data
        // through the template parser would either crash or strip the
        // braces — pin both the literal-equality AND that no
        // re-rendering happened by checking the byte length matches.
        let data = serde_json::json!({
            "flow": {"name": "{{/each}}{{evil}}"},
        });
        let out = hb.render_template("{{flow.name}}", &data).unwrap();
        assert_eq!(out, "{{/each}}{{evil}}");
        assert_eq!(
            out.len(),
            "{{/each}}{{evil}}".len(),
            "data length must round-trip; a re-parse would strip braces",
        );
    }
}

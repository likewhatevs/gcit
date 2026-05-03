// DiscordNotifier — implements `Notifier` for `[destination]
// kind = "discord_webhook"`.
//
// Pipeline (`on_run_complete`):
//   1. Check fire_on; skip if event missing.
//   2. Build Embed via `embed::build_run_complete_embed`.
//   3. Validate embed via twilight-validate (defense-in-depth — our
//      pre-truncation should make this unconditionally pass).
//   4. Call `Client.execute_webhook(id, &token).embeds(&[embed])
//      .await`.
//   5. Map twilight-http errors per status:
//        * 401/403/404/410         → Permanent (token revoked, etc.)
//        * 429                     → Transient with retry_after
//        * 5xx / Hyper / Timeout   → Transient
//        * Validation              → Permanent (config bug)

use std::sync::Arc;
use std::time::Duration;

use handlebars::Handlebars;
use http::StatusCode;
use secrecy::ExposeSecret;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::embed;
use super::webhook::{Client, ParsedWebhookUrl};
use crate::config::{DiscordTemplateConfig, FireEvent};
use crate::github::RunSummary;
use crate::notify::{Notifier, NotifyError, NotifyOutcome, RunContext, SkipReason};

/// Per-destination Discord notifier. One instance per
/// `[[flow.destination]]` of `kind = "discord_webhook"`.
pub struct DiscordNotifier {
    /// Stable id used by `Notifier::id` — matches the destination's
    /// `credential_id` so logs disambiguate when one flow has
    /// multiple Discord destinations.
    id: String,
    client: Client,
    parsed: ParsedWebhookUrl,
    fire_on: Vec<FireEvent>,
    template: DiscordTemplateConfig,
    handlebars: Arc<Handlebars<'static>>,
}

impl DiscordNotifier {
    /// Construct a notifier from validated config. The webhook URL
    /// has already been parsed at config-load via
    /// `webhook::parse_webhook_url`; this constructor stores it.
    pub fn new(
        id: impl Into<String>,
        client: Client,
        parsed: ParsedWebhookUrl,
        fire_on: Vec<FireEvent>,
        template: DiscordTemplateConfig,
        handlebars: Arc<Handlebars<'static>>,
    ) -> Self {
        Self {
            id: id.into(),
            client,
            parsed,
            fire_on,
            template,
            handlebars,
        }
    }

    fn fires_on(&self, event: FireEvent) -> bool {
        self.fire_on.contains(&event)
    }

    /// Fire the underlying webhook with a built embed.
    /// Splits validation, dispatch, and error classification so each
    /// step can be tested independently.
    async fn deliver(
        &self,
        embed: twilight_model::channel::message::Embed,
    ) -> Result<NotifyOutcome, NotifyError> {
        // (a) Defense-in-depth validation. Pre-truncation in
        // `embed::build_run_complete_embed` should make this
        // unconditionally pass; if it fails, the operator's
        // template config is so degenerate that we should surface
        // the failure as Permanent rather than retry.
        if let Err(e) = twilight_validate::embed::embed(&embed) {
            return Err(NotifyError::Permanent {
                source: anyhow::anyhow!("embed validation failed: {e}"),
            });
        }

        // (b) Aggregate codepoint cap. twilight-validate's
        // EMBED_TOTAL_LENGTH check is bytes-based but reports as
        // "chars"; we enforce by codepoints to match the real
        // Discord limit.
        let total = embed::embed_codepoint_count(&embed);
        if total > embed::EMBED_TOTAL_CODEPOINTS {
            return Err(NotifyError::Permanent {
                source: anyhow::anyhow!(
                    "embed total {total} codepoints exceeds Discord limit {}",
                    embed::EMBED_TOTAL_CODEPOINTS,
                ),
            });
        }

        // (c) Dispatch. Token must be exposed for twilight-http;
        // `secrecy::ExposeSecret` is the documented opt-in. The
        // exposed `&str` is borrowed only for the duration of the
        // builder call — twilight-http copies what it needs internally.
        let embeds = vec![embed];
        let token = self.parsed.token.expose_secret();
        let result = self
            .client
            .inner()
            .execute_webhook(self.parsed.id, token)
            .embeds(&embeds)
            .await;

        match result {
            Ok(_response) => {
                debug!(
                    notifier = %self.id,
                    webhook_id = self.parsed.id.get(),
                    "discord webhook delivered",
                );
                Ok(NotifyOutcome::Sent {
                    receipt: format!("webhook:{}", self.parsed.id.get()),
                })
            }
            Err(err) => Err(classify_twilight_error(err)),
        }
    }
}

/// Default Retry-After hint for the unreachable-but-defensive 429
/// arm in `classify_twilight_error`. twilight-http 0.17 transparently
/// re-fires the request on 429 (response/future.rs:389-398: when no
/// permit generator is registered it loops back through
/// response_generator), so a 429 NEVER surfaces to gcit even with
/// `.ratelimiter(None)`. The arm is kept as defense-in-depth in case
/// a future twilight version exposes 429s to callers; if it ever
/// fires, this 5s hint is tighter than backon's initial delay so the
/// next attempt isn't held up by a stale Retry-After we couldn't
/// parse.
const DEFAULT_RATE_LIMIT_RETRY_AFTER: Duration = Duration::from_secs(5);

/// Map a twilight-http error into the appropriate `NotifyError`
/// variant. Status codes follow Discord's webhook semantics:
///   - 401: Permanent (invalid token)
///   - 403: Permanent (forbidden)
///   - 404: Permanent (webhook deleted; recreate in Discord)
///   - 410: Permanent (webhook channel closed)
///   - Other 4xx: Permanent (raw status — caller cannot recover)
///   - 429: Transient + retry_after
///   - 5xx: Transient
///   - Validation: Permanent (config bug)
///   - Hyper / Timeout: Transient
fn classify_twilight_error(err: twilight_http::Error) -> NotifyError {
    use twilight_http::error::ErrorType;
    match err.kind() {
        ErrorType::Response { status, .. } => {
            let code = status.get();
            // twilight-http 0.17 auto-retries 429 inside its
            // response future (response/future.rs:389-398) regardless
            // of `.ratelimiter(None)` — when no permit generator is
            // registered, the 429 path loops back through
            // response_generator. So this arm is unreachable today.
            // Kept as defense-in-depth: a future twilight release
            // could surface 429 to callers (e.g. if the auto-retry is
            // capped or a new opt-out flag lands), and we want gcit's
            // backon to retry rather than fall through to the
            // catch-all "other 4xx → Permanent" arm below.
            if code == 429 {
                warn!(status = code, "discord webhook rate-limited");
                return NotifyError::Transient {
                    source: anyhow::anyhow!("discord HTTP {code}"),
                    retry_after: Some(DEFAULT_RATE_LIMIT_RETRY_AFTER),
                };
            }
            // Distinct messages per terminal status so operators see
            // an actionable diagnosis in `gcit status` last_error
            // rather than a generic "revoked or unauthorised" lump.
            match code {
                401 => {
                    return NotifyError::Permanent {
                        source: anyhow::anyhow!(
                            "discord HTTP 401: invalid token; the webhook token has been rotated or never existed",
                        ),
                    };
                }
                403 => {
                    return NotifyError::Permanent {
                        source: anyhow::anyhow!(
                            "discord HTTP 403: forbidden; the webhook lacks permission to post in its channel",
                        ),
                    };
                }
                404 => {
                    return NotifyError::Permanent {
                        source: anyhow::anyhow!(
                            "discord HTTP 404: webhook deleted; recreate it in Discord and update the credential",
                        ),
                    };
                }
                410 => {
                    return NotifyError::Permanent {
                        source: anyhow::anyhow!(
                            "discord HTTP 410: webhook channel closed; the destination channel was deleted",
                        ),
                    };
                }
                _ => {}
            }
            // 5xx → transient.
            if let Some(http_status) = StatusCode::from_u16(code)
                .ok()
                .filter(|s| s.is_server_error())
            {
                return NotifyError::Transient {
                    source: anyhow::anyhow!("discord HTTP {http_status}"),
                    retry_after: None,
                };
            }
            // Other 4xx not enumerated above → Permanent
            // (defensive: if we sent something Discord rejects,
            // retrying won't help).
            NotifyError::Permanent {
                source: anyhow::anyhow!("discord HTTP {code}"),
            }
        }
        ErrorType::Validation => NotifyError::Permanent {
            source: anyhow::anyhow!("twilight-http validation error: {err}"),
        },
        ErrorType::RequestTimedOut => NotifyError::Transient {
            source: anyhow::anyhow!("discord request timed out: {err}"),
            retry_after: None,
        },
        // Hyper/network/parse/cancellation failures → Transient.
        // Permanent variants (Validation, Unauthorized) handled above.
        _ => NotifyError::Transient {
            source: anyhow::anyhow!("discord transport error: {err}"),
            retry_after: None,
        },
    }
}

impl Notifier for DiscordNotifier {
    fn kind(&self) -> &'static str {
        "discord_webhook"
    }

    fn id(&self) -> &str {
        &self.id
    }

    // `on_run_start` and `on_job_complete` use the trait defaults —
    // they return `Skipped { NotConfigured }` until the supervisor
    // wires those events through. Implementing them today would
    // require either dispatching a partial embed or returning the
    // misleading `FireOnMismatch` reason regardless of configuration;
    // the default keeps the skip reason honest.

    async fn on_run_complete(
        &self,
        ctx: &RunContext,
        summary: &RunSummary,
        cancel: &CancellationToken,
    ) -> Result<NotifyOutcome, NotifyError> {
        if !self.fires_on(FireEvent::RunComplete) {
            return Ok(NotifyOutcome::Skipped {
                reason: SkipReason::FireOnMismatch,
            });
        }
        let embed =
            embed::build_run_complete_embed(self.handlebars.as_ref(), &self.template, ctx, summary)
                .map_err(|e| NotifyError::Permanent {
                    source: anyhow::anyhow!("embed render failed: {e}"),
                })?;
        // Race deliver against cancel. twilight-http's `Client`
        // already carries a per-request timeout (30s, set by
        // `supervisor::build_notifiers`) that bounds the await
        // independently — without this select arm, an SIGTERM
        // mid-deliver would still surface within 30s. The arm is
        // tighter: it gets the supervisor draining immediately
        // when cancel fires, instead of waiting for the
        // request_timeout to elapse.
        //
        // Dropping the deliver future cancels the in-flight
        // reqwest request as a side effect — futures are cancelled
        // by drop, so the underlying TCP/TLS work unwinds cleanly.
        // (Note: this is NOT JoinHandle-drop semantics —
        // tokio::JoinHandle drop is detach, not abort. The
        // unwinding here happens because the future itself is
        // dropped from inside the select! macro, not because any
        // JoinHandle was dropped.)
        //
        // The local-mail notifier needs a different style of
        // cancel check (early-exit + biased select! around
        // phase_rx) because its `spawn_blocking` flock wait has no
        // analogous internal bound — `lock.write()` blocks the OS
        // thread with no per-call timeout to fall back on.
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(NotifyError::Transient {
                // Wording is deliberately uncertain: cancel may
                // fire after the HTTP request has already been
                // sent but before reqwest reads the response. In
                // that race, the webhook may have reached Discord
                // even though we surface Transient. Saying
                // "delivered" or "not delivered" would overclaim;
                // operators reading last_error need to know the
                // delivery status is unknowable from here.
                source: anyhow::anyhow!(
                    "discord webhook {} cancelled; delivery status unknown — the request may or may not have reached Discord",
                    self.parsed.id.get(),
                ),
                retry_after: None,
            }),
            res = self.deliver(embed) => res,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::github::Conclusion;

    fn handlebars() -> Arc<Handlebars<'static>> {
        Arc::new(crate::notify::strict_handlebars())
    }

    use crate::util::ensure_crypto_provider;

    fn parsed() -> ParsedWebhookUrl {
        super::super::webhook::parse_webhook_url(
            "https://discord.com/api/webhooks/1234567890/testtoken",
        )
        .unwrap()
    }

    #[tokio::test]
    async fn on_run_start_and_on_job_complete_use_trait_default() {
        // DiscordNotifier does not yet implement RunStart / JobComplete
        // — the trait default returns `Skipped { NotConfigured }` so
        // operators see "this notifier does not handle that event"
        // rather than the misleading FireOnMismatch reason.
        ensure_crypto_provider();
        let client = Client::new(Duration::from_secs(5)).unwrap();
        // fire_on includes RunStart + JobComplete; the trait default
        // ignores fire_on entirely because the notifier kind itself
        // doesn't implement the event.
        let n = DiscordNotifier::new(
            "test",
            client,
            parsed(),
            vec![
                FireEvent::RunStart,
                FireEvent::JobComplete,
                FireEvent::RunComplete,
            ],
            DiscordTemplateConfig::default(),
            handlebars(),
        );
        let ctx = test_run_context();
        let cancel = CancellationToken::new();
        match n.on_run_start(&ctx, &cancel).await.unwrap() {
            NotifyOutcome::Skipped {
                reason: SkipReason::NotConfigured,
            } => {}
            other => panic!("expected NotConfigured, got {other:?}"),
        }
        let job = test_job();
        match n.on_job_complete(&ctx, &job, &cancel).await.unwrap() {
            NotifyOutcome::Skipped {
                reason: SkipReason::NotConfigured,
            } => {}
            other => panic!("expected NotConfigured, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fires_on_run_complete_skips_when_not_in_fire_on() {
        ensure_crypto_provider();
        let client = Client::new(Duration::from_secs(5)).unwrap();
        // fire_on doesn't include RunComplete → must skip.
        let n = DiscordNotifier::new(
            "test",
            client,
            parsed(),
            vec![FireEvent::RunStart],
            DiscordTemplateConfig::default(),
            handlebars(),
        );
        let ctx = test_run_context();
        let summary = test_summary();
        let cancel = CancellationToken::new();
        match n.on_run_complete(&ctx, &summary, &cancel).await.unwrap() {
            NotifyOutcome::Skipped {
                reason: SkipReason::FireOnMismatch,
            } => {}
            other => panic!("expected FireOnMismatch, got {other:?}"),
        }
    }

    #[test]
    fn default_rate_limit_retry_after_is_5s() {
        assert_eq!(DEFAULT_RATE_LIMIT_RETRY_AFTER, Duration::from_secs(5));
    }

    fn test_run_context() -> RunContext {
        use chrono::Utc;
        use gix_hash::ObjectId;
        RunContext {
            flow_name: "ci".into(),
            flow_description: None,
            source: crate::notify::SourceInfo {
                url: "https://example.com/r.git".into(),
                ref_name: "refs/heads/main".into(),
                sha: ObjectId::null(gix_hash::Kind::Sha1),
                sha_short: "0000000".into(),
            },
            action: crate::notify::ActionInfo {
                repo: "owner/repo".into(),
                workflow: "ci.yml".into(),
                run_id: 1,
                run_url: "https://github.com/owner/repo/actions/runs/1".into(),
                dispatched_at: Utc::now(),
            },
            gcit_run_id: uuid::Uuid::nil(),
        }
    }

    fn test_summary() -> RunSummary {
        use crate::github::RunStatus;
        use chrono::Utc;
        RunSummary {
            run_id: 1,
            run_url: "https://github.com/owner/repo/actions/runs/1".into(),
            run_number: 1,
            run_attempt: 1,
            status: RunStatus::Completed,
            conclusion: Some(Conclusion::Success),
            started_at: Some(Utc::now()),
            completed_at: Some(Utc::now()),
            jobs: Vec::new(),
        }
    }

    fn test_job() -> crate::github::JobResult {
        use chrono::Utc;
        crate::github::JobResult {
            job_id: 1,
            name: "build".into(),
            html_url: "https://github.com/owner/repo/actions/jobs/1".into(),
            conclusion: Some(Conclusion::Success),
            started_at: Some(Utc::now()),
            completed_at: Some(Utc::now()),
            steps: Vec::new(),
            run_attempt: 1,
        }
    }

    #[tokio::test]
    async fn on_run_complete_render_failure_returns_permanent_error() {
        // Production at src/discord/notifier.rs:277-281 maps a
        // build_run_complete_embed RenderError into
        // NotifyError::Permanent with "embed render failed: ..." in
        // the source message. The `Permanent` classification matters
        // because retrying a config-bug template forever is a waste
        // of dispatch budget.
        //
        // Bypass config-time validation by constructing the
        // notifier directly with a template that references a
        // namespace not in render_context. strict_handlebars rejects
        // unknown variables at render time; this is the production
        // path that fires when an operator added a typo'd dotted
        // path that probe_context happened to populate but
        // render_context does not.
        ensure_crypto_provider();
        let client = Client::new(Duration::from_secs(5)).unwrap();
        let n = DiscordNotifier::new(
            "test-render-err",
            client,
            parsed(),
            vec![FireEvent::RunComplete],
            DiscordTemplateConfig {
                title: Some("{{nonexistent_top_level_namespace.field}}".into()),
                ..DiscordTemplateConfig::default()
            },
            handlebars(),
        );
        let err = n
            .on_run_complete(
                &test_run_context(),
                &test_summary(),
                &CancellationToken::new(),
            )
            .await
            .expect_err("undefined-variable template must surface as Err");
        match err {
            NotifyError::Permanent { source } => {
                let msg = source.to_string();
                assert!(
                    msg.contains("embed render failed"),
                    "Permanent error must surface 'embed render failed'; got: {msg}",
                );
            }
            other => panic!("expected Permanent, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn on_run_complete_cancel_before_dispatch_returns_transient() {
        // Mirror of tests/discord_cancel.rs but verifying the cancel
        // arm message at src/discord/notifier.rs:307-321. A
        // pre-cancelled token wins the biased select before deliver
        // even runs, so the test never needs a wiremock — the
        // production code's "delivery status unknown" wording must
        // surface unconditionally when cancel is observed first.
        ensure_crypto_provider();
        let client = Client::new(Duration::from_secs(5)).unwrap();
        let n = DiscordNotifier::new(
            "test-cancel",
            client,
            parsed(),
            vec![FireEvent::RunComplete],
            DiscordTemplateConfig::default(),
            handlebars(),
        );
        let cancel = CancellationToken::new();
        cancel.cancel();
        let err = n
            .on_run_complete(&test_run_context(), &test_summary(), &cancel)
            .await
            .expect_err("pre-cancelled token must surface as Err");
        match err {
            NotifyError::Transient {
                source,
                retry_after,
            } => {
                let msg = source.to_string();
                assert!(
                    msg.contains("cancelled"),
                    "transient cancel error must mention 'cancelled'; got: {msg}",
                );
                assert!(
                    msg.contains("delivery status unknown"),
                    "transient cancel error must surface uncertainty; got: {msg}",
                );
                assert_eq!(
                    retry_after, None,
                    "cancel transient must not carry retry_after",
                );
            }
            other => panic!("expected Transient cancel, got {other:?}"),
        }
    }
}

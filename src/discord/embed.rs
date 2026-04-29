// Programmatic Embed builder. No JSON: every field is constructed
// from typed twilight_model::Embed components. Truncation runs
// per-field via `template::truncate_codepoints` before the Embed is
// handed to twilight-validate.
//
// Design:
//   - Build always returns a valid `Embed` (passes
//     twilight_validate::embed::embed). The combined-length check
//     in twilight-validate uses bytes (.len()) but reports as
//     "chars"; gcit avoids this by pre-truncating per-field at
//     codepoint boundaries. The aggregate byte total still matters
//     because twilight-validate rejects on bytes > 6000; we cap
//     each field individually so the aggregate fits even when every
//     field is at its codepoint maximum.
//   - The collapse decision is folded into the description: when
//     `should_collapse(conclusion) == true`, jobs are stitched into
//     a one-line summary; otherwise per-job fields are emitted up
//     to FIELD_COUNT.

use handlebars::{Handlebars, RenderError};
use twilight_model::channel::message::embed::{EmbedField, EmbedFooter};
use twilight_model::channel::message::Embed;
use twilight_validate::embed::{
    DESCRIPTION_LENGTH, FIELD_COUNT, FIELD_NAME_LENGTH, FIELD_VALUE_LENGTH, FOOTER_TEXT_LENGTH,
    TITLE_LENGTH,
};

use super::conclusion;
use super::template;
use crate::config::DiscordTemplateConfig;
use crate::github::{self, RunSummary};
use crate::notify::{self, RunContext};

/// Build the `Embed` for an `on_run_complete` event.
///
/// Pipeline:
///   1. Render configured templates against (ctx, summary).
///   2. Truncate each rendered string at its Discord per-field cap.
///   3. Choose color from `summary.conclusion`.
///   4. Choose collapse strategy from `summary.conclusion`.
///   5. Build typed `Embed`. Result is guaranteed to pass
///      `twilight_validate::embed::embed`.
///
/// `templates` carries the raw (unrendered) strings from
/// `DiscordTemplateConfig` — config validation has already compiled
/// them against a probe context, so rendering against the real ctx
/// is overwhelmingly likely to succeed. Failure surfaces a
/// `RenderError` which the notifier wraps into
/// `NotifyError::Permanent` at the call site.
pub fn build_run_complete_embed(
    handlebars: &Handlebars<'_>,
    templates: &DiscordTemplateConfig,
    ctx: &RunContext,
    summary: &RunSummary,
) -> Result<Embed, RenderError> {
    // Build the data context handlebars renders against via the
    // shared `notify::render_context` so the runtime namespace
    // matches `config::validate::probe_context`.
    let data = notify::render_context(ctx, summary);

    let title =
        template::render_optional(handlebars, templates.title.as_deref(), &data, TITLE_LENGTH)?;

    let description = template::render_optional(
        handlebars,
        templates.description.as_deref(),
        &data,
        DESCRIPTION_LENGTH,
    )?;

    let collapse = summary
        .conclusion
        .map(github::should_collapse)
        .unwrap_or(false);

    // Color: use conclusion if terminal, otherwise the in-progress
    // slate. on_run_complete should always carry a conclusion, but
    // defensive default.
    let color = summary
        .conclusion
        .map(conclusion::color_for)
        .unwrap_or(super::conclusion::COLOR_IN_PROGRESS);

    // Fields. When collapsed, we emit a single inline summary using
    // `collapsed_summary` (or a default) instead of per-job detail.
    let fields: Vec<EmbedField> = if collapse {
        build_collapsed_fields(handlebars, templates, &data, summary)?
    } else {
        build_expanded_fields(handlebars, templates, ctx, summary)?
    };

    // Footer: surface the run URL so operators can click straight to
    // GitHub. No template variable for the URL itself — it's
    // structural data, not user-rendered prose. Truncate to the
    // footer cap.
    let footer_text = template::truncate_codepoints(
        &format!(
            "Run #{} · attempt {}",
            summary.run_number, summary.run_attempt
        ),
        FOOTER_TEXT_LENGTH,
    );

    Ok(Embed {
        author: None,
        color: Some(color),
        description,
        fields,
        footer: Some(EmbedFooter {
            icon_url: None,
            proxy_icon_url: None,
            text: footer_text,
        }),
        image: None,
        kind: "rich".to_string(),
        provider: None,
        thumbnail: None,
        timestamp: None,
        title,
        url: Some(summary.run_url.clone()),
        video: None,
    })
}

/// Build the inline collapsed summary for success-like conclusions.
/// One field, one line — operators don't need detail when the run
/// worked.
fn build_collapsed_fields(
    handlebars: &Handlebars<'_>,
    templates: &DiscordTemplateConfig,
    data: &serde_json::Value,
    summary: &RunSummary,
) -> Result<Vec<EmbedField>, RenderError> {
    let summary_text = match templates.collapsed_summary.as_deref() {
        Some(t) => template::render_and_truncate(handlebars, t, data, FIELD_VALUE_LENGTH)?,
        None => {
            // Default collapsed summary: "{n} jobs · {conclusion}".
            let label = summary
                .conclusion
                .map(github::label_for)
                .unwrap_or("complete");
            template::truncate_codepoints(
                &format!("{} jobs · {}", summary.jobs.len(), label),
                FIELD_VALUE_LENGTH,
            )
        }
    };
    Ok(vec![EmbedField {
        inline: false,
        name: template::truncate_codepoints("Summary", FIELD_NAME_LENGTH),
        value: summary_text,
    }])
}

/// Build the per-job fields for failure-like conclusions. Capped at
/// FIELD_COUNT (25) with a tail field indicating truncation when
/// the run has more jobs than fit.
fn build_expanded_fields(
    handlebars: &Handlebars<'_>,
    templates: &DiscordTemplateConfig,
    ctx: &RunContext,
    summary: &RunSummary,
) -> Result<Vec<EmbedField>, RenderError> {
    let jobs = summary.jobs.as_slice();
    let mut fields = Vec::with_capacity(jobs.len().min(FIELD_COUNT));

    // Reserve one slot for a "and N more" indicator if we exceed
    // the cap; otherwise we'd silently drop jobs.
    let cap = if jobs.len() > FIELD_COUNT {
        FIELD_COUNT.saturating_sub(1)
    } else {
        FIELD_COUNT
    };

    for job in jobs.iter().take(cap) {
        let job_data = notify::render_context_with_job(ctx, summary, job);

        let name = match templates.field_name.as_deref() {
            Some(t) => template::render_and_truncate(handlebars, t, &job_data, FIELD_NAME_LENGTH)?,
            None => template::truncate_codepoints(&job.name, FIELD_NAME_LENGTH),
        };

        let value = match templates.field_value.as_deref() {
            Some(t) => template::render_and_truncate(handlebars, t, &job_data, FIELD_VALUE_LENGTH)?,
            None => {
                let label = job
                    .conclusion
                    .map(github::label_for)
                    .unwrap_or("in progress");
                template::truncate_codepoints(
                    &format!("[{}]({})", label, job.html_url),
                    FIELD_VALUE_LENGTH,
                )
            }
        };

        fields.push(EmbedField {
            inline: true,
            name,
            value,
        });
    }

    if jobs.len() > FIELD_COUNT {
        let extra = jobs.len() - cap;
        fields.push(EmbedField {
            inline: false,
            name: template::truncate_codepoints("…", FIELD_NAME_LENGTH),
            value: template::truncate_codepoints(
                &format!("and {} more job(s)", extra),
                FIELD_VALUE_LENGTH,
            ),
        });
    }

    Ok(fields)
}

/// Force `truncate_codepoints` to be in scope via re-export so
/// downstream tests don't need a deep path. The notifier integration
/// tests reach for this directly.
pub use template::truncate_codepoints as truncate_for_field;

/// Helper for tests + observability: total codepoint footprint of an
/// embed across the fields twilight-validate sums for
/// EMBED_TOTAL_LENGTH. Mirrors twilight-validate's `chars()` helper
/// but counts codepoints (not bytes) — useful for verifying we stay
/// under 6000 codepoints.
pub fn embed_codepoint_count(embed: &Embed) -> usize {
    let mut total = 0;
    if let Some(author) = &embed.author {
        total += author.name.chars().count();
    }
    if let Some(description) = &embed.description {
        total += description.chars().count();
    }
    if let Some(footer) = &embed.footer {
        total += footer.text.chars().count();
    }
    for field in &embed.fields {
        total += field.name.chars().count();
        total += field.value.chars().count();
    }
    if let Some(title) = &embed.title {
        total += title.chars().count();
    }
    total
}

/// Maximum total embed size in codepoints — pinned at the
/// twilight-validate constant so embedder logic shares the cap.
pub const EMBED_TOTAL_CODEPOINTS: usize = twilight_validate::embed::EMBED_TOTAL_LENGTH;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::github::{Conclusion, JobResult, RunStatus};
    use chrono::Utc;
    use gix_hash::ObjectId;

    fn ctx() -> RunContext {
        RunContext {
            flow_name: "ci-flow".into(),
            flow_description: None,
            source: crate::notify::SourceInfo {
                url: "https://example.com/repo.git".into(),
                ref_name: "refs/heads/main".into(),
                sha: ObjectId::null(gix_hash::Kind::Sha1),
                sha_short: "0000000".into(),
            },
            action: crate::notify::ActionInfo {
                repo: "owner/repo".into(),
                workflow: "ci.yml".into(),
                run_id: 42,
                run_url: "https://github.com/owner/repo/actions/runs/42".into(),
                dispatched_at: Utc::now(),
            },
            gcit_run_id: uuid::Uuid::nil(),
        }
    }

    fn summary(conclusion: Conclusion, jobs: usize) -> RunSummary {
        RunSummary {
            run_id: 42,
            run_url: "https://github.com/owner/repo/actions/runs/42".into(),
            run_number: 7,
            run_attempt: 1,
            status: RunStatus::Completed,
            conclusion: Some(conclusion),
            started_at: Some(Utc::now()),
            completed_at: Some(Utc::now()),
            jobs: (0..jobs)
                .map(|i| JobResult {
                    job_id: 100 + i as u64,
                    name: format!("job-{i}"),
                    html_url: format!("https://github.com/owner/repo/actions/jobs/{i}"),
                    conclusion: Some(Conclusion::Success),
                    started_at: Some(Utc::now()),
                    completed_at: Some(Utc::now()),
                    steps: Vec::new(),
                    run_attempt: 1,
                })
                .collect(),
        }
    }

    fn handlebars() -> Handlebars<'static> {
        notify::strict_handlebars()
    }

    #[test]
    fn build_run_complete_emits_color_for_conclusion() {
        let hb = handlebars();
        let templates = DiscordTemplateConfig::default();
        let s = summary(Conclusion::Failure, 3);
        let embed = build_run_complete_embed(&hb, &templates, &ctx(), &s).unwrap();
        assert_eq!(
            embed.color,
            Some(conclusion::color_for(Conclusion::Failure))
        );
    }

    #[test]
    fn build_run_complete_collapses_on_success() {
        let hb = handlebars();
        let templates = DiscordTemplateConfig::default();
        let s = summary(Conclusion::Success, 5);
        let embed = build_run_complete_embed(&hb, &templates, &ctx(), &s).unwrap();
        // Collapsed = exactly one field carrying the summary.
        assert_eq!(embed.fields.len(), 1);
        assert_eq!(embed.fields[0].name, "Summary");
    }

    #[test]
    fn build_run_complete_expands_on_failure() {
        let hb = handlebars();
        let templates = DiscordTemplateConfig::default();
        let s = summary(Conclusion::Failure, 3);
        let embed = build_run_complete_embed(&hb, &templates, &ctx(), &s).unwrap();
        // Expanded = one field per job.
        assert_eq!(embed.fields.len(), 3);
        for f in &embed.fields {
            assert!(f.inline, "per-job fields must be inline");
        }
    }

    #[test]
    fn build_run_complete_caps_fields_at_field_count() {
        let hb = handlebars();
        let templates = DiscordTemplateConfig::default();
        // 30 jobs > FIELD_COUNT(25). Expect 24 jobs + "and N more"
        // tail = 25 fields total.
        let s = summary(Conclusion::Failure, 30);
        let embed = build_run_complete_embed(&hb, &templates, &ctx(), &s).unwrap();
        assert_eq!(embed.fields.len(), FIELD_COUNT);
        // Last field is the "and N more" indicator.
        assert!(embed.fields[FIELD_COUNT - 1].value.contains("more"));
    }

    #[test]
    fn build_run_complete_passes_twilight_validate() {
        // Construct a worst-case embed with long custom templates and
        // verify twilight-validate accepts it.
        let hb = handlebars();
        let templates = DiscordTemplateConfig {
            title: Some("Run for {{flow.name}}".into()),
            description: Some("SHA {{source.sha_short}} on {{source.ref_name}}".into()),
            field_name: Some("{{job.name}}".into()),
            field_value: Some("{{job.conclusion}} ({{job.url}})".into()),
            collapsed_summary: None,
        };
        let s = summary(Conclusion::Failure, 10);
        let embed = build_run_complete_embed(&hb, &templates, &ctx(), &s).unwrap();
        twilight_validate::embed::embed(&embed).expect("must validate");
    }

    #[test]
    fn build_run_complete_truncates_long_template_output() {
        // Title template renders to 1000 codepoints — must clamp to
        // TITLE_LENGTH (256).
        let hb = handlebars();
        let big = "a".repeat(1000);
        let templates = DiscordTemplateConfig {
            title: Some(big),
            ..DiscordTemplateConfig::default()
        };
        let s = summary(Conclusion::Success, 0);
        let embed = build_run_complete_embed(&hb, &templates, &ctx(), &s).unwrap();
        let title = embed.title.expect("title set");
        assert!(
            title.chars().count() <= TITLE_LENGTH,
            "title chars={} exceeds {}",
            title.chars().count(),
            TITLE_LENGTH,
        );
    }

    #[test]
    fn embed_codepoint_count_sums_all_fields() {
        let hb = handlebars();
        let templates = DiscordTemplateConfig {
            title: Some("hello".into()),
            ..DiscordTemplateConfig::default()
        };
        let s = summary(Conclusion::Success, 0);
        let embed = build_run_complete_embed(&hb, &templates, &ctx(), &s).unwrap();
        assert!(embed_codepoint_count(&embed) > 0);
        assert!(embed_codepoint_count(&embed) <= EMBED_TOTAL_CODEPOINTS);
    }

    #[test]
    fn build_run_complete_uses_flow_name_in_template() {
        // Templates render against `notify::render_context` so
        // `{{flow.name}}` resolves at runtime. Regression: an earlier
        // local builder shadowed the shared helper and silently
        // dropped variables.
        let hb = handlebars();
        let templates = DiscordTemplateConfig {
            title: Some("flow={{flow.name}}".into()),
            ..DiscordTemplateConfig::default()
        };
        let s = summary(Conclusion::Success, 0);
        let embed = build_run_complete_embed(&hb, &templates, &ctx(), &s).unwrap();
        assert_eq!(embed.title.as_deref(), Some("flow=ci-flow"));
    }

    #[test]
    fn build_run_complete_renders_run_status_and_dispatched_at() {
        // Templates that compile against probe_context exercise
        // `run.status` and `action.dispatched_at`. Both must resolve
        // at runtime — a probe-vs-runtime mismatch surfaces here as
        // a strict-mode RenderError.
        let hb = handlebars();
        let templates = DiscordTemplateConfig {
            title: Some("status={{run.status}} at={{action.dispatched_at}}".into()),
            ..DiscordTemplateConfig::default()
        };
        let s = summary(Conclusion::Success, 0);
        let embed = build_run_complete_embed(&hb, &templates, &ctx(), &s).unwrap();
        let title = embed.title.expect("title set");
        assert!(
            title.starts_with("status=completed at="),
            "title: {title:?}"
        );
    }

    #[test]
    fn build_run_complete_renders_gcit_run_id() {
        // `{{gcit.run_id}}` is a reserved namespace — must surface the
        // UUID at runtime.
        let hb = handlebars();
        let templates = DiscordTemplateConfig {
            title: Some("gcit_run_id={{gcit.run_id}}".into()),
            ..DiscordTemplateConfig::default()
        };
        let s = summary(Conclusion::Success, 0);
        let embed = build_run_complete_embed(&hb, &templates, &ctx(), &s).unwrap();
        let title = embed.title.expect("title set");
        assert!(
            title.contains(&uuid::Uuid::nil().to_string()),
            "title missing nil uuid: {title}",
        );
    }
}

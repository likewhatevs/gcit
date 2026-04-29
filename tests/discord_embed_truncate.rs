// Embed truncation per twilight-validate constants.
//
// twilight-validate-0.17/src/embed.rs exposes:
//   AUTHOR_NAME_LENGTH = 256
//   COLOR_MAXIMUM = 0xff_ff_ff
//   DESCRIPTION_LENGTH = 4096
//   EMBED_TOTAL_LENGTH = 6000
//   FIELD_COUNT = 25
//   FIELD_NAME_LENGTH = 256
//   FIELD_VALUE_LENGTH = 1024
//   FOOTER_TEXT_LENGTH = 2048
//   TITLE_LENGTH = 256
//
// gcit truncates BEFORE the twilight-http call so the embed always
// validates. Truncation is by *codepoints* (the real Discord limit);
// twilight-validate's per-field validators use `.chars().count()` so
// codepoint-bounded strings always pass per-field. The aggregate
// EMBED_TOTAL_LENGTH check uses `.len()` (bytes) in twilight-validate;
// gcit does not enforce a 6000-byte aggregate cap of its own — these
// tests confirm the per-field caps are observed and document the
// twilight-validate aggregate behaviour.

use proptest::prelude::*;
use rstest::rstest;
use twilight_model::channel::message::embed::{EmbedField, EmbedFooter};
use twilight_model::channel::message::Embed;
use twilight_validate::embed::{
    embed as validate_embed, EmbedValidationErrorType, AUTHOR_NAME_LENGTH, COLOR_MAXIMUM,
    DESCRIPTION_LENGTH, EMBED_TOTAL_LENGTH, FIELD_COUNT, FIELD_NAME_LENGTH, FIELD_VALUE_LENGTH,
    FOOTER_TEXT_LENGTH, TITLE_LENGTH,
};

use gcit::config::DiscordTemplateConfig;
use gcit::discord::embed::{build_run_complete_embed, embed_codepoint_count};
use gcit::discord::template::truncate_codepoints;
use gcit::github::{Conclusion, JobResult, RunStatus, RunSummary};
use gcit::notify::{strict_handlebars, ActionInfo, RunContext, SourceInfo};

#[test]
fn pin_twilight_validate_constants() {
    // Regression guard: if a twilight-validate minor bump shifts any
    // of these, gcit's truncation logic must be revisited. Cargo.lock
    // catches semver-major changes; this test catches in-range
    // surprises that would silently break production.
    assert_eq!(TITLE_LENGTH, 256);
    assert_eq!(DESCRIPTION_LENGTH, 4096);
    assert_eq!(AUTHOR_NAME_LENGTH, 256);
    assert_eq!(FOOTER_TEXT_LENGTH, 2048);
    assert_eq!(FIELD_COUNT, 25);
    assert_eq!(FIELD_NAME_LENGTH, 256);
    assert_eq!(FIELD_VALUE_LENGTH, 1024);
    assert_eq!(EMBED_TOTAL_LENGTH, 6000);
    assert_eq!(COLOR_MAXIMUM, 0xff_ff_ff);
}

#[rstest]
#[case::title(257, 256)]
#[case::description(4097, 4096)]
#[case::field_name(257, 256)]
#[case::field_value(1025, 1024)]
#[case::footer(2049, 2048)]
#[case::author_name(257, 256)]
fn truncate_each_leaf_field_to_max(#[case] input_len: usize, #[case] expect: usize) {
    // Strategy: gcit's `truncate_codepoints(input, max)` returns at
    // most `max` codepoints; when truncation fires, one codepoint is
    // reserved for the trailing ellipsis (U+2026). For ASCII inputs
    // (which is what this rstest covers), codepoints == bytes.
    //
    // Mutation target: hard byte cut — would produce invalid UTF-8 on
    // multibyte boundaries (covered by the proptest below) and over
    // expected length for codepoint-counted assertions.
    let input: String = "a".repeat(input_len);
    let truncated = truncate_codepoints(&input, expect);
    assert_eq!(
        truncated.chars().count(),
        expect,
        "truncate_codepoints({input_len}, {expect}) must return exactly {expect} codepoints",
    );
    assert!(
        truncated.ends_with('…'),
        "truncation must signal cut with the ellipsis suffix",
    );
    // Sanity: the truncated string is valid UTF-8 by construction
    // (truncate_codepoints uses chars().take().collect()), but the
    // assertion makes the property explicit.
    assert!(std::str::from_utf8(truncated.as_bytes()).is_ok());
}

proptest! {
    #![proptest_config(ProptestConfig {
        // 256 cases is enough to exercise the boundary surfaces
        // without dominating the suite's wallclock.
        cases: 256,
        ..ProptestConfig::default()
    })]

    #[test]
    fn truncate_preserves_utf8_boundaries(
        // Generate arbitrary unicode strings up to 2k chars long; max
        // values from 0 to 2k cover the whole range of per-field caps
        // (0=empty, 256=title, 1024=field value, 2048=footer).
        s in "\\PC{0,2048}",
        max in 0usize..=2048,
    ) {
        let out = truncate_codepoints(&s, max);
        // Property 1: result is valid UTF-8 (no mid-codepoint cut).
        prop_assert!(std::str::from_utf8(out.as_bytes()).is_ok());
        // Property 2: result honours the codepoint budget.
        prop_assert!(
            out.chars().count() <= max,
            "out chars={} must be <= max={}",
            out.chars().count(),
            max,
        );
    }
}

#[test]
fn embed_total_length_rejected_by_twilight_when_oversized() {
    // Per the production contract: gcit truncates per-field at
    // codepoint boundaries (so each leaf passes twilight-validate's
    // per-field check) but does NOT enforce the 6000-byte aggregate
    // cap itself — twilight-validate is the canonical authority for
    // EMBED_TOTAL_LENGTH and rejects oversize embeds via
    // EmbedValidationErrorType::EmbedTooLarge.
    //
    // This test verifies that contract: hand-build an embed whose
    // byte total exceeds EMBED_TOTAL_LENGTH (with every leaf field at
    // its individual cap, the byte total is ~38_656 — well over 6000)
    // and assert twilight-validate rejects it with EmbedTooLarge.
    //
    // The build path in src/discord/embed.rs::build_run_complete_embed
    // never produces an embed this large in practice because it caps
    // jobs at FIELD_COUNT (25) and each field is small in real
    // workloads. But operators could in theory write templates whose
    // rendered output is at every field's cap; in that pathological
    // case twilight-validate rejects the embed at the
    // build_run_complete_embed → twilight_validate::embed::embed gate
    // and the notifier surfaces the failure.
    let title = "T".repeat(TITLE_LENGTH);
    let description = "D".repeat(DESCRIPTION_LENGTH);
    let footer_text = "F".repeat(FOOTER_TEXT_LENGTH);
    let fields: Vec<EmbedField> = (0..FIELD_COUNT)
        .map(|_| EmbedField {
            inline: false,
            name: "N".repeat(FIELD_NAME_LENGTH),
            value: "V".repeat(FIELD_VALUE_LENGTH),
        })
        .collect();
    let embed = Embed {
        author: None,
        color: None,
        description: Some(description),
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
        title: Some(title),
        url: None,
        video: None,
    };
    let err =
        validate_embed(&embed).expect_err("oversize embed must be rejected by twilight-validate");
    assert!(
        matches!(err.kind(), EmbedValidationErrorType::EmbedTooLarge { .. }),
        "expected EmbedTooLarge, got {err:?}",
    );
}

#[test]
fn field_count_capped_at_25() {
    // build_run_complete_embed accepts a RunSummary with arbitrary
    // job count and caps the rendered fields at FIELD_COUNT (25). If
    // the run has more jobs than fit, the last field is reserved for
    // an "and N more" tail indicator instead of silently dropping
    // jobs.
    //
    // Mutation target: off-by-one (24 or 26) — caught by the exact
    // length assertion. Removing the tail indicator entirely is also
    // caught (the last field's value would not contain "more").
    let hb = strict_handlebars();
    let templates = DiscordTemplateConfig::default();
    let summary = run_summary(Conclusion::Failure, 30);
    let embed = build_run_complete_embed(&hb, &templates, &run_ctx(), &summary)
        .expect("build must succeed for a 30-job run");
    assert_eq!(
        embed.fields.len(),
        FIELD_COUNT,
        "embed must cap fields at FIELD_COUNT regardless of job count",
    );
    let last_value = &embed.fields[FIELD_COUNT - 1].value;
    assert!(
        last_value.contains("more"),
        "last field must signal truncation with an `and N more` tail; got {last_value:?}",
    );
}

#[test]
fn embed_post_truncate_validates_with_twilight() {
    // Final guard: every embed produced by build_run_complete_embed
    // for the realistic operator input space must validate end-to-
    // end against `twilight_validate::embed::embed`.
    //
    // Important: gcit truncates per field, not in aggregate.
    // twilight-validate's EMBED_TOTAL_LENGTH check sums byte lengths
    // (`.len()`) — so an operator who maxes EVERY field can still
    // produce an embed exceeding 6000 bytes. The pathological
    // max-everything case is covered separately by
    // `embed_total_length_rejected_by_twilight_when_oversized`,
    // which documents that twilight-validate (not gcit) is the
    // 6000-byte authority. Here we cover the *realistic* cases
    // operators would hit and assert twilight accepts each.
    let hb = strict_handlebars();

    // Case 1: realistic custom templates. Title and description are
    // short, per-job fields use {{job.name}} / {{job.conclusion}}
    // which render to <100 codepoints each. 25 such fields fit
    // comfortably under 6000 bytes.
    let templates_realistic = DiscordTemplateConfig {
        title: Some("Run for {{flow.name}} on {{source.ref_name}}".into()),
        description: Some(
            "SHA {{source.sha_short}} · run {{action.run_id}} · dispatched {{action.dispatched_at}}"
                .into(),
        ),
        field_name: Some("{{job.name}}".into()),
        field_value: Some("[{{job.conclusion}}]({{job.url}})".into()),
        collapsed_summary: None,
    };
    let summary_realistic = run_summary(Conclusion::Failure, FIELD_COUNT);
    let embed_realistic =
        build_run_complete_embed(&hb, &templates_realistic, &run_ctx(), &summary_realistic)
            .expect("realistic-template build must succeed");
    validate_embed(&embed_realistic).expect("realistic-template embed must validate");

    // Case 2: multibyte content at modest sizes. Emoji and CJK
    // characters take more bytes per codepoint, so we keep job count
    // and template length small to stay under the 6000-byte cap.
    let templates_multibyte = DiscordTemplateConfig {
        title: Some("Run 😀 {{flow.name}}".into()),
        description: Some("Test 中文 description for {{action.workflow}}".into()),
        field_name: None,
        field_value: None,
        collapsed_summary: None,
    };
    let summary_mb = run_summary(Conclusion::Failure, 5);
    let embed_mb = build_run_complete_embed(&hb, &templates_multibyte, &run_ctx(), &summary_mb)
        .expect("multibyte build must succeed");
    validate_embed(&embed_mb).expect("multibyte embed must validate");

    // Case 3: empty/default templates with no jobs (success path,
    // collapsed summary, minimal embed).
    let templates_empty = DiscordTemplateConfig::default();
    let summary_empty = run_summary(Conclusion::Success, 0);
    let embed_empty = build_run_complete_embed(&hb, &templates_empty, &run_ctx(), &summary_empty)
        .expect("default build must succeed");
    validate_embed(&embed_empty).expect("default embed must validate");

    // Case 4: codepoint-count assertion as a sanity check on the
    // per-field truncation. `embed_codepoint_count` mirrors
    // twilight-validate's `chars()` helper but counts codepoints, so
    // it gives an upper bound of what twilight-validate would see
    // under a hypothetical codepoint-correct aggregate check.
    assert!(embed_codepoint_count(&embed_realistic) > 0);
    assert!(embed_codepoint_count(&embed_mb) > 0);

    // Case 5: per-field caps still enforced — even when the operator
    // writes a cap-busting template, every individual field clamps
    // at its codepoint cap so the per-field arms of
    // twilight_validate::embed never trigger. Build with an oversized
    // title template, validate per-field via assertions on the
    // returned embed.
    let templates_oversize_title = DiscordTemplateConfig {
        title: Some("X".repeat(TITLE_LENGTH + 100)),
        ..DiscordTemplateConfig::default()
    };
    let summary_zero = run_summary(Conclusion::Success, 0);
    let embed_t =
        build_run_complete_embed(&hb, &templates_oversize_title, &run_ctx(), &summary_zero)
            .expect("oversize-title build must succeed");
    let title = embed_t.title.as_deref().expect("title set");
    assert!(
        title.chars().count() <= TITLE_LENGTH,
        "title codepoints={} must be <= TITLE_LENGTH={TITLE_LENGTH}",
        title.chars().count(),
    );
    validate_embed(&embed_t).expect("clamped-title embed must validate");
}

#[test]
fn empty_input_yields_empty_string_not_panic() {
    // truncate_codepoints("", N) for any N must return "" without
    // panicking — empty input is a valid handlebars rendering when
    // every variable resolves to an empty string (e.g. flow.description
    // unset). Mutation target: using unwrap on
    // chars().nth() or similar, panics on the empty iterator.
    for max in [0usize, 1, 5, 256, 4096, EMBED_TOTAL_LENGTH] {
        let out = truncate_codepoints("", max);
        assert_eq!(
            out, "",
            "truncate_codepoints(\"\", {max}) must return empty"
        );
    }
    // Sanity at max=0 with non-empty input — also documented in
    // src/discord/template.rs::truncate_codepoints; mirrored here so
    // the integration suite catches a regression.
    assert_eq!(truncate_codepoints("anything", 0), "");
}

// --- helpers ----------------------------------------------------------------

fn run_ctx() -> RunContext {
    RunContext {
        flow_name: "ci-flow".into(),
        flow_description: None,
        source: SourceInfo {
            url: "https://example.com/repo.git".into(),
            ref_name: "refs/heads/main".into(),
            sha: gix_hash::ObjectId::null(gix_hash::Kind::Sha1),
            sha_short: "0000000".into(),
        },
        action: ActionInfo {
            repo: "owner/repo".into(),
            workflow: "ci.yml".into(),
            run_id: 42,
            run_url: "https://github.com/owner/repo/actions/runs/42".into(),
            dispatched_at: chrono::Utc::now(),
        },
        gcit_run_id: uuid::Uuid::nil(),
    }
}

fn run_summary(conclusion: Conclusion, jobs: usize) -> RunSummary {
    RunSummary {
        run_id: 42,
        run_url: "https://github.com/owner/repo/actions/runs/42".into(),
        run_number: 7,
        run_attempt: 1,
        status: RunStatus::Completed,
        conclusion: Some(conclusion),
        started_at: Some(chrono::Utc::now()),
        completed_at: Some(chrono::Utc::now()),
        jobs: (0..jobs)
            .map(|i| JobResult {
                job_id: 100 + i as u64,
                name: format!("job-{i}"),
                html_url: format!("https://github.com/owner/repo/actions/jobs/{i}"),
                conclusion: Some(Conclusion::Failure),
                started_at: Some(chrono::Utc::now()),
                completed_at: Some(chrono::Utc::now()),
                steps: Vec::new(),
                run_attempt: 1,
            })
            .collect(),
    }
}

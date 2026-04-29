// Conclusion -> Discord embed color mapping (integration coverage).
//
// Discord embed color is a 24-bit RGB int (0xRRGGBB). twilight-validate
// rejects values > COLOR_MAXIMUM (0xff_ff_ff) at the embed level —
// gcit must produce values inside that range for every Conclusion
// variant so embed validation never rejects a gcit-emitted color.
//
// The hex palette and per-variant mapping are pinned by the in-module
// unit test `crate::discord::conclusion::tests::color_for_pins_palette`
// (the canonical drift detector — runs in the dispatcher binary's
// `cargo test`). This file's role is the integration boundary:
//
//   1. Range check across every variant (defends against a malformed
//      RGB literal sneaking past per-variant pinning, e.g. an alpha
//      byte that pushes a value above 0xff_ff_ff).
//   2. End-to-end check that `Embed.color` is actually populated by
//      `build_run_complete_embed` for every conclusion — defends
//      against an implementation that computes the right color but
//      forgets to assign it to the field, leaving `embed.color = None`.

use chrono::Utc;
use gix_hash::ObjectId;
use twilight_validate::embed::COLOR_MAXIMUM;
use uuid::Uuid;

use gcit::config::DiscordTemplateConfig;
use gcit::discord::conclusion::color_for;
use gcit::discord::embed::build_run_complete_embed;
use gcit::github::{Conclusion, RunStatus, RunSummary};
use gcit::notify::{strict_handlebars, ActionInfo, RunContext, SourceInfo};

const ALL_CONCLUSIONS: [Conclusion; 8] = [
    Conclusion::Success,
    Conclusion::Skipped,
    Conclusion::Neutral,
    Conclusion::Failure,
    Conclusion::TimedOut,
    Conclusion::Cancelled,
    Conclusion::ActionRequired,
    Conclusion::Unknown,
];

fn ctx() -> RunContext {
    RunContext {
        flow_name: "ci-flow".into(),
        flow_description: None,
        source: SourceInfo {
            url: "https://example.com/repo.git".into(),
            ref_name: "refs/heads/main".into(),
            sha: ObjectId::null(gix_hash::Kind::Sha1),
            sha_short: "0000000".into(),
        },
        action: ActionInfo {
            repo: "owner/repo".into(),
            workflow: "ci.yml".into(),
            run_id: 42,
            run_url: "https://github.com/owner/repo/actions/runs/42".into(),
            dispatched_at: Utc::now(),
        },
        gcit_run_id: Uuid::nil(),
    }
}

fn summary(c: Conclusion) -> RunSummary {
    RunSummary {
        run_id: 42,
        run_url: "https://github.com/owner/repo/actions/runs/42".into(),
        run_number: 7,
        run_attempt: 1,
        status: RunStatus::Completed,
        conclusion: Some(c),
        started_at: Some(Utc::now()),
        completed_at: Some(Utc::now()),
        // Use 0 jobs so the test focuses on the color field — the
        // collapsed/expanded branch difference doesn't affect color
        // (color is set unconditionally before the field-build branch
        // at src/discord/embed.rs:84-87).
        jobs: Vec::new(),
    }
}

#[test]
fn all_colors_below_color_maximum() {
    // Every variant's color must satisfy `color <= COLOR_MAXIMUM`
    // (0xff_ff_ff). A regression where a literal slips past the
    // 24-bit boundary (e.g. accidentally writing 0xRRGGBBAA with an
    // alpha byte) would fail twilight-validate at runtime.
    for c in ALL_CONCLUSIONS {
        let rgb = color_for(c);
        assert!(
            rgb <= COLOR_MAXIMUM,
            "{c:?} -> 0x{rgb:08x} exceeds COLOR_MAXIMUM=0x{:06x}",
            COLOR_MAXIMUM,
        );
    }
}

#[test]
fn color_set_at_embed_color_field() {
    // For every Conclusion variant, building the run-complete embed
    // produces an Embed whose `color` field is populated with the
    // value `color_for` returns for that conclusion.
    //
    // Per twilight-model/src/channel/message/embed/mod.rs, `Embed.color`
    // is `Option<u32>`; `None` means "use Discord's default sidebar
    // gray", which gcit explicitly avoids by always assigning Some(_).
    //
    // Mutation target: a builder that drops the assignment on the
    // success path (e.g. by short-circuiting before the color
    // statement) would leave `embed.color = None` for that variant —
    // operator sees a default-gray strip regardless of conclusion.
    let hb = strict_handlebars();
    let templates = DiscordTemplateConfig::default();
    for c in ALL_CONCLUSIONS {
        let s = summary(c);
        let embed = build_run_complete_embed(&hb, &templates, &ctx(), &s)
            .unwrap_or_else(|e| panic!("build embed for {c:?}: {e}"));
        let expected = color_for(c);
        assert_eq!(
            embed.color,
            Some(expected),
            "{c:?}: embed.color must be Some(0x{expected:06x}); got {:?}",
            embed.color,
        );
    }
}

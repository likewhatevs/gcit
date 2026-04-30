// Embed shape: collapsed (success-like) vs expanded (failure-like).
//
// Collapse partition: Success / Skipped / Neutral collapse to a brief
// single-line summary embed; Failure / TimedOut / Cancelled /
// ActionRequired / Unknown emit a full embed with per-job fields. The
// intent: don't spam Discord with detailed Success summaries; do give
// full detail when something needs human attention.
//
// The collapse decision lives in `crate::github::should_collapse`
// (consumed by both Discord and mail notifiers — it describes
// `Conclusion` semantics, not Discord-specific rendering). The
// rendering side that branches on it lives in
// `crate::discord::embed::build_run_complete_embed`.
//
// These tests pin the rendering branch for each side of the partition
// (collapsed → exactly one "Summary" field; expanded → one inline
// field per job). The Conclusion-level partition is also pinned by
// the in-module unit test
// `crate::github::tests::should_collapse_matches_design_spec` and the
// integration test
// `tests/github_conclusion_mapping.rs::collapse_set_pins_documented_partition`
// — those exercise `should_collapse` directly; this file exercises
// the embed builder downstream of it.

use chrono::Utc;
use gix_hash::ObjectId;
use uuid::Uuid;

use gcit::config::DiscordTemplateConfig;
use gcit::discord::embed::build_run_complete_embed;
use gcit::github::{Conclusion, JobResult, RunStatus, RunSummary};
use gcit::notify::{strict_handlebars, ActionInfo, RunContext, SourceInfo};

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

/// Build a `RunSummary` with `n` Success-conclusion jobs and a
/// caller-controlled run-level conclusion. `run_conclusion` is the
/// `Option<Conclusion>` carried on `RunSummary.conclusion`; pass
/// `None` to model an in-progress run.
fn summary(run_conclusion: Option<Conclusion>, jobs: usize) -> RunSummary {
    RunSummary {
        run_id: 42,
        run_url: "https://github.com/owner/repo/actions/runs/42".into(),
        run_number: 7,
        run_attempt: 1,
        status: RunStatus::Completed,
        conclusion: run_conclusion,
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

/// Build a `RunSummary` with one Failure job for tests that need a
/// run-level Success with a job-level Failure inside (intentionally
/// impossible per current GitHub semantics — guards the run-level
/// scope of the collapse decision).
fn summary_with_failed_job(run_conclusion: Conclusion) -> RunSummary {
    let mut s = summary(Some(run_conclusion), 0);
    s.jobs.push(JobResult {
        job_id: 200,
        name: "doomed".into(),
        html_url: "https://github.com/owner/repo/actions/jobs/200".into(),
        conclusion: Some(Conclusion::Failure),
        started_at: Some(Utc::now()),
        completed_at: Some(Utc::now()),
        steps: Vec::new(),
        run_attempt: 1,
    });
    s
}

#[test]
fn collapsed_run_uses_collapsed_summary_template() {
    // For a Success run, the embed builder picks the collapsed branch:
    // exactly one inline field with the configured (or default)
    // `collapsed_summary` value, nothing per-job.
    //
    // Mutation target: a builder that picks the expanded branch for
    // Success would emit one field per job (3 in this fixture)
    // instead of the single "Summary" field.
    let hb = strict_handlebars();
    let templates = DiscordTemplateConfig {
        collapsed_summary: Some("{{flow.name}}: PASS".into()),
        ..DiscordTemplateConfig::default()
    };
    let s = summary(Some(Conclusion::Success), 3);
    let embed = build_run_complete_embed(&hb, &templates, &ctx(), &s).expect("build embed");
    assert_eq!(
        embed.fields.len(),
        1,
        "collapsed branch must emit exactly one Summary field; got {} fields",
        embed.fields.len(),
    );
    let f = &embed.fields[0];
    assert_eq!(f.name, "Summary");
    assert_eq!(f.value, "ci-flow: PASS");
}

#[test]
fn non_collapsed_run_uses_full_template() {
    // For a Failure run, the embed builder picks the expanded branch:
    // one inline field per job (capped at FIELD_COUNT, with a tail
    // field on overflow). With 4 jobs we expect 4 fields, all
    // inline, none of them named "Summary" (that's the collapsed
    // marker).
    //
    // Mutation target: a builder that reuses the collapsed path for
    // Failure would surface as a single "Summary" field — operator
    // never sees per-job detail.
    let hb = strict_handlebars();
    let templates = DiscordTemplateConfig::default();
    let s = summary(Some(Conclusion::Failure), 4);
    let embed = build_run_complete_embed(&hb, &templates, &ctx(), &s).expect("build embed");
    assert_eq!(
        embed.fields.len(),
        4,
        "expanded branch must emit one field per job; got {} fields",
        embed.fields.len(),
    );
    for (i, field) in embed.fields.iter().enumerate() {
        assert!(field.inline, "per-job field {i} must be inline");
        assert_ne!(
            field.name, "Summary",
            "field {i} must not carry the collapsed-branch name",
        );
    }
}

#[test]
fn collapse_decision_uses_run_conclusion_not_job_conclusion() {
    // The collapse decision branches on `RunSummary.conclusion`, not
    // on individual `RunSummary.jobs[].conclusion`. A Success run
    // with a Failure job inside still collapses — we trust the
    // run-level signal because GitHub bubbles per-job failures up
    // to the run conclusion, but if a future workflow shape decouples
    // them this test guards the chosen scope.
    //
    // Mutation target: a builder that recurses into
    // `summary.jobs[].conclusion` and only collapses when ALL jobs
    // are in the collapse-set would treat this fixture as expanded
    // (because of the Failure job) and emit per-job fields.
    let hb = strict_handlebars();
    let templates = DiscordTemplateConfig::default();
    let s = summary_with_failed_job(Conclusion::Success);
    let embed = build_run_complete_embed(&hb, &templates, &ctx(), &s).expect("build embed");
    assert_eq!(
        embed.fields.len(),
        1,
        "run-level Success must collapse regardless of per-job conclusions; got {} fields",
        embed.fields.len(),
    );
    assert_eq!(embed.fields[0].name, "Summary");
}

#[test]
fn collapse_with_conclusion_none_does_not_collapse() {
    // `RunSummary.conclusion` is `Option<Conclusion>` — `None` means
    // "still in progress" (RunStatus::InProgress / Queued / Waiting).
    // The embed builder treats `None` as "do not collapse" so any
    // progress notifications surface full per-job detail (which the
    // operator can use to see what's pending).
    //
    // The implementation is `summary.conclusion.map(should_collapse)
    // .unwrap_or(false)` inside build_run_complete_embed. This test
    // pins the unwrap_or default — flipping it to `true` would
    // collapse every progress event into a single Summary field.
    let hb = strict_handlebars();
    let templates = DiscordTemplateConfig::default();
    let s = summary(None, 2);
    let embed = build_run_complete_embed(&hb, &templates, &ctx(), &s).expect("build embed");
    assert_eq!(
        embed.fields.len(),
        2,
        "None conclusion (in-progress) must NOT collapse; got {} fields",
        embed.fields.len(),
    );
    for (i, field) in embed.fields.iter().enumerate() {
        assert!(field.inline, "per-job field {i} must be inline");
    }
}

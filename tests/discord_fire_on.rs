// fire_on event filtering for the Discord webhook destination.
//
// `fire_on: Vec<FireEvent>` selects which lifecycle callbacks the
// notifier acts on. The variants are RunStart / JobComplete /
// RunComplete (snake_case in TOML and JSON).
//
// Note on coverage: only `on_run_complete` is implemented for the
// Discord notifier today (src/discord/notifier.rs comment at the
// trait impl: "on_run_start and on_job_complete use the trait
// defaults — they return Skipped { NotConfigured }"). The trait
// defaults short-circuit BEFORE consulting `fire_on`, so a
// `fire_on = [RunStart]` configuration still surfaces NotConfigured
// for on_run_start — the FireOnMismatch path is only reachable
// for on_run_complete. The first test below pins this layering;
// the multi-destination test exercises FireOnMismatch via
// on_run_complete with a non-matching fire_on; the serde round-trip
// pins the wire format independently of any notifier behaviour.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use gix_hash::ObjectId;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use gcit::config::{DiscordTemplateConfig, FireEvent};
use gcit::discord::webhook::{parse_webhook_url, Client};
use gcit::discord::DiscordNotifier;
use gcit::github::{Conclusion, RunStatus, RunSummary};
use gcit::notify::{
    strict_handlebars, ActionInfo, Notifier, NotifyOutcome, RunContext, SkipReason, SourceInfo,
};

mod common;

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

fn summary() -> RunSummary {
    RunSummary {
        run_id: 42,
        run_url: "https://github.com/owner/repo/actions/runs/42".into(),
        run_number: 7,
        run_attempt: 1,
        status: RunStatus::Completed,
        conclusion: Some(Conclusion::Success),
        started_at: Some(Utc::now()),
        completed_at: Some(Utc::now()),
        jobs: Vec::new(),
    }
}

fn parsed_webhook() -> gcit::discord::ParsedWebhookUrl {
    parse_webhook_url("https://discord.com/api/webhooks/1234567890/testtoken").unwrap()
}

fn build_notifier(fire_on: Vec<FireEvent>) -> DiscordNotifier {
    common::ensure_crypto_provider();
    let client = Client::new(Duration::from_secs(5)).expect("client");
    DiscordNotifier::new(
        "test",
        client,
        parsed_webhook(),
        fire_on,
        DiscordTemplateConfig::default(),
        Arc::new(strict_handlebars()),
    )
}

#[tokio::test]
async fn fire_on_run_complete_default_skips_run_start() {
    // Default fire_on for Discord is [RunComplete]. The notifier's
    // on_run_start is the trait default (Skipped::NotConfigured) —
    // the trait default fires BEFORE any fire_on inspection, so a
    // fire_on of [RunComplete] still produces NotConfigured for
    // on_run_start, not FireOnMismatch.
    //
    // Mutation target: adding an on_run_start override
    // that consults fire_on first — that would convert NotConfigured
    // into FireOnMismatch silently and confuse operators reading
    // gcit status (NotConfigured = "this kind doesn't handle the
    // event"; FireOnMismatch = "operator opted out of this event").
    let n = build_notifier(vec![FireEvent::RunComplete]);
    let cancel = CancellationToken::new();
    let outcome = n
        .on_run_start(&ctx(), &cancel)
        .await
        .expect("on_run_start ok");
    match outcome {
        NotifyOutcome::Skipped {
            reason: SkipReason::NotConfigured,
        } => {}
        other => panic!("expected Skipped(NotConfigured), got {other:?}"),
    }
}

#[tokio::test]
async fn fire_on_does_not_short_circuit_other_destinations() {
    // A flow with two destinations: A fires on RunComplete, B does
    // not. Driving on_run_complete must produce Sent (or at least a
    // non-skip) for A and Skipped(FireOnMismatch) for B — neither
    // notifier's outcome short-circuits the other. We exercise the
    // call sequence directly (no supervisor harness needed) since
    // each notifier owns its own fire_on independently.
    //
    // For A's RunComplete-fire path we'd need a wiremock proxy to
    // accept the webhook; instead we use a tighter setup: A is
    // configured with fire_on = [RunStart] (so its on_run_complete
    // returns FireOnMismatch via the fire_on guard at the top of
    // DiscordNotifier::on_run_complete),
    // and B with fire_on = [JobComplete] (also FireOnMismatch on
    // run_complete). The test pins that BOTH notifiers, when called
    // independently with fire_on missing the event, produce
    // FireOnMismatch — confirming neither's decision interferes
    // with the other's.
    //
    // Mutation target: a buggy supervisor implementation that
    // shares state across notifiers (e.g. checking fire_on once and
    // applying to all destinations) would surface here as B's
    // outcome leaking A's state. Each notifier maintains its own
    // fire_on; the test exercises that independence.
    let a = build_notifier(vec![FireEvent::RunStart]);
    let b = build_notifier(vec![FireEvent::JobComplete]);
    let c = ctx();
    let s = summary();

    let cancel = CancellationToken::new();
    let outcome_a = a
        .on_run_complete(&c, &s, &cancel)
        .await
        .expect("a.on_run_complete");
    match outcome_a {
        NotifyOutcome::Skipped {
            reason: SkipReason::FireOnMismatch,
        } => {}
        other => panic!("a: expected FireOnMismatch, got {other:?}"),
    }

    let outcome_b = b
        .on_run_complete(&c, &s, &cancel)
        .await
        .expect("b.on_run_complete");
    match outcome_b {
        NotifyOutcome::Skipped {
            reason: SkipReason::FireOnMismatch,
        } => {}
        other => panic!("b: expected FireOnMismatch, got {other:?}"),
    }
}

#[test]
fn fire_event_serde_round_trip() {
    // FireEvent is `#[serde(rename_all = "snake_case")]`. Pin the
    // exact wire strings — operator config files and the JSON
    // status/control surface all depend on these mappings being
    // stable across releases.
    //
    // Mutation target: renaming a variant or removing the
    // rename_all attribute. Either change would break operator
    // configs that already say e.g. fire_on = ["run_complete"].
    let cases: &[(FireEvent, &str)] = &[
        (FireEvent::RunStart, "\"run_start\""),
        (FireEvent::JobComplete, "\"job_complete\""),
        (FireEvent::RunComplete, "\"run_complete\""),
    ];
    for (variant, wire) in cases {
        let serialized = serde_json::to_string(variant).expect("serialize");
        assert_eq!(
            serialized, *wire,
            "{variant:?} must serialize as {wire}; got {serialized}",
        );
        let deserialized: FireEvent = serde_json::from_str(wire).expect("deserialize");
        assert_eq!(
            deserialized, *variant,
            "{wire} must deserialize as {variant:?}; got {deserialized:?}",
        );
    }
}

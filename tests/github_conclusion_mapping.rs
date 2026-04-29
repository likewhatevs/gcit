// Conclusion enum mapping: pure-logic table tests through the public
// `gcit::github::*` API. No async, no wiremock. The
// rename_all="snake_case" + #[serde(other)] catch-all on Conclusion
// makes new GitHub conclusion strings surface as Unknown rather than
// crashing the monitor.

use rstest::rstest;

use gcit::github::{label_for, should_collapse, Conclusion};

#[rstest]
#[case::success("success", Conclusion::Success)]
#[case::skipped("skipped", Conclusion::Skipped)]
#[case::neutral("neutral", Conclusion::Neutral)]
#[case::failure("failure", Conclusion::Failure)]
#[case::timed_out("timed_out", Conclusion::TimedOut)]
#[case::cancelled("cancelled", Conclusion::Cancelled)]
#[case::action_required("action_required", Conclusion::ActionRequired)]
// Real GitHub conclusions not in the typed enum — must map to Unknown:
#[case::stale_unknown("stale", Conclusion::Unknown)]
#[case::startup_failure_unknown("startup_failure", Conclusion::Unknown)]
#[case::empty_unknown("", Conclusion::Unknown)]
#[case::garbage_unknown("garbage_string_xyz", Conclusion::Unknown)]
fn api_string_to_conclusion(#[case] api_str: &str, #[case] expected: Conclusion) {
    assert_eq!(Conclusion::from_api(api_str), expected);
}

#[rstest]
#[case::success(Conclusion::Success, "success")]
#[case::skipped(Conclusion::Skipped, "skipped")]
#[case::neutral(Conclusion::Neutral, "neutral")]
#[case::failure(Conclusion::Failure, "failure")]
#[case::timed_out(Conclusion::TimedOut, "timed_out")]
#[case::cancelled(Conclusion::Cancelled, "cancelled")]
#[case::action_required(Conclusion::ActionRequired, "action_required")]
#[case::unknown(Conclusion::Unknown, "unknown")]
fn conclusion_to_api_pins_canonical_strings(#[case] c: Conclusion, #[case] expected: &str) {
    // Conclusion::to_api is the inverse of from_api and is what
    // RunFinished's persisted `conclusion` field uses. Pin the
    // literals so a refactor doesn't silently break state.json
    // round-trip.
    assert_eq!(c.to_api(), expected);
}

#[test]
fn conclusion_to_from_api_round_trips() {
    for c in [
        Conclusion::Success,
        Conclusion::Skipped,
        Conclusion::Neutral,
        Conclusion::Failure,
        Conclusion::TimedOut,
        Conclusion::Cancelled,
        Conclusion::ActionRequired,
        Conclusion::Unknown,
    ] {
        assert_eq!(Conclusion::from_api(c.to_api()), c, "round trip for {c:?}");
    }
}

#[test]
fn conclusion_serde_round_trips_through_json() {
    // serde rename_all="snake_case" + Serialize+Deserialize via derive
    // matches the to_api/from_api pair. RunFinished.conclusion stores
    // a String form (not the typed enum) per the state.json schema, so
    // the serde round-trip below validates the wire shape independently
    // from to_api/from_api.
    for c in [
        Conclusion::Success,
        Conclusion::Skipped,
        Conclusion::Neutral,
        Conclusion::Failure,
        Conclusion::TimedOut,
        Conclusion::Cancelled,
        Conclusion::ActionRequired,
    ] {
        let s = serde_json::to_string(&c).unwrap();
        let back: Conclusion = serde_json::from_str(&s).unwrap();
        assert_eq!(c, back);
    }
    // Unknown's serialization yields "unknown"; deserializing "unknown"
    // round-trips back to Unknown via the catch-all.
    let unknown_s = serde_json::to_string(&Conclusion::Unknown).unwrap();
    let unknown_back: Conclusion = serde_json::from_str(&unknown_s).unwrap();
    assert_eq!(unknown_back, Conclusion::Unknown);
}

#[test]
fn collapse_set_pins_documented_partition() {
    // Collapse set: Success/Skipped/Neutral.
    // Don't-collapse set: Failure/TimedOut/Cancelled/ActionRequired/Unknown.
    assert!(should_collapse(Conclusion::Success));
    assert!(should_collapse(Conclusion::Skipped));
    assert!(should_collapse(Conclusion::Neutral));
    assert!(!should_collapse(Conclusion::Failure));
    assert!(!should_collapse(Conclusion::TimedOut));
    assert!(!should_collapse(Conclusion::Cancelled));
    assert!(!should_collapse(Conclusion::ActionRequired));
    assert!(!should_collapse(Conclusion::Unknown));
}

#[rstest]
#[case::success(Conclusion::Success, "success")]
#[case::failure(Conclusion::Failure, "failure")]
#[case::timed_out(Conclusion::TimedOut, "timed out")]
#[case::action_required(Conclusion::ActionRequired, "action required")]
#[case::unknown(Conclusion::Unknown, "unknown")]
fn label_for_pins_operator_facing_strings(#[case] c: Conclusion, #[case] expected: &'static str) {
    // label_for is the prose-rendering helper (Discord embed titles,
    // mbox subjects). Mutation target: hex/casing drift between
    // label_for and to_api would silently break notifications.
    assert_eq!(label_for(c), expected);
}

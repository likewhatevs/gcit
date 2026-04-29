// SHA comparison logic (baseline, change detected, no-change).
// poll: source_interval, jittered, rate-bucketed. SHA diff ->
// TriggerSignal.
//
// The diff logic is shared across all three strategies (GithubApi,
// Grokmirror, LsRemote). It compares the just-fetched SHA against the
// last-observed SHA. Three outcomes:
//
//   1. last == None  (first poll for this flow) -> baseline; no trigger.
//   2. last == observed -> no change; no trigger.
//   3. last != observed -> change detected; trigger.
//
// The state-writer side handles the PollObservation emission separately
// (callers always record the observation; compare_sha decides only
// whether the dispatcher should fire). Tests here pin the trigger
// decision against the public `gcit::git::compare_sha` API.

use rstest::rstest;

use gcit::git::compare_sha;

fn sha(byte: u8) -> gix_hash::ObjectId {
    let hex = format!("{byte:02x}").repeat(20);
    gix_hash::ObjectId::from_hex(hex.as_bytes()).unwrap()
}

#[test]
fn first_poll_records_baseline_no_trigger() {
    let outcome = compare_sha(None, sha(0xaa));
    assert_eq!(outcome.observed, sha(0xaa));
    assert!(!outcome.trigger, "first poll must not trigger");
}

#[test]
fn unchanged_sha_does_not_trigger() {
    let s = sha(0xaa);
    let outcome = compare_sha(Some(s), s);
    assert_eq!(outcome.observed, s);
    assert!(
        !outcome.trigger,
        "unchanged sha must not trigger (same byte sequence)",
    );
}

#[test]
fn changed_sha_triggers() {
    let outcome = compare_sha(Some(sha(0xaa)), sha(0xbb));
    assert_eq!(outcome.observed, sha(0xbb));
    assert!(outcome.trigger, "different sha must trigger");
}

#[rstest]
#[case::all_zeros(0x00, 0x00, false)]
#[case::all_fs(0xff, 0xff, false)]
#[case::change(0xde, 0xad, true)]
fn parameterized_change_detection(
    #[case] last_byte: u8,
    #[case] observed_byte: u8,
    #[case] expect_trigger: bool,
) {
    let outcome = compare_sha(Some(sha(last_byte)), sha(observed_byte));
    assert_eq!(outcome.trigger, expect_trigger);
}

#[test]
fn sha1_byte_equality_is_canonical() {
    // gix_hash::ObjectId compares byte-for-byte. The hex representation
    // is normalized to lowercase via from_hex. Two ObjectIds parsed
    // from the same hex (irrespective of input case) compare equal.
    let lower = gix_hash::ObjectId::from_hex("deadbeef00deadbeef00deadbeef00deadbeef00".as_bytes())
        .unwrap();
    let upper = gix_hash::ObjectId::from_hex("DEADBEEF00DEADBEEF00DEADBEEF00DEADBEEF00".as_bytes())
        .unwrap();
    assert_eq!(lower, upper);
    let outcome = compare_sha(Some(lower), upper);
    assert!(
        !outcome.trigger,
        "case-only difference must not trigger (lower==upper after parse)",
    );
}

#[test]
fn comparison_is_deterministic() {
    // Pure function: same inputs -> same outputs. Run with fixed
    // inputs many times; assert all calls produce identical
    // DiffOutcome values.
    let last = sha(0x01);
    let observed = sha(0x02);
    let baseline = compare_sha(Some(last), observed);
    for _ in 0..1_000 {
        assert_eq!(compare_sha(Some(last), observed), baseline);
    }
}

#[test]
fn baseline_after_state_cleared_is_silent_first_poll() {
    // Edge case: operator clears state.json -> daemon loads with
    // last_sha = None for every flow. First poll observes the
    // already-existing SHA — must NOT trigger, because deleting
    // state was the operator's reset signal.
    let outcome = compare_sha(None, sha(0xab));
    assert!(
        !outcome.trigger,
        "post-state-clear first poll must NOT trigger",
    );
}

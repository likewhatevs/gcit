// StateUpdate apply ordering (Last-Writer-Wins).
// Order-preserving LWW.
//
// LWW semantics: when multiple updates target the same field on the same
// flow, the LAST one applied wins. The order is the recv_many order from
// the mpsc channel — which is the SEND order from the producers (mpsc is
// FIFO per-receiver).
//
// Pure-logic tests of the apply function. No filesystem, no async, no
// channel — just StateUpdate -> State::apply.
//
// In-module unit tests at src/state/apply.rs::tests cover the granular
// per-variant rules; the integration tests here drive the same pipeline
// through the public `gcit::state::*` API and pin cross-variant
// interleaving the unit tests don't combine.

use chrono::{DateTime, Utc};
use rstest::rstest;

use gcit::state::{State, StateUpdate};

fn t(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(secs, 0).unwrap()
}

fn sha(byte: u8) -> gix_hash::ObjectId {
    let hex = format!("{byte:02x}").repeat(20);
    gix_hash::ObjectId::from_hex(hex.as_bytes()).unwrap()
}

fn poll(flow: &str, byte: u8, secs: i64) -> StateUpdate {
    StateUpdate::PollObservation {
        flow: flow.to_string(),
        last_sha: sha(byte),
        last_poll_at: t(secs),
        last_dispatched_at: None,
        cooldown_until: None,
    }
}

fn run_started(flow: &str, run_id: u64, secs: i64) -> StateUpdate {
    StateUpdate::RunStarted {
        flow: flow.to_string(),
        run_id,
        started_at: t(secs),
    }
}

fn run_finished(flow: &str, run_id: u64, conclusion: &str, secs: i64) -> StateUpdate {
    StateUpdate::RunFinished {
        flow: flow.to_string(),
        run_id,
        conclusion: conclusion.to_string(),
        completed_at: t(secs),
    }
}

#[test]
fn poll_observation_overwrites_last_sha_for_flow() {
    let mut state = State::default();
    state.apply(poll("f", 0xaa, 1));
    state.apply(poll("f", 0xbb, 2));
    let entry = state.flows.get("f").unwrap();
    assert_eq!(
        entry.last_sha.as_deref(),
        Some(sha(0xbb).to_hex().to_string().as_str()),
        "LWW: bb (applied second) wins",
    );
}

#[test]
fn run_started_appends_to_active_runs() {
    let mut state = State::default();
    state.apply(run_started("f", 1, 1));
    state.apply(run_started("f", 2, 2));
    let entry = state.flows.get("f").unwrap();
    assert_eq!(entry.active_runs.len(), 2);
    assert_eq!(entry.active_runs[0].run_id, 1);
    assert_eq!(entry.active_runs[1].run_id, 2);
}

#[test]
fn run_finished_moves_run_from_active_to_notified() {
    let mut state = State::default();
    state.apply(run_started("f", 42, 1));
    state.apply(run_finished("f", 42, "success", 2));
    let entry = state.flows.get("f").unwrap();
    assert!(entry.active_runs.is_empty());
    assert_eq!(entry.notified_runs.len(), 1);
    assert_eq!(entry.notified_runs[0].run_id, 42);
    assert_eq!(
        entry.notified_runs[0].conclusion.as_deref(),
        Some("success"),
    );
}

#[test]
fn run_finished_for_unknown_run_does_not_panic() {
    // RunFinished referencing a run_id never RunStarted'd must not crash.
    let mut state = State::default();
    state.apply(run_finished("f", 999, "failure", 1));
    // The flow entry may or may not be created (apply allows it), but
    // there must be no active or notified entry for the unknown id.
    if let Some(entry) = state.flows.get("f") {
        assert!(
            !entry.active_runs.iter().any(|r| r.run_id == 999),
            "unknown run id must not be added to active_runs",
        );
        assert!(
            !entry.notified_runs.iter().any(|r| r.run_id == 999),
            "unknown run id must not be added to notified_runs",
        );
    }
}

#[rstest]
#[case::interleaved_polls_then_run(
    &[
        ("poll", "f", 0xaa, 1),
        ("poll", "f", 0xbb, 2),
        ("start", "f", 1, 3),
        ("poll", "f", 0xcc, 4),
        ("finish", "f", 1, 5),
    ],
    sha(0xcc),
    0,
    1,
)]
#[case::two_flows_independent(
    &[
        ("poll", "f", 0xa1, 1),
        ("poll", "g", 0xb1, 2),
        ("poll", "f", 0xa2, 3),
        ("poll", "g", 0xb2, 4),
    ],
    sha(0xa2),
    0,
    0,
)]
fn apply_sequence_produces_expected_final_state(
    #[case] sequence: &[(&str, &str, u8, i64)],
    #[case] expected_f_sha: gix_hash::ObjectId,
    #[case] expected_active: usize,
    #[case] expected_notified: usize,
) {
    let mut state = State::default();
    for (op, flow, arg, secs) in sequence {
        match *op {
            "poll" => state.apply(poll(flow, *arg, *secs)),
            "start" => state.apply(run_started(flow, *arg as u64, *secs)),
            "finish" => state.apply(run_finished(flow, *arg as u64, "success", *secs)),
            other => panic!("unknown op {other}"),
        }
    }
    let f = state.flows.get("f").expect("f exists");
    assert_eq!(
        f.last_sha.as_deref(),
        Some(expected_f_sha.to_hex().to_string().as_str()),
    );
    assert_eq!(f.active_runs.len(), expected_active);
    assert_eq!(f.notified_runs.len(), expected_notified);
}

#[test]
fn apply_is_pure_function_of_input() {
    // Same input sequence on two fresh State instances must yield
    // identical results. Catches an apply() with hidden global state.
    let sequence = [
        poll("f", 0x01, 1),
        run_started("f", 1, 2),
        poll("g", 0x10, 3),
        run_started("g", 2, 4),
        poll("f", 0x02, 5),
        run_finished("g", 2, "failure", 6),
    ];

    let mut a = State::default();
    let mut b = State::default();
    for upd in &sequence {
        a.apply(upd.clone());
        b.apply(upd.clone());
    }
    assert_eq!(a, b, "apply must be a pure function of (state, update)");

    // Apply on a third fresh instance to catch cross-call interference.
    let mut c = State::default();
    for upd in &sequence {
        c.apply(upd.clone());
    }
    assert_eq!(a, c);
}

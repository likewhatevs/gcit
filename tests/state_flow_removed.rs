// FlowRemoved cleanup.
// StateUpdate::FlowRemoved is one of four variants.
// Flow removal during in-flight dispatch: removing a flow from config
// (and reloading) while a dispatched workflow run is still in flight
// may result in the workflow run completing without notification.
// This is intentional. Removed flows are not tracked further; their
// run state is dropped from in-memory tracking on reload, and the
// next state-writer flush emits a FlowRemoved event.
//
// FlowRemoved is the cleanup signal. After it applies, the flow's entry
// MUST be gone from State::flows. Subsequent updates referencing the same
// flow name are dropped (or treated as a NEW flow if the operator re-adds
// the same name later).
//
// The integration tests drive State::apply via the public API
// (gcit::state::*), exercising the same path the writer thread uses.
// Persistence-round-trip tests use atomic_write_json + load_or_init.

use chrono::{DateTime, Utc};
use tempfile::TempDir;

use gcit::state::{FlowState, RunState, State, StateUpdate};
use gcit::util::atomic_write_json;

fn t(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(secs, 0).unwrap()
}

fn sha(byte: u8) -> gix_hash::ObjectId {
    let hex = format!("{byte:02x}").repeat(20);
    gix_hash::ObjectId::from_hex(hex.as_bytes()).unwrap()
}

#[test]
fn flow_removed_drops_flow_entry() {
    let mut state = State::default();
    state.apply(StateUpdate::PollObservation {
        flow: "f".to_string(),
        last_sha: sha(0xaa),
        last_poll_at: t(1),
    });
    state.apply(StateUpdate::RunStarted {
        flow: "f".to_string(),
        run_id: 1,
        started_at: t(2),
    });
    assert!(state.flows.contains_key("f"));

    state.apply(StateUpdate::FlowRemoved {
        flow: "f".to_string(),
    });
    assert!(!state.flows.contains_key("f"));
    assert!(state.flows.is_empty());
}

#[test]
fn flow_removed_for_unknown_flow_does_not_panic() {
    let mut state = State::default();
    // No prior observations for this flow. apply must not panic.
    state.apply(StateUpdate::FlowRemoved {
        flow: "never-observed".to_string(),
    });
    assert!(state.flows.is_empty());
}

#[test]
fn flow_removed_persists_across_save_load() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");

    let mut state = State::default();
    state.apply(StateUpdate::PollObservation {
        flow: "f".to_string(),
        last_sha: sha(0xab),
        last_poll_at: t(1),
    });
    state.apply(StateUpdate::FlowRemoved {
        flow: "f".to_string(),
    });

    atomic_write_json(&path, &state, 0o600).unwrap();
    let loaded = gcit::state::load_or_init(&path).unwrap();
    assert!(
        !loaded.flows.contains_key("f"),
        "removed flow must NOT round-trip through state.json",
    );
    assert!(loaded.flows.is_empty());
}

#[test]
fn re_added_flow_starts_with_empty_state_not_resurrected() {
    // 1. apply PollObservation { flow: "f", last_sha: A, ... }
    // 2. apply FlowRemoved     { flow: "f" }
    // 3. apply PollObservation { flow: "f", last_sha: B, ... }  // operator re-added
    // -> state.flows["f"].last_sha == B
    // -> state.flows["f"].active_runs == empty (no resurrection)
    let mut state = State::default();
    state.apply(StateUpdate::PollObservation {
        flow: "f".to_string(),
        last_sha: sha(0x01),
        last_poll_at: t(1),
    });
    state.apply(StateUpdate::RunStarted {
        flow: "f".to_string(),
        run_id: 99,
        started_at: t(2),
    });
    state.apply(StateUpdate::FlowRemoved {
        flow: "f".to_string(),
    });
    state.apply(StateUpdate::PollObservation {
        flow: "f".to_string(),
        last_sha: sha(0x02),
        last_poll_at: t(3),
    });

    let entry = state.flows.get("f").expect("re-added flow exists");
    assert_eq!(
        entry.last_sha.as_deref(),
        Some(sha(0x02).to_hex().to_string().as_str()),
    );
    assert!(
        entry.active_runs.is_empty(),
        "active_runs must NOT resurrect after FlowRemoved; got {:?}",
        entry.active_runs,
    );
    assert!(entry.notified_runs.is_empty());
}

#[test]
fn flow_removed_does_not_affect_other_flows() {
    let mut state = State::default();
    state.apply(StateUpdate::PollObservation {
        flow: "f".to_string(),
        last_sha: sha(0xa1),
        last_poll_at: t(1),
    });
    state.apply(StateUpdate::PollObservation {
        flow: "g".to_string(),
        last_sha: sha(0xb2),
        last_poll_at: t(2),
    });
    state.apply(StateUpdate::FlowRemoved {
        flow: "f".to_string(),
    });

    assert!(!state.flows.contains_key("f"));
    let g = state.flows.get("g").expect("g must remain");
    assert_eq!(
        g.last_sha.as_deref(),
        Some(sha(0xb2).to_hex().to_string().as_str()),
    );
}

#[test]
fn flow_removed_with_active_and_notified_runs_clears_both() {
    // Stage every shape of FlowState data: last_sha, active_runs (one
    // started but not finished), notified_runs (started + finished).
    // FlowRemoved must drop all of them — no dangling references.
    let mut state = State::default();
    state.flows.insert(
        "f".to_string(),
        FlowState {
            last_sha: Some("aa".repeat(20)),
            last_poll_at: Some(t(1)),
            active_runs: vec![RunState {
                run_id: 1,
                started_at: t(2),
                conclusion: None,
                completed_at: None,
            }],
            notified_runs: vec![RunState {
                run_id: 2,
                started_at: t(0),
                conclusion: Some("success".to_string()),
                completed_at: Some(t(1)),
            }],
        },
    );

    state.apply(StateUpdate::FlowRemoved {
        flow: "f".to_string(),
    });
    assert!(state.flows.is_empty());
}

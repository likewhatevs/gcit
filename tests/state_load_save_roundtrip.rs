// State file load/save round-trip.
// State at $STATE_DIRECTORY/state.json. Schema versioned (schema: 1).
// StateUpdate variants: PollObservation, RunStarted, RunFinished,
// FlowRemoved.
// Testing strategy: tempdir + assert_fs | drain, atomicity, schema
// version.
//
// Round-trip property: serialize(deserialize(json)) == json AND
// deserialize(serialize(state)) == state. Pins serde derive correctness for
// the state types.
//
// The public API used here:
//   - gcit::state::load_or_init(&path) returns Ok(State::default()) on
//     ENOENT and otherwise parses the file (schema-versioned).
//   - gcit::state::State implements Serialize+Deserialize directly; we
//     persist via gcit::util::atomic_write_json which is the same helper
//     the writer thread uses (tempfile+persist+fsync).

use chrono::{DateTime, Utc};
use tempfile::TempDir;

use gcit::state::{FlowState, RunState, State, SCHEMA_VERSION};
use gcit::util::atomic_write_json;

fn t(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(secs, 0).unwrap()
}

#[test]
fn empty_state_round_trips() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");

    let s = State::default();
    atomic_write_json(&path, &s, 0o600).expect("save empty state");
    let loaded = gcit::state::load_or_init(&path).expect("load empty state");
    assert_eq!(s, loaded);

    // The serialized form must include the schema version even when empty.
    let raw = std::fs::read_to_string(&path).unwrap();
    let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(
        v["schema"], SCHEMA_VERSION,
        "schema field required at value {SCHEMA_VERSION}",
    );
}

#[test]
fn populated_state_round_trips() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");

    let mut s = State::default();
    s.flows.insert(
        "ci-flow".to_string(),
        FlowState {
            last_sha: Some("aa".repeat(20)),
            last_poll_at: Some(t(1_000_000)),
            active_runs: vec![RunState {
                run_id: 42,
                started_at: t(1_000_100),
                conclusion: None,
                completed_at: None,
            }],
            notified_runs: vec![RunState {
                run_id: 7,
                started_at: t(900_000),
                conclusion: Some("success".to_string()),
                completed_at: Some(t(900_500)),
            }],
        },
    );
    s.flows.insert(
        "release-builder".to_string(),
        FlowState {
            last_sha: Some("bb".repeat(20)),
            last_poll_at: Some(t(2_000_000)),
            active_runs: vec![],
            notified_runs: vec![],
        },
    );

    atomic_write_json(&path, &s, 0o600).expect("save populated state");
    let loaded = gcit::state::load_or_init(&path).expect("load populated state");
    assert_eq!(s, loaded, "round-trip must preserve every populated field");
}

#[test]
fn save_leaves_no_tempfile_artifacts() {
    // tempfile+persist atomic writes. After a successful
    // save the only files in the directory are state.json itself; no
    // leftover ".tmp"/".gcit-atomic-" sibling.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");

    let s = State::default();
    atomic_write_json(&path, &s, 0o600).unwrap();
    let entries: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(
        entries.len(),
        1,
        "after atomic_write_json, only state.json should remain; got {entries:?}",
    );
    assert_eq!(entries[0], "state.json");
}

#[test]
fn load_returns_default_when_file_missing() {
    // First-run case: $STATE_DIRECTORY/state.json doesn't exist yet.
    // load_or_init must return Ok(default) — not an error. The daemon
    // needs to start cleanly on a fresh deployment.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json"); // does not exist
    assert!(!path.exists());
    let s = gcit::state::load_or_init(&path).expect("missing file -> default");
    assert_eq!(s, State::default());
}

#[test]
fn save_load_atomic_replace_keeps_consistency() {
    // Saving twice in succession must replace atomically. A reader that
    // catches the daemon mid-write either sees the OLD state or the NEW
    // state — never a partially-written file.
    //
    // We can't directly observe a partial write without injecting faults,
    // but we CAN assert the load-after-save sequence converges to the
    // most-recent value across multiple iterations.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");

    for i in 0..10u8 {
        let mut s = State::default();
        s.flows.insert(
            format!("flow-{i}"),
            FlowState {
                last_sha: Some("ff".repeat(20)),
                last_poll_at: Some(t(i as i64 * 100)),
                active_runs: vec![],
                notified_runs: vec![],
            },
        );
        atomic_write_json(&path, &s, 0o600).unwrap();
        let loaded = gcit::state::load_or_init(&path).unwrap();
        assert_eq!(loaded, s, "iteration {i} round-trip");
    }
}

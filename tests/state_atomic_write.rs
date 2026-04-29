// State atomic write via tempfile + persist.
// tempfile+persist atomic writes.
//
// The atomic-write pattern: write to a sibling temp file, fsync, then
// rename(2) over the target. POSIX rename is atomic on the same filesystem.
// Crash-mid-write recovery: if the process dies between tempfile creation
// and persist, the target file is still the prior valid version (or
// missing on first run). The temp file is cleanup-on-drop via tempfile::
// NamedTempFile.
//
// The pub helper gcit::util::atomic_write_json drives this for the
// state writer (and the install manifest); these tests exercise the
// behaviour through that helper because the writer thread itself uses
// it on the persist path (src/state/writer.rs run/run_mirrored).

use chrono::{DateTime, Utc};
use tempfile::TempDir;

use gcit::state::{FlowState, State};
use gcit::util::atomic_write_json;

fn t(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(secs, 0).unwrap()
}

fn marker_state(marker: &str) -> State {
    let mut s = State::default();
    s.flows.insert(
        marker.to_string(),
        FlowState {
            last_sha: Some("aa".repeat(20)),
            last_poll_at: Some(t(0)),
            active_runs: vec![],
            notified_runs: vec![],
        },
    );
    s
}

#[test]
fn save_replaces_atomically_via_rename() {
    // 1. Save state version A.
    // 2. Save state version B.
    // 3. Read back; assert content is B (no torn write, no leftover tempfile).
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");

    let a = marker_state("version-a");
    atomic_write_json(&path, &a, 0o600).unwrap();
    let b = marker_state("version-b");
    atomic_write_json(&path, &b, 0o600).unwrap();

    let loaded = gcit::state::load_or_init(&path).unwrap();
    assert_eq!(loaded, b);
    assert!(!loaded.flows.contains_key("version-a"));
    assert!(loaded.flows.contains_key("version-b"));
}

#[test]
fn crash_mid_write_leaves_prior_state_intact() {
    // Simulate crash by writing a partially-formed JSON file alongside
    // state.json (NOT through atomic_write_json). The prior state.json
    // must remain loadable; the stray file must NOT be picked up.
    //
    // The atomic-write contract is: the target (state.json) is renamed
    // atomically over the prior content. A partial/incomplete tempfile
    // sibling is invisible to load_or_init because it has a different
    // filename.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");

    let prior = marker_state("survivor");
    atomic_write_json(&path, &prior, 0o600).unwrap();

    // Simulate a stray tempfile from a prior crash. Use the tempfile
    // crate's own prefix (".gcit-atomic-") so the test exercises the
    // exact pattern atomic_write would have used pre-crash.
    let stray = dir.path().join(".gcit-atomic-stray-partial");
    std::fs::write(&stray, b"{\"schema\": 1, \"fl").unwrap();
    assert!(stray.exists());

    let loaded = gcit::state::load_or_init(&path).expect("prior state survives stray tempfile");
    assert_eq!(loaded, prior);
}

#[test]
fn truncated_state_file_fails_load_loudly() {
    // If state.json itself is truncated, load_or_init must return Err with
    // an actionable message rather than silently dropping back to default.
    // Schema versioned (schema: 1). Refuses unknown versions — the
    // same defense applies to malformed JSON.
    //
    // Recommend (a) loud failure: silent state loss is catastrophic for
    // a daemon that tracks notification dedup state. The error message
    // must guide the operator to back up and remove the corrupt file.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");

    std::fs::write(&path, "{\"schema\": 1, \"fl").unwrap(); // truncated
    let err = gcit::state::load_or_init(&path).expect_err("truncated state must fail");
    let msg = format!("{err}");
    assert!(msg.contains("parse"), "error must mention parse: {msg}");
    assert!(
        msg.contains("back up") || msg.contains("remove"),
        "error must guide operator to back up + remove: {msg}",
    );
}

#[test]
fn save_round_trip_after_each_write_is_consistent() {
    // Smoke check that save() round-trips through load_or_init without
    // observable drift. Mutation target: a writer that omits the fsync
    // before rename leaves a window where the rename succeeds but the
    // data hasn't reached disk; the OS may surface this as zero bytes
    // on subsequent open. We can't simulate power loss in a test, but
    // the round-trip property is the operator-visible contract.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");

    for i in 0..5u8 {
        let s = marker_state(&format!("rt-{i}"));
        atomic_write_json(&path, &s, 0o600).unwrap();
        let loaded = gcit::state::load_or_init(&path).unwrap();
        assert_eq!(loaded, s, "iteration {i}");
    }
}

#[test]
fn save_does_not_leave_tempfile_artifacts_in_directory() {
    // tempfile::Builder::tempfile_in writes to the SAME directory as the
    // target so the rename is on one filesystem (atomic). After persist
    // the only entry in the parent dir for this test is state.json.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");

    let s = marker_state("clean");
    atomic_write_json(&path, &s, 0o600).unwrap();

    let entries: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(
        entries.len(),
        1,
        "atomic_write must clean up sibling tempfiles; got {entries:?}",
    );
    assert_eq!(entries[0], "state.json");
}

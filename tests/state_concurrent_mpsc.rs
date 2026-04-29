// Concurrent state updates via mpsc.
// State-writer thread (mpsc 256, batch 64).
// Order-preserving LWW.
//
// Multiple flow tasks send StateUpdates concurrently. The writer thread
// is the single consumer; mpsc gives FIFO per-receiver. The tests pin:
//
//   - Many concurrent senders MUST not lose updates (channel capacity 256
//     means 257 concurrent send().await calls block one until drained;
//     test verifies no silent drops).
//   - LWW ordering on per-flow fields holds when each flow has one sender.
//   - Cross-flow updates apply independently regardless of interleaving.

use chrono::{DateTime, Utc};
use tempfile::TempDir;
use tokio::runtime::Builder;

use gcit::state::{spawn, State, StateUpdate};

fn t(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(secs, 0).unwrap()
}

fn sha(byte: u8) -> gix_hash::ObjectId {
    let hex = format!("{byte:02x}").repeat(20);
    gix_hash::ObjectId::from_hex(hex.as_bytes()).unwrap()
}

#[test]
fn n_senders_m_updates_all_apply() {
    // 8 senders, each sending 100 updates to its own distinct flow
    // (total 800). Channel capacity 256 forces back-pressure. The
    // writer drains; final state contains 8 flows, each at its
    // sender's last SHA.
    //
    // Per-flow ordering is deterministic because each flow has exactly
    // one sender. Cross-flow ordering is irrelevant: we only assert
    // each sender's LAST update wins for ITS flow.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");

    let rt = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel::<StateUpdate>(256);
    let writer = spawn(State::default(), path.clone(), rx);

    rt.block_on(async {
        let mut handles = Vec::new();
        for sender in 0u8..8 {
            let tx = tx.clone();
            handles.push(tokio::spawn(async move {
                for i in 0u8..100 {
                    tx.send(StateUpdate::PollObservation {
                        flow: format!("flow-{sender}"),
                        last_sha: sha(i),
                        last_poll_at: t(i as i64),
                    })
                    .await
                    .unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    });
    drop(tx);
    writer.join().expect("writer joined");

    let raw = std::fs::read_to_string(&path).unwrap();
    let loaded: State = serde_json::from_str(&raw).unwrap();
    assert_eq!(loaded.flows.len(), 8, "8 distinct flows must be present");
    for sender in 0u8..8 {
        let flow = format!("flow-{sender}");
        let entry = loaded
            .flows
            .get(&flow)
            .unwrap_or_else(|| panic!("flow {flow} missing"));
        // Each sender's last write was sha=99 (i = 0..100 inclusive
        // means last i=99). Per-flow LWW pins this regardless of
        // tokio's cross-task scheduling.
        assert_eq!(
            entry.last_sha.as_deref(),
            Some(sha(99).to_hex().to_string().as_str()),
            "{flow} last_sha must be sender's final write",
        );
    }
}

#[test]
fn cross_flow_updates_do_not_pollute_each_other() {
    // Two senders, two flows. Sender A writes a sequence of
    // PollObservations on flow "a"; sender B does the same on flow
    // "b". Final state must have A's last SHA on flow "a" and B's on
    // flow "b" — never crossed.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");

    let rt = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel::<StateUpdate>(256);
    let writer = spawn(State::default(), path.clone(), rx);

    rt.block_on(async {
        let tx_a = tx.clone();
        let h_a = tokio::spawn(async move {
            for i in 0u8..50 {
                tx_a.send(StateUpdate::PollObservation {
                    flow: "a".to_string(),
                    last_sha: sha(0xa0 + i),
                    last_poll_at: t(i as i64),
                })
                .await
                .unwrap();
            }
        });
        let tx_b = tx.clone();
        let h_b = tokio::spawn(async move {
            for i in 0u8..50 {
                tx_b.send(StateUpdate::PollObservation {
                    flow: "b".to_string(),
                    last_sha: sha(0xb0 + i),
                    last_poll_at: t(i as i64),
                })
                .await
                .unwrap();
            }
        });
        h_a.await.unwrap();
        h_b.await.unwrap();
    });
    drop(tx);
    writer.join().unwrap();

    let raw = std::fs::read_to_string(&path).unwrap();
    let loaded: State = serde_json::from_str(&raw).unwrap();
    let a = loaded.flows.get("a").expect("flow a");
    let b = loaded.flows.get("b").expect("flow b");
    // a's last sha is 0xa0+49 = 0xd1; b's is 0xb0+49 = 0xe1
    assert_eq!(
        a.last_sha.as_deref(),
        Some(sha(0xa0 + 49).to_hex().to_string().as_str()),
        "flow a last_sha must come from sender A",
    );
    assert_eq!(
        b.last_sha.as_deref(),
        Some(sha(0xb0 + 49).to_hex().to_string().as_str()),
        "flow b last_sha must come from sender B",
    );
}

#[test]
fn single_sender_per_flow_lww_is_deterministic() {
    // Send a, b, c on the same flow from one sender. mpsc preserves
    // FIFO per-Sender, so apply order is a, b, c and LWW yields c.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");

    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel::<StateUpdate>(256);
    let writer = spawn(State::default(), path.clone(), rx);

    rt.block_on(async {
        for byte in [0xaa, 0xbb, 0xcc] {
            tx.send(StateUpdate::PollObservation {
                flow: "f".to_string(),
                last_sha: sha(byte),
                last_poll_at: t(byte as i64),
            })
            .await
            .unwrap();
        }
    });
    drop(tx);
    writer.join().unwrap();

    let raw = std::fs::read_to_string(&path).unwrap();
    let loaded: State = serde_json::from_str(&raw).unwrap();
    let f = loaded.flows.get("f").unwrap();
    assert_eq!(
        f.last_sha.as_deref(),
        Some(sha(0xcc).to_hex().to_string().as_str()),
        "single-sender FIFO -> last write wins (cc)",
    );
}

#[test]
fn back_pressure_does_not_silently_drop_updates() {
    // Tiny capacity (4) forces frequent send().await blocking. Send
    // 100 updates; the writer drains; all 100 land. Mutation target:
    // a writer that uses try_send and ignores Full would lose updates
    // here; this test counts the applied effects.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");

    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel::<StateUpdate>(4);
    let writer = spawn(State::default(), path.clone(), rx);

    rt.block_on(async {
        for i in 0u8..100 {
            tx.send(StateUpdate::PollObservation {
                flow: format!("bp-{i:03}"),
                last_sha: sha(i),
                last_poll_at: t(i as i64),
            })
            .await
            .unwrap();
        }
    });
    drop(tx);
    writer.join().unwrap();

    let raw = std::fs::read_to_string(&path).unwrap();
    let loaded: State = serde_json::from_str(&raw).unwrap();
    assert_eq!(loaded.flows.len(), 100);
}

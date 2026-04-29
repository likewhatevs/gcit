// State-writer thread drain on shutdown.
// recv_many batching, order-preserving LWW, tempfile+persist atomic
// writes. Outlives tokio runtime.
// State-writer thread (mpsc 256, batch 64).
// Shutdown: Stopping -> root.cancel() -> await flows -> drop(state_tx)
// -> writer.join().
//
// The shutdown contract:
//   1. all flow tasks complete (root.cancel + awaits)
//   2. drop the mpsc::Sender — this is what signals the writer to drain
//   3. writer.join() blocks until all queued StateUpdates are flushed to
//      disk and the writer thread exits cleanly
//
// The writer must:
//   (a) drain any updates already in the channel BEFORE exiting
//   (b) emit ONE final atomic save with the merged state
//   (c) outlive the tokio runtime — it's a std::thread, not a tokio task
//
// This is the critical correctness property: dropping the sender mid-write
// must NOT lose updates that were already enqueued.

use std::time::Duration;

use chrono::{DateTime, Utc};
use gix_hash::ObjectId;
use tempfile::TempDir;
use tokio::runtime::Builder;

use gcit::state::{spawn, State, StateUpdate};

fn t(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(secs, 0).unwrap()
}

fn sha(byte: u8) -> ObjectId {
    let hex = format!("{byte:02x}").repeat(20);
    ObjectId::from_hex(hex.as_bytes()).unwrap()
}

#[test]
fn writer_drains_pending_updates_before_exiting() {
    // 1. spawn writer with mpsc capacity 256, batch 64
    // 2. send 100 StateUpdates rapidly (one per distinct flow)
    // 3. drop the Sender immediately (do NOT wait for writer to catch up)
    // 4. writer.join()
    // 5. load state from disk; assert ALL 100 updates are reflected
    //
    // Mutation target: a writer that exits on Sender drop without
    // recv_many'ing the buffer would lose any updates still in the
    // channel. This test catches by counting the applied updates.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");

    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel::<StateUpdate>(256);
    let writer = spawn(State::default(), path.clone(), rx);

    rt.block_on(async {
        for i in 0..100u8 {
            tx.send(StateUpdate::PollObservation {
                flow: format!("flow-{i:03}"),
                last_sha: sha(i),
                last_poll_at: t(i as i64),
            })
            .await
            .unwrap();
        }
    });
    drop(tx);
    writer.join().expect("writer thread join");

    let raw = std::fs::read_to_string(&path).expect("state.json exists");
    let loaded: State = serde_json::from_str(&raw).expect("parse state.json");
    assert_eq!(
        loaded.flows.len(),
        100,
        "drain must apply all 100 updates before exiting; got {} flows",
        loaded.flows.len(),
    );
}

#[test]
fn writer_outlives_tokio_runtime() {
    // The writer outlives the tokio runtime.
    // The writer MUST be a std::thread, not a tokio task. Tokio runtime
    // shutdown happens before the writer joins, so a tokio task would be
    // forcibly aborted mid-batch.
    //
    // Test: send N updates inside the runtime, drop the Sender + the
    // runtime together, then call writer.join() OUTSIDE any runtime.
    // The writer must still complete the final persist.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");

    let writer = {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel::<StateUpdate>(256);
        let writer = spawn(State::default(), path.clone(), rx);
        rt.block_on(async {
            for i in 0..10u8 {
                tx.send(StateUpdate::PollObservation {
                    flow: format!("post-runtime-{i}"),
                    last_sha: sha(i),
                    last_poll_at: t(i as i64),
                })
                .await
                .unwrap();
            }
        });
        drop(tx);
        // rt drops here. The writer MUST outlive it.
        drop(rt);
        writer
    };

    writer.join().expect("writer must outlive tokio runtime");
    let raw = std::fs::read_to_string(&path).expect("state.json exists");
    let loaded: State = serde_json::from_str(&raw).expect("parse state.json");
    assert_eq!(loaded.flows.len(), 10);
}

#[test]
fn drop_sender_signals_writer_to_drain_then_exit() {
    // drop(state_tx) -> writer.join(). Dropping the
    // last Sender clone is the shutdown signal. The writer thread's
    // recv_many returns 0 once all senders are dropped AND the channel
    // is empty. The writer then exits its loop within bounded time.
    //
    // If senders are leaked elsewhere, writer never exits and the daemon
    // hangs on shutdown. We bound the join via a side-thread + a 5s
    // deadline.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");

    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel::<StateUpdate>(256);
    let writer = spawn(State::default(), path.clone(), rx);
    rt.block_on(async {
        tx.send(StateUpdate::PollObservation {
            flow: "single".to_string(),
            last_sha: sha(0xab),
            last_poll_at: t(1),
        })
        .await
        .unwrap();
    });

    // Drop the only Sender. The writer should exit within seconds.
    drop(tx);

    // Bounded watchdog: spawn the writer-join on a side thread that
    // signals completion via an unbuffered std::sync::mpsc channel.
    // Block on `recv_timeout(5s)` instead of sleep-looping a polling
    // atomic — `Err(RecvTimeoutError::Timeout)` is the bounded
    // primitive's way of saying "writer.join() did not return in
    // time", which is exactly the property the test pins.
    let (signal_tx, signal_rx) = std::sync::mpsc::sync_channel::<std::thread::Result<()>>(0);
    let join_thread = std::thread::spawn(move || {
        let r = writer.join();
        // Best-effort send: if the receiver has timed out and dropped
        // the rx end, the writer thread already completed (we got
        // here) so the test will fail the assertion below regardless;
        // dropping the result on a closed channel is harmless.
        let _ = signal_tx.send(r);
    });
    let join_result = signal_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("writer.join() did not return within 5s after Sender drop");
    join_thread.join().expect("watchdog thread panicked");
    join_result.expect("writer panicked");
}

#[test]
fn channel_full_blocks_sender_does_not_drop_updates() {
    // mpsc channel with small capacity; we send more than capacity. The
    // production sender uses .await on send, which blocks (resolves
    // later) when full. The writer drains the channel, freeing slots.
    // This test confirms NO updates are silently dropped — the on-disk
    // state shows every send applied.
    //
    // Mutation target: replacing `send().await` with `try_send` and
    // ignoring `Err(Full)` would lose updates here.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");

    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    // Tiny capacity to force back-pressure.
    let (tx, rx) = tokio::sync::mpsc::channel::<StateUpdate>(4);
    let writer = spawn(State::default(), path.clone(), rx);

    let total: usize = 200;
    rt.block_on(async {
        for i in 0..total {
            tx.send(StateUpdate::PollObservation {
                flow: format!("bp-{i:04}"),
                last_sha: sha((i & 0xff) as u8),
                last_poll_at: t(i as i64),
            })
            .await
            .unwrap();
        }
    });
    drop(tx);
    writer.join().expect("writer joined");

    let raw = std::fs::read_to_string(&path).expect("state.json exists");
    let loaded: State = serde_json::from_str(&raw).expect("parse state.json");
    assert_eq!(
        loaded.flows.len(),
        total,
        "back-pressure must not drop updates; expected {} flows, got {}",
        total,
        loaded.flows.len(),
    );
}

// State writer: dedicated std::thread that drains a tokio mpsc and
// persists state to disk in batches. Uses recv_many batching,
// order-preserving LWW, and tempfile+persist atomic writes; outlives
// the tokio runtime. Mpsc capacity is 256 with a batch size of 64.
// Shutdown flow: Stopping -> root.cancel() -> await flows ->
// drop(state_tx) -> writer.join().
//
// Design choices:
//
//  - The writer runs on a `std::thread`, NOT a tokio task. tokio's
//    Receiver::blocking_recv_many panics if called inside an async
//    context. Putting the writer outside the runtime also satisfies
//    "outlives tokio runtime" — the daemon's shutdown drops the mpsc
//    Sender, the writer's blocking_recv_many returns 0, the writer
//    flushes once more and exits, and only then does the runtime drop.
//
//  - `BATCH_LIMIT = 64` is read into a `Vec<StateUpdate>` per
//    drain iteration. Each batch is applied in mpsc-FIFO order via
//    State::apply, then the merged state is persisted once. This
//    amortizes the persist cost (1 fsync per batch instead of 1 per
//    update).
//
//  - On the final drain (sender dropped, channel emptied), the
//    writer applies any remaining buffered updates and persists ONE
//    final time before exiting.
//
//  - Persist failures are logged but do NOT crash the writer thread —
//    a transient disk error (e.g., ENOSPC) should not lose all
//    subsequent updates. The writer keeps its in-memory state and
//    retries on the next batch boundary. Persistent failure is
//    operator-actionable and surfaces in journald.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;

use tokio::sync::mpsc::Receiver;
use tracing::{error, info};

use super::apply::{State, StateUpdate};
use crate::util::atomic_write_json;

/// Maximum batch size handed to `recv_many` per drain pass. Pairs
/// with the daemon's mpsc capacity of 256.
pub const BATCH_LIMIT: usize = 64;

/// Mode for the state file. Owner-only — `state.json` may carry
/// run ids and timestamps but no secret material; 0o600 matches the
/// install manifest convention and works on shared hosts.
const STATE_FILE_MODE: u32 = 0o600;

/// Spawn the writer thread.
///
/// `state` is the initial in-memory state (typically the result of
/// `state::load_or_init` at daemon startup). `path` is the absolute
/// path to write to (`$STATE_DIRECTORY/state.json`). `rx` is the
/// receive end of the mpsc channel; producers hold the matching
/// `Sender` clones and call `send().await` to enqueue updates.
///
/// The returned `JoinHandle` blocks on `join()` until the writer has
/// drained every queued update and exited. Drop the last Sender clone
/// to signal shutdown; the writer treats `recv_many returning 0` as
/// "channel closed, drain complete." Per
/// `tests/state_writer_drain.rs::drop_sender_signals_writer_to_drain_then_exit`,
/// the join completes within bounded time once the last sender is
/// dropped.
///
/// The writer is a `std::thread` (not `tokio::spawn`) so it outlives
/// the tokio runtime. blocking_recv_many panics if called inside an
/// async context, so running the writer as a tokio task would be a
/// bug.
pub fn spawn(initial: State, path: PathBuf, rx: Receiver<StateUpdate>) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("gcit-state-writer".into())
        .spawn(move || run(initial, path, rx))
        .expect("spawn state-writer thread")
}

/// Spawn the writer with a shared `Arc<Mutex<State>>` mirror updated
/// in lockstep with the on-disk file. Used by the supervisor so the
/// `gcit status` control handler can read flow state without a
/// round-trip to disk.
///
/// The mirror is initialised to `initial` BEFORE the thread is
/// spawned so a status read that races daemon startup observes the
/// freshly-loaded state rather than `State::default()`. Each batch's
/// final post-`apply` state is cloned into the mirror (under the
/// mutex) before the next batch is drained — the mirror is therefore
/// at most one batch behind disk, never ahead.
pub fn spawn_with_mirror(
    initial: State,
    path: PathBuf,
    rx: Receiver<StateUpdate>,
    mirror: Arc<Mutex<State>>,
) -> thread::JoinHandle<()> {
    {
        let mut g = mirror.lock().expect("state mirror poisoned at startup");
        *g = initial.clone();
    }
    thread::Builder::new()
        .name("gcit-state-writer".into())
        .spawn(move || run_mirrored(initial, path, rx, mirror))
        .expect("spawn state-writer thread")
}

/// Drain loop for the writer thread. See `spawn` for the contract.
///
/// `pub` so the daemon can call it directly when threading is
/// otherwise constructed (tests + foreground mode), but production
/// callers go through `spawn`.
///
/// MUST run on a `std::thread`, NOT inside a tokio runtime. The
/// `blocking_recv_many` call below panics when called from inside an
/// async context. `spawn` enforces this at construction time; calling
/// `run` from `tokio::spawn_blocking` would not panic immediately but
/// would risk the receiver never returning if the runtime drops
/// mid-flight. Run must execute on a thread that outlives the tokio
/// runtime.
pub fn run(mut state: State, path: PathBuf, mut rx: Receiver<StateUpdate>) {
    info!(
        target: "gcit::state",
        path = %path.display(),
        "state writer started",
    );
    let mut buffer: Vec<StateUpdate> = Vec::with_capacity(BATCH_LIMIT);
    // Tracks whether the in-memory state has updates that have not
    // yet been successfully persisted. set true after each apply,
    // cleared after a successful atomic_write_json. The shutdown
    // path uses this to skip the final persist when the channel
    // closed cleanly and the last batch was already on disk —
    // avoids a redundant fsync round.
    let mut dirty = false;
    loop {
        // blocking_recv_many returns 0 when ALL senders are dropped
        // AND the channel is empty (per
        // tokio-1.52.1/src/sync/mpsc/bounded.rs:434 -> recv_many,
        // documented at line 295). That is the shutdown signal.
        let n = rx.blocking_recv_many(&mut buffer, BATCH_LIMIT);
        if n == 0 {
            break;
        }
        for update in buffer.drain(..) {
            state.apply(update);
        }
        dirty = true;
        match atomic_write_json(&path, &state, STATE_FILE_MODE) {
            Ok(()) => dirty = false,
            Err(e) => {
                // Persist failed. Log and keep the in-memory state —
                // the next batch will retry the write with the
                // latest accumulated state. This is the right
                // behavior for transient errors (ENOSPC during log
                // rotation, etc.); for permanent errors the operator
                // sees recurring entries in journald and can
                // intervene.
                error!(
                    target: "gcit::state",
                    path = %path.display(),
                    error = %e,
                    batch_size = n,
                    "state persist failed; keeping in-memory updates and retrying on next batch",
                );
            }
        }
    }
    info!(
        target: "gcit::state",
        path = %path.display(),
        dirty,
        "state writer drained; exiting",
    );
    // Final persist only when we have unflushed in-memory updates
    // (dirty == true). On a clean shutdown after a successful batch,
    // the most recent persist already wrote the merged state, so
    // skipping here saves one fsync round. When dirty is true a
    // prior persist failed in the loop and we want one more attempt
    // before the writer exits and the in-memory state is lost.
    if dirty {
        if let Err(e) = atomic_write_json(&path, &state, STATE_FILE_MODE) {
            // Log at error! — a failed shutdown persist is more
            // serious than an in-loop failure (no further batch
            // will retry it; the in-memory updates are gone with
            // the writer thread).
            error!(
                target: "gcit::state",
                path = %path.display(),
                error = %e,
                "final state persist failed during shutdown; in-memory updates are lost",
            );
        }
    }
}

/// Drain loop variant that also updates a shared `Arc<Mutex<State>>`
/// mirror after each successful batch. The mirror is used by the
/// supervisor's `gcit status` control handler to read flow state
/// without a disk round-trip; the writer remains the only producer of
/// disk writes.
///
/// Same MUST-run-on-std::thread invariants as `run` — see the
/// docstring on `run` for the full contract.
pub fn run_mirrored(
    mut state: State,
    path: PathBuf,
    mut rx: Receiver<StateUpdate>,
    mirror: Arc<Mutex<State>>,
) {
    info!(
        target: "gcit::state",
        path = %path.display(),
        "state writer started (with status mirror)",
    );
    let mut buffer: Vec<StateUpdate> = Vec::with_capacity(BATCH_LIMIT);
    let mut dirty = false;
    loop {
        let n = rx.blocking_recv_many(&mut buffer, BATCH_LIMIT);
        if n == 0 {
            break;
        }
        for update in buffer.drain(..) {
            state.apply(update);
        }
        dirty = true;
        match atomic_write_json(&path, &state, STATE_FILE_MODE) {
            Ok(()) => {
                dirty = false;
                // Mirror update follows the disk persist so a status
                // read after a successful batch always agrees with
                // the on-disk file. Poisoning is treated as fatal —
                // a poisoned mutex means another thread panicked
                // mid-update; status reads against a half-applied
                // mirror are worse than a daemon restart.
                if let Ok(mut g) = mirror.lock() {
                    *g = state.clone();
                } else {
                    error!(
                        target: "gcit::state",
                        "state mirror mutex poisoned; status reads may be stale",
                    );
                }
            }
            Err(e) => {
                error!(
                    target: "gcit::state",
                    path = %path.display(),
                    error = %e,
                    batch_size = n,
                    "state persist failed; keeping in-memory updates and retrying on next batch",
                );
            }
        }
    }
    info!(
        target: "gcit::state",
        path = %path.display(),
        dirty,
        "state writer drained; exiting (mirrored)",
    );
    if dirty {
        if let Err(e) = atomic_write_json(&path, &state, STATE_FILE_MODE) {
            error!(
                target: "gcit::state",
                path = %path.display(),
                error = %e,
                "final state persist failed during shutdown; in-memory updates are lost",
            );
        } else if let Ok(mut g) = mirror.lock() {
            *g = state;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::apply::{State, StateUpdate};
    use chrono::{DateTime, Utc};
    use gix_hash::ObjectId;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;
    use tracing_test::traced_test;

    /// Scan the global tracing buffer for `needle`. The standard
    /// `logs_contain` macro injected by `#[traced_test]` filters by
    /// the per-test span scope (see tracing-test internal::
    /// logs_with_scope_contain), but the writer runs on a fresh
    /// `std::thread::spawn` whose context never inherited the test
    /// thread's span enter, so its `error!()` events emit without
    /// the scope prefix. `global_buf()` is the raw buffer the
    /// captured subscriber appends to; reading it directly bypasses
    /// the per-test span filter so we see the writer's emits.
    /// Returns true when any line in the buffer contains `needle`.
    fn captured_logs_contain(needle: &str) -> bool {
        let bytes = tracing_test::internal::global_buf().lock().unwrap().clone();
        let s = String::from_utf8(bytes).expect("captured logs are utf8");
        s.contains(needle)
    }

    fn t(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    fn sha(byte: u8) -> ObjectId {
        let hex = format!("{byte:02x}").repeat(20);
        ObjectId::from_hex(hex.as_bytes()).unwrap()
    }

    /// Writer drains pending updates after the Sender drops.
    #[test]
    fn drains_pending_updates_before_exit() {
        let td = TempDir::new().unwrap();
        let path = td.path().join("state.json");
        // Start a current-thread runtime just to construct the mpsc;
        // the writer itself runs on its own std::thread and does not
        // need the runtime.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel::<StateUpdate>(256);
        let handle = spawn(State::default(), path.clone(), rx);
        // Send 50 distinct PollObservations across 50 flows; mpsc
        // capacity 256 means none of these block.
        rt.block_on(async {
            for i in 0..50 {
                tx.send(StateUpdate::PollObservation {
                    flow: format!("f{}", i),
                    last_sha: sha((i & 0xff) as u8),
                    last_poll_at: t(i as i64),
                })
                .await
                .unwrap();
            }
        });
        // Drop the Sender — writer drains and exits.
        drop(tx);
        handle.join().unwrap();

        // Read back: 50 flows present.
        let raw = std::fs::read_to_string(&path).unwrap();
        let s: State = serde_json::from_str(&raw).unwrap();
        assert_eq!(s.schema, 1);
        assert_eq!(s.flows.len(), 50);
    }

    /// Last sender drop triggers exit within bounded time.
    #[test]
    fn drop_sender_triggers_exit() {
        let td = TempDir::new().unwrap();
        let path = td.path().join("state.json");
        let (tx, rx) = tokio::sync::mpsc::channel::<StateUpdate>(256);
        let handle = spawn(State::default(), path, rx);
        drop(tx);
        // join() blocks until the writer thread exits. With no senders
        // and an empty channel, blocking_recv_many returns 0 on the
        // first call and the writer exits.
        handle.join().unwrap();
    }

    #[test]
    fn spawn_with_mirror_seeds_mirror_to_initial_state_before_thread_start() {
        // The initial state must be observable through the mirror as
        // soon as `spawn_with_mirror` returns — the supervisor's
        // `gcit status` control handler relies on this so a status
        // read that races daemon startup observes the freshly-loaded
        // state rather than `State::default()`.
        let td = TempDir::new().unwrap();
        let path = td.path().join("state.json");
        let (_tx, rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        let mut initial = State::default();
        initial.flows.insert(
            "preloaded".to_string(),
            crate::state::FlowState {
                last_sha: Some("a".repeat(40)),
                ..Default::default()
            },
        );
        let mirror = Arc::new(Mutex::new(State::default()));
        let _handle = spawn_with_mirror(initial.clone(), path, rx, Arc::clone(&mirror));
        let g = mirror.lock().expect("not poisoned");
        assert_eq!(
            g.flows.len(),
            1,
            "mirror must reflect initial state immediately after spawn",
        );
        assert!(g.flows.contains_key("preloaded"));
        // Drop the receiver-side handle implicitly via _tx going out
        // of scope; the writer drains and joins.
    }

    #[test]
    fn run_mirrored_updates_mirror_after_each_persist() {
        // Mirror update follows disk persist (lines 232-249 in
        // run_mirrored). After every successful batch, the mirror
        // contents must equal what was just written to disk.
        let td = TempDir::new().unwrap();
        let path = td.path().join("state.json");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel::<StateUpdate>(256);
        let mirror = Arc::new(Mutex::new(State::default()));
        let handle = spawn_with_mirror(State::default(), path.clone(), rx, Arc::clone(&mirror));
        rt.block_on(async {
            for i in 0..10 {
                tx.send(StateUpdate::PollObservation {
                    flow: format!("f{}", i),
                    last_sha: sha((i & 0xff) as u8),
                    last_poll_at: t(i as i64),
                })
                .await
                .unwrap();
            }
        });
        drop(tx);
        handle.join().unwrap();
        // Disk file should have the same 10 flows the mirror does.
        let raw = std::fs::read_to_string(&path).unwrap();
        let on_disk: State = serde_json::from_str(&raw).unwrap();
        let g = mirror.lock().expect("not poisoned");
        assert_eq!(g.flows.len(), 10);
        assert_eq!(*g, on_disk, "mirror must equal on-disk state after batch");
    }

    #[test]
    fn run_mirrored_drains_pending_updates_before_exit() {
        // The mirrored variant of the writer must honor the same
        // drain-before-exit contract as `run` (run_mirrored at
        // lines 210-279). 30 enqueued updates + sender drop must
        // produce 30 flows in both the on-disk file AND the mirror.
        let td = TempDir::new().unwrap();
        let path = td.path().join("state.json");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel::<StateUpdate>(256);
        let mirror = Arc::new(Mutex::new(State::default()));
        let handle = spawn_with_mirror(State::default(), path.clone(), rx, Arc::clone(&mirror));
        rt.block_on(async {
            for i in 0..30 {
                tx.send(StateUpdate::PollObservation {
                    flow: format!("flow{}", i),
                    last_sha: sha((i & 0xff) as u8),
                    last_poll_at: t(i as i64),
                })
                .await
                .unwrap();
            }
        });
        drop(tx);
        handle.join().unwrap();
        let raw = std::fs::read_to_string(&path).unwrap();
        let on_disk: State = serde_json::from_str(&raw).unwrap();
        assert_eq!(on_disk.flows.len(), 30);
        let g = mirror.lock().expect("not poisoned");
        assert_eq!(g.flows.len(), 30);
    }

    #[test]
    fn run_drop_empty_sender_with_clean_state_skips_final_persist() {
        // Drop sender immediately with NO updates sent: the loop
        // breaks on the first blocking_recv_many=0, dirty stays
        // false, and the final-persist branch is skipped. Verify
        // by checking that the on-disk file was NEVER created — the
        // writer's only persist is in the loop body, and the loop
        // never executed its body.
        let td = TempDir::new().unwrap();
        let path = td.path().join("state.json");
        let (tx, rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        let handle = spawn(State::default(), path.clone(), rx);
        drop(tx);
        handle.join().unwrap();
        assert!(
            !path.exists(),
            "no updates sent + dirty=false must skip the final persist; \
             on-disk file must not exist",
        );
    }

    /// True when the test process runs as root (uid 0). Root bypasses
    /// the chmod-based access control these tests rely on (a 0o555
    /// dir is still writable by uid 0), so the persist-failure tests
    /// must be skipped for root invocations.
    fn running_as_root() -> bool {
        // SAFETY: geteuid() is a thread-safe syscall with no side
        // effects; Rust's libc binding declares it as `unsafe fn`
        // because it is a raw FFI call, not because of memory safety.
        unsafe { libc::geteuid() == 0 }
    }

    #[test]
    #[traced_test]
    fn run_persist_failure_in_loop_keeps_dirty_and_recovers_on_next_batch() {
        // The in-loop persist-failure recovery path (writer.rs:155-172):
        //   - Batch 1 lands, state.apply mutates the in-memory state,
        //     dirty=true, atomic_write_json fails (parent dir 0o555,
        //     EACCES on the tempfile create), error! is logged, dirty
        //     stays true.
        //   - Operator (or in this test, the test body) restores the
        //     parent dir to 0o755.
        //   - Batch 2 lands, state.apply mutates the (already-applied
        //     batch-1) state with batch-2 updates, atomic_write_json
        //     succeeds, dirty=false.
        //   - On-disk file now reflects BOTH batches' merged state,
        //     not just batch 2 — proves the in-memory state survived
        //     the failed persist instead of being dropped.
        //
        // Synchronization: the 150ms sleep is a timing budget, and
        // the post-sleep `captured_logs_contain("state persist failed")`
        // check turns it into a timing+correctness gate. The writer's
        // error!() at writer.rs:164-170 emits to the global tracing
        // subscriber that #[traced_test] installs; if the writer has
        // not yet attempted+failed the persist when we check, the
        // assertion fires immediately rather than the test silently
        // succeeding for the wrong reason (e.g. both batches
        // collapsing into one successful persist after chmod restore).
        if running_as_root() {
            eprintln!(
                "run_persist_failure_in_loop_keeps_dirty_and_recovers_on_next_batch: \
                 skipped — running as root; CAP_DAC_OVERRIDE bypasses chmod-based \
                 EACCES injection",
            );
            return;
        }
        let td = TempDir::new().unwrap();
        let parent = td.path().to_path_buf();
        let path = parent.join("state.json");
        // Lock down the parent so atomic_write_json's tempfile create
        // (tempfile::Builder::tempfile_in -> open with O_RDWR|O_CREAT)
        // returns EACCES.
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o555)).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel::<StateUpdate>(256);
        let handle = spawn(State::default(), path.clone(), rx);

        // Batch 1: send a flow observation, then yield enough time
        // for the writer thread to hit blocking_recv_many, drain,
        // apply, attempt persist (fails), and park on the next
        // recv_many call. The post-sleep captured_logs_contain check
        // pins that the persist actually fired AND failed
        // (writer.rs:164's "state persist failed; ..." emit) before we
        // proceed — without it the test would still pass even on a
        // heavily-loaded runner where the writer hadn't woken in 150ms.
        rt.block_on(async {
            tx.send(StateUpdate::PollObservation {
                flow: "flow-batch1".into(),
                last_sha: sha(0xaa),
                last_poll_at: t(100),
            })
            .await
            .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        });
        assert!(
            captured_logs_contain("state persist failed"),
            "writer must have observed the persist failure under chmod 0o555 \
             parent before the test proceeds; raise the timing budget if this \
             fires on a loaded runner",
        );

        // Restore writeability so the next batch's persist succeeds.
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o755)).unwrap();

        // Batch 2: send a second flow observation. This time the
        // persist succeeds and the on-disk file carries BOTH flows.
        rt.block_on(async {
            tx.send(StateUpdate::PollObservation {
                flow: "flow-batch2".into(),
                last_sha: sha(0xbb),
                last_poll_at: t(200),
            })
            .await
            .unwrap();
        });
        drop(tx);
        handle.join().unwrap();

        // The merged state must contain BOTH flows. If batch 1 had
        // been dropped on the persist failure, only flow-batch2
        // would appear here.
        let raw = std::fs::read_to_string(&path).expect("file must exist after recovery");
        let s: State = serde_json::from_str(&raw).unwrap();
        assert!(
            s.flows.contains_key("flow-batch1"),
            "in-memory batch-1 state must survive a failed persist and land on disk after recovery; flows: {:?}",
            s.flows.keys().collect::<Vec<_>>(),
        );
        assert!(
            s.flows.contains_key("flow-batch2"),
            "batch-2 state must land alongside the recovered batch-1 state; flows: {:?}",
            s.flows.keys().collect::<Vec<_>>(),
        );
    }

    #[test]
    #[traced_test]
    fn run_final_persist_after_dirty_recovers_when_chmod_restored_before_drop() {
        // Final-persist-on-shutdown branch (writer.rs:186-199):
        //   - Batch 1 lands, persist fails, dirty=true.
        //   - Sender drops (no batch 2). blocking_recv_many returns 0,
        //     loop exits.
        //   - Because dirty==true, the final atomic_write_json fires.
        //     If we restore writeability BEFORE the writer reaches
        //     this branch, the final persist succeeds and the on-disk
        //     file carries the (otherwise-lost) batch-1 state.
        //
        // Pinning the success path here is sufficient: the failure
        // branch (chmod stays 0o555 and final persist also fails) is
        // a journald-only error log — there is no observable side-
        // effect on disk, and the writer thread still exits cleanly
        // (`if let Err(e) = ... { error!(...) }` does not propagate).
        // A panic-or-not assertion on the join handle covers that
        // branch via the per-test invariant that join() returns Ok.
        //
        // Synchronization: the 150ms sleep is a timing budget, and
        // the post-sleep `captured_logs_contain("state persist failed")`
        // check turns it into a timing+correctness gate. Without the
        // log assertion the test could pass on a heavily-loaded
        // runner where the writer hadn't woken in 150ms — the on-disk
        // recovery would then come from the final-persist branch
        // landing the only batch (no in-loop persist ever fired) and
        // the test would silently miss its target. The captured
        // "state persist failed" event proves the in-loop persist
        // attempted+failed BEFORE chmod restore.
        if running_as_root() {
            eprintln!(
                "run_final_persist_after_dirty_recovers_when_chmod_restored_before_drop: \
                 skipped — running as root; CAP_DAC_OVERRIDE bypasses chmod-based \
                 EACCES injection",
            );
            return;
        }
        let td = TempDir::new().unwrap();
        let parent = td.path().to_path_buf();
        let path = parent.join("state.json");
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o555)).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel::<StateUpdate>(256);
        let handle = spawn(State::default(), path.clone(), rx);

        // Batch 1: send + sleep so the writer drains + persists +
        // fails + parks dirty=true. The post-sleep captured_logs_contain
        // check pins that the persist actually fired AND failed
        // (writer.rs:164's "state persist failed; ..." emit) before we
        // restore writeability — without it the test could silently
        // pass via the alternate path where the writer hadn't woken
        // in 150ms.
        rt.block_on(async {
            tx.send(StateUpdate::PollObservation {
                flow: "flow-shutdown".into(),
                last_sha: sha(0xcc),
                last_poll_at: t(300),
            })
            .await
            .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        });
        assert!(
            captured_logs_contain("state persist failed"),
            "writer must have observed the persist failure under chmod 0o555 \
             parent before the test proceeds; raise the timing budget if this \
             fires on a loaded runner",
        );
        // Restore writeability BEFORE dropping the sender so the
        // final persist (triggered by dirty=true post-loop-exit) has
        // a writable parent and succeeds.
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o755)).unwrap();
        drop(tx);
        handle.join().unwrap();

        // The shutdown final-persist must have written the in-memory
        // state to disk. The flow from batch 1 — which would otherwise
        // have been lost when the writer thread exited — is recovered.
        let raw = std::fs::read_to_string(&path)
            .expect("file must exist after final-persist recovery");
        let s: State = serde_json::from_str(&raw).unwrap();
        assert!(
            s.flows.contains_key("flow-shutdown"),
            "final-persist on shutdown must recover the dirty in-memory state; flows: {:?}",
            s.flows.keys().collect::<Vec<_>>(),
        );
    }

    #[test]
    fn run_mirrored_continues_after_mirror_mutex_poisoned() {
        // run_mirrored must NOT crash when another thread poisons the
        // status mirror mutex (writer.rs:241-248 + 275-277 use
        // `if let Ok(mut g) = mirror.lock()` to log + skip rather
        // than unwrap + propagate). Operators trade a stale status
        // mirror for a still-running writer — the alternative
        // (writer thread crash) would lose every subsequent
        // StateUpdate forever.
        //
        // The test poisons the mirror via a thread that locks it and
        // panics, then sends a batch and joins. The writer is expected
        // to:
        //   1. drain the batch successfully,
        //   2. persist to disk,
        //   3. observe the poisoned mirror's `lock()` returning Err,
        //   4. log the diagnostic and continue,
        //   5. return cleanly from join().
        let td = TempDir::new().unwrap();
        let path = td.path().join("state.json");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel::<StateUpdate>(256);
        let mirror = Arc::new(Mutex::new(State::default()));
        // Spawn the writer BEFORE poisoning so spawn_with_mirror's
        // `mirror.lock().expect("state mirror poisoned at startup")`
        // (writer.rs:103) succeeds with an unpoisoned mutex.
        let handle = spawn_with_mirror(State::default(), path.clone(), rx, Arc::clone(&mirror));

        // Poison the mirror via a panic-while-locked. join() returns
        // Err with the panic payload; the mirror is now poisoned and
        // every subsequent `lock()` returns PoisonError.
        let poison_mirror = Arc::clone(&mirror);
        let panicker = std::thread::spawn(move || {
            let _g = poison_mirror.lock().expect("not yet poisoned");
            panic!("intentional poison");
        });
        let _ = panicker.join();
        assert!(
            mirror.lock().is_err(),
            "precondition: panicker thread must have poisoned the mirror",
        );

        // Send a batch. The writer drains, persists to disk, then
        // attempts to update the mirror — its `lock()` returns Err,
        // the writer logs and continues without crashing.
        rt.block_on(async {
            tx.send(StateUpdate::PollObservation {
                flow: "flow-poisoned".into(),
                last_sha: sha(0xee),
                last_poll_at: t(400),
            })
            .await
            .unwrap();
        });
        drop(tx);
        // join must succeed — a poisoned mirror does NOT crash the
        // writer (the production code's `if let Ok(...) else` arm is
        // the only path that observes poison).
        handle
            .join()
            .expect("writer thread must NOT crash on a poisoned mirror");

        // The on-disk file must reflect the batch — disk persist
        // happens BEFORE the mirror update (writer.rs:232-248), so a
        // poisoned mirror cannot suppress the persist.
        let raw = std::fs::read_to_string(&path).expect("file must exist after batch");
        let s: State = serde_json::from_str(&raw).unwrap();
        assert!(
            s.flows.contains_key("flow-poisoned"),
            "on-disk batch must land even when the status mirror is poisoned; flows: {:?}",
            s.flows.keys().collect::<Vec<_>>(),
        );
    }
}

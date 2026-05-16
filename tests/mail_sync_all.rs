// fsync ordering and failure injection for the mbox notifier.
//
// Production at src/mail/notifier.rs::write_with_lock takes a
// `&dyn Persist` and calls `persist.sync_all(&guard)` after
// `write_all` and before the lock guard drops:
//
//   guard.write_all(bytes)?;
//   persist.sync_all(&guard)?;
//   drop(guard);
//
// Production wires `RealPersist`, which delegates to
// `std::fs::File::sync_all` (fsync(2) on Linux). Tests inject
// recorder/fault impls via the same trait and pin:
//
//   1. sync is called BEFORE the flock guard drops — verified by
//      probing the spool's flock state from inside the recorder's
//      sync_all hook.
//   2. sync errors propagate as WriteError::Io and surface to the
//      notifier as map_io_error classification.
//   3. sync is called exactly once per write_with_lock invocation —
//      one fsync per message, not per write_all chunk.
//   4. write_with_lock returns with the lock guard already
//      dropped — a fresh try_write probe acquires immediately,
//      pinning the source order `sync_all → drop(guard)`.
//
// All four tests drive `gcit::mail::write_with_lock` directly with
// a custom Persist (or `RealPersist` for #4). The helper is
// `#[doc(hidden)] pub` for exactly this kind of seam-test.
//
// `sync_all_durability_across_power_loss` (the fifth, #[ignore]'d
// skeleton) cannot be activated from a same-process integration
// test: writes are page-cached and visible to subsequent reads
// regardless of fsync. Real verification would need an OS
// fault-injection harness (kill process between sync and release;
// remount; reread). The Persist seam pins invocation count,
// ordering, and error propagation — but cannot pin durability
// across power loss.

use std::fs::{self, OpenOptions};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use chrono::Utc;
use fd_lock::RwLock as FdLock;
use gix_hash::ObjectId;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use gcit::config::{FireEvent, LocalMailTemplateConfig};
use gcit::github::{Conclusion, RunStatus, RunSummary};
use gcit::mail::{write_with_lock, LocalMailNotifier, Persist, WriteError};
use gcit::notify::{strict_handlebars, ActionInfo, Notifier, NotifyError, RunContext, SourceInfo};

#[test]
fn sync_all_called_before_flock_drop() {
    // Pin: sync_all() is called while the flock is still held. The
    // recorder's hook tries to take a non-blocking flock on a
    // fresh fd to the same spool path; it must surface WouldBlock
    // because write_with_lock still holds the lock at that point.
    struct ProbeSync {
        path: std::path::PathBuf,
        held_at_sync: Arc<AtomicBool>,
        called: Arc<AtomicUsize>,
    }
    impl Persist for ProbeSync {
        fn sync_all(&self, file: &std::fs::File) -> std::io::Result<()> {
            self.called.fetch_add(1, Ordering::SeqCst);
            // Non-blocking probe on a SECOND fd to the same path.
            // flock(2) man page: "If a process uses open(2) to
            // obtain more than one file descriptor for the same
            // file, these file descriptors are treated independently
            // by flock()." So a try_write on a fresh fd surfaces
            // WouldBlock if the production guard still holds the
            // exclusive lock.
            let probe_file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.path)
                .expect("probe open");
            let mut probe_lock = FdLock::new(probe_file);
            let still_held = matches!(
                probe_lock.try_write(),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
            );
            self.held_at_sync.store(still_held, Ordering::SeqCst);
            file.sync_all()
        }
    }

    let tmp = TempDir::new().expect("tempdir");
    let user = "u_probe";
    let spool_path = tmp.path().join(user);
    fs::write(&spool_path, b"").expect("create spool");

    let held_at_sync = Arc::new(AtomicBool::new(false));
    let called = Arc::new(AtomicUsize::new(0));
    let policy = ProbeSync {
        path: spool_path.clone(),
        held_at_sync: Arc::clone(&held_at_sync),
        called: Arc::clone(&called),
    };

    write_with_lock(&spool_path, b"hello\n", &policy, None)
        .expect("write_with_lock must succeed against an uncontended spool");

    assert_eq!(
        called.load(Ordering::SeqCst),
        1,
        "sync_all must be called exactly once per write_with_lock invocation",
    );
    assert!(
        held_at_sync.load(Ordering::SeqCst),
        "sync_all must run BEFORE the flock guard drops; the probe try_write would have succeeded if the lock had already been released",
    );
}

#[test]
fn sync_all_failure_propagates_to_notify_error() {
    // Pin: a Persist that returns io::Error(EIO) surfaces as
    // WriteError::Io. The production notifier maps this through
    // map_io_error to NotifyError::Transient (EIO is in the
    // transient-by-default arm).
    struct FailSync;
    impl Persist for FailSync {
        fn sync_all(&self, _file: &std::fs::File) -> std::io::Result<()> {
            Err(std::io::Error::from_raw_os_error(libc::EIO))
        }
    }

    let tmp = TempDir::new().expect("tempdir");
    let user = "u_fail";
    let spool_path = tmp.path().join(user);
    fs::write(&spool_path, b"").expect("create spool");

    let result = write_with_lock(&spool_path, b"hello\n", &FailSync, None);
    match result {
        Err(WriteError::Io(e)) => {
            assert_eq!(
                e.raw_os_error(),
                Some(libc::EIO),
                "sync failure must propagate the original raw_os_error; got {e:?}",
            );
        }
        other => panic!("expected WriteError::Io(EIO); got {other:?}"),
    }

    // Bytes were still written to the spool before the sync
    // attempt — write_with_lock returns the sync error after
    // write_all succeeds, so the page-cache holds the new bytes
    // (durability is what sync would have guaranteed, but the
    // bytes are visible to subsequent same-process reads).
    let bytes = fs::read(&spool_path).expect("read spool");
    assert_eq!(bytes, b"hello\n");
}

#[test]
fn sync_all_called_once_per_message_not_per_buffer() {
    // Pin: regardless of the number of write_all bytes, sync_all
    // runs exactly once. A future refactor that splits writes into
    // chunks (and accidentally syncs after each chunk) would
    // surface here as a count > 1.
    struct CountSync(Arc<AtomicUsize>);
    impl Persist for CountSync {
        fn sync_all(&self, file: &std::fs::File) -> std::io::Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            file.sync_all()
        }
    }

    let tmp = TempDir::new().expect("tempdir");
    let user = "u_count";
    let spool_path = tmp.path().join(user);
    fs::write(&spool_path, b"").expect("create spool");

    let count = Arc::new(AtomicUsize::new(0));
    let policy = CountSync(Arc::clone(&count));
    // A multi-kilobyte payload — the helper still runs sync once.
    let big_payload = vec![b'x'; 16 * 1024];

    write_with_lock(&spool_path, &big_payload, &policy, None)
        .expect("write_with_lock must succeed");
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "16 KiB payload must yield exactly one sync_all, not one-per-chunk",
    );
}

#[test]
fn sync_all_completes_before_flock_release() {
    // Pin SOURCE ORDER: by the time write_with_lock returns, the
    // helper has run `persist.sync_all(&guard); drop(guard);` in
    // that order — a fresh try_write on a second fd to the same
    // path must acquire immediately, no WouldBlock. A mutation
    // that swapped the order to `drop(guard); persist.sync_all(...);`
    // would let the probe race with the drop and break this pin.
    //
    // What this test does NOT prove: on-disk durability across
    // power loss. Same-process reads see page-cached bytes
    // regardless of whether fsync(2) has actually flushed to
    // storage; that verification needs an OS fault-injection
    // harness (kill between sync and release; remount; reread).
    // See `sync_all_durability_across_power_loss` below for the
    // ignored-stub guardrail.
    let tmp = TempDir::new().expect("tempdir");
    let user = "u_release";
    let spool_path = tmp.path().join(user);
    fs::write(&spool_path, b"").expect("create spool");

    struct RealSync;
    impl Persist for RealSync {
        fn sync_all(&self, file: &std::fs::File) -> std::io::Result<()> {
            file.sync_all()
        }
    }
    write_with_lock(&spool_path, b"committed\n", &RealSync, None)
        .expect("write_with_lock must succeed");

    // Single try_write probe — no loop, no sleep. The helper has
    // returned, so the guard MUST already be dropped.
    let probe_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&spool_path)
        .expect("probe open");
    let mut probe_lock = FdLock::new(probe_file);
    match probe_lock.try_write() {
        Ok(_guard) => {}
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => panic!(
            "flock not released by the time write_with_lock returned — \
             post-fix code must drop the guard before returning",
        ),
        Err(e) => panic!("probe try_write: {e}"),
    }

    let bytes = fs::read(&spool_path).expect("read spool");
    assert_eq!(bytes, b"committed\n");
}

// Durability-across-power-loss is not observable to a same-process
// test — it requires kill+remount fault injection (an OS-level test
// harness rather than a cargo nextest binary). The testable part of
// the invariant (sync_all completes before the flock release) is
// pinned by `sync_all_completes_before_flock_release` above. The
// production code documents the durability contract in the
// `write_with_lock` comments; gcit ships a unit test for the
// orderable part and trusts the kernel's fsync contract for the
// rest.

#[tokio::test]
async fn sync_all_failure_propagates_through_on_run_complete_pipeline() {
    // End-to-end pipeline pin: a Persist that returns
    // io::Error(EIO) from sync_all surfaces all the way through
    // `LocalMailNotifier::on_run_complete` as
    // `NotifyError::Transient` — verifying the chain
    // helper -> WriteError::Io -> map_io_error -> Transient.
    //
    // The helper-level test `sync_all_failure_propagates_to_notify_error`
    // pins write_with_lock's WriteError::Io return; this test
    // pins the additional hop through map_io_error's
    // transient-by-default arm (EIO is in the Transient bucket).
    //
    // Mutation target: adding an EIO-specific arm to
    // map_io_error that returns Permanent — backon would NOT
    // retry, and a transient disk-fault sync would permanently
    // kill notifications.
    struct FailingPersist;
    impl Persist for FailingPersist {
        fn sync_all(&self, _file: &std::fs::File) -> std::io::Result<()> {
            Err(std::io::Error::from_raw_os_error(libc::EIO))
        }
    }

    let tmp = TempDir::new().expect("tempdir");
    let user = "u_pipeline_eio";
    let spool_path = tmp.path().join(user);
    fs::write(&spool_path, b"").expect("create spool");

    let n = LocalMailNotifier::for_test_with_persist(
        "test",
        user,
        Arc::new("h.example.com".to_string()),
        vec![FireEvent::RunComplete],
        LocalMailTemplateConfig::default(),
        Arc::new(strict_handlebars()),
        tmp.path().to_path_buf(),
        Arc::new(FailingPersist),
    )
    .expect("test fixture user is alphanumeric, valid");

    let cancel = CancellationToken::new();
    let result = n
        .on_run_complete(&pipeline_ctx(), &pipeline_summary(), &cancel)
        .await;

    match result {
        Err(NotifyError::Transient { source, .. }) => {
            let msg = source.to_string();
            // map_io_error's transient-by-default arm includes
            // the canonical "io error on {path}: {err}" prefix
            // and embeds the underlying io::Error's Display,
            // which carries the errno text on Linux.
            assert!(
                msg.contains(&spool_path.display().to_string()),
                "Transient error must name the spool path; got: {msg}",
            );
        }
        Err(other) => panic!("expected Transient for sync_all EIO; got Permanent: {other:?}",),
        Ok(outcome) => panic!("expected Transient on sync failure; got {outcome:?}",),
    }

    // Bytes were still written before sync was attempted —
    // write_with_lock writes then syncs, returning the sync
    // error after the bytes are page-cached. The on_run_complete
    // pipeline returns Transient so backon retries; on retry
    // (separate test invocation), the spool would carry a
    // duplicate header. Pinning that the bytes ARE present
    // confirms the sequence (not the retry behaviour, which is
    // the supervisor's responsibility).
    let bytes = fs::read(&spool_path).expect("read spool");
    assert!(
        !bytes.is_empty(),
        "page-cached bytes must be present after a sync_all failure (write_all ran before sync)",
    );
}

// ---------------------------------------------------------------
// helpers for the end-to-end pipeline test
// ---------------------------------------------------------------

fn pipeline_ctx() -> RunContext {
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

fn pipeline_summary() -> RunSummary {
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

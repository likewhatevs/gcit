// fd-lock flock(LOCK_EX) on the spool file: concurrency invariants.
//
// The pipeline under test is `LocalMailNotifier::on_run_complete`,
// whose `write_with_lock` helper opens the spool file with
// O_NOFOLLOW + O_APPEND, takes an advisory exclusive flock via
// the blocking `fd_lock::RwLock::write()` syscall, writes the
// formatted message, syncs via the `Persist` seam, and drops the
// guard (releases the flock).
//
// The helper runs inside `tokio::task::spawn_blocking`. The
// notifier signals lock-acquired via a oneshot channel and wraps
// the receiver in `tokio::time::timeout(LOCK_WAIT_DEADLINE)`, so
// lock-wait beyond 5s surfaces as `NotifyError::Transient`. After
// the lock is acquired, write+sync runs unbounded. These tests
// stay well inside the 5s budget by holding test-side flocks for
// ~100ms.
//
// fd-lock is the same crate the production helper uses, so
// test-side `fd_lock::RwLock::write()` on a separate fd to the
// same spool file contends with the daemon's flock per Linux
// flock(2) semantics ("If a process uses open(2) ... to obtain
// more than one file descriptor for the same file, these file
// descriptors are treated independently by flock(). An attempt to
// lock the file using one of these file descriptors may be denied
// by a lock that the calling process has already placed via
// another file descriptor.").

use std::fs::{self, OpenOptions};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use fd_lock::RwLock as FdLock;
use gix_hash::ObjectId;
use handlebars::Handlebars;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use gcit::config::{FireEvent, LocalMailTemplateConfig};
use gcit::github::{Conclusion, JobResult, RunStatus, RunSummary};
use gcit::mail::LocalMailNotifier;
use gcit::notify::{
    strict_handlebars, ActionInfo, Notifier, NotifyOutcome, RunContext, SourceInfo,
};

fn handlebars() -> Arc<Handlebars<'static>> {
    Arc::new(strict_handlebars())
}

fn ctx() -> RunContext {
    RunContext {
        flow_name: "myflow".into(),
        flow_description: None,
        source: SourceInfo {
            url: "https://example.com/r.git".into(),
            ref_name: "refs/heads/main".into(),
            sha: ObjectId::null(gix_hash::Kind::Sha1),
            sha_short: "0000000".into(),
        },
        action: ActionInfo {
            repo: "owner/repo".into(),
            workflow: "ci.yml".into(),
            run_id: 1,
            run_url: "https://example.com".into(),
            dispatched_at: Utc::now(),
        },
        gcit_run_id: uuid::Uuid::nil(),
    }
}

fn summary() -> RunSummary {
    RunSummary {
        run_id: 1,
        run_url: "https://example.com".into(),
        run_number: 1,
        run_attempt: 1,
        status: RunStatus::Completed,
        conclusion: Some(Conclusion::Success),
        started_at: Some(Utc::now()),
        completed_at: Some(Utc::now()),
        jobs: vec![JobResult {
            job_id: 1,
            name: "build".into(),
            html_url: "https://example.com/job/1".into(),
            conclusion: Some(Conclusion::Success),
            started_at: Some(Utc::now()),
            completed_at: Some(Utc::now()),
            steps: Vec::new(),
            run_attempt: 1,
        }],
    }
}

/// Build a notifier with a custom subject template that embeds a
/// caller-supplied marker. Per-writer markers let
/// `concurrent_appends_serialize_via_flock` find each writer's
/// message in the resulting spool without depending on internal
/// timestamps or jobs counts.
fn notifier_with_marker(spool_dir: PathBuf, user: &str, marker: &str) -> LocalMailNotifier {
    LocalMailNotifier::for_test(
        format!("test-{marker}"),
        user,
        Arc::new("host1".to_string()),
        vec![FireEvent::RunComplete],
        LocalMailTemplateConfig {
            subject: Some(format!("[gcit] test-msg-{marker}")),
            body: None,
        },
        handlebars(),
        spool_dir,
    )
    .expect("test fixture user is alphanumeric, valid")
}

fn notifier(spool_dir: PathBuf, user: &str) -> LocalMailNotifier {
    LocalMailNotifier::for_test(
        "test",
        user,
        Arc::new("host1".to_string()),
        vec![FireEvent::RunComplete],
        LocalMailTemplateConfig::default(),
        handlebars(),
        spool_dir,
    )
    .expect("test fixture user is alphanumeric, valid")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_appends_serialize_via_flock() {
    // 8 writers spawn concurrently, each writes a uniquely-marked
    // message. flock serializes them so the resulting spool is a
    // valid mbox containing all 8 messages, none corrupted.
    //
    // Mutation target: dropping the flock from `write_with_lock`
    // would let two writes interleave at the byte level. The
    // marker-search assertion catches: every writer's marker
    // string appears exactly once, intact, in the final spool.
    let tmp = TempDir::new().expect("tempdir");
    let user = "u_concurrent";
    let spool_path = tmp.path().join(user);
    fs::write(&spool_path, b"").expect("create empty spool");

    let spool_dir: PathBuf = tmp.path().to_path_buf();
    let mut handles = Vec::with_capacity(8);
    for i in 0..8u32 {
        let dir = spool_dir.clone();
        let user_owned = user.to_string();
        handles.push(tokio::spawn(async move {
            let n = notifier_with_marker(dir, &user_owned, &i.to_string());
            n.on_run_complete(&ctx(), &summary(), &CancellationToken::new())
                .await
        }));
    }
    for (i, h) in handles.into_iter().enumerate() {
        let outcome = h.await.expect("task join").expect("notifier ok");
        assert!(
            matches!(outcome, NotifyOutcome::Sent { .. }),
            "writer {i} produced {outcome:?}",
        );
    }

    let bytes = fs::read(&spool_path).expect("read spool");
    let text = std::str::from_utf8(&bytes).expect("spool is utf8");
    for i in 0..8u32 {
        let marker = format!("test-msg-{i}");
        let count = text.matches(marker.as_str()).count();
        assert_eq!(
            count,
            1,
            "marker {marker:?} must appear exactly once in spool; \
             spool size {} bytes, full text:\n{}",
            bytes.len(),
            text,
        );
    }

    // Spool starts with `From ` per mbox separator semantics, and
    // each message contributes one such separator. Pin the count to
    // 8 to catch any byte-interleaving that fragmented a message
    // header.
    let from_lines = text.lines().filter(|l| l.starts_with("From ")).count();
    assert_eq!(
        from_lines, 8,
        "expected exactly 8 mbox `From ` separators; got {from_lines}",
    );
}

#[tokio::test]
async fn flock_holds_exclusive_for_full_write_duration() {
    // The test holds a test-side flock on the spool fd for ~100ms.
    // The notifier's `write_with_lock` blocks on its own flock call
    // for the entire holding window, then proceeds once the test
    // drops its guard. Elapsed time for the notifier call must be
    // >= the holding window (lock contention forced serialization)
    // — never near zero (which would mean the production code
    // bypassed the lock or dropped it before the write).
    //
    // Mutation target: a refactor that acquires the lock, opens
    // the file, drops the lock, then writes — the test's blocking
    // window would NOT influence the notifier's timing because the
    // lock would already be released by the time the notifier
    // got into its write call. Pinning elapsed >= ~100ms catches
    // that race window.
    let tmp = TempDir::new().expect("tempdir");
    let user = "u_duration";
    let spool_path = tmp.path().join(user);
    fs::write(&spool_path, b"").expect("create empty spool");

    let hold_for = Duration::from_millis(150);
    let spool_for_thread = spool_path.clone();
    let holder = std::thread::spawn(move || {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&spool_for_thread)
            .expect("open for hold");
        let mut lock = FdLock::new(file);
        let guard = lock.write().expect("acquire test-side flock");
        std::thread::sleep(hold_for);
        drop(guard);
    });

    // Give the holder thread a moment to take the lock before the
    // notifier fires. 30ms is enough on any reasonable scheduler;
    // it's well below the 150ms hold window so the notifier still
    // sees the lock held when it calls flock.
    tokio::time::sleep(Duration::from_millis(30)).await;

    let n = notifier(tmp.path().to_path_buf(), user);
    let started = Instant::now();
    let outcome = n
        .on_run_complete(&ctx(), &summary(), &CancellationToken::new())
        .await
        .expect("notifier should eventually succeed once test releases flock");
    let elapsed = started.elapsed();
    holder.join().expect("holder thread");

    assert!(
        matches!(outcome, NotifyOutcome::Sent { .. }),
        "expected Sent after holder drops; got {outcome:?}",
    );
    // Holder slept 150ms after the notifier started; subtract the
    // 30ms warmup before the notifier fired. A 90ms floor leaves
    // generous slack for scheduler jitter while still being clearly
    // distinguishable from the no-contention case (~7ms in the
    // mail_no_follow regular-file test).
    assert!(
        elapsed >= Duration::from_millis(90),
        "notifier elapsed must include the held-flock window; got {elapsed:?} (expected >= 90ms)",
    );
}

#[tokio::test]
async fn flock_acquire_blocks_until_holder_drops() {
    // The notifier's `fd_lock::RwLock::write()` call is unbounded-
    // blocking (acquire blocks on the kernel's flock until granted).
    // The 5s LOCK_WAIT_DEADLINE bound comes from a
    // `tokio::time::timeout` around the receiver of the helper's
    // lock-acquired oneshot — the lock is acquired promptly once
    // the holder drops, well inside that window.
    //
    // Test: hold the flock from a thread for ~100ms, fire the
    // notifier, observe Sent + elapsed >= ~100ms (proving the
    // notifier waited for the drop).
    let tmp = TempDir::new().expect("tempdir");
    let user = "u_blocks";
    let spool_path = tmp.path().join(user);
    fs::write(&spool_path, b"").expect("create empty spool");

    let hold_for = Duration::from_millis(120);
    let spool_for_thread = spool_path.clone();
    let holder = std::thread::spawn(move || {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&spool_for_thread)
            .expect("open holder");
        let mut lock = FdLock::new(file);
        let guard = lock.write().expect("acquire holder flock");
        std::thread::sleep(hold_for);
        drop(guard);
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let n = notifier(tmp.path().to_path_buf(), user);
    let started = Instant::now();
    let outcome = n
        .on_run_complete(&ctx(), &summary(), &CancellationToken::new())
        .await
        .expect("notifier eventually succeeds once holder drops");
    let elapsed = started.elapsed();
    holder.join().expect("holder thread");

    assert!(matches!(outcome, NotifyOutcome::Sent { .. }));
    // Holder slept 120ms; subtract the 20ms warmup. 70ms is well
    // above any single-shot uncontended append time (~7ms in
    // mail_no_follow tests).
    assert!(
        elapsed >= Duration::from_millis(70),
        "notifier must block until holder drops the flock; elapsed {elapsed:?} (expected >= 70ms)",
    );
    // And under 5s — production's LOCK_WAIT_DEADLINE — to confirm
    // the path was acquire-not-Transient-timeout.
    assert!(
        elapsed < Duration::from_secs(2),
        "notifier should not approach LOCK_WAIT_DEADLINE; elapsed {elapsed:?}",
    );
}

#[tokio::test]
async fn flock_blocks_at_filesystem_level_not_per_open() {
    // Linux flock(2) is keyed on the underlying open-file
    // description (the kernel tracks the lock list on the inode's
    // `i_flctx->flc_flock` per `struct file_lock_context`), NOT
    // per-file-descriptor or per-process. Two separate `open()`
    // calls for the same path produce distinct fds whose flock
    // calls contend — verified by holding flock on fd1 and
    // observing try_write on fd2 surface ErrorKind::WouldBlock.
    //
    // Mutation target: a refactor that bypassed fd-lock and used
    // POSIX advisory locks (fcntl F_SETLK) — those are keyed
    // per-process, and within-process locks don't conflict, so
    // the second fd's lock attempt would silently succeed,
    // defeating the cross-task serialization the daemon depends
    // on.
    let tmp = TempDir::new().expect("tempdir");
    let spool_path = tmp.path().join("u_inode");
    fs::write(&spool_path, b"").expect("create spool");

    // fd1: open + lock.
    let f1 = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&spool_path)
        .expect("open fd1");
    let mut lock1 = FdLock::new(f1);
    let _guard1 = lock1.write().expect("acquire fd1 flock");

    // fd2: open same path, attempt try_write. Same process, same
    // path, distinct fd — flock(2) MUST report contention.
    let f2 = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&spool_path)
        .expect("open fd2");
    let mut lock2 = FdLock::new(f2);
    match lock2.try_write() {
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            // Expected: fd2's flock contends with fd1's flock.
        }
        Ok(_guard) => {
            panic!(
                "fd2 try_write succeeded while fd1 holds flock; \
                 implementation does not provide cross-fd contention",
            );
        }
        Err(other) => panic!("unexpected error from fd2 try_write: {other}"),
    }

    // Sanity: drop fd1's guard, then fd2's try_write must succeed
    // immediately. This pins that the contention was caused by
    // fd1's lock (not some other system-wide condition).
    drop(_guard1);
    drop(lock1);
    let _guard2 = lock2.try_write().expect("fd2 acquires after fd1 releases");
}

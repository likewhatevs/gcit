// LOCK_WAIT_DEADLINE timing: append succeeds when lock acquired in
// time; surfaces Transient when contention exceeds the deadline.
//
// Production state pin: `pub const LOCK_WAIT_DEADLINE: Duration =
// Duration::from_secs(5)` in src/mail/notifier.rs.
// `LocalMailNotifier::on_run_complete` spawns a blocking task that
// runs `write_with_lock`; the helper signals lock-acquired via a
// oneshot channel the moment `lock.write()` returns. The notifier
// wraps ONLY the receiver in `tokio::time::timeout(LOCK_WAIT_DEADLINE)`
// so the deadline applies to the flock-wait phase only. On deadline
// expiry, the notifier returns `NotifyError::Transient` with the
// message "spool lock not acquired within 5s". After the lock is
// acquired, write_all and sync run unbounded so a slow fsync does
// not surface as a lock timeout.
//
// These tests use real time on a real-IO path (wiremock-style
// `tokio::time::pause` cannot interleave correctly with a real
// flock held on a real OS thread). Slow tests therefore wait
// real wall-clock time bounded by the 5s deadline; total binary
// runtime is dominated by the deadline-fires assertions.

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
use gcit::mail::{LocalMailNotifier, LOCK_WAIT_DEADLINE};
use gcit::notify::{
    strict_handlebars, ActionInfo, Notifier, NotifyError, NotifyOutcome, RunContext, SourceInfo,
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

/// Spawn an OS thread that opens `path`, takes an exclusive flock,
/// then sleeps until `release_rx` is signalled. Returns the join
/// handle and the sender so the caller controls when the holder
/// drops the lock.
fn spawn_flock_holder(path: PathBuf) -> (std::thread::JoinHandle<()>, std::sync::mpsc::Sender<()>) {
    let (tx, rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open holder fd");
        let mut lock = FdLock::new(file);
        let guard = lock.write().expect("acquire holder flock");
        // Wait for release; recv() returns Err once the sender drops,
        // which also releases the lock cleanly.
        let _ = rx.recv();
        drop(guard);
    });
    (handle, tx)
}

#[test]
fn deadline_constant_is_5_seconds() {
    // The integration crate sees the constant via `gcit::mail::LOCK_WAIT_DEADLINE`
    // (re-exported from the notifier module). Pin the literal value
    // — operator behavior contracts (the documented 5-second
    // deadline) assume this exact duration. Mutation target:
    // silently changing the constant to 4500ms or 10s would
    // diverge without breaking the in-tree unit test that pins
    // the same value (because that test compares against itself).
    assert_eq!(
        LOCK_WAIT_DEADLINE,
        Duration::from_secs(5),
        "operator-facing 5-second deadline must be exactly 5s",
    );
}

#[tokio::test]
async fn lock_acquired_before_deadline_succeeds() {
    // Hold the flock for 200ms — well inside the 5s deadline. The
    // notifier blocks on its flock call, the holder drops, the
    // notifier acquires, writes, and returns Sent.
    //
    // Mutation target: a deadline timer that fires too early
    // (e.g. 100ms instead of 5s) would surface Transient before
    // the holder releases.
    let tmp = TempDir::new().expect("tempdir");
    let user = "u_under";
    let spool_path = tmp.path().join(user);
    fs::write(&spool_path, b"").expect("create spool");

    let (holder, release_tx) = spawn_flock_holder(spool_path.clone());
    // Give the holder thread time to acquire its lock before the
    // notifier fires.
    tokio::time::sleep(Duration::from_millis(30)).await;

    let n = notifier(tmp.path().to_path_buf(), user);
    // Release after 200ms — the notifier's flock call will be
    // pending. 200ms is well under LOCK_WAIT_DEADLINE (5s).
    let release_clone = release_tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = release_clone.send(());
    });

    let started = Instant::now();
    let cancel = CancellationToken::new();
    let outcome = n
        .on_run_complete(&ctx(), &summary(), &cancel)
        .await
        .expect("acquire-before-deadline must succeed");
    let elapsed = started.elapsed();

    assert!(matches!(outcome, NotifyOutcome::Sent { .. }));
    assert!(
        elapsed < LOCK_WAIT_DEADLINE,
        "succeeded acquire must be well under LOCK_WAIT_DEADLINE; got {elapsed:?}",
    );
    drop(release_tx);
    holder.join().expect("holder thread");
}

#[tokio::test]
async fn lock_not_acquired_at_deadline_returns_transient() {
    // Hold the flock for the full 5s LOCK_WAIT_DEADLINE plus 1.5s
    // of slack so the production timeout
    // (`tokio::time::timeout(LOCK_WAIT_DEADLINE, phase_rx)`) is
    // the trigger. Notifier surfaces Transient with the "spool
    // lock not acquired within" message.
    //
    // Test wall-clock cost is the deadline itself (~5s). After the
    // notifier returns, the test releases the holder so the
    // dangling spawn_blocking thread can finish (a Transient error
    // does not cancel the OS thread; it merely drops the
    // JoinHandle, leaving the kernel to clean up after the holder
    // releases the flock).
    let tmp = TempDir::new().expect("tempdir");
    let user = "u_deadline";
    let spool_path = tmp.path().join(user);
    fs::write(&spool_path, b"").expect("create spool");

    let (holder, release_tx) = spawn_flock_holder(spool_path.clone());
    tokio::time::sleep(Duration::from_millis(30)).await;

    let n = notifier(tmp.path().to_path_buf(), user);
    let started = Instant::now();
    let cancel = CancellationToken::new();
    let result = n.on_run_complete(&ctx(), &summary(), &cancel).await;
    let elapsed = started.elapsed();

    // Release the holder so the spawn_blocking task can clean up
    // its thread before the test ends. The release happens AFTER
    // the assertion-relevant elapsed measurement.
    drop(release_tx);
    holder.join().expect("holder thread");

    match result {
        Err(NotifyError::Transient {
            source,
            retry_after,
        }) => {
            let msg = format!("{source}");
            assert!(
                msg.contains("not acquired") || msg.contains("lock"),
                "Transient message must explain the lock contention; got: {msg}",
            );
            // retry_after is None — backon's default schedule
            // applies on the next attempt. Pin so a future change
            // that adds a hint surfaces as a test failure.
            assert!(
                retry_after.is_none(),
                "lock-deadline Transient should not carry a retry_after hint; got {retry_after:?}",
            );
        }
        other => panic!("expected Transient on lock-deadline; got {other:?}"),
    }
    // Elapsed must be at least the deadline (the timeout fires
    // exactly at LOCK_WAIT_DEADLINE, give or take scheduler jitter).
    // Lower bound 4.5s to absorb jitter; upper bound 7s as a
    // sanity cap.
    assert!(
        elapsed >= Duration::from_millis(4500),
        "Transient must fire at ~LOCK_WAIT_DEADLINE; got {elapsed:?}",
    );
    assert!(
        elapsed < Duration::from_secs(7),
        "Transient must not exceed LOCK_WAIT_DEADLINE by more than slack; got {elapsed:?}",
    );
}

#[tokio::test]
async fn deadline_independent_per_concurrent_appender() {
    // Two notifier calls on the same spool path; both face the
    // same held flock. Each independently runs its own
    // `tokio::time::timeout(LOCK_WAIT_DEADLINE, phase_rx)` future,
    // so both fire at their own ~5s mark. They do NOT block on
    // each other (they block on the holder), and neither's
    // timeout budget is consumed by the other.
    //
    // To stay efficient: run both concurrently so the test
    // wall-clock is one LOCK_WAIT_DEADLINE, not two.
    let tmp = TempDir::new().expect("tempdir");
    let user_a = "u_indep_a";
    let user_b = "u_indep_b";
    let spool_a = tmp.path().join(user_a);
    let spool_b = tmp.path().join(user_b);
    fs::write(&spool_a, b"").expect("create spool a");
    fs::write(&spool_b, b"").expect("create spool b");

    let (holder_a, release_a) = spawn_flock_holder(spool_a.clone());
    let (holder_b, release_b) = spawn_flock_holder(spool_b.clone());
    tokio::time::sleep(Duration::from_millis(30)).await;

    let dir_a = tmp.path().to_path_buf();
    let dir_b = tmp.path().to_path_buf();
    let user_a_owned = user_a.to_string();
    let user_b_owned = user_b.to_string();
    let started = Instant::now();
    let cancel = CancellationToken::new();
    let cancel_a = cancel.clone();
    let cancel_b = cancel.clone();
    let (res_a, res_b) = tokio::join!(
        async move {
            let n = notifier(dir_a, &user_a_owned);
            n.on_run_complete(&ctx(), &summary(), &cancel_a).await
        },
        async move {
            let n = notifier(dir_b, &user_b_owned);
            n.on_run_complete(&ctx(), &summary(), &cancel_b).await
        }
    );
    let elapsed = started.elapsed();

    drop(release_a);
    drop(release_b);
    holder_a.join().expect("holder a");
    holder_b.join().expect("holder b");

    assert!(
        matches!(res_a, Err(NotifyError::Transient { .. })),
        "appender A must fire Transient at its own deadline; got {res_a:?}",
    );
    assert!(
        matches!(res_b, Err(NotifyError::Transient { .. })),
        "appender B must fire Transient at its own deadline; got {res_b:?}",
    );
    // Both should fire at ~LOCK_WAIT_DEADLINE (5s); the wall-clock
    // total stays close to a single deadline (concurrent), not
    // two-times. 7s is the upper bound — a regression that
    // serialized the two appender deadlines (e.g. via a shared
    // mutex) would push elapsed past 9s.
    assert!(
        elapsed < Duration::from_secs(7),
        "concurrent appender deadlines must not serialize; got {elapsed:?}",
    );
}

#[tokio::test]
async fn deadline_wakes_immediately_when_holder_drops_after_deadline() {
    // Holder holds the flock for 7s real time (past LOCK_WAIT_DEADLINE)
    // via a std::thread that schedules its own release. The
    // notifier's deadline fires at ~5s with Transient — the
    // notifier's timer is independent of the holder's drop. The
    // holder dropping at 7s is ignored by the (already-returned)
    // notifier.
    //
    // The release is scheduled inside the holder thread itself
    // (real-time std::thread::sleep), keeping the synchronization
    // off the tokio runtime. A single-threaded `#[tokio::test]`
    // runtime cannot drive a tokio::time::sleep + tokio::spawn
    // release while the test thread is later blocked on
    // `holder.join()` — that would deadlock the runtime.
    //
    // Mutation target: a notifier whose deadline timer is reset by
    // observing the holder's drop signal would never time out, or
    // would reset and time out late. Pin elapsed in
    // [LOCK_WAIT_DEADLINE - 0.5s, LOCK_WAIT_DEADLINE + 1.5s].
    let tmp = TempDir::new().expect("tempdir");
    let user = "u_after";
    let spool_path = tmp.path().join(user);
    fs::write(&spool_path, b"").expect("create spool");

    // Holder takes the flock and self-releases after 7s real time.
    // No mpsc channel — no risk of mixing release signals with
    // tokio task scheduling.
    let spool_for_thread = spool_path.clone();
    let holder = std::thread::spawn(move || {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&spool_for_thread)
            .expect("open holder fd");
        let mut lock = FdLock::new(file);
        let guard = lock.write().expect("acquire holder flock");
        std::thread::sleep(Duration::from_secs(7));
        drop(guard);
    });
    tokio::time::sleep(Duration::from_millis(30)).await;

    let n = notifier(tmp.path().to_path_buf(), user);
    let started = Instant::now();
    let cancel = CancellationToken::new();
    let result = n.on_run_complete(&ctx(), &summary(), &cancel).await;
    let elapsed = started.elapsed();

    assert!(
        matches!(result, Err(NotifyError::Transient { .. })),
        "deadline-fires must surface Transient regardless of holder's later drop; got {result:?}",
    );
    assert!(
        elapsed >= Duration::from_millis(4500),
        "notifier must wait the full LOCK_WAIT_DEADLINE; got {elapsed:?}",
    );
    assert!(
        elapsed < Duration::from_millis(6500),
        "notifier must NOT extend past LOCK_WAIT_DEADLINE awaiting the holder's drop; got {elapsed:?}",
    );

    // Defense-in-depth: pin that the holder hasn't yet released
    // its flock at notifier-return time. The holder's 7s self-
    // release is still ~1.5-2.5s away. Try a non-blocking lock
    // attempt on a fresh fd; it must surface WouldBlock to confirm
    // the holder is still active. This rules out a buggy holder
    // that exited early via panic.
    let probe_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&spool_path)
        .expect("probe fd");
    let mut probe_lock = FdLock::new(probe_file);
    match probe_lock.try_write() {
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
        other => panic!(
            "expected holder still holds flock at notifier-return; probe try_write returned {other:?}",
        ),
    }

    // Wait for holder thread to finish its 7s sleep + drop guard.
    // This blocks the runtime thread, but no in-flight tokio task
    // depends on the runtime by this point.
    holder.join().expect("holder thread");
}

#[tokio::test]
async fn deadline_does_not_count_lock_held_time() {
    // Pin the deadline split: LOCK_WAIT_DEADLINE bounds the flock
    // wait phase ONLY, not the post-acquire write+sync path. Inject
    // a slow Persist that sleeps for 2 * LOCK_WAIT_DEADLINE inside
    // its sync_all() method. The lock acquires immediately (no
    // contention) — the production code signals via phase_tx the
    // moment the guard is in hand, the outer
    // tokio::time::timeout(LOCK_WAIT_DEADLINE, phase_rx) sees the
    // signal, and write+sync runs unbounded. A buggy implementation
    // that wrapped the whole spawn_blocking in
    // tokio::time::timeout(LOCK_WAIT_DEADLINE) would return Transient
    // at ~5s even though the lock was held the whole time. Post-fix:
    // returns Sent after the slow sync completes (~10s).
    use gcit::mail::Persist;

    struct SlowSync(Duration);
    impl Persist for SlowSync {
        fn sync_all(&self, file: &std::fs::File) -> std::io::Result<()> {
            std::thread::sleep(self.0);
            file.sync_all()
        }
    }

    let tmp = TempDir::new().expect("tempdir");
    let user = "u_slow_sync";
    let spool_path = tmp.path().join(user);
    fs::write(&spool_path, b"").expect("create spool");

    let n = LocalMailNotifier::for_test_with_persist(
        "test",
        user,
        Arc::new("host1".to_string()),
        vec![FireEvent::RunComplete],
        LocalMailTemplateConfig::default(),
        handlebars(),
        tmp.path().to_path_buf(),
        Arc::new(SlowSync(LOCK_WAIT_DEADLINE * 2)),
    )
    .expect("test fixture user is alphanumeric, valid");

    let cancel = CancellationToken::new();
    let started = Instant::now();
    let outcome = n
        .on_run_complete(&ctx(), &summary(), &cancel)
        .await
        .expect("slow sync must succeed; lock was acquired immediately");
    let elapsed = started.elapsed();

    assert!(
        matches!(outcome, NotifyOutcome::Sent { .. }),
        "expected Sent after slow sync completes; got {outcome:?}",
    );
    // Elapsed must be at least the slow-sync duration; a buggy
    // implementation that timed out at LOCK_WAIT_DEADLINE would
    // return well under that bound.
    assert!(
        elapsed >= LOCK_WAIT_DEADLINE,
        "slow sync must take at least LOCK_WAIT_DEADLINE wall clock; got {elapsed:?}",
    );
}

#[tokio::test]
async fn deadline_cancellation_token_aware() {
    // Pin SIGTERM-aware bail-out: when the cancellation token fires
    // while the notifier is blocked waiting for the flock, the
    // notifier returns Transient("cancelled") well before
    // LOCK_WAIT_DEADLINE elapses.
    //
    // Concurrency model:
    //   - Holder thread takes the flock and self-releases after 6s
    //     real time (past LOCK_WAIT_DEADLINE so the deadline path would
    //     normally fire first).
    //   - Notifier starts its on_run_complete; the flock wait blocks
    //     inside spawn_blocking.
    //   - Test cancels the token after 200ms.
    //   - Notifier observes cancel.cancelled() and returns
    //     Transient("cancelled") at ~200ms — far inside LOCK_WAIT_DEADLINE.
    //
    // Mutation target: dropping the cancel arm — the call
    // would wait the full LOCK_WAIT_DEADLINE before timing out.
    let tmp = TempDir::new().expect("tempdir");
    let user = "u_cancel";
    let spool_path = tmp.path().join(user);
    fs::write(&spool_path, b"").expect("create spool");

    let spool_for_thread = spool_path.clone();
    let holder = std::thread::spawn(move || {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&spool_for_thread)
            .expect("open holder fd");
        let mut lock = FdLock::new(file);
        let guard = lock.write().expect("acquire holder flock");
        std::thread::sleep(Duration::from_secs(6));
        drop(guard);
    });
    tokio::time::sleep(Duration::from_millis(30)).await;

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        cancel_clone.cancel();
    });

    let n = notifier(tmp.path().to_path_buf(), user);
    let started = Instant::now();
    let result = n.on_run_complete(&ctx(), &summary(), &cancel).await;
    let elapsed = started.elapsed();

    match result {
        Err(NotifyError::Transient { source, .. }) => {
            let msg = source.to_string();
            assert!(
                msg.contains("cancelled"),
                "cancel-aware Transient must carry the 'cancelled' message; got: {msg}",
            );
        }
        other => panic!("expected Transient(cancelled); got {other:?}"),
    }
    // Cancel fires at 200ms; the notifier must return well before
    // LOCK_WAIT_DEADLINE (5s). Allow some scheduler slack but pin the
    // bound far enough below the deadline that a regression that
    // ignores the cancel token would fail this assertion.
    assert!(
        elapsed < Duration::from_secs(2),
        "cancellation must short-circuit the LOCK_WAIT_DEADLINE wait; got elapsed {elapsed:?}",
    );

    // Cancel fired BEFORE the lock was acquired (holder still
    // holds it). Spool must be byte-identical to its pre-test
    // state (empty) — no write occurred, so a retry on the next
    // supervisor cycle won't produce a duplicate.
    let bytes_after = fs::read(&spool_path).expect("read spool after cancel");
    assert_eq!(
        bytes_after, b"",
        "spool must be untouched when cancel fires during lock-wait; \
         a write here would produce a duplicate on retry",
    );

    // Wait for the holder thread to finish its 6s sleep so the
    // tempdir teardown is not racing the holder's drop.
    holder.join().expect("holder thread");
}

#[tokio::test]
async fn cancel_after_completion_returns_sent() {
    // Pin: post-acquire cancel doesn't steal a completed write.
    // The biased select! arm in on_run_complete puts phase_rx
    // FIRST, so when both arms become ready in the same tokio
    // poll cycle (lock acquired AND cancel fired), phase_rx wins
    // and the notifier proceeds to await the blocking task's
    // write+sync. Without the biased ordering, a SIGTERM
    // simultaneous with lock-acquire would record Transient and
    // the supervisor would retry on the next cycle, producing a
    // duplicate spool entry once the original blocking task's
    // write finished.
    //
    // Test: inject a Persist that calls cancel.cancel() from
    // INSIDE its sync_all hook AFTER file.sync_all() succeeds.
    // At that point the lock is held, phase_tx has been sent,
    // and the spool bytes are written. Cancel firing now must
    // NOT convert the outcome to Transient — the helper has
    // already committed the write.
    use gcit::mail::Persist;

    struct CancelAfterSync(CancellationToken);
    impl Persist for CancelAfterSync {
        fn sync_all(&self, file: &std::fs::File) -> std::io::Result<()> {
            file.sync_all()?;
            // Sync succeeded — the spool bytes are durable.
            // Now cancel: a buggy notifier would surface this as
            // Transient even though the write is already done.
            self.0.cancel();
            Ok(())
        }
    }

    let tmp = TempDir::new().expect("tempdir");
    let user = "u_after_sync";
    let spool_path = tmp.path().join(user);
    fs::write(&spool_path, b"").expect("create spool");

    let cancel = CancellationToken::new();
    let n = LocalMailNotifier::for_test_with_persist(
        "test",
        user,
        Arc::new("host1".to_string()),
        vec![FireEvent::RunComplete],
        LocalMailTemplateConfig::default(),
        handlebars(),
        tmp.path().to_path_buf(),
        Arc::new(CancelAfterSync(cancel.clone())),
    )
    .expect("test fixture user is alphanumeric, valid");

    // Capture state at panic time so any future failure has the
    // diagnostic context to identify the failure mode:
    //   - elapsed: <21ms = pre-pipeline (open(2) EMFILE or cancel
    //     fired before on_run_complete polled); >5s = LOCK_WAIT_DEADLINE
    //     timeout (impossible without an outside flock holder);
    //     ~ms = expected happy-path range.
    //   - cancel_at_panic: true means cancel did fire (expected
    //     post-sync); false means the cancel-arm shouldn't have won
    //     and the failure points at a production bug.
    //   - spool_len: non-zero means the write committed before the
    //     production code surfaced Err — that's a duplicate-write
    //     bug (the operator would see the spool entry AND retry
    //     pressure).
    let started = Instant::now();
    let outcome = n.on_run_complete(&ctx(), &summary(), &cancel).await;
    let elapsed = started.elapsed();
    let outcome = outcome.unwrap_or_else(|e| {
        let cancel_at_panic = cancel.is_cancelled();
        let spool_len = fs::metadata(&spool_path)
            .map(|m| m.len())
            .unwrap_or(u64::MAX);
        panic!(
            "post-acquire cancel must NOT steal a completed write: \
             err={e:?} elapsed={elapsed:?} cancel_at_panic={cancel_at_panic} \
             spool_len={spool_len} spool_path={}",
            spool_path.display(),
        )
    });

    assert!(
        matches!(outcome, NotifyOutcome::Sent { .. }),
        "expected Sent after sync completed (cancel fired post-acquire); got {outcome:?}",
    );

    // Spool must contain the message bytes — the write committed
    // before cancel fired.
    let bytes_after = fs::read(&spool_path).expect("read spool");
    assert!(
        !bytes_after.is_empty(),
        "spool must contain the appended message after Sent; got empty",
    );
}

#[tokio::test]
async fn cancel_already_set_returns_immediately() {
    // Pre-cancel test: cancel.cancel() BEFORE on_run_complete is
    // called. The notifier observes the already-cancelled token
    // and returns Transient("cancelled before lock acquired ...")
    // immediately, without writing to the spool.
    //
    // Mutation target: dropping the cancel arm or making
    // it lazy (only checked AFTER spawn_blocking starts) — a
    // pre-cancelled token would still spend the full
    // LOCK_WAIT_DEADLINE blocking on lock-wait, returning at 5s.
    let tmp = TempDir::new().expect("tempdir");
    let user = "u_pre_cancel";
    let spool_path = tmp.path().join(user);
    fs::write(&spool_path, b"").expect("create spool");

    let cancel = CancellationToken::new();
    cancel.cancel(); // Set BEFORE the notifier runs.

    let n = notifier(tmp.path().to_path_buf(), user);
    let started = Instant::now();
    let result = n.on_run_complete(&ctx(), &summary(), &cancel).await;
    let elapsed = started.elapsed();

    match result {
        Err(NotifyError::Transient { source, .. }) => {
            let msg = source.to_string();
            assert!(
                msg.contains("cancelled"),
                "pre-cancel must surface Transient with 'cancelled'; got: {msg}",
            );
        }
        other => panic!("expected Transient(cancelled); got {other:?}"),
    }

    // Pre-cancel must short-circuit far below LOCK_WAIT_DEADLINE
    // (5s). Pin a bound that's tight enough to catch a regression
    // (a notifier that ignored the pre-cancelled token would burn
    // the full 5s lock-wait) but loose enough to absorb scheduler
    // jitter under concurrent suite load. 1s is well below the
    // deadline and well above worst-case concurrent-runner
    // overhead seen empirically.
    assert!(
        elapsed < Duration::from_secs(1),
        "pre-cancel must short-circuit well below LOCK_WAIT_DEADLINE; got elapsed {elapsed:?}",
    );

    // Spool must be byte-identical to its pre-test empty state —
    // the cancel arm fired before any write could happen.
    let bytes_after = fs::read(&spool_path).expect("read spool after pre-cancel");
    assert_eq!(
        bytes_after, b"",
        "spool must be untouched when cancel is set before on_run_complete runs",
    );
}

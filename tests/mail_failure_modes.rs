// Failure-mode mapping for the mbox notifier (production
// `LocalMailNotifier` at src/mail/notifier.rs).
//
// Three classifications are pinned end-to-end via the full
// `on_run_complete` pipeline (render → format_message → spawn_blocking
// open + flock + write + sync_all):
//
//   * ENOENT (spool absent at open)              → Permanent
//   * EACCES (spool open(2) refused for write)   → Permanent
//   * Lock contention beyond `LOCK_WAIT_DEADLINE` (5s)→ Transient
//
// Tests drive `LocalMailNotifier::for_test` with a tempdir spool to
// avoid touching `/var/mail`. The non-activated rstest case below
// (full classifier table) still requires expanded `map_io_error`
// arms; the logging/status-wiring stubs require tracing capture
// and supervisor scaffolding respectively.

use std::io::Write as _;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use gix_hash::ObjectId;
use rstest::rstest;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use tracing_test::traced_test;
use uuid::Uuid;

use gcit::config::{FireEvent, LocalMailTemplateConfig};
use gcit::github::{Conclusion, RunStatus, RunSummary};
use gcit::mail::LocalMailNotifier;
use gcit::notify::{strict_handlebars, ActionInfo, Notifier, NotifyError, RunContext, SourceInfo};

mod common;

fn ctx() -> RunContext {
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

fn summary_success() -> RunSummary {
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

/// Build a notifier that fires on RunComplete with the spool
/// redirected to `<spool_dir>/<user>` via the test-only constructor.
fn notifier_for(spool_dir: &std::path::Path, user: &str) -> LocalMailNotifier {
    LocalMailNotifier::for_test(
        "test",
        user,
        Arc::new("h.example.com".to_string()),
        vec![FireEvent::RunComplete],
        LocalMailTemplateConfig {
            subject: None,
            body: None,
        },
        Arc::new(strict_handlebars()),
        spool_dir.to_path_buf(),
    )
    .expect("test fixture user is alphanumeric, valid")
}

#[tokio::test]
async fn enoent_returns_permanent_with_remediation() {
    // Spool file is absent: `OpenOptions::new().append(true).open(path)`
    // returns `ErrorKind::NotFound`, mapped by `map_io_error` to
    // Permanent with a message containing "does not exist" and the
    // remediation hints (`useradd`/`mailx`/`touch`).
    //
    // Mutation target: mapping NotFound to Transient — gcit
    // would retry forever against an absent spool that no operator
    // intervention will create.
    let tmp = TempDir::new().expect("tempdir");
    // Deliberately do NOT create the spool file; tempdir parent
    // exists, the per-user spool path does not.
    let user = "u";
    let spool_path = tmp.path().join(user);
    assert!(
        !spool_path.exists(),
        "test invariant: spool must not exist before append",
    );

    let n = notifier_for(tmp.path(), user);
    let err = n
        .on_run_complete(&ctx(), &summary_success(), &CancellationToken::new())
        .await
        .expect_err("missing spool must surface as Err");
    assert!(
        !err.is_transient(),
        "absent-spool error must be Permanent (no operator action retries the open); got {err:?}",
    );
    let NotifyError::Permanent { source } = err else {
        panic!("expected Permanent for absent spool; got an unreachable variant");
    };
    let msg = source.to_string();
    assert!(
        msg.contains("does not exist"),
        "Permanent message must name the missing-spool condition; got {msg}",
    );
    // The error names the spool path so operators can find the file
    // they need to create.
    assert!(
        msg.contains(&spool_path.display().to_string()),
        "Permanent message must name the spool path {}; got {msg}",
        spool_path.display(),
    );
    // Remediation hints — at least one of the documented commands.
    assert!(
        msg.contains("useradd") || msg.contains("touch") || msg.contains("mailx"),
        "Permanent message must surface remediation hints (useradd/touch/mailx); got {msg}",
    );

    // Spool file must NOT have been side-created — gcit does not
    // auto-create absent spool files.
    assert!(
        !spool_path.exists(),
        "absent-spool path must NOT have been created by the failed open; mbox is read/write only",
    );
}

#[tokio::test]
async fn eacces_returns_permanent_with_systemd_directive() {
    // Spool file exists but is read-only (mode 0o444). On Linux,
    // `OpenOptions::new().append(true)` against a 0o444 file fails
    // with EACCES at open(2). `map_io_error` routes
    // `ErrorKind::PermissionDenied` to Permanent with a message
    // containing the `BindPaths=/var/mail` systemd directive
    // hint, so operators see how to relax the unit's sandboxing.
    //
    // Mutation target: mapping PermissionDenied to
    // Transient — gcit retries forever against a permission issue.
    //
    // Skipped under euid 0: root bypasses DAC mode bits via
    // CAP_DAC_OVERRIDE, so the open(2) succeeds and EACCES never
    // surfaces. CI runners are non-root.
    if common::euid_is_root() {
        eprintln!(
            "eacces_returns_permanent_with_systemd_directive: skipped — \
             euid 0 bypasses DAC and the open(2) would succeed; \
             test relies on EACCES at open which is non-root only",
        );
        return;
    }

    let tmp = TempDir::new().expect("tempdir");
    let user = "u";
    let spool_path = tmp.path().join(user);
    std::fs::write(&spool_path, b"").expect("seed empty spool");
    // 0o444: r--r--r--. No write bit for any class — open(2) with
    // O_APPEND / O_WRONLY refuses with EACCES.
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&spool_path, std::fs::Permissions::from_mode(0o444))
        .expect("chmod 0o444");

    let n = notifier_for(tmp.path(), user);
    let err = n
        .on_run_complete(&ctx(), &summary_success(), &CancellationToken::new())
        .await
        .expect_err("read-only spool must surface as Err");

    let NotifyError::Permanent { source } = err else {
        panic!("expected Permanent for EACCES at open; got {err:?}");
    };
    let msg = source.to_string();
    // The systemd-directive remediation: when the daemon runs under
    // a hardened unit, `BindPaths=/var/mail` is the relevant
    // sandbox knob. Pin the literal so dashboards / docs that
    // search-link operators to the directive remain accurate.
    assert!(
        msg.contains("BindPaths"),
        "Permanent message must surface the BindPaths systemd directive hint; got {msg}",
    );
    assert!(
        msg.contains(&spool_path.display().to_string()),
        "Permanent message must name the spool path {}; got {msg}",
        spool_path.display(),
    );

    // Restore writable mode so tempdir teardown does not race
    // (remove_dir_all on the parent must be able to unlink children
    // it created; on Linux the parent dir mode controls unlink, but
    // restoring the file mode is harmless cleanup).
    std::fs::set_permissions(&spool_path, std::fs::Permissions::from_mode(0o644))
        .expect("restore mode");
}

#[tokio::test]
async fn lock_contention_5_sec_deadline_returns_transient() {
    // A separate OS thread holds an exclusive `flock(LOCK_EX)` on
    // the spool file for longer than `LOCK_WAIT_DEADLINE` (5s in
    // production). The notifier's `on_run_complete` runs the
    // helper in `spawn_blocking`; the helper signals lock-acquired
    // via a oneshot, and `on_run_complete` wraps the receiver in
    // `tokio::time::timeout(LOCK_WAIT_DEADLINE, phase_rx)`. The
    // holder keeps the production path blocked on `lock.write()`
    // past the timeout; the timeout arm surfaces
    // `NotifyError::Transient` so backon retries on the next
    // supervisor cycle.
    //
    // Mutation target: dropping the timeout wrapper and
    // lets `lock.write()` block indefinitely — a wedged spool
    // reader hangs gcit forever.
    //
    // Mutation target: mapping the timeout to Permanent —
    // backon would NOT retry, and a transient flock contention
    // (e.g. another mbox writer cycling through the spool) would
    // permanently kill notifications.
    //
    // Concurrency model:
    //   - Holder thread takes flock(LOCK_EX) and signals "taken"
    //     via `taken_tx`.
    //   - Main test waits on `taken_rx`, then triggers the
    //     production append.
    //   - On Transient surface, main test signals "release" via
    //     `release_tx` so the holder drops the lock.
    //   - Holder thread is joined cleanly before test return.
    //
    // The production spawn_blocking task that's stuck in
    // `lock.write()` continues to run after the timeout fires
    // (spawn_blocking is non-cancellable; the OS thread waits on
    // the kernel's flock queue). Once the holder releases, that
    // lingering task obtains the lock, completes its write, and
    // exits. The tokio runtime drains the blocking pool at test
    // teardown; the post-timeout write lands in the file we are
    // about to drop, so it is harmless.
    let tmp = TempDir::new().expect("tempdir");
    let user = "u";
    let spool_path = tmp.path().join(user);
    std::fs::write(&spool_path, b"").expect("seed empty spool");

    let (taken_tx, taken_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let holder_path = spool_path.clone();
    let holder = std::thread::spawn(move || {
        // Open with O_APPEND mirroring the production open mode so
        // the inode-keyed flock(2) collides with the production
        // path. We hold the lock by leaking the guard's lifetime
        // through a stack frame that blocks on `release_rx.recv()`.
        let mut file = std::fs::OpenOptions::new()
            .create(false)
            .append(true)
            .open(&holder_path)
            .expect("holder open");
        // Use the libc flock(2) syscall directly — fd-lock would
        // work but it is a prod-only dep; libc is already a direct
        // dep accessible from integration tests (Cargo.toml).
        // LOCK_EX (exclusive): blocks all other LOCK_EX callers on
        // the same inode until released.
        let rc =
            unsafe { libc::flock(std::os::unix::io::AsRawFd::as_raw_fd(&file), libc::LOCK_EX) };
        assert_eq!(
            rc,
            0,
            "holder flock(LOCK_EX) must succeed: {}",
            std::io::Error::last_os_error()
        );
        // Non-zero write so a future investigator can see the
        // holder ran (helps when this test is debugged in CI).
        file.write_all(b"holder\n").expect("holder write");
        // Signal main test that the lock is held.
        taken_tx.send(()).expect("notify lock-taken");
        // Block until the main test releases us. recv() returns
        // Err only if the sender is dropped — that is itself a
        // valid release condition.
        let _ = release_rx.recv();
        // Drop happens via `flock(LOCK_UN)` on close, but be
        // explicit to make the release-point obvious.
        let rc =
            unsafe { libc::flock(std::os::unix::io::AsRawFd::as_raw_fd(&file), libc::LOCK_UN) };
        assert_eq!(
            rc,
            0,
            "holder flock(LOCK_UN) must succeed: {}",
            std::io::Error::last_os_error()
        );
        drop(file);
    });

    // Wait for holder to confirm lock is taken — bounded so a
    // wedged holder fails fast instead of hanging the suite.
    taken_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("holder must signal lock-taken within 5s");

    let n = notifier_for(tmp.path(), user);
    let before = std::time::Instant::now();
    // Outer 8s timeout: production deadline is 5s; the call should
    // return well within 8s. If on_run_complete is still pending at
    // 8s the inner `tokio::time::timeout` has been dropped
    // and the test would otherwise deadlock against the holder
    // thread. Surface that as a fast assertion failure rather than
    // hanging the suite until nextest's outer timeout reaps us.
    let surfaced = tokio::time::timeout(
        Duration::from_secs(8),
        n.on_run_complete(&ctx(), &summary_success(), &CancellationToken::new()),
    )
    .await;
    let err = match surfaced {
        Ok(Err(e)) => e,
        Ok(Ok(o)) => panic!(
            "contended lock must surface as Err, got Ok({o:?}) — \
             LOCK_WAIT_DEADLINE timeout may have been removed, \
             or the production write somehow succeeded against our holder",
        ),
        Err(_) => {
            // Surface release before failing so the holder thread
            // does not leak past test termination.
            let _ = release_tx.send(());
            let _ = holder.join();
            panic!(
                "contended lock did not surface within 8s — \
                 LOCK_WAIT_DEADLINE tokio::time::timeout was dropped \
                 (production should return Transient at ~5s, well inside this bound)",
            );
        }
    };
    let elapsed = before.elapsed();

    // Production deadline is `LOCK_WAIT_DEADLINE = 5s`. Wall clock
    // must sit between ~5s (the deadline fires) and the outer 8s
    // safety bound. Below 4.5s would mean the deadline did not
    // fire (the call returned via some other path).
    assert!(
        elapsed >= Duration::from_millis(4_500),
        "contended path must wait at least ~5s before surfacing Transient; elapsed {elapsed:?} — \
         a too-fast return suggests the deadline was bypassed (e.g. flock(LOCK_NB) without contention).",
    );

    let NotifyError::Transient { source, .. } = err else {
        panic!(
            "expected Transient (RateLimited / lock-contended retryable); got {err:?}. \
             A Permanent here means backon would NOT retry the next supervisor cycle, \
             defeating the deadline's purpose."
        );
    };
    let msg = source.to_string();
    assert!(
        msg.contains("lock") || msg.contains("not acquired"),
        "Transient message must surface the deadline cause (lock contention); got {msg}",
    );

    // Release the holder so its blocking flock(LOCK_UN) completes
    // and the thread exits. The lingering spawn_blocking task in
    // the production path also unblocks and finishes; tokio drains
    // it during test teardown.
    release_tx.send(()).expect("release holder");
    holder.join().expect("holder thread must join cleanly");
}

#[tokio::test]
#[ignore = "needs production change to expand map_io_error or a test seam for raw_os_error injection"]
async fn enospc_returns_transient_for_backon_retry() {
    // Simulating ENOSPC requires either a tmpfs mount with size
    // cap (root-only) or an injected `io::Error::from_raw_os_error(28)`
    // through a test seam. Production `map_io_error` already routes
    // unknown io::ErrorKind to Transient, so the contract holds —
    // pinning it requires the seam.
}

#[tokio::test]
#[ignore = "needs test seam to inject io::Error::from_raw_os_error(5)"]
async fn eio_returns_transient() {
    // Same shape as ENOSPC — needs raw_os_error injection.
}

#[tokio::test]
#[ignore = "needs test seam to inject io::Error::from_raw_os_error(16)"]
async fn ebusy_returns_transient() {
    // EBUSY can occur on mount-point during unmount. Treat as
    // Transient — operator's mount-state issue is presumably
    // self-resolving.
}

#[rstest]
// Permanent: no operator-free retry resolves these.
#[case::enoent(libc::ENOENT, false)]
#[case::eacces(libc::EACCES, false)]
#[case::eperm(libc::EPERM, false)]
#[case::eloop(libc::ELOOP, false)]
#[case::eisdir(libc::EISDIR, false)]
#[case::einval(libc::EINVAL, false)]
// Transient: retry on the next supervisor cycle may resolve these.
#[case::enospc(libc::ENOSPC, true)]
#[case::eio(libc::EIO, true)]
#[case::ebusy(libc::EBUSY, true)]
#[case::eintr(libc::EINTR, true)]
#[case::eagain(libc::EAGAIN, true)]
fn classify_io_errors(#[case] errno: i32, #[case] expect_transient: bool) {
    // Pin the full `map_io_error` classification table via the
    // production `gcit::mail::map_io_error` test seam (a doc(hidden)
    // pub re-export of the notifier's classifier).
    //
    // Permanent rationale per arm:
    //   - ENOENT: spool absent; gcit refuses to auto-create per the
    //     file-ownership contract. Operator must touch + chown.
    //   - EACCES / EPERM: mode bits, ACL, or capability; both map to
    //     ErrorKind::PermissionDenied per std/sys/io/error/unix.rs.
    //   - ELOOP: O_NOFOLLOW caught a symlink at the final component.
    //     Resolving the symlink is operator-side; retry won't help.
    //   - EISDIR: the spool path is a directory. Operator
    //     misconfigured the layout.
    //   - EINVAL: the filesystem rejected the open flags
    //     (O_APPEND + O_NOFOLLOW). Programmer-side or platform-side
    //     issue; retry won't help.
    //
    // Transient rationale per arm:
    //   - ENOSPC: disk fills, then drains. Backon retry may succeed.
    //   - EIO: filesystem hardware glitch; transient by definition.
    //   - EBUSY: typically mid-unmount on the spool's filesystem;
    //     self-resolves once unmount completes.
    //   - EINTR: signal delivered mid-syscall; retry resumes.
    //   - EAGAIN: temporary resource exhaustion (e.g. fork limits);
    //     retry after backoff.
    //
    // Mutation target: dropping any of the 5 raw_os_error arms in
    // `map_io_error` would surface a Permanent case as Transient
    // and gcit would retry forever against a non-recoverable
    // failure. Pinning each errno's classification catches the
    // regression cell-by-cell.
    let err = std::io::Error::from_raw_os_error(errno);
    let path = std::path::PathBuf::from("/var/mail/test");
    let mapped = gcit::mail::map_io_error(err, &path);
    assert_eq!(
        mapped.is_transient(),
        expect_transient,
        "errno {errno} ({}) classification mismatch — got is_transient={}, expected={}",
        std::io::Error::from_raw_os_error(errno),
        mapped.is_transient(),
        expect_transient,
    );
}

/// Permanent failures emit a notifier-side WARN under target
/// `gcit::mail` with `notifier`, `flow`, `user`, `spool_path`, and
/// `error` fields. The supervisor's `record_last_error` emits the
/// dashboard-level WARN with `kind`/`body`/`retry_at`; this one
/// gives journalctl readers the notifier-specific context (which
/// spool was attempted, which user the mbox targets) without
/// cross-referencing.
///
/// Drives the ENOENT path (absent spool -> `Permanent` via
/// `map_io_error`) because it's the same path the supervisor
/// would record under `last_error.kind = "notifier_failed"`.
#[tokio::test]
#[traced_test]
async fn permanent_error_logged_at_warn_with_flow_context() {
    let tmp = TempDir::new().expect("tempdir");
    let user = "u";
    let n = notifier_for(tmp.path(), user);
    let err = n
        .on_run_complete(&ctx(), &summary_success(), &CancellationToken::new())
        .await
        .expect_err("absent spool must surface as Err");
    assert!(matches!(err, NotifyError::Permanent { .. }));

    // tracing-subscriber renders `Display`-formatted fields as
    // `name=value` without surrounding quotes. Pin the field set
    // dashboards filter on, plus the literal message body so the
    // assertion only fires on the canonical permanent-failure event.
    assert!(
        logs_contain("local_mail permanent failure"),
        "permanent failure must emit the canonical message body",
    );
    assert!(
        logs_contain("flow=ci-flow"),
        "permanent emit must carry the flow correlation key",
    );
    assert!(
        logs_contain(&format!("user={user}")),
        "permanent emit must carry the spool user",
    );
    let spool_path = tmp.path().join(user);
    assert!(
        logs_contain(&format!("spool_path={}", spool_path.display())),
        "permanent emit must name the spool path",
    );
}

/// Transient failures emit a notifier-side INFO under target
/// `gcit::mail` with the same field set as the permanent WARN — the
/// supervisor's record_last_error still emits the dashboard-level
/// WARN after backon exhausts the retries. Drives the pre-cancelled
/// token path (early-bail `Transient` at on_run_complete entry).
///
/// The "WARN after retry exhausts" half of the original test
/// description belongs at the supervisor harness — that loop owns the
/// retry budget; the notifier never re-runs internally.
#[tokio::test]
#[traced_test]
async fn transient_error_logged_at_info_with_flow_context() {
    let tmp = TempDir::new().expect("tempdir");
    let user = "u";
    let n = notifier_for(tmp.path(), user);
    let cancel = CancellationToken::new();
    cancel.cancel();
    let err = n
        .on_run_complete(&ctx(), &summary_success(), &cancel)
        .await
        .expect_err("pre-cancelled token must surface as Err");
    assert!(matches!(err, NotifyError::Transient { .. }));

    assert!(
        logs_contain("local_mail transient failure"),
        "transient failure must emit the canonical message body",
    );
    assert!(
        logs_contain("flow=ci-flow"),
        "transient emit must carry the flow correlation key",
    );
    assert!(
        logs_contain(&format!("user={user}")),
        "transient emit must carry the spool user",
    );
}

#[tokio::test]
#[ignore = "needs supervisor wiring — last_error is updated outside the notifier"]
async fn permanent_error_writes_to_status_last_error() {
    // `gcit status` shows last_error per flow. The notifier returns
    // Permanent; the supervisor records last_error. Pinning this
    // requires running the supervisor end-to-end against a fixture
    // (separate work item).
}

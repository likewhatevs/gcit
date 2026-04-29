// Single-instance flock on the runtime-directory lock file.
//
// `state::open_instance_lock_file` opens (or creates) the lock
// file under $RUNTIME_DIRECTORY/gcit.lock and wraps the fd in
// `fd_lock::RwLock`. The supervisor calls `try_write` and holds
// the resulting guard for the daemon's lifetime; a second
// instance's `try_write` surfaces ErrorKind::WouldBlock and the
// supervisor returns DaemonError::State(StateError::LockHeld).
//
// These tests exercise `open_instance_lock_file` + `try_write`
// directly because the supervisor's full `run()` requires a
// systemd environment (notify FDs, control listener, etc.) that
// is not feasible to construct in an integration test. The
// helper is the load-bearing single-instance gate; testing it
// covers the contention path without booting the daemon.

use fd_lock::RwLock as FdLock;
use std::fs::OpenOptions;
use tempfile::TempDir;

#[test]
fn open_creates_file_with_mode_0600() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = TempDir::new().expect("tempdir");
    let lock_path = tmp.path().join("gcit.lock");
    assert!(
        !lock_path.exists(),
        "test invariant: lock must not pre-exist"
    );

    let _lock = gcit::state::open_instance_lock_file(&lock_path).expect("open lock");

    assert!(lock_path.exists(), "lock file must be created on open");
    let meta = std::fs::metadata(&lock_path).expect("stat lock");
    let mode = meta.permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "lock file mode must be 0600; got {mode:o}",);
}

#[test]
fn open_creates_missing_parent_directory() {
    let tmp = TempDir::new().expect("tempdir");
    let nested_dir = tmp.path().join("nested").join("gcit");
    let lock_path = nested_dir.join("gcit.lock");
    assert!(
        !nested_dir.exists(),
        "test invariant: nested parent must not pre-exist",
    );

    let _lock = gcit::state::open_instance_lock_file(&lock_path).expect("open lock");

    assert!(
        nested_dir.is_dir(),
        "open_instance_lock_file must create the parent directory",
    );
    assert!(lock_path.exists(), "lock file must be created");
}

#[test]
fn second_try_write_on_held_lock_returns_wouldblock() {
    // Pin the contention contract: when one process (or one
    // RwLock instance in the same process) holds the write
    // guard, a fresh open + try_write on the same path surfaces
    // ErrorKind::WouldBlock. This is the signal the supervisor
    // converts into StateError::LockHeld.
    //
    // The two RwLock instances open the file independently and
    // call flock(2) on different fds; per Linux flock(2) semantics
    // ("If a process uses open(2) ... to obtain more than one file
    // descriptor for the same file, these file descriptors are
    // treated independently by flock()"), the second fd's try_write
    // observes the first fd's exclusive hold and returns WouldBlock.
    let tmp = TempDir::new().expect("tempdir");
    let lock_path = tmp.path().join("gcit.lock");

    let mut first_lock = gcit::state::open_instance_lock_file(&lock_path).expect("open first lock");
    let _first_guard = first_lock
        .try_write()
        .expect("first try_write must succeed");

    // Second open + try_write must surface WouldBlock.
    let mut second_lock =
        gcit::state::open_instance_lock_file(&lock_path).expect("open second lock");
    let result = second_lock.try_write();
    match &result {
        Ok(_guard) => panic!(
            "second try_write must fail with WouldBlock while first guard is held; \
             flock(LOCK_EX) is not exclusive across fds",
        ),
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
        Err(e) => panic!("second try_write returned unexpected error: {e:?}"),
    }
    drop(result);
}

#[test]
fn drop_first_guard_releases_lock_for_second_acquire() {
    // Pin the release contract: dropping the write guard
    // releases the flock, and a subsequent open + try_write on
    // the same path acquires immediately. The supervisor's
    // RAII-via-stack-frame pattern relies on guard drop at
    // run() exit to release for a follow-up start.
    let tmp = TempDir::new().expect("tempdir");
    let lock_path = tmp.path().join("gcit.lock");

    {
        let mut first_lock =
            gcit::state::open_instance_lock_file(&lock_path).expect("open first lock");
        let _first_guard = first_lock.try_write().expect("first try_write");
        // First guard goes out of scope here and releases the
        // flock.
    }

    let mut second_lock =
        gcit::state::open_instance_lock_file(&lock_path).expect("open second lock");
    let _second_guard = second_lock
        .try_write()
        .expect("second try_write must succeed after first guard drops");
}

#[test]
fn cooperating_writer_using_fdlock_directly_is_blocked_by_held_guard() {
    // Defense-in-depth: a cooperating writer that uses fd-lock
    // directly (no open_instance_lock_file helper) on the same
    // path also observes WouldBlock when the helper-acquired
    // guard is held. This proves the lock semantics are not a
    // helper-internal artifact — the same flock that a
    // hand-rolled test fixture sees is the same flock the
    // production helper takes.
    let tmp = TempDir::new().expect("tempdir");
    let lock_path = tmp.path().join("gcit.lock");

    let mut helper_lock =
        gcit::state::open_instance_lock_file(&lock_path).expect("open helper lock");
    let _helper_guard = helper_lock.try_write().expect("helper try_write");

    let direct_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("direct open");
    let mut direct_lock = FdLock::new(direct_file);
    let result = direct_lock.try_write();
    match &result {
        Ok(_guard) => panic!("direct fd-lock try_write must fail while helper guard is held",),
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
        Err(e) => panic!("direct try_write returned unexpected error: {e:?}"),
    }
    drop(result);
}

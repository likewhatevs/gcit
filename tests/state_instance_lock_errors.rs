// Error-path tests for `state::open_instance_lock_file`.
//
// The happy paths (file created, mode 0600, contention -> WouldBlock,
// release on guard drop) are pinned in tests/state_instance_lock.rs.
// This file pins the two error arms that fire when the kernel rejects
// the create_dir_all + OpenOptions::open calls:
//
//   - `StateError::CreateRuntimeDirectory`: parent directory cannot be
//     created. We force this by passing a nested path under a parent
//     that is itself a regular file (mkdir below a file fails with
//     ENOTDIR).
//   - `StateError::LockOpen`: open(2) on the lock file itself fails.
//     We force this by passing a path that is an existing directory
//     (open with O_RDWR + O_CREAT against a directory fails EISDIR
//     under Linux).

use tempfile::TempDir;

#[test]
fn open_instance_lock_file_returns_create_runtime_directory_when_parent_is_a_file() {
    // Build: <td>/file (regular file)
    //        <td>/file/sub/gcit.lock (lock under that file)
    //
    // create_dir_all on <td>/file/sub fails because /file is not a
    // directory; the error variant must be CreateRuntimeDirectory.
    let td = TempDir::new().expect("tempdir");
    let parent_file = td.path().join("file");
    std::fs::write(&parent_file, b"not a directory").expect("write parent file");
    let lock_path = parent_file.join("sub").join("gcit.lock");

    let err = gcit::state::open_instance_lock_file(&lock_path)
        .expect_err("open must fail when parent path is a regular file");
    let msg = err.to_string();
    assert!(
        msg.contains("cannot create runtime directory"),
        "error must surface CreateRuntimeDirectory's display prefix; got: {msg}",
    );
    // The sub-directory path is what create_dir_all chokes on; the
    // error must surface the whole path so the operator sees the full
    // chain rather than just the leaf or the existing file.
    assert!(
        msg.contains(&parent_file.join("sub").display().to_string()),
        "error must name the parent path it could not create; got: {msg}",
    );
}

#[test]
fn open_instance_lock_file_returns_lock_open_when_path_is_a_directory() {
    // open(2) with O_RDWR + O_CREAT against an existing DIRECTORY
    // fails with EISDIR under Linux. The error variant must be
    // LockOpen (the create_dir_all step succeeded — the parent is
    // valid — but the open of the lock path itself fails).
    let td = TempDir::new().expect("tempdir");
    // Use td.path() itself as the lock path. The parent (td.path()'s
    // parent) is a directory, create_dir_all is a no-op, but the open
    // hits the existing directory.
    let lock_path = td.path();

    let err = gcit::state::open_instance_lock_file(lock_path)
        .expect_err("open must fail when the lock path is a directory");
    let msg = err.to_string();
    assert!(
        msg.contains("cannot open instance lock"),
        "error must surface LockOpen's display prefix; got: {msg}",
    );
    assert!(
        msg.contains(&lock_path.display().to_string()),
        "error must name the lock path it could not open; got: {msg}",
    );
}

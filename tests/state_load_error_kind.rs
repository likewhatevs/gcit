// `load_or_init` error-kind discrimination at the open-file stage.
//
// `load_or_init` must distinguish ENOENT from
// every other open(2) failure: ENOENT is the first-run path and
// returns `Ok(State::default())`; any other open or read error must
// surface as `StateError::LoadIo { .. }` so the operator sees a
// precise diagnostic instead of a silently empty in-memory state on
// top of an inaccessible on-disk file.
//
// I/O failures route through `LoadIo` (not `Load`) — `Load` carries
// the "back up + remove" guidance for corrupt-content scenarios
// (parse / schema), which is misleading for permission or path-shape
// failures where the file may be valid.
//
// This test pins the match-guard at the `Err(e) if e.kind() ==
// io::ErrorKind::NotFound =>` arm: a mutation that drops the guard
// (replacing the predicate with `true`) would route every open error
// through the default-state path, masking permission failures and
// other I/O faults at startup.
//
// Approach: write a real state.json into a tempdir, chmod it 0o000 so
// `fs::File::open` (read mode) fails with EACCES at open(2), then
// invoke `load_or_init` and assert the result is
// `Err(StateError::LoadIo)` rather than `Ok(State::default())`.
// Permissions are restored before drop so the tempfile cleanup in
// `tempdir`'s Drop impl can remove the file.
//
// Skipped under euid 0: root bypasses DAC mode bits via
// CAP_DAC_OVERRIDE, so open(2) succeeds even on a 0o000 file and EACCES
// never surfaces. CI runners are non-root.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use gcit::state::{load_or_init, State, StateError};

mod common;

/// Restore-on-drop guard so a panic between chmod-000 and the explicit
/// chmod-back at end of test does not leave the tempdir un-cleanable.
/// `tempfile::TempDir` removes via `std::fs::remove_dir_all`, which
/// fails if a child file is 0o000 AND the test panicked before the
/// explicit restore. This RAII guard ensures the chmod-back happens
/// regardless of the unwinding path.
struct RestoreMode<'a> {
    path: &'a Path,
    mode: u32,
}

impl Drop for RestoreMode<'_> {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(self.path, std::fs::Permissions::from_mode(self.mode));
    }
}

#[test]
fn load_or_init_eacces_at_open_surfaces_as_load_io_not_default() {
    if common::euid_is_root() {
        eprintln!(
            "load_or_init_eacces_at_open_surfaces_as_load_io_not_default: skipped — \
             euid 0 bypasses DAC and open(2) succeeds on a 0o000 file; \
             test relies on EACCES at open(2) which is non-root only",
        );
        return;
    }

    let td = tempfile::tempdir().expect("tempdir");
    let p = td.path().join("state.json");
    // Seed with valid schema-1 content. The mutant under test
    // (`Err(e) if true`) would route the EACCES at open(2) into the
    // default-state arm; if open(2) DID succeed and reach the parse
    // pass, the seeded body proves the test isn't accidentally
    // succeeding via a parse failure.
    std::fs::write(&p, r#"{"schema": 1, "flows": {}}"#).expect("seed valid state body");

    // 0o000: no read/write/execute for any class. open(2) with
    // O_RDONLY refuses with EACCES.
    std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o000)).expect("chmod 0o000");
    // mode must match the fixture's pre-chmod-000 permissions (0o600
    // per the write at line 73).
    let _restore = RestoreMode {
        path: &p,
        mode: 0o600,
    };

    let result = load_or_init(&p);
    // The mutant survives if `load_or_init` returns Ok(State::default())
    // — the kept `Err(e) if e.kind() == NotFound` guard is the only
    // thing keeping a non-NotFound open error out of the default-state
    // arm. Pin the failure mode by demanding StateError::LoadIo (the
    // I/O-stage variant; parse/schema failures route through Load).
    let err = result.expect_err("EACCES at open(2) must NOT route to State::default()");
    match &err {
        StateError::LoadIo { path, message } => {
            assert_eq!(path, &p, "LoadIo.path must echo the offending state path");
            assert!(
                message.contains("cannot open"),
                "LoadIo.message must lead with the open-stage label; got {message:?}",
            );
            // The wrapped io::Error's Display includes "Permission denied"
            // for EACCES on Linux. Pin the substring so a future
            // `format!("{}", e)` rewrite that drops the kernel string
            // surfaces here.
            assert!(
                message.contains("Permission denied") || message.contains("permission denied"),
                "LoadIo.message must surface the permission-denied wording; got {message:?}",
            );
        }
        other => panic!("expected StateError::LoadIo, got {other:?}"),
    }
    // Pin LoadIo's Display contract: it must NOT carry the
    // "back up the file and remove it" guidance that StateError::Load
    // appends. Misleading remediation on a permission-denied error
    // would tell the operator to delete a perfectly valid state file.
    let display = err.to_string();
    assert!(
        !display.contains("back up"),
        "LoadIo Display must NOT carry the 'back up' guidance reserved \
         for parse/schema corruption; got {display:?}",
    );
    // Pin the {path} substitution in the LoadIo #[error] template
    // (src/state/mod.rs LoadIo variant). A mutation that drops the
    // path placeholder from the format string would still let the
    // match arm above pass — the display rendering is the operator-
    // visible surface, so guard it directly.
    assert!(
        display.contains(&p.display().to_string()),
        "LoadIo Display must include the offending path; got {display:?}",
    );

    // Defensive: prove State::default() is NOT what we got. The mutant
    // would silently produce State::default() — re-load on a known-good
    // chmod 0600 path and confirm the path is reachable so the EACCES
    // path is the only difference. `drop(_restore)` triggers the
    // RestoreMode Drop which chmods back to 0o600; no extra
    // set_permissions call needed.
    drop(_restore);
    let recovered = load_or_init(&p).expect("load on 0o600 must succeed");
    assert_eq!(recovered, State::default());
}

#[test]
fn load_or_init_path_is_a_directory_surfaces_as_load_io() {
    // Belt-and-suspenders: a second non-NotFound failure mode that
    // doesn't depend on chmod (so it runs under root too). Construct
    // `state.json` as a DIRECTORY: `fs::File::open` succeeds on a
    // directory on Linux, but `read_to_string` immediately fails with
    // EISDIR ("Is a directory"). The read(2) error path is a separate
    // match arm (load_or_init's read_to_string error arm) — pin it
    // too so the read-stage failure mode is also test-covered. The
    // ENOTDIR test below pins the open(2) arm under any euid.
    let td = tempfile::tempdir().expect("tempdir");
    let p = td.path().join("state.json");
    std::fs::create_dir(&p).expect("create state.json AS A DIRECTORY");
    assert!(p.is_dir(), "fixture: state.json must be a directory");

    let err = load_or_init(&p).expect_err("open(state.json/) must surface as Err");
    match err {
        StateError::LoadIo { path, message } => {
            assert_eq!(path, p);
            // On Linux, fs::File::open on a directory SUCCEEDS at
            // open(2); the failure surfaces from read_to_string with
            // EISDIR, routed through load_or_init's read_to_string
            // error arm with format "read: {e}". Pin the stage label
            // AND the kernel wording so a refactor that changes
            // either surfaces here.
            assert!(
                message.contains("read:"),
                "LoadIo.message must label the read stage; got {message:?}",
            );
            assert!(
                message.contains("Is a directory"),
                "LoadIo.message must surface the EISDIR wording; got {message:?}",
            );
        }
        other => panic!("expected StateError::LoadIo, got {other:?}"),
    }
}

#[test]
fn load_or_init_enotdir_at_open_surfaces_as_load_io_under_any_euid() {
    // Pin the open(2) error arm with a failure mode that does NOT
    // depend on DAC bypass: a regular file as an intermediate path
    // component. `fs::File::open` traverses the path; when an
    // intermediate component is a regular file, the kernel returns
    // ENOTDIR (errno 20, `io::ErrorKind::NotADirectory`). This is the
    // primary kill-the-mutant test — it runs under root, in
    // containerised CI, and on hardened tmpfs without any euid skip
    // logic.
    //
    // The mutant under test (replace `e.kind() == NotFound` match
    // guard with `true`) would route this ENOTDIR error into the
    // default-state arm and silently produce State::default() instead
    // of surfacing StateError::LoadIo. Pinning the LoadIo variant
    // (the I/O-stage variant; parse/schema failures route through
    // Load) kills the mutation regardless of the executing user.
    let td = tempfile::tempdir().expect("tempdir");
    // Create `notdir` as a regular FILE — not a directory.
    let intermediate = td.path().join("notdir");
    std::fs::write(&intermediate, b"x").expect("create regular file as intermediate component");
    // Now ask for `<tempdir>/notdir/state.json`. The kernel walks the
    // path; encountering `notdir` as a regular file (not a directory),
    // open(2) returns ENOTDIR.
    let p = intermediate.join("state.json");

    let err = load_or_init(&p).expect_err("ENOTDIR at open(2) must NOT route to State::default()");
    match err {
        StateError::LoadIo { path, message } => {
            assert_eq!(path, p, "LoadIo.path must echo the offending state path");
            assert!(
                message.contains("cannot open"),
                "LoadIo.message must lead with the open-stage label; got {message:?}",
            );
            // Pin the kernel ENOTDIR wording — Rust's std maps errno
            // 20 to "Not a directory" via libstd's strerror layer.
            assert!(
                message.contains("Not a directory") || message.contains("not a directory"),
                "LoadIo.message must surface the ENOTDIR wording; got {message:?}",
            );
        }
        other => panic!("expected StateError::LoadIo, got {other:?}"),
    }
}

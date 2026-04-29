// State file location.
// Path: $STATE_DIRECTORY/state.json. Lock: $RUNTIME_DIRECTORY/gcit.lock
// (fd-lock, tmpfs).
// State file lives at XDG_STATE_HOME, NEVER next to config.
//
// Per systemd, $STATE_DIRECTORY is automatically set when the unit declares
// StateDirectory=. For --user installs, that's
// $XDG_STATE_HOME/gcit (typically ~/.local/state/gcit). For --system,
// /var/lib/gcit.
//
// Critical: the state file MUST resolve relative to $STATE_DIRECTORY, NOT
// next to the config file. Mixing config and state breaks operator
// expectations and complicates backup (config is hand-written, state is
// machine-managed).
//
// The detailed env-driven branches (relative-path rejection, XDG fallback
// chain, parent-dir creation) are tested as in-module unit tests at
// src/state/mod.rs `tests::path_*` and `tests::lock_path_*`. Those tests
// serialize env mutation via a Mutex local to the unit-test binary.
// Integration tests run as a separate process with its OWN env state, so
// they can mutate STATE_DIRECTORY/RUNTIME_DIRECTORY safely as long as
// individual test cases serialize via `serial_test::serial`.

use serial_test::serial;
use tempfile::TempDir;

#[test]
#[serial(env_state_dir)]
fn state_path_resolves_under_state_directory_env() {
    // STATE_DIRECTORY=<tempdir> -> path returns <tempdir>/state.json AND
    // creates the parent directory if missing (per the documented
    // create_dir_all step in src/state/mod.rs::path).
    let dir = TempDir::new().unwrap();
    let abs = dir.path().to_path_buf();

    // SAFETY: serial_test::serial gates concurrent env mutation across
    // tests in this binary. The std::env::set_var safety contract
    // requires no other thread observes the env mid-mutation; the
    // serialization satisfies that for in-test threads, and the test
    // binary itself is single-threaded at this point of the body.
    unsafe {
        std::env::set_var("STATE_DIRECTORY", &abs);
        std::env::remove_var("XDG_STATE_HOME");
    }
    let p = gcit::state::path().expect("path resolves");
    assert_eq!(p, abs.join("state.json"));
    assert!(abs.is_dir(), "path() must create the parent directory");

    unsafe {
        std::env::remove_var("STATE_DIRECTORY");
    }
}

#[test]
#[serial(env_state_dir)]
fn state_path_falls_back_to_xdg_state_home_when_unset() {
    let dir = TempDir::new().unwrap();
    let xdg_root = dir.path().to_path_buf();

    unsafe {
        std::env::remove_var("STATE_DIRECTORY");
        std::env::set_var("XDG_STATE_HOME", &xdg_root);
    }
    let p = gcit::state::path().expect("XDG fallback resolves");
    assert_eq!(p, xdg_root.join("gcit").join("state.json"));

    unsafe {
        std::env::remove_var("XDG_STATE_HOME");
    }
}

#[test]
#[serial(env_runtime_dir)]
fn lock_path_resolves_under_runtime_directory_env() {
    let dir = TempDir::new().unwrap();
    let abs = dir.path().to_path_buf();

    unsafe {
        std::env::set_var("RUNTIME_DIRECTORY", &abs);
        std::env::remove_var("XDG_RUNTIME_DIR");
    }
    let p = gcit::state::lock_path().expect("lock path resolves");
    assert_eq!(p, abs.join("gcit.lock"));

    unsafe {
        std::env::remove_var("RUNTIME_DIRECTORY");
    }
}

#[test]
#[serial(env_runtime_dir)]
fn lock_path_falls_back_to_xdg_runtime_dir_when_unset() {
    let dir = TempDir::new().unwrap();
    let xdg_runtime = dir.path().to_path_buf();

    unsafe {
        std::env::remove_var("RUNTIME_DIRECTORY");
        std::env::set_var("XDG_RUNTIME_DIR", &xdg_runtime);
    }
    let p = gcit::state::lock_path().expect("XDG runtime fallback resolves");
    assert_eq!(p, xdg_runtime.join("gcit").join("gcit.lock"));

    unsafe {
        std::env::remove_var("XDG_RUNTIME_DIR");
    }
}

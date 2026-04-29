// Persistent daemon state.
//
// The state writer thread drains an mpsc (capacity 256, batched in
// groups of 64), persists via atomic-rename writes, and outlives the
// tokio runtime. `$STATE_DIRECTORY/state.json` is schema-versioned
// (schema: 1); unknown versions are refused.
//
// The gcit library is not a published API; pub items here are
// crate-internal and unstable. Integration tests link across the
// boundary so `pub(crate)` is insufficient.

pub mod apply;
pub mod writer;

pub use apply::{FlowState, RunState, State, StateUpdate, NOTIFIED_RUNS_CAP, SCHEMA_VERSION};
pub use writer::{spawn, spawn_with_mirror, BATCH_LIMIT};

use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;
use tracing::info;

/// Filename gcit reads/writes inside `$STATE_DIRECTORY` (and the
/// fallback locations resolved by `path()` for `--foreground`/dev
/// mode).
pub const STATE_FILENAME: &str = "state.json";

/// Filename of the runtime-directory single-instance lock. Lives at
/// `$RUNTIME_DIRECTORY/gcit.lock` (fd-lock, tmpfs).
pub const LOCK_FILENAME: &str = "gcit.lock";

/// Two-field probe for the schema version.
///
/// Used by `load_or_init` for a cheap first-pass deserialize that
/// pulls only the `schema` field out of the JSON. The full `State`
/// deserialize runs on the same source bytes after this probe
/// confirms the version is supported. Skipping `deny_unknown_fields`
/// here means the probe ignores the rest of the document — which is
/// exactly what we want, since the FULL deserialize is the place to
/// enforce shape.
#[derive(Debug, Deserialize)]
struct SchemaProbe {
    /// `Option` so a missing field deserializes successfully and the
    /// caller can produce a precise "missing schema" error rather
    /// than serde's generic "missing field" message.
    schema: Option<serde_json::Value>,
}

/// Errors `load_or_init` and `path` can return.
///
/// `Load` and `LoadIo` are split because their remediation paths
/// diverge:
///
/// - `Load` covers parse / schema / deserialize failures — the on-
///   disk file is structurally corrupt (truncated JSON, unsupported
///   schema, wrong field type). The message instructs the operator
///   to back up + remove + restart so the daemon starts fresh on a
///   known-good state. The trailing parenthetical makes the data-
///   loss consequence explicit so the operator does not assume the
///   daemon will recover the runs gcit was tracking before the
///   corrupt state was found.
/// - `LoadIo` covers I/O failures at open(2) or read(2) — EACCES,
///   EISDIR, ENOTDIR, or any other `io::Error` other than
///   `NotFound`. The file may be perfectly valid but inaccessible;
///   "back up + remove" is misleading guidance for these because
///   the fix is to repair filesystem permissions / path layout, not
///   to discard the contents. The message names the path + the I/O
///   error so the operator can apply the right fix (chmod, chown,
///   remove a stale directory in the way, etc.) without losing data.
#[derive(Debug, Error)]
pub enum StateError {
    #[error("STATE_DIRECTORY must be absolute, got: {0}")]
    RelativeStateDirectory(PathBuf),
    #[error("RUNTIME_DIRECTORY must be absolute, got: {0}")]
    RelativeRuntimeDirectory(PathBuf),
    #[error("cannot resolve state directory: $STATE_DIRECTORY is unset and no XDG / HOME fallback is available")]
    NoStateDirectory,
    #[error("cannot resolve runtime directory: $RUNTIME_DIRECTORY is unset and no XDG_RUNTIME_DIR fallback is available")]
    NoRuntimeDirectory,
    #[error("cannot create state directory {path}: {source}")]
    CreateStateDirectory { path: PathBuf, source: io::Error },
    #[error("cannot create runtime directory {path}: {source}")]
    CreateRuntimeDirectory { path: PathBuf, source: io::Error },
    #[error("state file at {path}: {message} (back up the file and remove it to start fresh on schema {SCHEMA_VERSION}; all flow tracking state is lost; the daemon will re-poll fresh)")]
    Load { path: PathBuf, message: String },
    #[error("state file at {path}: {message}")]
    LoadIo { path: PathBuf, message: String },
    #[error("cannot open instance lock {path}: {source}")]
    LockOpen { path: PathBuf, source: io::Error },
    #[error(
        "another gcit instance is running ({path} is already locked); \
         exit the other instance or remove the lock file if it is stale"
    )]
    LockHeld { path: PathBuf },
    #[error("instance-lock acquire on {path} failed: {source}")]
    LockAcquireFailed { path: PathBuf, source: io::Error },
}

/// Resolve the absolute path to `state.json` AND ensure the parent
/// directory exists.
///
/// Resolution order:
///   1. `$STATE_DIRECTORY/state.json` (set by systemd's
///      `StateDirectory=` directive).
///   2. `$XDG_STATE_HOME/gcit/state.json` (XDG fallback for
///      `--foreground` / dev mode).
///   3. `$HOME/.local/state/gcit/state.json` (XDG default).
///
/// Relative `$STATE_DIRECTORY` values are rejected — systemd never
/// emits a relative value, so a relative override is a misconfigured
/// dev environment. The state file lives at XDG_STATE_HOME and is
/// never placed next to the config; none of the three resolution
/// branches resolves to the config directory.
///
/// The parent directory (e.g. `$XDG_STATE_HOME/gcit/`) is created
/// with `fs::create_dir_all` if missing. systemd's `StateDirectory=`
/// already creates the system-scope directory at unit start, but
/// XDG/HOME fallbacks (foreground + dev) reach this code with no
/// directory yet. Without create_dir_all the first persist would
/// fail with ENOENT and nothing on disk would tell the operator how
/// to fix it.
pub fn path() -> Result<PathBuf, StateError> {
    let p = resolve_state_path()?;
    if let Some(parent) = p.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|e| StateError::CreateStateDirectory {
                path: parent.to_path_buf(),
                source: e,
            })?;
        }
    }
    Ok(p)
}

/// Pure path resolution. Separated from `path()` so unit tests can
/// exercise the env-driven branches without the side effect of
/// creating directories on disk.
fn resolve_state_path() -> Result<PathBuf, StateError> {
    if let Some(dir) = std::env::var_os("STATE_DIRECTORY") {
        let p = PathBuf::from(dir);
        if p.as_os_str().is_empty() {
            // Treated like "unset" — fall through to the XDG fallback.
        } else {
            if !p.is_absolute() {
                return Err(StateError::RelativeStateDirectory(p));
            }
            return Ok(p.join(STATE_FILENAME));
        }
    }
    if let Some(dir) = std::env::var_os("XDG_STATE_HOME") {
        let p = PathBuf::from(dir);
        if !p.as_os_str().is_empty() {
            return Ok(p.join("gcit").join(STATE_FILENAME));
        }
    }
    if let Some(home) = std::env::var_os("HOME") {
        let h = PathBuf::from(home);
        if !h.as_os_str().is_empty() {
            return Ok(h
                .join(".local")
                .join("state")
                .join("gcit")
                .join(STATE_FILENAME));
        }
    }
    Err(StateError::NoStateDirectory)
}

/// Resolve the absolute path to the single-instance lock file.
///
/// Resolution order:
///   1. `$RUNTIME_DIRECTORY/gcit.lock` (systemd `RuntimeDirectory=`).
///   2. `$XDG_RUNTIME_DIR/gcit/gcit.lock` (foreground / dev mode on
///      a per-user systemd).
///
/// Asymmetry vs `path()` (no `$HOME/.local/run/...` fallback): the
/// runtime directory is intentionally per-session and tmpfs-backed,
/// so it lives on whatever ephemeral filesystem the session manager
/// provides. Falling back to `$HOME` would put the lock on durable
/// storage, defeating the "single-instance per session" semantics
/// that systemd's `RuntimeDirectory=` provides — a stale lock file
/// would survive a crash and block the next start. With no
/// `XDG_RUNTIME_DIR` set, refusing to start is the correct outcome:
/// the operator must run inside a systemd-managed session or
/// supply the env var explicitly.
///
/// Relative paths are rejected for the same reason as `path()`.
pub fn lock_path() -> Result<PathBuf, StateError> {
    if let Some(dir) = std::env::var_os("RUNTIME_DIRECTORY") {
        let p = PathBuf::from(dir);
        if p.as_os_str().is_empty() {
            // Fall through to XDG fallback.
        } else {
            if !p.is_absolute() {
                return Err(StateError::RelativeRuntimeDirectory(p));
            }
            return Ok(p.join(LOCK_FILENAME));
        }
    }
    if let Some(dir) = std::env::var_os("XDG_RUNTIME_DIR") {
        let p = PathBuf::from(dir);
        if !p.as_os_str().is_empty() {
            return Ok(p.join("gcit").join(LOCK_FILENAME));
        }
    }
    Err(StateError::NoRuntimeDirectory)
}

/// Open the single-instance lock file at `path`, creating the file
/// (mode 0600) and any missing parent directory along the way.
/// Returns the wrapped `fd_lock::RwLock<File>` so the caller can
/// take the exclusive write guard for the daemon's lifetime via
/// `lock.try_write()`.
///
/// Why this helper is split from the actual `try_write` call:
/// `RwLockWriteGuard` borrows from the `RwLock`. The supervisor
/// stores the `RwLock<File>` on its stack frame and calls
/// `try_write` itself so the guard's borrow lifetime matches the
/// `run()` scope. A helper that returned the guard would either
/// have to leak the `RwLock` (boxed and `mem::forget`'d) or use a
/// self-referential pattern.
///
/// The file is opened with create-if-missing semantics: an empty
/// gcit.lock file always exists in `$RUNTIME_DIRECTORY`, and the
/// flock is held against this fd. `RuntimeDirectory=` (systemd)
/// is tmpfs-backed, so a stale lock file does not survive a host
/// reboot. The XDG fallback (`$XDG_RUNTIME_DIR/gcit/gcit.lock`)
/// is also tmpfs-backed on systemd-user setups.
pub fn open_instance_lock_file(path: &Path) -> Result<fd_lock::RwLock<fs::File>, StateError> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|e| StateError::CreateRuntimeDirectory {
                path: parent.to_path_buf(),
                source: e,
            })?;
        }
    }
    use std::os::unix::fs::OpenOptionsExt;
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(path)
        .map_err(|e| StateError::LockOpen {
            path: path.to_path_buf(),
            source: e,
        })?;
    Ok(fd_lock::RwLock::new(file))
}

/// Load `state.json` from `path`, or return `Ok(State::default())` on
/// ENOENT. Other errors split by remediation path:
///   - I/O failure at open(2) or read(2) (EACCES, EISDIR, ENOTDIR,
///     etc.) -> `StateError::LoadIo`. The file may be valid but
///     inaccessible; the operator's fix is filesystem repair (chmod,
///     chown, remove an obstructing directory).
///   - Parse failure, unknown schema, missing schema field, wrong
///     type, or deserialize drift -> `StateError::Load`. The on-disk
///     contents are corrupt; the operator's fix is to back up the
///     file and remove it so the daemon starts fresh on a known-good
///     state.
///
/// state.json is schema-versioned (schema: 1); unknown versions are
/// refused.
///
/// The validation runs in two passes:
///   1. A cheap `SchemaProbe` deserialize pulls only the `schema`
///      field; this lets us produce a precise "missing schema",
///      "wrong type", or "unsupported version" error before paying
///      the cost of decoding the full document. The probe ignores
///      every other field (no `deny_unknown_fields`) so drift in
///      sibling fields doesn't break the version check.
///   2. The full `State` deserialize runs only after the probe
///      confirms `schema == SCHEMA_VERSION`. `State` carries
///      `deny_unknown_fields` so any drift surfaces here, not as a
///      silent quiet success.
///
/// Two parses; the first is cheap (one integer field).
pub fn load_or_init(path: &Path) -> Result<State, StateError> {
    let mut f = match fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            info!(
                target: "gcit::state",
                path = %path.display(),
                "state.json not found; starting fresh on schema {}", SCHEMA_VERSION,
            );
            return Ok(State::default());
        }
        Err(e) => {
            return Err(StateError::LoadIo {
                path: path.to_path_buf(),
                message: format!("cannot open: {}", e),
            });
        }
    };
    let mut buf = String::new();
    if let Err(e) = f.read_to_string(&mut buf) {
        return Err(StateError::LoadIo {
            path: path.to_path_buf(),
            message: format!("read: {}", e),
        });
    }

    // Pass 1: cheap schema probe. SchemaProbe pulls only the `schema`
    // field (Option) so a missing field deserializes successfully and
    // we can produce a precise "missing schema" error rather than
    // serde's generic "missing field" wording.
    let probe: SchemaProbe = serde_json::from_str(&buf).map_err(|e| StateError::Load {
        path: path.to_path_buf(),
        message: format!("parse JSON: {}", e),
    })?;
    let raw_schema = probe.schema.ok_or_else(|| StateError::Load {
        path: path.to_path_buf(),
        message: "missing required field `schema` (was the file written by an older gcit?)".into(),
    })?;
    let n = raw_schema.as_u64().ok_or_else(|| StateError::Load {
        path: path.to_path_buf(),
        message: format!(
            "field `schema` must be an unsigned integer, got: {} ({})",
            raw_schema,
            raw_schema
                .as_str()
                .map_or_else(|| "non-string".to_string(), |s| format!("string {:?}", s)),
        ),
    })?;
    if u32::try_from(n) != Ok(SCHEMA_VERSION) {
        return Err(StateError::Load {
            path: path.to_path_buf(),
            message: format!(
                "schema version {} is unsupported; this gcit only handles schema {}",
                n, SCHEMA_VERSION,
            ),
        });
    }

    // Pass 2: full deserialize with deny_unknown_fields. The schema
    // field is re-read by serde here (cheap — it's already in the
    // arena); the probe above already proved it's correct.
    let state: State = serde_json::from_str(&buf).map_err(|e| StateError::Load {
        path: path.to_path_buf(),
        message: format!("deserialize state: {}", e),
    })?;
    info!(
        target: "gcit::state",
        path = %path.display(),
        flows = state.flows.len(),
        "state loaded",
    );
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Env-mutating tests are serialized so that one test setting
    /// STATE_DIRECTORY does not race with another one reading it.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn path_uses_state_directory_when_absolute() {
        let _g = ENV_LOCK.lock().unwrap();
        // SAFETY: this binary holds a Mutex across env mutations and
        // matching reads, so no other thread observes the env mid-mutation.
        unsafe {
            std::env::set_var("STATE_DIRECTORY", "/var/lib/gcit");
            std::env::remove_var("XDG_STATE_HOME");
            std::env::remove_var("HOME");
        }
        assert_eq!(
            resolve_state_path().unwrap(),
            PathBuf::from("/var/lib/gcit/state.json"),
        );
    }

    #[test]
    fn path_rejects_relative_state_directory() {
        let _g = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::set_var("STATE_DIRECTORY", "relative/path");
        }
        match resolve_state_path() {
            Err(StateError::RelativeStateDirectory(p)) => {
                assert_eq!(p, PathBuf::from("relative/path"));
            }
            other => panic!("expected RelativeStateDirectory, got {:?}", other),
        }
        unsafe {
            std::env::remove_var("STATE_DIRECTORY");
        }
    }

    #[test]
    fn path_creates_parent_directory() {
        // path() must create the parent directory so the first persist
        // after a fresh start does not fail ENOENT. Use a temp
        // directory + a subpath that does not yet exist so we can
        // observe the side effect cleanly.
        let _g = ENV_LOCK.lock().unwrap();
        let td = tempfile::tempdir().unwrap();
        let state_dir = td.path().join("nested").join("gcit-state");
        unsafe {
            std::env::set_var("STATE_DIRECTORY", &state_dir);
            std::env::remove_var("XDG_STATE_HOME");
            std::env::remove_var("HOME");
        }
        assert!(!state_dir.exists(), "fixture: target dir must start absent");
        let p = path().unwrap();
        assert_eq!(p, state_dir.join("state.json"));
        assert!(state_dir.is_dir(), "path() must create parent directory");
        unsafe {
            std::env::remove_var("STATE_DIRECTORY");
        }
    }

    #[test]
    fn path_falls_back_to_xdg_state_home() {
        let _g = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::remove_var("STATE_DIRECTORY");
            std::env::set_var("XDG_STATE_HOME", "/tmp/xdg/state");
        }
        assert_eq!(
            resolve_state_path().unwrap(),
            PathBuf::from("/tmp/xdg/state/gcit/state.json"),
        );
        unsafe {
            std::env::remove_var("XDG_STATE_HOME");
        }
    }

    #[test]
    fn path_falls_back_to_home_dot_local_state() {
        let _g = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::remove_var("STATE_DIRECTORY");
            std::env::remove_var("XDG_STATE_HOME");
            std::env::set_var("HOME", "/home/operator");
        }
        assert_eq!(
            resolve_state_path().unwrap(),
            PathBuf::from("/home/operator/.local/state/gcit/state.json"),
        );
        unsafe {
            std::env::remove_var("HOME");
        }
    }

    #[test]
    fn lock_path_uses_runtime_directory() {
        let _g = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::set_var("RUNTIME_DIRECTORY", "/run/gcit");
            std::env::remove_var("XDG_RUNTIME_DIR");
        }
        assert_eq!(lock_path().unwrap(), PathBuf::from("/run/gcit/gcit.lock"));
        unsafe {
            std::env::remove_var("RUNTIME_DIRECTORY");
        }
    }

    #[test]
    fn lock_path_rejects_relative_runtime_directory() {
        let _g = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::set_var("RUNTIME_DIRECTORY", "rel/run");
            std::env::remove_var("XDG_RUNTIME_DIR");
        }
        assert!(matches!(
            lock_path(),
            Err(StateError::RelativeRuntimeDirectory(_))
        ));
        unsafe {
            std::env::remove_var("RUNTIME_DIRECTORY");
        }
    }

    #[test]
    fn load_or_init_returns_default_when_missing() {
        let td = tempfile::tempdir().unwrap();
        let p = td.path().join("state.json");
        let s = load_or_init(&p).unwrap();
        assert_eq!(s, State::default());
    }

    #[test]
    fn load_or_init_round_trip_via_writer() {
        let td = tempfile::tempdir().unwrap();
        let p = td.path().join("state.json");
        let mut s = State::default();
        s.flows.insert(
            "f".into(),
            FlowState {
                last_sha: Some("aa".repeat(20)),
                last_poll_at: None,
                last_dispatched_at: None,
                cooldown_until: None,
                active_runs: vec![],
                notified_runs: vec![],
            },
        );
        crate::util::atomic_write_json(&p, &s, 0o600).unwrap();
        let loaded = load_or_init(&p).unwrap();
        assert_eq!(loaded, s);
    }

    #[test]
    fn load_or_init_rejects_unknown_schema() {
        let td = tempfile::tempdir().unwrap();
        let p = td.path().join("state.json");
        std::fs::write(&p, r#"{"schema": 99, "flows": {}}"#).unwrap();
        let err = load_or_init(&p).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("99"),
            "error must name encountered version 99: {msg}",
        );
        assert!(
            msg.contains("schema 1"),
            "error must name supported version 1: {msg}",
        );
        assert!(
            msg.contains("back up"),
            "error must guide operator to back up + remove: {msg}",
        );
        // The error must spell out the data-loss consequence so the
        // operator does not assume the daemon recovers prior tracking
        // state on its own.
        assert!(
            msg.contains("re-poll fresh") || msg.contains("tracking state is lost"),
            "error must warn that tracking state is lost: {msg}",
        );
    }

    #[test]
    fn load_or_init_rejects_missing_schema() {
        let td = tempfile::tempdir().unwrap();
        let p = td.path().join("state.json");
        std::fs::write(&p, r#"{"flows": {}}"#).unwrap();
        let err = load_or_init(&p).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("schema"), "error must mention schema: {msg}");
        // Pin the operator-facing hint on missing-schema so a
        // mutation that drops it surfaces.
        assert!(
            msg.contains("missing required field"),
            "error must label the failure mode (missing required field): {msg}",
        );
        assert!(
            msg.contains("older gcit"),
            "error must hint at the migration cause (older gcit): {msg}",
        );
        // The Load variant always includes the path so editors / grep
        // can navigate from terminal output.
        assert!(
            msg.contains(p.to_string_lossy().as_ref()),
            "error must include the path: {msg}",
        );
    }

    #[test]
    fn load_or_init_unknown_schema_message_includes_offending_path() {
        // The Load Display includes the path. Pin that so a mutation
        // dropping the path surfaces here.
        let td = tempfile::tempdir().unwrap();
        let p = td.path().join("state.json");
        std::fs::write(&p, r#"{"schema": 99, "flows": {}}"#).unwrap();
        let err = load_or_init(&p).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains(p.to_string_lossy().as_ref()),
            "error must name the offending file: {msg}",
        );
        assert!(
            msg.contains("unsupported"),
            "error must label version as unsupported: {msg}",
        );
    }

    #[test]
    fn load_or_init_string_schema_includes_offending_value_in_message() {
        // The "integer" rejection message includes the offending raw
        // value so the operator sees what was on disk.
        let td = tempfile::tempdir().unwrap();
        let p = td.path().join("state.json");
        std::fs::write(&p, r#"{"schema": "abc-version", "flows": {}}"#).unwrap();
        let err = load_or_init(&p).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("abc-version"),
            "error must name the offending value: {msg}",
        );
        assert!(
            msg.contains("integer"),
            "error must say the field must be integer: {msg}",
        );
    }

    #[test]
    fn load_or_init_rejects_string_schema() {
        let td = tempfile::tempdir().unwrap();
        let p = td.path().join("state.json");
        std::fs::write(&p, r#"{"schema": "1", "flows": {}}"#).unwrap();
        let err = load_or_init(&p).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("schema") && msg.contains("integer"),
            "error must mention schema + integer: {msg}",
        );
    }

    #[test]
    fn load_or_init_rejects_truncated_json() {
        let td = tempfile::tempdir().unwrap();
        let p = td.path().join("state.json");
        std::fs::write(&p, r#"{"schema": 1, "fl"#).unwrap();
        let err = load_or_init(&p).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("parse"), "error must mention parse: {msg}");
    }

    #[test]
    fn load_or_init_rejects_unknown_top_level_field() {
        let td = tempfile::tempdir().unwrap();
        let p = td.path().join("state.json");
        std::fs::write(&p, r#"{"schema": 1, "flows": {}, "future_field": 7}"#).unwrap();
        // deny_unknown_fields rejects in serde. Surface as Load.
        let err = load_or_init(&p).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("future_field") || msg.contains("unknown"),
            "error must name unknown field: {msg}",
        );
    }
}

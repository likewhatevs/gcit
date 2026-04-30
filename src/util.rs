// Shared utilities reused across modules.
//
// Currently: atomic-write helpers used by both the install manifest
// (src/cli/install.rs) and the state writer (src/state/writer.rs).
// The pattern is identical in both call sites — tempfile in the
// destination's parent directory + write contents + set permissions +
// sync_all + persist (rename) + fsync the parent directory — and
// keeping a single implementation here prevents the two call sites
// from drifting on durability semantics.

use std::fs;
use std::io::{self, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use serde::Serialize;

/// Atomic durable write of `contents` at `dest` with mode `mode`.
///
/// Sequence:
///   1. Create a tempfile in `dest`'s parent directory (so the final
///      rename is on the same filesystem and POSIX-atomic).
///   2. Write all bytes.
///   3. Set the requested file mode.
///   4. `sync_all()` the data and metadata to the underlying device
///      BEFORE the rename — `tempfile::NamedTempFile::persist` does
///      not fsync, and a power loss between the unfsynced data and
///      the rename leaves an empty file at the destination.
///   5. Persist (atomic rename) to `dest`.
///   6. Open + `sync_all()` the parent directory so the rename's
///      directory entry is durable across crash. Without this step,
///      a crash between rename and the next directory-entry flush
///      can resurrect the prior file (or no file at all on first
///      install).
///
/// Returns the underlying `io::Error` for any step that fails. The
/// tempfile is automatically cleaned up by `tempfile`'s Drop on the
/// happy path; on failure, the destination is unchanged.
pub fn atomic_write(dest: &Path, contents: &[u8], mode: u32) -> io::Result<()> {
    let parent = dest
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let mut tf = tempfile::Builder::new()
        .prefix(".gcit-atomic-")
        .tempfile_in(&parent)?;
    tf.write_all(contents)?;
    tf.as_file_mut()
        .set_permissions(fs::Permissions::from_mode(mode))?;
    tf.as_file_mut().sync_all()?;
    tf.persist(dest).map_err(|e| e.error)?;
    let dir = fs::File::open(&parent)?;
    dir.sync_all()?;
    Ok(())
}

/// Atomic durable write of `value` serialized as pretty JSON at
/// `dest`. Wraps `atomic_write` so callers can persist a `Serialize`
/// type in one step.
///
/// `mode` is the POSIX mode the target file is created with. Default
/// for state files is `0o600` (owner-only); install manifests use
/// `0o600` for the same reason.
pub fn atomic_write_json<T: Serialize>(dest: &Path, value: &T, mode: u32) -> io::Result<()> {
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|e| io::Error::other(format!("serialize: {}", e)))?;
    atomic_write(dest, &bytes, mode)
}

/// Install the rustls `ring` provider once across the entire test
/// process. Tests that build a `github::client::Client` (octocrab's
/// hyper-rustls underpinning), a `discord::webhook::Client` (twilight-
/// http's hyper-rustls underpinning), or any other code that goes
/// through hyper-rustls during the test process must call this first
/// — the binary entry installs the provider from `main()`, but unit
/// tests do not. `std::sync::Once` makes the call idempotent across
/// every test in the lib-test crate. `install_default` returns Err if
/// a provider is already installed; treat that as success (another
/// test installed first).
///
/// `#[cfg(test)] pub` so every `#[cfg(test)] mod tests` block in this
/// crate can call `crate::util::ensure_crypto_provider` rather than
/// duplicating the body. Integration tests under `tests/<name>.rs` are
/// separate crates per Cargo and cannot reach this symbol; those use
/// the matching helper at tests/common/mod.rs.
#[cfg(test)]
pub fn ensure_crypto_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn atomic_write_creates_file_with_mode() {
        let td = tempfile::tempdir().unwrap();
        let dest = td.path().join("out.txt");
        atomic_write(&dest, b"hello", 0o600).unwrap();
        assert_eq!(fs::read(&dest).unwrap(), b"hello");
        let mode = fs::metadata(&dest).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn atomic_write_replaces_existing_file() {
        let td = tempfile::tempdir().unwrap();
        let dest = td.path().join("out.txt");
        atomic_write(&dest, b"first", 0o600).unwrap();
        atomic_write(&dest, b"second", 0o600).unwrap();
        assert_eq!(fs::read(&dest).unwrap(), b"second");
    }

    #[test]
    fn atomic_write_leaves_no_tempfile() {
        let td = tempfile::tempdir().unwrap();
        let dest = td.path().join("out.txt");
        atomic_write(&dest, b"content", 0o644).unwrap();
        let leftovers: Vec<_> = fs::read_dir(td.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name() != "out.txt")
            .collect();
        assert!(
            leftovers.is_empty(),
            "atomic_write must clean up tempfiles; found: {:?}",
            leftovers.iter().map(|e| e.file_name()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn atomic_write_json_round_trip() {
        let td = tempfile::tempdir().unwrap();
        let dest = td.path().join("v.json");
        let value = serde_json::json!({"a": 1, "b": [2, 3]});
        atomic_write_json(&dest, &value, 0o600).unwrap();
        let on_disk: serde_json::Value = serde_json::from_slice(&fs::read(&dest).unwrap()).unwrap();
        assert_eq!(on_disk, value);
    }

    #[test]
    fn atomic_write_emits_pretty_json_with_newlines() {
        // serde_json::to_vec_pretty produces multiline JSON. Pin
        // that the json variant uses pretty-printing rather than
        // compact: a mutation flipping the function to to_vec
        // (compact) would still round-trip via from_slice but
        // would change the on-disk readability operators rely on.
        let td = tempfile::tempdir().unwrap();
        let dest = td.path().join("v.json");
        let value = serde_json::json!({"a": 1, "b": [2, 3]});
        atomic_write_json(&dest, &value, 0o600).unwrap();
        let raw = fs::read_to_string(&dest).unwrap();
        assert!(
            raw.contains('\n'),
            "atomic_write_json must use pretty-printing (multiline): {raw:?}",
        );
    }

    #[test]
    #[serial]
    fn atomic_write_uses_current_dir_when_dest_has_no_parent() {
        // When `dest` is a bare filename (no parent path component),
        // `Path::parent` returns Some("") which the helper coerces
        // to "." via the `filter(!is_empty()).unwrap_or_else(|| ".")`
        // fallback. Mutations that flip the negation or drop the
        // filter would write the tempfile in the wrong directory.
        //
        // We don't actually want to write in CWD during tests, so
        // pin the equivalent: tempdir as CWD, then a bare filename.
        //
        // `#[serial]` is mandatory: this test mutates the process-wide
        // CWD via `set_current_dir`, which races every other test that
        // resolves a relative path (or reads CWD). Without
        // serialization a parallel test could observe the tempdir CWD
        // and write its own outputs into a directory that's about to
        // be removed, producing flakes.
        let td = tempfile::tempdir().unwrap();
        let prev = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(td.path()).expect("chdir");
        let result = atomic_write(Path::new("bare.txt"), b"x", 0o600);
        let restored = std::env::set_current_dir(&prev);
        assert!(
            result.is_ok(),
            "bare filename must write to CWD: {result:?}"
        );
        assert!(td.path().join("bare.txt").exists());
        restored.expect("restore cwd");
    }

    #[test]
    fn atomic_write_mode_0644_overrides_default() {
        // Pin the mode parameter is honoured (not hardcoded to 0o600).
        // A mutation that ignores the parameter and always writes 0o600
        // surfaces as a permission-bits mismatch.
        let td = tempfile::tempdir().unwrap();
        let dest = td.path().join("644.txt");
        atomic_write(&dest, b"x", 0o644).unwrap();
        let mode = fs::metadata(&dest).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o644, "mode parameter must be honoured");
    }

    #[test]
    fn atomic_write_propagates_io_error_when_parent_missing() {
        // The parent directory must exist before atomic_write.
        // tempfile::Builder::tempfile_in returns ENOENT when the
        // parent doesn't exist. Pin that the error propagates with
        // a meaningful kind rather than silently succeeding.
        let td = tempfile::tempdir().unwrap();
        let dest = td
            .path()
            .join("nested")
            .join("does-not-exist")
            .join("x.txt");
        let err =
            atomic_write(&dest, b"x", 0o600).expect_err("missing parent must surface as I/O error");
        // The exact ErrorKind varies (NotFound vs Other) by tempfile
        // version, but the system error message always mentions
        // "No such file or directory" or carries the
        // path. Pin presence of either signal so a mutation that
        // squashes the error to Ok(()) surfaces.
        let msg = format!("{err}");
        assert!(
            err.kind() == std::io::ErrorKind::NotFound
                || msg.contains("No such file")
                || msg.contains("not found"),
            "missing-parent error must surface I/O failure; got kind={:?}, msg={msg}",
            err.kind(),
        );
    }
}

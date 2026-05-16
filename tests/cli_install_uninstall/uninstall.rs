// Uninstall lifecycle: happy path, modified-file refusal, --force
// override, schema-version mismatch, path-traversal rejection,
// pre-deleted files, symlink defense, and manifest-entries-missing.

use std::os::unix::fs::symlink;
use std::os::unix::fs::PermissionsExt;

use predicates::prelude::*;
use tempfile::TempDir;

use super::common::{
    isolated_command, run_user_install, sha256_hex, user_scope_paths, write_minimal_config,
};

#[test]
fn uninstall_user_after_successful_install_removes_all_managed_files_and_exits_zero() {
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let paths = user_scope_paths(td.path());

    run_user_install(td.path(), &cfg);
    // Every managed file is on disk after install.
    assert!(paths.service_unit.exists());
    assert!(paths.socket_unit.exists());
    assert!(paths.config.exists());
    assert!(paths.manifest.exists());

    isolated_command(td.path())
        .arg("uninstall")
        .arg("--user")
        .assert()
        .code(0)
        .stdout(predicate::str::contains("removed"));

    // Every managed file must be gone after uninstall.
    assert!(
        !paths.service_unit.exists(),
        "uninstall must remove the service unit",
    );
    assert!(
        !paths.socket_unit.exists(),
        "uninstall must remove the socket unit",
    );
    assert!(
        !paths.config.exists(),
        "uninstall must remove the config copy",
    );
    assert!(
        !paths.manifest.exists(),
        "uninstall must remove the manifest itself last",
    );
    // State directory is preserved intentionally: the parent dir
    // still exists even after the manifest file is removed.
    assert!(
        paths.manifest.parent().expect("manifest parent").exists(),
        "uninstall must preserve the state directory parent",
    );
}

#[test]
fn uninstall_user_after_install_with_modified_file_without_force_refuses_with_oserr_71() {
    // cli::uninstall::run refuses to remove an operator-modified
    // file without --force, exiting 71 (EX_OSERR). The manifest sha
    // mismatch surfaces in stderr with both the recorded and on-disk
    // shas.
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let paths = user_scope_paths(td.path());

    run_user_install(td.path(), &cfg);

    // Tamper with the service unit so its on-disk sha drifts from the
    // manifest's recorded sha.
    let original = std::fs::read(&paths.service_unit).expect("service unit exists");
    let tampered = [original.as_slice(), b"\n# operator edit\n"].concat();
    std::fs::write(&paths.service_unit, &tampered).expect("write tampered file");

    isolated_command(td.path())
        .arg("uninstall")
        .arg("--user")
        .assert()
        .code(71)
        .stderr(predicate::str::contains("operator-modified files"))
        .stderr(predicate::str::contains("--force"));

    // The tampered file must still be on disk — uninstall refused to
    // remove it.
    assert!(
        paths.service_unit.exists(),
        "refused uninstall must NOT remove the tampered file",
    );
    // The manifest must also still exist for the operator's --force
    // retry.
    assert!(
        paths.manifest.exists(),
        "refused uninstall must NOT remove the manifest before completion",
    );
}

#[test]
fn uninstall_user_after_install_with_modified_file_force_overrides_and_exits_zero() {
    // Same setup as above but with --force. Per the comment in
    // cli::uninstall::run, --force overrides the sha-mismatch gate;
    // uninstall completes and removes the tampered file.
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let paths = user_scope_paths(td.path());

    run_user_install(td.path(), &cfg);

    let original = std::fs::read(&paths.service_unit).expect("service unit exists");
    let tampered = [original.as_slice(), b"\n# operator edit\n"].concat();
    std::fs::write(&paths.service_unit, &tampered).expect("write tampered file");

    isolated_command(td.path())
        .arg("uninstall")
        .arg("--user")
        .arg("--force")
        .assert()
        .code(0);

    assert!(
        !paths.service_unit.exists(),
        "--force uninstall must remove even tampered files",
    );
    assert!(!paths.manifest.exists());
}

#[test]
fn uninstall_user_with_schema_version_mismatch_exits_oserr_71() {
    // cli::uninstall::run rejects a manifest whose schema_version
    // differs from MANIFEST_SCHEMA_VERSION (currently 1 per
    // cli::install). EX_OSERR=71 distinguishes this from the
    // manifest-not-found case (EX_CONFIG=78).
    let td = TempDir::new().unwrap();
    let paths = user_scope_paths(td.path());

    // Hand-write a manifest with schema_version=999 — guaranteed not
    // to match. Empty `files` keeps the path-traversal scan trivial.
    std::fs::create_dir_all(paths.manifest.parent().unwrap()).unwrap();
    let bad_manifest = r#"{"schema_version":999,"files":[]}"#;
    std::fs::write(&paths.manifest, bad_manifest).unwrap();

    isolated_command(td.path())
        .arg("uninstall")
        .arg("--user")
        .assert()
        .code(71)
        .stderr(predicate::str::contains("schema_version"))
        .stderr(predicate::str::contains("999"));
}

#[test]
fn uninstall_user_with_path_outside_install_roots_exits_oserr_71() {
    // cli::uninstall::run refuses any manifest entry that
    // canonicalizes outside the expected_roots derived from
    // install_paths. Hand-write a manifest pointing at a tempdir-rooted
    // file OUTSIDE the user-scope managed dirs (e.g. directly under
    // $HOME) so canonicalize succeeds + the prefix check fails.
    let td = TempDir::new().unwrap();
    let paths = user_scope_paths(td.path());

    // Place a file the manifest will reference, somewhere INSIDE the
    // tempdir but OUTSIDE every user-scope managed dir
    // ($HOME/<not-managed>/sentinel.txt).
    let outside = td.path().join("sentinel.txt");
    std::fs::write(&outside, b"sentinel content").unwrap();

    // SHA-256 of "sentinel content" so the sha gate doesn't fire
    // first. canonicalize on `outside` resolves; the resulting path
    // does not start with any of the expected_roots — uninstall.rs's
    // for loop in validate_manifest_path runs through every root and
    // returns Err.
    let sha = sha256_hex(b"sentinel content");
    std::fs::create_dir_all(paths.manifest.parent().unwrap()).unwrap();
    let manifest_json = format!(
        r#"{{"schema_version":1,"files":[{{"path":{:?},"sha256":{:?},"mode":420}}]}}"#,
        outside.to_string_lossy(),
        sha,
    );
    std::fs::write(&paths.manifest, manifest_json).unwrap();
    // Permissions don't matter for path validation but stay readable.
    std::fs::set_permissions(&paths.manifest, std::fs::Permissions::from_mode(0o600)).unwrap();

    isolated_command(td.path())
        .arg("uninstall")
        .arg("--user")
        .assert()
        .code(71)
        .stderr(predicate::str::contains("manifest entry rejected"))
        .stderr(predicate::str::contains("outside the install directories"));

    // Path traversal defense MUST NOT remove the offending file even
    // though `--force` was not passed.
    assert!(
        outside.exists(),
        "path-traversal rejection must NOT remove anything",
    );
}

#[test]
fn uninstall_user_with_pre_deleted_managed_file_succeeds_and_removes_remaining() {
    // cli::uninstall::run treats "file already gone" as a no-op
    // rather than an error. The remaining managed files are removed
    // normally, the manifest is removed last, and exit is 0.
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let paths = user_scope_paths(td.path());

    run_user_install(td.path(), &cfg);
    // Operator manually removes the socket unit BEFORE running
    // uninstall. The manifest still references it; the sha-mismatch
    // gate sees `path.exists() == false` and skips the file via
    // `continue`, so no "operator-modified" error fires.
    assert!(paths.socket_unit.exists());
    std::fs::remove_file(&paths.socket_unit).expect("remove pre-uninstall");
    assert!(!paths.socket_unit.exists(), "pre-deletion staged");

    isolated_command(td.path())
        .arg("uninstall")
        .arg("--user")
        .assert()
        .code(0);

    // Remaining managed files are gone too — the remove loop in
    // cli::uninstall::run removes every entry whose path still
    // exists.
    assert!(
        !paths.service_unit.exists(),
        "uninstall must remove the still-present service unit",
    );
    assert!(
        !paths.config.exists(),
        "uninstall must remove the config copy",
    );
    assert!(
        !paths.manifest.exists(),
        "uninstall must remove the manifest itself",
    );
}

#[test]
fn uninstall_user_with_managed_file_replaced_by_symlink_rejected_with_oserr_71() {
    // The symlink-defense arm inside validate_manifest_path rejects
    // any manifest entry where `symlink_metadata` reports a symlink,
    // EVEN when --force is passed. The recorded sha256 was over the
    // original file content — following the symlink would let an
    // attacker substitute arbitrary content for the manifest's
    // intended target.
    let td = TempDir::new().unwrap();
    let cfg = write_minimal_config(td.path());
    let paths = user_scope_paths(td.path());

    run_user_install(td.path(), &cfg);
    // Replace the service unit with a symlink. The manifest still
    // points at the path. validate_manifest_path's
    // symlink_metadata branch fires and emits the
    // "manifest entry rejected: ...: symlink ..." stderr.
    assert!(paths.service_unit.exists());
    std::fs::remove_file(&paths.service_unit).unwrap();
    let target = td.path().join("symlink-target.txt");
    std::fs::write(&target, b"attacker-controlled content").unwrap();
    symlink(&target, &paths.service_unit).expect("create symlink");

    // --force is OFF so a sha mismatch would fire ANYWAY; the
    // symlink defense fires FIRST per the order in cli::uninstall::run
    // (path validation runs before sha verification).
    isolated_command(td.path())
        .arg("uninstall")
        .arg("--user")
        .assert()
        .code(71)
        .stderr(predicate::str::contains("manifest entry rejected"))
        .stderr(predicate::str::contains("symlink"));

    // Symlink defense MUST NOT remove the symlink itself or the
    // attacker-controlled target.
    assert!(
        paths.service_unit.exists(),
        "symlink-rejection must not delete the symlink",
    );
    assert!(
        target.exists(),
        "symlink-rejection must not delete the symlink target",
    );
    // The manifest must still be present so a follow-up uninstall
    // (e.g. after the operator restores the file) can complete.
    assert!(
        paths.manifest.exists(),
        "symlink-rejection must not remove the manifest before completion",
    );
}

#[test]
fn uninstall_user_with_manifest_entries_inside_roots_but_missing_succeeds() {
    // The path-validation branch in validate_manifest_path
    // (`Err(_) if !path.exists() => return Ok(())`) returns Ok for
    // paths that don't canonicalize but also don't exist. The
    // sha-verify loop then skips the entry via
    // `if !entry.path.exists() { continue; }`. Net behavior: missing
    // files inside expected_roots are no-ops and the uninstall
    // succeeds.
    let td = TempDir::new().unwrap();
    let paths = user_scope_paths(td.path());

    // Build a manifest by hand. Every entry's path canonicalizes to
    // somewhere INSIDE the user-scope managed dirs (units_dir, config
    // dir), but the files themselves do not exist on disk.
    std::fs::create_dir_all(paths.manifest.parent().unwrap()).unwrap();
    // We need expected_roots to canonicalize successfully. Pre-create
    // the parent dirs of the managed files so canonicalize on them
    // returns Ok inside expected_roots().
    std::fs::create_dir_all(paths.service_unit.parent().unwrap()).unwrap();
    std::fs::create_dir_all(paths.config.parent().unwrap()).unwrap();

    let mock_sha = sha256_hex(b"ignored");
    let manifest_json = format!(
        r#"{{"schema_version":1,"files":[{{"path":{:?},"sha256":{:?},"mode":420}},{{"path":{:?},"sha256":{:?},"mode":420}}]}}"#,
        paths.service_unit.to_string_lossy(),
        mock_sha,
        paths.config.to_string_lossy(),
        mock_sha,
    );
    std::fs::write(&paths.manifest, manifest_json).unwrap();
    std::fs::set_permissions(&paths.manifest, std::fs::Permissions::from_mode(0o600)).unwrap();

    isolated_command(td.path())
        .arg("uninstall")
        .arg("--user")
        .assert()
        .code(0);

    // Manifest itself is removed at the end of cli::uninstall::run.
    assert!(
        !paths.manifest.exists(),
        "uninstall must remove the manifest after a missing-files run",
    );
}

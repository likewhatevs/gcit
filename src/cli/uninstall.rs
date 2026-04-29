// `gcit uninstall` — manifest-driven removal.
//
// Behavior:
//   - Read the manifest at $STATE_DIRECTORY/.install-manifest.json
//     (resolved via `install_paths(scope, $HOME)`).
//   - For each file: validate the path is inside one of the expected
//     install directories (canonicalize parents and prefix-check).
//     Refuse with EX_OSERR on any escape — prevents a tampered
//     manifest from steering `sudo gcit uninstall` into removing
//     arbitrary files.
//   - For each file: if its on-disk sha256 differs from the manifest,
//     refuse to remove (exit 71 EX_OSERR) unless `--force`.
//   - Files NOT in the manifest are NEVER touched.
//   - State directory is preserved.
//   - daemon-reload via the user session bus, or hint for --system.
//   - If the manifest has `user_required: "gcit"`, print the userdel
//     reminder (gcit never created the account itself; only the
//     operator's matching `useradd` from install time should be
//     reversed if the operator decides to).

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use crate::cli::{exit, install};
use crate::systemd::{
    install_paths, trigger_daemon_reload, InstallPaths, InstallScope, ReloadOutcome,
};

/// Run `gcit uninstall`. Returns the exit code.
///
/// Async because daemon-reload talks to the user session bus via
/// zbus. Callers run inside the binary's tokio runtime.
pub async fn run(scope: InstallScope, force: bool) -> ExitCode {
    let home = match install::home_dir() {
        Some(h) => h,
        None => {
            eprintln!("gcit uninstall: cannot resolve $HOME");
            return ExitCode::from(exit::OSERR);
        }
    };
    let paths = install_paths(scope, &home);

    let manifest = match install::read_manifest(&paths.manifest) {
        Ok(m) => m,
        Err(e) => {
            eprintln!(
                "gcit uninstall: cannot read manifest at {}: {}",
                paths.manifest.display(),
                e
            );
            return ExitCode::from(exit::CONFIG);
        }
    };
    if manifest.schema_version != install::MANIFEST_SCHEMA_VERSION {
        eprintln!(
            "gcit uninstall: manifest schema_version {} != supported {}; refusing to act on a manifest from a different gcit version",
            manifest.schema_version, install::MANIFEST_SCHEMA_VERSION
        );
        return ExitCode::from(exit::OSERR);
    }

    // Path-traversal defense: every entry.path must resolve under one
    // of the expected install directories. A tampered manifest pointing
    // at /etc/passwd or /home/<other>/... is rejected before any
    // removal happens. Even with --force, the gate stays — the
    // operator overrides sha mismatch, not path safety. Symlinks are
    // rejected too: a manifest entry that points to a symlink should
    // be refused regardless of where the link resolves, because the
    // manifest's recorded sha256 was over the file content, not the
    // link target's content.
    let allow_roots = expected_roots(&paths);
    for entry in &manifest.files {
        if let Err(e) = validate_manifest_path(&entry.path, &allow_roots) {
            eprintln!(
                "gcit uninstall: manifest entry rejected: {}: {}",
                entry.path.display(),
                e
            );
            return ExitCode::from(exit::OSERR);
        }
    }

    // Verify each manifest entry's on-disk sha256 matches what was
    // recorded at install time. Any mismatch is "operator-modified"
    // and stops the uninstall unless --force.
    let mut modified: Vec<(String, String, String)> = Vec::new();
    for entry in &manifest.files {
        if !entry.path.exists() {
            // File already gone — nothing to remove. Not an error;
            // matches the manifest semantics ("paths NOT in the
            // manifest are never touched", and a path that's already
            // gone is a no-op).
            continue;
        }
        match install::file_sha256(&entry.path) {
            Ok(sha) if sha == entry.sha256 => {}
            Ok(sha) => modified.push((entry.path.display().to_string(), entry.sha256.clone(), sha)),
            Err(e) => {
                eprintln!(
                    "gcit uninstall: cannot hash {}: {}",
                    entry.path.display(),
                    e
                );
                return ExitCode::from(exit::OSERR);
            }
        }
    }
    if !modified.is_empty() && !force {
        eprintln!(
            "gcit uninstall: refusing to remove operator-modified files (re-run with --force to override):"
        );
        for (path, manifest_sha, on_disk_sha) in &modified {
            eprintln!(
                "  {} (manifest sha256={}, on-disk sha256={})",
                path, manifest_sha, on_disk_sha
            );
        }
        return ExitCode::from(exit::OSERR);
    }

    // Remove each manifest entry. The manifest itself is removed
    // last — if a file removal fails midway, the operator can re-run
    // uninstall and pick up where the previous run left off.
    let mut errors: Vec<(String, String)> = Vec::new();
    for entry in &manifest.files {
        if !entry.path.exists() {
            continue;
        }
        if let Err(e) = fs::remove_file(&entry.path) {
            errors.push((entry.path.display().to_string(), e.to_string()));
        } else {
            println!("removed {}", entry.path.display());
        }
    }
    if !errors.is_empty() {
        eprintln!("gcit uninstall: errors during removal:");
        for (p, msg) in &errors {
            eprintln!("  {}: {}", p, msg);
        }
        return ExitCode::from(exit::OSERR);
    }

    // Manifest itself is the last thing to go. If everything else
    // succeeded, drop it; the operator can `gcit install` clean from
    // here.
    if paths.manifest.exists() {
        if let Err(e) = fs::remove_file(&paths.manifest) {
            eprintln!(
                "gcit uninstall: removed all managed files but failed to remove manifest {}: {}",
                paths.manifest.display(),
                e
            );
            return ExitCode::from(exit::OSERR);
        }
        println!("removed manifest {}", paths.manifest.display());
    }

    // daemon-reload (user only — system requires sudo and is hinted).
    // Already inside the binary's tokio runtime; await directly.
    match trigger_daemon_reload(scope).await {
        Ok(ReloadOutcome::Reloaded) => {
            println!("systemd daemon-reload completed via session bus.");
        }
        Ok(ReloadOutcome::SkippedSystemRequiresRoot) => {
            // Hinted in the next-steps banner below.
        }
        Err(e) => {
            eprintln!(
                "warning: daemon-reload failed: {} (run the systemctl command from the next-steps banner manually)",
                e
            );
        }
    }

    print_post_uninstall(
        scope,
        manifest.user_required.as_deref(),
        manifest.user_created_by_install,
        &paths.manifest,
    );
    ExitCode::from(exit::OK)
}

/// Set of directory prefixes that manifest entries are allowed to
/// live under. Built from the `InstallPaths` the wizard wrote at
/// install time, so a `--user` install only authorizes user-scoped
/// directories and a `--system` install only authorizes system ones.
fn expected_roots(paths: &InstallPaths) -> Vec<PathBuf> {
    let mut roots: Vec<PathBuf> = Vec::new();
    for p in [&paths.service_unit, &paths.socket_unit, &paths.config] {
        if let Some(parent) = p.parent() {
            roots.push(parent.to_path_buf());
        }
    }
    roots
}

/// Verify `path` is a regular file living under one of `allow_roots`.
/// Both sides are compared after canonicalization — symlinks in
/// either path are followed, but `path` must canonicalize to a
/// location whose ancestor list includes one of the canonical
/// `allow_roots`. Returns the offending kind on failure so the
/// caller can include it in the operator-facing error message.
fn validate_manifest_path(path: &Path, allow_roots: &[PathBuf]) -> Result<(), String> {
    // symlink_metadata so we don't follow the link before deciding —
    // we want to refuse "manifest entry is a symlink" outright, since
    // the recorded sha256 was over the file content gcit wrote, not
    // the link target's content.
    if let Ok(meta) = fs::symlink_metadata(path) {
        if meta.file_type().is_symlink() {
            return Err(
                "symlink (manifest sha was over installed file content, not link target)".into(),
            );
        }
    }
    let canon_path = match fs::canonicalize(path) {
        Ok(p) => p,
        // If the path can't be canonicalized (e.g., it's already gone)
        // we cannot prove it's safe to remove. Reject — uninstall is
        // happy to no-op missing entries elsewhere, but a
        // not-canonicalizable path that nonetheless reports
        // `path.exists() == false` is fine. Distinguish: if it
        // genuinely doesn't exist, the removal step skips it, so
        // returning Ok is safe here.
        Err(_) if !path.exists() => return Ok(()),
        Err(e) => return Err(format!("canonicalize failed: {}", e)),
    };
    for root in allow_roots {
        let canon_root = match fs::canonicalize(root) {
            Ok(p) => p,
            Err(_) => continue,
        };
        if canon_path.starts_with(&canon_root) {
            return Ok(());
        }
    }
    Err(format!(
        "path is outside the install directories ({:?})",
        allow_roots
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>(),
    ))
}

fn print_post_uninstall(
    scope: InstallScope,
    user_required: Option<&str>,
    user_created_by_install: bool,
    manifest_path: &Path,
) {
    println!("\n# Next steps");
    // Files are already gone, so there's nothing to disable. Only the
    // daemon-reload remains.
    match scope {
        InstallScope::System => {
            println!("  sudo systemctl daemon-reload");
        }
        InstallScope::User => {
            println!("  systemctl --user daemon-reload");
        }
    }
    if let Some(user) = user_required {
        if user_created_by_install {
            // gcit install ran useradd; offer the reverse but keep it
            // operator-driven so a `User=<user>` reference in another
            // unit is not silently broken. We can't reliably scan for
            // that here without parsing every unit on the system.
            println!(
                "\n# Static user '{}' was created by `gcit install` for the previous config (local_mail).",
                user
            );
            println!(
                "  Provided no other unit still references User={}, you can remove it manually:",
                user
            );
            println!("    sudo userdel {}", user);
        } else {
            // The user pre-existed at install time; gcit did not
            // create it, so reversing the account isn't ours to do.
            println!(
                "\n# Static user '{}' was required by the previous config (local_mail) but pre-existed at install time;",
                user
            );
            println!("  `gcit install` did not create it, so `gcit uninstall` does not propose removing it.");
        }
    }
    println!(
        "\nState directory preserved so a future re-install picks up where this run left off. Manifest already removed: {}.",
        manifest_path.display()
    );
}

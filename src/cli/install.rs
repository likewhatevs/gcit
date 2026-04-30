// `gcit install` — guided 5-step setup wizard.
//
// Steps:
//   1. Credential walkthrough (per credential_id).
//   2. Local mail check (warns; never fatal at install time).
//   3. Path preview + confirmation (refuses without explicit `y`;
//      `--non-interactive` skips the prompt for CI).
//   4. Write files atomically + write the install manifest at
//      $STATE_DIRECTORY/.install-manifest.json.
//   5. Print post-install systemctl commands.
//
// Conventions:
//   - Manifest schema:
//       { "schema_version": 1,
//         "files": [ {"path": "...", "sha256": "<hex>", "mode": <int>} ],
//         "user_required": "gcit"  // optional sentinel for local_mail systems
//         "user_created_by_install": true  // optional; true when this run minted the account
//       }
//   - Atomic write: tempfile + write + sync_all + persist + parent dir sync.
//   - --user + local_mail: REJECT (clear error).
//   - Refused confirmation: exit 0 (operator changed their mind).
//   - Refused overwrite: exit 78 (EX_CONFIG).
//   - --user/--system: required mutex via clap ArgGroup.
//   - Path preview: each entry annotated [exists] or [new].
//   - Walkthrough: prints URL + path + chmod for each credential_id.
//   - useradd lifecycle: install runs `useradd --system --no-create-home
//     --shell /usr/sbin/nologin -G mail gcit` when local_mail is configured.
//     Exit 0 sets user_created_by_install=true; exit 9 (E_NAME_IN_USE)
//     leaves it false; any other exit is fatal.

use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::cli::exit;
use crate::config::credential_file::{self, Probe};
use crate::config::{self, Config, CredentialKindHint, Destination};
use crate::systemd::{
    install_paths, render_service_unit, render_socket_unit, trigger_daemon_reload, InstallPaths,
    InstallScope, ReloadOutcome,
};

/// Manifest schema version. Bump when the on-disk shape changes so
/// uninstall can detect a manifest from an older gcit version (the
/// ruling here is to refuse uninstall rather than guess).
pub const MANIFEST_SCHEMA_VERSION: u32 = 1;

/// Static system user gcit creates at install time for local_mail
/// flows. When at least one local_mail destination is configured,
/// `gcit install` shells out to
/// `useradd --system --no-create-home --shell /usr/sbin/nologin -G mail gcit`
/// so the rendered systemd unit's `User=gcit, Group=mail` pair has a
/// real account to bind to.
pub const STATIC_USER: &str = "gcit";

/// On-disk shape of the install manifest at
/// `$STATE_DIRECTORY/.install-manifest.json`.
///
/// `user_required` is `Some("gcit")` when the install required the
/// static `gcit` system user (local_mail destination present). The
/// uninstall path uses this to print the matching `userdel` reminder.
///
/// `user_created_by_install` is `true` ONLY when the install's
/// `useradd` invocation actually created the account (exit 0). If the
/// account already existed at install time (useradd reported
/// `E_NAME_IN_USE` = exit 9), this stays `false` so uninstall does NOT
/// reverse a side effect it did not cause — that distinction matters
/// because the static user may be referenced by other system units.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub schema_version: u32,
    pub files: Vec<ManifestEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_required: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub user_created_by_install: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub path: PathBuf,
    /// Lowercase hex sha256 of the file contents at install time.
    pub sha256: String,
    /// POSIX mode (lower 12 bits significant) as a u32, matching
    /// `std::fs::Permissions::mode()`.
    pub mode: u32,
}

/// Run `gcit install`. Returns the exit code.
///
/// `interactive = true` prompts for confirmation; `false` skips the
/// prompt (CI use). `force = true` overwrites existing files; without
/// it, encountering any file in `paths` already on disk is fatal
/// (EX_CONFIG=78).
///
/// Async because `trigger_daemon_reload` talks to the user session bus
/// via zbus. Callers run inside the binary's tokio runtime.
pub async fn run(
    config_path: &Path,
    scope: InstallScope,
    interactive: bool,
    force: bool,
) -> ExitCode {
    // Parse + validate config first. If the config doesn't load, we
    // cannot guide the user through the rest of the wizard.
    let cfg = match config::load(config_path) {
        Ok(c) => c,
        Err(errors) => {
            for e in &errors {
                eprintln!("{}", e);
            }
            return ExitCode::from(exit::CONFIG);
        }
    };

    // --user + local_mail: reject. /var/mail group access requires a
    // system-managed static user; the per-user systemd manager cannot
    // useradd or join the `mail` group. Surfacing this at install time
    // prevents a silently-broken --user install for local_mail flows.
    let has_local_mail = cfg.flow.iter().any(|f| {
        f.destination
            .iter()
            .any(|d| matches!(d, Destination::LocalMail(_)))
    });
    if scope == InstallScope::User && has_local_mail {
        eprintln!(
            "gcit install --user: configuration uses `local_mail` destination(s) but \
             /var/mail/<user> requires the static `mail` group, which the per-user systemd \
             manager cannot grant. Re-run with --system, or remove the local_mail \
             destinations from the config."
        );
        return ExitCode::from(exit::USAGE);
    }

    let home = match home_dir() {
        Some(h) => h,
        None => {
            eprintln!("gcit install: cannot resolve $HOME");
            return ExitCode::from(exit::USAGE);
        }
    };
    let paths = install_paths(scope, &home);

    // Resolve the install-time gcit binary so the rendered systemd
    // unit's ExecStart and ExecReload point at THIS binary, not at a
    // hardcoded /usr/bin/gcit (the previous value, which broke
    // --user installs from ~/.cargo/bin/gcit). std::env::current_exe()
    // returns the absolute path the kernel exec'd; ProtectSystem=strict
    // in the unit makes the path immutable from the daemon's
    // perspective so the path the operator records here is the path
    // they get at runtime.
    let binary_path = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            eprintln!(
                "gcit install: cannot resolve current_exe() for ExecStart= path: {}",
                e
            );
            return ExitCode::from(exit::OSERR);
        }
    };

    // Step 1: credential walkthrough.
    print_credential_walkthrough(&cfg, &paths, scope);

    // Step 2: local mail spool check. Warning only; the operator can
    // fix it before starting the service.
    if has_local_mail {
        print_local_mail_check(&cfg);
    }

    // Step 3: path preview + confirmation. Build the outputs (which
    // reads cfg.source_path) before printing so a missing source file
    // fails fast.
    let outputs = match build_outputs(&cfg, scope, &paths, &binary_path) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("gcit install: cannot prepare install outputs: {}", e);
            return ExitCode::from(exit::CONFIG);
        }
    };
    print_path_preview(&paths, &outputs, has_local_mail);

    // Idempotency: refuse silent overwrite of any existing managed
    // file.
    if !force {
        let existing: Vec<&PathBuf> = outputs
            .iter()
            .map(|o| &o.path)
            .filter(|p| p.exists())
            .collect();
        if !existing.is_empty() {
            eprintln!(
                "\ngcit install: refusing silent overwrite. The following file(s) already exist:"
            );
            for p in existing {
                eprintln!("  {}", p.display());
            }
            eprintln!("Re-run with `--force` to overwrite, or `gcit uninstall` first.");
            return ExitCode::from(exit::CONFIG);
        }
    }

    if interactive {
        print!("\nProceed? [y/N] ");
        if let Err(e) = io::stdout().flush() {
            // stdout broken; we cannot prompt — abort safely without
            // writing anything. Surface the I/O failure as EX_OSERR
            // so a wrapper script (CI, packager) can distinguish a
            // broken environment from an operator declining the
            // prompt (which exits OK below).
            eprintln!("gcit install: stdout flush failed during prompt: {}", e);
            return ExitCode::from(exit::OSERR);
        }
        let mut answer = String::new();
        if let Err(e) = io::stdin().read_line(&mut answer) {
            eprintln!("gcit install: stdin read failed during prompt: {}", e);
            return ExitCode::from(exit::OSERR);
        }
        let answer = answer.trim();
        if !matches!(answer, "y" | "Y" | "yes" | "YES" | "Yes") {
            // Operator changed their mind — not a failure.
            println!("install cancelled; nothing written.");
            return ExitCode::from(exit::OK);
        }
    }

    // Step 4a: create the static `gcit` user when local_mail is
    // present + scope is system. `gcit install` owns this lifecycle
    // so the unit's `User=gcit, Group=mail` has a real account to
    // bind to. The --user + local_mail combination was already
    // rejected above, so this only fires for --system.
    let user_created_by_install = if has_local_mail && matches!(scope, InstallScope::System) {
        match ensure_static_user(STATIC_USER) {
            Ok(created) => created,
            Err(e) => {
                eprintln!("gcit install: useradd failed: {}", e);
                return ExitCode::from(exit::OSERR);
            }
        }
    } else {
        false
    };

    // Step 4b: write files + manifest.
    let manifest = match write_outputs(
        &outputs,
        &paths.manifest,
        has_local_mail,
        user_created_by_install,
    ) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("gcit install: write failed: {}", e);
            return ExitCode::from(exit::CONFIG);
        }
    };
    println!(
        "\nWrote {} file(s); manifest at {}.",
        manifest.files.len(),
        paths.manifest.display()
    );

    // daemon-reload on the user session bus (or skip with hint for
    // --system). We're already inside the binary's tokio runtime;
    // await directly.
    match trigger_daemon_reload(scope).await {
        Ok(ReloadOutcome::Reloaded) => {
            println!("systemd daemon-reload completed via session bus.");
        }
        Ok(ReloadOutcome::SkippedSystemRequiresRoot) => {
            // Expected when running --system without root. Hint emitted
            // below in the post-install banner.
        }
        Err(e) => {
            eprintln!(
                "warning: daemon-reload failed: {} (run the systemctl command from the next-steps banner manually)",
                e
            );
        }
    }

    // Step 5: post-install next steps.
    print_post_install(scope);

    ExitCode::from(exit::OK)
}

/// One file the install wizard will write. `mode` is the POSIX mode the
/// file is created with; `kind` exists so the preview can label each
/// entry by purpose.
#[derive(Debug, Clone)]
struct OutputFile {
    path: PathBuf,
    contents: Vec<u8>,
    mode: u32,
    label: &'static str,
}

fn build_outputs(
    cfg: &Config,
    scope: InstallScope,
    paths: &InstallPaths,
    binary_path: &Path,
) -> io::Result<Vec<OutputFile>> {
    let service = render_service_unit(cfg, scope, binary_path);
    let socket = render_socket_unit();
    // Re-read the source config from disk so the rendered
    // `paths.config` copy matches the byte-for-byte input the operator
    // edited (rather than re-serializing the parsed Config, which
    // strips comments and reorders keys). If the file disappeared
    // between `gcit check` and `gcit install` (TOCTOU), bail with a
    // clear IO error rather than installing an empty config.
    let cfg_text = fs::read_to_string(&cfg.source_path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "could not re-read config file {} for install: {}",
                cfg.source_path.display(),
                e
            ),
        )
    })?;
    Ok(vec![
        OutputFile {
            path: paths.service_unit.clone(),
            contents: service.into_bytes(),
            // 0644 — readable by everyone (no secret material), writable
            // only by the install user.
            mode: 0o644,
            label: "gcit.service",
        },
        OutputFile {
            path: paths.socket_unit.clone(),
            contents: socket.into_bytes(),
            mode: 0o644,
            label: "gcit.socket",
        },
        OutputFile {
            path: paths.config.clone(),
            contents: cfg_text.into_bytes(),
            // 0640 — operator-readable; the daemon reads it via
            // ConfigurationDirectory= which has its own perms.
            mode: 0o640,
            label: "config.toml",
        },
    ])
}

fn print_credential_walkthrough(cfg: &Config, paths: &InstallPaths, scope: InstallScope) {
    let by_id = collect_credential_uses(cfg);
    if by_id.is_empty() {
        println!("# Credentials");
        println!("(no credential_id referenced; nothing to configure.)");
        return;
    }
    println!("# Credentials");
    println!("Each `credential_id` listed below needs a credential file at the printed path.");
    println!("Copy the secret value into that file (do NOT pass it on the command line),");
    println!("then run `chmod 0600 <path>` so gcit can read it (group/other access is refused).\n");
    for use_ in &by_id {
        let dest = paths.credentials_dir.join(use_.id.as_str());
        // Already-configured short-circuit: when the credential file
        // at the canonical path already meets the daemon's runtime
        // invariants (regular file, mode bitmask `mode & 0o077 == 0`
        // — so 0400/0500/0600/0700 all qualify — owned by the invoking
        // euid OR root), print a one-line confirmation and skip the
        // long instructions. The ownership check defends against a
        // foreign-owned 0600 file giving a false "✓"; root ownership
        // is accepted because under DynamicUser=yes the daemon's
        // transient uid is unknowable in advance and operators drop
        // credentials via sudo, leaving root-owned files.
        if is_credential_already_configured(&dest) {
            // Annotate root-owned credentials under --user scope so
            // operators know future rotations require sudo (they
            // cannot rewrite the file as themselves).
            if credential_root_owned_in_user_scope(&dest, scope) {
                println!(
                    "- ✓ {}: configured at {} (owned by root — rotate via sudo)",
                    use_.id,
                    dest.display(),
                );
            } else {
                println!("- ✓ {}: configured at {}", use_.id, dest.display());
            }
            println!();
            continue;
        }
        println!("- credential_id: {}", use_.id);
        println!("  used by: {}", use_.consumers.join(", "));
        match &use_.kind {
            CredentialKindHint::GithubPat { repo } => {
                println!("  kind: GitHub PAT (fine-grained)");
                println!("  obtain at: https://github.com/settings/tokens?type=beta");
                println!("  target: {} (Actions: read+write)", repo);
            }
            CredentialKindHint::DiscordWebhook => {
                println!("  kind: Discord webhook URL");
                println!(
                    "  obtain at: Discord -> Server Settings -> Integrations -> Webhooks -> New Webhook -> Copy URL"
                );
            }
            CredentialKindHint::SourceFetch => {
                println!("  kind: source-side fetch credential");
            }
        }
        println!("  destination: {}", dest.display());
        println!("  chmod 0600 {}", dest.display());
        println!();
    }
}

/// Returns true when `path` is a regular file that the daemon will
/// accept as a credential at runtime: not a symlink, regular file,
/// mode with no group/other access bits (`mode & 0o077 == 0` — so
/// 0o400 / 0o500 / 0o600 / 0o700 all qualify), and owned by the
/// invoking euid OR root. Defers entirely to `credential_file::probe`
/// so the wizard's "already configured" check, the supervisor's
/// runtime resolution, and `gcit check` agree on what counts as a
/// usable credential. Anything other than `Probe::Ok` is treated as
/// "not configured" — we never claim a credential is set up when we
/// couldn't verify it.
fn is_credential_already_configured(path: &Path) -> bool {
    matches!(credential_file::probe(path), Probe::Ok)
}

/// Returns true when the on-disk credential at `path` is owned by
/// root (uid 0) AND the invoking euid is non-root. Used by the
/// install wizard to annotate the "✓ configured" line for a
/// root-owned credential under `--user` scope: the operator should
/// know that any future rotation requires `sudo` because the file
/// is not owned by their own uid.
///
/// Returns false on any stat error or non-root ownership; the helper
/// is a soft annotation, not a security check (the actual ownership
/// gate lives in `credential_file::probe`'s `UnexpectedOwner`
/// branch).
fn credential_root_owned_in_user_scope(path: &Path, scope: InstallScope) -> bool {
    use std::os::unix::fs::MetadataExt;
    if !matches!(scope, InstallScope::User) {
        return false;
    }
    let meta = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(_) => return false,
    };
    if meta.uid() != 0 {
        return false;
    }
    // SAFETY: geteuid() is async-signal-safe and always succeeds.
    let euid = unsafe { libc::geteuid() };
    euid != 0
}

fn print_local_mail_check(cfg: &Config) {
    println!("# Local mail check");
    let users: BTreeSet<String> = cfg
        .flow
        .iter()
        .flat_map(|f| f.destination.iter())
        .filter_map(|d| match d {
            Destination::LocalMail(m) => Some(m.user.clone()),
            _ => None,
        })
        .collect();
    for user in users {
        let spool = PathBuf::from(format!("/var/mail/{}", user));
        match fs::metadata(&spool) {
            Ok(meta) => {
                let mode = meta.permissions().mode() & 0o777;
                let group_writable = mode & 0o060 != 0;
                if group_writable {
                    println!(
                        "  {}: present (mode {:o}); the daemon writes via the `mail` group.",
                        spool.display(),
                        mode
                    );
                } else {
                    println!(
                        "  warning: {} mode {:o} is not group-writable; the daemon (group=mail) may fail to append.",
                        spool.display(),
                        mode
                    );
                    println!(
                        "    fix: chmod g+w {} && chgrp mail {}",
                        spool.display(),
                        spool.display()
                    );
                }
            }
            Err(_) => {
                println!(
                    "  warning: {} is missing; create it before starting the service:",
                    spool.display()
                );
                println!("    sudo touch {}", spool.display());
                println!(
                    "    sudo chgrp mail {} && sudo chmod 0660 {}",
                    spool.display(),
                    spool.display()
                );
            }
        }
    }
    println!();
}

fn print_path_preview(paths: &InstallPaths, outputs: &[OutputFile], has_local_mail: bool) {
    println!("# Files gcit will write");
    for o in outputs {
        let tag = if o.path.exists() { "[exists]" } else { "[new]" };
        println!("  {} {} ({})", tag, o.path.display(), o.label);
    }
    println!();
    println!("# Manifest");
    let tag = if paths.manifest.exists() {
        "[exists]"
    } else {
        "[new]"
    };
    println!("  {} {}", tag, paths.manifest.display());
    println!();
    println!("# Directories systemd will auto-create on first start");
    println!("  RuntimeDirectory:       (resolves to %t/gcit)");
    println!("  StateDirectory:         (resolves to %S/gcit)");
    println!("  ConfigurationDirectory: (resolves to %E/gcit)");
    println!();
    if has_local_mail {
        println!("# Service user model");
        println!(
            "  User=gcit, Group=mail, SupplementaryGroups=mail (config uses local_mail; needs /var/mail group access)"
        );
        // Surface the side-effect operators tend to forget about: the
        // install will shell out to useradd to create the static
        // `gcit` system account. Naming the exact invocation in the
        // path-preview lets operators reject the install if their
        // distro / IDM does not allow ad-hoc account creation.
        println!();
        println!("# System users gcit will create");
        println!(
            "  useradd --system --no-create-home --shell /usr/sbin/nologin -G mail {}",
            STATIC_USER,
        );
        println!("    (skipped if account already exists; uninstall does NOT remove it unless");
        println!(
            "     this install created it — see install-manifest's user_created_by_install field)"
        );
    } else {
        println!("# Service user model");
        println!("  DynamicUser=yes (no local_mail destinations in config)");
    }
}

fn print_post_install(scope: InstallScope) {
    println!("\n# Next steps");
    match scope {
        InstallScope::System => {
            // useradd was already run during step 4a when local_mail
            // is configured; no operator action required for it here.
            println!(
                "  sudo systemctl daemon-reload && sudo systemctl enable --now gcit.socket gcit.service"
            );
            println!("  journalctl -u gcit -f");
        }
        InstallScope::User => {
            println!(
                "  systemctl --user daemon-reload && systemctl --user enable --now gcit.socket gcit.service"
            );
            println!("  journalctl --user -u gcit -f");
        }
    }
}

/// Per-id summary the install walkthrough prints. Aggregates the
/// per-reference iterator from `config::walk_credentials` into one
/// row per credential id with deduped consumer flow names and a
/// best-effort kind hint (most-specific kind wins; SourceFetch loses
/// to GithubPat / DiscordWebhook).
#[derive(Debug)]
struct CredentialUse {
    id: config::CredentialId,
    kind: CredentialKindHint,
    consumers: Vec<String>,
}

fn collect_credential_uses(cfg: &Config) -> Vec<CredentialUse> {
    use std::collections::BTreeMap;
    let mut map: BTreeMap<config::CredentialId, CredentialUse> = BTreeMap::new();
    for r in config::walk_credentials(cfg) {
        match map.get_mut(&r.id) {
            Some(u) => {
                u.consumers.push(r.flow.clone());
                // Upgrade SourceFetch (least informative) to a more
                // specific kind when later references provide one.
                if matches!(u.kind, CredentialKindHint::SourceFetch)
                    && !matches!(r.kind, CredentialKindHint::SourceFetch)
                {
                    u.kind = r.kind.clone();
                }
            }
            None => {
                map.insert(
                    r.id.clone(),
                    CredentialUse {
                        id: r.id.clone(),
                        kind: r.kind.clone(),
                        consumers: vec![r.flow.clone()],
                    },
                );
            }
        }
    }
    // Dedup consumer lists per id while preserving first-occurrence
    // order. walk_credentials emits one entry per reference site, so a
    // flow that names the same id from source AND action shows up
    // twice before this dedup pass.
    for u in map.values_mut() {
        let mut seen = BTreeSet::new();
        u.consumers.retain(|n| seen.insert(n.clone()));
    }
    map.into_values().collect()
}

fn write_outputs(
    outputs: &[OutputFile],
    manifest_path: &Path,
    has_local_mail: bool,
    user_created_by_install: bool,
) -> io::Result<Manifest> {
    let mut entries: Vec<ManifestEntry> = Vec::with_capacity(outputs.len());
    for o in outputs {
        ensure_parent(&o.path)?;
        atomic_write(&o.path, &o.contents, o.mode)?;
        entries.push(ManifestEntry {
            path: o.path.clone(),
            sha256: hex_sha256(&o.contents),
            mode: o.mode,
        });
    }
    let manifest = Manifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        files: entries,
        user_required: if has_local_mail {
            Some(STATIC_USER.to_string())
        } else {
            None
        },
        user_created_by_install,
    };
    ensure_parent(manifest_path)?;
    crate::util::atomic_write_json(manifest_path, &manifest, 0o600)?;
    Ok(manifest)
}

/// Ensure the static `gcit` system user exists. Returns `Ok(true)`
/// when this invocation created it, `Ok(false)` when it was already
/// present, or `Err(_)` for any other failure (useradd missing, no
/// permission, etc.).
///
/// Invokes `useradd --system --no-create-home --shell /usr/sbin/nologin
/// -G mail <name>`. Exit code 9 is `E_NAME_IN_USE`
/// (account already exists) — treated as success because the install
/// goal is "this name resolves to a usable system account", not "we
/// just minted it." Exit code 4 (`E_UID_IN_USE`) on a system where the
/// gcit name maps to a different uid would be confusing; we surface
/// that as an error with the useradd stderr attached.
///
/// Pre-flight: the function checks for the `mail` group via
/// `mail_group_exists()` before invoking `useradd -G mail`. Without
/// that check `useradd` would fail with a generic message; the
/// pre-flight surfaces an actionable error pointing at how to create
/// the group or install the package that provides it. The check
/// shells out to `getent group mail` so it is NSS-aware (covers LDAP,
/// SSSD, and any other NSS source rather than only the local
/// `/etc/group` file), and tolerates a getent spawn failure by
/// skipping the gate — when in doubt, defer to `useradd`'s own error
/// so the operator at least sees a real failure rather than a false
/// negative.
///
/// Spawn ENOENT (the `useradd` binary is not on `$PATH`) is wrapped
/// with a hint pointing at the packages that provide it
/// (`shadow-utils` on RHEL/Fedora, `passwd` on Debian/Ubuntu — both
/// named so the message does not have to guess the operator's distro).
/// The wrap preserves `io::ErrorKind::NotFound` so callers can still
/// pattern-match on the kind. Without the wrap the operator sees only
/// `No such file or directory` with no path mentioned, which is
/// misleading because the install path itself is fine — only the
/// helper binary is missing.
fn ensure_static_user(name: &str) -> io::Result<bool> {
    use std::process::Command;

    // Pre-flight: `useradd -G mail <name>` will fail with a useradd-
    // generic message if the `mail` group is absent. Surface a clear
    // error pointing at how to create the group before we spawn the
    // helper. `mail_group_exists` is permissive on getent spawn
    // failure (returns true) so a missing getent never causes a false
    // negative; the actual failure would still surface via useradd in
    // that case.
    if !mail_group_exists() {
        return Err(io::Error::other(
            "the `mail` group must exist for local_mail destinations; create it via \
             `groupadd --system mail` or remove local_mail destinations from config",
        ));
    }

    println!(
        "\nCreating system user `{}` (useradd --system --no-create-home --shell /usr/sbin/nologin -G mail {})",
        name, name,
    );
    let output = match Command::new("useradd")
        .arg("--system")
        .arg("--no-create-home")
        .arg("--shell")
        .arg("/usr/sbin/nologin")
        .arg("-G")
        .arg("mail")
        .arg(name)
        .output()
    {
        Ok(o) => o,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "useradd binary not found in PATH ({}); install shadow-utils \
                     (RHEL/Fedora) or passwd (Debian/Ubuntu) and re-run, or remove \
                     local_mail destinations from config",
                    e,
                ),
            ));
        }
        Err(e) => return Err(e),
    };
    if output.status.success() {
        println!("  created.");
        return Ok(true);
    }
    // useradd's man page documents exit 9 = E_NAME_IN_USE. The user
    // already exists; nothing to do, and uninstall must NOT later
    // userdel because we did not create the account.
    if output.status.code() == Some(9) {
        println!("  already exists; not modified.");
        return Ok(false);
    }
    Err(io::Error::other(format!(
        "useradd exited with status {:?}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim(),
    )))
}

/// True if the `mail` group is present on the system. Shells out to
/// `getent group mail` so the check is NSS-aware (covers `/etc/group`
/// AND LDAP, SSSD, and any other NSS source the operator's nsswitch.conf
/// configures). `getent` exits 0 when the requested entry resolves and
/// non-zero when it does not.
///
/// When `getent` itself cannot be spawned (binary missing on a minimal
/// container, sandbox without `/usr/bin/getent`, etc.), this returns
/// `true` permissively so the gate never blocks on a missing-evidence
/// failure — `useradd` will still surface its own error in that case.
fn mail_group_exists() -> bool {
    use std::process::Command;
    match Command::new("getent").args(["group", "mail"]).status() {
        Ok(status) => status.success(),
        Err(_) => true,
    }
}

fn ensure_parent(p: &Path) -> io::Result<()> {
    if let Some(parent) = p.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    Ok(())
}

// Atomic-write delegated to `crate::util::atomic_write` so the install
// manifest and the state writer share identical durability semantics
// (tempfile + sync_all + persist + parent dir fsync).
use crate::util::atomic_write;

fn hex_sha256(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    format!("{:x}", h.finalize())
}

/// Returns `$HOME` as a `PathBuf` if it's set and non-empty. Used so
/// `install_paths(InstallScope::User, ..)` resolves relative to the
/// invoking user without pulling in the `dirs` crate.
pub fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
}

/// Read and parse a manifest from disk. Used by `gcit uninstall`.
pub fn read_manifest(path: &Path) -> io::Result<Manifest> {
    let mut f = fs::File::open(path)?;
    let mut buf = String::new();
    f.read_to_string(&mut buf)?;
    let m: Manifest = serde_json::from_str(&buf)
        .map_err(|e| io::Error::other(format!("parse manifest: {}", e)))?;
    Ok(m)
}

/// Compute the sha256 of an existing file (used by uninstall to verify
/// the manifest's recorded hash still matches what's on disk).
pub fn file_sha256(p: &Path) -> io::Result<String> {
    let mut f = fs::File::open(p)?;
    let mut h = Sha256::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(format!("{:x}", h.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_round_trip() {
        let m = Manifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            files: vec![ManifestEntry {
                path: PathBuf::from("/tmp/x"),
                sha256: "deadbeef".into(),
                mode: 0o644,
            }],
            user_required: Some("gcit".into()),
            user_created_by_install: true,
        };
        let s = serde_json::to_string(&m).unwrap();
        let back: Manifest = serde_json::from_str(&s).unwrap();
        assert_eq!(back.schema_version, m.schema_version);
        assert_eq!(back.files.len(), 1);
        assert_eq!(back.files[0].sha256, "deadbeef");
        assert_eq!(back.user_required.as_deref(), Some("gcit"));
        assert!(back.user_created_by_install);
    }

    #[test]
    fn manifest_user_required_omitted_when_none() {
        let m = Manifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            files: vec![],
            user_required: None,
            user_created_by_install: false,
        };
        let s = serde_json::to_string(&m).unwrap();
        // `skip_serializing_if = "Option::is_none"` keeps user_required
        // out of the on-disk representation. user_created_by_install is
        // also omitted via `skip_serializing_if = "is_false"` when no
        // useradd happened, so a Discord-only install's manifest is
        // visually clean.
        assert!(!s.contains("user_required"));
        assert!(!s.contains("user_created_by_install"));
    }

    #[test]
    fn manifest_user_created_round_trips() {
        let m = Manifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            files: vec![],
            user_required: Some(STATIC_USER.to_string()),
            user_created_by_install: false,
        };
        let s = serde_json::to_string(&m).unwrap();
        // user_created_by_install = false is the default and gets
        // skipped, but user_required must stay.
        assert!(s.contains("user_required"));
        assert!(!s.contains("user_created_by_install"));
        let back: Manifest = serde_json::from_str(&s).unwrap();
        assert!(!back.user_created_by_install);
    }

    // atomic_write coverage moved to crate::util::tests where the
    // implementation now lives.

    #[test]
    fn hex_sha256_known_vector() {
        // sha256("") is the canonical empty-string digest.
        assert_eq!(
            hex_sha256(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}

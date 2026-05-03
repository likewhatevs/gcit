// systemd unit-file rendering.
//
// Every hardening directive is emitted byte-for-byte. The
// DynamicUser=yes / User=gcit/Group=mail swap is driven from
// `has_local_mail`.
//
// LoadCredential lines are generated from the set of credential ids
// referenced by the config (collected by the caller; this module
// emits the unit text given the set).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::config::{self, Config, CredentialId, Destination};

/// Whether to install for the user systemd manager (`--user`) or the
/// system manager (`--system`). Drives both file destinations and the
/// daemon-reload command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallScope {
    User,
    System,
}

/// Concrete absolute paths the install/uninstall flows operate on. The
/// caller resolves XDG / FHS prefixes once at the top of the wizard
/// (so everything downstream sees the same paths regardless of test
/// override or production deployment).
#[derive(Debug, Clone)]
pub struct InstallPaths {
    /// Where `gcit.service` is written.
    pub service_unit: PathBuf,
    /// Where `gcit.socket` is written.
    pub socket_unit: PathBuf,
    /// Where the rendered config (`config.toml`) is written.
    pub config: PathBuf,
    /// Directory under which credential files belong
    /// (`<config_dir>/credentials/<id>`). gcit install does not write
    /// the credentials themselves (operator does that out-of-band) but
    /// it lists the destinations during the walkthrough.
    pub credentials_dir: PathBuf,
    /// Where the install manifest is written
    /// (`<state_dir>/.install-manifest.json`).
    pub manifest: PathBuf,
}

/// Resolve the absolute install paths for a given scope. For `User`
/// scope this honors `$XDG_CONFIG_HOME` / `$XDG_STATE_HOME` with the
/// canonical fallbacks (`~/.config`, `~/.local/state`); for `System`
/// it uses `/etc/gcit`, `/etc/systemd/system`, `/var/lib/gcit`.
///
/// `home` is taken as a parameter so tests can pin a tempdir without
/// relying on `$HOME`. Production callers pass `dirs::home_dir()`.
pub fn install_paths(scope: InstallScope, home: &std::path::Path) -> InstallPaths {
    match scope {
        InstallScope::User => {
            let xdg_config = std::env::var_os("XDG_CONFIG_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".config"));
            let xdg_state = std::env::var_os("XDG_STATE_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".local").join("state"));
            let config_dir = xdg_config.join("gcit");
            let units_dir = xdg_config.join("systemd").join("user");
            let state_dir = xdg_state.join("gcit");
            InstallPaths {
                service_unit: units_dir.join("gcit.service"),
                socket_unit: units_dir.join("gcit.socket"),
                config: config_dir.join("config.toml"),
                credentials_dir: config_dir.join("credentials"),
                manifest: state_dir.join(".install-manifest.json"),
            }
        }
        InstallScope::System => InstallPaths {
            service_unit: PathBuf::from("/etc/systemd/system/gcit.service"),
            socket_unit: PathBuf::from("/etc/systemd/system/gcit.socket"),
            config: PathBuf::from("/etc/gcit/config.toml"),
            credentials_dir: PathBuf::from("/etc/gcit/credentials"),
            manifest: PathBuf::from("/var/lib/gcit/.install-manifest.json"),
        },
    }
}

/// Render `gcit.socket`. The unit is identical for system and user
/// installs; the path the kernel binds to differs (`%t` resolves at
/// runtime to `/run/gcit` or `$XDG_RUNTIME_DIR/gcit` respectively,
/// both directly off the operator's perspective).
pub fn render_socket_unit() -> String {
    "[Unit]\n\
     Description=gcit control socket\n\
     \n\
     [Socket]\n\
     ListenStream=%t/gcit/control.sock\n\
     SocketMode=0600\n\
     FileDescriptorName=control\n\
     \n\
     [Install]\n\
     WantedBy=sockets.target\n"
        .to_string()
}

/// Render `gcit.service` for the given config + scope.
///
/// The service form depends on whether any flow uses `local_mail`:
/// when yes, swap `DynamicUser=yes` for `User=gcit / Group=mail /
/// SupplementaryGroups=mail`. Every other hardening directive is
/// emitted byte-for-byte regardless of form so
/// `systemd-analyze security gcit.service` reports identical hardening
/// in either branch.
///
/// `binary_path` is the absolute path to the gcit binary that
/// `ExecStart` and `ExecReload` will reference. The caller resolves
/// this from `std::env::current_exe()` so a `--user` install rooted
/// at `~/.cargo/bin/gcit` records that exact path rather than the
/// system-only `/usr/bin/gcit`.
///
/// LoadCredential lines are emitted in deterministic (sorted) order
/// so the rendered output is reproducible for snapshot tests.
pub fn render_service_unit(cfg: &Config, scope: InstallScope, binary_path: &Path) -> String {
    let credential_ids = collect_credential_ids(cfg);
    let has_local_mail = cfg.flow.iter().any(|f| {
        f.destination
            .iter()
            .any(|d| matches!(d, Destination::LocalMail(_)))
    });
    let bin = binary_path.display();

    let mut s = String::with_capacity(2048);

    s.push_str("[Unit]\n");
    s.push_str("Description=gcit (poll git, dispatch GitHub Actions, notify Discord/mail)\n");
    s.push_str("Documentation=https://github.com/likewhatevs/gcit\n");
    s.push_str("Requires=gcit.socket\n");
    s.push_str("After=network-online.target\n");
    s.push_str("Wants=network-online.target\n");
    s.push('\n');

    s.push_str("[Service]\n");
    s.push_str("Type=notify\n");
    // `gcit run` is the daemon entry; ExecStart/ExecReload always emit
    // the absolute path the install caller passed. For `--system`
    // installs this is typically `/usr/bin/gcit`; for `--user` installs
    // it's wherever the operator's session has gcit (e.g.
    // `~/.cargo/bin/gcit`). Resolving via current_exe() at install time
    // (not `which gcit` at unit-load time) means the systemd unit
    // continues to point at the install-time binary even if the
    // operator later installs a second copy somewhere ahead in $PATH.
    s.push_str(&format!("ExecStart={} run\n", bin));
    s.push_str(&format!("ExecReload={} reload\n", bin));

    if has_local_mail {
        // local_mail requires a static system user because DynamicUser
        // cannot join the `mail` group needed to write
        // /var/mail/<user>.
        s.push_str("User=gcit\n");
        s.push_str("Group=mail\n");
        s.push_str("SupplementaryGroups=mail\n");
    } else {
        s.push_str("DynamicUser=yes\n");
    }

    // Hardening directives, byte-for-byte. Two directives are gated:
    //   * `BindPaths=/var/mail` only when `has_local_mail` so a
    //     Discord-only install does not punch a writable path through
    //     `ProtectSystem=strict` (the local_mail notifier is the only
    //     writer to /var/mail).
    //   * `PrivateUsers=yes` is unconditional. systemd maps the unit's
    //     Group=mail gid to 0 inside the user namespace and translates
    //     back through the gid_map on host-filesystem access, so
    //     BindPaths=/var/mail writes still check against the host mail
    //     gid correctly.
    //
    // The two `SystemCallFilter=` lines stack: systemd treats
    // multiple SystemCallFilter= entries as additive, so the
    // `@system-service` allow-list runs first and the `~@resources` /
    // `~@privileged` denials prune syscalls back out of it. systemd
    // documents this composition rule in systemd.exec(5).
    //
    // `DevicePolicy=closed` complements the empty `DeviceAllow=`. With
    // `PrivateDevices=yes` alone, systemd grants implicit read access
    // to /dev/rtc; `DevicePolicy=closed` removes that without affecting
    // the standard tty/pseudo-tty/null/zero/random/urandom set
    // PrivateDevices already provides.
    //
    // `RootDirectory` + `TemporaryFileSystem` + `BindReadOnlyPaths` are
    // gated on `!has_local_mail` alongside `PrivateUsers`. Together they
    // isolate the daemon's filesystem view and mount only the CA cert
    // bundle (Fedora primary, Ubuntu fallback) and DNS resolver. Gated
    // because /var/mail access inside a chroot requires BindPaths which
    // has not been empirically verified with the local_mail flock path.
    //
    // `IPAddressDeny=any` + `IPAddressAllow=any` establishes a
    // default-deny IP address policy. gcit needs arbitrary routable
    // unicast (GitHub API, Discord webhooks, operator-configured git
    // remotes), so the allow-all re-opens everything — but the
    // default-deny framework lets operators tighten the policy via
    // systemd drop-ins without editing the unit.
    let mut hardening: Vec<&str> = vec![
        "NoNewPrivileges=yes",
        "ProtectSystem=strict",
        "ProtectHome=yes",
        "PrivateTmp=yes",
        "PrivateDevices=yes",
        "ProtectKernelTunables=yes",
        "ProtectKernelModules=yes",
        "ProtectKernelLogs=yes",
        "ProtectControlGroups=yes",
        "ProtectClock=yes",
        "ProtectHostname=yes",
        "ProtectProc=invisible",
        "ProcSubset=pid",
        "RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6",
        "RestrictNamespaces=yes",
        "RestrictRealtime=yes",
        "RestrictSUIDSGID=yes",
        "LockPersonality=yes",
        "MemoryDenyWriteExecute=yes",
        "SystemCallFilter=@system-service",
        "SystemCallFilter=~@resources",
        "SystemCallFilter=~@privileged",
        "SystemCallArchitectures=native",
        "CapabilityBoundingSet=",
        "AmbientCapabilities=",
        "DeviceAllow=",
        "DevicePolicy=closed",
        "IPAddressDeny=any",
        "IPAddressAllow=any",
        "RemoveIPC=yes",
        "RootDirectory=%t/gcit/root",
        "TemporaryFileSystem=/:ro",
        "BindReadOnlyPaths=/etc/pki/tls/certs -/etc/ssl/certs /etc/resolv.conf",
        "UMask=0077",
        "RuntimeDirectory=gcit",
        "RuntimeDirectoryMode=0700",
        "StateDirectory=gcit",
        "StateDirectoryMode=0700",
        "ConfigurationDirectory=gcit",
        "ConfigurationDirectoryMode=0750",
    ];
    hardening.push("PrivateUsers=yes");
    if has_local_mail {
        hardening.push("BindPaths=/var/mail");
    }
    for line in [
        "Restart=on-failure",
        "NotifyAccess=main",
        "TimeoutStopSec=360",
    ] {
        hardening.push(line);
    }
    for line in &hardening {
        s.push_str(line);
        s.push('\n');
    }

    // LoadCredential lines. The source path matches the convention
    // from `gcit install`: each credential lives at
    // <config_dir>/credentials/<id>. Operators who want a different
    // source can edit the unit after install, but the default is the
    // gcit-managed path.
    let creds_dir = match scope {
        InstallScope::User => "%E/gcit/credentials",
        InstallScope::System => "/etc/gcit/credentials",
    };
    for cid in &credential_ids {
        s.push_str(&format!(
            "LoadCredential={}:{}/{}\n",
            cid.as_str(),
            creds_dir,
            cid.as_str()
        ));
    }

    s.push('\n');
    s.push_str("[Install]\n");
    s.push_str("WantedBy=default.target\n");
    s
}

/// Collect every credential id referenced by the config, sorted for
/// deterministic output. Delegates to the canonical
/// `config::walk_credentials` so this module shares the same iteration
/// shape as cli/install.rs and cli/check.rs.
fn collect_credential_ids(cfg: &Config) -> Vec<CredentialId> {
    let set: BTreeSet<CredentialId> = config::walk_credentials(cfg).map(|r| r.id).collect();
    set.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn socket_unit_includes_listen_stream_and_mode() {
        let unit = render_socket_unit();
        assert!(unit.contains("ListenStream=%t/gcit/control.sock"));
        assert!(unit.contains("SocketMode=0600"));
        assert!(unit.contains("FileDescriptorName=control"));
    }

    fn build_minimal_config(with_local_mail: bool) -> Config {
        let toml = if with_local_mail {
            r#"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "github_pat"
[[flow.destination]]
kind = "local_mail"
user = "ops"
"#
        } else {
            r#"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "github_pat"
[[flow.destination]]
kind = "discord_webhook"
credential_id = "discord_webhook"
"#
        };
        crate::config::load_str(toml, std::path::Path::new("inline")).expect("fixture must load")
    }

    #[test]
    fn service_unit_uses_dynamic_user_when_no_local_mail() {
        let cfg = build_minimal_config(false);
        let unit = render_service_unit(
            &cfg,
            InstallScope::System,
            std::path::Path::new("/usr/bin/gcit"),
        );
        assert!(unit.contains("DynamicUser=yes"));
        assert!(!unit.contains("User=gcit"));
    }

    #[test]
    fn service_unit_uses_static_user_when_local_mail_present() {
        let cfg = build_minimal_config(true);
        let unit = render_service_unit(
            &cfg,
            InstallScope::System,
            std::path::Path::new("/usr/bin/gcit"),
        );
        assert!(unit.contains("User=gcit"));
        assert!(unit.contains("Group=mail"));
        assert!(unit.contains("SupplementaryGroups=mail"));
        assert!(!unit.contains("DynamicUser=yes"));
    }

    #[test]
    fn service_unit_emits_load_credential_lines_sorted() {
        let cfg = build_minimal_config(false);
        let unit = render_service_unit(
            &cfg,
            InstallScope::System,
            std::path::Path::new("/usr/bin/gcit"),
        );
        assert!(
            unit.contains("LoadCredential=discord_webhook:/etc/gcit/credentials/discord_webhook")
        );
        assert!(unit.contains("LoadCredential=github_pat:/etc/gcit/credentials/github_pat"));
        // BTreeSet iteration is sorted; "discord_webhook" < "github_pat".
        let dw = unit.find("LoadCredential=discord_webhook").unwrap();
        let gp = unit.find("LoadCredential=github_pat").unwrap();
        assert!(dw < gp, "credential lines must emit in sorted order");
    }

    #[test]
    fn service_unit_includes_every_hardening_directive() {
        let cfg = build_minimal_config(false);
        let unit = render_service_unit(
            &cfg,
            InstallScope::System,
            std::path::Path::new("/usr/bin/gcit"),
        );
        for required in [
            "NoNewPrivileges=yes",
            "ProtectSystem=strict",
            "ProtectHome=yes",
            "PrivateTmp=yes",
            "PrivateDevices=yes",
            "ProtectKernelTunables=yes",
            "ProtectKernelModules=yes",
            "ProtectKernelLogs=yes",
            "ProtectControlGroups=yes",
            "ProtectClock=yes",
            "ProtectHostname=yes",
            "ProtectProc=invisible",
            "ProcSubset=pid",
            "RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6",
            "RestrictNamespaces=yes",
            "RestrictRealtime=yes",
            "RestrictSUIDSGID=yes",
            "LockPersonality=yes",
            "MemoryDenyWriteExecute=yes",
            "SystemCallFilter=@system-service",
            "SystemCallFilter=~@resources",
            "SystemCallFilter=~@privileged",
            "SystemCallArchitectures=native",
            "CapabilityBoundingSet=",
            "AmbientCapabilities=",
            "DeviceAllow=",
            "DevicePolicy=closed",
            "IPAddressDeny=any",
            "IPAddressAllow=any",
            "RemoveIPC=yes",
            "PrivateUsers=yes",
            "RootDirectory=%t/gcit/root",
            "TemporaryFileSystem=/:ro",
            "BindReadOnlyPaths=/etc/pki/tls/certs -/etc/ssl/certs /etc/resolv.conf",
            "UMask=0077",
            "RuntimeDirectory=gcit",
            "RuntimeDirectoryMode=0700",
            "StateDirectory=gcit",
            "StateDirectoryMode=0700",
            "ConfigurationDirectory=gcit",
            "ConfigurationDirectoryMode=0750",
            "Restart=on-failure",
            "NotifyAccess=main",
            "TimeoutStopSec=360",
        ] {
            assert!(
                unit.contains(required),
                "service unit missing hardening directive: {}",
                required,
            );
        }
    }

    #[test]
    fn service_unit_always_emits_private_users() {
        // PrivateUsers=yes is unconditional. systemd maps Group=mail's
        // host gid through the user namespace gid_map, so
        // BindPaths=/var/mail access still checks against the host
        // mail gid correctly.
        for has_local_mail in [false, true] {
            let cfg = build_minimal_config(has_local_mail);
            let unit = render_service_unit(
                &cfg,
                InstallScope::System,
                std::path::Path::new("/usr/bin/gcit"),
            );
            assert!(
                unit.contains("PrivateUsers=yes"),
                "PrivateUsers=yes must be emitted for has_local_mail={has_local_mail}; got: {}",
                unit,
            );
        }
    }

    #[test]
    fn service_unit_emits_system_call_filter_resources_and_privileged_denials() {
        // The deny-list filters compose with the @system-service
        // allow-list — systemd applies them in order, so @resources /
        // @privileged carve out their respective syscall sets after the
        // @system-service base.
        let cfg = build_minimal_config(false);
        let unit = render_service_unit(
            &cfg,
            InstallScope::System,
            std::path::Path::new("/usr/bin/gcit"),
        );
        assert!(
            unit.contains("SystemCallFilter=~@resources"),
            "service unit must deny @resources syscalls; got: {}",
            unit,
        );
        assert!(
            unit.contains("SystemCallFilter=~@privileged"),
            "service unit must deny @privileged syscalls; got: {}",
            unit,
        );
        // The base allow-list must still appear before the denials so
        // systemd applies them in the right order.
        let base = unit.find("SystemCallFilter=@system-service").unwrap();
        let resources = unit.find("SystemCallFilter=~@resources").unwrap();
        let privileged = unit.find("SystemCallFilter=~@privileged").unwrap();
        assert!(
            base < resources && base < privileged,
            "base SystemCallFilter must precede the deny filters",
        );
    }

    #[test]
    fn service_unit_emits_ip_address_default_deny_with_open_allow() {
        // `IPAddressDeny=any` + `IPAddressAllow=any` establishes a
        // default-deny IP policy that gcit's required outbound traffic
        // (GitHub API, Discord webhooks, operator-configured remotes)
        // re-opens via the allow-all. The shape lets operators tighten
        // the policy via systemd drop-ins without editing the unit, and
        // satisfies systemd-analyze's check that the service defines
        // an IP address allow list. Pinned so a future edit that drops
        // either directive reverts the score gain.
        let cfg = build_minimal_config(false);
        let unit = render_service_unit(
            &cfg,
            InstallScope::System,
            std::path::Path::new("/usr/bin/gcit"),
        );
        assert!(
            unit.contains("IPAddressDeny=any"),
            "service unit must default-deny outbound IP traffic; got: {}",
            unit,
        );
        assert!(
            unit.contains("IPAddressAllow=any"),
            "service unit must re-open the IP space via IPAddressAllow=any; got: {}",
            unit,
        );
    }

    #[test]
    fn service_unit_emits_device_policy_closed() {
        // DeviceAllow= empty alone leaves /dev/rtc readable via
        // PrivateDevices=yes' implicit allow. DevicePolicy=closed
        // removes that without breaking PrivateDevices' standard set.
        let cfg = build_minimal_config(false);
        let unit = render_service_unit(
            &cfg,
            InstallScope::System,
            std::path::Path::new("/usr/bin/gcit"),
        );
        assert!(
            unit.contains("DevicePolicy=closed"),
            "service unit must close the device ACL; got: {}",
            unit,
        );
    }

    #[test]
    fn service_unit_omits_var_mail_read_write_path_without_local_mail() {
        // `BindPaths=/var/mail` punches a writable path through
        // `ProtectSystem=strict`. For Discord-only installs nothing
        // writes there, so the directive must be absent.
        let cfg = build_minimal_config(false);
        let unit = render_service_unit(
            &cfg,
            InstallScope::System,
            std::path::Path::new("/usr/bin/gcit"),
        );
        assert!(
            !unit.contains("BindPaths=/var/mail"),
            "Discord-only install must NOT emit BindPaths=/var/mail",
        );
    }

    #[test]
    fn service_unit_emits_var_mail_read_write_path_with_local_mail() {
        // local_mail destinations require write access to /var/mail/<user>.
        // Pin that the directive is gated on `has_local_mail` and surfaces
        // alongside the User=gcit / Group=mail pair.
        let cfg = build_minimal_config(true);
        let unit = render_service_unit(
            &cfg,
            InstallScope::System,
            std::path::Path::new("/usr/bin/gcit"),
        );
        assert!(
            unit.contains("BindPaths=/var/mail"),
            "local_mail install must emit BindPaths=/var/mail",
        );
    }

    #[test]
    fn service_unit_threads_binary_path_into_exec_lines() {
        let cfg = build_minimal_config(false);
        // A `--user` install at ~/.cargo/bin/gcit must serialize that
        // path into ExecStart and ExecReload — not /usr/bin/gcit.
        let bin = std::path::Path::new("/home/operator/.cargo/bin/gcit");
        let unit = render_service_unit(&cfg, InstallScope::User, bin);
        assert!(
            unit.contains("ExecStart=/home/operator/.cargo/bin/gcit run\n"),
            "ExecStart must use the install-time binary path",
        );
        assert!(
            unit.contains("ExecReload=/home/operator/.cargo/bin/gcit reload\n"),
            "ExecReload must use the install-time binary path",
        );
        assert!(
            !unit.contains("/usr/bin/gcit"),
            "user install must not hardcode /usr/bin/gcit",
        );
    }
}

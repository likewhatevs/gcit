// systemd unit emission for local_mail destinations.
//
// Production: src/systemd/unit.rs::render_service_unit drives the
// DynamicUser=yes / User=gcit/Group=mail/SupplementaryGroups=mail
// swap on whether any flow has a `local_mail` destination. The other
// hardening directives are emitted byte-for-byte regardless of form,
// so `systemd-analyze security gcit.service` reports identical
// hardening in either branch. ReadWritePaths=/var/mail is gated on
// `has_local_mail`: emitted only when at least one flow's destination
// is local_mail. Discord-only installs do not punch a writable path
// through ProtectSystem=strict for /var/mail because the local_mail
// notifier is the only writer to that directory.
//
// Tests below drive the public renderer directly with two fixture
// configs (with and without a local_mail destination). Install /
// uninstall lifecycle stubs (useradd / userdel) require a command-
// runner harness that does not exist yet and remain ignored.

use std::path::Path;

use rstest::rstest;

use gcit::config::load_str;
use gcit::systemd::unit::{render_service_unit, InstallScope};

/// Minimal config with one flow whose only destination is a Discord
/// webhook — exercises the `DynamicUser=yes` branch of the renderer.
const CFG_NO_LOCAL_MAIL: &str = r#"
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
"#;

/// Minimal config with one flow whose only destination is local_mail —
/// exercises the `User=gcit / Group=mail / SupplementaryGroups=mail`
/// branch of the renderer.
const CFG_WITH_LOCAL_MAIL: &str = r#"
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
"#;

fn render_unit(toml: &str) -> String {
    let cfg = load_str(toml, Path::new("inline")).expect("fixture must load");
    render_service_unit(&cfg, InstallScope::System, Path::new("/usr/bin/gcit"))
}

#[test]
fn unit_without_local_mail_uses_dynamic_user() {
    // Renderer with no local_mail destinations emits DynamicUser=yes
    // and omits the static-user trio (User=gcit, Group=mail,
    // SupplementaryGroups=mail).
    //
    // Mutation target: always emitting the static trio
    // (would conflict with DynamicUser at unit-load time).
    let unit = render_unit(CFG_NO_LOCAL_MAIL);
    assert!(
        unit.contains("DynamicUser=yes"),
        "no-local-mail unit must use DynamicUser=yes; unit:\n{unit}",
    );
    assert!(
        !unit.contains("User=gcit"),
        "no-local-mail unit must not pin User=gcit; unit:\n{unit}",
    );
    assert!(
        !unit.contains("Group=mail"),
        "no-local-mail unit must not pin Group=mail; unit:\n{unit}",
    );
    assert!(
        !unit.contains("SupplementaryGroups=mail"),
        "no-local-mail unit must not pin SupplementaryGroups=mail; unit:\n{unit}",
    );
}

#[test]
fn unit_with_local_mail_uses_user_gcit_group_mail() {
    // Renderer with a local_mail destination emits the static-user
    // trio and omits DynamicUser. The mail-group membership lets
    // the daemon write to /var/mail/<user> — DynamicUser cannot
    // join a static group.
    //
    // Mutation target: the "any local_mail?" check using
    // the wrong predicate (e.g. checks fire_on instead of kind).
    let unit = render_unit(CFG_WITH_LOCAL_MAIL);
    assert!(
        unit.contains("User=gcit"),
        "local-mail unit must pin User=gcit; unit:\n{unit}",
    );
    assert!(
        unit.contains("Group=mail"),
        "local-mail unit must pin Group=mail; unit:\n{unit}",
    );
    assert!(
        unit.contains("SupplementaryGroups=mail"),
        "local-mail unit must pin SupplementaryGroups=mail; unit:\n{unit}",
    );
    assert!(
        !unit.contains("DynamicUser=yes"),
        "local-mail unit must NOT use DynamicUser; unit:\n{unit}",
    );
}

#[test]
fn unit_gates_read_write_paths_var_mail_on_local_mail_present() {
    // ReadWritePaths=/var/mail is gated on has_local_mail. A Discord-
    // only install never writes to /var/mail; emitting the line would
    // punch a needless writable path through ProtectSystem=strict.
    // The local_mail variant must include it because the local_mail
    // notifier is the sole writer and would otherwise be blocked by
    // the sandbox.
    //
    // Mutation target: dropping the gate and emitting
    // unconditionally; harmless-looking but reduces the hardening of
    // every Discord-only install.
    let unit_no = render_unit(CFG_NO_LOCAL_MAIL);
    let unit_yes = render_unit(CFG_WITH_LOCAL_MAIL);
    assert!(
        !unit_no.contains("ReadWritePaths=/var/mail"),
        "no-local-mail unit must NOT include ReadWritePaths=/var/mail; unit:\n{unit_no}",
    );
    assert!(
        unit_yes.contains("ReadWritePaths=/var/mail"),
        "local-mail unit must include ReadWritePaths=/var/mail; unit:\n{unit_yes}",
    );
}

#[rstest]
#[case::no_new_privileges("NoNewPrivileges=yes")]
#[case::protect_system("ProtectSystem=strict")]
#[case::protect_home("ProtectHome=yes")]
#[case::private_tmp("PrivateTmp=yes")]
#[case::private_devices("PrivateDevices=yes")]
#[case::protect_kernel_tunables("ProtectKernelTunables=yes")]
#[case::protect_kernel_modules("ProtectKernelModules=yes")]
#[case::protect_kernel_logs("ProtectKernelLogs=yes")]
#[case::protect_control_groups("ProtectControlGroups=yes")]
#[case::protect_clock("ProtectClock=yes")]
#[case::protect_hostname("ProtectHostname=yes")]
#[case::protect_proc("ProtectProc=invisible")]
#[case::proc_subset("ProcSubset=pid")]
#[case::restrict_address_families("RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6")]
#[case::restrict_namespaces("RestrictNamespaces=yes")]
#[case::restrict_realtime("RestrictRealtime=yes")]
#[case::restrict_suid_sgid("RestrictSUIDSGID=yes")]
#[case::lock_personality("LockPersonality=yes")]
#[case::memory_deny_write_execute("MemoryDenyWriteExecute=yes")]
#[case::system_call_filter("SystemCallFilter=@system-service")]
#[case::system_call_architectures("SystemCallArchitectures=native")]
#[case::capability_bounding_set("CapabilityBoundingSet=")]
#[case::device_allow("DeviceAllow=")]
#[case::umask("UMask=0077")]
#[case::runtime_directory("RuntimeDirectory=gcit")]
#[case::runtime_directory_mode("RuntimeDirectoryMode=0700")]
#[case::state_directory("StateDirectory=gcit")]
#[case::state_directory_mode("StateDirectoryMode=0700")]
#[case::configuration_directory("ConfigurationDirectory=gcit")]
#[case::configuration_directory_mode("ConfigurationDirectoryMode=0750")]
#[case::restart("Restart=on-failure")]
#[case::notify_access("NotifyAccess=main")]
#[case::timeout_stop_sec("TimeoutStopSec=360")]
fn unit_contains_hardening_directive(#[case] directive: &str) {
    // Every hardening directive must appear in BOTH the
    // DynamicUser=yes and User=gcit/Group=mail variants — the user-
    // form swap does not relax any sandboxing. A regression that
    // drops one directive surfaces here as a localized failure;
    // systemd-analyze security checks the score holistically and
    // gives less precise diagnostics.
    //
    // Mutation target: a refactor that conditionally emits a
    // directive (e.g. only under one branch), or drops one in a
    // typo, or reorders the byte-for-byte block in a way that
    // accidentally elides a line.
    let unit_no = render_unit(CFG_NO_LOCAL_MAIL);
    let unit_yes = render_unit(CFG_WITH_LOCAL_MAIL);
    assert!(
        unit_no.contains(directive),
        "no-local-mail unit must include {directive:?}; unit:\n{unit_no}",
    );
    assert!(
        unit_yes.contains(directive),
        "local-mail unit must include {directive:?}; unit:\n{unit_yes}",
    );
}

#[test]
#[ignore = "needs `systemd-analyze security` binary on the runner; gate behind SYSTEMD_TESTS=1 \
            (project canon for systemd-dependent tests) when activated"]
fn unit_systemd_analyze_score_below_three() {
    // Pin score ≤ 3.0 ("GOOD" hardening band) by piping the
    // rendered unit through `systemd-analyze security`. The text-
    // based hardening directive tests above catch most regressions;
    // this is the holistic verifier.
}

#[tokio::test]
#[ignore = "needs install-flow command-runner test seam: gcit install currently calls useradd via std::process::Command; \
            tests can't intercept without a Runner trait or process harness"]
async fn install_creates_gcit_user_when_local_mail_present() {
    // gcit install with a local_mail destination must invoke
    // `useradd --system --no-create-home --shell /usr/sbin/nologin
    // -G mail gcit` and record the operation in the install
    // manifest. Activation requires a command-runner test seam.
}

#[tokio::test]
#[ignore = "needs install-flow command-runner test seam"]
async fn install_skips_useradd_when_no_local_mail() {
    // No local_mail destinations must skip the useradd call
    // entirely — pollutes /etc/passwd otherwise.
}

#[tokio::test]
#[ignore = "needs install-flow command-runner test seam"]
async fn install_skips_useradd_when_gcit_user_already_exists() {
    // Pre-existing gcit user: install must check getpwnam first
    // and skip the call (rather than letting useradd fail with
    // EEXIST). Manifest records the skip so uninstall does not
    // remove a user it did not create.
}

#[tokio::test]
#[ignore = "needs uninstall-flow command-runner test seam + manifest fixture"]
async fn uninstall_removes_gcit_user_only_if_install_created_it() {
    // Manifest-driven cleanup: gcit uninstall calls userdel only
    // when the manifest records a previous useradd. A pre-existing
    // gcit user must survive uninstall.
}

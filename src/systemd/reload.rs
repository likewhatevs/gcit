// systemd daemon-reload trigger via D-Bus.
//
// `gcit install` runs daemon-reload after writing units, and prints
// the matching sudo command for system-scope installs.
//
// Scope semantics:
//   - User: zbus session() bus -> org.freedesktop.systemd1.Manager.Reload()
//     This works without elevated privileges because the user's systemd
//     instance is per-user and reachable on the session bus.
//   - System: gcit cannot daemon-reload as a non-root process. Rather
//     than failing midway through install, we return Ok with an info
//     marker and the caller (cli/install.rs) prints the sudo command
//     in the post-install next-steps banner
//     (`sudo systemctl daemon-reload && ...`).

use zbus::{proxy, Connection};

use super::unit::InstallScope;

/// systemd Manager interface, scoped to the methods we use. Generated
/// by zbus's proxy macro against the well-known D-Bus name and path.
#[proxy(
    interface = "org.freedesktop.systemd1.Manager",
    default_service = "org.freedesktop.systemd1",
    default_path = "/org/freedesktop/systemd1"
)]
trait SystemdManager {
    /// Equivalent to `systemctl daemon-reload`. Forces the manager to
    /// re-read every unit file after gcit writes new ones during
    /// install/uninstall.
    fn reload(&self) -> zbus::Result<()>;
}

/// Result of attempting daemon-reload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReloadOutcome {
    /// daemon-reload completed successfully via the user session bus.
    Reloaded,
    /// gcit is running unprivileged with `--system` scope; the caller
    /// should print the sudo command in its post-install banner. The
    /// install/uninstall itself is NOT a failure when this is returned.
    SkippedSystemRequiresRoot,
}

/// Trigger systemd daemon-reload for the given scope.
///
/// User scope: connect to the session bus and invoke
/// org.freedesktop.systemd1.Manager.Reload.
/// System scope: short-circuits with `SkippedSystemRequiresRoot` so the
/// caller can guide the operator toward `sudo systemctl daemon-reload`.
///
/// Errors only surface on transport failures (no session bus, manager
/// not running) — those are operator-actionable and the install flow
/// reports them with a clear message.
pub async fn trigger_daemon_reload(scope: InstallScope) -> zbus::Result<ReloadOutcome> {
    match scope {
        InstallScope::User => {
            let conn = Connection::session().await?;
            let manager = SystemdManagerProxy::new(&conn).await?;
            manager.reload().await?;
            Ok(ReloadOutcome::Reloaded)
        }
        InstallScope::System => {
            // gcit cannot daemon-reload the system manager without
            // root. The install caller emits the sudo command in the
            // next-steps text; we return SkippedSystemRequiresRoot to
            // make this branch testable without spawning a subprocess.
            Ok(ReloadOutcome::SkippedSystemRequiresRoot)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn trigger_daemon_reload_system_scope_short_circuits_without_dbus() {
        // System scope must NOT touch the D-Bus session bus — the
        // caller has no elevated rights and a session-bus connect
        // would either fail or reload the user manager (wrong).
        // The branch returns Ok(SkippedSystemRequiresRoot) so the
        // install/uninstall caller can emit the sudo banner without
        // surfacing a transport error.
        //
        // No assertion on session-bus state needed: if the branch ever
        // started dialing D-Bus the test would either fail in a
        // headless CI runner (no DBUS_SESSION_BUS_ADDRESS) or reload
        // the runner's own user manager (visibly wrong). Pin the
        // Ok variant and the value so a mutation that flipped the
        // outcome or replaced the short-circuit with a real call
        // would surface here.
        let outcome = trigger_daemon_reload(InstallScope::System)
            .await
            .expect("System scope must short-circuit without dialing D-Bus");
        assert_eq!(outcome, ReloadOutcome::SkippedSystemRequiresRoot);
    }

    #[test]
    fn reload_outcome_partial_eq_distinguishes_variants() {
        // Pin that the derived PartialEq doesn't conflate variants —
        // callers match on `==` (e.g. in install.rs's post-action
        // banner branch) and would mis-route if PartialEq returned
        // true across variants.
        assert_eq!(ReloadOutcome::Reloaded, ReloadOutcome::Reloaded);
        assert_eq!(
            ReloadOutcome::SkippedSystemRequiresRoot,
            ReloadOutcome::SkippedSystemRequiresRoot,
        );
        assert_ne!(
            ReloadOutcome::Reloaded,
            ReloadOutcome::SkippedSystemRequiresRoot,
        );
    }
}

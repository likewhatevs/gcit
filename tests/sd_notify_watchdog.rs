// systemd watchdog: WATCHDOG_USEC + WATCHDOG_PID env vars set by
// systemd before exec; the daemon must emit b"WATCHDOG=1\n" at
// WATCHDOG_USEC/2 cadence. Exactly one #[test] per file (env-
// mutating).
//
// sd_notify::watchdog_enabled() (verified against the sd-notify
// crate source) requires WATCHDOG_PID == process::id() to return
// Some. Test pattern:
//   env::set_var("WATCHDOG_PID", process::id().to_string());
//   env::set_var("WATCHDOG_USEC", "5000000");
//
// gcit's watchdog feeder schedules ticks at WATCHDOG_USEC / 2.
// Under tokio::time::pause + advance, the cadence is deterministic.
//
// The watchdog feeder cadence policy (exact half vs jittered half)
// is not pinned. This skeleton documents the test shape
// so it can guard whatever cadence is chosen.

use std::os::unix::net::UnixDatagram;
use std::process;
use tempfile::TempDir;

#[test]
#[ignore = "requires watchdog feeder (not yet implemented; spec silent on cadence policy)"]
fn watchdog_tick_emits_at_half_period() {
    let dir = TempDir::new().unwrap();
    let sock_path = dir.path().join("notify.sock");
    let _ = std::fs::remove_file(&sock_path);
    let _receiver = UnixDatagram::bind(&sock_path).unwrap();

    unsafe {
        std::env::set_var("NOTIFY_SOCKET", &sock_path);
        std::env::set_var("WATCHDOG_USEC", "5000000"); // 5s
        std::env::set_var("WATCHDOG_PID", process::id().to_string());
    }

    // Run the gcit watchdog feeder under tokio::time::pause:
    // - assert sd_notify::watchdog_enabled() returns Some(Duration::from_micros(5_000_000))
    // - advance time by 2.5s; assert one b"WATCHDOG=1\n" datagram received
    // - advance another 2.5s; assert second datagram received
    // - jitter: actual cadence MAY include a small randomized offset to avoid
    //   thundering-herd; the test should accept tick_interval in [WATCHDOG_USEC/2 - 10%,
    //   WATCHDOG_USEC/2] rather than asserting exact equality.
    //
    // SPEC GAP: the watchdog cadence policy (exact half vs jittered
    // half) is not pinned. Test as written assumes exact-half-period.
}

// End-to-end supervisor loop tests: drive the FULL daemon select! loop
// (boot → control commands → flow lifecycle → panic-respawn → SIGHUP
// reload → signal-driven shutdown) against scripted poll + dispatch
// executors injected via `gcit::flow::supervisor::run_with_factories`.
//
// Mirrors the per-component seams in `tests/poll_unborn_ref.rs` and
// `tests/flow_dispatcher_executor.rs` (which call the
// `*::run_with_executor` / `handle_trigger_with_executor` seams
// directly), but exercises them through `spawn_flow` -> `JoinSet` ->
// the supervisor's select! loop, so the real registry / last_errors
// map / signal handlers / sd_notify / control wiring is what runs.
//
// Tests serialize via `#[serial_test::serial]` because they all set
// process-global env vars (STATE_DIRECTORY, RUNTIME_DIRECTORY,
// CREDENTIALS_DIRECTORY) and install SIGTERM/SIGHUP/SIGINT signal
// handlers — concurrent tests would clobber each other's env state
// and race on signal delivery.
//
// Per-theme tests live in submodules under
// `tests/supervisor_loop_factories/` — cargo compiles this file as one
// integration-test binary and the submodules participate via the
// `mod ...;` declarations below.

mod common;

#[path = "supervisor_loop_factories/boot.rs"]
mod boot;
#[path = "supervisor_loop_factories/control.rs"]
mod control;
#[path = "supervisor_loop_factories/errors.rs"]
mod errors;
#[path = "supervisor_loop_factories/fixtures.rs"]
mod fixtures;
#[path = "supervisor_loop_factories/reload.rs"]
mod reload;
#[path = "supervisor_loop_factories/respawn.rs"]
mod respawn;

// Daemon supervisor: top-level select! loop, signal handling,
// per-flow task management, config reload, control-server wiring,
// sd_notify lifecycle.
//
// Cross-cutting invariants (handler-specific prose lives in each
// submodule's header):
//   - Each flow runs as a child task tree under its own
//     CancellationToken (root.child_token()).
//   - Panics are caught inside the spawned future via
//     `std::panic::AssertUnwindSafe(...).catch_unwind()` so the flow
//     name is preserved on the JoinSet exit; JoinError::is_panic
//     alone would discard the per-task identity once the panic
//     propagates through the runtime.
//
// Module layout:
//   - run: daemon entry point, select! loop, shutdown sequence
//   - flows: per-flow spawn + notifier construction
//   - types: shared per-flow types + last-error recording
//   - respawn: panic-respawn decision + execution pipeline
//   - reload: SIGHUP config-diff + flow restart
//   - control: control-socket command dispatch
//   - credentials: per-credential resource pool

mod control;
mod credentials;
mod flows;
mod reload;
mod respawn;
mod run;
mod types;

pub use run::{run, DaemonError, DaemonParams, STATE_QUEUE};
pub(crate) use types::is_synthetic_daemon_key;
pub use types::FlowLastError;
// `record_last_error` is `#[doc(hidden)] pub` (see types.rs); the
// re-export here keeps the existing `crate::flow::supervisor::record_last_error`
// path used by `flow::poll` and `flow::dispatcher`. Integration tests
// reach the same path via `gcit::flow::supervisor::record_last_error`.
#[doc(hidden)]
pub use types::record_last_error;

// Test seam: `run_with_factories` lets the supervisor end-to-end test
// harness drive the FULL daemon select! loop (boot, reload, panic-
// respawn, control commands, signals) with caller-supplied
// `PollTaskFactory` / `DispatchTaskFactory` closures. Production
// callers go through `run`, which builds
// `production_{poll,dispatch}_task_factory()` and delegates here.
#[doc(hidden)]
pub use flows::{
    production_dispatch_task_factory, production_poll_task_factory, DispatchTaskFactory,
    PollTaskFactory,
};
#[doc(hidden)]
pub use run::run_with_factories;

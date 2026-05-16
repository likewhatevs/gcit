// Per-flow panic-respawn pipeline.
//
// Module layout:
//   - `decision` — `RespawnDecision` + `decide_respawn` pure classifier
//     (no side effects; unit-testable without standing up a JoinSet).
//   - `exit`     — `handle_flow_exit` side-effect handler that
//                  translates a classifier decision into mutations on
//                  `FlowRegistry`, last_errors, and the panic-watcher
//                  spawn.
//   - `request`  — `handle_respawn_request` drain-defer driver that
//                  consumes `RespawnRequest`s and either re-enqueues
//                  (old-gen pair not yet drained) or spawns the new
//                  generation.

use std::time::Duration;

mod decision;
mod exit;
mod request;

pub(super) use exit::handle_flow_exit;
pub(super) use request::handle_respawn_request;

/// Cap on `RespawnRequest::attempts`. With `RESPAWN_RETRY_INTERVAL`
/// = 1s and `RESPAWN_MAX_ATTEMPTS` = 30, the maximum drain-defer
/// wait is 30 seconds — symmetric with the 30s timeout in
/// `run_reload`'s drain block.
pub(super) const RESPAWN_MAX_ATTEMPTS: u32 = 30;

/// Polling interval between drain checks. Each retry re-enqueues
/// the request after this delay.
pub(super) const RESPAWN_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Per-flow respawn request. Carries the flow name plus an
/// `attempts` counter that bounds drain-deferral retries when the
/// old-gen pair has not fully exited the JoinSet. The supervisor's
/// select! responsiveness is preserved because each retry's timer
/// runs in its own spawned task.
pub(super) struct RespawnRequest {
    pub(super) flow: String,
    pub(super) attempts: u32,
}

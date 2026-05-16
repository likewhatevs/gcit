// SIGHUP / control-channel reload pipeline.
//
// Module layout (refactored from the prior single-file `reload.rs`):
//   - `action`  — `ReloadAction` enum + `compute_reload_actions`
//                 pure classifier.
//   - `diff`    — `flow_config_unchanged` + supporting helpers
//                 (canonical-multiset destination compare, kept
//                 credential collection, FlowDiff).
//   - `apply`   — `run_reload` orchestrator + integration tests.

mod action;
mod apply;
mod diff;

pub(super) use apply::run_reload;

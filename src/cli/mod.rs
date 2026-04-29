// CLI subcommand implementations.
//
// Every subcommand has a module here. `gcit run` is wired in
// `bin/gcit.rs` directly (it routes to `gcit::flow::run_daemon`
// rather than a `cli::*` helper) and `gcit completions` is also
// inline in `bin/gcit.rs` because the completion generator needs the
// constructed `clap::Command` from `Cli::command()` rather than an
// out-of-band helper.
//
// Items are pub for integration-test reachability — Rust's
// `pub(crate)` does not span the integration-test boundary, so every
// public name reachable here must be treated as crate-internal and
// unstable. (Same convention as `parse.rs`.)

pub mod check;
pub mod exit;
pub mod install;
pub mod reload;
pub mod status;
pub mod trigger;
pub mod uninstall;
pub mod validate_template;

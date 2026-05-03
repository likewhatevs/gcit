// gcit is a Linux + systemd daemon. The hard platform requirement is enforced
// at compile time so an accidental cross-build to macOS / Windows / BSD fails
// loudly during `cargo build` rather than producing a non-functional binary.
// macOS, Windows, and other Unix variants are not supported and `lib.rs`
// emits a `compile_error!` on non-Linux targets.
#[cfg(not(target_os = "linux"))]
compile_error!("gcit requires Linux + systemd. Other operating systems are not supported.");

// The library is not a published API surface; the only consumers are
// the `gcit` binary and the integration test harness under `tests/`.
// Integration tests link against the crate as an external dependency,
// so the items must be `pub` (Rust's `pub(crate)` does not span the
// integration-test boundary). Treat every name reachable here as
// crate-internal and unstable.
pub mod cli;
pub mod config;
pub mod control;
pub mod discord;
pub mod flow;
pub mod git;
pub mod github;
pub mod log;
pub mod mail;
pub mod notify;
pub mod state;
pub mod systemd;
pub mod util;

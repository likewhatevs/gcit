// systemd integration: unit-file rendering + daemon-reload trigger.
//
// The gcit library is not a published API; pub items here are
// crate-internal and unstable. Integration tests link across the
// boundary so `pub(crate)` is insufficient.

pub mod reload;
pub mod unit;

pub use reload::{trigger_daemon_reload, ReloadOutcome};
pub use unit::{
    install_paths, render_service_unit, render_socket_unit, InstallPaths, InstallScope,
};

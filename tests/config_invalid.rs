// Table-driven + per-section tests for invalid gcit config inputs.
//
// Each test loads a TOML body (inline or fixture file) and asserts a
// SPECIFIC ConfigError variant — Parse / Validate / TemplateCompile —
// with the field, value, line, and suggestion shape an operator
// would see. Pinning the diagnostic shape is the difference between
// a useful validator and a frustrating one.
//
// Per-theme tests live in submodules under `tests/config_invalid/`
// — cargo compiles this file as one integration-test binary and the
// submodules participate via the `mod ...;` declarations below.

#[path = "config_invalid/actions.rs"]
mod actions;
#[path = "config_invalid/common.rs"]
mod common;
#[path = "config_invalid/credentials.rs"]
mod credentials;
#[path = "config_invalid/destinations.rs"]
mod destinations;
#[path = "config_invalid/fixtures.rs"]
mod fixtures;
#[path = "config_invalid/flow_cadence_http.rs"]
mod flow_cadence_http;
#[path = "config_invalid/spool.rs"]
mod spool;
#[path = "config_invalid/templates.rs"]
mod templates;

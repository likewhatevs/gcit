// Black-box CLI tests for `gcit install` and `gcit uninstall` argument
// parsing + early-exit error paths.
//
// These tests drive the cargo-built `gcit` binary via assert_cmd and
// pin the clap-derived argument-parser surface (`--user`/`--system`
// mutex, `--non-interactive`, `--force`, `--dry-run`) plus the early
// CONFIG / USAGE / OSERR exit branches in `cli::install::run` and
// `cli::uninstall::run` that fire before any side effect against the
// filesystem.
//
// Coverage focus is the CLI surface — the deeper install/uninstall
// pipelines (manifest writing, systemd reload, useradd) live behind
// in-module unit tests in src/cli/install.rs and src/cli/uninstall.rs
// and behind the install_*.rs sibling integration files. This file
// covers the clap-parser layer + the two early-exit branches that
// don't write any state: missing config, manifest not found.
//
// Exit codes (per cli::exit / sysexits):
//   * 0   = OK
//   * 64  = USAGE       (EX_USAGE; clap parse error, missing arg group)
//   * 71  = OSERR       (EX_OSERR; HOME unresolvable, manifest schema
//                        mismatch, file-removal failure)
//   * 78  = CONFIG      (EX_CONFIG; config load failure on install,
//                        manifest read failure on uninstall)
//
// Per-theme tests live in submodules under `tests/cli_install_uninstall/`
// — cargo compiles this file as one integration-test binary and the
// submodules participate via `mod ...;` declarations below.

// `#[path]` attributes are required because Rust's default module
// resolution from `tests/cli_install_uninstall.rs` would look for
// `tests/common.rs` etc. (parallel files), not files inside the
// matching subdirectory. The path-attribute form is the standard
// way to keep all submodules colocated under one directory.
#[path = "cli_install_uninstall/common.rs"]
mod common;
#[path = "cli_install_uninstall/dry_run.rs"]
mod dry_run;
#[path = "cli_install_uninstall/help_and_args.rs"]
mod help_and_args;
#[path = "cli_install_uninstall/install_errors.rs"]
mod install_errors;
#[path = "cli_install_uninstall/install_lifecycle.rs"]
mod install_lifecycle;
#[path = "cli_install_uninstall/uninstall.rs"]
mod uninstall;
#[path = "cli_install_uninstall/walkthrough.rs"]
mod walkthrough;

// Shared helpers for the config_invalid submodules. Each test module
// pulls in `super::common::*` so the per-section tests stay focused
// on the diagnostic shape under test rather than TOML boilerplate.

#![allow(dead_code)]

use std::path::PathBuf;

pub fn fixture(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join(rel)
}

pub fn build_flow_with_name(name: &str) -> String {
    format!(
        r#"
[[flow]]
name = "{}"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
"#,
        name,
    )
}

pub fn build_with_jitter(j: f64) -> String {
    format!(
        r#"
[poll]
jitter = {}
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
"#,
        j,
    )
}

pub fn build_local_mail_user(user: &str) -> String {
    format!(
        r#"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
[[flow.destination]]
kind = "local_mail"
user = "{}"
"#,
        user,
    )
}

pub fn build_with_repo(repo: &str) -> String {
    format!(
        r#"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "{}"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
"#,
        repo,
    )
}

/// Workflow values are wrapped in TOML literal-string (single-quote)
/// quoting because some valid test values contain backslashes; TOML
/// basic-string (double-quote) syntax interprets backslash as an escape
/// character, so `workflow = "ci\extra.yml"` is a Parse error rather
/// than a workflow with a literal backslash.
pub fn build_with_workflow(workflow: &str) -> String {
    format!(
        r#"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = '{}'
ref = "refs/heads/main"
credential_id = "c"
"#,
        workflow,
    )
}

pub fn build_with_max_concurrent(n: usize) -> String {
    format!(
        r#"
[http]
max_concurrent = {}
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
"#,
        n,
    )
}

pub fn build_with_discord_title(title: &str) -> String {
    format!(
        r#"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
[[flow.destination]]
kind = "discord_webhook"
credential_id = "c"
[flow.destination.template]
title = "{}"
"#,
        title,
    )
}

pub fn build_with_http_request_timeout(timeout: &str) -> String {
    format!(
        r#"
[http]
request_timeout = "{}"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
"#,
        timeout,
    )
}

pub fn local_mail_only_config(user: &str) -> String {
    format!(
        r#"
[[flow]]
name = "spool-test-flow"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "c"
[[flow.destination]]
kind = "local_mail"
user = "{}"
"#,
        user,
    )
}

pub fn build_with_credential_id(id: &str) -> String {
    format!(
        r#"
[[flow]]
name = "x"
[flow.source]
url = "https://git.kernel.org/x.git"
ref = "refs/heads/master"
[flow.action]
kind = "github_workflow_dispatch"
repo = "o/r"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "{}"
"#,
        id,
    )
}

/// True if the running process's effective uid is 0. Used to skip
/// tests that depend on DAC behavior root bypasses.
pub fn euid_is_root() -> bool {
    // SAFETY: geteuid() is async-signal-safe and always succeeds.
    unsafe { libc::geteuid() == 0 }
}

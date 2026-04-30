// `gcit status` — per-flow status snapshot via the control socket.
//
// Behavior: open a one-shot Client, send Request::Status, render the
// daemon's response as either text (default) or json. Exits 0 on
// success or EX_TEMPFAIL=75 when the daemon is unreachable / errors.

use std::path::Path;
use std::process::ExitCode;

use clap::ValueEnum;
use uuid::Uuid;

use crate::cli::exit;
use crate::control::{Client, Request, Response};

/// Output format for `gcit status`.
#[derive(Debug, Clone, Copy, ValueEnum)]
#[clap(rename_all = "kebab-case")]
pub enum Format {
    Text,
    Json,
}

/// Run `gcit status [FLOW] [--format text/json]`.
///
/// Async because the control client uses tokio I/O. Callers run inside
/// the binary's tokio runtime.
pub async fn run(socket_path: &Path, flow: Option<String>, format: Format) -> ExitCode {
    let mut client = match Client::connect(socket_path).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "gcit status: cannot connect to {}: {} (is the daemon running?)",
                socket_path.display(),
                e
            );
            return ExitCode::from(exit::TEMPFAIL);
        }
    };
    let id = Uuid::new_v4();
    let req = Request::Status { id, flow };
    match client.send(req).await {
        Ok(Response::Ok { data, .. }) => {
            match format {
                Format::Json => {
                    // Surface the daemon's JSON shape verbatim (the CLI
                    // does not re-shape; the daemon owns the schema).
                    let s = match serde_json::to_string_pretty(&data) {
                        Ok(s) => s,
                        Err(e) => {
                            eprintln!("gcit status: serialize: {}", e);
                            return ExitCode::from(exit::TEMPFAIL);
                        }
                    };
                    println!("{}", s);
                }
                Format::Text => {
                    render_text(&data);
                }
            }
            ExitCode::from(exit::OK)
        }
        Ok(Response::Error { message, .. }) => {
            eprintln!("gcit status: {}", message);
            ExitCode::from(exit::TEMPFAIL)
        }
        Err(e) => {
            eprintln!("gcit status: transport error: {}", e);
            ExitCode::from(exit::TEMPFAIL)
        }
    }
}

/// Best-effort text render of the daemon's status JSON. The daemon owns
/// the canonical schema; the CLI is intentionally lenient — it walks
/// whatever shape it gets and prints flow names + summary fields.
///
/// The daemon's payload shape: an object keyed by flow name, each
/// value carrying `state`, `last_sha`, `last_poll_at`, `active_runs`,
/// `notified_runs`, and `last_error` (object with `at`, `kind`,
/// `message`, optional `retry_at`, OR JSON `null` when no error has
/// been recorded). The renderer prints "<flow>: <state>" plus
/// indented summary lines for the populated fields.
///
/// `last_error.retry_at` is emitted only for `GithubErrorKind::
/// RateLimited` errors (the dispatcher extracts the quota window
/// reset from response headers and routes it through
/// `record_last_error` into `FlowLastError.retry_at`). For every
/// other error kind it is JSON `null` and the renderer hides the
/// indented "retry_at" line.
///
/// Synthetic daemon-level keys (any key wrapped in parentheses such as
/// `(reload)`) are not flow names — they carry daemon-scoped errors the
/// supervisor records under a sentinel key so `gcit status` surfaces
/// them alongside per-flow entries. They are printed with a `[daemon]`
/// prefix on the header line so an operator scanning the output can
/// tell at a glance that the entry is not a flow they configured.
fn render_text(data: &serde_json::Value) {
    if let serde_json::Value::Object(map) = data {
        if map.is_empty() {
            println!("(no flows)");
            return;
        }
        for (flow, value) in map {
            let state = value
                .get("state")
                .and_then(|v| v.as_str())
                .unwrap_or("(unknown)");
            // Synthetic daemon-level keys are wrapped in parens (e.g.
            // "(reload)"). Prefix the header with [daemon] so an
            // operator does not mistake a sentinel for a flow name.
            // The convention itself lives in flow::supervisor as
            // `is_synthetic_daemon_key` so all consumers stay in
            // lockstep with the producer.
            if crate::flow::supervisor::is_synthetic_daemon_key(flow) {
                println!("[daemon] {}: {}", flow, state);
            } else {
                println!("{}: {}", flow, state);
            }
            if let Some(sha) = value.get("last_sha").and_then(|v| v.as_str()) {
                println!("  last_sha: {}", sha);
            }
            if let Some(at) = value.get("last_poll_at").and_then(|v| v.as_str()) {
                println!("  last_poll_at: {}", at);
            }
            if let Some(active) = value.get("active_runs").and_then(|v| v.as_u64()) {
                if active > 0 {
                    println!("  active_runs: {}", active);
                }
            }
            if let Some(notified) = value.get("notified_runs").and_then(|v| v.as_u64()) {
                if notified > 0 {
                    println!("  notified_runs: {}", notified);
                }
            }
            // The daemon writes JSON `null` for `last_error` when no
            // error is recorded (flow/supervisor/control.rs's
            // render_one). Match explicitly on Object so a Null value
            // does not produce a "?" placeholder line. `retry_at` is
            // JSON `null` for every error kind except
            // `GithubErrorKind::RateLimited`; hide the indented
            // "retry_at" line when absent.
            if let Some(serde_json::Value::Object(err)) = value.get("last_error") {
                let at = err.get("at").and_then(|v| v.as_str()).unwrap_or("?");
                let kind = err.get("kind").and_then(|v| v.as_str()).unwrap_or("?");
                let msg = err.get("message").and_then(|v| v.as_str()).unwrap_or("");
                println!("  last_error[{}] {}: {}", kind, at, msg);
                if let Some(retry_at) = err.get("retry_at").and_then(|v| v.as_str()) {
                    println!("    retry_at: {}", retry_at);
                }
            }
        }
    } else {
        // Fallback: dump whatever the daemon sent.
        println!("{}", data);
    }
}

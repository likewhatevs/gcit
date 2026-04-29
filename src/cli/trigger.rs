// `gcit trigger` — manually fire a flow's dispatch (or render a
// dry-run payload).
//
// Behavior: open a one-shot Client, send Request::Trigger, exit 0 on
// Response::Ok. Daemon-side errors (unknown flow, dispatch failure)
// surface as Response::Error and map to EX_TEMPFAIL=75 — same rule as
// reload/status: anything the CLI can do is "try again later."

use std::path::Path;
use std::process::ExitCode;

use uuid::Uuid;

use crate::cli::exit;
use crate::control::{Client, Request, Response};

/// Run `gcit trigger <FLOW> [--dry-run]`.
///
/// Async because the control client uses tokio I/O. Callers run inside
/// the binary's tokio runtime.
pub async fn run(socket_path: &Path, flow: String, dry_run: bool) -> ExitCode {
    if flow.is_empty() {
        eprintln!("gcit trigger: FLOW name is required and must not be empty");
        return ExitCode::from(exit::USAGE);
    }
    let mut client = match Client::connect(socket_path).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "gcit trigger: cannot connect to {}: {} (is the daemon running?)",
                socket_path.display(),
                e
            );
            return ExitCode::from(exit::TEMPFAIL);
        }
    };
    let id = Uuid::new_v4();
    let req = Request::Trigger { id, flow, dry_run };
    match client.send(req).await {
        Ok(Response::Ok { data, .. }) => {
            // Surface whatever the daemon returned — for a dry-run
            // this is the rendered payload; for a real fire it's a
            // confirmation object. Either way, JSON pretty-print.
            match serde_json::to_string_pretty(&data) {
                Ok(s) => println!("{}", s),
                Err(e) => {
                    eprintln!("gcit trigger: serialize response: {}", e);
                    return ExitCode::from(exit::TEMPFAIL);
                }
            }
            ExitCode::from(exit::OK)
        }
        Ok(Response::Error { message, .. }) => {
            eprintln!("gcit trigger: {}", message);
            ExitCode::from(exit::TEMPFAIL)
        }
        Err(e) => {
            eprintln!("gcit trigger: transport error: {}", e);
            ExitCode::from(exit::TEMPFAIL)
        }
    }
}

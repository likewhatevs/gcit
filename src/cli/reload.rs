// `gcit reload` — send Reload over the control socket.
//
// `gcit reload` and SIGHUP run the same daemon path.
//
// Behavior: open a one-shot Client to the daemon's control socket,
// send Request::Reload, exit 0 on Response::Ok or 75 (EX_TEMPFAIL) on
// any other outcome (Response::Error, transport failure, daemon not
// running, rate-limit). 75 means "transient — try again later," which
// covers both daemon-not-running and the 1/sec reload rate-limit.

use std::path::Path;
use std::process::ExitCode;

use uuid::Uuid;

use crate::cli::exit;
use crate::control::{Client, Request, Response};

/// Run `gcit reload`.
///
/// `socket_path` is the absolute path to the daemon's control socket
/// (resolved by the binary entry point from $XDG_RUNTIME_DIR or
/// /run/gcit, depending on scope; the CLI does not infer scope).
///
/// Async because the control client uses tokio I/O. Callers run inside
/// the binary's tokio runtime.
pub async fn run(socket_path: &Path) -> ExitCode {
    let mut client = match Client::connect(socket_path).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "gcit reload: cannot connect to {}: {} (is the daemon running?)",
                socket_path.display(),
                e
            );
            return ExitCode::from(exit::TEMPFAIL);
        }
    };
    let id = Uuid::new_v4();
    match client.send(Request::Reload { id }).await {
        Ok(Response::Ok { .. }) => ExitCode::from(exit::OK),
        Ok(Response::Error { message, .. }) => {
            eprintln!("gcit reload: {}", message);
            ExitCode::from(exit::TEMPFAIL)
        }
        Err(e) => {
            eprintln!("gcit reload: transport error: {}", e);
            ExitCode::from(exit::TEMPFAIL)
        }
    }
}

// Control protocol wire types.
//
// Wire format: tokio_util::codec::LengthDelimitedCodec configured with
// 4-byte big-endian u32 prefix (the codec default) and a 64 KiB cap
// (overriding the 8 MiB default — control messages are tiny). Body is
// serde_json bytes.
//
// Request and Response are externally tagged enums. Every variant
// carries a `uuid::Uuid` so a client can correlate replies to in-flight
// requests over a single connection.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Maximum frame size for the control codec. Control messages are
/// status/trigger/reload/version requests (no streaming payloads),
/// so 64 KiB is a generous ceiling that prevents an unbounded length
/// prefix from triggering a multi-MB allocation before the body is
/// read.
pub const MAX_FRAME_LEN: usize = 64 * 1024;

/// Per-frame read deadline. 5s as a slow-loris-style defense — a
/// peer that stalls between sending a length prefix and the matching
/// body has no legitimate workload, and holding the accept slot
/// indefinitely allows trivial DoS against the control surface.
pub const READ_TIMEOUT_SECS: u64 = 5;

/// A request from a control-channel peer to the daemon.
///
/// Each variant is dispatched server-side to a specific handler.
/// Unknown variants on the wire surface as a serde error and are
/// turned into `Response::Error` by the server before the connection
/// is dropped.
///
/// `deny_unknown_fields` rejects extra fields outside the declared
/// variant payload (a forked/older client sending an unknown field
/// gets a clear deserialize error rather than a silent drop).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    /// Manually fire a flow's dispatch path. `dry_run = true` returns
    /// the rendered payload(s) without contacting GitHub or any
    /// notifier. `dry_run = false` triggers the real path.
    ///
    /// `dry_run` is required on the wire — a client that omits the
    /// field gets a deserialization error rather than silently
    /// triggering a real dispatch under the false default.
    Trigger {
        id: Uuid,
        flow: String,
        dry_run: bool,
    },
    /// Per-flow status snapshot. When `flow` is `None`, returns every
    /// flow.
    Status {
        id: Uuid,
        #[serde(default)]
        flow: Option<String>,
    },
    /// Reload the daemon's configuration. Equivalent to SIGHUP; both
    /// paths run the identical `reload()` function. Rate-limited to
    /// 1 per second across all peers.
    Reload { id: Uuid },
    /// Returns the daemon's `gcit --version` string verbatim.
    Version { id: Uuid },
}

impl Request {
    /// Convenience: every request carries an id; this returns it
    /// regardless of variant.
    pub fn id(&self) -> Uuid {
        match self {
            Request::Trigger { id, .. } => *id,
            Request::Status { id, .. } => *id,
            Request::Reload { id } => *id,
            Request::Version { id } => *id,
        }
    }
}

/// A response from the daemon. `id` MUST echo the originating
/// request's id so a client multiplexing several requests over one
/// connection can correlate replies. Body shape is variant-defined;
/// `data` is intentionally `serde_json::Value` so future request types
/// can extend the response without protocol-level change.
///
/// `deny_unknown_fields` rejects extra fields outside the declared
/// variant payload — symmetric with `Request` above.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Response {
    Ok {
        id: Uuid,
        #[serde(default)]
        data: serde_json::Value,
    },
    Error {
        id: Uuid,
        message: String,
    },
}

impl Response {
    /// Convenience: every response carries an id; this returns it
    /// regardless of variant.
    pub fn id(&self) -> Uuid {
        match self {
            Response::Ok { id, .. } => *id,
            Response::Error { id, .. } => *id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trip every Request variant through serde_json. Catches
    /// renames/typos in the `kind` discriminator without booting a
    /// runtime.
    #[test]
    fn request_round_trip() {
        let id = Uuid::nil();
        let cases = [
            Request::Trigger {
                id,
                flow: "x".into(),
                dry_run: true,
            },
            Request::Status {
                id,
                flow: Some("x".into()),
            },
            Request::Status { id, flow: None },
            Request::Reload { id },
            Request::Version { id },
        ];
        for req in &cases {
            let bytes = serde_json::to_vec(req).expect("serialize");
            let back: Request = serde_json::from_slice(&bytes).expect("deserialize");
            assert_eq!(format!("{:?}", req), format!("{:?}", back));
            assert_eq!(req.id(), back.id());
        }
    }

    #[test]
    fn response_round_trip() {
        let id = Uuid::nil();
        let cases = [
            Response::Ok {
                id,
                data: serde_json::json!({"ok": true}),
            },
            Response::Error {
                id,
                message: "boom".into(),
            },
        ];
        for resp in &cases {
            let bytes = serde_json::to_vec(resp).expect("serialize");
            let back: Response = serde_json::from_slice(&bytes).expect("deserialize");
            assert_eq!(format!("{:?}", resp), format!("{:?}", back));
            assert_eq!(resp.id(), back.id());
        }
    }

    #[test]
    fn status_flow_omitted_round_trips_to_none() {
        let bytes = br#"{"kind":"status","id":"00000000-0000-0000-0000-000000000000"}"#;
        let req: Request = serde_json::from_slice(bytes).expect("deserialize");
        match req {
            Request::Status { flow, .. } => assert!(flow.is_none()),
            other => panic!("expected Status, got {:?}", other),
        }
    }

    #[test]
    fn trigger_dry_run_is_required() {
        // Omitting `dry_run` on the wire must fail deserialization
        // rather than defaulting to false and silently triggering a
        // real dispatch.
        let bytes = br#"{"kind":"trigger","id":"00000000-0000-0000-0000-000000000000","flow":"x"}"#;
        let r: Result<Request, _> = serde_json::from_slice(bytes);
        assert!(
            r.is_err(),
            "Trigger without dry_run must reject; got Ok({:?})",
            r.ok()
        );
    }

    #[test]
    fn unknown_kind_rejected() {
        let bytes = br#"{"kind":"banana","id":"00000000-0000-0000-0000-000000000000"}"#;
        let r: Result<Request, _> = serde_json::from_slice(bytes);
        assert!(r.is_err(), "unknown kind must reject");
    }
}

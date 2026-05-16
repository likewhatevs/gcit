// State-mirror readers + panic-payload conversion. Used by
// `spawn_flow` to seed the per-flow poll loop's in-memory baselines
// and by the `catch_unwind` wrapper to surface a printable panic
// message in `last_error`.

use std::sync::{Arc, Mutex as StdMutex};

use chrono::{DateTime, Utc};

use crate::state::State;

/// Read the persisted `last_sha` for `flow_name`. Used to seed the
/// per-flow poll loop's in-memory `last_sha` so a daemon restart
/// against an unchanged source does not fire a spurious
/// `TriggerSignal` on the first poll cycle. Returns `None` when:
/// the flow has no observation yet, the stored hex is malformed,
/// or the mutex is poisoned (defense-in-depth — a poisoned mutex
/// would also fail loudly elsewhere via the writer thread).
pub(super) fn read_persisted_last_sha(
    state_mirror: &Arc<StdMutex<State>>,
    flow_name: &str,
) -> Option<gix_hash::ObjectId> {
    let guard = state_mirror.lock().ok()?;
    let entry = guard.flows.get(flow_name)?;
    let hex = entry.last_sha.as_ref()?;
    gix_hash::ObjectId::from_hex(hex.as_bytes()).ok()
}

/// Read the persisted `last_dispatched_at` for `flow_name`. Used to
/// seed the per-flow poll loop's in-memory cooldown clock so a
/// daemon restart inherits the prior cooldown window. Without this,
/// every flow's first post-restart SHA-diff would dispatch
/// immediately even if the pre-restart dispatch was seconds ago.
pub(super) fn read_persisted_last_dispatched_at(
    state_mirror: &Arc<StdMutex<State>>,
    flow_name: &str,
) -> Option<DateTime<Utc>> {
    let guard = state_mirror.lock().ok()?;
    let entry = guard.flows.get(flow_name)?;
    entry.last_dispatched_at
}

/// Best-effort downcast of a panic payload to a printable string.
/// `panic!("&'static str literal")` produces a `&'static str` payload;
/// `panic!("formatted {}", x)` allocates a `String`; `panic_any` with
/// arbitrary types falls through to the placeholder so `last_error`
/// always has SOMETHING surfaced.
pub(super) fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "(panic payload not stringifiable)".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::FlowState;

    #[test]
    fn panic_payload_to_string_recovers_static_str() {
        // `panic!("&'static str literal")` produces a `&'static str`
        // payload — pin the static-str downcast arm directly.
        let payload: Box<dyn std::any::Any + Send> = Box::new("static panic message");
        assert_eq!(
            panic_payload_to_string(payload),
            "static panic message".to_string(),
        );
    }

    #[test]
    fn panic_payload_to_string_recovers_owned_string() {
        // `panic!("owned: {}", x)` with format args allocates a String.
        // Cover both downcast arms so a regression that breaks one
        // doesn't survive on the other.
        let payload: Box<dyn std::any::Any + Send> = Box::new("owned panic message".to_string());
        assert_eq!(
            panic_payload_to_string(payload),
            "owned panic message".to_string(),
        );
    }

    #[test]
    fn panic_payload_to_string_falls_back_for_arbitrary_payload() {
        // panic_any with a non-stringy value lands in the catch-all
        // arm. Fallback string is operator-readable so last_error
        // always has SOMETHING surfaced.
        let payload: Box<dyn std::any::Any + Send> = Box::new(42u64);
        assert_eq!(
            panic_payload_to_string(payload),
            "(panic payload not stringifiable)".to_string(),
        );
    }

    #[test]
    fn read_persisted_last_sha_returns_some_when_state_has_valid_hex() {
        let mut s = State::default();
        s.flows.insert(
            "flow1".to_string(),
            FlowState {
                last_sha: Some("deadbeefcafe1234567890abcdef1234567890ab".to_string()),
                ..Default::default()
            },
        );
        let mirror = Arc::new(StdMutex::new(s));
        let sha = read_persisted_last_sha(&mirror, "flow1").expect("must be Some");
        assert_eq!(sha.to_string(), "deadbeefcafe1234567890abcdef1234567890ab");
    }

    #[test]
    fn read_persisted_last_sha_returns_none_when_flow_absent() {
        let mirror = Arc::new(StdMutex::new(State::default()));
        assert!(read_persisted_last_sha(&mirror, "missing-flow").is_none());
    }

    #[test]
    fn read_persisted_last_sha_returns_none_when_flow_has_no_sha_field() {
        // FlowState exists but last_sha is None — the seed must be
        // None so in-memory baseline matches on-disk.
        let mut s = State::default();
        s.flows.insert(
            "flow1".to_string(),
            FlowState {
                last_sha: None,
                ..Default::default()
            },
        );
        let mirror = Arc::new(StdMutex::new(s));
        assert!(read_persisted_last_sha(&mirror, "flow1").is_none());
    }

    #[test]
    fn read_persisted_last_sha_returns_none_when_stored_hex_is_malformed() {
        // Defense-in-depth: hand-edited state.json could slip a
        // non-hex string past the reader. Return None (rather than
        // erroring) so spawn_flow proceeds without a baseline; the
        // next poll observation overwrites the bad value.
        let mut s = State::default();
        s.flows.insert(
            "flow1".to_string(),
            FlowState {
                last_sha: Some("not-valid-hex".to_string()),
                ..Default::default()
            },
        );
        let mirror = Arc::new(StdMutex::new(s));
        assert!(read_persisted_last_sha(&mirror, "flow1").is_none());
    }

    #[test]
    fn read_persisted_last_dispatched_at_returns_some_when_state_has_value() {
        let ts = DateTime::parse_from_rfc3339("2026-05-01T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut s = State::default();
        s.flows.insert(
            "flow1".to_string(),
            FlowState {
                last_dispatched_at: Some(ts),
                ..Default::default()
            },
        );
        let mirror = Arc::new(StdMutex::new(s));
        assert_eq!(
            read_persisted_last_dispatched_at(&mirror, "flow1"),
            Some(ts),
        );
    }

    #[test]
    fn read_persisted_last_dispatched_at_returns_none_when_field_absent() {
        let mut s = State::default();
        s.flows.insert(
            "flow1".to_string(),
            FlowState {
                last_dispatched_at: None,
                ..Default::default()
            },
        );
        let mirror = Arc::new(StdMutex::new(s));
        assert!(read_persisted_last_dispatched_at(&mirror, "flow1").is_none());
    }

    #[test]
    fn read_persisted_last_dispatched_at_returns_none_when_flow_absent() {
        let mirror = Arc::new(StdMutex::new(State::default()));
        assert!(read_persisted_last_dispatched_at(&mirror, "missing-flow").is_none());
    }
}

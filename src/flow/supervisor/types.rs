// Shared per-flow types and the last-error tracker.
//
// `FlowRegistry` bundles the three collections every supervisor entry
// point (`flows::spawn_flow`, `reload::run_reload`,
// `respawn::handle_flow_exit`, `respawn::handle_respawn_request`,
// `control::handle_control_command`) needs to mutate together. The
// `FlowHandle` per-flow record (cancel token + trigger sender) and the
// `FlowExit` shape used by the JoinSet also live here so every other
// sub-module can name them without a cycle.
//
// `FlowLastError` is the per-flow last-error record surfaced via
// `gcit status`; `record_last_error` is the shared insert path used by
// every producer (poll, dispatcher, supervisor itself). The
// `RELOAD_SYNTHETIC_KEY` synthetic key + `is_synthetic_daemon_key`
// classifier sit here because `record_last_error` writes the synthetic
// key during a failed config reload.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Synthetic last_errors key the supervisor records when a SIGHUP
/// reload fails to parse the new config. Surfaced via `gcit status`
/// alongside per-flow entries so an operator running SIGHUP after a
/// typo gets feedback about the silent no-op rather than seeing the
/// daemon "still running" with no error trace.
///
/// Convention: every daemon-scoped synthetic key is wrapped in
/// parentheses so it cannot collide with a real flow id (validate.rs
/// rejects parens at config load via the `[a-zA-Z0-9_-]+` rule).
/// `is_synthetic_daemon_key` walks that convention.
pub(crate) const RELOAD_SYNTHETIC_KEY: &str = "(reload)";

/// True when `name` is a daemon-scoped synthetic last_errors key
/// rather than a real flow name. Used at every consumer (status text
/// renderer, status JSON render_one, etc.) so the convention stays
/// pinned in one place.
pub(crate) fn is_synthetic_daemon_key(name: &str) -> bool {
    name.starts_with('(') && name.ends_with(')')
}

/// Per-flow dispatcher / poll handle pair tracked by the supervisor.
/// The supervisor uses these to issue per-flow cancellation during
/// reload (config removed the flow) and to look up triggers when
/// `gcit trigger <flow>` lands via the control surface.
pub(super) struct FlowHandle {
    /// Cancels the dispatcher + poll tasks for this flow.
    pub(super) cancel: CancellationToken,
    /// Used to inject a synthetic `TriggerSignal` from the
    /// `gcit trigger` control command into the dispatcher's mpsc.
    pub(super) trigger_tx: mpsc::Sender<crate::flow::TriggerSignal>,
}

/// Per-flow task exit shape for the supervisor's JoinSet. Carries
/// the flow name + role + the panic-payload-as-string when applicable
/// so the supervisor's panic-respawn path knows which flow to re-add
/// — JoinError::is_panic alone discards the per-task identity once
/// the panic propagates through tokio's runtime.
#[derive(Debug)]
pub(super) struct FlowExit {
    pub(super) flow: String,
    pub(super) role: FlowRole,
    /// `None` on a clean exit (cancellation or natural completion);
    /// `Some(message)` when the inner future panicked. The supervisor
    /// uses `Some` to drive the respawn path with a fresh config
    /// snapshot.
    pub(super) panic: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum FlowRole {
    Poll,
    Dispatcher,
}

/// Per-flow registry of the three collections every supervisor entry
/// point (handle_flow_exit, run_reload, handle_control_command,
/// handle_respawn_request) needs to mutate together:
///   - `handles`: live flow's FlowHandle (cancel token + trigger_tx).
///   - `respawning_flows`: flows whose panic-watcher is sleeping
///     RESPAWN_DELAY before enqueueing a respawn request.
///   - `pending_exits`: count of unobserved old-gen task exits per
///     flow (used by handle_respawn_request as a drain gate before
///     spawning the new generation).
///
/// Bundling these three eliminates argument threading and makes the
/// invariants between them more visible: every spawn increments
/// pending_exits[flow] by 2 and inserts into handles[flow]; every
/// observed exit decrements pending_exits[flow]; respawning_flows is
/// an orthogonal panic-respawn flag.
///
/// Not bundled here: `last_errors` (separately tracked because poll
/// + dispatcher + supervisor all write to it from different
///   task contexts and it lives behind an `Arc<Mutex<...>>` to allow
///   concurrent updates), `join_set` (mutable JoinSet that can't share
///   the same `&mut self` borrow as the registry), and the control
///   handler's `flow_names` (separate read-side mirror behind an
///   `Arc<RwLock<...>>` so the control thread can read names while the
///   supervisor thread mutates `handles`).
pub(super) struct FlowRegistry {
    pub(super) handles: BTreeMap<String, FlowHandle>,
    pub(super) respawning_flows: BTreeSet<String>,
    pub(super) pending_exits: BTreeMap<String, u8>,
}

impl FlowRegistry {
    pub(super) fn new() -> Self {
        Self {
            handles: BTreeMap::new(),
            respawning_flows: BTreeSet::new(),
            pending_exits: BTreeMap::new(),
        }
    }
}

/// Per-flow last-error tracking. Carried in a `BTreeMap` keyed by
/// flow name and surfaced via the control surface's `Status` reply.
///
/// `pub` so:
///   1. `flow::poll` can name the type in its `PollParams` to clear
///      stale entries on the first successful poll observation after
///      a respawn (sticky-error fix).
///   2. The supervisor end-to-end test harness in
///      tests/poll_unborn_ref.rs can read entries the loop records
///      via the accessor methods below.
///
/// Fields are kept private so the JSON shape over the control
/// surface stays owned by `flow/supervisor/control.rs`'s
/// `render_one` serializer — tests read via the accessors.
#[derive(Debug, Clone)]
pub struct FlowLastError {
    at: chrono::DateTime<chrono::Utc>,
    kind: String,
    message: String,
    /// When the daemon expects the error condition to clear. Currently
    /// populated only for `GithubErrorKind::RateLimited` (carries the
    /// quota window's reset epoch from the response headers). `None`
    /// for every other error kind — operators reading
    /// `gcit status` see a hint about when to expect dispatch retries
    /// rather than guessing at the next poll cadence.
    retry_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl FlowLastError {
    /// `at` accessor — wall-clock instant the error was recorded.
    /// Surfaced for the supervisor end-to-end test harness so tests
    /// can assert ordering / freshness against the production
    /// timestamp without reaching into private fields.
    pub fn at(&self) -> chrono::DateTime<chrono::Utc> {
        self.at
    }

    /// `kind` accessor for the supervisor end-to-end test harness in
    /// tests/poll_unborn_ref.rs. The fields are kept private so the
    /// JSON shape over the control surface is owned by
    /// flow/supervisor/control.rs's render_one serializer; tests get
    /// read-only views via these accessors.
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// `message` accessor, see `kind`.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// `retry_at` accessor, see `kind`.
    pub fn retry_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.retry_at
    }
}

/// Insert a `FlowLastError` for `flow` and emit a `tracing::warn!`
/// event with `kind`, `flow`, `body`, and `retry_at` as fields.
///
/// Every error producer (poll, dispatcher, supervisor itself) routes
/// through this function so the tracing event is the single source of
/// truth for "an error was just recorded against this flow." Operators
/// reading journalctl see the same `kind` discriminator the control
/// surface returns, with the same `flow` correlation key — no extra
/// log statements at each call site.
///
/// The error body is recorded under the `body` field rather than
/// `message`: `message` is tracing's reserved canonical-message field
/// and overlapping it with a struct field can shadow the tracing
/// event's literal message body in some fmt layers (the emit's "last_
/// error recorded" string above the field list).
///
/// `#[doc(hidden)] pub` so integration tests under `tests/` can drive
/// the tracing-event side of this path via `#[traced_test]`. Mirrors
/// the existing test-seam pattern in `flow::dispatcher` / `flow::monitor`.
#[doc(hidden)]
pub async fn record_last_error(
    map: &Arc<Mutex<BTreeMap<String, FlowLastError>>>,
    flow: &str,
    kind: &str,
    message: &str,
    retry_at: Option<chrono::DateTime<chrono::Utc>>,
) {
    warn!(
        target: "gcit::supervisor",
        flow = %flow,
        kind = %kind,
        body = %message,
        retry_at = ?retry_at,
        "last_error recorded",
    );
    let mut g = map.lock().await;
    g.insert(
        flow.to_string(),
        FlowLastError {
            at: chrono::Utc::now(),
            kind: kind.to_string(),
            message: message.to_string(),
            retry_at,
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_test::traced_test;

    #[tokio::test]
    async fn record_last_error_inserts_with_supplied_kind_and_message() {
        let map = Arc::new(Mutex::new(BTreeMap::new()));
        record_last_error(&map, "flow-x", "git_poll_failed", "boom", None).await;
        let g = map.lock().await;
        let entry = g.get("flow-x").expect("entry present");
        assert_eq!(entry.kind, "git_poll_failed");
        assert_eq!(entry.message, "boom");
        assert!(entry.retry_at.is_none());
    }

    /// Pins the tracing-event side of `record_last_error`. `#[traced_test]`
    /// installs a global subscriber that captures events into an in-memory
    /// buffer; `logs_contain` searches lines tagged with this test's span.
    /// Asserting both the `kind=` field and the literal "last_error recorded"
    /// message body proves the production tracing payload an operator sees
    /// in journalctl matches the control-surface `last_error.kind` they see
    /// via `gcit status`.
    #[tokio::test]
    #[traced_test]
    async fn record_last_error_emits_tracing_warn_with_kind_field() {
        let map = Arc::new(Mutex::new(BTreeMap::new()));
        record_last_error(&map, "flow-panic", "panic", "boom in dispatcher", None).await;
        // tracing-subscriber's default fmt layer renders `Display`-formatted
        // fields as `name=value` (no surrounding quotes); only Debug-formatted
        // values are quoted. record_last_error emits with %-formatting (Display)
        // so the captured line shape is e.g.
        //   `WARN ...: last_error recorded flow=flow-panic kind=panic ...`
        //
        // Pin: the kind discriminator (operator triage), the flow key
        // (correlation), and the literal "last_error recorded" message body
        // that proves the assertion hit the canonical recording event rather
        // than some incidental tracing emit.
        assert!(
            logs_contain("kind=panic"),
            "tracing event must surface the `kind=panic` field",
        );
        assert!(
            logs_contain("flow=flow-panic"),
            "tracing event must surface the `flow` correlation key",
        );
        assert!(
            logs_contain("last_error recorded"),
            "tracing event must surface the canonical message body",
        );
    }

    /// Pin the `body=` and `retry_at=` field shapes the tracing event
    /// emits when a caller supplies `Some(retry_at)`. The production
    /// emit names the message body under `body` (deliberately not
    /// `message`, which would shadow tracing's reserved canonical-message
    /// field — see comment above record_last_error). `retry_at` uses
    /// `?` (Debug) formatting so the captured shape is `retry_at=Some(...)`
    /// for the populated case and `retry_at=None` for the empty one.
    /// Asserting both exact substrings catches a regression that
    /// reverted the rename back to `message=` or dropped the retry_at
    /// field entirely.
    #[tokio::test]
    #[traced_test]
    async fn record_last_error_emits_tracing_body_and_retry_at_fields() {
        let map = Arc::new(Mutex::new(BTreeMap::new()));
        let reset = chrono::DateTime::parse_from_rfc3339("2026-04-28T14:00:00Z")
            .expect("valid rfc3339")
            .with_timezone(&chrono::Utc);
        record_last_error(&map, "flow-rate", "github_error", "rate limited", Some(reset)).await;
        assert!(
            logs_contain("body="),
            "tracing event must surface the message body under the `body=` field \
             (NOT `message=` — `message` would shadow tracing's reserved \
             canonical-message field)",
        );
        assert!(
            logs_contain("retry_at=Some"),
            "Some(retry_at) must render via Debug formatting as `retry_at=Some(...)`",
        );
    }

    #[tokio::test]
    async fn record_last_error_propagates_retry_at_when_supplied() {
        // The rate-limit dispatcher path passes Some(reset_at) so
        // `gcit status` can show operators when dispatch will resume.
        // A None retry_at would render as an unhelpful "rate limited
        // until ?" — pin that the supplied value is preserved verbatim.
        let map = Arc::new(Mutex::new(BTreeMap::new()));
        let reset = chrono::DateTime::parse_from_rfc3339("2026-04-28T12:00:00Z")
            .expect("valid rfc3339")
            .with_timezone(&chrono::Utc);
        record_last_error(
            &map,
            "flow-rate",
            "github_error",
            "rate limited",
            Some(reset),
        )
        .await;
        let g = map.lock().await;
        let entry = g.get("flow-rate").expect("entry present");
        assert_eq!(entry.retry_at, Some(reset));
    }

    #[tokio::test]
    async fn record_last_error_overwrites_previous_entry_for_same_flow() {
        // The supervisor records every fresh failure for a flow under
        // the same key — operators reading `gcit status` always see the
        // most recent error rather than the oldest. A regression that
        // accidentally preserved the first entry (e.g. switching from
        // `insert` to `entry().or_insert`) would surface here.
        let map = Arc::new(Mutex::new(BTreeMap::new()));
        record_last_error(&map, "flow-x", "first_kind", "first message", None).await;
        record_last_error(&map, "flow-x", "second_kind", "second message", None).await;
        let g = map.lock().await;
        let entry = g.get("flow-x").expect("entry present");
        assert_eq!(entry.kind, "second_kind");
        assert_eq!(entry.message, "second message");
    }

    #[tokio::test]
    async fn flow_last_error_accessors_return_recorded_values() {
        // `at`, `kind`, `message`, `retry_at` are the public read API
        // for `control::status`. The fields are private so tests must
        // go through these accessors. Pin all four against a recorded
        // entry so a regression that drops or renames any of them
        // surfaces.
        let map = Arc::new(Mutex::new(BTreeMap::new()));
        let before = chrono::Utc::now();
        let reset = chrono::DateTime::parse_from_rfc3339("2026-04-28T13:00:00Z")
            .expect("valid rfc3339")
            .with_timezone(&chrono::Utc);
        record_last_error(&map, "flow-y", "github_error", "boom", Some(reset)).await;
        let after = chrono::Utc::now();
        let g = map.lock().await;
        let entry = g.get("flow-y").expect("entry present");
        assert_eq!(entry.kind(), "github_error");
        assert_eq!(entry.message(), "boom");
        assert_eq!(entry.retry_at(), Some(reset));
        let at = entry.at();
        assert!(
            at >= before && at <= after,
            "at() must reflect the wall-clock timestamp recorded by record_last_error; got {at} not in [{before}, {after}]",
        );
    }

    #[test]
    fn is_synthetic_daemon_key_recognises_parenthesized_keys() {
        // The convention pinned in production: `(reload)` is the
        // current daemon-scoped synthetic key. Future entries follow
        // the same `(name)` shape (see `RELOAD_SYNTHETIC_KEY` and
        // `is_synthetic_daemon_key` callers in the CLI status path).
        assert!(is_synthetic_daemon_key(RELOAD_SYNTHETIC_KEY));
        assert!(is_synthetic_daemon_key("(reload)"));
        assert!(is_synthetic_daemon_key("(future-sentinel)"));
    }

    #[test]
    fn is_synthetic_daemon_key_rejects_real_flow_names() {
        // Flow names are validated against `[a-zA-Z0-9_-]+` at config
        // load. Parens are
        // outside that character class, so a flow name can never
        // collide with the synthetic-key convention. Pin a few shapes
        // that look adjacent — leading paren only, trailing paren only,
        // empty string — so the classifier doesn't drift toward
        // matching them.
        assert!(!is_synthetic_daemon_key("ci-flow"));
        assert!(!is_synthetic_daemon_key("flow_name"));
        assert!(!is_synthetic_daemon_key("(no-trailing"));
        assert!(!is_synthetic_daemon_key("no-leading)"));
        assert!(!is_synthetic_daemon_key(""));
    }

    #[test]
    fn flow_registry_new_starts_with_empty_collections() {
        // `FlowRegistry::new` is called once at boot from
        // `supervisor::run`, and the supervisor mutates it in place
        // from there. Bundling invariants depend on the constructor
        // not pre-seeding any of the three collections — a regression
        // that injected sentinel entries (e.g. from an earlier
        // refactor that pre-allocated capacity) would surface here.
        let reg = FlowRegistry::new();
        assert!(reg.handles.is_empty());
        assert!(reg.respawning_flows.is_empty());
        assert!(reg.pending_exits.is_empty());
    }
}

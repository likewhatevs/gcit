// In-memory daemon state and the StateUpdate apply rules.
//
// Update apply is order-preserving Last-Writer-Wins. StateUpdate
// variants: PollObservation, PollTimestamp, RunStarted, RunFinished,
// FlowRemoved. state.json is schema versioned (schema: 1). Flow
// removal during in-flight dispatch is intentional; FlowRemoved drops
// in-memory state for the flow.
//
// The shape of `State` here pins the on-disk JSON format. Producers
// dispatch `StateUpdate` over the mpsc channel; the writer thread
// applies them in order via `State::apply`. `apply` is a pure
// function of (current state, update) for state mutation; the only
// side effect is `tracing::warn!` on diagnostic edges (duplicate
// RunStarted, RunFinished against an unknown flow or run id,
// FlowRemoved against an unknown flow) — these route through the
// global subscriber and never affect the function's return. Test
// skeletons under tests/state_*.rs drive every variant +
// interleaving combination.
//
// `last_sha` is stored on disk as a 40-char (or 64-char for sha256)
// hex string rather than a tagged-enum `gix_hash::ObjectId`. This
// keeps the state file readable and keeps gcit's Cargo.toml from
// pulling in `gix-hash` with the `serde` feature, which would emit
// `{"Sha1":[..bytes..]}` instead of the human-readable hex form. The
// public API still takes `ObjectId` for type safety; the conversion
// happens in `apply` via `to_hex()`.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use gix_hash::ObjectId;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// Current schema version of the state file. The on-disk format is
/// schema-versioned and `load_or_init` refuses unknown versions.
pub const SCHEMA_VERSION: u32 = 1;

/// Per-flow upper bound on the `notified_runs` ring. Once this many
/// runs have completed, RunFinished drops the oldest from the front
/// of the deque. State only needs enough history to dedup
/// notifications across daemon restarts; 100 covers any reasonable
/// poll cadence × runs/poll without unbounded growth.
pub const NOTIFIED_RUNS_CAP: usize = 100;

/// The full daemon state persisted at `$STATE_DIRECTORY/state.json`.
///
/// `Default` returns an empty state with the current schema — this is
/// what `load_or_init` returns when the file does not yet exist (first
/// run on a fresh deployment).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct State {
    pub schema: u32,
    /// Per-flow state, keyed by flow `name`.
    pub flows: BTreeMap<String, FlowState>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            schema: SCHEMA_VERSION,
            flows: BTreeMap::new(),
        }
    }
}

/// State for a single flow.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlowState {
    /// Most recent observed git sha at `source.url:source.ref`. Hex
    /// encoded (40 chars for sha1, 64 chars for sha256). `None` until
    /// the first PollObservation lands.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_sha: Option<String>,
    /// RFC3339 timestamp of the most recent poll. LWW with last_sha.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_poll_at: Option<DateTime<Utc>>,
    /// RFC3339 timestamp of the most recent poll-originated dispatch
    /// acceptance. Updated whenever a `PollObservation` carries
    /// `last_dispatched_at: Some(...)`; preserved when it carries
    /// `None`. Used by the per-flow poll loop's cooldown check —
    /// seeded from this value at flow spawn so a daemon restart
    /// inherits the prior cooldown window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_dispatched_at: Option<DateTime<Utc>>,
    /// RFC3339 timestamp at which the cooldown window expires. Set to
    /// `last_dispatched_at + effective.cooldown` whenever a
    /// `PollObservation` carries `cooldown_until: Some(...)`; preserved
    /// when it carries `None`. Surfaced in `gcit status` so operators
    /// can see when the next poll-originated dispatch will be allowed
    /// without computing it themselves from `last_dispatched_at` plus
    /// the configured cooldown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cooldown_until: Option<DateTime<Utc>>,
    /// Workflow runs gcit dispatched that have not yet reached a
    /// terminal status. RunStarted appends; RunFinished moves entries
    /// to `notified_runs`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub active_runs: Vec<RunState>,
    /// Workflow runs gcit dispatched and saw through to completion.
    /// Used to dedupe so the operator does not get a second
    /// notification if the daemon restarts mid-poll.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notified_runs: Vec<RunState>,
}

/// Per-run state. Represents a single GitHub Actions run gcit
/// dispatched on this flow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunState {
    pub run_id: u64,
    pub started_at: DateTime<Utc>,
    /// Set when the run finishes (Conclusion-like string). `None`
    /// while still active.
    ///
    /// Stored as `Option<String>` rather than `Option<Conclusion>`
    /// for forward-compat: state.json round-trips across gcit
    /// versions even when the `Conclusion` enum gains new variants.
    /// `Conclusion` does carry `#[serde(other)] Unknown` so a typed
    /// enum here would deserialize unknown variants without a schema
    /// bump, but the raw string preserves the original API value
    /// across persistence rather than collapsing it to Unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conclusion: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
}

/// A state mutation produced by a flow task. The state-writer thread
/// applies these in mpsc-FIFO order via `State::apply`.
///
/// Apply semantics per variant (pinned by `tests/state_apply_lww.rs`):
///   - `PollObservation`: LWW on `(last_sha, last_poll_at)` for the
///     named flow. Creates the flow entry if it does not yet exist.
///     `last_dispatched_at: Some(t)` overwrites the field with `t`;
///     `None` leaves the prior value untouched (observation-only
///     poll cycle; cooldown clock not re-armed). `cooldown_until`
///     follows the same overwrite-or-preserve rule as
///     `last_dispatched_at` (always paired in production: a
///     dispatch acceptance arms both at once).
///   - `PollTimestamp`: refresh `last_poll_at` only, leaving
///     `last_sha` untouched. Used by strategies that prove a fast-path
///     "no change" without producing a fresh ObjectId (e.g. grokmirror
///     manifest fingerprint match). Creates the flow entry if it does
///     not yet exist; never clears `last_sha`.
///   - `RunStarted`: append to `flows[name].active_runs`. Multiple
///     concurrent in-flight runs are allowed.
///   - `RunFinished`: remove the matching `run_id` from
///     `active_runs`, fill in `conclusion`/`completed_at`, push to
///     `notified_runs`. Unknown `run_id` emits a WARN and is a noop.
///   - `FlowRemoved`: drop the entry for `flow` from `state.flows`
///     entirely. Subsequent updates to the same name start fresh
///     (no resurrection of prior `active_runs`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateUpdate {
    PollObservation {
        flow: String,
        last_sha: ObjectId,
        last_poll_at: DateTime<Utc>,
        /// `Some(t)` when the cycle accepted a trigger and the poll
        /// loop wants to arm cooldown at `t`. `None` when no trigger
        /// fired (observation-only) — `apply` preserves the prior
        /// `last_dispatched_at` rather than clearing it.
        last_dispatched_at: Option<DateTime<Utc>>,
        /// `Some(t)` when the cycle accepted a trigger and the poll
        /// loop computed the cooldown deadline as
        /// `last_dispatched_at + effective.cooldown`. `None` when no
        /// trigger fired — `apply` preserves the prior
        /// `cooldown_until` rather than clearing it. Always paired
        /// with `last_dispatched_at` in production.
        cooldown_until: Option<DateTime<Utc>>,
    },
    PollTimestamp {
        flow: String,
        last_poll_at: DateTime<Utc>,
    },
    RunStarted {
        flow: String,
        run_id: u64,
        started_at: DateTime<Utc>,
    },
    RunFinished {
        flow: String,
        run_id: u64,
        conclusion: String,
        completed_at: DateTime<Utc>,
    },
    FlowRemoved {
        flow: String,
    },
}

impl State {
    /// Apply a single update to the state. Pure function — same input
    /// sequence on a fresh `State::default()` always yields the same
    /// result. Diagnostic warnings (unknown run id, unknown flow on
    /// removal) are emitted via `tracing::warn!`; they do not affect
    /// the function's return value.
    ///
    /// Producers should NOT depend on apply returning a value — the
    /// writer thread treats every update as fire-and-forget. The
    /// `tracing::warn!` log is the only externally-observable side
    /// effect, and it routes to journald (or stderr in foreground
    /// mode) like any other log line.
    pub fn apply(&mut self, update: StateUpdate) {
        match update {
            StateUpdate::PollObservation {
                flow,
                last_sha,
                last_poll_at,
                last_dispatched_at,
                cooldown_until,
            } => self.apply_poll_observation(
                flow,
                last_sha,
                last_poll_at,
                last_dispatched_at,
                cooldown_until,
            ),
            StateUpdate::PollTimestamp { flow, last_poll_at } => {
                self.apply_poll_timestamp(flow, last_poll_at)
            }
            StateUpdate::RunStarted {
                flow,
                run_id,
                started_at,
            } => self.apply_run_started(flow, run_id, started_at),
            StateUpdate::RunFinished {
                flow,
                run_id,
                conclusion,
                completed_at,
            } => self.apply_run_finished(flow, run_id, conclusion, completed_at),
            StateUpdate::FlowRemoved { flow } => self.apply_flow_removed(flow),
        }
    }

    /// Update the per-flow `last_sha` + `last_poll_at` from a fresh
    /// `Refreshed` outcome. `last_dispatched_at` and `cooldown_until`
    /// follow a paired-Some contract: `Some` arms the cooldown,
    /// `None` leaves the prior value untouched (observation-only
    /// poll cycle).
    fn apply_poll_observation(
        &mut self,
        flow: String,
        last_sha: gix_hash::ObjectId,
        last_poll_at: chrono::DateTime<chrono::Utc>,
        last_dispatched_at: Option<chrono::DateTime<chrono::Utc>>,
        cooldown_until: Option<chrono::DateTime<chrono::Utc>>,
    ) {
        let entry = self.flows.entry(flow).or_default();
        entry.last_sha = Some(last_sha.to_hex().to_string());
        entry.last_poll_at = Some(last_poll_at);
        if last_dispatched_at.is_some() {
            entry.last_dispatched_at = last_dispatched_at;
        }
        if cooldown_until.is_some() {
            entry.cooldown_until = cooldown_until;
        }
    }

    /// Liveness-only refresh: bumps `last_poll_at` for a flow whose
    /// upstream poll cycle observed no SHA change (grokmirror
    /// fingerprint match, GitHub API 304 not-modified, UnbornRef).
    fn apply_poll_timestamp(&mut self, flow: String, last_poll_at: chrono::DateTime<chrono::Utc>) {
        let entry = self.flows.entry(flow).or_default();
        entry.last_poll_at = Some(last_poll_at);
    }

    /// Append a freshly-correlated run to `active_runs`. Dedup: the
    /// dispatcher must not double-emit RunStarted for the same
    /// run_id (could happen if a transient retry re-runs the dispatch
    /// path). A duplicate would inflate active_runs without ever
    /// clearing because RunFinished removes the FIRST match — the
    /// second would persist forever. Drop the dup here with a WARN
    /// so the bug is operator-visible.
    fn apply_run_started(
        &mut self,
        flow: String,
        run_id: u64,
        started_at: chrono::DateTime<chrono::Utc>,
    ) {
        let entry = self.flows.entry(flow.clone()).or_default();
        if entry.active_runs.iter().any(|r| r.run_id == run_id) {
            warn!(
                target: "gcit::state",
                flow = %flow,
                run_id,
                "RunStarted for run_id already in active_runs; ignoring duplicate",
            );
            return;
        }
        entry.active_runs.push(RunState {
            run_id,
            started_at,
            conclusion: None,
            completed_at: None,
        });
    }

    /// Move a run from `active_runs` to `notified_runs` with its
    /// terminal conclusion. Drops with a WARN when the flow or
    /// run_id is unknown (out-of-order delivery during reload, or a
    /// dispatcher bug). Caps `notified_runs` to `NOTIFIED_RUNS_CAP`
    /// by draining the oldest entries; state only needs enough
    /// history to dedup notifications across daemon restarts.
    fn apply_run_finished(
        &mut self,
        flow: String,
        run_id: u64,
        conclusion: String,
        completed_at: chrono::DateTime<chrono::Utc>,
    ) {
        let Some(entry) = self.flows.get_mut(&flow) else {
            warn!(
                target: "gcit::state",
                flow = %flow,
                run_id,
                "RunFinished for unknown flow; dropping",
            );
            return;
        };
        let Some(pos) = entry.active_runs.iter().position(|r| r.run_id == run_id) else {
            warn!(
                target: "gcit::state",
                flow = %flow,
                run_id,
                "RunFinished for run_id not in active_runs; dropping",
            );
            return;
        };
        let mut run = entry.active_runs.remove(pos);
        run.conclusion = Some(conclusion);
        run.completed_at = Some(completed_at);
        entry.notified_runs.push(run);
        if entry.notified_runs.len() > NOTIFIED_RUNS_CAP {
            let excess = entry.notified_runs.len() - NOTIFIED_RUNS_CAP;
            entry.notified_runs.drain(..excess);
        }
    }

    /// Drop the per-flow entry on reload-driven removal or
    /// URL-changed restart. Logs a WARN when the flow is unknown
    /// (already-removed flow re-emitted FlowRemoved, or producer
    /// race during reload).
    fn apply_flow_removed(&mut self, flow: String) {
        if self.flows.remove(&flow).is_none() {
            warn!(
                target: "gcit::state",
                flow = %flow,
                "FlowRemoved for unknown flow; dropping",
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::util::{test_sha as sha, test_ts as t};

    #[test]
    fn poll_observation_lww_on_last_sha() {
        let mut s = State::default();
        s.apply(StateUpdate::PollObservation {
            flow: "f".into(),
            last_sha: sha(0xaa),
            last_poll_at: t(1),
            last_dispatched_at: None,
            cooldown_until: None,
        });
        s.apply(StateUpdate::PollObservation {
            flow: "f".into(),
            last_sha: sha(0xbb),
            last_poll_at: t(2),
            last_dispatched_at: None,
            cooldown_until: None,
        });
        let f = &s.flows["f"];
        assert_eq!(f.last_sha.as_deref(), Some("bb".repeat(20).as_str()));
        assert_eq!(f.last_poll_at, Some(t(2)));
    }

    #[test]
    fn poll_timestamp_refreshes_only_last_poll_at() {
        let mut s = State::default();
        s.apply(StateUpdate::PollObservation {
            flow: "f".into(),
            last_sha: sha(0xaa),
            last_poll_at: t(1),
            last_dispatched_at: None,
            cooldown_until: None,
        });
        s.apply(StateUpdate::PollTimestamp {
            flow: "f".into(),
            last_poll_at: t(2),
        });
        let f = &s.flows["f"];
        assert_eq!(f.last_sha.as_deref(), Some("aa".repeat(20).as_str()));
        assert_eq!(f.last_poll_at, Some(t(2)));
    }

    #[test]
    fn poll_timestamp_creates_entry_without_last_sha() {
        let mut s = State::default();
        s.apply(StateUpdate::PollTimestamp {
            flow: "f".into(),
            last_poll_at: t(7),
        });
        let f = &s.flows["f"];
        assert!(f.last_sha.is_none());
        assert_eq!(f.last_poll_at, Some(t(7)));
    }

    #[test]
    fn run_started_appends_active_runs() {
        let mut s = State::default();
        s.apply(StateUpdate::RunStarted {
            flow: "f".into(),
            run_id: 1,
            started_at: t(10),
        });
        s.apply(StateUpdate::RunStarted {
            flow: "f".into(),
            run_id: 2,
            started_at: t(11),
        });
        let f = &s.flows["f"];
        assert_eq!(f.active_runs.len(), 2);
        assert_eq!(f.active_runs[0].run_id, 1);
        assert_eq!(f.active_runs[1].run_id, 2);
    }

    #[test]
    fn run_started_dedup_drops_duplicate_run_id() {
        // A duplicate RunStarted for the same run_id must be dropped
        // (with a WARN), not pushed twice. A double-push would leak a
        // run forever — RunFinished removes only the first match and
        // leaves the second behind.
        let mut s = State::default();
        s.apply(StateUpdate::RunStarted {
            flow: "f".into(),
            run_id: 7,
            started_at: t(10),
        });
        s.apply(StateUpdate::RunStarted {
            flow: "f".into(),
            run_id: 7,
            started_at: t(11),
        });
        let f = &s.flows["f"];
        assert_eq!(f.active_runs.len(), 1);
        // The first RunStarted's started_at wins; the duplicate is
        // dropped without overwriting.
        assert_eq!(f.active_runs[0].started_at, t(10));
    }

    #[test]
    fn notified_runs_capped_drops_oldest_first() {
        // notified_runs is capped at NOTIFIED_RUNS_CAP. Beyond the
        // cap, oldest entries drain from the front.
        let mut s = State::default();
        let n = NOTIFIED_RUNS_CAP + 5;
        for i in 0..n {
            let id = (i + 1) as u64;
            s.apply(StateUpdate::RunStarted {
                flow: "f".into(),
                run_id: id,
                started_at: t(i as i64),
            });
            s.apply(StateUpdate::RunFinished {
                flow: "f".into(),
                run_id: id,
                conclusion: "success".into(),
                completed_at: t((i as i64) + 1),
            });
        }
        let f = &s.flows["f"];
        assert_eq!(f.notified_runs.len(), NOTIFIED_RUNS_CAP);
        // The 5 oldest entries (run_ids 1..=5) must have been
        // drained; the newest 100 (6..=105) are retained.
        assert_eq!(f.notified_runs.first().unwrap().run_id, 6);
        assert_eq!(f.notified_runs.last().unwrap().run_id, n as u64);
    }

    #[test]
    fn run_finished_moves_active_to_notified() {
        let mut s = State::default();
        s.apply(StateUpdate::RunStarted {
            flow: "f".into(),
            run_id: 42,
            started_at: t(1),
        });
        s.apply(StateUpdate::RunFinished {
            flow: "f".into(),
            run_id: 42,
            conclusion: "success".into(),
            completed_at: t(2),
        });
        let f = &s.flows["f"];
        assert!(f.active_runs.is_empty());
        assert_eq!(f.notified_runs.len(), 1);
        assert_eq!(f.notified_runs[0].run_id, 42);
        assert_eq!(f.notified_runs[0].conclusion.as_deref(), Some("success"));
        assert_eq!(f.notified_runs[0].completed_at, Some(t(2)));
    }

    #[test]
    fn run_finished_for_unknown_run_is_noop() {
        let mut s = State::default();
        s.apply(StateUpdate::RunStarted {
            flow: "f".into(),
            run_id: 1,
            started_at: t(1),
        });
        // Apply a finish for a run_id that never started — must not
        // panic, must not move anything.
        s.apply(StateUpdate::RunFinished {
            flow: "f".into(),
            run_id: 99,
            conclusion: "success".into(),
            completed_at: t(2),
        });
        let f = &s.flows["f"];
        assert_eq!(f.active_runs.len(), 1);
        assert!(f.notified_runs.is_empty());
    }

    #[test]
    fn run_finished_for_unknown_flow_is_noop() {
        let mut s = State::default();
        s.apply(StateUpdate::RunFinished {
            flow: "ghost".into(),
            run_id: 1,
            conclusion: "success".into(),
            completed_at: t(2),
        });
        assert!(s.flows.is_empty());
    }

    #[test]
    fn flow_removed_drops_entry() {
        let mut s = State::default();
        s.apply(StateUpdate::PollObservation {
            flow: "f".into(),
            last_sha: sha(0xaa),
            last_poll_at: t(1),
            last_dispatched_at: None,
            cooldown_until: None,
        });
        s.apply(StateUpdate::RunStarted {
            flow: "f".into(),
            run_id: 1,
            started_at: t(2),
        });
        s.apply(StateUpdate::FlowRemoved { flow: "f".into() });
        assert!(s.flows.is_empty());
    }

    #[test]
    fn flow_removed_for_unknown_is_noop() {
        let mut s = State::default();
        s.apply(StateUpdate::FlowRemoved {
            flow: "ghost".into(),
        });
        assert!(s.flows.is_empty());
    }

    #[test]
    fn flow_removed_does_not_affect_other_flows() {
        let mut s = State::default();
        s.apply(StateUpdate::PollObservation {
            flow: "f".into(),
            last_sha: sha(0xaa),
            last_poll_at: t(1),
            last_dispatched_at: None,
            cooldown_until: None,
        });
        s.apply(StateUpdate::PollObservation {
            flow: "g".into(),
            last_sha: sha(0xbb),
            last_poll_at: t(1),
            last_dispatched_at: None,
            cooldown_until: None,
        });
        s.apply(StateUpdate::FlowRemoved { flow: "f".into() });
        assert!(!s.flows.contains_key("f"));
        assert!(s.flows.contains_key("g"));
        assert_eq!(
            s.flows["g"].last_sha.as_deref(),
            Some("bb".repeat(20).as_str())
        );
    }

    #[test]
    fn re_added_flow_starts_empty() {
        let mut s = State::default();
        s.apply(StateUpdate::PollObservation {
            flow: "f".into(),
            last_sha: sha(0xaa),
            last_poll_at: t(1),
            last_dispatched_at: None,
            cooldown_until: None,
        });
        s.apply(StateUpdate::RunStarted {
            flow: "f".into(),
            run_id: 1,
            started_at: t(2),
        });
        s.apply(StateUpdate::FlowRemoved { flow: "f".into() });
        s.apply(StateUpdate::PollObservation {
            flow: "f".into(),
            last_sha: sha(0xbb),
            last_poll_at: t(3),
            last_dispatched_at: None,
            cooldown_until: None,
        });
        let f = &s.flows["f"];
        assert_eq!(f.last_sha.as_deref(), Some("bb".repeat(20).as_str()));
        assert!(f.active_runs.is_empty());
        assert!(f.notified_runs.is_empty());
    }

    #[test]
    fn apply_is_pure_function_of_input() {
        let updates = vec![
            StateUpdate::PollObservation {
                flow: "f".into(),
                last_sha: sha(0x01),
                last_poll_at: t(1),
                last_dispatched_at: None,
                cooldown_until: None,
            },
            StateUpdate::RunStarted {
                flow: "f".into(),
                run_id: 7,
                started_at: t(2),
            },
            StateUpdate::PollObservation {
                flow: "f".into(),
                last_sha: sha(0x02),
                last_poll_at: t(3),
                last_dispatched_at: None,
                cooldown_until: None,
            },
            StateUpdate::RunFinished {
                flow: "f".into(),
                run_id: 7,
                conclusion: "success".into(),
                completed_at: t(4),
            },
        ];
        let mut a = State::default();
        for u in &updates {
            a.apply(u.clone());
        }
        let mut b = State::default();
        for u in &updates {
            b.apply(u.clone());
        }
        assert_eq!(a, b);
    }

    #[test]
    fn cross_flow_updates_do_not_pollute() {
        let mut s = State::default();
        s.apply(StateUpdate::PollObservation {
            flow: "a".into(),
            last_sha: sha(0xaa),
            last_poll_at: t(1),
            last_dispatched_at: None,
            cooldown_until: None,
        });
        s.apply(StateUpdate::RunStarted {
            flow: "a".into(),
            run_id: 1,
            started_at: t(1),
        });
        s.apply(StateUpdate::PollObservation {
            flow: "b".into(),
            last_sha: sha(0xbb),
            last_poll_at: t(1),
            last_dispatched_at: None,
            cooldown_until: None,
        });
        s.apply(StateUpdate::RunStarted {
            flow: "b".into(),
            run_id: 2,
            started_at: t(1),
        });
        assert_eq!(
            s.flows["a"].last_sha.as_deref(),
            Some("aa".repeat(20).as_str())
        );
        assert_eq!(s.flows["a"].active_runs[0].run_id, 1);
        assert_eq!(
            s.flows["b"].last_sha.as_deref(),
            Some("bb".repeat(20).as_str())
        );
        assert_eq!(s.flows["b"].active_runs[0].run_id, 2);
    }

    #[test]
    fn empty_default_serializes_with_schema_version() {
        let s = State::default();
        let json: serde_json::Value = serde_json::to_value(&s).unwrap();
        assert_eq!(json["schema"], 1);
        assert!(json["flows"].is_object());
        assert_eq!(json["flows"].as_object().unwrap().len(), 0);
    }

    #[test]
    fn poll_observation_none_last_dispatched_preserves_prior() {
        // Cooldown invariant: an observation-only PollObservation
        // (last_dispatched_at = None) must not clobber a prior
        // dispatch timestamp. apply guards the write with
        // `if last_dispatched_at.is_some()` — this test pins that
        // branch.
        let mut s = State::default();
        s.apply(StateUpdate::PollObservation {
            flow: "f".into(),
            last_sha: sha(0xaa),
            last_poll_at: t(1),
            last_dispatched_at: Some(t(1)),
            cooldown_until: Some(t(61)),
        });
        s.apply(StateUpdate::PollObservation {
            flow: "f".into(),
            last_sha: sha(0xbb),
            last_poll_at: t(2),
            last_dispatched_at: None,
            cooldown_until: None,
        });
        let f = &s.flows["f"];
        assert_eq!(f.last_dispatched_at, Some(t(1)));
        // cooldown_until is preserved by the same guard.
        assert_eq!(f.cooldown_until, Some(t(61)));
    }
}

// GitHub API integration: dispatcher, monitor, error classifier,
// rate-limit tracking. Types shared across all submodules
// (Conclusion, RunStatus, RunSummary, JobResult, StepResult) live
// here.
//
// Items here are pub for integration-test reachability and treated as
// crate-internal + unstable.

pub mod client;
pub mod correlator;
pub mod dispatcher;
pub mod error;
pub mod monitor;
pub mod rate_limit;

pub use client::Client;
pub use error::GithubErrorKind;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Outcome of a completed workflow run, as reported by the GitHub API.
/// Mapping from the API string is case-sensitive snake_case
/// (`success`, `skipped`, `neutral`, `failure`, `timed_out`,
/// `cancelled`, `action_required`). Anything else (`stale`,
/// `startup_failure`, NULL, garbage, future additions) maps to
/// `Unknown` so the daemon never crashes on a new API string — a
/// WARN log captures the raw value for operator
/// follow-up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Conclusion {
    Success,
    Skipped,
    Neutral,
    Failure,
    TimedOut,
    Cancelled,
    ActionRequired,
    /// Catch-all for unrecognised conclusion strings. The
    /// `#[serde(other)]` attribute on this variant captures any
    /// snake_case string that isn't one of the seven enumerated
    /// values. Without this, GitHub adding a new conclusion
    /// (e.g. `stale`, `startup_failure`) would surface as a
    /// deserialize error and crash a poll cycle.
    #[serde(other)]
    Unknown,
}

impl Conclusion {
    /// Map a raw API string to a `Conclusion`. Useful for
    /// non-serde paths (e.g. when reading a stringified value from
    /// state.json or test fixtures). Returns `Conclusion::Unknown`
    /// for anything that's not one of the seven canonical variants.
    pub fn from_api(s: &str) -> Self {
        match s {
            "success" => Conclusion::Success,
            "skipped" => Conclusion::Skipped,
            "neutral" => Conclusion::Neutral,
            "failure" => Conclusion::Failure,
            "timed_out" => Conclusion::TimedOut,
            "cancelled" => Conclusion::Cancelled,
            "action_required" => Conclusion::ActionRequired,
            _ => Conclusion::Unknown,
        }
    }

    /// Map a `Conclusion` back to its canonical snake_case API
    /// string. Inverse of `from_api`. Used by the persistence path
    /// (`StateUpdate::RunFinished.conclusion`) so state.json values
    /// round-trip through `from_api` cleanly — `label_for` returns
    /// prose ("timed out", "action required") and would NOT round
    /// trip.
    pub fn to_api(self) -> &'static str {
        match self {
            Conclusion::Success => "success",
            Conclusion::Skipped => "skipped",
            Conclusion::Neutral => "neutral",
            Conclusion::Failure => "failure",
            Conclusion::TimedOut => "timed_out",
            Conclusion::Cancelled => "cancelled",
            Conclusion::ActionRequired => "action_required",
            Conclusion::Unknown => "unknown",
        }
    }
}

/// Whether to collapse the run's per-job/per-step detail in Discord
/// embeds. Collapse on success-like terminal conclusions to keep the
/// embed compact; expand on failure-like conclusions so operators
/// see what went wrong.
pub fn should_collapse(c: Conclusion) -> bool {
    matches!(
        c,
        Conclusion::Success | Conclusion::Skipped | Conclusion::Neutral,
    )
}

/// Operator-facing label for a conclusion. Used in Discord embed
/// titles, mbox subjects, and any rendered prose. The match is
/// exhaustive because `Conclusion` is non-exhaustive — future
/// additions surface as `Unknown` and read "unknown" here.
pub fn label_for(c: Conclusion) -> &'static str {
    match c {
        Conclusion::Success => "success",
        Conclusion::Skipped => "skipped",
        Conclusion::Neutral => "neutral",
        Conclusion::Failure => "failure",
        Conclusion::TimedOut => "timed out",
        Conclusion::Cancelled => "cancelled",
        Conclusion::ActionRequired => "action required",
        Conclusion::Unknown => "unknown",
    }
}

/// Operator-facing label for a `RunStatus`. Used in template
/// rendering for `{{run.status}}`. Mirrors `label_for(Conclusion)`
/// in shape so prose reads consistently across the two enums.
pub fn run_status_label(s: RunStatus) -> &'static str {
    match s {
        RunStatus::Queued => "queued",
        RunStatus::InProgress => "in progress",
        RunStatus::Completed => "completed",
        RunStatus::Waiting => "waiting",
        RunStatus::Unknown => "unknown",
    }
}

/// Lifecycle state of a workflow run reported by GitHub. Documented
/// values map to the explicit variants; everything else (`pending`,
/// `requested`, GitHub additions) maps to `Unknown` — same naming as
/// `Conclusion::Unknown` for cross-enum consistency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RunStatus {
    Queued,
    InProgress,
    Completed,
    Waiting,
    /// Catch-all for unrecognised run statuses. Same rationale as
    /// `Conclusion::Unknown`.
    #[serde(other)]
    Unknown,
}

impl RunStatus {
    pub fn from_api(s: &str) -> Self {
        match s {
            "queued" => RunStatus::Queued,
            "in_progress" => RunStatus::InProgress,
            "completed" => RunStatus::Completed,
            "waiting" => RunStatus::Waiting,
            _ => RunStatus::Unknown,
        }
    }

    /// Whether the run is finished — `Completed` only. Other states
    /// are still in flight.
    pub fn is_terminal(self) -> bool {
        matches!(self, RunStatus::Completed)
    }
}

/// Snapshot of a workflow run's overall state at one point in time.
/// This struct is what the monitor task hands to the notifier on
/// every poll cycle (and finally when the run reaches a terminal
/// status).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSummary {
    pub run_id: u64,
    pub run_url: String,
    pub run_number: u64,
    pub run_attempt: u32,
    pub status: RunStatus,
    pub conclusion: Option<Conclusion>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub jobs: Vec<JobResult>,
}

/// Per-job state inside a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobResult {
    pub job_id: u64,
    pub name: String,
    pub html_url: String,
    pub conclusion: Option<Conclusion>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub steps: Vec<StepResult>,
    pub run_attempt: u32,
}

/// Per-step state inside a job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepResult {
    pub name: String,
    pub number: u32,
    pub conclusion: Option<Conclusion>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conclusion_from_api_known_variants() {
        assert_eq!(Conclusion::from_api("success"), Conclusion::Success);
        assert_eq!(Conclusion::from_api("skipped"), Conclusion::Skipped);
        assert_eq!(Conclusion::from_api("neutral"), Conclusion::Neutral);
        assert_eq!(Conclusion::from_api("failure"), Conclusion::Failure);
        assert_eq!(Conclusion::from_api("timed_out"), Conclusion::TimedOut);
        assert_eq!(Conclusion::from_api("cancelled"), Conclusion::Cancelled);
        assert_eq!(
            Conclusion::from_api("action_required"),
            Conclusion::ActionRequired,
        );
    }

    #[test]
    fn conclusion_to_api_round_trip_with_from_api() {
        // state.json's RunFinished.conclusion must persist as
        // snake_case so a daemon restart that re-loads state.json
        // round-trips through from_api back to the same variant.
        // label_for renders prose ("timed out") and breaks round-trip;
        // to_api preserves it.
        for c in [
            Conclusion::Success,
            Conclusion::Skipped,
            Conclusion::Neutral,
            Conclusion::Failure,
            Conclusion::TimedOut,
            Conclusion::Cancelled,
            Conclusion::ActionRequired,
        ] {
            assert_eq!(
                Conclusion::from_api(c.to_api()),
                c,
                "to_api/from_api round trip failed for {c:?}",
            );
        }
        // Unknown is a sentinel — its to_api yields "unknown", which
        // from_api treats as the catch-all. Confirm the sentinel
        // round-trips to itself rather than blowing up.
        assert_eq!(
            Conclusion::from_api(Conclusion::Unknown.to_api()),
            Conclusion::Unknown,
        );
    }

    #[test]
    fn conclusion_to_api_pins_strings() {
        // Drift here would silently break state.json round-trip; pin
        // the literals.
        assert_eq!(Conclusion::Success.to_api(), "success");
        assert_eq!(Conclusion::Skipped.to_api(), "skipped");
        assert_eq!(Conclusion::Neutral.to_api(), "neutral");
        assert_eq!(Conclusion::Failure.to_api(), "failure");
        assert_eq!(Conclusion::TimedOut.to_api(), "timed_out");
        assert_eq!(Conclusion::Cancelled.to_api(), "cancelled");
        assert_eq!(Conclusion::ActionRequired.to_api(), "action_required");
        assert_eq!(Conclusion::Unknown.to_api(), "unknown");
    }

    #[test]
    fn conclusion_from_api_unknown_variants_map_to_unknown() {
        // Per tests/github_conclusion_mapping.rs::api_string_to_conclusion:
        // GitHub-real values not in the documented set + bogus inputs
        // both map to Unknown (no crash).
        for s in ["stale", "startup_failure", "", "garbage_string_xyz"] {
            assert_eq!(Conclusion::from_api(s), Conclusion::Unknown, "input {s:?}",);
        }
    }

    #[test]
    fn conclusion_serde_round_trip() {
        // Each known variant round-trips through serde JSON. The
        // catch-all Unknown does NOT round-trip to its serialized
        // form (serde_json renders it as "unknown" but parsing
        // "unknown" back yields Unknown; that's the intended
        // sentinel behavior).
        for c in [
            Conclusion::Success,
            Conclusion::Skipped,
            Conclusion::Neutral,
            Conclusion::Failure,
            Conclusion::TimedOut,
            Conclusion::Cancelled,
            Conclusion::ActionRequired,
        ] {
            let s = serde_json::to_string(&c).unwrap();
            let back: Conclusion = serde_json::from_str(&s).unwrap();
            assert_eq!(c, back);
        }
    }

    #[test]
    fn conclusion_serde_unknown_strings_deserialize_to_unknown() {
        // serde rename_all="snake_case" + #[serde(other)] catch-all:
        // any string that isn't one of the enumerated variants
        // deserializes to Unknown. Pin so a future GitHub addition
        // doesn't crash the monitor.
        for raw in [r#""stale""#, r#""startup_failure""#, r#""future_value""#] {
            let c: Conclusion = serde_json::from_str(raw).unwrap();
            assert_eq!(c, Conclusion::Unknown, "input {raw}");
        }
    }

    #[test]
    fn should_collapse_matches_spec() {
        //   Collapse: Success / Skipped / Neutral
        //   Don't collapse: Failure / TimedOut / Cancelled / ActionRequired / Unknown
        assert!(should_collapse(Conclusion::Success));
        assert!(should_collapse(Conclusion::Skipped));
        assert!(should_collapse(Conclusion::Neutral));
        assert!(!should_collapse(Conclusion::Failure));
        assert!(!should_collapse(Conclusion::TimedOut));
        assert!(!should_collapse(Conclusion::Cancelled));
        assert!(!should_collapse(Conclusion::ActionRequired));
        assert!(!should_collapse(Conclusion::Unknown));
    }

    #[test]
    fn run_status_from_api_known() {
        assert_eq!(RunStatus::from_api("queued"), RunStatus::Queued);
        assert_eq!(RunStatus::from_api("in_progress"), RunStatus::InProgress);
        assert_eq!(RunStatus::from_api("completed"), RunStatus::Completed);
        assert_eq!(RunStatus::from_api("waiting"), RunStatus::Waiting);
    }

    #[test]
    fn run_status_from_api_unknown_maps_to_unknown() {
        for s in ["pending", "requested", "", "garbage"] {
            assert_eq!(RunStatus::from_api(s), RunStatus::Unknown, "input {s:?}");
        }
    }

    #[test]
    fn run_status_is_terminal_only_completed() {
        assert!(RunStatus::Completed.is_terminal());
        for s in [
            RunStatus::Queued,
            RunStatus::InProgress,
            RunStatus::Waiting,
            RunStatus::Unknown,
        ] {
            assert!(!s.is_terminal(), "{s:?} must not be terminal");
        }
    }

    #[test]
    fn label_for_pins_strings() {
        assert_eq!(label_for(Conclusion::Success), "success");
        assert_eq!(label_for(Conclusion::Skipped), "skipped");
        assert_eq!(label_for(Conclusion::Neutral), "neutral");
        assert_eq!(label_for(Conclusion::Failure), "failure");
        assert_eq!(label_for(Conclusion::TimedOut), "timed out");
        assert_eq!(label_for(Conclusion::Cancelled), "cancelled");
        assert_eq!(label_for(Conclusion::ActionRequired), "action required");
        assert_eq!(label_for(Conclusion::Unknown), "unknown");
    }

    #[test]
    fn run_status_label_pins_strings() {
        assert_eq!(run_status_label(RunStatus::Queued), "queued");
        assert_eq!(run_status_label(RunStatus::InProgress), "in progress");
        assert_eq!(run_status_label(RunStatus::Completed), "completed");
        assert_eq!(run_status_label(RunStatus::Waiting), "waiting");
        assert_eq!(run_status_label(RunStatus::Unknown), "unknown");
    }
}

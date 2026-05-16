// Crate-internal `DynNotifier` test helper. Two modes:
//
//   - Recording: each hook increments its counter and returns
//     `Ok(NotifyOutcome::Sent { receipt: "recorded" })`. Use
//     `.count(hook)` to inspect counters.
//   - Stub: every hook returns a fixed outcome (commonly
//     `Ok(Skipped { reason: NotConfigured })`) without recording.
//     Useful for `log_notify_outcome` matrix tests that only need
//     the trait surface satisfied.
//
// The integration-test mirror under `tests/common/recording_notifier.rs`
// is more featureful (full call records + scripted outcomes) because
// integration tests pin fan-out shape across the dispatcher boundary;
// this in-crate helper is intentionally smaller — its job is to satisfy
// unit-test mocks that previously hand-rolled the trait surface.
// Cannot share a single implementation because the integration-test
// crate cannot reach `#[cfg(test)]` items from the lib crate.

use std::sync::Mutex;

use tokio_util::sync::CancellationToken;

use crate::flow::dispatcher::{DynNotifier, DynNotifyFuture};
use crate::github::{JobResult, RunSummary};
use crate::notify::{NotifyOutcome, RunContext};

/// One of three notifier hooks. Used to index counters in recording
/// mode and as the discriminant test bodies match on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Hook {
    RunStart = 0,
    JobComplete = 1,
    RunComplete = 2,
}

enum Mode {
    Recording { counts: Mutex<[usize; 3]> },
    Stub(NotifyOutcome),
}

pub(crate) struct RecordingNotifier {
    kind: &'static str,
    id: String,
    mode: Mode,
}

impl RecordingNotifier {
    /// Recording mode. Counters start at zero; each hook invocation
    /// increments the matching counter and returns
    /// `Ok(Sent { receipt: "recorded" })`.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            kind: "recording",
            id: id.into(),
            mode: Mode::Recording {
                counts: Mutex::new([0; 3]),
            },
        }
    }

    /// Stub mode. Every hook returns `outcome` without recording.
    /// `kind` lets the caller pin the surfaced kind ("test", "discord",
    /// etc.) when the assertion under test cares about the kind string.
    pub fn stub(kind: &'static str, id: impl Into<String>, outcome: NotifyOutcome) -> Self {
        Self {
            kind,
            id: id.into(),
            mode: Mode::Stub(outcome),
        }
    }

    /// Per-hook invocation count. Panics on a stub — stubs do not
    /// record. The panic surfaces test-author error (called `.count()`
    /// against a stub) immediately instead of returning a misleading
    /// zero.
    pub fn count(&self, hook: Hook) -> usize {
        match &self.mode {
            Mode::Recording { counts } => {
                counts.lock().expect("recording counters mutex")[hook as usize]
            }
            Mode::Stub(_) => panic!(
                "count() is only valid on RecordingNotifier::new — RecordingNotifier::stub does not record",
            ),
        }
    }

    fn record_and_outcome(&self, hook: Hook) -> NotifyOutcome {
        match &self.mode {
            Mode::Recording { counts } => {
                counts.lock().expect("recording counters mutex")[hook as usize] += 1;
                NotifyOutcome::Sent {
                    receipt: "recorded".to_string(),
                }
            }
            Mode::Stub(o) => o.clone(),
        }
    }
}

impl DynNotifier for RecordingNotifier {
    fn kind(&self) -> &'static str {
        self.kind
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn on_run_start<'a>(
        &'a self,
        _ctx: &'a RunContext,
        _cancel: &'a CancellationToken,
    ) -> DynNotifyFuture<'a> {
        Box::pin(async move { Ok(self.record_and_outcome(Hook::RunStart)) })
    }

    fn on_job_complete<'a>(
        &'a self,
        _ctx: &'a RunContext,
        _job: &'a JobResult,
        _cancel: &'a CancellationToken,
    ) -> DynNotifyFuture<'a> {
        Box::pin(async move { Ok(self.record_and_outcome(Hook::JobComplete)) })
    }

    fn on_run_complete<'a>(
        &'a self,
        _ctx: &'a RunContext,
        _summary: &'a RunSummary,
        _cancel: &'a CancellationToken,
    ) -> DynNotifyFuture<'a> {
        Box::pin(async move { Ok(self.record_and_outcome(Hook::RunComplete)) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notify::SkipReason;

    #[test]
    fn new_counters_start_at_zero() {
        let r = RecordingNotifier::new("rec");
        assert_eq!(r.count(Hook::RunStart), 0);
        assert_eq!(r.count(Hook::JobComplete), 0);
        assert_eq!(r.count(Hook::RunComplete), 0);
    }

    #[test]
    fn record_and_outcome_increments_only_named_hook_and_returns_default_sent() {
        let r = RecordingNotifier::new("rec");
        let o = r.record_and_outcome(Hook::JobComplete);
        assert!(matches!(o, NotifyOutcome::Sent { .. }));
        assert_eq!(r.count(Hook::RunStart), 0);
        assert_eq!(r.count(Hook::JobComplete), 1);
        assert_eq!(r.count(Hook::RunComplete), 0);
    }

    #[test]
    fn stub_returns_the_fixed_outcome() {
        let r = RecordingNotifier::stub(
            "test",
            "stub-1",
            NotifyOutcome::Skipped {
                reason: SkipReason::NotConfigured,
            },
        );
        let o = r.record_and_outcome(Hook::RunStart);
        assert!(matches!(
            o,
            NotifyOutcome::Skipped {
                reason: SkipReason::NotConfigured,
            }
        ));
        assert_eq!(r.kind(), "test");
        assert_eq!(r.id(), "stub-1");
    }

    #[test]
    #[should_panic(expected = "count() is only valid")]
    fn count_against_stub_panics() {
        let r = RecordingNotifier::stub(
            "test",
            "stub",
            NotifyOutcome::Skipped {
                reason: SkipReason::NotConfigured,
            },
        );
        r.count(Hook::RunStart);
    }
}

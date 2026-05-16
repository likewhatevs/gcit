// Per-flow task pipeline + daemon supervisor.
//
// Module layout:
//   - supervisor: the daemon's top-level select! loop. Owns the
//     CancellationToken tree, JoinSet of per-flow tasks, signal
//     handlers, watch<Arc<Config>>, control server handler. Drives the
//     SIGHUP -> reload path.
//   - poll: per-flow polling lifecycle. Wraps the one-shot strategies
//     in `crate::git` (auto_detect, github_api/grokmirror/ls_remote,
//     compare_sha) into a long-running loop, threads observations to
//     the state writer, and emits TriggerSignal on diff.
//   - dispatcher: per-flow dispatch lifecycle. Receives TriggerSignal,
//     renders inputs, calls `crate::github::dispatcher::dispatch_with_retry`,
//     then `crate::github::correlator::correlate`, emits RunStarted +
//     hands the resulting CorrelationOutcome off to a per-run monitor.
//   - monitor: per-run monitor lifecycle. Wraps
//     `crate::github::monitor::monitor_run` in a task that drains
//     MonitorEvent::{Update, Done} into the state writer + notifier
//     dispatch.
//
// Each flow runs as a child task tree under its own
// `CancellationToken`. The supervisor's root token cancels every
// child on shutdown; per-flow cancellation cancels only the named
// flow (used for config-reload removal).
//
// Items here are pub for integration-test reachability and treated
// as crate-internal + unstable.

pub mod dispatcher;
pub mod monitor;
pub mod poll;
pub mod supervisor;

use std::sync::Arc;
use std::time::Duration;

use crate::notify::{NotifyError, NotifyOutcome};
use dispatcher::DynNotifier;

pub use supervisor::run as run_daemon;

/// Delay between observing a panicked flow task and respawning it.
/// 30 seconds: constant rather than configurable so the value pushes
/// the operator toward fixing the underlying bug rather than tuning
/// it away. A panic in a polling loop that respawns every second
/// would mask the real fault.
pub const RESPAWN_DELAY: Duration = Duration::from_secs(30);

/// Capacity of the per-flow `TriggerSignal` mpsc the poll task feeds
/// the dispatcher with. Triggers are rare (one per source-side SHA
/// change), so 8 is plenty even with config reload bursts.
pub const TRIGGER_QUEUE: usize = 8;

/// One signal emitted by the poll task's SHA-diff comparator. The
/// dispatcher consumes these and turns each one into a single
/// `workflow_dispatch` round-trip + correlation + monitor spawn.
///
/// `observed_sha` is the SHA the poll just saw. The dispatcher uses
/// it to fill `RunContext::source.sha` and to pass into
/// `correlator::CorrelateParams::head_sha` (the fallback path
/// filters by `?head_sha=<sha>`). The poll task ALREADY persisted
/// this SHA via `StateUpdate::PollObservation`; the dispatcher does
/// not re-emit.
#[derive(Debug, Clone)]
pub struct TriggerSignal {
    pub observed_sha: gix_hash::ObjectId,
    pub observed_at: chrono::DateTime<chrono::Utc>,
}

/// Spawn one `tokio::task` per notifier, each running the future
/// produced by `f` and uniformly logging the outcome under the
/// `gcit::flow::notify` tracing target with a per-event `label` (e.g.
/// "run-start", "run-complete", "job-complete"). Returns the spawned
/// `JoinHandle`s so callers can either await them all (preserves
/// "fan-out completes before caller proceeds" semantics, used by the
/// monitor's `Done` path) OR drop the handles for fire-and-forget
/// (used by the dispatcher's run-start path so a slow notifier does
/// not delay monitor spawn).
///
/// `extra_job_id` adds a `job_id` field to every per-notifier log line
/// when set; used by the monitor's per-job fan-out so an operator
/// reading logs can correlate notifier results to specific jobs.
///
/// All notifier outcomes log under one tracing target rather than the
/// per-call-site target the inline code used previously. The tracing
/// `target:` macro arg requires a `&'static str` (literal) — passing
/// a runtime value through the helper is rejected at compile time
/// because the macro expansion stores it in a `static __CALLSITE`
/// initializer. The `label` field still distinguishes the call site
/// in log records.
///
/// Outcome shape mirrors the trait: `Ok(Sent { receipt })` → INFO with
/// the receipt, `Ok(Skipped { reason })` → DEBUG with the reason
/// debug-formatted, `Err(_)` → WARN with the error string. One
/// notifier's failure never affects the others — each task is
/// independent.
pub(super) fn spawn_fan_out<F, Fut>(
    notifiers: &[Arc<dyn DynNotifier>],
    label: &'static str,
    extra_job_id: Option<u64>,
    f: F,
) -> Vec<tokio::task::JoinHandle<()>>
where
    F: Fn(Arc<dyn DynNotifier>) -> Fut,
    Fut: std::future::Future<Output = Result<NotifyOutcome, NotifyError>> + Send + 'static,
{
    notifiers
        .iter()
        .map(|n| {
            let notifier = Arc::clone(n);
            let fut = f(Arc::clone(&notifier));
            tokio::spawn(async move {
                log_notify_outcome(label, &*notifier, extra_job_id, fut.await);
            })
        })
        .collect()
}

/// Render a per-notifier outcome as a uniform INFO/DEBUG/WARN line
/// under the `gcit::flow::notify` tracing target. Factored so
/// `spawn_fan_out` and any future direct-call site emit identical
/// structured fields. `job_id`, when present, is added as a top-level
/// field on every variant.
fn log_notify_outcome(
    label: &'static str,
    n: &dyn DynNotifier,
    job_id: Option<u64>,
    result: Result<NotifyOutcome, NotifyError>,
) {
    use tracing::{debug, info, warn};
    let kind = n.kind();
    let id = n.id();
    match result {
        Ok(NotifyOutcome::Sent { receipt }) => match job_id {
            Some(j) => info!(
                target: "gcit::flow::notify",
                kind, id, job_id = j, label, receipt = %receipt,
                "notifier delivered",
            ),
            None => info!(
                target: "gcit::flow::notify",
                kind, id, label, receipt = %receipt,
                "notifier delivered",
            ),
        },
        Ok(NotifyOutcome::Skipped { reason }) => match job_id {
            Some(j) => debug!(
                target: "gcit::flow::notify",
                kind, id, job_id = j, label, reason = ?reason,
                "notifier skipped",
            ),
            None => debug!(
                target: "gcit::flow::notify",
                kind, id, label, reason = ?reason,
                "notifier skipped",
            ),
        },
        Err(e) => match job_id {
            Some(j) => warn!(
                target: "gcit::flow::notify",
                kind, id, job_id = j, label, error = %e,
                "notifier failed",
            ),
            None => warn!(
                target: "gcit::flow::notify",
                kind, id, label, error = %e,
                "notifier failed",
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notify::SkipReason;
    use crate::test_notifier::RecordingNotifier;

    fn stub() -> RecordingNotifier {
        RecordingNotifier::stub(
            "test",
            "stub-1",
            NotifyOutcome::Skipped {
                reason: SkipReason::NotConfigured,
            },
        )
    }

    /// log_notify_outcome's `Ok(Sent)` arm with `job_id = None` emits
    /// an INFO log under target "gcit::flow::notify". The function is
    /// pure logging — no state to assert on the call itself; the test
    /// pins that the function does not panic across every variant of
    /// the `(result, job_id)` matrix.
    ///
    /// Branch matrix coverage:
    ///   - Ok(Sent), Some(j) | None
    ///   - Ok(Skipped), Some(j) | None
    ///   - Err, Some(j) | None
    ///
    /// Six branches; without these tests three (job_id = None for each
    /// outcome variant) and three more (job_id = Some) all stay unhit.
    #[test]
    fn log_notify_outcome_sent_without_job_id_does_not_panic() {
        let n = stub();
        log_notify_outcome(
            "run-start",
            &n,
            None,
            Ok(NotifyOutcome::Sent {
                receipt: "test-receipt".into(),
            }),
        );
    }

    #[test]
    fn log_notify_outcome_sent_with_job_id_does_not_panic() {
        let n = stub();
        log_notify_outcome(
            "job-complete",
            &n,
            Some(42),
            Ok(NotifyOutcome::Sent {
                receipt: "test-receipt".into(),
            }),
        );
    }

    #[test]
    fn log_notify_outcome_skipped_without_job_id_does_not_panic() {
        let n = stub();
        log_notify_outcome(
            "run-start",
            &n,
            None,
            Ok(NotifyOutcome::Skipped {
                reason: SkipReason::FireOnMismatch,
            }),
        );
    }

    #[test]
    fn log_notify_outcome_skipped_with_job_id_does_not_panic() {
        let n = stub();
        log_notify_outcome(
            "job-complete",
            &n,
            Some(7),
            Ok(NotifyOutcome::Skipped {
                reason: SkipReason::NotConfigured,
            }),
        );
    }

    #[test]
    fn log_notify_outcome_err_without_job_id_does_not_panic() {
        let n = stub();
        log_notify_outcome(
            "run-complete",
            &n,
            None,
            Err(NotifyError::Permanent {
                source: anyhow::anyhow!("test failure"),
            }),
        );
    }

    #[test]
    fn log_notify_outcome_err_with_job_id_does_not_panic() {
        let n = stub();
        log_notify_outcome(
            "job-complete",
            &n,
            Some(99),
            Err(NotifyError::Transient {
                source: anyhow::anyhow!("test transient"),
                retry_after: None,
            }),
        );
    }

    /// `spawn_fan_out` with an empty notifier slice returns an empty
    /// JoinHandle vec without spawning any tasks. Pinned because a
    /// regression that skipped the empty-slice fast path would
    /// allocate unnecessary state.
    #[tokio::test]
    async fn spawn_fan_out_with_empty_slice_returns_empty_handle_vec() {
        let notifiers: Vec<Arc<dyn DynNotifier>> = Vec::new();
        let handles = spawn_fan_out(&notifiers, "run-start", None, |_n| async move {
            unreachable!("closure must NOT be invoked for empty slice")
        });
        assert_eq!(handles.len(), 0);
    }

    /// `spawn_fan_out` spawns one task per notifier and returns one
    /// handle per spawned task. The closure is invoked for every
    /// notifier in the slice.
    #[tokio::test]
    async fn spawn_fan_out_invokes_closure_once_per_notifier() {
        let notifiers: Vec<Arc<dyn DynNotifier>> = vec![
            Arc::new(stub()) as Arc<dyn DynNotifier>,
            Arc::new(stub()) as Arc<dyn DynNotifier>,
            Arc::new(stub()) as Arc<dyn DynNotifier>,
        ];
        let counter = Arc::new(tokio::sync::Mutex::new(0_usize));
        let counter_for_closure = Arc::clone(&counter);
        let handles = spawn_fan_out(&notifiers, "run-start", None, move |_n| {
            let counter = Arc::clone(&counter_for_closure);
            async move {
                *counter.lock().await += 1;
                Ok(NotifyOutcome::Sent {
                    receipt: "ok".into(),
                })
            }
        });
        assert_eq!(handles.len(), 3);
        // Drive every spawned task to completion.
        for h in handles {
            h.await.expect("spawned task must not panic");
        }
        assert_eq!(*counter.lock().await, 3);
    }
}

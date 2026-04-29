// flow::monitor::run_monitor_with_source lifecycle tests.
//
// Drives the per-flow monitor task against a `ScriptedEventSource`
// — pre-baked `MonitorEvent` stream + final `MonitorOutcome` — and
// asserts the post-event lifecycle:
//   - on Update: per-job fan-out fires for newly terminal jobs.
//   - on Done: per-job sweep + on_run_complete fan-out + RunFinished
//     emitted on state_tx.
//   - dedup: a second Update carrying the same terminal job_id does
//     NOT re-fire on_job_complete.
//   - serialization: terminal status's conclusion round-trips through
//     `Conclusion::to_api` (snake_case) and `None` becomes "unknown".
//   - multi-notifier fan-out: every notifier receives on_run_complete.
//
// Cancel-during-Update is NOT exercised here — the cancel-aware
// path lives in the github-layer `monitor_run` and is covered by
// `tests/github_monitor_lifecycle.rs::cancel_mid_poll_returns_drained_mid_run`.
//
// Uses the production `DynNotifier` seam via `RecordingNotifier`
// (tests/common/recording_notifier.rs); no octocrab, no wiremock,
// no real fs.

mod common;

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use common::recording_notifier::{NotifyHook, RecordingNotifier};

use gcit::flow::dispatcher::DynNotifier;
use gcit::flow::monitor::{run_monitor_with_source, MonitorEventSource, MonitorParams};
use gcit::github::monitor::{MonitorEvent, MonitorOutcome};
use gcit::github::{Conclusion, JobResult, RunStatus, RunSummary};
use gcit::notify::{ActionInfo, RunContext, SourceInfo};
use gcit::state::StateUpdate;

/// Pre-baked event stream + final outcome for the `MonitorEventSource`
/// trait. The harness pushes every event onto `event_tx` in order, then
/// returns the supplied outcome. Any send failure (receiver dropped)
/// short-circuits with `ReceiverDropped`.
struct ScriptedEventSource {
    events: Vec<MonitorEvent>,
    outcome: MonitorOutcome,
}

impl MonitorEventSource for ScriptedEventSource {
    fn drive<'a>(
        self: Box<Self>,
        event_tx: mpsc::Sender<MonitorEvent>,
        _cancel: CancellationToken,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = MonitorOutcome> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            for ev in self.events {
                if event_tx.send(ev).await.is_err() {
                    return MonitorOutcome::ReceiverDropped;
                }
            }
            // Drop event_tx so run_monitor's recv loop exits cleanly
            // after consuming the scripted events. Otherwise the
            // receiver would block waiting for more events.
            drop(event_tx);
            self.outcome
        })
    }
}

fn run_context() -> RunContext {
    RunContext {
        flow_name: "ci-flow".into(),
        flow_description: None,
        source: SourceInfo {
            url: "https://example.com/r.git".into(),
            ref_name: "refs/heads/main".into(),
            sha: gix_hash::ObjectId::null(gix_hash::Kind::Sha1),
            sha_short: "0000000".into(),
        },
        action: ActionInfo {
            repo: "owner/repo".into(),
            workflow: "ci.yml".into(),
            run_id: 7,
            run_url: "https://github.com/owner/repo/actions/runs/7".into(),
            dispatched_at: Utc::now(),
        },
        gcit_run_id: Uuid::nil(),
    }
}

fn job(id: u64, conclusion: Option<Conclusion>) -> JobResult {
    JobResult {
        job_id: id,
        name: format!("build-{id}"),
        html_url: format!("https://example.com/jobs/{id}"),
        conclusion,
        started_at: Some(Utc::now()),
        completed_at: conclusion.map(|_| Utc::now()),
        steps: Vec::new(),
        run_attempt: 1,
    }
}

fn summary(status: RunStatus, conclusion: Option<Conclusion>, jobs: Vec<JobResult>) -> RunSummary {
    RunSummary {
        run_id: 7,
        run_url: "https://github.com/owner/repo/actions/runs/7".into(),
        run_number: 1,
        run_attempt: 1,
        status,
        conclusion,
        started_at: Some(Utc::now()),
        completed_at: conclusion.map(|_| Utc::now()),
        jobs,
    }
}

fn build_params(notifiers: Vec<Arc<dyn DynNotifier>>) -> MonitorParams {
    use gcit::config::CredentialId;
    use gcit::github::client::Client;
    use gcit::github::rate_limit::RateLimitState;
    use secrecy::SecretString;

    // Real client/rate_limit are required by `MonitorParams`'s shape but
    // the scripted event source ignores both fields. We point the
    // client at an unreachable URL so any accidental production-path
    // call would fail loudly rather than silently hit github.com.
    common::ensure_crypto_provider();
    let github_client = Arc::new(
        Client::builder()
            .credential(CredentialId::new("github_pat").expect("valid id"))
            .token(SecretString::from(
                "github_pat_unreachable_test_should_not_call".to_string(),
            ))
            .request_timeout(Duration::from_secs(1))
            .base_uri("http://127.0.0.1:1")
            .build()
            .expect("client build"),
    );
    let rate_limit = Arc::new(RateLimitState::new());
    MonitorParams {
        flow_name: "ci-flow".into(),
        repo: "owner/repo".into(),
        workflow: "ci.yml".into(),
        run_id: 7,
        job_interval: Duration::from_millis(50),
        github_client,
        rate_limit,
        notifiers,
        run_context: run_context(),
    }
}

#[tokio::test]
async fn done_event_triggers_run_complete_and_run_finished() {
    // Single Done event with conclusion=Success. The lifecycle must:
    //   1. Fire on_run_complete on every notifier exactly once.
    //   2. Emit StateUpdate::RunFinished with conclusion="success" on
    //      state_tx.
    //   3. Return without panicking.
    let recorder = Arc::new(RecordingNotifier::new("disc-1"));
    let notifiers: Vec<Arc<dyn DynNotifier>> = vec![Arc::clone(&recorder) as _];
    let params = build_params(notifiers);

    let source = Box::new(ScriptedEventSource {
        events: vec![MonitorEvent::Done {
            summary: summary(RunStatus::Completed, Some(Conclusion::Success), Vec::new()),
        }],
        outcome: MonitorOutcome::Terminated,
    });

    let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(4);
    let cancel = CancellationToken::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        run_monitor_with_source(params, source, state_tx, cancel),
    )
    .await
    .expect("run_monitor_with_source must complete within 5s");

    // Notifier captures: exactly one on_run_complete, no
    // on_run_start (the dispatcher emits that, not the monitor),
    // no on_job_complete (Done's per-job sweep finds no newly
    // terminal jobs because `summary.jobs` is empty).
    assert_eq!(recorder.count(NotifyHook::RunComplete), 1);
    assert_eq!(recorder.count(NotifyHook::RunStart), 0);
    assert_eq!(recorder.count(NotifyHook::JobComplete), 0);

    // RunFinished landed on state_tx with the right shape.
    let upd = state_rx.recv().await.expect("RunFinished must be emitted");
    match upd {
        StateUpdate::RunFinished {
            flow,
            run_id,
            conclusion,
            ..
        } => {
            assert_eq!(flow, "ci-flow");
            assert_eq!(run_id, 7);
            assert_eq!(conclusion, "success");
        }
        other => panic!("expected RunFinished, got {other:?}"),
    }
}

#[tokio::test]
async fn update_then_done_dedups_per_job_completion() {
    // First event: Update with job 100 newly terminal (success).
    // Second event: Done with the SAME job 100 in the summary (still
    // terminal). The per-job dedup set must prevent on_job_complete
    // from firing twice for job 100. on_run_complete fires once.
    let recorder = Arc::new(RecordingNotifier::new("disc-1"));
    let notifiers: Vec<Arc<dyn DynNotifier>> = vec![Arc::clone(&recorder) as _];
    let params = build_params(notifiers);

    let job_100 = job(100, Some(Conclusion::Success));
    let source = Box::new(ScriptedEventSource {
        events: vec![
            MonitorEvent::Update {
                summary: summary(RunStatus::InProgress, None, vec![job_100.clone()]),
            },
            MonitorEvent::Done {
                summary: summary(
                    RunStatus::Completed,
                    Some(Conclusion::Success),
                    vec![job_100],
                ),
            },
        ],
        outcome: MonitorOutcome::Terminated,
    });

    let (state_tx, _state_rx) = mpsc::channel::<StateUpdate>(4);
    let cancel = CancellationToken::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        run_monitor_with_source(params, source, state_tx, cancel),
    )
    .await
    .expect("must complete within 5s");

    // Exactly one on_job_complete (Update fan-out), exactly one
    // on_run_complete (Done fan-out). The Done event's per-job
    // sweep finds job 100 already in completed_jobs and skips it.
    assert_eq!(
        recorder.count(NotifyHook::JobComplete),
        1,
        "job_complete must fire exactly once per (run, job) — dedup set prevents re-fire on Done's sweep",
    );
    assert_eq!(recorder.count(NotifyHook::RunComplete), 1);
}

#[tokio::test]
async fn update_with_in_progress_job_does_not_fire_job_complete() {
    // Update event with job 200 still in_progress (conclusion=None).
    // The per-job fan-out gate is `j.conclusion.is_some()`; an
    // in-progress job must not fire on_job_complete.
    let recorder = Arc::new(RecordingNotifier::new("disc-1"));
    let notifiers: Vec<Arc<dyn DynNotifier>> = vec![Arc::clone(&recorder) as _];
    let params = build_params(notifiers);

    let job_200 = job(200, None); // in_progress
    let source = Box::new(ScriptedEventSource {
        events: vec![
            MonitorEvent::Update {
                summary: summary(RunStatus::InProgress, None, vec![job_200]),
            },
            MonitorEvent::Done {
                summary: summary(RunStatus::Completed, Some(Conclusion::Success), Vec::new()),
            },
        ],
        outcome: MonitorOutcome::Terminated,
    });

    let (state_tx, _state_rx) = mpsc::channel::<StateUpdate>(4);
    let cancel = CancellationToken::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        run_monitor_with_source(params, source, state_tx, cancel),
    )
    .await
    .expect("must complete within 5s");

    assert_eq!(
        recorder.count(NotifyHook::JobComplete),
        0,
        "in_progress job (conclusion=None) must NOT fire on_job_complete",
    );
    assert_eq!(recorder.count(NotifyHook::RunComplete), 1);
}

#[tokio::test]
async fn done_event_with_failure_emits_run_finished_with_failure_string() {
    // The conclusion serialization round-trips through
    // `Conclusion::to_api` (snake_case) so state.json can re-parse
    // via `from_api`. Pin the wire format for the failure case.
    let recorder = Arc::new(RecordingNotifier::new("disc-1"));
    let notifiers: Vec<Arc<dyn DynNotifier>> = vec![Arc::clone(&recorder) as _];
    let params = build_params(notifiers);

    let source = Box::new(ScriptedEventSource {
        events: vec![MonitorEvent::Done {
            summary: summary(RunStatus::Completed, Some(Conclusion::Failure), Vec::new()),
        }],
        outcome: MonitorOutcome::Terminated,
    });

    let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(4);
    let cancel = CancellationToken::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        run_monitor_with_source(params, source, state_tx, cancel),
    )
    .await
    .expect("must complete within 5s");

    let upd = state_rx.recv().await.expect("RunFinished must be emitted");
    match upd {
        StateUpdate::RunFinished { conclusion, .. } => {
            assert_eq!(conclusion, "failure");
        }
        other => panic!("expected RunFinished, got {other:?}"),
    }
    assert_eq!(recorder.count(NotifyHook::RunComplete), 1);
}

#[tokio::test]
async fn done_with_no_conclusion_emits_unknown_string() {
    // RunSummary::conclusion is Option<Conclusion>; when GitHub
    // reports a terminal status without a conclusion (rare; e.g.
    // workflow cancelled mid-flight), the persistence path emits
    // "unknown" so re-parse via `Conclusion::from_api` yields
    // Unknown rather than panicking.
    let recorder = Arc::new(RecordingNotifier::new("disc-1"));
    let notifiers: Vec<Arc<dyn DynNotifier>> = vec![Arc::clone(&recorder) as _];
    let params = build_params(notifiers);

    let source = Box::new(ScriptedEventSource {
        events: vec![MonitorEvent::Done {
            summary: summary(RunStatus::Completed, None, Vec::new()),
        }],
        outcome: MonitorOutcome::Terminated,
    });

    let (state_tx, mut state_rx) = mpsc::channel::<StateUpdate>(4);
    let cancel = CancellationToken::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        run_monitor_with_source(params, source, state_tx, cancel),
    )
    .await
    .expect("must complete within 5s");

    let upd = state_rx.recv().await.expect("RunFinished must be emitted");
    match upd {
        StateUpdate::RunFinished { conclusion, .. } => {
            assert_eq!(conclusion, "unknown");
        }
        other => panic!("expected RunFinished, got {other:?}"),
    }
}

#[tokio::test]
async fn multiple_notifiers_each_receive_run_complete() {
    // Fan-out exercised: two recording notifiers, both must observe
    // exactly one on_run_complete each. Pins that the monitor's
    // Done branch dispatches the on_run_complete hook to every
    // notifier in `params.notifiers`, not just the first.
    // Failure-isolation (one notifier erroring does not block
    // others) is a separate property — tracked in the queue.
    let n1 = Arc::new(RecordingNotifier::new("disc-1"));
    let n2 = Arc::new(RecordingNotifier::new("mail-1"));
    let notifiers: Vec<Arc<dyn DynNotifier>> = vec![Arc::clone(&n1) as _, Arc::clone(&n2) as _];
    let params = build_params(notifiers);

    let source = Box::new(ScriptedEventSource {
        events: vec![MonitorEvent::Done {
            summary: summary(RunStatus::Completed, Some(Conclusion::Success), Vec::new()),
        }],
        outcome: MonitorOutcome::Terminated,
    });

    let (state_tx, _state_rx) = mpsc::channel::<StateUpdate>(4);
    let cancel = CancellationToken::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        run_monitor_with_source(params, source, state_tx, cancel),
    )
    .await
    .expect("must complete within 5s");

    assert_eq!(
        n1.count(NotifyHook::RunComplete),
        1,
        "first notifier must fire"
    );
    assert_eq!(
        n2.count(NotifyHook::RunComplete),
        1,
        "second notifier must fire"
    );
}

// Recording notifier — captures every notifier hook invocation so
// integration tests can pin fan-out behaviour (which hook fired, with
// what RunContext, with what RunSummary or JobResult) without standing
// up a real DiscordNotifier (twilight-http + wiremock + crypto
// provider) or LocalMailNotifier (real /var/mail spool).
//
// Usage:
//
//   let recorder = Arc::new(RecordingNotifier::new("discord"));
//   let notifiers: Vec<Arc<dyn DynNotifier>> = vec![Arc::clone(&recorder) as _];
//   // ... drive the dispatcher / monitor against this notifier ...
//   let calls = recorder.calls();
//   assert_eq!(calls.len(), 1);
//   assert_eq!(calls[0].hook, NotifyHook::RunComplete);
//
// The recorder also supports scripted outcomes — the caller pre-loads
// a deque of `Result<NotifyOutcome, NotifyError>` shapes and each
// hook invocation pops one. When the deque is exhausted, the default
// is `Ok(NotifyOutcome::Sent { receipt: "recorded" })`. This lets
// tests pin behaviour for "first hook fails, second succeeds"
// scenarios that the production notifier kinds (Discord, mail) can
// only produce by manipulating their backing service.

#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::Mutex;

use tokio_util::sync::CancellationToken;

use gcit::flow::dispatcher::DynNotifier;
use gcit::github::{JobResult, RunSummary};
use gcit::notify::{NotifyError, NotifyOutcome, RunContext};

/// Which hook the notifier observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotifyHook {
    RunStart,
    JobComplete,
    RunComplete,
}

/// One captured invocation. The full RunContext, RunSummary, and
/// JobResult are cloned in so post-test assertions can match on any
/// field without holding a borrow into the notifier's internal state.
#[derive(Debug, Clone)]
pub struct NotifyCall {
    pub hook: NotifyHook,
    pub ctx: RunContext,
    /// Populated for JobComplete only.
    pub job: Option<JobResult>,
    /// Populated for RunComplete only.
    pub summary: Option<RunSummary>,
}

/// One scripted outcome. The recorder pops one per hook invocation
/// (in invocation order across hooks). Each outcome is a `Result`
/// because notifier failures are first-class — the dispatcher's
/// fan-out path treats one notifier's failure as isolated from
/// others, and tests need to pin that.
type Scripted = Result<NotifyOutcome, NotifyError>;

/// Inner state behind the notifier's `&self` API. The recorder is
/// always wrapped in `Arc` so multiple references can drive the
/// dispatcher's fan-out paths and the test's assertion phase.
struct Inner {
    /// Recorded invocations in order.
    calls: Vec<NotifyCall>,
    /// Scripted outcomes; popped front per invocation. Empty queue
    /// produces the default `Ok(Sent { receipt: "recorded" })`.
    scripted: VecDeque<Scripted>,
}

/// Notifier that records every hook + (optionally) returns scripted
/// outcomes per call.
pub struct RecordingNotifier {
    /// Stable id for `Notifier::id` — useful when a flow has more than
    /// one recording notifier and the test wants to disambiguate
    /// captures.
    id: String,
    inner: Mutex<Inner>,
}

impl RecordingNotifier {
    /// Build with no scripted outcomes — every hook returns the
    /// default `Ok(Sent { receipt: "recorded" })`.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            inner: Mutex::new(Inner {
                calls: Vec::new(),
                scripted: VecDeque::new(),
            }),
        }
    }

    /// Build with a scripted outcome list. The first hook invocation
    /// pops the front and returns it; subsequent invocations pop in
    /// order; an empty list falls back to the default Ok variant.
    pub fn with_scripted(id: impl Into<String>, outcomes: Vec<Scripted>) -> Self {
        Self {
            id: id.into(),
            inner: Mutex::new(Inner {
                calls: Vec::new(),
                scripted: outcomes.into(),
            }),
        }
    }

    /// Snapshot of recorded calls. Cloned out so the caller can
    /// inspect post-drop without holding the lock.
    pub fn calls(&self) -> Vec<NotifyCall> {
        self.inner
            .lock()
            .expect("recording notifier mutex")
            .calls
            .clone()
    }

    /// Count by hook variant. Convenience for "fan-out fired exactly
    /// N times" assertions.
    pub fn count(&self, hook: NotifyHook) -> usize {
        self.inner
            .lock()
            .expect("recording notifier mutex")
            .calls
            .iter()
            .filter(|c| c.hook == hook)
            .count()
    }

    /// Pop the next scripted outcome (or fall back to the default).
    /// Records the call regardless of script result.
    fn next_outcome(&self, call: NotifyCall) -> Scripted {
        let mut inner = self.inner.lock().expect("recording notifier mutex");
        inner.calls.push(call);
        inner.scripted.pop_front().unwrap_or_else(|| {
            Ok(NotifyOutcome::Sent {
                receipt: "recorded".to_string(),
            })
        })
    }
}

impl DynNotifier for RecordingNotifier {
    fn kind(&self) -> &'static str {
        "recording"
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn on_run_start<'a>(
        &'a self,
        ctx: &'a RunContext,
        _cancel: &'a CancellationToken,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<NotifyOutcome, NotifyError>> + Send + 'a>,
    > {
        let call = NotifyCall {
            hook: NotifyHook::RunStart,
            ctx: ctx.clone(),
            job: None,
            summary: None,
        };
        Box::pin(async move { self.next_outcome(call) })
    }

    fn on_job_complete<'a>(
        &'a self,
        ctx: &'a RunContext,
        job: &'a JobResult,
        _cancel: &'a CancellationToken,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<NotifyOutcome, NotifyError>> + Send + 'a>,
    > {
        let call = NotifyCall {
            hook: NotifyHook::JobComplete,
            ctx: ctx.clone(),
            job: Some(job.clone()),
            summary: None,
        };
        Box::pin(async move { self.next_outcome(call) })
    }

    fn on_run_complete<'a>(
        &'a self,
        ctx: &'a RunContext,
        summary: &'a RunSummary,
        _cancel: &'a CancellationToken,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<NotifyOutcome, NotifyError>> + Send + 'a>,
    > {
        let call = NotifyCall {
            hook: NotifyHook::RunComplete,
            ctx: ctx.clone(),
            job: None,
            summary: Some(summary.clone()),
        };
        Box::pin(async move { self.next_outcome(call) })
    }
}

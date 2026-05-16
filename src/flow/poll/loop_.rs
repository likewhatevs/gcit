// Driver: the per-flow polling loop body.
//
// `run_with_executor` is the strategy-agnostic outer loop that wraps
// any `PollExecutor` (production = `RealPollExecutor`, tests =
// `ScriptedPollExecutor`). It owns sleep/jitter, the rate-bucket
// gate, the cancel race, and the per-arm wiring into the state
// writer + dispatcher trigger channel.

use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::flow::supervisor::record_last_error;
use crate::flow::TriggerSignal;
use crate::git::{compare_sha, strategy, PollOutcome};
use crate::state::StateUpdate;

use super::executor::{PollCycleError, PollExecutor};
use super::send::{send_trigger_then_observation, SendOutcome};
use super::PollParams;

/// Generic poll-loop driver. Production goes through `super::run`,
/// which constructs a `RealPollExecutor`; the supervisor end-to-end
/// test harness in `tests/poll_unborn_ref.rs` wires in a
/// `ScriptedPollExecutor`.
pub async fn run_with_executor<E>(
    params: PollParams,
    executor: E,
    initial_last_sha: Option<gix_hash::ObjectId>,
    initial_last_dispatched_at: Option<DateTime<Utc>>,
    state_tx: Sender<StateUpdate>,
    trigger_tx: Sender<TriggerSignal>,
    cancel: CancellationToken,
) where
    E: PollExecutor,
{
    info!(
        target: "gcit::flow::poll",
        flow = %params.flow_name,
        url = %params.url,
        ref_name = %params.ref_name,
        strategy = executor.strategy_label(),
        interval_secs = params.effective_poll.source_interval.as_secs(),
        cooldown_secs = params.effective_poll.cooldown.as_secs(),
        "poll loop starting",
    );
    let mut last_sha = initial_last_sha;
    let mut last_dispatched_at = initial_last_dispatched_at;
    // Per-flow jitter RNG seeded by flow name so two flows polling
    // the same upstream stay phase-shifted across daemon restarts.
    let mut rng = fastrand::Rng::with_seed(seed_from_name(&params.flow_name));
    // First `Refreshed` / `Unchanged` clears any stale last_error from
    // before respawn; subsequent successes skip the mutex lock.
    let mut last_error_cleared = false;

    loop {
        let sleep_for = strategy::apply_jitter(
            params.effective_poll.source_interval,
            params.effective_poll.jitter,
            &mut rng,
        );
        tokio::select! {
            _ = cancel.cancelled() => {
                info!(target: "gcit::flow::poll", flow = %params.flow_name, "poll loop cancelled");
                return;
            }
            _ = tokio::time::sleep(sleep_for) => {}
        }

        if let Some(b) = &params.rate_bucket {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = b.acquire() => {}
            }
        }

        let outcome = match executor.poll_cycle(&params, &cancel).await {
            Ok(o) => o,
            Err(PollCycleError::Cancelled) => {
                // Supervisor revoked the token. Do NOT record a
                // last_error — `git_poll_failed: cancelled` would be
                // a transient lie in `gcit status`.
                info!(
                    target: "gcit::flow::poll",
                    flow = %params.flow_name,
                    "poll loop cancelled mid-cycle",
                );
                return;
            }
            Err(PollCycleError::Failed(message)) => {
                warn!(
                    target: "gcit::flow::poll",
                    flow = %params.flow_name,
                    error = %message,
                    "poll cycle failed; continuing on cadence",
                );
                record_last_error(
                    &params.last_errors,
                    &params.flow_name,
                    "git_poll_failed",
                    &message,
                    None,
                )
                .await;
                last_error_cleared = false;
                continue;
            }
        };

        match outcome {
            PollOutcome::Refreshed { sha } => {
                let now = Utc::now();
                let diff = compare_sha(last_sha, sha);
                if diff.trigger {
                    let cooled_down =
                        is_cooled_down(last_dispatched_at, now, params.effective_poll.cooldown);
                    if cooled_down {
                        // `from_std` only fails for durations beyond
                        // i64::MAX ms — unreachable for a cooldown.
                        let cooldown_until =
                            chrono::Duration::from_std(params.effective_poll.cooldown)
                                .ok()
                                .map(|d| now + d);
                        let observation = StateUpdate::PollObservation {
                            flow: params.flow_name.clone(),
                            last_sha: sha,
                            last_poll_at: now,
                            last_dispatched_at: Some(now),
                            cooldown_until,
                        };
                        let signal = Some(TriggerSignal {
                            observed_sha: sha,
                            observed_at: now,
                        });
                        match send_trigger_then_observation(
                            &params.flow_name,
                            signal,
                            observation,
                            &state_tx,
                            &trigger_tx,
                            &cancel,
                        )
                        .await
                        {
                            SendOutcome::Sent => {}
                            SendOutcome::Cancelled
                            | SendOutcome::TriggerChannelClosed
                            | SendOutcome::StateChannelClosed => return,
                        }
                        last_dispatched_at = Some(now);
                        last_sha = Some(diff.observed);
                    } else {
                        // Cooldown active: suppress trigger, refresh
                        // last_poll_at only, leave in-memory last_sha
                        // unchanged so the diff re-fires after the
                        // window elapses.
                        debug!(
                            target: "gcit::flow::poll",
                            flow = %params.flow_name,
                            "cooldown active; suppressing trigger",
                        );
                        if !send_state_or_exit(
                            &state_tx,
                            StateUpdate::PollTimestamp {
                                flow: params.flow_name.clone(),
                                last_poll_at: now,
                            },
                            &params.flow_name,
                        )
                        .await
                        {
                            return;
                        }
                    }
                } else {
                    // Baseline / unchanged-since-last-observation.
                    // last_dispatched_at: None preserves the prior
                    // cooldown timestamp via the apply rule; the
                    // companion cooldown_until: None follows the
                    // same paired contract.
                    let observation = StateUpdate::PollObservation {
                        flow: params.flow_name.clone(),
                        last_sha: sha,
                        last_poll_at: now,
                        last_dispatched_at: None,
                        cooldown_until: None,
                    };
                    if !send_state_or_exit(&state_tx, observation, &params.flow_name).await {
                        return;
                    }
                    last_sha = Some(diff.observed);
                }
                maybe_clear_last_error(&params, &mut last_error_cleared).await;
            }
            PollOutcome::Unchanged => {
                // Grokmirror fingerprint match: liveness without a
                // fresh ObjectId. Refresh last_poll_at via
                // PollTimestamp so `gcit status` shows the flow is
                // alive.
                let now = Utc::now();
                if !send_state_or_exit(
                    &state_tx,
                    StateUpdate::PollTimestamp {
                        flow: params.flow_name.clone(),
                        last_poll_at: now,
                    },
                    &params.flow_name,
                )
                .await
                {
                    return;
                }
                maybe_clear_last_error(&params, &mut last_error_cleared).await;
            }
            PollOutcome::UnbornRef => {
                // Configuration condition (operator typo, branch
                // deleted, branch not yet pushed). Surface under
                // `git_poll_failed`; the next Refreshed/Unchanged
                // clears it via the `last_error_cleared` latch.
                // Liveness: emit PollTimestamp so last_poll_at
                // advances despite no fresh ObjectId.
                let now = Utc::now();
                if !send_state_or_exit(
                    &state_tx,
                    StateUpdate::PollTimestamp {
                        flow: params.flow_name.clone(),
                        last_poll_at: now,
                    },
                    &params.flow_name,
                )
                .await
                {
                    return;
                }
                let message = unborn_ref_message(&params.flow_name, &params.url, &params.ref_name);
                warn!(
                    target: "gcit::flow::poll",
                    flow = %params.flow_name,
                    url = %params.url,
                    ref_name = %params.ref_name,
                    "configured ref does not exist on remote; continuing on cadence",
                );
                record_last_error(
                    &params.last_errors,
                    &params.flow_name,
                    "git_poll_failed",
                    &message,
                    None,
                )
                .await;
                last_error_cleared = false;
            }
        }
    }
}

/// Send `update` and return `true` on success; on a closed writer,
/// log and return `false` so the caller can exit the loop. Factored
/// to dedupe the four mpsc-send-or-exit blocks in the per-arm wiring.
async fn send_state_or_exit(
    state_tx: &Sender<StateUpdate>,
    update: StateUpdate,
    flow_name: &str,
) -> bool {
    if state_tx.send(update).await.is_err() {
        debug!(
            target: "gcit::flow::poll",
            flow = %flow_name,
            "state writer dropped; exiting",
        );
        false
    } else {
        true
    }
}

/// First successful Refreshed/Unchanged after spawn — drop any stale
/// last_error left over from a prior respawn. Idempotent via the
/// `cleared` latch so steady-state polls don't lock the mutex.
async fn maybe_clear_last_error(params: &PollParams, cleared: &mut bool) {
    if !*cleared {
        params.last_errors.lock().await.remove(&params.flow_name);
        *cleared = true;
    }
}

/// Operator-facing message for `PollOutcome::UnbornRef`. Names the
/// flow, the ref, and the source URL so an operator can route the
/// fix from `gcit status`.
fn unborn_ref_message(flow: &str, url: &str, ref_name: &str) -> String {
    format!(
        "ref {ref_name} not found at {url} — verify the ref exists upstream, wait if it's still being pushed, or update flow.{flow}.source.ref",
    )
}

/// Cooldown gate. Three cases: no prior dispatch → allow; clock
/// moved backwards → allow (don't strand on operator clock skew —
/// `last_dispatched_at` is wall-clock and persists across restart,
/// so a backwards ntp step would otherwise indefinitely suppress
/// dispatch); elapsed >= cooldown → allow. `Duration::ZERO`
/// disables the gate.
fn is_cooled_down(
    last_dispatched_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    cooldown: Duration,
) -> bool {
    let Some(t) = last_dispatched_at else {
        return true;
    };
    if now < t {
        return true;
    }
    // `now >= t` here, so `signed_duration_since` is non-negative
    // and `to_std` cannot fail. The expect surfaces any future
    // reordering that breaks this invariant rather than silently
    // bypassing the cooldown.
    let elapsed = now
        .signed_duration_since(t)
        .to_std()
        .expect("elapsed is non-negative after the now<t guard above");
    elapsed >= cooldown
}

/// Hash a flow name to seed the per-flow jitter RNG. Two flows
/// polling the same upstream stay phase-shifted across restarts.
fn seed_from_name(name: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut h);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flow::poll::EffectivePoll;
    use crate::util::test_sha as sha;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn seed_from_name_is_deterministic() {
        assert_eq!(seed_from_name("flow-a"), seed_from_name("flow-a"));
        assert_ne!(seed_from_name("flow-a"), seed_from_name("flow-b"));
    }

    #[test]
    fn is_cooled_down_none_returns_true() {
        let now = chrono::Utc::now();
        assert!(is_cooled_down(None, now, Duration::from_secs(60)));
    }

    #[test]
    fn is_cooled_down_within_window_returns_false() {
        let now = chrono::Utc::now();
        let last = now - chrono::Duration::seconds(30);
        assert!(!is_cooled_down(Some(last), now, Duration::from_secs(60)));
    }

    #[test]
    fn is_cooled_down_past_window_returns_true() {
        let now = chrono::Utc::now();
        let last = now - chrono::Duration::seconds(120);
        assert!(is_cooled_down(Some(last), now, Duration::from_secs(60)));
    }

    #[test]
    fn is_cooled_down_clock_skew_backward_returns_true() {
        // Backwards clock must not strand a flow under cooldown.
        let now = chrono::Utc::now();
        let last = now + chrono::Duration::seconds(30);
        assert!(is_cooled_down(Some(last), now, Duration::from_secs(60)));
    }

    #[test]
    fn is_cooled_down_zero_cooldown_always_true() {
        let now = chrono::Utc::now();
        let last = now - chrono::Duration::seconds(1);
        assert!(is_cooled_down(Some(last), now, Duration::ZERO));
        assert!(is_cooled_down(Some(now), now, Duration::ZERO));
    }

    #[test]
    fn unborn_ref_message_names_flow_ref_and_url() {
        let m = unborn_ref_message("kernel-rc", "https://github.com/o/r", "refs/heads/main");
        assert!(m.contains("refs/heads/main"));
        assert!(m.contains("https://github.com/o/r"));
        assert!(m.contains("flow.kernel-rc.source.ref"));
    }

    #[test]
    fn unborn_ref_message_exact_format_is_stable() {
        let expected = "ref refs/tags/v1.0 not found at https://example.com/repo.git — verify the ref exists upstream, wait if it's still being pushed, or update flow.flow-x.source.ref";
        assert_eq!(
            unborn_ref_message("flow-x", "https://example.com/repo.git", "refs/tags/v1.0"),
            expected,
        );
    }

    /// PollExecutor that panics on call — used to prove the loop's
    /// entry-time cancel arm fires BEFORE the executor is reached.
    struct NeverFiresExecutor;

    impl PollExecutor for NeverFiresExecutor {
        fn strategy_label(&self) -> &'static str {
            "never-fires"
        }
        async fn poll_cycle(
            &self,
            _params: &PollParams,
            _cancel: &CancellationToken,
        ) -> Result<PollOutcome, PollCycleError> {
            panic!("NeverFiresExecutor::poll_cycle called — entry-time cancel arm must win first",);
        }
    }

    /// Scripted executor: pops outcomes from a queue, returns
    /// `Cancelled` when empty so the loop exits cleanly.
    struct UnitScriptedPollExecutor {
        script: tokio::sync::Mutex<std::collections::VecDeque<Result<PollOutcome, PollCycleError>>>,
    }

    impl UnitScriptedPollExecutor {
        fn new(outcomes: Vec<Result<PollOutcome, PollCycleError>>) -> Self {
            Self {
                script: tokio::sync::Mutex::new(outcomes.into()),
            }
        }
    }

    impl PollExecutor for UnitScriptedPollExecutor {
        fn strategy_label(&self) -> &'static str {
            "unit-scripted"
        }
        async fn poll_cycle(
            &self,
            _params: &PollParams,
            _cancel: &CancellationToken,
        ) -> Result<PollOutcome, PollCycleError> {
            let mut g = self.script.lock().await;
            g.pop_front().unwrap_or(Err(PollCycleError::Cancelled))
        }
    }

    fn unit_poll_params(
        flow_name: &str,
        last_errors: Arc<
            tokio::sync::Mutex<BTreeMap<String, crate::flow::supervisor::FlowLastError>>,
        >,
    ) -> PollParams {
        PollParams {
            flow_name: flow_name.to_string(),
            url: "https://example.com/repo.git".to_string(),
            ref_name: "refs/heads/main".to_string(),
            effective_poll: EffectivePoll {
                source_interval: Duration::from_millis(1),
                job_interval: Duration::from_secs(30),
                jitter: 0.0,
                cooldown: Duration::ZERO,
            },
            rate_bucket: None,
            octo: None,
            reqwest: None,
            last_errors,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn run_with_executor_unchanged_emits_poll_timestamp_and_no_trigger() {
        let last_errors = Arc::new(tokio::sync::Mutex::new(BTreeMap::new()));
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        let (trigger_tx, mut trigger_rx) = tokio::sync::mpsc::channel::<TriggerSignal>(8);
        let cancel = CancellationToken::new();
        let params = unit_poll_params("unchanged-flow", Arc::clone(&last_errors));
        let executor = UnitScriptedPollExecutor::new(vec![
            Ok(PollOutcome::Unchanged),
            Err(PollCycleError::Cancelled),
        ]);

        let cancel_for_task = cancel.clone();
        let task = tokio::spawn(async move {
            run_with_executor(
                params,
                executor,
                None,
                None,
                state_tx,
                trigger_tx,
                cancel_for_task,
            )
            .await;
        });

        // Drive virtual time and poll for the PollTimestamp — fixed
        // budgets race the scheduler under load.
        let mut saw_timestamp = false;
        for _ in 0..400 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            while let Ok(update) = state_rx.try_recv() {
                if let StateUpdate::PollTimestamp { flow, .. } = update {
                    assert_eq!(flow, "unchanged-flow");
                    saw_timestamp = true;
                }
            }
            if saw_timestamp {
                break;
            }
        }
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("loop must exit within 5s of cancel")
            .expect("loop must not panic");

        assert!(saw_timestamp, "Unchanged must emit PollTimestamp");
        assert!(trigger_rx.try_recv().is_err(), "Unchanged must NOT trigger");
    }

    #[tokio::test(start_paused = true)]
    async fn run_with_executor_failed_executor_records_last_error_and_continues() {
        let last_errors = Arc::new(tokio::sync::Mutex::new(BTreeMap::new()));
        let (state_tx, _state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        let (trigger_tx, _trigger_rx) = tokio::sync::mpsc::channel::<TriggerSignal>(8);
        let cancel = CancellationToken::new();
        let params = unit_poll_params("failed-flow", Arc::clone(&last_errors));
        let executor = UnitScriptedPollExecutor::new(vec![
            Err(PollCycleError::Failed(
                "upstream 502 bad gateway".to_string(),
            )),
            Err(PollCycleError::Cancelled),
        ]);

        let cancel_for_task = cancel.clone();
        let task = tokio::spawn(async move {
            run_with_executor(
                params,
                executor,
                None,
                None,
                state_tx,
                trigger_tx,
                cancel_for_task,
            )
            .await;
        });
        let mut recorded = false;
        for _ in 0..400 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            if last_errors.lock().await.contains_key("failed-flow") {
                recorded = true;
                break;
            }
        }
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("loop must exit")
            .expect("loop must not panic");

        assert!(recorded, "Failed cycle must record last_error");
        let g = last_errors.lock().await;
        let entry = g.get("failed-flow").expect("entry under flow name");
        assert_eq!(entry.kind(), "git_poll_failed");
        assert!(
            entry.message().contains("upstream 502 bad gateway"),
            "verbatim Failed body must surface in last_error; got: {:?}",
            entry.message(),
        );
    }

    #[tokio::test(start_paused = true)]
    async fn run_with_executor_refreshed_with_diff_emits_observation_and_trigger() {
        let last_errors = Arc::new(tokio::sync::Mutex::new(BTreeMap::new()));
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        let (trigger_tx, mut trigger_rx) = tokio::sync::mpsc::channel::<TriggerSignal>(8);
        let cancel = CancellationToken::new();
        let params = unit_poll_params("diff-flow", Arc::clone(&last_errors));
        let new_sha = sha(0xab);
        let executor = UnitScriptedPollExecutor::new(vec![
            Ok(PollOutcome::Refreshed { sha: new_sha }),
            Err(PollCycleError::Cancelled),
        ]);
        let initial = Some(sha(0xff));

        let cancel_for_task = cancel.clone();
        let task = tokio::spawn(async move {
            run_with_executor(
                params,
                executor,
                initial,
                None,
                state_tx,
                trigger_tx,
                cancel_for_task,
            )
            .await;
        });
        let mut received: Option<TriggerSignal> = None;
        let mut saw_observation = false;
        for _ in 0..400 {
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
            if received.is_none() {
                if let Ok(t) = trigger_rx.try_recv() {
                    received = Some(t);
                }
            }
            while let Ok(update) = state_rx.try_recv() {
                if let StateUpdate::PollObservation { flow, last_sha, .. } = update {
                    if flow == "diff-flow" && last_sha == new_sha {
                        saw_observation = true;
                    }
                }
            }
            if received.is_some() && saw_observation {
                break;
            }
        }
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("loop must exit")
            .expect("loop must not panic");

        let received = received.expect("Refreshed-with-diff must emit a TriggerSignal");
        assert_eq!(received.observed_sha, new_sha);
        assert!(saw_observation, "must emit PollObservation with new SHA");
    }

    #[tokio::test(start_paused = true)]
    async fn run_with_executor_returns_immediately_on_pre_cancelled_token() {
        // Pre-cancelled token must exit at the top-of-loop select!
        // before the executor is reached. Under `start_paused = true`
        // virtual time only advances when nothing else is ready;
        // `cancel.cancelled()` is immediately ready (token already
        // fired) so the select! resolves it without ever polling
        // `tokio::time::sleep`. Pin two facts: the loop returns AT
        // ALL (the timeout would catch a hang), and virtual time
        // does not advance (the sleep arm never won).
        let cancel = CancellationToken::new();
        cancel.cancel();
        let params = PollParams {
            flow_name: "entry-cancel-flow".to_string(),
            url: "https://example.com/repo.git".to_string(),
            ref_name: "refs/heads/main".to_string(),
            effective_poll: EffectivePoll {
                source_interval: Duration::from_secs(60),
                job_interval: Duration::from_secs(30),
                jitter: 0.0,
                cooldown: Duration::ZERO,
            },
            rate_bucket: None,
            octo: None,
            reqwest: None,
            last_errors: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
        };
        let (state_tx, _state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(8);
        let (trigger_tx, _trigger_rx) = tokio::sync::mpsc::channel::<TriggerSignal>(8);
        let executor = NeverFiresExecutor;
        let virtual_start = tokio::time::Instant::now();
        tokio::time::timeout(
            Duration::from_secs(5),
            run_with_executor(params, executor, None, None, state_tx, trigger_tx, cancel),
        )
        .await
        .expect("pre-cancelled token must terminate the loop");
        let virtual_elapsed = virtual_start.elapsed();
        // Virtual time may advance by microseconds while the runtime
        // schedules — pin that it doesn't approach the source_interval
        // (60s) which would mean the sleep arm won.
        assert!(
            virtual_elapsed < Duration::from_secs(1),
            "loop must exit on cancel arm without consuming source_interval; virtual elapsed: {virtual_elapsed:?}",
        );
    }
}

// Trigger-then-observation sender: enforces the order in which the
// poll loop hands a `TriggerSignal` to the dispatcher and a
// `StateUpdate::PollObservation` to the state writer.

use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::flow::TriggerSignal;
use crate::state::StateUpdate;

/// Outcome of `send_trigger_then_observation`. The caller matches on
/// this to decide whether to continue (`Sent`) or exit. Distinct
/// non-Sent variants tell *which* side of the sequence terminated
/// the loop.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SendOutcome {
    Sent,
    /// `cancel.cancelled()` won the trigger-arm select. Per tokio
    /// mpsc cancel-safety the trigger is NOT enqueued; the
    /// observation is skipped too, so state.json keeps the old SHA
    /// and the next-gen poll re-detects.
    Cancelled,
    /// Dispatcher recv-end dropped. Poll task exits without
    /// persisting the observation.
    TriggerChannelClosed,
    /// State writer recv-end dropped. Trigger was already delivered.
    StateChannelClosed,
}

/// Send the trigger (if any) first, then the observation. Trigger
/// FIRST so a cancel between the two leaves state.json with the OLD
/// SHA — the next poll re-detects and re-fires; observation-first
/// would advance state and lose the trigger silently.
///
/// The trigger send is cancel-raced so a wedged dispatcher does not
/// block shutdown. The observation send is NOT cancel-raced: once
/// the trigger is accepted, we want the observation to flush so the
/// in-memory and on-disk SHA stay in sync. If `state_tx` is closed
/// after the trigger flushed, `StateChannelClosed` lets the caller
/// exit; the trigger has been delivered, so dispatch is at-least-once
/// across that residual window.
///
/// `signal == None` is the "no SHA diff" path: skip the trigger,
/// only persist the observation.
pub(crate) async fn send_trigger_then_observation(
    flow_name: &str,
    signal: Option<TriggerSignal>,
    observation: StateUpdate,
    state_tx: &Sender<StateUpdate>,
    trigger_tx: &Sender<TriggerSignal>,
    cancel: &CancellationToken,
) -> SendOutcome {
    if let Some(s) = signal {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!(
                    target: "gcit::flow::poll",
                    flow = %flow_name,
                    "poll loop cancelled while waiting on dispatcher trigger queue",
                );
                return SendOutcome::Cancelled;
            }
            r = trigger_tx.send(s) => {
                if let Err(e) = r {
                    debug!(
                        target: "gcit::flow::poll",
                        flow = %flow_name,
                        error = %e,
                        "dispatcher trigger channel closed; poll task exiting",
                    );
                    return SendOutcome::TriggerChannelClosed;
                }
            }
        }
    }
    if state_tx.send(observation).await.is_err() {
        debug!(
            target: "gcit::flow::poll",
            flow = %flow_name,
            "state writer dropped; exiting",
        );
        return SendOutcome::StateChannelClosed;
    }
    SendOutcome::Sent
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::{test_sha as sha, test_ts as ts};
    use std::time::Duration;

    /// Order invariant: pre-fill the state channel; the helper's
    /// observation send blocks while the trigger send completes
    /// first. A reordered helper (observation-first) would race
    /// for the state slot before the trigger ever arrives.
    #[tokio::test]
    async fn send_trigger_then_observation_sends_trigger_before_observation() {
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(1);
        let (trigger_tx, mut trigger_rx) = tokio::sync::mpsc::channel::<TriggerSignal>(1);
        let cancel = CancellationToken::new();

        state_tx
            .send(StateUpdate::PollObservation {
                flow: "(prefilled)".to_string(),
                last_sha: sha(0xff),
                last_poll_at: ts(0),
                last_dispatched_at: None,
                cooldown_until: None,
            })
            .await
            .unwrap();

        let observed_sha = sha(0xab);
        let signal = Some(TriggerSignal {
            observed_sha,
            observed_at: ts(100),
        });
        let observation = StateUpdate::PollObservation {
            flow: "ordering-flow".to_string(),
            last_sha: observed_sha,
            last_poll_at: ts(100),
            last_dispatched_at: None,
            cooldown_until: None,
        };

        let helper = tokio::spawn({
            let state_tx = state_tx.clone();
            let trigger_tx = trigger_tx.clone();
            let cancel = cancel.clone();
            async move {
                send_trigger_then_observation(
                    "ordering-flow",
                    signal,
                    observation,
                    &state_tx,
                    &trigger_tx,
                    &cancel,
                )
                .await
            }
        });

        let received_trigger = tokio::time::timeout(Duration::from_secs(5), trigger_rx.recv())
            .await
            .expect("trigger must arrive within 5s")
            .expect("trigger channel must yield Some");
        assert_eq!(received_trigger.observed_sha, observed_sha);
        assert_eq!(received_trigger.observed_at, ts(100));

        // First state recv must be the sentinel — a reordered helper
        // would have written the observation here.
        let first_state = tokio::time::timeout(Duration::from_secs(5), state_rx.recv())
            .await
            .expect("state recv 1 must complete within 5s")
            .expect("state channel must yield Some");
        match first_state {
            StateUpdate::PollObservation { flow, .. } => assert_eq!(flow, "(prefilled)"),
            other => panic!("expected sentinel PollObservation, got {other:?}"),
        }

        let second_state = tokio::time::timeout(Duration::from_secs(5), state_rx.recv())
            .await
            .expect("state recv 2 must complete within 5s")
            .expect("state channel must yield Some");
        match second_state {
            StateUpdate::PollObservation { flow, last_sha, .. } => {
                assert_eq!(flow, "ordering-flow");
                assert_eq!(last_sha, observed_sha);
            }
            other => panic!("expected ordering-flow PollObservation, got {other:?}"),
        }
        let outcome = tokio::time::timeout(Duration::from_secs(5), helper)
            .await
            .expect("helper must complete within 5s")
            .expect("helper task panicked");
        assert_eq!(outcome, SendOutcome::Sent);
    }

    /// `signal == None` (baseline / unchanged-poll path) skips the
    /// trigger send entirely; the trigger channel stays empty.
    #[tokio::test]
    async fn send_trigger_then_observation_skips_trigger_when_signal_none() {
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(1);
        let (trigger_tx, mut trigger_rx) = tokio::sync::mpsc::channel::<TriggerSignal>(1);
        let cancel = CancellationToken::new();

        let observation = StateUpdate::PollObservation {
            flow: "no-diff-flow".to_string(),
            last_sha: sha(0x10),
            last_poll_at: ts(50),
            last_dispatched_at: None,
            cooldown_until: None,
        };
        let outcome = send_trigger_then_observation(
            "no-diff-flow",
            None,
            observation,
            &state_tx,
            &trigger_tx,
            &cancel,
        )
        .await;
        assert_eq!(outcome, SendOutcome::Sent);

        let recv_state = state_rx.recv().await.expect("observation must arrive");
        match recv_state {
            StateUpdate::PollObservation { flow, .. } => assert_eq!(flow, "no-diff-flow"),
            other => panic!("expected PollObservation, got {other:?}"),
        }
        assert!(matches!(
            trigger_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty),
        ));
    }

    /// Cancellation preempts a BLOCKED trigger send. Fill the
    /// trigger mpsc so the helper parks on send, then cancel; the
    /// cancel arm becomes the only ready branch. Pre-cancel would
    /// race the trigger send for the select! pseudo-random pick;
    /// this scenario pins the blocked-send case deterministically.
    #[tokio::test]
    async fn send_trigger_then_observation_returns_cancelled_when_trigger_send_blocks() {
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(1);
        let (trigger_tx, mut trigger_rx) = tokio::sync::mpsc::channel::<TriggerSignal>(1);
        let cancel = CancellationToken::new();

        trigger_tx
            .send(TriggerSignal {
                observed_sha: sha(0x00),
                observed_at: ts(0),
            })
            .await
            .unwrap();

        let observation = StateUpdate::PollObservation {
            flow: "cancelled-flow".to_string(),
            last_sha: sha(0x42),
            last_poll_at: ts(200),
            last_dispatched_at: None,
            cooldown_until: None,
        };
        let helper = tokio::spawn({
            let state_tx = state_tx.clone();
            let trigger_tx = trigger_tx.clone();
            let cancel = cancel.clone();
            async move {
                send_trigger_then_observation(
                    "cancelled-flow",
                    Some(TriggerSignal {
                        observed_sha: sha(0x42),
                        observed_at: ts(200),
                    }),
                    observation,
                    &state_tx,
                    &trigger_tx,
                    &cancel,
                )
                .await
            }
        });

        tokio::task::yield_now().await;
        cancel.cancel();

        let outcome = tokio::time::timeout(Duration::from_secs(5), helper)
            .await
            .expect("helper must complete within 5s")
            .expect("helper task panicked");
        assert_eq!(outcome, SendOutcome::Cancelled);

        // Helper's trigger was NOT enqueued (mpsc cancel-safety).
        let pre_filled = trigger_rx.try_recv().expect("pre-fill trigger present");
        assert_eq!(pre_filled.observed_sha, sha(0x00));
        assert!(matches!(
            trigger_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty),
        ));
        assert!(matches!(
            state_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty),
        ));
    }

    /// Dispatcher recv-end dropped before helper runs: trigger send
    /// returns SendError, helper returns `TriggerChannelClosed`,
    /// observation is NOT attempted.
    #[tokio::test]
    async fn send_trigger_then_observation_returns_trigger_channel_closed() {
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(1);
        let (trigger_tx, trigger_rx) = tokio::sync::mpsc::channel::<TriggerSignal>(1);
        drop(trigger_rx);
        let cancel = CancellationToken::new();

        let observation = StateUpdate::PollObservation {
            flow: "closed-trigger".to_string(),
            last_sha: sha(0x07),
            last_poll_at: ts(7),
            last_dispatched_at: None,
            cooldown_until: None,
        };
        let outcome = send_trigger_then_observation(
            "closed-trigger",
            Some(TriggerSignal {
                observed_sha: sha(0x07),
                observed_at: ts(7),
            }),
            observation,
            &state_tx,
            &trigger_tx,
            &cancel,
        )
        .await;
        assert_eq!(outcome, SendOutcome::TriggerChannelClosed);
        assert!(matches!(
            state_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty),
        ));
    }

    /// State writer dropped after trigger sent: helper returns
    /// `StateChannelClosed`; the trigger was already delivered so
    /// dispatch is at-least-once across this residual window.
    #[tokio::test]
    async fn send_trigger_then_observation_returns_state_channel_closed_after_trigger_sent() {
        let (state_tx, state_rx) = tokio::sync::mpsc::channel::<StateUpdate>(1);
        drop(state_rx);
        let (trigger_tx, mut trigger_rx) = tokio::sync::mpsc::channel::<TriggerSignal>(1);
        let cancel = CancellationToken::new();

        let observation = StateUpdate::PollObservation {
            flow: "closed-state".to_string(),
            last_sha: sha(0x09),
            last_poll_at: ts(9),
            last_dispatched_at: None,
            cooldown_until: None,
        };
        let outcome = send_trigger_then_observation(
            "closed-state",
            Some(TriggerSignal {
                observed_sha: sha(0x09),
                observed_at: ts(9),
            }),
            observation,
            &state_tx,
            &trigger_tx,
            &cancel,
        )
        .await;
        assert_eq!(outcome, SendOutcome::StateChannelClosed);
        let recv_trigger = trigger_rx.recv().await.expect("trigger must have arrived");
        assert_eq!(recv_trigger.observed_sha, sha(0x09));
    }
}

// Pure classifier for a JoinSet result. Inspects the exit shape and
// the `respawning_flows` snapshot and returns the next action without
// any side effect. Side-effects happen in `exit::handle_flow_exit`.

use super::super::types::FlowExit;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum RespawnDecision {
    /// Tokio reported a non-panic, non-cancelled JoinError. Should
    /// not happen given the catch_unwind wrapping — log and continue.
    UnexpectedJoinError,
    /// Supervisor cancelled the task (reload or shutdown). No action;
    /// caller's drain owns the handle.
    Cancelled,
    /// Inner future returned cleanly. Drop the handle entry; do NOT
    /// touch `respawning_flows` (the sibling role's panic-respawn,
    /// if any, must still proceed).
    CleanExit,
    /// Inner future panicked but the sibling role is already in the
    /// respawn pipeline. Skip the duplicate.
    PanicDuplicate,
    /// Inner future panicked and this is the first observation.
    /// Cancel the sibling, mark the flow as respawning, arm the
    /// panic-watcher.
    PanicFirst,
}

pub(super) fn decide_respawn(
    joined: &Result<FlowExit, tokio::task::JoinError>,
    already_respawning: bool,
) -> RespawnDecision {
    let exit = match joined {
        Ok(e) => e,
        Err(join_err) if join_err.is_cancelled() => return RespawnDecision::Cancelled,
        Err(_) => return RespawnDecision::UnexpectedJoinError,
    };
    if exit.panic.is_none() {
        return RespawnDecision::CleanExit;
    }
    if already_respawning {
        RespawnDecision::PanicDuplicate
    } else {
        RespawnDecision::PanicFirst
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::types::FlowRole;
    use super::*;

    fn poll_clean(name: &str) -> Result<FlowExit, tokio::task::JoinError> {
        Ok(FlowExit {
            flow: name.to_string(),
            role: FlowRole::Poll,
            panic: None,
        })
    }

    fn dispatcher_clean(name: &str) -> Result<FlowExit, tokio::task::JoinError> {
        Ok(FlowExit {
            flow: name.to_string(),
            role: FlowRole::Dispatcher,
            panic: None,
        })
    }

    fn poll_panic(name: &str, msg: &str) -> Result<FlowExit, tokio::task::JoinError> {
        Ok(FlowExit {
            flow: name.to_string(),
            role: FlowRole::Poll,
            panic: Some(msg.to_string()),
        })
    }

    fn dispatcher_panic(name: &str, msg: &str) -> Result<FlowExit, tokio::task::JoinError> {
        Ok(FlowExit {
            flow: name.to_string(),
            role: FlowRole::Dispatcher,
            panic: Some(msg.to_string()),
        })
    }

    #[test]
    fn decide_respawn_clean_exit_returns_clean_exit() {
        let r = poll_clean("flow1");
        assert_eq!(decide_respawn(&r, false), RespawnDecision::CleanExit);
    }

    #[test]
    fn decide_respawn_clean_exit_with_sibling_already_respawning_returns_clean_exit() {
        // Sibling already respawning doesn't change a clean exit's
        // verdict — the sibling's watcher carries the respawn forward.
        let r = dispatcher_clean("flow1");
        assert_eq!(decide_respawn(&r, true), RespawnDecision::CleanExit);
    }

    #[test]
    fn decide_respawn_first_panic_returns_panic_first() {
        let r = poll_panic("flow1", "boom");
        assert_eq!(decide_respawn(&r, false), RespawnDecision::PanicFirst);
    }

    #[test]
    fn decide_respawn_duplicate_panic_returns_panic_duplicate() {
        let r = dispatcher_panic("flow1", "boom");
        assert_eq!(decide_respawn(&r, true), RespawnDecision::PanicDuplicate);
    }

    #[test]
    fn decide_respawn_panic_race_clean_exit_observed_first_then_panic_returns_panic_first() {
        // Poll returns cleanly first; supervisor's CleanExit arm drops
        // the FlowHandle. Then dispatcher panics. An implementation
        // inferring respawn state from handle membership would
        // incorrectly skip the respawn (handles is empty). The pure
        // decide_respawn looks ONLY at `respawning_flows`, still empty
        // (clean exits don't touch it). Decision: PanicFirst.
        let clean = poll_clean("flow1");
        assert_eq!(decide_respawn(&clean, false), RespawnDecision::CleanExit);
        let panicked = dispatcher_panic("flow1", "boom");
        assert_eq!(
            decide_respawn(&panicked, false),
            RespawnDecision::PanicFirst
        );
    }

    #[test]
    fn decide_respawn_panic_then_clean_exit_classifies_clean_exit_with_already_respawning() {
        // Reverse: dispatcher panics first (PanicFirst, caller inserts
        // into respawning_flows). Then poll cleanly returns; even with
        // already_respawning=true, the decision is CleanExit.
        let panicked = dispatcher_panic("flow1", "boom");
        assert_eq!(
            decide_respawn(&panicked, false),
            RespawnDecision::PanicFirst
        );
        let clean = poll_clean("flow1");
        assert_eq!(decide_respawn(&clean, true), RespawnDecision::CleanExit);
    }

    #[test]
    fn decide_respawn_join_cancelled_returns_cancelled() {
        let cancelled: Result<FlowExit, tokio::task::JoinError> = {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("rt build");
            rt.block_on(async {
                let h = tokio::spawn(async { futures_util::future::pending::<FlowExit>().await });
                h.abort();
                h.await
            })
        };
        assert!(cancelled.as_ref().map(|_| ()).err().unwrap().is_cancelled());
        assert_eq!(
            decide_respawn(&cancelled, false),
            RespawnDecision::Cancelled
        );
    }

    #[test]
    fn decide_respawn_uncaught_panic_returns_unexpected_join_error() {
        // The production catch_unwind converts panics into
        // Ok(FlowExit{panic:Some}). The UnexpectedJoinError arm is
        // defensive for a hypothetical bug where a panic escapes.
        let panicked: Result<FlowExit, tokio::task::JoinError> = {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("rt build");
            rt.block_on(async {
                let h: tokio::task::JoinHandle<FlowExit> = tokio::spawn(async {
                    panic!("test panic that escapes catch_unwind");
                });
                h.await
            })
        };
        let err = panicked.as_ref().expect_err("must be Err");
        assert!(err.is_panic());
        assert!(!err.is_cancelled());
        assert_eq!(
            decide_respawn(&panicked, false),
            RespawnDecision::UnexpectedJoinError,
        );
    }
}

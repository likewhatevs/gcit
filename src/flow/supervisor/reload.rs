// SIGHUP / control-channel reload pipeline. Diffs the new config
// against the running set, cancels removed/changed flows, drains the
// JoinSet, emits FlowRemoved for state cleanup, then respawns.
//
// Key semantics:
//   - Unchanged flows skip cancel/respawn entirely so in-flight
//     monitor tracking stays alive across the reload.
//   - URL-changed flows emit FlowRemoved so the persisted last_sha
//     does not seed a spurious dispatch on the new source's first
//     poll cycle.
//   - Disabled flows cancel without FlowRemoved (operator may re-
//     enable later and expect last_sha + notified_runs to still be
//     there) UNLESS the URL also changed.
//   - Failed parse: log + record under the synthetic `(reload)` key
//     so `gcit status` surfaces the error, then re-emit Ready so
//     systemd does not stay stuck in Reloading state.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use sd_notify::NotifyState;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::config::{ActionConfig, Config, CredentialId, Destination, FlowConfig, SourceConfig};
use crate::state::StateUpdate;

use super::control::ControlHandler;
use super::flows::{spawn_flow, SpawnContext};
use super::types::{record_last_error, FlowExit, FlowRegistry, RELOAD_SYNTHETIC_KEY};

/// Per-flow action the supervisor must perform during a config reload.
/// Produced by `compute_reload_actions` from the (old config, new
/// config, currently-running handles) triple. The reload pipeline
/// consumes the resulting Vec to drive the side effects (cancel,
/// emit FlowRemoved, respawn).
///
/// Variants are ordered roughly by lifecycle phase so a reader of the
/// action vector can predict execution order without re-reading
/// `run_reload`'s body.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum ReloadAction {
    /// Flow's shape is identical to the previous config (per
    /// `flow_config_unchanged`) AND its handle is currently live.
    /// The supervisor leaves the existing poll/dispatcher pair alive
    /// — no cancel, no respawn, no FlowRemoved.
    Keep { name: String },
    /// Flow has been removed from the new config. The supervisor
    /// cancels the live handle (when one exists) AND emits
    /// `FlowRemoved` so the writer drops persisted state. The
    /// `live_handle` flag distinguishes the "cancel needed" path
    /// from the "no handle to cancel" sweep path the supervisor
    /// runs over old-but-not-new flow names.
    Remove { name: String, live_handle: bool },
    /// Flow exists in the new config but is disabled. The supervisor
    /// cancels the live handle. `url_changed` mirrors the
    /// disabled-URL-change reset rule: when true, emit FlowRemoved
    /// so a future re-enable does not seed a spurious dispatch on
    /// the new source's first poll cycle. When false, persisted
    /// state is preserved for re-enable.
    Disable { name: String, url_changed: bool },
    /// Flow exists, is enabled, and has a live handle but its
    /// poll/dispatch shape changed. The supervisor cancels the live
    /// handle and respawns. `url_changed` triggers a FlowRemoved so
    /// the new generation does not inherit the old source's
    /// last_sha. State for non-URL-changed restarts is preserved.
    Restart { name: String, url_changed: bool },
    /// Flow exists in the new config + is enabled but does NOT have
    /// a live handle. Two scenarios reach this:
    ///
    /// 1. genuinely new flow (not present in old config);
    /// 2. flow was running before but its handle is currently
    ///    missing (panic-mid-respawn or clean-exit-then-removed).
    ///
    /// Either way, the supervisor spawns a fresh pair.
    Spawn { name: String },
}

/// Pure config-diff: classify every flow that needs a reload action.
///
/// Inputs:
///   - `old_flows`: the previous config indexed by flow name.
///   - `new_flows`: the freshly-parsed config indexed by flow name.
///   - `live_handles`: the set of flow names whose `FlowHandle` is
///     currently in `FlowRegistry.handles` (i.e., the supervisor has
///     a cancel token + trigger sender for them right now).
///
/// Output: one action per flow. Every flow that appears in any of
/// the three input sets produces exactly one action; a flow that
/// requires no work (e.g., disabled in old AND disabled in new with
/// no URL change AND no live handle) is omitted.
///
/// Rules (matching `run_reload`'s historical inline classification):
///   - `name in new and enabled and unchanged from old and live` → `Keep`
///   - `name in new and disabled and live` → `Disable { url_changed }`
///   - `name in new and enabled and changed and live` → `Restart`
///   - `name not in new` → `Remove { live_handle }`
///   - `name in new and enabled and not live` → `Spawn`
///   - `name in new and disabled and not live` → no action emitted
///     (nothing to cancel; persisted state survives unchanged across
///     reloads)
pub(super) fn compute_reload_actions(
    old_flows: &BTreeMap<String, FlowConfig>,
    new_flows: &BTreeMap<String, FlowConfig>,
    live_handles: &BTreeSet<String>,
) -> Vec<ReloadAction> {
    let mut actions: Vec<ReloadAction> = Vec::new();

    // Iterate every name that appears in either config or in the
    // live handles set. BTreeSet union via chain + dedup keeps
    // output order deterministic for tests.
    let mut names: BTreeSet<&str> = BTreeSet::new();
    names.extend(old_flows.keys().map(String::as_str));
    names.extend(new_flows.keys().map(String::as_str));
    names.extend(live_handles.iter().map(String::as_str));

    for name in names {
        let old = old_flows.get(name);
        let new = new_flows.get(name);
        let live = live_handles.contains(name);
        match (new, old, live) {
            // Removed from new config — emit Remove regardless of
            // whether a handle is live (the consumer's cancel arm is
            // gated on `live_handle`).
            (None, Some(_), live_handle) => actions.push(ReloadAction::Remove {
                name: name.to_string(),
                live_handle,
            }),
            // Removed from new AND not in old AND live: orphan
            // handle (impossible per registry invariants but we
            // produce a Remove so the consumer cancels the orphan).
            (None, None, true) => actions.push(ReloadAction::Remove {
                name: name.to_string(),
                live_handle: true,
            }),
            // In new but disabled: cancel and reset state if the URL
            // also changed. With no live handle and no URL change
            // there's nothing to do — fall through to no-action.
            (Some(nf), Some(of), true) if !nf.enabled => {
                actions.push(ReloadAction::Disable {
                    name: name.to_string(),
                    url_changed: of.source.url != nf.source.url,
                });
            }
            (Some(nf), None, true) if !nf.enabled => {
                // Disabled flow with a live handle but no old config
                // — should be unreachable; treat as Disable with no
                // URL change so the consumer at least cancels.
                actions.push(ReloadAction::Disable {
                    name: name.to_string(),
                    url_changed: false,
                });
            }
            (Some(nf), _, false) if !nf.enabled => {
                // Disabled, no live handle, no work. Persisted state
                // is preserved across the reload for a future
                // re-enable.
                continue;
            }
            // In new + enabled + live + identical shape: keep.
            (Some(nf), Some(of), true) if flow_config_unchanged(of, nf) => {
                actions.push(ReloadAction::Keep {
                    name: name.to_string(),
                });
            }
            // In new + enabled + live + changed: cancel + respawn.
            (Some(nf), Some(of), true) => {
                actions.push(ReloadAction::Restart {
                    name: name.to_string(),
                    url_changed: of.source.url != nf.source.url,
                });
            }
            // In new + enabled + live but no old entry: handle
            // existed without a matching old config (unreachable per
            // registry invariants). Treat as Restart with url_changed
            // false (cancel + respawn).
            (Some(_), None, true) => {
                actions.push(ReloadAction::Restart {
                    name: name.to_string(),
                    url_changed: false,
                });
            }
            // In new + enabled + no live handle: spawn. Covers both
            // genuinely-new flows and flows whose handle is missing
            // due to panic-mid-respawn or clean-exit-then-respawning.
            (Some(_), _, false) => {
                actions.push(ReloadAction::Spawn {
                    name: name.to_string(),
                });
            }
            // No new + no old + no live: impossible (we built `names`
            // from the union).
            (None, None, false) => unreachable!(
                "compute_reload_actions: name in iteration set but absent from all three inputs",
            ),
        }
    }

    actions
}

/// Reload entry point shared by SIGHUP and the control-channel
/// `Reload` request — both paths call this function.
///
/// Per-flow diff semantics: each existing flow handle is classified
/// as removed, disabled, source-URL changed, otherwise-changed, or
/// unchanged. Removed and URL-changed flows additionally emit
/// `FlowRemoved` so persisted state for the gone-or-stale source
/// does not seed a spurious dispatch on the next poll cycle.
/// Unchanged flows skip cancel/respawn entirely so their existing
/// poll/dispatcher pair (and any in-flight monitor tracking) stays
/// alive across the reload.
///
/// Cached credentials are invalidated up front so a rotated PAT
/// (operator wrote a new value into the credential file before
/// SIGHUP) is picked up by the new generation without a daemon
/// restart.
///
/// Returns nothing because the function manages its own sd_notify
/// state transitions (Reloading -> Ready on success, Reloading ->
/// Ready on failure with a WARN log — systemd must never observe
/// the daemon stuck in Reloading state).
pub(super) async fn run_reload(
    config_path: &std::path::Path,
    config_watch: Arc<watch::Sender<Arc<Config>>>,
    ctx: &SpawnContext,
    join_set: &mut JoinSet<FlowExit>,
    registry: &mut FlowRegistry,
    control_handler: &Arc<ControlHandler>,
) {
    // Notify systemd we're reloading (NotifyState::Reloading +
    // MonotonicUsec per sd-notify v253+).
    let mut reloading_states: Vec<NotifyState> = vec![NotifyState::Reloading];
    if let Ok(m) = NotifyState::monotonic_usec_now() {
        reloading_states.push(m);
    }
    let _ = sd_notify::notify(&reloading_states);

    let new_cfg = match crate::config::load(config_path) {
        Ok(c) => Arc::new(c),
        Err(errs) => {
            // Re-emit Ready on failure so systemd does not stay stuck
            // in Reloading state. Format each ConfigError via Display.
            let formatted: Vec<String> = errs.iter().map(|e| format!("{}", e)).collect();
            warn!(
                target: "gcit::supervisor",
                errors = ?formatted,
                "reload failed to parse config; staying on previous config",
            );
            // Record the reload failure under a synthetic flow key
            // "(reload)" so `gcit status` surfaces the parse errors
            // (otherwise an operator running SIGHUP after a typo gets
            // no feedback that the reload silently no-op'd).
            record_last_error(
                &ctx.last_errors,
                RELOAD_SYNTHETIC_KEY,
                "config_reload",
                &formatted.join("; "),
                None,
            )
            .await;
            if let Err(e) = sd_notify::notify(&[NotifyState::Ready]) {
                warn!(
                    target: "gcit::supervisor",
                    error = %e,
                    "sd_notify Ready (post-failed-reload) failed",
                );
            }
            return;
        }
    };
    // Capture the previous config before the watch replacement so the
    // diff loop below can compare each flow's old vs new shape (config
    // hash for cancel-skip, source URL for state reset).
    let old_cfg = config_watch.borrow().clone();
    info!(
        target: "gcit::supervisor",
        flows = new_cfg.flow.len(),
        "config reloaded; diffing per-flow",
    );

    // Build the indexed config maps + the live-handles snapshot for
    // the pure classifier. The classifier returns a Vec of per-flow
    // actions; the side-effect loop below threads each action through
    // the live FlowRegistry / state_tx / cancel paths.
    //
    // Rules covered (full list documented on `ReloadAction`):
    //   - REMOVED → Remove { live_handle }: cancel handle (when
    //     present) + emit FlowRemoved so state.json drops the entry.
    //   - DISABLED → Disable { url_changed }: cancel without
    //     emitting FlowRemoved — operator may re-enable later and
    //     expect last_sha + notified_runs to still be there. Reset
    //     state when the URL also changed (re-enable on a different
    //     source).
    //   - URL_CHANGED + ENABLED → Restart { url_changed: true }:
    //     cancel + respawn + emit FlowRemoved so the persisted
    //     last_sha for the OLD source does not seed a spurious
    //     dispatch on the NEW source's first poll cycle.
    //   - CHANGED + ENABLED → Restart { url_changed: false }: cancel
    //     + respawn; state is preserved so the new generation picks
    //     up where the old left off.
    //   - UNCHANGED → Keep: skip cancel/respawn. Existing pair stays
    //     alive and continues to monitor in-flight runs.
    //   - NEW or REVIVED → Spawn: bring up a fresh pair (new flow,
    //     or an existing flow whose handle is missing because it
    //     panicked + is mid-respawn).
    let old_by_name: BTreeMap<String, FlowConfig> = old_cfg
        .flow
        .iter()
        .map(|f| (f.name.clone(), f.clone()))
        .collect();
    let new_by_name: BTreeMap<String, FlowConfig> = new_cfg
        .flow
        .iter()
        .map(|f| (f.name.clone(), f.clone()))
        .collect();
    let live_handles: BTreeSet<String> = registry.handles.keys().cloned().collect();
    let actions = compute_reload_actions(&old_by_name, &new_by_name, &live_handles);

    let mut to_keep: BTreeSet<String> = BTreeSet::new();
    let mut url_resets: Vec<String> = Vec::new();
    let mut removed_flows: Vec<String> = Vec::new();
    let mut drained = std::mem::take(&mut registry.handles);
    for action in &actions {
        match action {
            ReloadAction::Keep { name } => {
                to_keep.insert(name.clone());
                if let Some(handle) = drained.remove(name) {
                    registry.handles.insert(name.clone(), handle);
                }
            }
            ReloadAction::Remove {
                name,
                live_handle: true,
            } => {
                if let Some(handle) = drained.remove(name) {
                    handle.cancel.cancel();
                }
                removed_flows.push(name.clone());
                registry.respawning_flows.remove(name);
            }
            ReloadAction::Remove {
                name,
                live_handle: false,
            } => {
                // No live handle to cancel (panic-mid-respawn or
                // clean-exit-already-dropped scenario). Still emit
                // FlowRemoved so persisted state drops.
                removed_flows.push(name.clone());
                registry.respawning_flows.remove(name);
            }
            ReloadAction::Disable { name, url_changed } => {
                if let Some(handle) = drained.remove(name) {
                    handle.cancel.cancel();
                }
                if *url_changed {
                    url_resets.push(name.clone());
                }
                // Release any pending respawn slot so a future
                // panic-respawn arriving after a re-enable is not
                // deduped against the disabled-period slot.
                registry.respawning_flows.remove(name);
            }
            ReloadAction::Restart { name, url_changed } => {
                if let Some(handle) = drained.remove(name) {
                    handle.cancel.cancel();
                }
                if *url_changed {
                    url_resets.push(name.clone());
                }
                registry.respawning_flows.remove(name);
            }
            ReloadAction::Spawn { .. } => {
                // The spawn is performed by the new-config respawn
                // loop further down; classifier just records that we
                // need it. The respawn loop iterates new_cfg.flow
                // and skips entries already in `to_keep`.
            }
        }
    }
    // Drained handles whose name didn't surface in `actions` are
    // orphans (registry invariant violation) — drop them so the
    // cancel token releases. In practice this branch never fires.
    for (_name, handle) in drained {
        handle.cancel.cancel();
    }

    // Build the set of credential ids referenced by kept-alive flows
    // (action.credential_id + every Discord destination's credential_id).
    // Pass it to `invalidate_except` so cached credentials referenced by
    // those flows survive the reload — their rate-limit pollers stay
    // running, their cached secrets stay resolved, and a new flow that
    // happens to reuse the same credential lands on the same shared
    // pool entry (so both flows share a single Arc<RateLimitState> and
    // observe each other's API calls). Credentials no longer referenced
    // by any kept-alive flow are dropped: the rate-limit poller is
    // cancelled and the entry removed so the next `acquire_github` call
    // re-reads the credential file (picking up a rotated PAT).
    let keep_credentials = collect_kept_credentials(&new_cfg, &to_keep);
    ctx.credential_pool
        .write()
        .await
        .invalidate_except(&keep_credentials);

    // Drain the JoinSet of exits that came from the cancellations
    // above. Per the cancel-and-respawn contract: every cancelled
    // flow must return an exit before we spawn the new set, otherwise
    // two generations of the same flow would briefly race on the
    // state writer mpsc.
    //
    // The drain waits for the NUMBER of exits the cancellations should
    // produce. Kept-alive flows live in the same JoinSet but produce
    // no exit while running, so a count-based bound stops once the
    // cancelled tasks have unwound rather than blocking on the live
    // ones. Each flow contributes 2 tasks (poll + dispatcher) so the
    // expected post-drain JoinSet length is `kept * 2`.
    //
    // 30s timeout caps the wait when a cancelled task is wedged
    // (e.g. blocked in a syscall that ignores its CancellationToken).
    // After the timeout the supervisor stops waiting and proceeds with
    // the respawn — the leftover wedged task stays in the JoinSet and
    // is reaped lazily when the supervisor's main `select!` arm
    // observes its eventual exit via `handle_flow_exit`.
    //
    // Wedged old-gen state-writer race: the wedged task still holds
    // its `state_tx.clone()` and can enqueue StateUpdates after the
    // new-gen has spawned with its own clone. Both clones land in the
    // same writer mpsc; the writer applies them in FIFO order. Two
    // mitigations:
    //   - `handle_respawn_request` (panic-respawn path) gates the new
    //     spawn on an explicit `pending_exits[flow] == None` check,
    //     deferring via re-enqueue on a 1s timer (bounded by
    //     RESPAWN_MAX_ATTEMPTS) until the old-gen pair has fully
    //     unwound.
    //   - `run_reload` (config-reload path) uses the count-based
    //     drain above with a 30s timeout cap.
    //
    // Both paths fall back to "proceed anyway with overlap" on
    // budget exhaustion. The remaining race manifests only when a
    // task is cancelled but ignores its CancellationToken AND the
    // drain budget runs out. Per-variant convergence (apply.rs)
    // bounds the worst case in that exit path:
    //   - PollObservation: LWW on (last_sha, last_poll_at). A late
    //     old-gen value gets overwritten by the new-gen's next
    //     observation. Worst case: one stale-but-valid SHA appears
    //     transiently in state.json.
    //   - PollTimestamp: LWW on last_poll_at only. Same convergence.
    //   - RunStarted: appends to active_runs with run_id dedup. A
    //     wedged old-gen dispatcher whose correlator finally returns
    //     after the drain timeout will enqueue RunStarted for a
    //     run_id the new-gen never saw, producing a phantom
    //     active_runs entry. The old-gen's monitor task was
    //     CANCELLED at drain time (it shares the per-flow cancel
    //     token), so its RunFinished will NEVER arrive — the phantom
    //     run persists until either (a) the flow is removed from
    //     config in a future reload (FlowRemoved drops the entry),
    //     or (b) the daemon is restarted (state.json is rewritten
    //     from the empty active_runs of the fresh process). For a
    //     flow that stays in config indefinitely, the phantom never
    //     clears on its own. This is a real leak; the trade we
    //     accept is that it requires the rare drain timeout AND a
    //     wedged correlator that returns after cancel was already
    //     fired, and the leak is bounded by daemon-restart frequency
    //     and operator-initiated config changes that remove the
    //     flow. Operators see the phantom via `gcit status`
    //     active_runs alongside the active runs the new-gen does
    //     monitor.
    //   - RunFinished: removes by run_id; old-gen and new-gen own
    //     disjoint run_ids so cross-talk is impossible.
    //   - FlowRemoved: drops the entire entry. A subsequent old-gen
    //     RunStarted/PollObservation re-creates the entry via
    //     or_default. Mitigation: emit FlowRemoved AFTER the drain so
    //     this scenario only fires on drain timeout (see comment
    //     below).
    let kept_task_count = to_keep.len() * 2;
    let drain_completed = {
        let drain = async {
            while join_set.len() > kept_task_count {
                match join_set.join_next().await {
                    Some(Ok(_exit)) => {
                        // Cancelled or natural exit; nothing to do.
                    }
                    Some(Err(e)) if e.is_cancelled() => {}
                    Some(Err(e)) => {
                        warn!(
                            target: "gcit::supervisor",
                            error = %e,
                            "flow task join error during reload drain",
                        );
                    }
                    None => break,
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(30), drain)
            .await
            .is_ok()
    };
    if !drain_completed {
        warn!(
            target: "gcit::supervisor",
            remaining = join_set.len(),
            "reload drain timed out after 30s; proceeding with respawn (leftover tasks will be observed via the main select! loop when they complete)",
        );
    }

    // Emit FlowRemoved AFTER the drain completes. Sequencing matters
    // for state correctness: a cancelled poll task can have one or
    // more in-flight `state_tx.send(PollObservation { ... })` calls
    // that finished AFTER the cancel token fired but BEFORE the task
    // observed cancellation. Those PollObservations are already in
    // the writer's mpsc buffer when the task exits. tokio mpsc is
    // FIFO across all `send().await` completions on the shared
    // channel, so any FlowRemoved we send here lands AFTER those
    // pending observations and the writer applies them in the
    // intended order: observations first, then removal.
    //
    // If the drain timed out, the cancelled tasks may still be
    // running and could still enqueue PollObservations after our
    // FlowRemoved. The trade we accept: timeouts are rare (the
    // supervisor is wedged in some other way) and the writer's LWW
    // semantics auto-correct on the next reload.
    // respawning_flows entries for removed names were drained by the
    // sweep above; no further cleanup is required here.
    for name in &removed_flows {
        let _ = ctx
            .state_tx
            .send(StateUpdate::FlowRemoved { flow: name.clone() })
            .await;
    }
    for name in &url_resets {
        // Drop the persisted state so the freshly-spawned flow's poll
        // loop seeds its in-memory baseline from `None` and treats the
        // first observation against the NEW source as the initial
        // baseline (no spurious dispatch).
        let _ = ctx
            .state_tx
            .send(StateUpdate::FlowRemoved { flow: name.clone() })
            .await;
    }

    // Push the new config into the watch BEFORE respawning so the
    // panic-respawn path (which reads from the watch) sees the new
    // shape if any flow panics during its first iteration.
    config_watch.send_replace(Arc::clone(&new_cfg));

    // Respawn every enabled flow that is NOT in the keep-alive set.
    // Release the flow's panic-respawn slot before spawning so a
    // future panic on the freshly-spawned generation is not deduped
    // against the old slot. (A late RespawnRequest from a sleeping
    // panic-watcher is independently filtered by
    // `handle_respawn_request`'s `handles.contains_key` check, so
    // clearing here only governs the new-generation lifecycle, not
    // the old.)
    for flow in &new_cfg.flow {
        if !flow.enabled {
            info!(
                target: "gcit::supervisor",
                flow = %flow.name,
                "flow disabled in new config; not spawning",
            );
            continue;
        }
        if to_keep.contains(&flow.name) {
            info!(
                target: "gcit::supervisor",
                flow = %flow.name,
                "flow config unchanged across reload; keeping in-flight monitor tracking",
            );
            continue;
        }
        registry.respawning_flows.remove(&flow.name);
        spawn_flow(flow, new_cfg.as_ref(), ctx, join_set, registry).await;
    }

    // Clear any stale "(reload)" entry from a prior failed parse
    // BEFORE refreshing flow_names. A status reader interleaving
    // between these two writes would otherwise observe fresh
    // flow_names alongside the stale "(reload)" entry — internally
    // inconsistent ("the reload succeeded with these flow names"
    // alongside "the reload failed with this parse error").
    // Clearing first inverts the worst-case observation to stale
    // flow_names + cleared "(reload)", which is consistent with
    // "successful reload, not yet visible to this reader".
    ctx.last_errors.lock().await.remove(RELOAD_SYNTHETIC_KEY);

    // Refresh the control handler's flow-name list so `gcit status`
    // returns the new set.
    *control_handler.flow_names.write().await = registry.handles.keys().cloned().collect();

    if let Err(e) = sd_notify::notify(&[NotifyState::Ready]) {
        warn!(target: "gcit::supervisor", error = %e, "sd_notify Ready (post-reload) failed");
    }
    info!(target: "gcit::supervisor", "reload complete");
}

/// Returns true when two `FlowConfig`s describe identical
/// poll/dispatch/notify shapes, so `run_reload` can keep the existing
/// poll + dispatcher pair alive (and its in-flight monitor tracking)
/// instead of cancelling and respawning. Compares every field except
/// `name` (used as the map key) and `description` (purely operator-
/// facing — a description tweak should not interrupt monitors).
///
/// Source/action/poll fields are compared structurally via
/// `serde_json::to_value` whose transitive `PartialEq` traverses the
/// tree. The Serialize impl on each type is canonical (BTreeMap for
/// inputs, declared field order), so two semantically-identical
/// configs produce identical Values.
///
/// Destinations are compared as a multiset rather than a Vec because
/// the supervisor's per-destination wiring is order-insensitive: a
/// reorder of `[[flow.destination]]` entries with no field change
/// produces the same fan-out behaviour and should not trigger a
/// cancel-and-respawn. Each destination is serialized to its own
/// `serde_json::Value`, those values are sorted by their JSON
/// rendering, and the sorted vectors are compared. Destinations whose
/// content differs still surface a difference (multiset equality
/// requires identical element counts). Note: notifier IDs
/// (`<flow>.dest<idx>`) are positional at spawn time. A pure
/// reorder is treated as unchanged, so existing notifiers keep
/// their original IDs; new IDs only appear after a respawn for
/// an unrelated reason.
///
/// On serialization failure (should never happen — every nested type
/// derives Serialize), conservatively return `false` so the flow
/// is cancel-and-respawned.
fn flow_config_unchanged(old: &FlowConfig, new: &FlowConfig) -> bool {
    if old.enabled != new.enabled {
        return false;
    }
    let Ok(old_v) = serde_json::to_value(FlowDiff::from(old)) else {
        return false;
    };
    let Ok(new_v) = serde_json::to_value(FlowDiff::from(new)) else {
        return false;
    };
    if old_v != new_v {
        return false;
    }
    let Some(old_dests) = canonical_destination_multiset(&old.destination) else {
        return false;
    };
    let Some(new_dests) = canonical_destination_multiset(&new.destination) else {
        return false;
    };
    old_dests == new_dests
}

/// Canonical multiset representation of a destination list. Each
/// destination is serialized to a JSON value, the inner `fire_on`
/// array is sorted (notifier dispatch checks `fire_on.contains(...)`,
/// so element order is semantically irrelevant), the value is
/// stringified, and the resulting vector is sorted. Two lists with
/// the same elements in different orders compare equal; two lists
/// with element-content differences (or different counts of the same
/// element — multiset semantics) still surface as different.
/// Returns `None` on any serialization failure so the caller can
/// fall back to "changed" (the safe default).
fn canonical_destination_multiset(destinations: &[Destination]) -> Option<Vec<String>> {
    let mut canonical: Vec<String> = Vec::with_capacity(destinations.len());
    for d in destinations {
        let mut v = serde_json::to_value(d).ok()?;
        canonicalize_fire_on(&mut v);
        canonical.push(serde_json::to_string(&v).ok()?);
    }
    canonical.sort();
    Some(canonical)
}

/// Collect the set of CredentialIds referenced by every kept-alive
/// flow's action AND every Discord destination on a kept-alive flow.
/// Used by `run_reload` to feed `CredentialPool::invalidate_except` so
/// the rate-limit poller, cached secret, and `Arc<GithubClient>` for
/// each credential survive the reload — kept-alive flows continue to
/// use the resources they already hold via `Arc` clones.
///
/// LocalMail destinations have no credential to track (they write to a
/// local mbox spool); only Discord destinations carry a credential_id
/// in the destination tree.
fn collect_kept_credentials(
    new_cfg: &Config,
    to_keep: &BTreeSet<String>,
) -> BTreeSet<CredentialId> {
    let mut keep: BTreeSet<CredentialId> = BTreeSet::new();
    for flow in &new_cfg.flow {
        if !to_keep.contains(&flow.name) {
            continue;
        }
        match &flow.action {
            ActionConfig::GithubWorkflowDispatch { credential_id, .. } => {
                keep.insert(credential_id.clone());
            }
        }
        for dest in &flow.destination {
            if let Destination::DiscordWebhook(w) = dest {
                keep.insert(w.credential_id.clone());
            }
        }
    }
    keep
}

/// Sort the `fire_on` array (if any) inside a destination's JSON
/// representation. Notifier dispatch tests `fire_on.contains(event)`,
/// so two lists with the same set of events fire identically
/// regardless of element order. Sorting by the rendered string of
/// each element keeps the canonical output deterministic without
/// pulling in a stronger Ord constraint on FireEvent.
fn canonicalize_fire_on(value: &mut serde_json::Value) {
    if let serde_json::Value::Object(map) = value {
        if let Some(serde_json::Value::Array(arr)) = map.get_mut("fire_on") {
            arr.sort_by_key(|a| a.to_string());
        }
    }
}

/// Helper struct for `flow_config_unchanged` that excludes the
/// `destination` field. Destinations are compared separately as a
/// multiset; including them here would force order-sensitive equality
/// and defeat the multiset comparison.
#[derive(serde::Serialize)]
struct FlowDiff<'a> {
    source: &'a SourceConfig,
    action: &'a ActionConfig,
    poll: &'a crate::config::PollOverride,
}

impl<'a> From<&'a FlowConfig> for FlowDiff<'a> {
    fn from(f: &'a FlowConfig) -> Self {
        Self {
            source: &f.source,
            action: &f.action,
            poll: &f.poll,
        }
    }
}

// `BTreeMap` re-exported from std for the `new_by_name` / `old_by_name`
// lookups above. The body uses fully-qualified paths so the local
// `use` is omitted; this trait `use` was a leftover from the parent
// module and not needed here.
use std::collections::BTreeMap;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        DiscordTemplateConfig, DiscordWebhookConfig, FireEvent, LocalMailConfig,
        LocalMailTemplateConfig, PollOverride, SourceConfig,
    };

    fn cred(id: &str) -> CredentialId {
        CredentialId::new(id).expect("valid credential id")
    }

    fn discord_dest(id: &str, fire_on: Vec<FireEvent>) -> Destination {
        Destination::DiscordWebhook(DiscordWebhookConfig {
            credential_id: cred(id),
            fire_on,
            template: DiscordTemplateConfig::default(),
        })
    }

    fn mail_dest(user: &str, fire_on: Vec<FireEvent>) -> Destination {
        Destination::LocalMail(LocalMailConfig {
            user: user.to_string(),
            fire_on,
            template: LocalMailTemplateConfig::default(),
        })
    }

    fn flow(name: &str, url: &str, enabled: bool, destinations: Vec<Destination>) -> FlowConfig {
        FlowConfig {
            name: name.to_string(),
            enabled,
            description: None,
            source: SourceConfig {
                url: url.to_string(),
                ref_name: "refs/heads/main".to_string(),
                credential_id: None,
            },
            action: ActionConfig::GithubWorkflowDispatch {
                repo: "owner/repo".to_string(),
                workflow: "ci.yml".to_string(),
                ref_name: "refs/heads/main".to_string(),
                credential_id: cred("gh"),
                inputs: std::collections::BTreeMap::new(),
            },
            destination: destinations,
            poll: PollOverride::default(),
        }
    }

    #[test]
    fn flow_config_unchanged_treats_destination_reorder_as_unchanged() {
        let d_a = discord_dest("hookA", vec![FireEvent::RunComplete]);
        let d_b = mail_dest("alice", vec![FireEvent::RunComplete]);
        let old = flow("f", "u", true, vec![d_a.clone(), d_b.clone()]);
        let new = flow("f", "u", true, vec![d_b, d_a]);
        assert!(flow_config_unchanged(&old, &new));
    }

    #[test]
    fn flow_config_unchanged_treats_fire_on_reorder_as_unchanged() {
        let old = flow(
            "f",
            "u",
            true,
            vec![discord_dest(
                "hookA",
                vec![FireEvent::RunStart, FireEvent::RunComplete],
            )],
        );
        let new = flow(
            "f",
            "u",
            true,
            vec![discord_dest(
                "hookA",
                vec![FireEvent::RunComplete, FireEvent::RunStart],
            )],
        );
        assert!(flow_config_unchanged(&old, &new));
    }

    #[test]
    fn flow_config_unchanged_detects_destination_content_change() {
        let old = flow(
            "f",
            "u",
            true,
            vec![discord_dest("hookA", vec![FireEvent::RunComplete])],
        );
        let new = flow(
            "f",
            "u",
            true,
            vec![discord_dest("hookB", vec![FireEvent::RunComplete])],
        );
        assert!(!flow_config_unchanged(&old, &new));
    }

    #[test]
    fn flow_config_unchanged_distinguishes_duplicate_count() {
        let dest = discord_dest("hookA", vec![FireEvent::RunComplete]);
        let old = flow("f", "u", true, vec![dest.clone(), dest.clone()]);
        let new = flow("f", "u", true, vec![dest]);
        assert!(!flow_config_unchanged(&old, &new));
    }

    #[test]
    fn flow_config_unchanged_detects_url_change() {
        let old = flow("f", "https://example.com/old.git", true, Vec::new());
        let new = flow("f", "https://example.com/new.git", true, Vec::new());
        assert!(!flow_config_unchanged(&old, &new));
    }

    #[test]
    fn flow_config_unchanged_detects_enabled_change_short_circuits() {
        // `enabled` is checked first inside `flow_config_unchanged`
        // so a flip from enabled=true to enabled=false (or vice versa)
        // returns false without descending into the FlowDiff
        // comparison. The classifier downstream relies on this so an
        // enable/disable toggle drives the Disable / Spawn arm rather
        // than Keep.
        let old = flow("f", "https://example.com/repo.git", true, Vec::new());
        let new = flow("f", "https://example.com/repo.git", false, Vec::new());
        assert!(!flow_config_unchanged(&old, &new));
        // Reverse direction surfaces the same answer.
        assert!(!flow_config_unchanged(&new, &old));
    }

    #[test]
    fn flow_config_unchanged_ignores_description_change() {
        // The doc comment on flow_config_unchanged states description
        // is intentionally omitted so a "tweak the operator-facing
        // description" reload does not interrupt in-flight monitor
        // tracking. Pin that contract — a regression that included
        // description in FlowDiff would surface as a false "changed"
        // verdict here.
        let mut old = flow("f", "https://example.com/repo.git", true, Vec::new());
        let mut new = flow("f", "https://example.com/repo.git", true, Vec::new());
        old.description = Some("CI pipeline".to_string());
        new.description = Some("CI pipeline (rev 2)".to_string());
        assert!(flow_config_unchanged(&old, &new));
    }

    #[test]
    fn flow_config_unchanged_detects_action_repo_change() {
        // FlowDiff carries the full ActionConfig so a repo rename
        // (same URL, same workflow) still triggers a respawn. Without
        // this the dispatcher would keep firing into the old repo's
        // workflow_dispatch endpoint — a real correctness bug, not
        // just an aesthetic.
        let mut old = flow("f", "https://example.com/repo.git", true, Vec::new());
        let mut new = flow("f", "https://example.com/repo.git", true, Vec::new());
        let ActionConfig::GithubWorkflowDispatch { repo, .. } = &mut old.action;
        *repo = "owner/old-repo".to_string();
        let ActionConfig::GithubWorkflowDispatch { repo, .. } = &mut new.action;
        *repo = "owner/new-repo".to_string();
        assert!(!flow_config_unchanged(&old, &new));
    }

    #[test]
    fn flow_config_unchanged_detects_poll_override_change() {
        // PollOverride drives the per-flow effective cadence. A change
        // in source_interval or jitter must invalidate the live
        // poll/dispatcher pair so the new cadence takes effect — pin
        // that the diff path catches it.
        let mut old = flow("f", "https://example.com/repo.git", true, Vec::new());
        let mut new = flow("f", "https://example.com/repo.git", true, Vec::new());
        old.poll.source_interval = Some(Duration::from_secs(60));
        new.poll.source_interval = Some(Duration::from_secs(120));
        assert!(!flow_config_unchanged(&old, &new));
    }

    #[test]
    fn canonical_destination_multiset_sorts_outer_and_fire_on() {
        let d_a = discord_dest("a", vec![FireEvent::RunStart, FireEvent::RunComplete]);
        let d_b = mail_dest("u", vec![FireEvent::JobComplete]);
        let canon_one = canonical_destination_multiset(&[d_a.clone(), d_b.clone()]).unwrap();
        let canon_two = canonical_destination_multiset(&[
            mail_dest("u", vec![FireEvent::JobComplete]),
            discord_dest("a", vec![FireEvent::RunComplete, FireEvent::RunStart]),
        ])
        .unwrap();
        assert_eq!(canon_one, canon_two);
    }

    fn flow_with_action_cred(
        name: &str,
        action_cred: &str,
        destinations: Vec<Destination>,
    ) -> FlowConfig {
        let mut f = flow(name, "https://example.com/repo.git", true, destinations);
        let ActionConfig::GithubWorkflowDispatch { credential_id, .. } = &mut f.action;
        *credential_id = cred(action_cred);
        f
    }

    fn cfg_with_flows(flows: Vec<FlowConfig>) -> Config {
        Config {
            source_path: std::path::PathBuf::new(),
            poll: crate::config::PollDefaults::default(),
            log: crate::config::LogConfig::default(),
            http: crate::config::HttpConfig::default(),
            flow: flows,
            credential_lines: std::collections::BTreeMap::new(),
        }
    }

    fn keep_set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn collect_kept_credentials_includes_action_credential() {
        let f = flow_with_action_cred("kept", "ghpat", Vec::new());
        let cfg = cfg_with_flows(vec![f]);
        let keep = collect_kept_credentials(&cfg, &keep_set(&["kept"]));
        assert!(keep.contains(&cred("ghpat")));
        assert_eq!(keep.len(), 1);
    }

    #[test]
    fn collect_kept_credentials_includes_discord_destination_credential() {
        let f = flow_with_action_cred(
            "kept",
            "ghpat",
            vec![discord_dest("hookA", vec![FireEvent::RunComplete])],
        );
        let cfg = cfg_with_flows(vec![f]);
        let keep = collect_kept_credentials(&cfg, &keep_set(&["kept"]));
        assert!(keep.contains(&cred("ghpat")));
        assert!(keep.contains(&cred("hookA")));
        assert_eq!(keep.len(), 2);
    }

    #[test]
    fn collect_kept_credentials_excludes_local_mail_destinations() {
        let f = flow_with_action_cred(
            "kept",
            "ghpat",
            vec![mail_dest("alice", vec![FireEvent::RunComplete])],
        );
        let cfg = cfg_with_flows(vec![f]);
        let keep = collect_kept_credentials(&cfg, &keep_set(&["kept"]));
        assert!(keep.contains(&cred("ghpat")));
        assert_eq!(keep.len(), 1);
    }

    #[test]
    fn collect_kept_credentials_skips_non_kept_flows() {
        let kept = flow_with_action_cred("kept", "ghpat", Vec::new());
        let respawning = flow_with_action_cred("respawning", "other", Vec::new());
        let cfg = cfg_with_flows(vec![kept, respawning]);
        let keep = collect_kept_credentials(&cfg, &keep_set(&["kept"]));
        assert!(keep.contains(&cred("ghpat")));
        assert!(!keep.contains(&cred("other")));
        assert_eq!(keep.len(), 1);
    }

    #[test]
    fn collect_kept_credentials_dedups_shared_credentials() {
        let a = flow_with_action_cred("a", "ghpat", Vec::new());
        let b = flow_with_action_cred(
            "b",
            "ghpat",
            vec![discord_dest("ghpat", vec![FireEvent::RunComplete])],
        );
        let cfg = cfg_with_flows(vec![a, b]);
        let keep = collect_kept_credentials(&cfg, &keep_set(&["a", "b"]));
        assert_eq!(keep.len(), 1);
        assert!(keep.contains(&cred("ghpat")));
    }

    // ---------------------------------------------------------------
    // compute_reload_actions table-driven tests. The pure classifier
    // is the heart of the reload diff; covering its arms in isolation
    // means the side-effect loop in `run_reload` only has to wire the
    // returned actions through to the registry / state_tx / cancel
    // paths — not also re-derive the classification.
    // ---------------------------------------------------------------

    /// Helper: build the (old, new, live_handles) triple for the
    /// classifier and return the resulting Vec<ReloadAction>.
    fn run(old: Vec<FlowConfig>, new: Vec<FlowConfig>, live: &[&str]) -> Vec<ReloadAction> {
        let old_map: BTreeMap<String, FlowConfig> =
            old.into_iter().map(|f| (f.name.clone(), f)).collect();
        let new_map: BTreeMap<String, FlowConfig> =
            new.into_iter().map(|f| (f.name.clone(), f)).collect();
        let live_set: BTreeSet<String> = live.iter().map(|s| s.to_string()).collect();
        compute_reload_actions(&old_map, &new_map, &live_set)
    }

    #[test]
    fn compute_reload_actions_keep_for_unchanged_enabled_live_flow() {
        let f = flow("ci", "https://example.com/repo.git", true, Vec::new());
        let actions = run(vec![f.clone()], vec![f], &["ci"]);
        assert_eq!(
            actions,
            vec![ReloadAction::Keep {
                name: "ci".to_string()
            }],
        );
    }

    #[test]
    fn compute_reload_actions_remove_when_flow_drops_out_of_new_config() {
        let old_f = flow("ci", "https://example.com/repo.git", true, Vec::new());
        let actions = run(vec![old_f], vec![], &["ci"]);
        assert_eq!(
            actions,
            vec![ReloadAction::Remove {
                name: "ci".to_string(),
                live_handle: true,
            }],
        );
    }

    #[test]
    fn compute_reload_actions_remove_without_live_handle_when_flow_drops_out() {
        // The flow was in the old config but its handle is not live
        // (panic-mid-respawn or clean-exit-already-dropped). The
        // classifier still emits Remove so the writer drops persisted
        // state; the live_handle=false flag tells the consumer not
        // to attempt a cancel.
        let old_f = flow("ci", "https://example.com/repo.git", true, Vec::new());
        let actions = run(vec![old_f], vec![], &[]);
        assert_eq!(
            actions,
            vec![ReloadAction::Remove {
                name: "ci".to_string(),
                live_handle: false,
            }],
        );
    }

    #[test]
    fn compute_reload_actions_disable_when_new_config_disables_flow() {
        let old_f = flow("ci", "https://example.com/repo.git", true, Vec::new());
        let new_f = flow("ci", "https://example.com/repo.git", false, Vec::new());
        let actions = run(vec![old_f], vec![new_f], &["ci"]);
        assert_eq!(
            actions,
            vec![ReloadAction::Disable {
                name: "ci".to_string(),
                url_changed: false,
            }],
        );
    }

    #[test]
    fn compute_reload_actions_disable_with_url_changed_when_disable_and_url_diff() {
        let old_f = flow("ci", "https://example.com/old.git", true, Vec::new());
        let new_f = flow("ci", "https://example.com/new.git", false, Vec::new());
        let actions = run(vec![old_f], vec![new_f], &["ci"]);
        assert_eq!(
            actions,
            vec![ReloadAction::Disable {
                name: "ci".to_string(),
                url_changed: true,
            }],
        );
    }

    #[test]
    fn compute_reload_actions_no_action_for_disabled_flow_with_no_live_handle() {
        // Flow was disabled in old AND in new, no live handle. No
        // cancel needed, no FlowRemoved (preserve persisted state for
        // future re-enable). The classifier produces NO action.
        let old_f = flow("ci", "https://example.com/repo.git", false, Vec::new());
        let new_f = flow("ci", "https://example.com/repo.git", false, Vec::new());
        let actions = run(vec![old_f], vec![new_f], &[]);
        assert!(
            actions.is_empty(),
            "disabled-no-handle flow needs no action; got {actions:?}",
        );
    }

    #[test]
    fn compute_reload_actions_restart_for_changed_enabled_live_flow() {
        // Same URL, different action.repo (via flow_config_unchanged
        // failure on a non-URL field). The classifier returns
        // Restart with url_changed=false because the URL is the same.
        let mut old_f = flow("ci", "https://example.com/repo.git", true, Vec::new());
        let ActionConfig::GithubWorkflowDispatch { repo, .. } = &mut old_f.action;
        *repo = "owner/old-repo".to_string();
        let mut new_f = flow("ci", "https://example.com/repo.git", true, Vec::new());
        let ActionConfig::GithubWorkflowDispatch { repo, .. } = &mut new_f.action;
        *repo = "owner/new-repo".to_string();
        let actions = run(vec![old_f], vec![new_f], &["ci"]);
        assert_eq!(
            actions,
            vec![ReloadAction::Restart {
                name: "ci".to_string(),
                url_changed: false,
            }],
        );
    }

    #[test]
    fn compute_reload_actions_restart_with_url_changed_for_url_diff() {
        let old_f = flow("ci", "https://example.com/old.git", true, Vec::new());
        let new_f = flow("ci", "https://example.com/new.git", true, Vec::new());
        let actions = run(vec![old_f], vec![new_f], &["ci"]);
        assert_eq!(
            actions,
            vec![ReloadAction::Restart {
                name: "ci".to_string(),
                url_changed: true,
            }],
        );
    }

    #[test]
    fn compute_reload_actions_spawn_for_genuinely_new_flow() {
        let new_f = flow("ci", "https://example.com/repo.git", true, Vec::new());
        let actions = run(vec![], vec![new_f], &[]);
        assert_eq!(
            actions,
            vec![ReloadAction::Spawn {
                name: "ci".to_string()
            }],
        );
    }

    #[test]
    fn compute_reload_actions_spawn_for_revived_flow_with_no_live_handle() {
        // Flow exists in both old and new, enabled, but its handle
        // is missing (panic-mid-respawn). The classifier emits Spawn
        // so the supervisor brings up a fresh pair. The pre-existing
        // persisted state is preserved (no FlowRemoved) so the new
        // poll loop's baseline reflects the last observed SHA.
        let f = flow("ci", "https://example.com/repo.git", true, Vec::new());
        let actions = run(vec![f.clone()], vec![f], &[]);
        assert_eq!(
            actions,
            vec![ReloadAction::Spawn {
                name: "ci".to_string()
            }],
        );
    }

    #[test]
    fn compute_reload_actions_handles_multiple_flows_independently() {
        // 4 flows simultaneously: keep, remove, disable, restart.
        // Confirms the classifier produces an action per flow and
        // doesn't cross-talk between names.
        let kept_old = flow("kept", "https://example.com/k.git", true, Vec::new());
        let kept_new = kept_old.clone();
        let removed = flow("removed", "https://example.com/r.git", true, Vec::new());
        let disabled_old = flow("dis", "https://example.com/d.git", true, Vec::new());
        let disabled_new = flow("dis", "https://example.com/d.git", false, Vec::new());
        let restart_old = flow("rs", "https://example.com/old.git", true, Vec::new());
        let restart_new = flow("rs", "https://example.com/new.git", true, Vec::new());
        let actions = run(
            vec![kept_old, removed, disabled_old, restart_old],
            vec![kept_new, disabled_new, restart_new],
            &["kept", "removed", "dis", "rs"],
        );
        // Output ordering follows the BTreeSet name-ordering. Names
        // sort alphabetically: "dis" < "kept" < "removed" < "rs".
        assert_eq!(
            actions,
            vec![
                ReloadAction::Disable {
                    name: "dis".to_string(),
                    url_changed: false,
                },
                ReloadAction::Keep {
                    name: "kept".to_string(),
                },
                ReloadAction::Remove {
                    name: "removed".to_string(),
                    live_handle: true,
                },
                ReloadAction::Restart {
                    name: "rs".to_string(),
                    url_changed: true,
                },
            ],
        );
    }

    // -----------------------------------------------------------------
    // run_reload integration tests
    //
    // The pure compute_reload_actions classifier is exercised above;
    // these tests drive run_reload's side-effect body for the paths
    // that DO NOT require spawn_flow (which would pull in the
    // credential pool's HTTP machinery + the rate-limit poller).
    // Covered: parse-error path (RELOAD_SYNTHETIC_KEY recording),
    // Remove path (cancel + FlowRemoved emit), Disable path (cancel
    // without FlowRemoved), Disable+url_change path (cancel + FlowRemoved).
    //
    // Spawn-required paths (Add flow, URL-change Restart, non-URL
    // Restart, Reload-race) live in tests/supervisor_loop_factories.rs
    // where the full daemon boots a real credential pool against a
    // tempdir-backed credential file.
    // -----------------------------------------------------------------

    use std::collections::BTreeMap as StdBTreeMap;
    use std::path::Path;
    use std::sync::Mutex as StdMutex;

    use tokio::sync::{mpsc, Mutex, RwLock};
    use tokio_util::sync::CancellationToken;

    use super::super::control::ControlCommand;
    use super::super::credentials::CredentialPool;
    use super::super::flows::{DispatchTaskFactory, PollTaskFactory};
    use super::super::types::{FlowHandle, FlowLastError};
    use crate::flow::TRIGGER_QUEUE;
    use crate::state::{State, StateUpdate};

    /// Tempdir + ready-to-go run_reload arguments. Holding the TempDir
    /// on the struct keeps the on-disk config alive for the duration
    /// of the test (Drop runs `remove_dir_all`).
    struct ReloadFixture {
        config_path: std::path::PathBuf,
        config_watch: Arc<watch::Sender<Arc<Config>>>,
        last_errors: Arc<Mutex<StdBTreeMap<String, FlowLastError>>>,
        state_rx: mpsc::Receiver<StateUpdate>,
        ctx: SpawnContext,
        registry: FlowRegistry,
        join_set: JoinSet<FlowExit>,
        control_handler: Arc<ControlHandler>,
        _config_dir: tempfile::TempDir,
    }

    /// Build a ReloadFixture with the supplied initial config stored in
    /// the `config_watch`. Pre-populates the registry with one
    /// `FlowHandle` per name in `pre_seed_handles`. Returns the
    /// per-flow cancel tokens so the caller can assert
    /// `is_cancelled()` after run_reload.
    ///
    /// Factories panic if invoked — these tests exercise paths that
    /// never reach `spawn_flow`. A factory invocation surfaces as a
    /// loud test failure rather than a silent wrong-arm regression.
    fn build_reload_fixture(
        initial: &Config,
        pre_seed_handles: &[String],
    ) -> (ReloadFixture, BTreeMap<String, CancellationToken>) {
        let config_dir = tempfile::tempdir().expect("config tempdir");
        let config_path = config_dir.path().join("gcit.toml");
        // Placeholder write so the file exists. Each test rewrites
        // before calling run_reload; the placeholder body would only
        // matter if a test forgot to overwrite, in which case
        // crate::config::load returns a parse error (caught by the
        // parse-error arm).
        std::fs::write(&config_path, b"placeholder\n").expect("placeholder write");

        let initial_arc = Arc::new(initial.clone());
        let (config_watch_tx, _config_watch_rx) =
            watch::channel::<Arc<Config>>(Arc::clone(&initial_arc));
        let config_watch = Arc::new(config_watch_tx);

        let last_errors: Arc<Mutex<StdBTreeMap<String, FlowLastError>>> =
            Arc::new(Mutex::new(StdBTreeMap::new()));

        let (state_tx, state_rx) = mpsc::channel::<StateUpdate>(64);
        let state_mirror = Arc::new(StdMutex::new(State::default()));

        let ctx = SpawnContext {
            credential_pool: Arc::new(RwLock::new(CredentialPool::default())),
            shared_reqwest: Arc::new(reqwest::Client::new()),
            hostname: Arc::new("ci-host".to_string()),
            state_tx,
            state_mirror: Arc::clone(&state_mirror),
            root_cancel: CancellationToken::new(),
            last_errors: Arc::clone(&last_errors),
            poll_task_factory: panic_poll_factory(),
            dispatch_task_factory: panic_dispatch_factory(),
        };

        let mut registry = FlowRegistry::new();
        let mut cancel_tokens = BTreeMap::new();
        for name in pre_seed_handles {
            let cancel = CancellationToken::new();
            let (trigger_tx, _trigger_rx) = mpsc::channel(TRIGGER_QUEUE);
            registry.handles.insert(
                name.clone(),
                FlowHandle {
                    cancel: cancel.clone(),
                    trigger_tx,
                },
            );
            cancel_tokens.insert(name.clone(), cancel);
        }

        let join_set: JoinSet<FlowExit> = JoinSet::new();

        let (cmd_tx, _cmd_rx) = mpsc::channel::<ControlCommand>(8);
        let control_handler = Arc::new(ControlHandler {
            cmd_tx,
            state_mirror,
            last_errors: Arc::clone(&last_errors),
            flow_names: Arc::new(RwLock::new(
                pre_seed_handles.iter().cloned().collect::<Vec<_>>(),
            )),
        });

        let fixture = ReloadFixture {
            config_path,
            config_watch,
            last_errors,
            state_rx,
            ctx,
            registry,
            join_set,
            control_handler,
            _config_dir: config_dir,
        };
        (fixture, cancel_tokens)
    }

    fn panic_poll_factory() -> PollTaskFactory {
        Arc::new(|_, _, _, _, _| {
            panic!(
                "poll factory invoked unexpectedly: \
                 reload-side-effect tests exercise paths that never reach spawn_flow"
            )
        })
    }

    fn panic_dispatch_factory() -> DispatchTaskFactory {
        Arc::new(|_, _, _, _, _| {
            panic!(
                "dispatch factory invoked unexpectedly: \
                 reload-side-effect tests exercise paths that never reach spawn_flow"
            )
        })
    }

    /// Serialize a `Config` to a TOML string the production
    /// `crate::config::load` parses back into an equivalent value.
    /// Used to write the new-config body to `config_path` before
    /// triggering the reload.
    ///
    /// `Config.poll.source_interval` is `Option<Duration>` (the parser
    /// uses None to denote "use the strategy's default"); the helper
    /// renders it only when Some so the parser does not see a stray
    /// `source_interval = null` line.
    fn config_to_toml(cfg: &Config) -> String {
        let mut out = String::new();
        out.push_str("[poll]\n");
        if let Some(d) = cfg.poll.source_interval {
            out.push_str(&format!("source_interval = \"{}s\"\n", d.as_secs()));
        }
        out.push_str(&format!(
            "job_interval = \"{}s\"\n",
            cfg.poll.job_interval.as_secs(),
        ));
        out.push_str(&format!("jitter = {}\n", cfg.poll.jitter));
        out.push_str("\n[http]\n");
        out.push_str(&format!(
            "request_timeout = \"{}s\"\n",
            cfg.http.request_timeout.as_secs(),
        ));
        for f in &cfg.flow {
            out.push_str("\n[[flow]]\n");
            out.push_str(&format!("name = \"{}\"\n", f.name));
            if !f.enabled {
                out.push_str("enabled = false\n");
            }
            out.push_str("\n[flow.source]\n");
            out.push_str(&format!("url = \"{}\"\n", f.source.url));
            out.push_str(&format!("ref = \"{}\"\n", f.source.ref_name));
            let ActionConfig::GithubWorkflowDispatch {
                repo,
                workflow,
                ref_name,
                credential_id,
                ..
            } = &f.action;
            out.push_str("\n[flow.action]\n");
            out.push_str("kind          = \"github_workflow_dispatch\"\n");
            out.push_str(&format!("repo          = \"{repo}\"\n"));
            out.push_str(&format!("workflow      = \"{workflow}\"\n"));
            out.push_str(&format!("ref           = \"{ref_name}\"\n"));
            out.push_str(&format!(
                "credential_id = \"{}\"\n",
                credential_id.as_str(),
            ));
        }
        out
    }

    /// `PollDefaults` matching the validate.rs MIN_INTERVAL floor (15s)
    /// so `crate::config::load` accepts the rendered TOML.
    /// `source_interval` is Some so the rendered body always carries a
    /// value.
    fn default_poll_defaults() -> crate::config::PollDefaults {
        crate::config::PollDefaults {
            source_interval: Some(Duration::from_secs(15)),
            job_interval: Duration::from_secs(15),
            jitter: 0.0,
        }
    }

    fn cfg_with_defaults(flows: Vec<FlowConfig>) -> Config {
        Config {
            source_path: std::path::PathBuf::new(),
            poll: default_poll_defaults(),
            log: crate::config::LogConfig::default(),
            http: crate::config::HttpConfig::default(),
            flow: flows,
            credential_lines: std::collections::BTreeMap::new(),
        }
    }

    /// Drain every queued StateUpdate from the receiver. run_reload's
    /// FlowRemoved emissions are awaited synchronously before
    /// run_reload returns, so a single drain after the call captures
    /// the full set.
    fn drain_state_updates(rx: &mut mpsc::Receiver<StateUpdate>) -> Vec<StateUpdate> {
        let mut out = Vec::new();
        while let Ok(u) = rx.try_recv() {
            out.push(u);
        }
        out
    }

    fn write_config_at(path: &Path, body: &str) {
        std::fs::write(path, body).expect("rewrite config");
    }

    #[tokio::test]
    async fn run_reload_parse_failure_records_synthetic_key_and_leaves_registry_unchanged() {
        // When crate::config::load fails to parse the new config,
        // run_reload records every nested ConfigError under
        // RELOAD_SYNTHETIC_KEY="(reload)" with kind="config_reload"
        // and re-emits sd_notify(Ready) so systemd does not stay stuck
        // in Reloading. The running registry MUST be untouched —
        // operator's typo should not cancel any live flow.
        let initial = cfg_with_defaults(vec![flow(
            "ci",
            "https://example.com/repo.git",
            true,
            Vec::new(),
        )]);
        let (mut fixture, cancel_tokens) = build_reload_fixture(&initial, &["ci".to_string()]);
        write_config_at(&fixture.config_path, "this is not valid TOML [[[\n");

        run_reload(
            &fixture.config_path,
            Arc::clone(&fixture.config_watch),
            &fixture.ctx,
            &mut fixture.join_set,
            &mut fixture.registry,
            &fixture.control_handler,
        )
        .await;

        // Registry untouched.
        assert!(
            fixture.registry.handles.contains_key("ci"),
            "parse-error path must NOT remove the live FlowHandle",
        );
        assert!(
            !cancel_tokens["ci"].is_cancelled(),
            "parse-error path must NOT fire the cancel token",
        );

        // RELOAD_SYNTHETIC_KEY recorded.
        let errs = fixture.last_errors.lock().await;
        let entry = errs
            .get(RELOAD_SYNTHETIC_KEY)
            .expect("(reload) synthetic key must be recorded on parse failure");
        assert_eq!(
            entry.kind(),
            "config_reload",
            "parse-error last_error must use kind='config_reload' so the renderer routes it as a reload-stage failure",
        );
        drop(errs);

        let drained = drain_state_updates(&mut fixture.state_rx);
        assert!(
            drained.is_empty(),
            "parse-error path must NOT emit any StateUpdate; got: {drained:?}",
        );
    }

    #[tokio::test]
    async fn run_reload_remove_one_of_two_flows_cancels_handle_and_emits_flow_removed() {
        // Old config: 2 flows ("kept" + "removed"); new config: only
        // "kept". compute_reload_actions returns Keep{kept} +
        // Remove{removed, live_handle:true}. run_reload (1) calls
        // handle.cancel.cancel() on `removed`, (2) emits
        // StateUpdate::FlowRemoved{flow:"removed"} on state_tx, (3)
        // leaves the `kept` handle untouched (its cancel token must
        // NOT fire — the Keep arm just re-inserts the existing handle
        // back into registry.handles), (4) refreshes
        // control_handler.flow_names to ["kept"].
        //
        // Config validation rejects an empty flow list, so the
        // "Remove only" scenario is expressed as a 2->1 flows reload
        // rather than a 1->0 reload — the 1->0 reload would surface
        // as the parse-error arm tested separately above.
        let initial = cfg_with_defaults(vec![
            flow("kept", "https://example.com/k.git", true, Vec::new()),
            flow("removed", "https://example.com/r.git", true, Vec::new()),
        ]);
        let (mut fixture, cancel_tokens) =
            build_reload_fixture(&initial, &["kept".to_string(), "removed".to_string()]);
        let new_cfg = cfg_with_defaults(vec![flow(
            "kept",
            "https://example.com/k.git",
            true,
            Vec::new(),
        )]);
        write_config_at(&fixture.config_path, &config_to_toml(&new_cfg));

        run_reload(
            &fixture.config_path,
            Arc::clone(&fixture.config_watch),
            &fixture.ctx,
            &mut fixture.join_set,
            &mut fixture.registry,
            &fixture.control_handler,
        )
        .await;

        // Removed flow's handle dropped + cancel fired.
        assert!(
            cancel_tokens["removed"].is_cancelled(),
            "Remove arm must fire the cancel token on the removed FlowHandle",
        );
        assert!(
            !fixture.registry.handles.contains_key("removed"),
            "Remove arm must drop the removed FlowHandle entry",
        );
        // Kept flow's handle untouched (the Keep arm re-inserts the
        // same FlowHandle back into registry.handles).
        assert!(
            !cancel_tokens["kept"].is_cancelled(),
            "Keep arm must NOT fire the kept flow's cancel token",
        );
        assert!(
            fixture.registry.handles.contains_key("kept"),
            "Keep arm must preserve the kept FlowHandle entry",
        );
        // FlowRemoved emitted exactly once for `removed`, not for `kept`.
        let drained = drain_state_updates(&mut fixture.state_rx);
        let removed_flows: Vec<&str> = drained
            .iter()
            .filter_map(|u| match u {
                StateUpdate::FlowRemoved { flow } => Some(flow.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            removed_flows,
            vec!["removed"],
            "must emit FlowRemoved only for the removed flow; got: {drained:?}",
        );
        // control_handler.flow_names refreshed.
        let names = fixture.control_handler.flow_names.read().await;
        assert_eq!(
            *names,
            vec!["kept".to_string()],
            "post-reload flow_names must reflect the kept registry; got: {names:?}",
        );
    }

    #[tokio::test]
    async fn run_reload_disable_without_url_change_cancels_handle_without_emitting_flow_removed() {
        // Old: enabled flow at URL X; new: SAME flow + URL X but
        // enabled=false. compute_reload_actions returns
        // Disable{url_changed:false}. Cancel fires; NO FlowRemoved
        // emitted (persisted state survives for re-enable).
        let initial = cfg_with_defaults(vec![flow(
            "ci",
            "https://example.com/repo.git",
            true,
            Vec::new(),
        )]);
        let (mut fixture, cancel_tokens) = build_reload_fixture(&initial, &["ci".to_string()]);
        let new_cfg = cfg_with_defaults(vec![flow(
            "ci",
            "https://example.com/repo.git",
            false,
            Vec::new(),
        )]);
        write_config_at(&fixture.config_path, &config_to_toml(&new_cfg));

        run_reload(
            &fixture.config_path,
            Arc::clone(&fixture.config_watch),
            &fixture.ctx,
            &mut fixture.join_set,
            &mut fixture.registry,
            &fixture.control_handler,
        )
        .await;

        assert!(cancel_tokens["ci"].is_cancelled());
        assert!(!fixture.registry.handles.contains_key("ci"));
        let drained = drain_state_updates(&mut fixture.state_rx);
        let removed_count = drained
            .iter()
            .filter(|u| matches!(u, StateUpdate::FlowRemoved { .. }))
            .count();
        assert_eq!(
            removed_count, 0,
            "Disable WITHOUT url_changed must NOT emit FlowRemoved; got: {drained:?}",
        );
    }

    #[tokio::test]
    async fn run_reload_disable_with_url_change_cancels_handle_and_emits_flow_removed_for_url_reset() {
        // Same as above but with a URL change. compute_reload_actions
        // returns Disable{url_changed:true}, which adds the flow name
        // to `url_resets` and emits FlowRemoved so a future re-enable
        // does not seed a spurious dispatch on the new source's first
        // poll cycle.
        let initial = cfg_with_defaults(vec![flow(
            "ci",
            "https://example.com/old.git",
            true,
            Vec::new(),
        )]);
        let (mut fixture, cancel_tokens) = build_reload_fixture(&initial, &["ci".to_string()]);
        let new_cfg = cfg_with_defaults(vec![flow(
            "ci",
            "https://example.com/new.git",
            false,
            Vec::new(),
        )]);
        write_config_at(&fixture.config_path, &config_to_toml(&new_cfg));

        run_reload(
            &fixture.config_path,
            Arc::clone(&fixture.config_watch),
            &fixture.ctx,
            &mut fixture.join_set,
            &mut fixture.registry,
            &fixture.control_handler,
        )
        .await;

        assert!(cancel_tokens["ci"].is_cancelled());
        let drained = drain_state_updates(&mut fixture.state_rx);
        let removed = drained
            .iter()
            .filter(|u| matches!(u, StateUpdate::FlowRemoved { flow } if flow == "ci"))
            .count();
        assert_eq!(
            removed, 1,
            "Disable+url_changed arm must emit exactly one FlowRemoved (the url_resets sweep); got: {drained:?}",
        );
    }

    #[tokio::test]
    async fn run_reload_disabled_flow_with_no_handle_emits_no_action() {
        // Reload from "no flows" to "one disabled flow with no live
        // handle" hits the no-action arm of `compute_reload_actions`.
        // The surrounding run_reload still updates the watch +
        // flow_names + emits Ready, but no FlowRemoved/cancel firings.
        let initial = cfg_with_defaults(vec![]);
        let (mut fixture, _cancel_tokens) = build_reload_fixture(&initial, &[]);
        let new_cfg = cfg_with_defaults(vec![flow(
            "ci",
            "https://example.com/repo.git",
            false,
            Vec::new(),
        )]);
        write_config_at(&fixture.config_path, &config_to_toml(&new_cfg));

        run_reload(
            &fixture.config_path,
            Arc::clone(&fixture.config_watch),
            &fixture.ctx,
            &mut fixture.join_set,
            &mut fixture.registry,
            &fixture.control_handler,
        )
        .await;

        let drained = drain_state_updates(&mut fixture.state_rx);
        assert!(
            drained.is_empty(),
            "no-action arm must emit no StateUpdate; got: {drained:?}",
        );
        // config_watch updated so the panic-respawn path reads the
        // new shape next time it fires.
        let watched = fixture.config_watch.borrow().clone();
        assert_eq!(
            watched.flow.len(),
            1,
            "config_watch must reflect the new (disabled-flow) config",
        );
    }

    #[tokio::test]
    async fn run_reload_remove_then_disabled_flow_with_no_handle_emits_only_remove() {
        // Old config: flow A enabled with live handle; new config: A
        // dropped + B disabled with no live handle.
        // compute_reload_actions emits Remove{A,live_handle:true} and
        // NO action for B. run_reload emits exactly one FlowRemoved
        // (for A) and cancels A's handle. B touches nothing.
        let initial = cfg_with_defaults(vec![flow(
            "a",
            "https://example.com/a.git",
            true,
            Vec::new(),
        )]);
        let (mut fixture, cancel_tokens) = build_reload_fixture(&initial, &["a".to_string()]);
        let new_cfg = cfg_with_defaults(vec![flow(
            "b",
            "https://example.com/b.git",
            false,
            Vec::new(),
        )]);
        write_config_at(&fixture.config_path, &config_to_toml(&new_cfg));

        run_reload(
            &fixture.config_path,
            Arc::clone(&fixture.config_watch),
            &fixture.ctx,
            &mut fixture.join_set,
            &mut fixture.registry,
            &fixture.control_handler,
        )
        .await;

        assert!(cancel_tokens["a"].is_cancelled());
        assert!(!fixture.registry.handles.contains_key("a"));
        let drained = drain_state_updates(&mut fixture.state_rx);
        let removed: Vec<&str> = drained
            .iter()
            .filter_map(|u| match u {
                StateUpdate::FlowRemoved { flow } => Some(flow.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            removed,
            vec!["a"],
            "must emit FlowRemoved only for the removed flow A, not for the disabled-no-handle B; got: {drained:?}",
        );
    }

    #[tokio::test]
    async fn run_reload_clears_stale_synthetic_reload_key_after_successful_reload() {
        // A stale `(reload)` last_error from a previous failed reload
        // is cleared on the NEXT successful reload. The cleanup runs
        // before the flow_names refresh so a status reader seeing the
        // new flow_names sees consistent last_errors (no stale
        // "(reload)" alongside fresh flow_names).
        let initial = cfg_with_defaults(vec![flow(
            "ci",
            "https://example.com/repo.git",
            true,
            Vec::new(),
        )]);
        let (mut fixture, _cancel_tokens) = build_reload_fixture(&initial, &["ci".to_string()]);
        record_last_error(
            &fixture.last_errors,
            RELOAD_SYNTHETIC_KEY,
            "config_reload",
            "stale parse error from previous reload",
            None,
        )
        .await;
        assert!(
            fixture
                .last_errors
                .lock()
                .await
                .contains_key(RELOAD_SYNTHETIC_KEY),
            "precondition: stale (reload) entry must be present",
        );

        // New config equal to old (no-op reload, but the cleanup arm
        // still runs).
        let new_cfg = cfg_with_defaults(vec![flow(
            "ci",
            "https://example.com/repo.git",
            true,
            Vec::new(),
        )]);
        write_config_at(&fixture.config_path, &config_to_toml(&new_cfg));

        run_reload(
            &fixture.config_path,
            Arc::clone(&fixture.config_watch),
            &fixture.ctx,
            &mut fixture.join_set,
            &mut fixture.registry,
            &fixture.control_handler,
        )
        .await;

        let errs = fixture.last_errors.lock().await;
        assert!(
            !errs.contains_key(RELOAD_SYNTHETIC_KEY),
            "successful reload must clear the stale (reload) synthetic key; got: {:?}",
            errs.keys().collect::<Vec<_>>(),
        );
    }

    #[tokio::test]
    async fn run_reload_updates_config_watch_with_new_config_for_panic_respawn_path() {
        // The config_watch is replaced with the new Arc<Config> AFTER
        // drain + FlowRemoved emit but BEFORE the spawn loop. The
        // panic-respawn path (`handle_respawn_request`) reads from
        // this watch to decide whether the respawning flow is still
        // in config — without this update, a respawn arriving after a
        // reload would observe the OLD config's enabled/url state.
        let initial = cfg_with_defaults(vec![flow(
            "ci",
            "https://example.com/old.git",
            true,
            Vec::new(),
        )]);
        let (mut fixture, _cancel_tokens) = build_reload_fixture(&initial, &[]);
        assert_eq!(
            fixture.config_watch.borrow().flow[0].source.url,
            "https://example.com/old.git",
        );
        let new_cfg = cfg_with_defaults(vec![flow(
            "ci",
            "https://example.com/different.git",
            false,
            Vec::new(),
        )]);
        write_config_at(&fixture.config_path, &config_to_toml(&new_cfg));

        run_reload(
            &fixture.config_path,
            Arc::clone(&fixture.config_watch),
            &fixture.ctx,
            &mut fixture.join_set,
            &mut fixture.registry,
            &fixture.control_handler,
        )
        .await;

        let watched = fixture.config_watch.borrow().clone();
        assert_eq!(watched.flow.len(), 1);
        assert_eq!(
            watched.flow[0].source.url, "https://example.com/different.git",
            "config_watch must be replaced with the new config so respawn path reads fresh state",
        );
        assert!(!watched.flow[0].enabled);
    }
}

// `run_reload` — apply the classifier's `ReloadAction` list to live
// state: cancel handles, drain the JoinSet, emit FlowRemoved,
// invalidate stale credentials, and spawn the new generation.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use sd_notify::NotifyState;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::config::{Config, FlowConfig};
use crate::state::StateUpdate;

use super::super::control::ControlHandler;
use super::super::flows::{spawn_flow, SpawnContext};
use super::super::types::{record_last_error, FlowExit, FlowRegistry, RELOAD_SYNTHETIC_KEY};

use super::action::{compute_reload_actions, ReloadAction};
use super::diff::collect_kept_credentials;

/// Reload entry point shared by SIGHUP and the control-channel
/// `Reload` request.
///
/// Per-flow diff semantics: each running handle is classified as
/// removed, disabled, URL-changed, otherwise-changed, or unchanged.
/// Removed and URL-changed flows additionally emit `FlowRemoved` so
/// persisted state for the gone-or-stale source does not seed a
/// spurious dispatch on the next poll. Unchanged flows skip
/// cancel/respawn so their existing pair (and in-flight monitor
/// tracking) stays alive.
///
/// Cached credentials are invalidated up front so a rotated PAT is
/// picked up by the new generation without a daemon restart.
///
/// Manages sd_notify state transitions (Reloading -> Ready on
/// success; Reloading -> Ready on failure with a WARN log — systemd
/// must never observe the daemon stuck in Reloading).
pub(crate) async fn run_reload(
    config_path: &std::path::Path,
    config_watch: Arc<watch::Sender<Arc<Config>>>,
    ctx: &SpawnContext,
    join_set: &mut JoinSet<FlowExit>,
    registry: &mut FlowRegistry,
    control_handler: &Arc<ControlHandler>,
) {
    emit_reloading_notify();

    let new_cfg = match load_new_config_or_record_error(config_path, ctx).await {
        Some(cfg) => cfg,
        None => return,
    };
    let old_cfg = config_watch.borrow().clone();
    info!(
        target: "gcit::supervisor",
        flows = new_cfg.flow.len(),
        "config reloaded; diffing per-flow",
    );

    let actions = compute_actions(&old_cfg, &new_cfg, registry);
    let plan = apply_actions_to_registry(&actions, registry);

    invalidate_stale_credentials(ctx, &new_cfg, &plan.to_keep).await;
    drain_until_kept(join_set, plan.to_keep.len() * 2, &plan.to_keep).await;
    emit_flow_removed(ctx, &plan.removed_flows, &plan.url_resets).await;

    // Push the new config BEFORE respawning so the panic-respawn
    // path (which reads from the watch) sees the new shape if any
    // freshly-spawned flow panics during its first iteration.
    config_watch.send_replace(Arc::clone(&new_cfg));

    spawn_new_generation(&new_cfg, ctx, join_set, registry, &plan.to_keep).await;

    finalize_reload(ctx, control_handler, registry).await;
}

fn emit_reloading_notify() {
    let mut reloading_states: Vec<NotifyState> = vec![NotifyState::Reloading];
    match NotifyState::monotonic_usec_now() {
        Ok(m) => reloading_states.push(m),
        Err(e) => {
            // CLOCK_MONOTONIC unavailable. sd-notify still accepts
            // Reloading=1 without MONOTONIC_USEC; systemd's reload
            // deadline tracking is degraded but the daemon still
            // signals reload-in-progress.
            tracing::debug!(
                target: "gcit::supervisor",
                error = %e,
                "sd_notify monotonic_usec_now failed; Reloading state missing MONOTONIC_USEC",
            );
        }
    }
    let _ = sd_notify::notify(&reloading_states);
}

/// Re-parses the config file. On Err records a `(reload)` synthetic
/// last_error, re-emits Ready (so systemd doesn't stay in Reloading),
/// and returns None — the caller short-circuits the rest of the reload.
async fn load_new_config_or_record_error(
    config_path: &std::path::Path,
    ctx: &SpawnContext,
) -> Option<Arc<Config>> {
    match crate::config::load(config_path) {
        Ok(c) => Some(Arc::new(c)),
        Err(errs) => {
            let formatted: Vec<String> = errs.iter().map(|e| format!("{}", e)).collect();
            warn!(
                target: "gcit::supervisor",
                errors = ?formatted,
                "reload failed to parse config; staying on previous config",
            );
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
            None
        }
    }
}

fn compute_actions(
    old_cfg: &Config,
    new_cfg: &Config,
    registry: &FlowRegistry,
) -> Vec<ReloadAction> {
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
    compute_reload_actions(&old_by_name, &new_by_name, &live_handles)
}

/// Aggregated side-effect plan produced by walking the per-flow
/// actions. `to_keep` is the set of unchanged flow names (their
/// handles stay live); `url_resets` and `removed_flows` are the
/// names whose persisted state should be dropped via FlowRemoved;
/// they are mutually exclusive (one flow lands in exactly one list).
struct ReloadPlan {
    to_keep: BTreeSet<String>,
    url_resets: Vec<String>,
    removed_flows: Vec<String>,
}

/// Walks the per-flow actions: mutates `registry.handles` in place
/// (cancelling old generations, preserving kept ones, dropping the
/// pending-respawn slot for cancelled flows), cancels any orphan
/// handles not named by an action, and returns the side-effect plan
/// the rest of run_reload needs.
fn apply_actions_to_registry(actions: &[ReloadAction], registry: &mut FlowRegistry) -> ReloadPlan {
    let mut plan = ReloadPlan {
        to_keep: BTreeSet::new(),
        url_resets: Vec::new(),
        removed_flows: Vec::new(),
    };
    let mut drained = std::mem::take(&mut registry.handles);
    for action in actions {
        match action {
            ReloadAction::Keep { name } => {
                plan.to_keep.insert(name.clone());
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
                plan.removed_flows.push(name.clone());
                registry.respawning_flows.remove(name);
            }
            ReloadAction::Remove {
                name,
                live_handle: false,
            } => {
                // No live handle to cancel (panic-mid-respawn or
                // already-exited); still emit FlowRemoved so state
                // drops.
                plan.removed_flows.push(name.clone());
                registry.respawning_flows.remove(name);
            }
            ReloadAction::Disable { name, url_changed } => {
                if let Some(handle) = drained.remove(name) {
                    handle.cancel.cancel();
                }
                if *url_changed {
                    plan.url_resets.push(name.clone());
                }
                // Release pending respawn slot so a future re-enable
                // is not deduped against the disabled-period slot.
                registry.respawning_flows.remove(name);
            }
            ReloadAction::Restart { name, url_changed } => {
                if let Some(handle) = drained.remove(name) {
                    handle.cancel.cancel();
                }
                if *url_changed {
                    plan.url_resets.push(name.clone());
                }
                registry.respawning_flows.remove(name);
            }
            ReloadAction::Spawn { .. } => {
                // Performed by the new-config respawn loop below;
                // classifier just signals that we need it.
            }
        }
    }
    // Drained handles not in `actions` are orphans (registry
    // invariant violation) — cancel so the token releases.
    for (_name, handle) in drained {
        handle.cancel.cancel();
    }
    plan
}

async fn invalidate_stale_credentials(
    ctx: &SpawnContext,
    new_cfg: &Config,
    to_keep: &BTreeSet<String>,
) {
    let keep_credentials = collect_kept_credentials(new_cfg, to_keep);
    ctx.credential_pool
        .write()
        .await
        .invalidate_except(&keep_credentials);
}

/// Drains the JoinSet down to `kept_task_count` exits remaining, then
/// returns. Per the cancel-and-respawn contract: every cancelled flow
/// must return an exit before the new generation spawns, otherwise
/// two generations briefly race on the state writer mpsc. Kept-alive
/// flows produce no exit while running, so the count-based bound
/// stops once cancelled tasks have unwound. Each flow contributes 2
/// tasks (poll + dispatcher).
///
/// 30s timeout caps the wait when a cancelled task ignores its
/// CancellationToken. Leftover wedged tasks stay in the JoinSet and
/// are reaped lazily via the supervisor's main select! arm.
///
/// Worst-case wedged-old-gen state-writer race: a cancelled poll
/// task can have an in-flight `state_tx.send(PollObservation)` that
/// completes AFTER the new generation has spawned. tokio mpsc FIFO +
/// per-variant LWW in `apply.rs` bound the impact:
///   - PollObservation / PollTimestamp: LWW corrects on next
///     observation. One stale-but-valid SHA appears briefly.
///   - RunStarted: appends with run_id dedup. A wedged old-gen
///     dispatcher landing a RunStarted after cancel produces a
///     phantom active_runs entry whose monitor was already cancelled
///     (so its RunFinished never arrives). Cleared on flow removal
///     or daemon restart. Real but bounded leak.
///   - RunFinished: removes by run_id; disjoint across generations.
///   - FlowRemoved: drops the entry. Emitted AFTER the drain so
///     old-gen observations in the FIFO land first.
async fn drain_until_kept(
    join_set: &mut JoinSet<FlowExit>,
    kept_task_count: usize,
    to_keep: &BTreeSet<String>,
) {
    let drain = async {
        while join_set.len() > kept_task_count {
            match join_set.join_next().await {
                Some(Ok(exit)) => {
                    // Kept-alive flows that panic during the drain
                    // surface here too (the JoinSet doesn't partition
                    // by name); log so the panic stays
                    // operator-visible instead of disappearing.
                    if let Some(message) = &exit.panic {
                        let kept = to_keep.contains(&exit.flow);
                        warn!(
                            target: "gcit::supervisor",
                            flow = %exit.flow,
                            role = ?exit.role,
                            kept,
                            panic = %message,
                            "flow task panicked during reload drain",
                        );
                    }
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
    let drain_completed = tokio::time::timeout(Duration::from_secs(30), drain)
        .await
        .is_ok();
    if !drain_completed {
        warn!(
            target: "gcit::supervisor",
            remaining = join_set.len(),
            "reload drain timed out after 30s; proceeding with respawn (leftover tasks will be observed via the main select! loop when they complete)",
        );
    }
}

/// Emit FlowRemoved AFTER the drain. Sequencing: a cancelled poll
/// task can have one or more in-flight `state_tx.send(...)` calls
/// that finished after the cancel token fired but before the task
/// observed cancellation. tokio mpsc is FIFO so any FlowRemoved here
/// lands after those pending observations.
///
/// `removed_flows` and `url_resets` are exclusive per the classifier
/// (a flow lands in exactly one), so chaining is safe and reads as
/// one logical emit pass. A send error means the state writer is
/// gone — log because the FlowRemoved is lost and persisted state
/// for this flow will be stale until the next daemon restart.
async fn emit_flow_removed(ctx: &SpawnContext, removed_flows: &[String], url_resets: &[String]) {
    for name in removed_flows.iter().chain(url_resets.iter()) {
        if let Err(e) = ctx
            .state_tx
            .send(StateUpdate::FlowRemoved { flow: name.clone() })
            .await
        {
            warn!(
                target: "gcit::supervisor",
                flow = %name,
                error = %e,
                "state writer dropped during reload FlowRemoved emit; persisted state may be stale",
            );
        }
    }
}

async fn spawn_new_generation(
    new_cfg: &Config,
    ctx: &SpawnContext,
    join_set: &mut JoinSet<FlowExit>,
    registry: &mut FlowRegistry,
    to_keep: &BTreeSet<String>,
) {
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
        // Release the panic-respawn slot so a future panic on the
        // freshly-spawned generation is not deduped against the old
        // slot. (A late RespawnRequest from a sleeping panic-watcher
        // is independently filtered by `handle_respawn_request`'s
        // `handles.contains_key` check.)
        registry.respawning_flows.remove(&flow.name);
        spawn_flow(flow, new_cfg, ctx, join_set, registry).await;
    }
}

/// Clear the stale `(reload)` entry from a prior failed parse BEFORE
/// refreshing flow_names — a status reader interleaving between
/// these two writes would otherwise observe fresh flow_names
/// alongside the stale `(reload)` entry. Then refresh the control
/// handler's flow_names mirror and re-emit Ready so systemd leaves
/// the Reloading state.
async fn finalize_reload(
    ctx: &SpawnContext,
    control_handler: &Arc<ControlHandler>,
    registry: &FlowRegistry,
) {
    ctx.last_errors.lock().await.remove(RELOAD_SYNTHETIC_KEY);

    *control_handler.flow_names.write().await = registry.handles.keys().cloned().collect();

    if let Err(e) = sd_notify::notify(&[NotifyState::Ready]) {
        warn!(target: "gcit::supervisor", error = %e, "sd_notify Ready (post-reload) failed");
    }
    info!(target: "gcit::supervisor", "reload complete");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ActionConfig, Destination, FlowConfig, PollOverride, SourceConfig};
    use std::collections::BTreeMap as StdBTreeMap;
    use std::path::Path;
    use std::sync::Mutex as StdMutex;

    use tokio::sync::{mpsc, Mutex, RwLock};
    use tokio_util::sync::CancellationToken;

    use super::super::super::control::ControlCommand;
    use super::super::super::credentials::CredentialPool;
    use super::super::super::flows::{DispatchTaskFactory, PollTaskFactory};
    use super::super::super::types::{FlowHandle, FlowLastError};
    use crate::flow::TRIGGER_QUEUE;
    use crate::state::{State, StateUpdate};
    use crate::util::test_cred as cred;

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

    /// Tempdir + ready-to-go `run_reload` arguments. The TempDir
    /// stays on the struct so the on-disk config outlives the test
    /// (Drop runs `remove_dir_all`).
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

    /// Build a `ReloadFixture` with the supplied initial config in
    /// the watch + one `FlowHandle` per name in `pre_seed_handles`.
    /// Returns the per-flow cancel tokens so the caller can assert
    /// `is_cancelled()` after `run_reload`.
    ///
    /// Factories panic if invoked — these tests exercise paths that
    /// never reach `spawn_flow`. A factory invocation surfaces as a
    /// loud test failure.
    fn build_reload_fixture(
        initial: &Config,
        pre_seed_handles: &[String],
    ) -> (ReloadFixture, BTreeMap<String, CancellationToken>) {
        let config_dir = tempfile::tempdir().expect("config tempdir");
        let config_path = config_dir.path().join("gcit.toml");
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
            source_rate_buckets: Arc::new(StdMutex::new(BTreeMap::new())),
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
            flow_names: Arc::new(RwLock::new(pre_seed_handles.to_vec())),
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
        Arc::new(|_, _, _, _, _, _| {
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

    /// Serialize a `Config` to TOML the production
    /// `crate::config::load` parses back into an equivalent value.
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
            out.push_str(&format!("credential_id = \"{}\"\n", credential_id.as_str(),));
        }
        out
    }

    /// `PollDefaults` matching `validate::MIN_INTERVAL` (15s) so
    /// `crate::config::load` accepts the rendered TOML.
    fn default_poll_defaults() -> crate::config::PollDefaults {
        crate::config::PollDefaults {
            source_interval: Some(Duration::from_secs(15)),
            job_interval: Duration::from_secs(15),
            jitter: 0.0,
            cooldown: Duration::ZERO,
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
        // Parse failure must record under RELOAD_SYNTHETIC_KEY with
        // kind="config_reload" + re-emit sd_notify(Ready). Registry
        // untouched — operator typo must not cancel a live flow.
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

        assert!(fixture.registry.handles.contains_key("ci"));
        assert!(!cancel_tokens["ci"].is_cancelled());

        let errs = fixture.last_errors.lock().await;
        let entry = errs
            .get(RELOAD_SYNTHETIC_KEY)
            .expect("(reload) synthetic key must be recorded");
        assert_eq!(entry.kind(), "config_reload");
        drop(errs);

        let drained = drain_state_updates(&mut fixture.state_rx);
        assert!(
            drained.is_empty(),
            "parse-error emits no StateUpdate; got: {drained:?}"
        );
    }

    #[tokio::test]
    async fn run_reload_remove_one_of_two_flows_cancels_handle_and_emits_flow_removed() {
        // 2-flow config → 1-flow config. Classifier: Keep{kept} +
        // Remove{removed, live_handle:true}. run_reload cancels
        // `removed`, emits FlowRemoved for it, keeps `kept`,
        // refreshes control_handler.flow_names to ["kept"].
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

        assert!(cancel_tokens["removed"].is_cancelled());
        assert!(!fixture.registry.handles.contains_key("removed"));
        assert!(!cancel_tokens["kept"].is_cancelled());
        assert!(fixture.registry.handles.contains_key("kept"));

        let drained = drain_state_updates(&mut fixture.state_rx);
        let removed_flows: Vec<&str> = drained
            .iter()
            .filter_map(|u| match u {
                StateUpdate::FlowRemoved { flow } => Some(flow.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(removed_flows, vec!["removed"]);

        let names = fixture.control_handler.flow_names.read().await;
        assert_eq!(*names, vec!["kept".to_string()]);
    }

    #[tokio::test]
    async fn run_reload_disable_without_url_change_cancels_handle_without_emitting_flow_removed() {
        // Same URL, enabled→false. Disable{url_changed:false}.
        // Cancel fires; no FlowRemoved (state preserved for
        // re-enable).
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
        assert_eq!(removed_count, 0, "got: {drained:?}");
    }

    #[tokio::test]
    async fn run_reload_disable_with_url_change_cancels_handle_and_emits_flow_removed_for_url_reset(
    ) {
        // Disable + URL change adds to url_resets and emits
        // FlowRemoved so a future re-enable doesn't seed a spurious
        // dispatch.
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
        assert_eq!(removed, 1, "got: {drained:?}");
    }

    #[tokio::test]
    async fn run_reload_disabled_flow_with_no_handle_emits_no_action() {
        // No flows → 1 disabled flow with no live handle. No
        // FlowRemoved/cancel. Watch + flow_names still update; Ready
        // still re-emitted.
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
        assert!(drained.is_empty(), "got: {drained:?}");
        let watched = fixture.config_watch.borrow().clone();
        assert_eq!(watched.flow.len(), 1);
    }

    #[tokio::test]
    async fn run_reload_remove_then_disabled_flow_with_no_handle_emits_only_remove() {
        // Old: A enabled+live; new: A dropped, B disabled+no-handle.
        // Classifier: Remove{A, live:true} + no action for B.
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
        assert_eq!(removed, vec!["a"]);
    }

    #[tokio::test]
    async fn run_reload_clears_stale_synthetic_reload_key_after_successful_reload() {
        // Stale `(reload)` from a prior failed reload must be
        // cleared on the next successful reload. Cleanup runs
        // BEFORE flow_names refresh so the two stay consistent for
        // a status reader.
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
        assert!(fixture
            .last_errors
            .lock()
            .await
            .contains_key(RELOAD_SYNTHETIC_KEY));

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
            "got: {:?}",
            errs.keys().collect::<Vec<_>>(),
        );
    }

    #[tokio::test]
    async fn run_reload_updates_config_watch_with_new_config_for_panic_respawn_path() {
        // The watch must be replaced with the new Arc<Config> AFTER
        // drain + FlowRemoved emit but BEFORE the spawn loop —
        // `handle_respawn_request` reads from the watch so a
        // respawn arriving after a reload sees fresh state.
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
            watched.flow[0].source.url,
            "https://example.com/different.git",
        );
        assert!(!watched.flow[0].enabled);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[tracing_test::traced_test]
    async fn run_reload_drain_timeout_warns_when_tasks_ignore_cancel() {
        // The 30s drain timeout exists so a cancelled poll/dispatcher
        // task that refuses to observe its CancellationToken (wedged
        // syscall, deadlocked mutex, etc.) does not stall the entire
        // reload. After 30s the drain returns Err and emits a
        // `reload drain timed out` warn — the leftover tasks stay in
        // the JoinSet for the main `select!` loop to reap lazily.
        //
        // Pin the warn path: seed the JoinSet with two wedged tasks
        // that take a year of virtual time to exit and never observe
        // cancellation. Under `start_paused = true` the 30s timeout
        // fires before the wedged sleeps would have ended, so the
        // drain returns Err and the warn surfaces in the captured
        // trace buffer.
        use super::super::super::types::FlowRole;

        let initial = cfg_with_defaults(vec![flow(
            "stubborn",
            "https://example.com/stubborn.git",
            true,
            Vec::new(),
        )]);
        let (mut fixture, _cancel_tokens) =
            build_reload_fixture(&initial, &["stubborn".to_string()]);

        // Each flow contributes 2 tasks (poll + dispatcher) to the
        // kept_task_count math. Both ignore cancellation and just
        // sleep for a virtual year.
        for role in [FlowRole::Poll, FlowRole::Dispatcher] {
            fixture.join_set.spawn(async move {
                tokio::time::sleep(Duration::from_secs(60 * 60 * 24 * 365)).await;
                FlowExit {
                    flow: "stubborn".to_string(),
                    role,
                    panic: None,
                }
            });
        }

        // New config disables `stubborn` — reload classifier produces
        // Disable, the cancel-and-drain path runs and waits on the
        // two wedged tasks. A fully-empty new config is rejected by
        // validation ("at least one flow is required"), so we toggle
        // `enabled` instead.
        let new_cfg = cfg_with_defaults(vec![flow(
            "stubborn",
            "https://example.com/stubborn.git",
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

        assert!(
            logs_contain("reload drain timed out after 30s"),
            "drain-timeout warn must fire when JoinSet tasks ignore cancel",
        );
    }
}

// Per-flow spawn machinery. `spawn_initial_flows` is called from
// boot; `spawn_flow` is called from boot, panic-respawn, and
// config-reload — all three route through the same wiring so a
// respawn is an exact replica of the boot pair (poll + dispatcher
// tasks under the same per-flow cancel token, with notifiers built
// fresh from the current config).
//
// Panics in the spawned tasks are caught via
// `AssertUnwindSafe(...).catch_unwind()` so the FlowExit on the
// JoinSet preserves the flow name even on panic.
//
// Module layout (refactored from the prior single-file `flows.rs`):
//   - `mod.rs`     — `SpawnContext`, factory type aliases,
//                    `production_*_factory`, `spawn_initial_flows`,
//                    `spawn_flow` orchestrator.
//   - `notifiers`  — `build_notifiers` (destination → notifier vec).
//   - `state`      — state-mirror readers (`read_persisted_last_sha`,
//                    `read_persisted_last_dispatched_at`) and
//                    `panic_payload_to_string`.

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};

use chrono::{DateTime, Utc};
use futures_util::FutureExt;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{ActionConfig, Config, FlowConfig};
use crate::git::rate_bucket::RateBucket;
use crate::state::{State, StateUpdate};

use super::credentials::CredentialPool;
use super::types::{
    record_last_error, FlowExit, FlowHandle, FlowLastError, FlowRegistry, FlowRole,
};
use crate::flow::dispatcher::FlowDispatchParams;
use crate::flow::poll::{EffectivePoll, PollParams};
use crate::flow::{TriggerSignal, TRIGGER_QUEUE};

mod notifiers;
mod state;

use notifiers::build_notifiers;
use state::{panic_payload_to_string, read_persisted_last_dispatched_at, read_persisted_last_sha};

/// Builds the per-flow poll task's future. Production wraps
/// `crate::flow::poll::run`; integration tests inject a closure that
/// delegates to `poll::run_with_executor` with a scripted executor.
/// `Arc<dyn Fn>` so `SpawnContext` is `Clone` without propagating a
/// type parameter through every call site.
///
/// Panics raised inside the returned future are caught by
/// `AssertUnwindSafe(...).catch_unwind()` in `spawn_flow`. Synchronous
/// panics raised in the factory body BEFORE the future is returned
/// escape that wrapper (the `AssertUnwindSafe` arg is evaluated first)
/// and propagate to the JoinSet via `JoinError::is_panic`, surfacing
/// as `UnexpectedJoinError` in `handle_flow_exit`.
pub type PollTaskFactory = Arc<
    dyn Fn(
            PollParams,
            Option<gix_hash::ObjectId>,
            Option<DateTime<Utc>>,
            Sender<StateUpdate>,
            Sender<TriggerSignal>,
            CancellationToken,
        ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

/// Builds the per-flow dispatcher task's future. Production wraps
/// `crate::flow::dispatcher::run`; tests delegate to
/// `dispatcher::run_with_executor` with a scripted executor. Mirrors
/// `PollTaskFactory`'s shape and lifetime contract.
pub type DispatchTaskFactory = Arc<
    dyn Fn(
            FlowDispatchParams,
            Receiver<TriggerSignal>,
            Sender<StateUpdate>,
            Arc<Mutex<BTreeMap<String, FlowLastError>>>,
            CancellationToken,
        ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

/// Bundle every cross-cutting input the spawn path needs: shared
/// transports, per-flow trackers, and the task-building factories.
///
/// Factory closures persist across the daemon lifetime — every spawn
/// callsite (boot via `spawn_initial_flows`, SIGHUP via `run_reload`,
/// panic respawn via `handle_respawn_request`, control commands via
/// `handle_control_command`) uses the SAME factory instances the
/// daemon was started with. Integration tests rely on this identity
/// invariant: a scripted factory installed at boot is the closure
/// that runs for every subsequent reload/respawn, so a single
/// `Arc<RecordingExecutor>` captured by the closure observes every
/// generation's events.
///
/// Cloning is cheap (every field is `Arc` / `CancellationToken::clone`)
/// and derived for test orchestration; production paths borrow via
/// `&SpawnContext`.
#[derive(Clone)]
pub struct SpawnContext {
    /// Per-credential resource pool (octocrab client + rate buckets +
    /// rate-limit poller). Shared across flows that name the same
    /// `credential_id` so SIGHUP doesn't re-issue `/rate_limit`.
    pub(super) credential_pool: Arc<RwLock<CredentialPool>>,
    /// Shared `reqwest::Client` built once at boot from
    /// `config.http.request_timeout`. Used by the grokmirror polling
    /// strategy and the credential pool's HTTP probe.
    pub(super) shared_reqwest: Arc<reqwest::Client>,
    /// Hostname read once at boot via `mail::read_hostname_or_default`.
    /// Used as the `From:` host in `LocalMailNotifier` rendering.
    pub(super) hostname: Arc<String>,
    /// State-writer mpsc sender. Cloned per spawn so reload / respawn
    /// can hand fresh clones to new generations.
    pub(super) state_tx: mpsc::Sender<StateUpdate>,
    /// Live `State` clone refreshed after every successful disk
    /// persist. Read to seed the per-flow poll loop's in-memory
    /// baseline on spawn.
    pub(super) state_mirror: Arc<StdMutex<State>>,
    /// Daemon-shutdown cancellation root. Each per-flow spawn derives
    /// a `child_token()` so flow cancel during reload only severs the
    /// per-flow subtree without firing the daemon's shutdown sequence.
    pub(super) root_cancel: CancellationToken,
    /// Per-flow last-error map keyed by flow name. Updated by
    /// `record_last_error` from spawn-time setup failures and by the
    /// per-flow tasks themselves; surfaced via `gcit status`.
    pub(super) last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>>,
    /// Builds the per-flow poll task's future.
    pub(super) poll_task_factory: PollTaskFactory,
    /// Builds the per-flow dispatcher task's future.
    pub(super) dispatch_task_factory: DispatchTaskFactory,
}

/// Production `PollTaskFactory`. Boxed once per spawn; the per-cycle
/// hot path never sees this allocation.
#[doc(hidden)]
pub fn production_poll_task_factory() -> PollTaskFactory {
    Arc::new(
        |params, last_sha, last_dispatched_at, state_tx, trigger_tx, cancel| {
            Box::pin(crate::flow::poll::run(
                params,
                last_sha,
                last_dispatched_at,
                state_tx,
                trigger_tx,
                cancel,
            ))
        },
    )
}

/// Production `DispatchTaskFactory`. Mirrors
/// `production_poll_task_factory`.
#[doc(hidden)]
pub fn production_dispatch_task_factory() -> DispatchTaskFactory {
    Arc::new(|params, trigger_rx, state_tx, last_errors, cancel| {
        Box::pin(crate::flow::dispatcher::run(
            params,
            trigger_rx,
            state_tx,
            last_errors,
            cancel,
        ))
    })
}

/// Spawn the initial per-flow task set from a freshly-loaded config.
pub(super) async fn spawn_initial_flows(
    config: &Config,
    ctx: &SpawnContext,
    join_set: &mut JoinSet<FlowExit>,
    registry: &mut FlowRegistry,
) {
    for flow in &config.flow {
        if !flow.enabled {
            info!(
                target: "gcit::supervisor",
                flow = %flow.name,
                "flow disabled; skipping",
            );
            continue;
        }
        spawn_flow(flow, config, ctx, join_set, registry).await;
    }
}

/// Spawn one flow's poll + dispatcher tasks. Records the flow's
/// `FlowHandle` in `registry.handles` for reload-time lookup. Seeds
/// the poll loop's in-memory `last_sha` and `last_dispatched_at`
/// from `ctx.state_mirror` so a daemon restart does not fire a
/// spurious dispatch or skip the cooldown clock.
///
/// The two spawned futures are produced by `ctx.poll_task_factory` and
/// `ctx.dispatch_task_factory`. Both are wrapped in
/// `AssertUnwindSafe(...).catch_unwind()` so the `FlowExit` on the
/// JoinSet preserves the flow name even on panic.
pub(super) async fn spawn_flow(
    flow: &FlowConfig,
    config: &Config,
    ctx: &SpawnContext,
    join_set: &mut JoinSet<FlowExit>,
    registry: &mut FlowRegistry,
) {
    let flow_cancel = ctx.root_cancel.child_token();
    let (trigger_tx, trigger_rx) = mpsc::channel::<TriggerSignal>(TRIGGER_QUEUE);

    let action_credential_id = match &flow.action {
        ActionConfig::GithubWorkflowDispatch { credential_id, .. } => credential_id.clone(),
    };

    let notifiers = match build_notifiers(
        flow,
        &ctx.credential_pool,
        &ctx.hostname,
        config.http.request_timeout,
    )
    .await
    {
        Ok(n) => n,
        Err(e) => {
            // Prefix with the flow name so `gcit status` / journald
            // show which flow is blocked when several share a credential.
            return record_setup_failure_and_skip(ctx, &flow.name, "notifier_setup", &e).await;
        }
    };

    // Acquire (or build) the GitHub credential machinery. The pool
    // owns the rate-limit poller's CancellationToken (child of root
    // cancel) so daemon shutdown unwinds it.
    let cred_resources = match ctx
        .credential_pool
        .write()
        .await
        .acquire_github(
            &action_credential_id,
            config.http.request_timeout,
            Arc::clone(&ctx.shared_reqwest),
            ctx.root_cancel.clone(),
        )
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return record_setup_failure_and_skip(ctx, &flow.name, "credential", &e.to_string())
                .await;
        }
    };

    let strategy = crate::git::auto_detect(&flow.source.url);
    let effective = EffectivePoll::compute(&config.poll, &flow.poll, strategy);

    // Source-side rate bucket: deferred to v1. Per-flow jitter paces
    // the source side.
    let source_rate_bucket: Option<Arc<RateBucket>> = None;

    let initial_last_sha = read_persisted_last_sha(&ctx.state_mirror, &flow.name);
    let initial_last_dispatched_at =
        read_persisted_last_dispatched_at(&ctx.state_mirror, &flow.name);

    let poll_params = PollParams {
        flow_name: flow.name.clone(),
        url: flow.source.url.clone(),
        ref_name: flow.source.ref_name.clone(),
        effective_poll: effective,
        rate_bucket: source_rate_bucket,
        octo: Some(cred_resources.octocrab.clone()),
        reqwest: Some(cred_resources.reqwest.clone()),
        last_errors: Arc::clone(&ctx.last_errors),
    };
    let poll_state_tx = ctx.state_tx.clone();
    let poll_trigger_tx = trigger_tx.clone();
    let poll_cancel = flow_cancel.clone();
    let poll_flow = flow.name.clone();
    let poll_factory = Arc::clone(&ctx.poll_task_factory);
    join_set.spawn(async move {
        let result = AssertUnwindSafe((poll_factory)(
            poll_params,
            initial_last_sha,
            initial_last_dispatched_at,
            poll_state_tx,
            poll_trigger_tx,
            poll_cancel,
        ))
        .catch_unwind()
        .await;
        flow_exit_from_result(result, poll_flow, FlowRole::Poll)
    });

    let dispatcher_params = FlowDispatchParams {
        flow_name: flow.name.clone(),
        flow_description: flow.description.clone(),
        url: flow.source.url.clone(),
        ref_name: flow.source.ref_name.clone(),
        action: flow.action.clone(),
        destinations: flow.destination.clone(),
        github_client: Arc::clone(&cred_resources.github_client),
        rate_bucket: Arc::clone(&cred_resources.rate_bucket),
        rate_limit: Arc::clone(&cred_resources.rate_limit),
        job_interval: effective.job_interval,
        notifiers,
    };
    let dispatcher_state_tx = ctx.state_tx.clone();
    let dispatcher_cancel = flow_cancel.clone();
    let dispatcher_flow = flow.name.clone();
    let dispatcher_last_errors = Arc::clone(&ctx.last_errors);
    let dispatch_factory = Arc::clone(&ctx.dispatch_task_factory);
    join_set.spawn(async move {
        let result = AssertUnwindSafe((dispatch_factory)(
            dispatcher_params,
            trigger_rx,
            dispatcher_state_tx,
            dispatcher_last_errors,
            dispatcher_cancel,
        ))
        .catch_unwind()
        .await;
        flow_exit_from_result(result, dispatcher_flow, FlowRole::Dispatcher)
    });

    registry.handles.insert(
        flow.name.clone(),
        FlowHandle {
            cancel: flow_cancel,
            trigger_tx,
        },
    );
    // Track the new pair (poll + dispatcher) so
    // `handle_respawn_request` can gate the next respawn on both
    // old-gen tasks exiting the JoinSet. The count accumulates
    // across overlapping generations: an old-gen pair contributing 2
    // plus a fresh respawn contributing 2 sums to 4, and the
    // pending-exits gate only opens once all 4 exits land.
    *registry.pending_exits.entry(flow.name.clone()).or_insert(0) += 2;
    info!(
        target: "gcit::supervisor",
        flow = %flow.name,
        "flow spawned",
    );
}

/// Record a spawn-time setup failure (credential or notifier_setup)
/// with a flow-name-prefixed message and log a warn. Shared by both
/// early-return paths so the message shape stays consistent.
async fn record_setup_failure_and_skip(
    ctx: &SpawnContext,
    flow: &str,
    kind: &'static str,
    err: &str,
) {
    let with_flow = format!("flow `{flow}`: {err}");
    record_last_error(&ctx.last_errors, flow, kind, &with_flow, None).await;
    warn!(
        target: "gcit::supervisor",
        flow = %flow,
        error = %err,
        "{kind} failed; flow disabled until reload fixes it",
    );
}

/// Build a `FlowExit` from a `catch_unwind` result. Centralizes the
/// Ok→panic:None / Err→panic:Some(stringified) mapping shared by the
/// poll and dispatcher spawn arms.
fn flow_exit_from_result(
    result: Result<(), Box<dyn std::any::Any + Send>>,
    flow: String,
    role: FlowRole,
) -> FlowExit {
    match result {
        Ok(()) => FlowExit {
            flow,
            role,
            panic: None,
        },
        Err(payload) => FlowExit {
            flow,
            role,
            panic: Some(panic_payload_to_string(payload)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CredentialId, Destination, FireEvent, HttpConfig, LocalMailConfig, LocalMailTemplateConfig,
        LogConfig, PollDefaults, PollOverride, SourceConfig,
    };
    use crate::flow::supervisor::types::FlowRegistry;
    use crate::flow::supervisor::FlowLastError;

    fn flow_no_destinations(name: &str) -> FlowConfig {
        FlowConfig {
            name: name.to_string(),
            enabled: true,
            description: None,
            source: SourceConfig {
                url: "https://github.com/myorg/linux-builder.git".to_string(),
                ref_name: "refs/heads/main".to_string(),
                credential_id: None,
            },
            action: ActionConfig::GithubWorkflowDispatch {
                repo: "myorg/linux-builder".to_string(),
                workflow: "ci.yml".to_string(),
                ref_name: "refs/heads/main".to_string(),
                credential_id: CredentialId::new("github_pat").expect("valid id"),
                inputs: BTreeMap::new(),
            },
            destination: vec![],
            poll: PollOverride::default(),
        }
    }

    /// Build a minimal `Config` wrapping the supplied flow.
    /// spawn_flow's early-return tests exit before any non-default
    /// http/poll fields are consulted.
    fn config_wrapping(flow: FlowConfig) -> Config {
        Config {
            source_path: std::path::PathBuf::new(),
            poll: PollDefaults::default(),
            log: LogConfig::default(),
            http: HttpConfig::default(),
            flow: vec![flow],
            credential_lines: BTreeMap::new(),
        }
    }

    /// SpawnContext with panic-if-invoked factories. Tests using this
    /// helper must exercise an early-return arm — a factory invocation
    /// surfaces as a loud panic.
    fn spawn_context_for_failure_test(
        last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>>,
    ) -> SpawnContext {
        let (state_tx, _state_rx) = mpsc::channel(8);
        SpawnContext {
            credential_pool: Arc::new(RwLock::new(CredentialPool::default())),
            shared_reqwest: Arc::new(reqwest::Client::new()),
            hostname: Arc::new("ci-host".to_string()),
            state_tx,
            state_mirror: Arc::new(StdMutex::new(State::default())),
            root_cancel: CancellationToken::new(),
            last_errors,
            poll_task_factory: Arc::new(|_, _, _, _, _, _| {
                panic!(
                    "poll factory invoked unexpectedly: \
                     test exercises an early-return arm of spawn_flow"
                )
            }),
            dispatch_task_factory: Arc::new(|_, _, _, _, _| {
                panic!(
                    "dispatch factory invoked unexpectedly: \
                     test exercises an early-return arm of spawn_flow"
                )
            }),
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn spawn_flow_credential_resolution_failure_records_credential_kind_last_error_and_skips_spawn(
    ) {
        // CREDENTIALS_DIRECTORY scrubbed, env var scrubbed, no
        // config_dir → resolve_secret falls through to the step-4
        // not-found error. acquire_github wraps it; spawn_flow records
        // last_error.kind="credential" and returns BEFORE spawning.
        let credential_id = CredentialId::new("nonexistent-action-cred").expect("valid id");
        // SAFETY: gated by #[serial]; single-threaded env mutation.
        unsafe {
            std::env::remove_var("CREDENTIALS_DIRECTORY");
            std::env::remove_var(credential_id.to_env_var());
        }

        let mut flow = flow_no_destinations("flow-bad-cred");
        let ActionConfig::GithubWorkflowDispatch {
            credential_id: ref mut id,
            ..
        } = &mut flow.action;
        *id = credential_id.clone();

        let config = config_wrapping(flow.clone());
        let last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>> =
            Arc::new(Mutex::new(BTreeMap::new()));
        let ctx = spawn_context_for_failure_test(Arc::clone(&last_errors));
        let mut join_set: JoinSet<FlowExit> = JoinSet::new();
        let mut registry = FlowRegistry::new();

        spawn_flow(&flow, &config, &ctx, &mut join_set, &mut registry).await;

        assert_eq!(join_set.len(), 0, "no tasks spawned");
        assert!(!registry.handles.contains_key("flow-bad-cred"));
        assert!(!registry.pending_exits.contains_key("flow-bad-cred"));
        let errs = last_errors.lock().await;
        let entry = errs
            .get("flow-bad-cred")
            .expect("credential failure must record a last_error");
        assert_eq!(entry.kind(), "credential");
        assert!(
            entry.message().starts_with("flow `flow-bad-cred`:"),
            "credential failure message must lead with the flow-name prefix; got: {}",
            entry.message(),
        );
    }

    #[tokio::test]
    async fn spawn_flow_notifier_build_failure_records_notifier_setup_kind_last_error_and_skips_spawn(
    ) {
        // LocalMailNotifier::new rejects "bad/user" via ContainsSlash.
        // spawn_flow records last_error.kind="notifier_setup" and
        // returns BEFORE calling acquire_github.
        let mut flow = flow_no_destinations("flow-bad-notifier");
        flow.destination
            .push(Destination::LocalMail(LocalMailConfig {
                user: "bad/user".to_string(),
                fire_on: vec![FireEvent::RunComplete],
                template: LocalMailTemplateConfig::default(),
            }));

        let config = config_wrapping(flow.clone());
        let last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>> =
            Arc::new(Mutex::new(BTreeMap::new()));
        let ctx = spawn_context_for_failure_test(Arc::clone(&last_errors));
        let mut join_set: JoinSet<FlowExit> = JoinSet::new();
        let mut registry = FlowRegistry::new();

        spawn_flow(&flow, &config, &ctx, &mut join_set, &mut registry).await;

        assert_eq!(join_set.len(), 0);
        assert!(!registry.handles.contains_key("flow-bad-notifier"));
        assert!(!registry.pending_exits.contains_key("flow-bad-notifier"));
        let errs = last_errors.lock().await;
        let entry = errs
            .get("flow-bad-notifier")
            .expect("notifier-setup failure must record a last_error");
        assert_eq!(entry.kind(), "notifier_setup");
        let message = entry.message();
        assert!(
            message.starts_with("flow `flow-bad-notifier`:"),
            "notifier-setup message must lead with the flow-name prefix; got: {message}",
        );
        assert!(
            message.contains("local_mail user:"),
            "notifier-setup message must surface the inner 'local_mail user:' prefix; got: {message}",
        );
    }
}

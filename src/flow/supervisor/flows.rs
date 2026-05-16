// Per-flow spawn machinery. `spawn_initial_flows` is called from boot;
// `spawn_flow` is called from boot, panic-respawn, and config-reload.
// Both routes through the same wiring so a respawn produces an exact
// replica of the boot pair (poll + dispatcher tasks under the same
// per-flow CancellationToken, with notifiers built fresh from the
// current config).
//
// Panics in the spawned tasks are caught via
// `AssertUnwindSafe(...).catch_unwind()` so the FlowExit on the
// JoinSet preserves the flow name even when the inner future
// panicked. `panic_payload_to_string` extracts a printable message
// from the panic payload for the operator-facing last_error map.
//
// `build_notifiers` translates the destination list into the
// `Vec<Arc<dyn DynNotifier>>` the dispatcher's fan-out path consumes.
// `read_persisted_last_sha` seeds the per-flow poll loop's in-memory
// baseline from state.json so a daemon restart against an unchanged
// source does not fire a spurious TriggerSignal on cycle 1.
//
// `SpawnContext` bundles every cross-cutting input the per-flow spawn
// path needs: shared transports, per-flow tracking, and the
// `PollTaskFactory` / `DispatchTaskFactory` closures that build each
// per-flow task's future. Production wires `crate::flow::poll::run` and
// `crate::flow::dispatcher::run` (which build their own
// `Real{Poll,Dispatch}Executor` internally); the supervisor end-to-end
// test harness wires factories that delegate to
// `flow::{poll,dispatcher}::run_with_executor` with scripted executors,
// so integration tests can drive the FULL supervisor select! loop
// (signals, JoinSet, registry mutations, last_errors, control commands)
// without touching the network.

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::FutureExt;
use secrecy::ExposeSecret;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{ActionConfig, Config, Destination, FlowConfig};
use crate::discord::{self, DiscordNotifier};
use crate::git::rate_bucket::RateBucket;
use crate::mail::LocalMailNotifier;
use crate::state::{State, StateUpdate};

use super::credentials::CredentialPool;
use super::types::{
    record_last_error, FlowExit, FlowHandle, FlowLastError, FlowRegistry, FlowRole,
};
use crate::flow::dispatcher::{DynNotifier, FlowDispatchParams};
use crate::flow::poll::{EffectivePoll, PollParams};
use crate::flow::{TriggerSignal, TRIGGER_QUEUE};

/// Builds the per-flow poll task's future given the assembled
/// `PollParams` and channel handles. Production: returns
/// `crate::flow::poll::run(...)`. Tests: returns
/// `crate::flow::poll::run_with_executor(..., scripted_executor, ...)`.
/// The returned future is boxed via `Box::pin` (allocated once per
/// spawn; the per-cycle hot path never sees this allocation).
///
/// `Arc<dyn Fn>` rather than a generic `F: Fn` so `SpawnContext` is
/// `Clone` (needed for the panic-respawn path that re-spawns the same
/// flow) without propagating an extra type parameter through every
/// call site.
///
/// Panics raised inside the returned future are caught by
/// `AssertUnwindSafe(...).catch_unwind()` in `spawn_flow` so the
/// `FlowExit` on the JoinSet preserves the flow name on panic.
/// Synchronous panics raised in the factory closure body BEFORE the
/// future is returned escape that wrapper (the `AssertUnwindSafe`
/// argument is evaluated first, then wrapped); they propagate to the
/// JoinSet via `JoinError::is_panic` instead, surfacing as
/// `UnexpectedJoinError` in `handle_flow_exit`.
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

/// Builds the per-flow dispatcher task's future. Production: delegates
/// to `crate::flow::dispatcher::run(...)`. Tests: delegates to
/// `crate::flow::dispatcher::run_with_executor(..., scripted_executor, ...)`.
/// The returned future is boxed via `Box::pin` (allocated once per
/// spawn, mirroring `PollTaskFactory`).
///
/// The trigger `Receiver<TriggerSignal>` is taken by value (not `mut`
/// in the type alias because closures bind their parameters as
/// patterns); the receiving function declares `mut trigger_rx` to
/// drive `trigger_rx.recv()`. Production wrapper at
/// `production_dispatch_task_factory` and the test scripted factories
/// both forward the receiver into `dispatcher::run` /
/// `dispatcher::run_with_executor`, which take `mut trigger_rx`
/// internally.
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
/// callsite (boot via `spawn_initial_flows`, SIGHUP reload via
/// `run_reload`, panic respawn via `handle_respawn_request`, and
/// control-channel commands via `handle_control_command`) uses the
/// SAME factory instances the daemon was started with. Integration
/// tests rely on this identity invariant: a scripted factory
/// installed at boot is the closure that runs for every subsequent
/// reload/respawn, so a single `Arc<RecordingExecutor>` captured by
/// the closure observes every generation's events. Cloning a
/// `SpawnContext` is cheap (every field is `Arc` /
/// `CancellationToken::clone`) and derived for test orchestration;
/// production paths borrow via `&SpawnContext`. Per-spawn state
/// (channels, tokens, params) must be constructed inside the
/// closure body — the closure itself is reused.
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
    /// can hand fresh clones to new generations; every clone must
    /// drop before the channel signals disconnect to the writer.
    pub(super) state_tx: mpsc::Sender<StateUpdate>,
    /// Live `State` clone refreshed after every successful disk
    /// persist. Read by `read_persisted_last_sha` to seed the per-flow
    /// poll loop's in-memory baseline on spawn.
    pub(super) state_mirror: Arc<StdMutex<State>>,
    /// Daemon-shutdown cancellation root. Each per-flow spawn derives
    /// a `child_token()` so flow cancel during reload only severs the
    /// per-flow subtree without firing the daemon's shutdown sequence.
    pub(super) root_cancel: CancellationToken,
    /// Per-flow last-error map keyed by flow name. Updated by
    /// `record_last_error` from spawn-time setup failures and by the
    /// per-flow tasks themselves; surfaced via `gcit status`.
    pub(super) last_errors: Arc<Mutex<BTreeMap<String, FlowLastError>>>,
    /// Builds the per-flow poll task's future. Production wraps
    /// `crate::flow::poll::run`; integration tests inject a closure
    /// that delegates to `poll::run_with_executor` with a scripted
    /// `PollExecutor`.
    pub(super) poll_task_factory: PollTaskFactory,
    /// Builds the per-flow dispatcher task's future. Production wraps
    /// `crate::flow::dispatcher::run`; integration tests inject a
    /// closure that delegates to `dispatcher::run_with_executor` with
    /// a scripted `DispatchExecutor`.
    pub(super) dispatch_task_factory: DispatchTaskFactory,
}

/// Production `PollTaskFactory` — wraps `crate::flow::poll::run`,
/// which constructs a `RealPollExecutor` internally (boxed via
/// `Box::pin`). Boxed once per spawn; the per-cycle hot path never
/// sees this allocation.
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

/// Production `DispatchTaskFactory` — wraps
/// `crate::flow::dispatcher::run`, which constructs a
/// `RealDispatchExecutor` internally (boxed via `Box::pin`). Mirrors
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

/// Read the persisted `last_sha` for `flow_name` from the state
/// mirror. Used by `spawn_flow` and `handle_respawn_request` to seed
/// the per-flow poll loop's in-memory `last_sha` so a daemon restart
/// does not spuriously dispatch on the first poll cycle. Returns
/// `None` when the state mirror has no observation for this flow yet,
/// when the stored hex is malformed, or when the mutex is poisoned
/// (defense in depth — a poisoned mutex would also make the writer
/// thread fail loudly elsewhere).
fn read_persisted_last_sha(
    state_mirror: &Arc<StdMutex<State>>,
    flow_name: &str,
) -> Option<gix_hash::ObjectId> {
    let guard = state_mirror.lock().ok()?;
    let entry = guard.flows.get(flow_name)?;
    let hex = entry.last_sha.as_ref()?;
    gix_hash::ObjectId::from_hex(hex.as_bytes()).ok()
}

/// Read the persisted `last_dispatched_at` for `flow_name` from the
/// state mirror. Used by `spawn_flow` to seed the per-flow poll loop's
/// in-memory cooldown clock so a daemon restart inherits the prior
/// cooldown window. Returns `None` when the state mirror has no
/// observation for this flow yet, the field was never set, or the
/// mutex is poisoned.
fn read_persisted_last_dispatched_at(
    state_mirror: &Arc<StdMutex<State>>,
    flow_name: &str,
) -> Option<DateTime<Utc>> {
    let guard = state_mirror.lock().ok()?;
    let entry = guard.flows.get(flow_name)?;
    entry.last_dispatched_at
}

/// Spawn one flow's poll + dispatcher tasks. Records the flow's
/// `FlowHandle` in `handles` for reload-time lookup.
///
/// `ctx.state_mirror` is consulted (read-only) to seed the per-flow
/// poll loop's in-memory `last_sha` from the persisted state. Without
/// this, a daemon restart would fire a spurious `TriggerSignal` on the
/// first poll cycle for every flow because the in-memory baseline
/// would be `None` while the source's actual SHA matches the stored
/// one.
///
/// The two spawned futures are produced by `ctx.poll_task_factory` and
/// `ctx.dispatch_task_factory`. Production wires factories that build
/// `crate::flow::poll::run` / `crate::flow::dispatcher::run`
/// (delegating internally to `Real{Poll,Dispatch}Executor`); the
/// supervisor end-to-end test harness wires factories that delegate to
/// the `*::run_with_executor` seams with scripted executors. Both
/// factory invocations are wrapped in
/// `AssertUnwindSafe(...).catch_unwind()` so the `FlowExit` on the
/// `JoinSet` preserves the flow name even on panic.
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

    // Build notifiers for every destination.
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
            // Prefix the operator-visible last_error with the flow name
            // so `gcit status` / journald show which flow is blocked
            // when several share a credential.
            let with_flow = format!("flow `{}`: {}", flow.name, e);
            record_last_error(
                &ctx.last_errors,
                &flow.name,
                "notifier_setup",
                &with_flow,
                None,
            )
            .await;
            warn!(
                target: "gcit::supervisor",
                flow = %flow.name,
                error = %e,
                "notifier setup failed; flow disabled until reload fixes it",
            );
            return;
        }
    };

    // Acquire (or build) the GitHub credential machinery. The pool
    // owns the rate-limit poller's CancellationToken — child of the
    // root cancel so daemon shutdown unwinds the poller.
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
            // Prefix the operator-visible last_error with the flow
            // name. The pool's `resolve_secret` only knows the
            // credential id, but a single bad credential can block
            // many flows — surfacing the flow name here makes it
            // unambiguous which flow is currently disabled in
            // `gcit status` / journald.
            let with_flow = format!("flow `{}`: {}", flow.name, e);
            record_last_error(&ctx.last_errors, &flow.name, "credential", &with_flow, None).await;
            warn!(
                target: "gcit::supervisor",
                flow = %flow.name,
                error = %e,
                "credential resolution failed; flow disabled until reload fixes it",
            );
            return;
        }
    };

    // Effective poll cadence (already validated, just overlay).
    // EffectivePoll carries job_interval too.
    let strategy = crate::git::auto_detect(&flow.source.url);
    let effective = EffectivePoll::compute(&config.poll, &flow.poll, strategy);

    // Source-side rate bucket: a dedicated bucket keyed by source URL
    // host (one per remote git server). Skipping for v1 — pace via
    // the per-flow loop's own jitter.
    let source_rate_bucket: Option<Arc<RateBucket>> = None;

    // Read the persisted last_sha from the state mirror. The poll
    // loop uses this as its in-memory baseline so a daemon restart
    // against an unchanged source does not fire a spurious
    // TriggerSignal on cycle 1.
    let initial_last_sha = read_persisted_last_sha(&ctx.state_mirror, &flow.name);
    // Read the persisted last_dispatched_at so the cooldown clock
    // survives a daemon restart. Without this, every flow's first
    // post-restart SHA-diff would dispatch immediately even if the
    // pre-restart dispatch landed seconds ago.
    let initial_last_dispatched_at =
        read_persisted_last_dispatched_at(&ctx.state_mirror, &flow.name);

    // Spawn poll task wrapped in catch_unwind so the JoinSet exit
    // carries the flow name even on panic.
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

    // Spawn dispatcher task with the same catch_unwind wrapping.
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
    // Track the new pair (poll + dispatcher) so handle_respawn_request
    // can gate the next respawn on both old-gen tasks having exited
    // the JoinSet — without this, a wedged old-gen sibling can race
    // the new gen's state_tx clone on the writer mpsc.
    *registry.pending_exits.entry(flow.name.clone()).or_insert(0) += 2;
    info!(
        target: "gcit::supervisor",
        flow = %flow.name,
        "flow spawned",
    );
}

/// Best-effort downcast of a panic payload to a printable string.
/// Panics may carry `&'static str`, `String`, or arbitrary types; we
/// recover the first two and fall back to a generic placeholder
/// otherwise so the operator-facing `last_error` always has SOMETHING.
fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "(panic payload not stringifiable)".to_string()
    }
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

/// Build the per-flow notifier vector from the destination list.
/// Discord destinations construct a `DiscordNotifier`; local_mail
/// destinations construct a `LocalMailNotifier`. Failures (e.g. a
/// webhook URL that fails parse) bubble up as a `String` for
/// last_error reporting.
async fn build_notifiers(
    flow: &FlowConfig,
    credential_pool: &Arc<RwLock<CredentialPool>>,
    hostname: &Arc<String>,
    http_request_timeout: Duration,
) -> Result<Vec<Arc<dyn DynNotifier>>, String> {
    let mut out: Vec<Arc<dyn DynNotifier>> = Vec::with_capacity(flow.destination.len());
    let hb = Arc::new(crate::notify::strict_handlebars());
    for (idx, dest) in flow.destination.iter().enumerate() {
        match dest {
            Destination::DiscordWebhook(w) => {
                let url_secret = credential_pool
                    .write()
                    .await
                    .resolve_secret(&w.credential_id)
                    .await
                    .map_err(|e| format!("discord credential: {e}"))?;
                let parsed = discord::parse_webhook_url(url_secret.expose_secret())
                    .map_err(|e| format!("discord webhook URL: {e}"))?;
                let client = discord::Client::new(http_request_timeout)
                    .map_err(|e| format!("discord client: {e}"))?;
                let n = DiscordNotifier::new(
                    format!("{}.dest{}", flow.name, idx),
                    client,
                    parsed,
                    w.fire_on.clone(),
                    w.template.clone(),
                    Arc::clone(&hb),
                );
                out.push(Arc::new(n));
            }
            Destination::LocalMail(m) => {
                let n = LocalMailNotifier::new(
                    format!("{}.dest{}", flow.name, idx),
                    m.user.clone(),
                    Arc::clone(hostname),
                    m.fire_on.clone(),
                    m.template.clone(),
                    Arc::clone(&hb),
                )
                .map_err(|e| format!("local_mail user: {e}"))?;
                out.push(Arc::new(n));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CredentialId, FireEvent, LocalMailConfig, LocalMailTemplateConfig, PollOverride,
        SourceConfig,
    };
    use crate::state::FlowState;

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

    #[test]
    fn panic_payload_to_string_recovers_static_str() {
        // `std::panic::panic_any` and `panic!("&'static str literal")`
        // both produce a payload Box that downcasts to `&'static str`
        // when the panic message was a literal string. Synthesize one
        // by boxing a `&'static str` directly — same shape the runtime
        // would deliver.
        let payload: Box<dyn std::any::Any + Send> = Box::new("static panic message");
        assert_eq!(
            panic_payload_to_string(payload),
            "static panic message".to_string(),
        );
    }

    #[test]
    fn panic_payload_to_string_recovers_owned_string() {
        // `panic!("owned: {}", x)` with format args produces a `String`
        // payload (the format machinery allocates). Cover both downcast
        // arms so a regression that breaks one doesn't survive on the
        // back of the other.
        let payload: Box<dyn std::any::Any + Send> = Box::new("owned panic message".to_string());
        assert_eq!(
            panic_payload_to_string(payload),
            "owned panic message".to_string(),
        );
    }

    #[test]
    fn panic_payload_to_string_falls_back_for_arbitrary_payload() {
        // panic_any with a non-stringy value (e.g. a struct, or in
        // this case a u64) lands in the catch-all arm. The fallback
        // string is operator-readable rather than empty so last_error
        // always has SOMETHING surfaced.
        let payload: Box<dyn std::any::Any + Send> = Box::new(42u64);
        assert_eq!(
            panic_payload_to_string(payload),
            "(panic payload not stringifiable)".to_string(),
        );
    }

    #[test]
    fn read_persisted_last_sha_returns_some_when_state_has_valid_hex() {
        let mut s = State::default();
        s.flows.insert(
            "flow1".to_string(),
            FlowState {
                last_sha: Some("deadbeefcafe1234567890abcdef1234567890ab".to_string()),
                ..Default::default()
            },
        );
        let mirror = Arc::new(StdMutex::new(s));
        let sha = read_persisted_last_sha(&mirror, "flow1").expect("must be Some");
        assert_eq!(sha.to_string(), "deadbeefcafe1234567890abcdef1234567890ab");
    }

    #[test]
    fn read_persisted_last_sha_returns_none_when_flow_absent() {
        let mirror = Arc::new(StdMutex::new(State::default()));
        assert!(read_persisted_last_sha(&mirror, "missing-flow").is_none());
    }

    #[test]
    fn read_persisted_last_sha_returns_none_when_flow_has_no_sha_field() {
        // FlowState exists (poll cycle landed a PollTimestamp without
        // a SHA observation, or hand-edited state.json) but `last_sha`
        // is None. The seed must be None in that case so the in-memory
        // baseline matches the on-disk record.
        let mut s = State::default();
        s.flows.insert(
            "flow1".to_string(),
            FlowState {
                last_sha: None,
                ..Default::default()
            },
        );
        let mirror = Arc::new(StdMutex::new(s));
        assert!(read_persisted_last_sha(&mirror, "flow1").is_none());
    }

    #[test]
    fn read_persisted_last_sha_returns_none_when_stored_hex_is_malformed() {
        // Defense-in-depth path: the writer guards against malformed
        // hex on its own input, but a hand-edited state.json could
        // still slip a non-hex string past the reader. read_persisted_last_sha
        // returns None (rather than erroring) so spawn_flow proceeds
        // without a baseline — the next poll observation will overwrite
        // the bad value.
        let mut s = State::default();
        s.flows.insert(
            "flow1".to_string(),
            FlowState {
                last_sha: Some("not-valid-hex".to_string()),
                ..Default::default()
            },
        );
        let mirror = Arc::new(StdMutex::new(s));
        assert!(read_persisted_last_sha(&mirror, "flow1").is_none());
    }

    #[tokio::test]
    async fn build_notifiers_returns_empty_vec_for_no_destinations() {
        let flow = flow_no_destinations("flow1");
        let pool = Arc::new(RwLock::new(CredentialPool::default()));
        let hostname = Arc::new("ci-host".to_string());
        let n = build_notifiers(&flow, &pool, &hostname, std::time::Duration::from_secs(30))
            .await
            .expect("build must succeed");
        assert!(
            n.is_empty(),
            "no destinations must yield empty notifier vec",
        );
    }

    #[tokio::test]
    async fn build_notifiers_constructs_local_mail_notifier_for_mail_destination() {
        let mut flow = flow_no_destinations("flow-mail");
        flow.destination
            .push(Destination::LocalMail(LocalMailConfig {
                user: "ci".to_string(),
                fire_on: vec![FireEvent::RunComplete],
                template: LocalMailTemplateConfig::default(),
            }));
        let pool = Arc::new(RwLock::new(CredentialPool::default()));
        let hostname = Arc::new("ci-host".to_string());
        let n = build_notifiers(&flow, &pool, &hostname, std::time::Duration::from_secs(30))
            .await
            .expect("build must succeed for local_mail-only flow");
        assert_eq!(
            n.len(),
            1,
            "single local_mail destination must produce one notifier",
        );
    }

    #[tokio::test]
    async fn build_notifiers_constructs_two_notifiers_for_two_local_mail_destinations() {
        // Multiple local_mail destinations on the same flow are
        // allowed (operators may want to fan out to multiple system
        // users). Each one becomes its own notifier with a distinct
        // `flow.destN` id derived from the index.
        let mut flow = flow_no_destinations("flow-multi");
        flow.destination
            .push(Destination::LocalMail(LocalMailConfig {
                user: "ci".to_string(),
                fire_on: vec![FireEvent::RunComplete],
                template: LocalMailTemplateConfig::default(),
            }));
        flow.destination
            .push(Destination::LocalMail(LocalMailConfig {
                user: "ops".to_string(),
                fire_on: vec![FireEvent::JobComplete],
                template: LocalMailTemplateConfig::default(),
            }));
        let pool = Arc::new(RwLock::new(CredentialPool::default()));
        let hostname = Arc::new("ci-host".to_string());
        let n = build_notifiers(&flow, &pool, &hostname, std::time::Duration::from_secs(30))
            .await
            .expect("build must succeed");
        assert_eq!(n.len(), 2);
    }

    #[tokio::test]
    async fn build_notifiers_propagates_local_mail_user_validation_failure() {
        // LocalMailNotifier::new rejects users containing nul, "..",
        // or "/" (see src/mail/notifier.rs::validate_user). build_notifiers
        // wraps that error with a "local_mail user: " prefix so the
        // operator-facing last_error names the failing path even when
        // several destinations share the flow.
        //
        // Use a "/" — the simplest sentinel that surfaces a clear
        // ContainsSlash error and exercises the same wrap site as
        // any other validation failure.
        let mut flow = flow_no_destinations("flow-bad-user");
        flow.destination
            .push(Destination::LocalMail(LocalMailConfig {
                user: "bad/user".to_string(),
                fire_on: vec![FireEvent::RunComplete],
                template: LocalMailTemplateConfig::default(),
            }));
        let pool = Arc::new(RwLock::new(CredentialPool::default()));
        let hostname = Arc::new("ci-host".to_string());
        // `Result<Vec<Arc<dyn DynNotifier>>, String>::expect_err` would
        // require Debug on the Ok variant; DynNotifier is dyn-erased
        // and not Debug. Match instead so the test stays terse.
        let err = match build_notifiers(&flow, &pool, &hostname, std::time::Duration::from_secs(30))
            .await
        {
            Ok(_) => panic!("invalid unix user must surface as Err"),
            Err(e) => e,
        };
        assert!(
            err.starts_with("local_mail user:"),
            "error must lead with the path-naming prefix; got: {err}",
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn build_notifiers_propagates_discord_credential_resolution_failure() {
        // The Discord arm of `build_notifiers` calls
        // `credential_pool.resolve_secret(...)` and wraps any failure
        // with "discord credential: ".
        // Construct a pool with an empty config_dir, scrub the env var
        // for the credential id, and add a Discord destination that
        // references that id — resolve_secret falls through every step
        // and returns a not-found error. build_notifiers wraps it; the
        // operator-facing last_error names the discord pipeline so a
        // single missing PAT for one of three destinations doesn't
        // hide which destination needs attention.
        use crate::config::DiscordTemplateConfig;
        let credential_id = CredentialId::new("nonexistent-discord-cred").expect("valid id");
        // SAFETY: serialized via #[serial]; single-threaded env mutation.
        unsafe {
            std::env::remove_var("CREDENTIALS_DIRECTORY");
            std::env::remove_var(credential_id.to_env_var());
        }
        let mut flow = flow_no_destinations("flow-bad-discord");
        flow.destination.push(Destination::DiscordWebhook(
            crate::config::DiscordWebhookConfig {
                credential_id: credential_id.clone(),
                fire_on: vec![FireEvent::RunStart],
                template: DiscordTemplateConfig::default(),
            },
        ));
        // Pool with no config_dir → step 3 skipped; with no env var and
        // no $CREDENTIALS_DIRECTORY → steps 1+2 also skipped; the
        // pool's resolve_secret falls through to the step-4 error.
        let pool = Arc::new(RwLock::new(CredentialPool::default()));
        let hostname = Arc::new("ci-host".to_string());
        let err = match build_notifiers(&flow, &pool, &hostname, std::time::Duration::from_secs(30))
            .await
        {
            Ok(_) => panic!("missing discord credential must surface as Err"),
            Err(e) => e,
        };
        assert!(
            err.starts_with("discord credential:"),
            "error must lead with the discord-credential prefix; got: {err}",
        );
        // Pin the BACKTICK-wrapped form `<id>` rather than just the
        // bare id — `resolve_secret`'s not-found arm wraps the id in
        // backticks (`credential `{id}` not found at ...`). Operators
        // grep journald for the wrapped form to find the single line
        // naming the missing credential; a regression that drops the
        // wrapping (or swaps quote style) breaks that workflow even
        // though a bare-id substring check would still pass.
        let backticked = format!("`{}`", credential_id.as_str());
        assert!(
            err.contains(&backticked),
            "error must name the credential id wrapped as {backticked}; got: {err}",
        );
    }

    // -----------------------------------------------------------------
    // spawn_flow side-effect tests for the early-return failure arms.
    //
    // The `build_notifiers` helper has direct unit tests above. These
    // tests exercise the FULL spawn_flow path so that the
    // record_last_error + early-return wiring is pinned end-to-end:
    // the registry stays empty (no FlowHandle inserted), the JoinSet
    // stays empty (no poll/dispatcher tasks spawned), the factories
    // are never invoked, and the operator-visible last_errors map
    // carries the expected `kind` discriminator.
    // -----------------------------------------------------------------

    use crate::config::{Config, HttpConfig, LogConfig, PollDefaults};
    use crate::flow::supervisor::types::FlowRegistry;
    use crate::flow::supervisor::FlowLastError;

    /// Build a minimal `Config` wrapping the supplied flow. spawn_flow
    /// reads `config.http.request_timeout` and `config.poll` — both
    /// default-derived values are fine for the early-return tests
    /// because they exit before any request_timeout / poll cadence is
    /// consulted.
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

    /// Build a SpawnContext with default `CredentialPool` (no cached
    /// entries, no config_dir) and panic-if-invoked factories. Tests
    /// using this helper must exercise an early-return arm — a factory
    /// invocation surfaces as a loud panic rather than a silent wrong-
    /// arm regression.
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
        // The action's credential_id resolves to nothing:
        // CREDENTIALS_DIRECTORY scrubbed, env var scrubbed,
        // CredentialPool::default() has no config_dir, so
        // resolve_secret falls through to the step-4 not-found error.
        // acquire_github wraps it; spawn_flow records last_error.kind=
        // "credential" with a "flow `<name>`:" prefix and returns
        // BEFORE spawning either task.
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

        // No tasks spawned (the factories would panic if invoked, so
        // an assertion is also covered by the panic-if-invoked guard).
        assert_eq!(
            join_set.len(),
            0,
            "credential failure path must NOT spawn poll/dispatcher tasks",
        );
        // No FlowHandle inserted.
        assert!(
            !registry.handles.contains_key("flow-bad-cred"),
            "credential failure path must NOT insert a FlowHandle",
        );
        // pending_exits unchanged.
        assert!(
            !registry.pending_exits.contains_key("flow-bad-cred"),
            "credential failure path must NOT touch pending_exits",
        );
        // last_error recorded with kind="credential" and the "flow `<name>`:" prefix.
        let errs = last_errors.lock().await;
        let entry = errs
            .get("flow-bad-cred")
            .expect("credential failure must record a last_error");
        assert_eq!(
            entry.kind(),
            "credential",
            "credential failure must record kind='credential'",
        );
        assert!(
            entry.message().starts_with("flow `flow-bad-cred`:"),
            "credential failure message must lead with the flow-name prefix; got: {}",
            entry.message(),
        );
    }

    #[tokio::test]
    async fn spawn_flow_notifier_build_failure_records_notifier_setup_kind_last_error_and_skips_spawn(
    ) {
        // build_notifiers fails when LocalMailNotifier::new rejects an
        // invalid unix user (a "/" in the user name surfaces
        // ContainsSlash from validate_user — see
        // build_notifiers_propagates_local_mail_user_validation_failure
        // above). spawn_flow records last_error.kind="notifier_setup"
        // with a "flow `<name>`:" prefix and returns BEFORE calling
        // acquire_github or spawning any task.
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

        assert_eq!(
            join_set.len(),
            0,
            "notifier-setup failure must NOT spawn poll/dispatcher tasks",
        );
        assert!(
            !registry.handles.contains_key("flow-bad-notifier"),
            "notifier-setup failure must NOT insert a FlowHandle",
        );
        assert!(
            !registry.pending_exits.contains_key("flow-bad-notifier"),
            "notifier-setup failure must NOT touch pending_exits",
        );
        let errs = last_errors.lock().await;
        let entry = errs
            .get("flow-bad-notifier")
            .expect("notifier-setup failure must record a last_error");
        assert_eq!(
            entry.kind(),
            "notifier_setup",
            "notifier-setup failure must record kind='notifier_setup'",
        );
        // The wrapped message carries the "local_mail user:" inner
        // prefix from build_notifiers, after the flow-name prefix.
        let message = entry.message();
        assert!(
            message.starts_with("flow `flow-bad-notifier`:"),
            "notifier-setup message must lead with the flow-name prefix; got: {message}",
        );
        assert!(
            message.contains("local_mail user:"),
            "notifier-setup message must surface the inner 'local_mail user:' prefix from build_notifiers; got: {message}",
        );
    }
}

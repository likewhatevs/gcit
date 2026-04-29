# Architecture overview

This chapter is operator-oriented context for what the daemon does at
runtime. It is not a design document. For the source itself, see the
modules under `src/`.

## Module layout

| module | responsibility |
|---|---|
| `cli` | every subcommand implementation (`check`, `install`, `uninstall`, `status`, `trigger`, `reload`, `validate-template`); exit codes |
| `config` | TOML parse, validation, credential id resolution |
| `control` | the Unix-socket control protocol (length-delimited JSON) |
| `discord` | Discord webhook notifier, embed builder, conclusion → color/label mapping |
| `flow` | per-flow task pipeline (poll → dispatch → monitor) and the daemon supervisor |
| `git` | three polling strategies (`github_api`, `grokmirror`, `ls_remote`), strategy auto-detection, jitter math, SHA-diff |
| `github` | `workflow_dispatch` send + correlator + run/job monitor |
| `log` | tracing init (journald + stderr layers) |
| `mail` | local-mail (mbox append) notifier |
| `notify` | shared `Notifier` trait, `RunContext`, error/outcome types, strict handlebars factory |
| `state` | persistent on-disk state (mpsc-driven, atomic-rename writes, schema-versioned) |
| `systemd` | unit-file rendering, install paths, daemon-reload via session bus |

The library is not a published API. Every `pub` item is crate-internal
and unstable; integration tests reach across the boundary so
`pub(crate)` does not span the test boundary.

## Supervisor lifecycle

The daemon entry is `gcit::flow::run_daemon`. The supervisor owns:

- A root `CancellationToken`. Each flow runs as a child task tree under
  `root.child_token()`. The supervisor's root cancels every child on
  shutdown; per-flow cancellation cancels only the named flow (used for
  config-reload removal of changed flows).
- A `JoinSet<...>` of per-flow tasks. Panics in a flow are caught
  inside the spawned future via
  `std::panic::AssertUnwindSafe(...).catch_unwind()` so the flow name
  is preserved on the JoinSet exit; `JoinError::is_panic` alone would
  discard the per-task identity.
- A `tokio::sync::watch<Arc<Config>>` so reload-aware components see the
  latest config without locking.
- The control-server accept loop (length-delimited JSON over the
  socket-activated Unix stream).
- Signal handlers for `SIGTERM`, `SIGINT`, and `SIGHUP`.
- `sd_notify` lifecycle (`READY=1` once the supervisor's `select!` loop
  is live; `STOPPING=1` on shutdown).

On `SIGHUP` the supervisor reloads the config, diffs each flow against
its previous shape, and:

- **Unchanged flows** keep their poll/dispatcher pair, credential
  resources, and rate-limit state — any in-flight run monitor stays
  attached and the per-credential rate-limit poller keeps refreshing
  without interruption.
- **Changed and removed flows** are cancelled cleanly. The new
  generation starts a fresh poll cycle on the next supervisor cycle.

Credential file rotation takes effect only when no kept-alive flow still
references the credential id. Restart the daemon (rather than reload)
when rotating because the old token was compromised. See [Credential
management — Rotation](./credentials.md#rotation).

Panicked flow tasks are respawned after a 30-second delay
(`RESPAWN_DELAY`). The constant is deliberately not configurable so the
value pushes the operator toward fixing the underlying bug rather than
tuning it away — a panic in a polling loop that respawns every second
would mask the real fault.

## Per-flow task tree

Each enabled flow runs three tasks under its own cancellation token:

| task | purpose |
|---|---|
| poll | runs the strategy auto-detect once, then loops: jitter the cadence, fetch the source, apply the SHA-diff, emit `TriggerSignal` on change, persist a `PollObservation` either way |
| dispatcher | reads `TriggerSignal` from an mpsc (capacity 8), renders dispatch inputs, calls `dispatch_with_retry`, runs the correlator, emits `RunStarted`, hands the resulting `CorrelationOutcome` to a per-run monitor task |
| monitor (per run) | wraps `monitor_run` in a task that drains `MonitorEvent::{Update, Done}` into the state writer + notifier dispatch. Awaited inside the supervisor's JoinSet so completion fan-outs run before shutdown finishes. |

The dispatcher's run-start fan-out is deliberately **not** awaited so a
slow notifier cannot delay monitor spawn. See [Notifiers — Run-start
delivery semantics](./notifiers.md#run-start-delivery-semantics) for the
shutdown implications.

## State persistence

The state-writer thread:

- Drains an mpsc channel (capacity 256, batched in groups of 64) of
  `StateUpdate` variants emitted by the per-flow tasks.
- Persists state via atomic-rename writes to
  `$STATE_DIRECTORY/state.json` (tempfile + write + `sync_all` +
  persist + parent dir fsync).
- Outlives the tokio runtime — runs on a dedicated OS thread so
  shutdown can flush after the runtime tears down.
- Refuses unknown schema versions. The on-disk format is schema-
  versioned (`schema: 1`); a state file from a future gcit version
  fails fast rather than silently dropping unfamiliar fields.

`StateUpdate` variants and their apply rules:

| variant | apply |
|---|---|
| `PollObservation` | LWW on `(last_sha, last_poll_at)` for the named flow. Creates the flow entry if it does not yet exist. |
| `PollTimestamp` | refresh `last_poll_at` only (used by strategies that prove a fast-path "no change" without a fresh ObjectId — e.g. grokmirror manifest fingerprint match). Never clears `last_sha`. |
| `RunStarted` | append to `flows[name].active_runs`. Multiple runs per flow are supported. |
| `RunFinished` | move the entry from `active_runs` to `notified_runs`, capped at 100 per flow. |
| `FlowRemoved` | drop in-memory state for the flow (used by reload to clean up removed flows). |

`apply` is a pure function of `(current state, update)` — no side
effects, no logging — so test skeletons under `tests/state_*.rs` can
drive every variant + interleaving combination deterministically.

## Control protocol

The control socket carries length-delimited JSON request/response
messages. The supported requests:

- `Reload` — equivalent to SIGHUP.
- `Status { flow }` — per-flow snapshot or all flows.
- `Trigger { flow, dry_run }` — manually fire a flow's dispatch path
  (or render its payload).

Each request carries a UUID id; the response echoes it. The CLI uses a
one-shot client per invocation. Request and response shapes are
crate-internal and unstable — do not script against the JSON wire format
without checking the source first.

## Single-instance lock

The daemon holds an exclusive `flock(2)` on
`$RUNTIME_DIRECTORY/gcit.lock` (tmpfs). A second `gcit run` against the
same runtime directory exits with a clear "another gcit is running"
error rather than racing for state-file writes or control-socket binds.

## Notifier fan-out

Each run-completion event spawns one task per configured notifier (via
`spawn_fan_out`). One notifier's failure never affects the others —
each task is independent. All notifier outcomes log under the single
tracing target `gcit::flow::notify` with structured `kind` / `id` /
`label` (and optional `job_id`) fields so an operator can filter for a
specific notifier without inspecting per-call-site targets.

The `on_run_complete` and per-job `on_job_complete` fan-outs ARE awaited
inside the per-run monitor task (which the supervisor joins), so those
deliveries either complete or surface their failure in the journal
before shutdown finishes. The `on_run_start` fan-out is NOT awaited; see
[Notifiers — Run-start delivery semantics](./notifiers.md#run-start-delivery-semantics).

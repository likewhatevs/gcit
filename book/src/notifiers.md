# Notifiers

A flow's destinations decide who hears about each run. Two notifier kinds
ship with v1: Discord webhook and local mail (mbox append). Each
destination is independent — one flow can have multiple destinations of
the same kind, and a slow or failing notifier does not block any other.

Both kinds share the same lifecycle hooks:

| hook | when |
|---|---|
| `on_run_start` | after a `workflow_dispatch` succeeds AND the resulting `Run.id` is correlated, before the per-run monitor is spawned |
| `on_job_complete` | once per job per run, in observation order |
| `on_run_complete` | exactly once when the run reaches a terminal status — the only event guaranteed to deliver a summary |

`fire_on` selects which hooks fire for this destination. A hook whose
event isn't in `fire_on` returns `Skipped { FireOnMismatch }` and is logged
at DEBUG.

## Discord webhook (`kind = "discord_webhook"`)

Posts programmatic [twilight](https://twilight.rs) embeds with
conclusion-coloured states.

- **Webhook host allowlist.** The credential URL must be on `discord.com`,
  `discordapp.com`, `ptb.discord.com`, or `canary.discord.com`. Other hosts
  are rejected at config load.
- **Embed templates.** All template fields are optional handlebars
  templates rendered against the run context.

| field | scope | notes |
|---|---|---|
| `title` | once per run | embed title; truncated codepoint-safe to Discord's limit |
| `description` | once per run | embed description; truncated codepoint-safe |
| `collapsed_summary` | once per run | shown inline when many jobs are present, replacing per-job fields |
| `field_name` | once per job | per-job embed field name; `{{job.*}}` available here |
| `field_value` | once per job | per-job embed field value; `{{job.*}}` available here |

The Discord embed builder is collapse-aware: when too many jobs would
exceed Discord's limits, gcit collapses the per-job fields into a single
summary block (using `collapsed_summary` if configured, otherwise an
auto-generated fallback). The total embed codepoint count is enforced
defensively before the HTTP call.

- **Errors.**
  - 401 / 403 / 404 / 410 → `Permanent` (token revoked, webhook deleted,
    etc.).
  - 429 → `Transient` with the API's `retry_after` hint.
  - 5xx / Hyper / Timeout → `Transient`.
  - Validation failures (embed too large, etc.) → `Permanent` (config
    bug).
- **Cancellation.** Each per-flow `CancellationToken` is plumbed into the
  webhook call. When cancel fires after the HTTP request has been sent
  but before the response is read, the delivery state is unknowable from
  gcit's side. The cancel `source` reflects this: `"discord webhook <id>
  cancelled; delivery status unknown — the request may or may not have
  reached Discord"`.

## Local mail (`kind = "local_mail"`)

Appends an mboxrd-formatted message to `/var/mail/<user>` directly. No
SMTP, no MTA dependency. Multiple gcit instances coordinate via
`flock(LOCK_EX)`. Other mbox writers (mailx, procmail, postfix) may use
dotlocking instead of flock — gcit does not acquire dotlocks, so
concurrent writes from a dotlock-only writer are not coordinated. On
systems where the mail spool is written by both gcit and a traditional
MUA, verify the MUA also uses flock or configure a dedicated spool.

- **Spool path.** `/var/mail/<user>` (POSIX convention). The validator
  rejects user names outside `[a-zA-Z0-9_-]+` or longer than 32
  characters.
- **Filesystem invariants.**
  - `O_NOFOLLOW` on open — refuses a symlinked spool.
  - `flock(LOCK_EX)` for cross-process exclusion. The lock-wait deadline
    is 5 seconds; if the lock is not acquired within that window, gcit
    returns `Transient` with `"spool lock not acquired within 5s"`.
  - `O_APPEND` write of the formatted message.
  - `fsync(2)` after write to durably commit.
- **Templates.**

| field | scope | notes |
|---|---|---|
| `subject` | once per run | mbox `Subject:` header |
| `body` | once per run | mbox body — codepoint-capped for very large messages |

- **Cancellation semantics.** The mail notifier splits its 5-second
  deadline: `LOCK_WAIT_DEADLINE` bounds **only** the flock acquisition
  wait. After the lock is acquired, `write_all` and the post-lock `fsync`
  run unbounded. A slow disk that holds `fsync` longer than 5 seconds
  completes the write successfully — the deadline is for lock contention,
  not for durability. This also means cancellation can only short-circuit
  the lock-wait phase: if cancel fires after the lock is acquired, the
  write+sync runs to completion to keep the spool record atomic.
- **Lock-wait race.** Because the post-lock-wait blocking task
  (`spawn_blocking` running `flock(LOCK_EX)`) is not cancellable from
  userspace, a cancellation during lock-wait may still result in a spool
  entry once the holder releases — the cancel returns Transient to the
  supervisor first, but the OS thread proceeds with its blocking syscall.
  The `cancelled` message includes the addendum `"the blocking task may
  still write if the lock becomes available before the deadline"` so an
  operator reading `journalctl -u gcit` knows a stray spool entry may
  appear.

### Install scope requirement

`local_mail` requires a system-scope install. Under `--user` installs
the daemon runs as the operator and cannot append to system mail spools.
`gcit install --user` rejects any config with `local_mail` destinations
with a clear error. With `--system`, the install wizard:

- Switches the unit from `DynamicUser=yes` to `User=gcit` + `Group=mail`
  + `SupplementaryGroups=mail`.
- Pre-flights `/var/mail/<user>` for each configured user — warns when
  the spool is missing or not group-writable, and prints the exact
  `touch` / `chgrp mail` / `chmod 0660` commands to fix it.
- Creates the static `gcit` user via `useradd --system --no-create-home
  --shell /usr/sbin/nologin -G mail gcit` (skipped if the account
  already exists).

## Logging

Notifier outcomes (`Sent` / `Skipped` / `Err`) all log under the single
tracing target `gcit::flow::notify`. Filter on this target to adjust
notifier verbosity independently of the per-flow targets. Each record
carries:

| field | values |
|---|---|
| `kind` | `discord`, `local_mail` |
| `id` | the destination id from config |
| `label` | `run-start`, `run-complete`, `job-complete` |
| `job_id` | only on per-job records |

`Sent` records also carry an opaque `receipt` string identifying the
delivery. For Discord this is a synthetic `webhook:{id}` token derived
from the parsed webhook URL's id segment — gcit does not request a
message-body reply from Discord (no `?wait=true`), so a real message id
is never available. For local mail the receipt is `file:{path}` — the
absolute spool path that was appended to, prefixed with `file:` so a
journald reader can `grep '^file:'` to find every successful mbox
delivery.

`Skipped` records carry a debug-formatted `reason`:

| reason | meaning |
|---|---|
| `NotConfigured` | the destination uses a default no-op `on_run_start` / `on_job_complete` impl. Never returned by `on_run_complete`. |
| `FireOnMismatch` | the destination's `fire_on` array does not include the triggering event. Operator opted out. |
| `RateLimited` | the notifier's per-credential rate bucket said defer. Caller retries on the next supervisor cycle. |

## Canonical Transient messages

When investigating `last_error` in `gcit status` or filtering journald,
expect these canonical message shapes from the notifier path:

| message prefix | meaning | operator action |
|---|---|---|
| `cancelled before lock acquired on <path>` | Pre-cancel or cancel during flock-wait fired before the OS thread started its blocking syscall. No spool write occurred. | None — normal shutdown/reload artifact. |
| `cancelled before lock acquired on <path>; the blocking task may still write if the lock becomes available before the deadline` | Cancel fired during lock-wait; the OS thread is still blocked on `flock(LOCK_EX)`. A stray spool entry MAY appear post-cancel if the holder releases before the deadline elapses. | None — normal shutdown/reload artifact. Re-read the spool only if you suspect a duplicate run on retry. |
| `spool lock not acquired within 5s` | Another writer held the flock past `LOCK_WAIT_DEADLINE`. backon will retry on the next supervisor cycle. | Investigate the other writer (mailx, procmail, another gcit instance) if contention persists. |
| `discord webhook <id> cancelled; delivery status unknown — the request may or may not have reached Discord` | Cancel fired mid-HTTP. Delivery state is ambiguous. | None — normal shutdown/reload artifact. |
| any `map_io_error` variant (e.g. `spool file <path> does not exist`, `permission denied writing to <path>`, `<path> is a symlink`, etc.) | Operator-actionable I/O failure surfaced through `NotifyError::Permanent`. | Follow the remediation embedded in the message (`useradd`/`touch`, `BindPaths=`, resolve the symlink, etc.). |

## Run-start delivery semantics

Notifier fan-out tasks for `on_run_start` are spawned without waiting for
completion — the dispatcher does not await them so a slow notifier cannot
delay monitor spawn. On shutdown these tasks are not joined and may be cut
off mid-send by tokio runtime teardown. As a result, a `run-start`
notification that was in flight when `SIGTERM` arrived may or may not be
delivered, and the operator has no way to tell which.

The `on_run_complete` and per-job `on_job_complete` fan-outs ARE awaited
inside the per-run monitor task (which the supervisor joins), so those
deliveries either complete or surface their failure in the journal before
shutdown finishes. If lossless shutdown matters for run-start, drain
dispatch first by `gcit reload`-ing onto a config with the relevant flows
disabled, wait until `gcit status` no longer shows an `active_runs:` line
for each affected flow (the text renderer suppresses the line when the
count is zero), then `systemctl stop gcit`.

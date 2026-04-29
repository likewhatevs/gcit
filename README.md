# gcit

Poll git repos, dispatch GitHub Actions workflows, post Discord notifications. Linux + systemd.

[![ci](https://github.com/likewhatevs/gcit/actions/workflows/ci.yml/badge.svg)](https://github.com/likewhatevs/gcit/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/likewhatevs/gcit/branch/main/graph/badge.svg)](https://codecov.io/gh/likewhatevs/gcit)
[![mutants](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/likewhatevs/gcit/gh-pages/mutants.json)](https://github.com/likewhatevs/gcit/actions/workflows/ci.yml)
[![docs](https://img.shields.io/badge/docs-mdbook-blue.svg)](https://likewhatevs.github.io/gcit/book/)
[![license](https://img.shields.io/badge/license-GPL--2.0--only-blue.svg)](LICENSE)

## What it does

gcit watches one or more git remotes for new commits on a configured ref. When the tip
moves, it dispatches a GitHub Actions `workflow_dispatch` against a target repository,
correlates the dispatch to its resulting run id, monitors the run to completion, and
notifies operators via Discord webhooks and/or local Unix mail.

A single gcit daemon hosts many independent flows. Each flow has one source (git remote +
ref), one action (workflow dispatch target), and zero or more destinations (Discord
webhook, local mail). Flows are isolated: a panic, network failure, or auth error in one
flow cannot crash the daemon or affect other flows.

## Features

- **Three git polling strategies, auto-selected from URL**: GitHub API
  (`get_ref`) for github.com remotes, grokmirror manifest fingerprinting for
  git.kernel.org, ls-refs over gix-protocol for everything else.
- **GitHub Actions dispatch**: `workflow_dispatch` via octocrab with run id
  correlation through an injected `gcit_run_id` UUID and `run-name` directive,
  with a `head_sha + created>=` fallback when `run-name` is not configured.
- **Discord webhook notifications**: programmatic
  [twilight](https://twilight.rs) embeds with conclusion-coloured states,
  configurable templates, and webhook host allowlist (`discord.com`,
  `discordapp.com`, `ptb.discord.com`, `canary.discord.com`).
- **Local mail (mbox append)**: notifier writes mboxrd-formatted messages to
  `/var/mail/<user>` directly. No SMTP, no MTA dependency. Uses `O_NOFOLLOW` +
  `flock(LOCK_EX)` + `fsync` for safe concurrent appends.
- **systemd-native**: type=notify lifecycle, socket activation for the
  control channel, `LoadCredential=` for secrets, `DynamicUser=yes` (or
  `User=gcit` + `Group=mail` when `local_mail` is configured), and a full
  hardening profile (`ProtectSystem=strict`, `NoNewPrivileges`,
  `RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6`,
  `MemoryDenyWriteExecute`, `SystemCallFilter=@system-service`, etc.).
- **SIGHUP reload**: live config reload diffs each flow against its
  previous shape. Flows whose config is unchanged keep their
  poll/dispatcher pair, credential resources, and rate-limit state —
  any in-flight run monitor stays attached and the per-credential
  rate-limit poller keeps refreshing without interruption. Credential
  file rotation takes effect only when no kept-alive flow still
  references the credential id. As long as any unchanged flow holds a
  credential, every flow using it continues with the previously-
  resolved token. If rotating because the old token was compromised,
  restart the daemon (`systemctl restart gcit`) rather than SIGHUP to
  ensure all flows use the new token immediately. Changed and removed
  flows are cancelled cleanly and the new generation starts a fresh
  poll cycle.
- **Per-flow dispatch cooldown**: bounds dispatch frequency (default 5m) to coalesce rapid pushes. `cooldown = "0s"` opts out.
- **Strict-mode templates**: handlebars with `set_strict_mode(true)`,
  variables namespaced as `{{flow.*}}`, `{{source.*}}`, `{{action.*}}`,
  `{{run.*}}`, `{{gcit.*}}`. No control flow, no helpers, no partials.
- **Credential safety**: secrets wrapped in `secrecy::SecretString`, redacted
  in `Debug`/`Display` and in `gcit status` / `gcit trigger --dry-run`
  output. Credential files must have no group or other access bits set
  (mode `0600` recommended; `0400`, `0500`, `0700` also accepted) and
  be owned by the daemon's effective uid (or root, so operators on
  `DynamicUser=yes` units can drop credentials via `sudo`).

## Status

gcit is pre-1.0. The wire format of the state file, the control protocol, and
config schema are subject to change before 1.0.

## Requirements

- Linux. The crate emits a `compile_error!` on non-Linux targets.
- systemd. `--foreground` mode is intended for development and testing; the
  supported deployment surface is the systemd units installed by `gcit
  install`.
- Rust 1.85 or newer to build from source.

## Install

### From source

```sh
git clone https://github.com/likewhatevs/gcit
cd gcit
cargo build --release
sudo install -m 0755 target/release/gcit /usr/local/bin/gcit
```

### Generate systemd units and config skeleton

```sh
sudo gcit install --system     # writes /etc/systemd/system + /etc/gcit
gcit install --user            # writes XDG paths under $HOME
```

The install command is interactive: it walks the operator through each
referenced credential, previews every file path it will create, and refuses to
write without confirmation. Pass `--non-interactive` for CI. After install,
gcit prints the next-step systemd commands (e.g. `sudo systemctl enable --now
gcit.socket gcit.service`).

`gcit uninstall` reverses an install via the on-disk install manifest; files
not in the manifest are never touched. Operator-modified files are detected
by sha256 mismatch and the uninstall **refuses to proceed** — pass `--force`
to remove them anyway. Without `--force`, the operator-modified files stay
untouched and the uninstall exits non-zero so a deployment script never
silently destroys local edits.

## Quick start

Minimal config at `/etc/gcit/config.toml` (or `$XDG_CONFIG_HOME/gcit/config.toml`
for user installs):

```toml
[poll]
source_interval = "60s"
job_interval    = "30s"
jitter          = 0.1

[log]
filter = "info,gcit=debug"

[http]
request_timeout = "30s"

[[flow]]
name = "linux-mainline-ci"
enabled = true
description = "Watch torvalds/linux master and dispatch our CI builder."

[flow.source]
url = "https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git"
ref = "refs/heads/master"

[flow.action]
kind = "github_workflow_dispatch"
repo = "myorg/linux-builder"
workflow = "ci.yml"
ref = "refs/heads/main"
credential_id = "github_pat"
inputs = { upstream_sha = "{{source.sha}}" }

[[flow.destination]]
kind = "discord_webhook"
credential_id = "discord_ci_webhook"
fire_on = ["run_complete"]

[flow.destination.template]
title = "{{flow.name}}: {{run.conclusion}}"
description = "linux@{{source.sha_short}} -> {{action.repo}}/{{action.workflow}}"

[[flow.destination]]
kind = "local_mail"
user = "ops"
fire_on = ["run_complete"]
```

> **`local_mail` requires a system-scope install.** The notifier writes
> directly to `/var/mail/<user>` and depends on the daemon being a
> member of the `mail` group. Under `--user` installs the daemon runs
> as the operator and cannot append to system mail spools — `gcit
> install --user` rejects any config containing `local_mail`
> destinations with a clear error. Use `gcit install --system` for any
> flow with a `local_mail` destination, and the installer will switch
> the unit from `DynamicUser=yes` to `User=gcit` + `Group=mail`
> automatically.

Validate the config and credential resolution:

```sh
gcit check
```

Start the daemon (under systemd; do not run `gcit run` directly in production):

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now gcit.socket gcit.service
journalctl -u gcit -f
```

## CLI

| subcommand | purpose |
|---|---|
| `gcit run [--foreground]` | daemon entry; routed via the systemd unit |
| `gcit install {--user\|--system}` | interactive install of config skeleton + systemd units |
| `gcit uninstall {--user\|--system}` | reverse a prior install via the install manifest |
| `gcit check [--config PATH]` | validate config; print every error in one pass |
| `gcit status [FLOW] [--format text\|json]` | per-flow status snapshot via the control socket |
| `gcit trigger <FLOW> [--dry-run]` | manually fire a flow or render its dispatch payload(s) |
| `gcit reload` | send `Reload` to the daemon (equivalent to `SIGHUP`) |
| `gcit validate-template <FILE>` | compile a template against the daemon's strict-mode probe context |
| `gcit completions <SHELL>` | print shell completion script to stdout |
| `gcit --version` | version + git SHA |

Exit codes follow `sysexits.h`: `0` success, `64` (`EX_USAGE`) bad invocation,
`65` (`EX_DATAERR`) bad template, `70` (`EX_SOFTWARE`) internal software error,
`71` (`EX_OSERR`) OS-level failure, `75` (`EX_TEMPFAIL`) transient (e.g.
control socket unreachable), `78` (`EX_CONFIG`) config error.

When `--config` is not passed, the default depends on who is running
gcit. `gcit install --user` and `gcit uninstall --user` always resolve
to the user-scope path (the install is explicitly user-scoped). For any
other subcommand invoked by a non-root euid, gcit defaults to
`$XDG_CONFIG_HOME/gcit/config.toml` (or `$HOME/.config/gcit/config.toml`
when `$XDG_CONFIG_HOME` is unset) so a non-root shell running `gcit run
--foreground`, `gcit check`, or `gcit status` without `--config` reads
the operator's user-scope config rather than the system one. Root
invocations without `--config` use the bottom default
`/etc/gcit/config.toml`. An explicit `--config` always wins regardless
of euid or scope.

## Credentials

Credentials are looked up in this order:

1. `$CREDENTIALS_DIRECTORY/<credential_id>` (systemd `LoadCredential=`).
2. `GCIT_CREDENTIAL_<UPPER_SNAKE_ID>` env var (e.g.
   `GCIT_CREDENTIAL_DISCORD_CI_WEBHOOK`).
3. `<config_dir>/credentials/<credential_id>` file. Mode `0600` is
   recommended; gcit accepts any mode whose group and other bits are
   all clear (e.g. `0400`, `0500`, `0600`, `0700`). Owned by the
   daemon's effective uid or root.
4. Otherwise: error listing every searched path and the flows that need it.

`LoadCredential=` is the recommended ingress under systemd. Credential file
mode and owner are checked at every load; gcit refuses to read a file whose
group or other read/write/execute bits are set (mode `0600` is the
recommended canonical form, but any mode with no group/other bits — e.g.
`0400`, `0500`, `0600`, `0700` — is accepted), or whose owner uid is
neither the daemon's effective uid nor root (root is accepted so an
operator on a `DynamicUser=yes` unit can drop credential files via
`sudo` — the transient daemon uid is not knowable in advance).

GitHub authentication accepts only fine-grained personal access tokens (tokens
beginning `github_pat_`) for `github_workflow_dispatch`. Classic PATs and
GitHub App authentication are out of scope for v1.

### Ownership rules by install scope

The owner-uid check at credential load depends on the install scope. The
following examples use credential id `github_pat`:

- **`gcit install --user`** writes config under
  `$XDG_CONFIG_HOME/gcit` (typically `~/.config/gcit`). The daemon runs
  as the invoking operator. Credential files must be owned by the
  operator's uid:

  ```sh
  chmod 0600 ~/.config/gcit/credentials/github_pat
  chown $(id -u):$(id -g) ~/.config/gcit/credentials/github_pat
  ```

- **`gcit install --system`** with the default `DynamicUser=yes` unit
  allocates a transient uid each time the unit starts; the operator
  cannot match it. Drop credential files as root and gcit accepts them:

  ```sh
  sudo install -m 0600 -o root -g root /path/to/token /etc/gcit/credentials/github_pat
  ```

  Or use systemd's `LoadCredential=` directive which bypasses the
  on-disk file entirely:

  ```ini
  # /etc/systemd/system/gcit.service.d/credentials.conf
  [Service]
  LoadCredential=github_pat:/etc/gcit/credentials/github_pat
  ```

- **`gcit install --system`** with a `local_mail` destination switches
  the unit from `DynamicUser=yes` to `User=gcit` + `Group=mail`. Drop
  credentials owned by `gcit` (or root), mode `0600`:

  ```sh
  sudo install -m 0600 -o gcit -g gcit /path/to/token /etc/gcit/credentials/github_pat
  ```

### `gcit check` exit semantics

`gcit check` reports one of three states:

| state | meaning | exit code |
|---|---|---|
| 1 | Config parses, validates, and every referenced credential resolves now via one of the three resolution steps. | `0` |
| 2 | Config parse or validation error, OR a credential id is referenced by a flow but not declared anywhere. | `78` (`EX_CONFIG`) |
| 3 | Config validates AND `$CREDENTIALS_DIRECTORY` is set and points at a real directory but the credential is not present **right now** (the daemon will receive it from systemd at runtime via `LoadCredential=`). | `0` with an `INFO` note |

State 3 lets `gcit check` run from a developer shell where
`$CREDENTIALS_DIRECTORY` is not yet populated without falsely
reporting a config bug. Run `gcit check` from inside the unit (e.g.
`systemctl start gcit-check.service` if you wire one up) for an
end-to-end check that exercises step 1 of the resolution chain.

## Templates

Notification messages, dispatch input values, and Discord embed
fields are all rendered via [handlebars](https://docs.rs/handlebars)
in strict mode (`set_strict_mode(true)`). Every variable must resolve
or rendering fails. Block helpers (`each`, `with`, `if`, `unless`,
`lookup`, `log`, `raw`) are deregistered so templates are leaf-only
substitutions; comparison helpers (`eq`, `ne`, `gt`, ...) and `len`
remain available.

Variables are namespaced. Bare names like `{{flow}}` or
`{{gcit_run_id}}` are rejected at config load.

### Available keys

The following keys are available in **every** template (run-start,
run-complete, mbox subject/body, Discord title/description/
collapsed_summary, dispatch `[action.inputs]` values):

| key | source |
|---|---|
| `{{flow.name}}` | flow name from `[[flow]] name = ...` |
| `{{flow.description}}` | optional `[[flow]] description = ...` (renders as empty string when unset) |
| `{{source.url}}` | `[flow.source] url` |
| `{{source.ref_name}}` | `[flow.source] ref` (the `ref` field — Rust keyword conflict forces the `_name` suffix on the rendered key) |
| `{{source.sha}}` | full 40-char hex SHA observed at trigger time |
| `{{source.sha_short}}` | 12-char hex prefix of `source.sha` |
| `{{action.repo}}` | `[flow.action] repo` |
| `{{action.workflow}}` | `[flow.action] workflow` |
| `{{action.run_id}}` | GitHub Actions Run.id resolved by the correlator |
| `{{action.run_url}}` | `https://github.com/<repo>/actions/runs/<id>` |
| `{{action.dispatched_at}}` | RFC 3339 timestamp at dispatch time |
| `{{run.status}}` | run status label (`queued`, `in progress`, `completed`, ...) |
| `{{run.conclusion}}` | terminal conclusion (`success`, `failure`, `timed out`, `action required`, ...). For `[action.inputs]` rendered at trigger time, this is `(in progress)` because the run hasn't completed yet. |
| `{{gcit.run_id}}` | UUID gcit injects into the dispatch payload and uses for run correlation |

### Per-job-only keys

Discord `field_name` and `field_value` templates render once per job
and additionally have access to:

| key | source |
|---|---|
| `{{job.id}}` | GitHub Actions Job.id |
| `{{job.name}}` | Job.name from the workflow |
| `{{job.url}}` | Job.html_url |
| `{{job.conclusion}}` | per-job conclusion label |
| `{{job.attempt}}` | run_attempt counter |

`{{job.*}}` is **not** available on `title`, `description`, or
`collapsed_summary` templates — they render once per run, not per
job.

> **Known limitation:** the config validator's probe context does not
> yet include a `job` namespace, so `gcit check` and `gcit
> validate-template` currently reject `{{job.*}}` references in **all**
> templates (not just the run-scoped ones). At runtime, per-job keys
> render correctly inside `field_name` and `field_value`. A future
> release will add `job` stubs to the probe context so templates using
> `{{job.*}}` pass validation. Until then, operators can either omit
> `field_name`/`field_value` or test their templates against a real
> dispatched run and read the journald output.

### Strict-mode pitfalls

- A typo in any key path (e.g. `{{source.shaa}}`) is rejected at
  config load — `gcit check` shows the exact line.
- Untrusted DATA values (sha, ref names, run conclusion strings, job
  names) render as inert text and cannot be re-interpreted as template
  fragments. The escape function is replaced with `handlebars::no_escape`
  so plain-text outputs (mbox bodies, Discord embed leaves) are NOT
  HTML-encoded — Discord renders the result as markdown, mbox renders
  as plain text. Operators do not see literal `&lt;`/`&gt;`/`&amp;` in
  delivered messages.

## Workflow dispatch correlation

GitHub's `workflow_dispatch` API does not return the resulting run id. gcit
correlates the dispatch to the spawned run by:

1. Generating a UUID at dispatch time and injecting it into the
   `workflow_dispatch` `inputs` payload as `gcit_run_id`.
2. Polling `list_workflow_runs` and matching by `Run.name.contains("gcit-<uuid>")`.

Workflows opt in by declaring the input and embedding it in `run-name`:

```yaml
on:
  workflow_dispatch:
    inputs:
      gcit_run_id:
        type: string

run-name: gcit-${{ inputs.gcit_run_id }}
```

When `run-name` is not configured, gcit falls back to filtering by
`head_sha + ?created>=<dispatch_iso>` and selects the most recent matching
run. The fallback emits a WARN recommending the `run-name` directive.

## Logging

gcit emits structured records via `tracing` to journald (when run under
systemd) or stderr (under `--foreground`). All records carry a `target`
that names the daemon subsystem; `--log-filter` takes a tracing
`EnvFilter`-compatible string (default: `info,gcit=debug`). The
config-file `[log] filter` field is parsed but not yet wired into log
init; the CLI flag is the only operative source.

The target hierarchy:

| target | what it covers |
|---|---|
| `gcit::supervisor` | top-level select! loop, signal handlers, reload, shutdown |
| `gcit::flow::poll` | per-flow poll loop, strategy selection, SHA-diff observations |
| `gcit::flow::dispatcher` | per-flow `workflow_dispatch` send + correlation |
| `gcit::flow::monitor` | per-run monitor loop, terminal detection |
| `gcit::flow::notify` | uniform per-notifier outcome (Sent/Skipped/Err) for run-start, run-complete, and per-job-complete fan-outs |
| `gcit::control` | control socket accept loop and per-connection handlers |
| `gcit::state` | state-writer thread, persistence, schema-version checks |
| `gcit::git::ls_remote` | ls-refs transport (gix-protocol) |

Notifier outcomes (Sent / Skipped / Err) use the single target
`gcit::flow::notify`; filter on this target to adjust notifier log
verbosity independently of the per-flow targets. Each record carries
`kind` (e.g. `discord`, `local_mail`), `id` (the destination id from
config), and `label` (`run-start`, `run-complete`, `job-complete`);
per-job records additionally carry `job_id`.

## Shutdown semantics

On `SIGTERM` (or `SIGINT`) the supervisor cancels its root token, awaits
every per-flow poll/dispatcher/monitor task in the JoinSet, drains the
control server, then signals the state writer to flush and waits for the
state-writer thread to exit. State persisted before shutdown is durable.
(`systemctl stop gcit` sends `SIGTERM`; Ctrl-C in foreground sends `SIGINT`.)

Known limitation: notifier fan-out tasks for `on_run_start` are spawned
without waiting for completion — the dispatcher does not await them so a
slow notifier cannot delay monitor spawn. On shutdown these tasks are not
joined and may be cut off mid-send by tokio runtime teardown. As a result,
a `run-start` notification that was in flight when `SIGTERM` arrived may
or may not be delivered, and the operator has no way to tell which.
The `on_run_complete` and per-job `on_job_complete` fan-outs ARE awaited
inside the per-run monitor task (which the supervisor joins), so those
deliveries either complete or surface their failure in the journal
before shutdown finishes. If lossless shutdown matters for run-start,
drain dispatch first by `gcit reload`-ing onto a config with the
relevant flows disabled, wait until `gcit status` no longer shows
an `active_runs:` line for each affected flow (the text renderer
suppresses the line when the count is zero), then `systemctl stop gcit`.

### Notifier cancellation

Each per-flow `CancellationToken` is plumbed into every notifier hook
(`on_run_start`, `on_job_complete`, `on_run_complete`). When the token
fires (SIGTERM or per-flow reload), notifiers race their wait against
`cancel.cancelled()` and surface `NotifyError::Transient` with a
`"cancelled"` message. The supervisor logs this under the
`gcit::flow::notify` tracing target — it is a normal shutdown/reload
artifact, not an error to investigate.

The mail notifier splits its 5-second deadline: `LOCK_WAIT_DEADLINE`
bounds **only** the flock acquisition wait. After the lock is
acquired, `write_all` and the post-lock `fsync` run unbounded. A slow
disk that holds `fsync` longer than 5 seconds completes the write
successfully — the deadline is for lock contention, not for
durability. This also means cancellation can only short-circuit the
lock-wait phase: if cancel fires after the lock is acquired, the
write+sync runs to completion to keep the spool record atomic.

Because the post-lock-wait blocking task (`spawn_blocking` running
`flock(LOCK_EX)`) is not cancellable from userspace, a cancellation
during lock-wait may still result in a spool entry once the holder
releases — the cancel returns Transient to the supervisor first, but
the OS thread proceeds with its blocking syscall. The `cancelled`
message includes the addendum `"the blocking task may still write if
the lock becomes available before the deadline"` so an operator
reading `journalctl -u gcit` knows a stray spool entry may appear.

The Discord notifier accepts cancel and races it against its
`deliver` future via `tokio::select!`. When cancel fires after the
HTTP request has been sent but before the response is read, the
delivery state is unknowable from gcit's side: the message may have
reached Discord, or it may not. The cancel `source` reflects this
ambiguity: `"discord webhook <id> cancelled; delivery status unknown
— the request may or may not have reached Discord"`.

#### Canonical Transient messages

See [Notifiers — Canonical Transient messages](https://likewhatevs.github.io/gcit/book/notifiers.html#canonical-transient-messages) for the full table of notifier error shapes and operator actions.

## License

GPL-2.0-only. See [LICENSE](LICENSE).

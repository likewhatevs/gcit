# Troubleshooting

This chapter covers common failure modes when installing, configuring,
and operating gcit. The two most useful tools are `gcit check` (validate
the config and credentials) and `gcit status` (snapshot of every flow's
runtime state).

## `gcit check` exit states

| state | meaning | exit |
|---|---|---|
| 1 | Config parses, validates, and every credential resolves. | `0` |
| 2 | Config parse / validation error, OR credential id referenced but not declared. | `78` (`EX_CONFIG`) |
| 3 | Config validates AND `$CREDENTIALS_DIRECTORY` is set + real but the credential is missing right now. | `0` with an `INFO` note |

State 3 lets `gcit check` run from a developer shell where
`$CREDENTIALS_DIRECTORY` is not yet populated without falsely reporting a
config bug — the daemon will receive the credential at runtime via
`LoadCredential=`. Run `gcit check` from inside the unit (e.g.
`systemctl start gcit-check.service` if you wire one up) for an end-to-
end check that exercises step 1 of the resolution chain.

`gcit check` prints **every** error in one pass rather than stopping at
the first, so you can fix multiple issues per edit cycle.

## Common config errors

Every config-load error displays as `path:line[s]: <message>` so editors
and `grep` can navigate from the terminal output to the source.

### `at least one flow is required`

The config has no `[[flow]]` block. Add one with at least `name`,
`source`, and `action`.

### `duplicate flow name; N occurrences`

Two or more `[[flow]]` blocks share the same `name`. The error names
every line. Rename each occurrence so flow names are unique.

### `credential_id collision: ids map to the same env var`

Two `credential_id` strings produce the same `GCIT_CREDENTIAL_*` env var
name (rule: `id.uppercase().replace('-','_')`). Rename one so each id
maps to a unique env var.

### `credential 'X' not found`

A flow references a `credential_id` that does not resolve. The error
lists every searched path:

```text
credential 'github_pat' not found
  searched:
    /etc/gcit/credentials/github_pat
    $env::GCIT_CREDENTIAL_GITHUB_PAT
  consumed by: linux-mainline-ci
```

Drop the credential at one of the searched paths (mode `0600`, owner
`root` or the daemon's effective uid), or set the env var, or add a
`LoadCredential=` line to the unit.

### `credential 'X' at <path> has mode 0644`

The on-disk file failed the mode check. `mode & 0o077 != 0`. Fix:

```sh
sudo chmod 0600 /etc/gcit/credentials/X
```

### `credential 'X' at <path> is owned by uid N but the resolving process runs as uid M`

The file owner does not match either the daemon's effective uid or root.
The error names both uids and the `chown` command.

### `credential 'X' at <path> is a symlink`

Symlinks are refused outright (a `0600` symlink could pivot the
resolution to a world-readable target). Place the credential file
directly at the path.

### Template typos rejected at config load

`gcit check` compiles every template against a probe context. A typo
like `{{source.shaa}}` is rejected with the offending line and the
strict-mode rendering error.

```sh
gcit validate-template path/to/template
```

renders a standalone template against the same probe context and exits
`EX_DATAERR=65` on failure.

### Bare names rejected

Single-segment template names like `{{flow}}` or `{{gcit_run_id}}` are
rejected because the runtime template namespace is dotted only. Rewrite
to the dotted form (`{{flow.name}}`, `{{gcit.run_id}}`).

## Common runtime issues

### `gcit status` cannot connect to the control socket

```text
gcit status: cannot connect to /run/gcit/control.sock: No such file or directory (is the daemon running?)
```

The daemon is not running, or the socket path differs from the default.
Check:

```sh
# system-scope install:
systemctl status gcit
journalctl -u gcit -n 100

# user-scope install:
systemctl --user status gcit
journalctl --user -u gcit -n 100
```

For a non-default socket path, pass `--control-socket`. For user-scope
installs, the default is `$XDG_RUNTIME_DIR/gcit/control.sock`.

### Flow `state` shows `errored`

`gcit status` prints a `last_error[kind] at: message` indented under the
flow header. The kind names the failure category. The emitted values
are `git_poll_failed` (poll failure), `dispatch` (workflow_dispatch
failure), `correlate` (run-id correlation failure), `input_render`
(handlebars input render failure), `state_writer` (state writer
dropped), `notifier_setup` (notifier construction failure), `credential`
(credential resolution failure during flow setup), `panic` (task
panicked), and the synthetic `config_reload` (paired with the
`(reload)` daemon-level key when SIGHUP fails to parse the new config).
For `dispatch` and `correlate` errors backed by GitHub's rate-limit
response, the renderer also prints `retry_at: <RFC3339 timestamp>`
showing the quota reset window.

### `(reload)` entries in `gcit status`

Synthetic daemon-level keys (parenthesized names) are not flow names —
they carry daemon-scoped errors the supervisor records under a sentinel
key. The text renderer prefixes those entries with `[daemon]` so an
operator scanning the output can tell at a glance that the entry is not
a flow they configured. A `(reload)` entry typically means the most
recent SIGHUP failed to apply (e.g. config now references a missing
credential id).

### Workflow runs not correlated

If gcit dispatches but never logs a correlated `Run.id`, the workflow
likely doesn't declare the `gcit_run_id` input. Add:

```yaml
on:
  workflow_dispatch:
    inputs:
      gcit_run_id:
        type: string

run-name: gcit-${{ inputs.gcit_run_id }}
```

Without `run-name`, gcit falls back to `head_sha + ?created>=<dispatch
iso>` and selects the most recent matching run, emitting a `WARN`
recommending the `run-name` directive.

### `local_mail` notifier returns `permission denied`

The daemon (effective gid `mail`) cannot write `/var/mail/<user>`.
Verify:

```sh
ls -l /var/mail/<user>
# expected: -rw-rw---- 1 <user> mail
```

Fix:

```sh
sudo touch /var/mail/<user>
sudo chgrp mail /var/mail/<user>
sudo chmod 0660 /var/mail/<user>
```

`gcit check` runs the same `validate_spool_writability` probe used by
the daemon and surfaces the failure mode (`ParentMissing`,
`SpoolMissing`, `NotWritable`) with the appropriate remediation.

### `local_mail` lock contention

```text
spool lock not acquired within 5s
```

Another writer (mailx, procmail, another gcit instance) is holding the
flock past `LOCK_WAIT_DEADLINE`. backon will retry on the next
supervisor cycle. If contention persists, identify and resolve the other
writer.

### Discord webhook `permanent` errors

| status | meaning |
|---|---|
| 401 | webhook token revoked or invalid |
| 403 | webhook lacks permission (rare for Discord webhooks) |
| 404 | webhook deleted |
| 410 | webhook permanently gone |

Re-create the webhook in Discord, update the credential file, and
`systemctl restart gcit` (not just reload — see [Credential management
— Rotation](./credentials.md#rotation)).

### `gcit run --foreground` exits with `log init failed`

The default mode tries to attach to journald. Pass `--foreground` for a
stderr-only logger, or fix the journald reachability problem.

## Logging

gcit emits structured records via `tracing` to journald (when run under
systemd) or stderr (under `--foreground`). All records carry a `target`
that names the daemon subsystem; `--log-filter` takes a tracing
`EnvFilter`-compatible string (default: `info,gcit=debug`).

Target hierarchy:

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

Filter on a single target to focus an investigation. For example, to
trace only the dispatch path:

```sh
gcit run --foreground --log-filter "warn,gcit::flow::dispatcher=trace"
```

When investigating notifier behaviour, filter on
`gcit::flow::notify=trace` and read the structured fields (`kind`,
`id`, `label`, `job_id`, `receipt` / `reason` / `error`).

## Diagnostic checklist

When a flow misbehaves, run through these in order:

1. `gcit check` — config and credentials valid?
2. `gcit status <flow> --format json` — what state is the supervisor
   in? Any `last_error` recorded?
3. `journalctl -u gcit --since "10 minutes ago"` — what did the daemon
   actually do?
4. `gcit trigger <flow> --dry-run` — does the dispatch payload render
   correctly with current source SHA?
5. If still unclear: `journalctl -u gcit -f` and `gcit trigger <flow>`
   (without `--dry-run`) to fire a manual dispatch and watch the
   journald output in real time.

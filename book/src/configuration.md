# Configuration reference

gcit reads a TOML configuration file. The default path depends on who is
running gcit and which subcommand is invoked.

## Default config path

When `--config` is not passed, the default depends on the running euid and
subcommand:

- `gcit install --user` / `gcit uninstall --user` always resolve to the
  user-scope path.
- For any other subcommand invoked by a non-root euid, gcit defaults to
  `$XDG_CONFIG_HOME/gcit/config.toml` (or `$HOME/.config/gcit/config.toml`
  when `$XDG_CONFIG_HOME` is unset). A non-root shell running `gcit run
  --foreground`, `gcit check`, or `gcit status` without `--config` reads the
  operator's user-scope config rather than the system one.
- Root invocations without `--config` use the bottom default
  `/etc/gcit/config.toml`.
- An explicit `--config` always wins regardless of euid or scope.

## Top-level tables

| table | purpose |
|---|---|
| `[poll]` | default poll cadence and jitter (each flow can override). |
| `[log]` | reserved for future log-config integration. |
| `[http]` | shared HTTP client tuning. |
| `[[flow]]` | one or more flow definitions. |

A config is rejected at load time if the flow list is empty.

## `[poll]`

Poll defaults applied to every flow unless the flow overrides them under
`[flow.poll]`.

| field | type | bound | default |
|---|---|---|---|
| `source_interval` | humantime duration | inclusive `[15s, 24h]` | unset; resolved per strategy (60s for GitHub API and grokmirror, 5m for ls-remote) |
| `job_interval` | humantime duration | inclusive `[15s, 24h]` | `30s` |
| `jitter` | float | inclusive `[0.0, 0.5]` | `0.1` |
| `cooldown` | humantime duration | `0s` disables throttling; non-zero bounded `[15s, 24h]` | `5m` |

Effective interval has a 15-second floor after jitter is applied:
`interval = base * (1 + sample * jitter)` where `sample` is uniform in
`[-1, +1)`. Values outside the documented `jitter` range are clamped at
runtime, but the validator still rejects out-of-range values at config load.

Cooldown bounds dispatch frequency. After a trigger acceptance, subsequent SHA-diff observations within `cooldown` are suppressed. The most recent SHA at the end of the window is dispatched when the cooldown expires. Set `cooldown = "0s"` to opt out.

## `[log]`

| field | type | notes |
|---|---|---|
| `filter` | string (`tracing_subscriber::EnvFilter` syntax) | applies to subcommands that load the config (`run`, `check`, `install`). Precedence: `--log-filter` CLI flag > this field > built-in default `info,gcit=debug`. The control-channel subcommands (`reload`, `status`, `trigger`) read no config and use the CLI flag or the default. |

## `[http]`

| field | type | bound | default |
|---|---|---|---|
| `request_timeout` | humantime duration | inclusive `[1s, 300s]` | `30s` |
| `max_concurrent` | unsigned integer | accepted for back-compat | deprecated; no effect — concurrency is bounded by octocrab's per-credential rate-limiter and the tokio scheduler |

## `[[flow]]`

A flow is a (source, action, destinations) triple plus optional poll
overrides. Each flow runs as an independent task tree under the supervisor.

| field | type | required | notes |
|---|---|---|---|
| `name` | string | yes | unique across all flows; charset `[a-zA-Z0-9_-]+`, 1..=64 chars |
| `enabled` | boolean | no, default `true` | disabled flows parse and validate but are not spawned |
| `description` | string | no | renders as `{{flow.description}}` in templates; missing renders as empty string |
| `source` | inline table | yes | see `[flow.source]` |
| `action` | inline table | yes | see `[flow.action]` |
| `destination` | array of inline tables | no | see `[[flow.destination]]` |
| `poll` | inline table | no | per-flow override of `[poll]` defaults |

### `[flow.source]`

The git side of the flow — what gcit polls.

| field | type | required | notes |
|---|---|---|---|
| `url` | string | yes | parsed via `url::Url`; scheme must be one of `http`, `https`, `ssh`, `git`, `file` |
| `ref` | string | yes | must start with `refs/`. For the GitHub API strategy, only `refs/heads/*` and `refs/tags/*` are supported by the underlying `get_ref` endpoint |
| `credential_id` | string | no | auth credential for the git fetch (used by ls-remote when the upstream requires HTTP basic) |

The polling strategy is auto-detected from `url`. See [Polling
strategies](./polling.md) for the dispatch rules.

### `[flow.action]`

The dispatch side of the flow — what gcit fires when the source SHA changes.

The only supported `kind` in v1 is `github_workflow_dispatch`.

| field | type | required | notes |
|---|---|---|---|
| `kind` | string | yes | currently `github_workflow_dispatch` only |
| `repo` | string | yes | `owner/repo` form (exactly one `/`) |
| `workflow` | string | yes | non-empty; cannot contain `/`, `\`, or `..` (a workflow filename, not a path) |
| `ref` | string | yes | must start with `refs/`; the workflow ref to dispatch on the target repo |
| `credential_id` | string | yes | GitHub fine-grained PAT (`github_pat_*`) with Actions read+write on `repo` |
| `inputs` | inline table | no | string-keyed string-valued; values are handlebars templates rendered at trigger time |

Each `inputs` value is a handlebars template that renders against the same
namespace as notifier templates — see [Templates](#templates) below. gcit
also injects `gcit_run_id` into the rendered inputs payload at dispatch
time so the resulting GitHub Actions run can be correlated back to gcit.
See [Polling strategies](./polling.md#workflow-dispatch-correlation) for
the correlation contract.

### `[[flow.destination]]`

Each flow can attach zero or more destinations. The `kind` discriminator
selects the variant.

#### `kind = "discord_webhook"`

| field | type | required | notes |
|---|---|---|---|
| `kind` | string | yes | `"discord_webhook"` |
| `credential_id` | string | yes | Discord webhook URL stored as a credential — host must be one of `discord.com`, `discordapp.com`, `ptb.discord.com`, or `canary.discord.com` |
| `fire_on` | array of `FireEvent` | no, default `["run_complete"]` | duplicates rejected at config load — one error per duplicate |
| `template` | inline table | no | see [Templates](#templates) |

#### `kind = "local_mail"`

| field | type | required | notes |
|---|---|---|---|
| `kind` | string | yes | `"local_mail"` |
| `user` | string | yes | local Unix user; charset `[a-zA-Z0-9_-]+`, 1..=32 chars; the daemon writes to `/var/mail/<user>` |
| `fire_on` | array of `FireEvent` | no, default `["run_complete"]` | duplicates rejected at config load |
| `template` | inline table | no | see [Templates](#templates) |

`local_mail` requires a system-scope install (`gcit install --system`);
`gcit install --user` rejects any config with `local_mail` destinations.

### `FireEvent` values

`fire_on` accepts these string values (snake_case):

| value | when |
|---|---|
| `run_start` | after a `workflow_dispatch` succeeds AND the resulting `Run.id` is correlated, before the per-run monitor is spawned |
| `job_complete` | each individual job within a run reaches a terminal state (per-job, in observation order) |
| `run_complete` | the run itself reaches a terminal status (the only event guaranteed to deliver a summary) |

### `[flow.poll]` (per-flow overrides)

Same fields as `[poll]`. Any field not set falls back to the top-level
default; if the top-level default is also unset, gcit uses the strategy's
built-in default (60s / 60s / 5m for GithubApi / Grokmirror / LsRemote).

## Templates

Notification messages, dispatch input values, and Discord embed fields are
rendered via [handlebars](https://docs.rs/handlebars) in strict mode
(`set_strict_mode(true)`). Every variable must resolve or rendering fails.
Block helpers (`each`, `with`, `if`, `unless`, `lookup`, `log`, `raw`) are
deregistered, so templates are leaf-only substitutions; comparison helpers
(`eq`, `ne`, `gt`, ...) and `len` remain available.

The escape function is replaced with `handlebars::no_escape` so plain-text
outputs (mbox bodies, Discord embed leaves) are NOT HTML-encoded — Discord
renders the result as markdown, mbox renders as plain text.

Variables are namespaced. Bare names like `{{flow}}` or `{{gcit_run_id}}`
are rejected at config load.

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

Discord `field_name` and `field_value` templates render once per job and
additionally have access to:

| key | source |
|---|---|
| `{{job.id}}` | GitHub Actions Job.id |
| `{{job.name}}` | Job.name from the workflow |
| `{{job.url}}` | Job.html_url |
| `{{job.conclusion}}` | per-job conclusion label |
| `{{job.attempt}}` | run_attempt counter |

`{{job.*}}` is **not** available on `title`, `description`, or
`collapsed_summary` templates — they render once per run, not per job. The
config validator's probe context supplies stubs for every documented
`job` key so `gcit check` and `gcit validate-template` accept templates
that reference `{{job.*}}` in `field_name` or `field_value`. The notification
path supplies the real per-job data when iterating.

### Strict-mode pitfalls

- A typo in any key path (e.g. `{{source.shaa}}`) is rejected at config
  load — `gcit check` shows the exact line.
- Untrusted DATA values (sha, ref names, run conclusion strings, job
  names) render as inert text and cannot be re-interpreted as template
  fragments.

## Reload behavior

On `SIGHUP` (or `gcit reload`), the supervisor diffs each flow against its
previous shape. Flows whose config is unchanged keep their poll/dispatcher
pair, credential resources, and rate-limit state — any in-flight run
monitor stays attached and the per-credential rate-limit poller keeps
refreshing without interruption. Credential file rotation takes effect only
when no kept-alive flow still references the credential id. As long as any
unchanged flow holds a credential, every flow using it continues with the
previously-resolved token. If rotating because the old token was
compromised, restart the daemon (`systemctl restart gcit`) rather than
SIGHUP to ensure all flows use the new token immediately. Changed and
removed flows are cancelled cleanly and the new generation starts a fresh
poll cycle.

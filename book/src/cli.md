# CLI reference

```text
gcit [OPTIONS] <SUBCOMMAND>
```

## Global options

| flag | purpose |
|---|---|
| `--config <PATH>` | path to the config file (see [Configuration reference](./configuration.md#default-config-path) for default-resolution rules) |
| `--log-filter <FILTER>` | tracing filter per `tracing_subscriber::EnvFilter` syntax. Default `info,gcit=debug`. The CLI flag is the only operative source — the config-file `[log] filter` field is parsed but not currently wired into log init, and gcit does not honour any `RUST_LOG`-style env var. |
| `--control-socket <PATH>` | path to the daemon's Unix control socket. Default: `$XDG_RUNTIME_DIR/gcit/control.sock` (when set) or `/run/gcit/control.sock`. |
| `--version` | print version + git SHA and exit |
| `--help` | print help and exit |

## Exit codes

Exit codes follow `sysexits.h`:

| code | name | meaning |
|---|---|---|
| `0` | `EX_OK` | success |
| `64` | `EX_USAGE` | bad invocation (missing required arg, mutex violation) |
| `65` | `EX_DATAERR` | bad template (returned by `gcit validate-template`) |
| `70` | `EX_SOFTWARE` | internal software error |
| `71` | `EX_OSERR` | OS-level failure (FS error, manifest sha mismatch on uninstall without `--force`) |
| `75` | `EX_TEMPFAIL` | transient (control socket unreachable, daemon not responding) |
| `78` | `EX_CONFIG` | configuration error (parse, validation, refused silent overwrite, unresolved credential) |

## Subcommands

### `gcit run [--foreground]`

Daemon entry. Routed via the systemd unit in production. The daemon polls
every configured flow, dispatches workflow runs on SHA changes, monitors
them to completion, and posts notifications.

Pass `--foreground` for development to log to stderr instead of journald.
Without it, gcit initializes the journald layer and refuses to start when
journald is unreachable (logs from a misconfigured daemon must NOT silently
route to stderr that nothing reads).

### `gcit install --user | --system [--non-interactive] [--force]`

Interactive install of config skeleton + systemd units.

- One of `--user` or `--system` is required (clap `ArgGroup` mutex).
- `--non-interactive` skips the path-preview confirmation prompt for CI.
- `--force` overwrites existing managed files. Without it, encountering
  any existing managed file is fatal (`EX_CONFIG=78`).

The wizard:

1. Walks each referenced credential id and prints the URL hint, target
   repo, install path, and `chmod 0600` command. Annotates already-
   configured credentials with a `✓` line.
2. For configs with `local_mail` destinations, pre-flights every
   `/var/mail/<user>` and prints actionable warnings.
3. Previews every file path it will create (`[exists]` / `[new]` per
   entry) and refuses to write without confirmation.
4. Creates the static `gcit` user via `useradd` when `local_mail` is
   present + scope is system.
5. Writes files atomically + writes the install manifest at
   `$STATE_DIRECTORY/.install-manifest.json`.
6. Triggers `daemon-reload` via the user session bus (or hints for
   `--system`).
7. Prints next-step systemctl commands.

### `gcit uninstall --user | --system [--force]`

Reverses a prior `gcit install` using the install manifest. One of
`--user` or `--system` is required.

- Files NOT in the manifest are NEVER touched.
- Operator-modified files (sha256 mismatch with the manifest) are
  detected and the uninstall **refuses to proceed** with non-zero exit.
  Pass `--force` to remove them anyway.
- Path-traversal defense: every manifest entry must canonicalize under one
  of the expected install directories. A tampered manifest pointing at
  `/etc/passwd` is rejected before any removal happens.
- The state directory is preserved so a future re-install can pick up
  where the previous run left off. The manifest itself is removed last;
  if a file removal fails midway, re-run uninstall.

### `gcit check [--config PATH]`

Validate the configuration. Parses, validates every rule, and verifies
that every referenced credential id can be resolved. Prints every error
in one pass (rather than stopping at the first) so the operator can fix
multiple issues per edit.

Three exit states:

| state | meaning | exit |
|---|---|---|
| 1 | Config parses, validates, and every credential resolves now. | `0` |
| 2 | Config error OR credential id referenced but not declared. | `78` (`EX_CONFIG`) |
| 3 | Config validates AND `$CREDENTIALS_DIRECTORY` is set + real but the credential is missing right now. | `0` with an `INFO` note |

State 3 lets `gcit check` run from a developer shell without falsely
reporting a config bug — the daemon will receive the credential at
runtime via systemd `LoadCredential=`. Run `gcit check` from inside the
unit for an end-to-end check.

### `gcit status [FLOW] [--format text|json]`

Per-flow status snapshot via the control socket. Without a flow argument,
prints all flows.

- `--format text` (default): human-readable per-flow lines. Flow header
  shows `name: state`. Indented summary lines for `last_sha`,
  `last_poll_at`, `active_runs`, `notified_runs`, `last_error[kind] at:
  message`. The `active_runs:` and `notified_runs:` lines are suppressed
  when the count is zero. The `retry_at` sub-line appears only for
  `GithubErrorKind::RateLimited` errors.
- `--format json`: the daemon's JSON shape printed verbatim. Use this for
  scripting.

Synthetic daemon-level keys (any key wrapped in parentheses such as
`(reload)`) are not flow names — they carry daemon-scoped errors the
supervisor records under a sentinel key. The text renderer prefixes those
entries with `[daemon]` so an operator scanning the output can tell at a
glance that the entry is not a flow they configured.

Exits `EX_TEMPFAIL=75` when the daemon is unreachable.

### `gcit trigger <FLOW> [--dry-run]`

Manually fire a flow's dispatch path. The flow name is required and must
be non-empty.

With `--dry-run`, the daemon returns the rendered dispatch payload(s)
without contacting GitHub or any notifier. Useful for verifying your
templates and `[action.inputs]` values render correctly. Secrets in the
returned payload are redacted.

Without `--dry-run`, the daemon fires the dispatch as if a SHA change had
been observed at the current source SHA, runs the correlator, monitors
the run to completion, and fires the configured notifiers.

Exits `EX_TEMPFAIL=75` on transport error or unknown flow.

### `gcit reload`

Send `Reload` to the daemon over the control socket. Equivalent to
`SIGHUP` to the daemon process. The supervisor diffs each flow against
its previous shape and restarts only the changed and removed flows; see
[Configuration reference](./configuration.md#reload-behavior) for the
full semantics.

### `gcit validate-template <FILE> [--kind discord | local-mail]`

Compile a standalone template file using the same template rules and
namespaces (`{{flow.*}}`, `{{source.*}}`, `{{action.*}}`, `{{run.*}}`,
`{{gcit.*}}`) the daemon uses at notification time. The file's contents
are read verbatim — no TOML wrapping; just the raw template string.

The pipeline matches the daemon's config-load validation:

1. Register the template into a `notify::strict_handlebars()` instance —
   strict_mode + DEREGISTERED_HELPERS + no_escape.
2. AST check — reject single-segment references like `{{flow}}` or
   `{{gcit_run_id}}`. The runtime template namespace is dotted only.
3. Render against the probe context (the same shape `gcit check` uses to
   catch typos in dotted leaves like `{{flow.naem}}`).

On success, the rendered output is printed to stdout. On compile or
render failure, exits `EX_DATAERR=65` with the underlying error on
stderr. Operators routinely pipe the success output into `jq` or compare
against expected text in CI.

`--kind` is recorded in a one-line stderr header so reviewers reading CI
logs see which surface was validated. The underlying probe context is
shared across surfaces today; surface-specific checks (e.g. Discord's
per-job key set, local_mail's body cap) hang off this flag in future
iterations.

The probe context currently uses empty strings for every namespaced leaf
— matches `gcit check` behaviour.

### `gcit completions <SHELL>`

Print a shell-completion script to stdout. Supported shells (per
`clap_complete::Shell`): `bash`, `elvish`, `fish`, `powershell`, `zsh`.

Per-shell install hints print to stderr so the hint never contaminates
the generated script when piped to a file:

| shell | suggested path |
|---|---|
| bash | `~/.local/share/bash-completion/completions/gcit` (per-user) or `/etc/bash_completion.d/gcit` (system) |
| zsh | a directory on `$fpath` named `_gcit`; e.g. `~/.zsh/completions/_gcit` then add `fpath+=(~/.zsh/completions)` before `compinit` in `~/.zshrc` |
| fish | `~/.config/fish/completions/gcit.fish` |
| elvish | `~/.config/elvish/lib/gcit.elv` and add `use gcit` to `~/.config/elvish/rc.elv` |
| powershell | a `.ps1` file (e.g. `~/.config/powershell/gcit.ps1`) and dot-source it from `$PROFILE` |

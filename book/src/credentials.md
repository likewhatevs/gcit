# Credential management

Every secret gcit needs (GitHub PAT, Discord webhook URL, optional source-
side fetch credential) is referenced by a `credential_id` string in the
config. The id is opaque; it never appears in a URL or in the workflow
dispatch payload. gcit resolves the id to a secret value at boot, on
SIGHUP reload, and on every CLI subcommand that needs the secret (`gcit
check`, `gcit trigger --dry-run`).

Secrets are wrapped in `secrecy::SecretString` and redacted in
`Debug`/`Display` and in `gcit status` / `gcit trigger --dry-run` output.

## Resolution order

Credentials are looked up in this order. The first hit wins; failures stop
the walk so a misconfigured file is not silently overridden by a later step.

### 1. `$CREDENTIALS_DIRECTORY/<credential_id>`

systemd's `LoadCredential=` directive populates `$CREDENTIALS_DIRECTORY`
with one file per credential. This is the recommended ingress under
systemd. The default `gcit.service` unit emits `LoadCredential=` lines
sourced from `<config_dir>/credentials/<id>` for every id referenced by
the config.

The on-disk invariants below apply.

### 2. `GCIT_CREDENTIAL_<UPPER_SNAKE_ID>` environment variable

Each id is converted to an env var name by uppercasing and replacing `-`
with `_`. Example: a credential id `discord_ci_webhook` resolves to
`GCIT_CREDENTIAL_DISCORD_CI_WEBHOOK`.

Two ids that map to the same env var name are rejected at config load with
a `credential_id` collision error naming both ids and the rule
(`id.uppercase().replace('-','_')`).

### 3. `<config_dir>/credentials/<credential_id>` file

The fallback path is always relative to the directory holding `config.toml`.
For a system install that resolves to `/etc/gcit/credentials/<id>`; for a
user install, `$XDG_CONFIG_HOME/gcit/credentials/<id>`.

The on-disk invariants below apply.

### 4. Otherwise: error

If every step misses, the resolution surfaces an error listing every
searched path and the flows that need the credential.

## On-disk invariants

Both file-based resolution paths (steps 1 and 3) enforce the same on-disk
checks. The probe is shared across `gcit check`, `gcit install`, and the
daemon supervisor so they agree on what counts as a usable credential.

### Mode

The file mode bitmask must satisfy `mode & 0o077 == 0`. Any mode whose
group and other bits are all clear qualifies — `0400`, `0600`, `0700`
are typical examples; `0640` and `0644` are rejected.

The recommended canonical form is `0600`. Failed mode checks render a
single-line operator-facing message including the offending mode and a
`chmod 0600 <path>` recovery command.

### File type

The path must be a regular file. Symlinks are refused outright (a `0600`
symlink could pivot the resolution to a world-readable target). Fifos,
sockets, directories, and block/char devices are also refused.

The probe uses `symlink_metadata`, not `metadata`, so the symlink check
fires before any link is followed.

### Owner uid

The owner uid must be one of:

- the daemon's effective uid (the resolving process), or
- root (uid 0).

Root is accepted so an operator on a `DynamicUser=yes` unit can drop
credential files via `sudo` — the transient daemon uid is not knowable in
advance. Failed owner checks include both the file's owner uid and the
resolving euid in the error message, plus a `chown` command.

## Ownership rules by install scope

### `gcit install --user`

Writes config under `$XDG_CONFIG_HOME/gcit` (typically `~/.config/gcit`).
The daemon runs as the invoking operator. Credential files must be owned
by the operator's uid:

```sh
chmod 0600 ~/.config/gcit/credentials/github_pat
chown $(id -u):$(id -g) ~/.config/gcit/credentials/github_pat
```

### `gcit install --system` with `DynamicUser=yes`

The default for system installs without `local_mail`. systemd allocates a
transient uid each time the unit starts; the operator cannot match it.
Drop credential files as root and gcit accepts them:

```sh
sudo install -m 0600 -o root -g root /path/to/token /etc/gcit/credentials/github_pat
```

Or use systemd's `LoadCredential=` directive which bypasses the on-disk
file entirely:

```ini
# /etc/systemd/system/gcit.service.d/credentials.conf
[Service]
LoadCredential=github_pat:/etc/gcit/credentials/github_pat
```

The default unit already emits `LoadCredential=` lines for every credential
id referenced by the config; the override above is only needed when
sourcing from a non-default path.

### `gcit install --system` with `local_mail` destination

The unit switches from `DynamicUser=yes` to `User=gcit` + `Group=mail` +
`SupplementaryGroups=mail`. The install wizard creates the static `gcit`
user via `useradd --system --no-create-home --shell /usr/sbin/nologin -G
mail gcit`. Drop credentials owned by `gcit` (or root), mode `0600`:

```sh
sudo install -m 0600 -o gcit -g gcit /path/to/token /etc/gcit/credentials/github_pat
```

## GitHub authentication

gcit accepts only fine-grained personal access tokens for
`github_workflow_dispatch`. A fine-grained PAT begins `github_pat_`. The
token must have Actions read+write on the target `repo`. Classic PATs and
GitHub App authentication are out of scope for v1.

## `gcit check` exit semantics

`gcit check` reports one of three states:

| state | meaning | exit code |
|---|---|---|
| 1 | Config parses, validates, and every referenced credential resolves now via one of the three resolution steps. | `0` |
| 2 | Config parse or validation error, OR a credential id is referenced by a flow but not declared anywhere. | `78` (`EX_CONFIG`) |
| 3 | Config validates AND `$CREDENTIALS_DIRECTORY` is set and points at a real directory but the credential is not present **right now** (the daemon will receive it from systemd at runtime via `LoadCredential=`). | `0` with an `INFO` note |

State 3 lets `gcit check` run from a developer shell where
`$CREDENTIALS_DIRECTORY` is not yet populated without falsely reporting a
config bug. Run `gcit check` from inside the unit (e.g. `systemctl start
gcit-check.service` if you wire one up) for an end-to-end check that
exercises step 1 of the resolution chain.

`gcit check` runs the same `validate_spool_writability` probe used by the
daemon for every `local_mail` destination. A missing or non-writable
`/var/mail/<user>` surfaces alongside any credential errors so the
operator sees every problem in one pass.

## Rotation

To rotate a credential safely:

1. Update the credential file in place (or re-deploy via your secret
   management tool).
2. Run `systemctl restart gcit` (system) or `systemctl --user restart
   gcit` (user). A `SIGHUP` reload only re-resolves credentials for flows
   whose shape changed — flows whose config is unchanged keep the
   previously resolved token, by design, to avoid disrupting in-flight
   runs.

If the old token is known to be compromised, always restart rather than
reload. The reload-vs-restart distinction is documented in the
[Configuration reference](./configuration.md#reload-behavior).

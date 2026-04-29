# Quick start

This walkthrough installs gcit system-wide, configures one flow that watches
the Linux mainline tree and dispatches a GitHub Actions workflow on every
new commit, and starts the daemon under systemd.

## 1. Build and install the binary

```sh
git clone https://github.com/likewhatevs/gcit
cd gcit
cargo build --release
sudo install -m 0755 target/release/gcit /usr/local/bin/gcit
```

## 2. Author a minimal config

Create `/etc/gcit/config.toml`:

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
> directly to `/var/mail/<user>` and depends on the daemon being a member of
> the `mail` group. Under `--user` installs the daemon runs as the operator
> and cannot append to system mail spools — `gcit install --user` rejects any
> config containing `local_mail` destinations with a clear error. Use `gcit
> install --system` for any flow with a `local_mail` destination, and the
> installer will switch the unit from `DynamicUser=yes` to `User=gcit` +
> `Group=mail` automatically.

## 3. Drop credentials

Each `credential_id` referenced in the config needs a credential file at
`<config_dir>/credentials/<credential_id>`. Mode `0600` is recommended;
gcit accepts any mode whose group and other bits are all clear (e.g.
`0400`, `0500`, `0600`, `0700`). The file must be owned by the daemon's
effective uid or by root.

```sh
sudo install -m 0600 -o root -g root /path/to/token /etc/gcit/credentials/github_pat
sudo install -m 0600 -o root -g root /path/to/url /etc/gcit/credentials/discord_ci_webhook
```

For a `--user` install:

```sh
chmod 0600 ~/.config/gcit/credentials/github_pat
chown $(id -u):$(id -g) ~/.config/gcit/credentials/github_pat
```

GitHub authentication accepts only fine-grained personal access tokens
(tokens beginning `github_pat_`).

See [Credential management](./credentials.md) for the full resolution chain
(`$CREDENTIALS_DIRECTORY`, env var, file) and ownership rules per install
scope.

## 4. Install systemd units

```sh
sudo gcit install --system --config /etc/gcit/config.toml
```

The install command previews every file path it will create (`[exists]` /
`[new]` per entry) and refuses to write without explicit confirmation.
It copies the config, emits `gcit.service` and `gcit.socket`, and prints
the next-step systemd commands. Pass `--non-interactive` for CI.

For a user-scope install (no `local_mail` destinations):
`gcit install --user --config path/to/config.toml`.

`gcit uninstall` reverses an install via the on-disk install manifest;
operator-modified files are detected by sha256 mismatch and the uninstall
refuses to proceed without `--force`.

## 5. Validate

```sh
gcit check
```

`gcit check` parses the config, validates every rule, prints every problem
in one pass (rather than stopping at the first), and verifies that every
credential id referenced by a flow can be resolved. See
[Troubleshooting](./troubleshooting.md) for the three exit states.

## 6. Start

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now gcit.socket gcit.service
journalctl -u gcit -f
```

The first poll cycle records a baseline SHA without firing — gcit only
dispatches when the previously recorded SHA differs from the just-observed
one. The next poll that sees a changed tip will dispatch the workflow,
correlate the run id, monitor it to completion, and fire the configured
notifiers.

## 7. Inspect

```sh
gcit status                   # all flows
gcit status linux-mainline-ci # one flow
gcit status --format json     # machine-readable
gcit trigger linux-mainline-ci --dry-run   # render the dispatch payload
```

The control socket is created by the `gcit.socket` unit (system path
`/run/gcit/control.sock`, user path `$XDG_RUNTIME_DIR/gcit/control.sock`)
with `SocketMode=0600`.

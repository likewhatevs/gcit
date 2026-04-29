# Systemd integration

gcit is a Type=notify systemd service with socket activation for the
control channel. The supported deployment surface is the systemd units
that `gcit install` writes; `--foreground` is for development only.

## Install paths

| scope | service unit | socket unit | config dir | state dir |
|---|---|---|---|---|
| `--system` | `/etc/systemd/system/gcit.service` | `/etc/systemd/system/gcit.socket` | `/etc/gcit` | `/var/lib/gcit` |
| `--user` | `$XDG_CONFIG_HOME/systemd/user/gcit.service` | `$XDG_CONFIG_HOME/systemd/user/gcit.socket` | `$XDG_CONFIG_HOME/gcit` | `$XDG_STATE_HOME/gcit` |

For `--user` installs, `$XDG_CONFIG_HOME` defaults to `~/.config` and
`$XDG_STATE_HOME` defaults to `~/.local/state` when the env vars are
unset.

The install manifest is written at `<state_dir>/.install-manifest.json`
and tracks every file the wizard wrote, with sha256 + mode. `gcit
uninstall` reads the manifest to know what to remove (and only what to
remove).

## Service unit

The rendered `gcit.service` includes:

- `Type=notify` and `NotifyAccess=main`. The daemon emits
  `READY=1` once the supervisor's `select!` loop is live and ready to
  accept SIGTERM / SIGHUP / control commands.
- `Requires=gcit.socket` (the control socket is socket-activated).
- `After=network-online.target` + `Wants=network-online.target`.
- `ExecStart=<binary> run` and `ExecReload=<binary> reload`. The binary
  path is resolved at install time via `std::env::current_exe()` so a
  `--user` install rooted at `~/.cargo/bin/gcit` records that exact path
  rather than a hardcoded `/usr/bin/gcit`.

## User model

| config has `local_mail`? | user model |
|---|---|
| no | `DynamicUser=yes` (systemd assigns a transient uid each start) |
| yes | `User=gcit`, `Group=mail`, `SupplementaryGroups=mail` |

The `local_mail` notifier writes to `/var/mail/<user>` and needs `mail`
group access. `DynamicUser=yes` cannot join the `mail` group, so the
install wizard switches to a static `gcit` system user it creates via
`useradd --system --no-create-home --shell /usr/sbin/nologin -G mail gcit`.

Exit code 9 from `useradd` (`E_NAME_IN_USE`) is treated as success — the
account already exists, nothing to do, and uninstall will NOT remove it
unless this install created it. The manifest's
`user_created_by_install: true` flag tracks whether the install minted
the account.

## Hardening directives

The unit emits the following byte-for-byte hardening profile so
`systemd-analyze security gcit.service` reports identical hardening
regardless of the user-model branch:

```ini
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectKernelLogs=yes
ProtectControlGroups=yes
ProtectClock=yes
ProtectHostname=yes
ProtectProc=invisible
ProcSubset=pid
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
RestrictNamespaces=yes
RestrictRealtime=yes
RestrictSUIDSGID=yes
LockPersonality=yes
MemoryDenyWriteExecute=yes
SystemCallFilter=@system-service
SystemCallArchitectures=native
CapabilityBoundingSet=
DeviceAllow=
UMask=0077
RuntimeDirectory=gcit
RuntimeDirectoryMode=0700
StateDirectory=gcit
StateDirectoryMode=0700
ConfigurationDirectory=gcit
ConfigurationDirectoryMode=0750
ReadWritePaths=/var/mail
Restart=on-failure
NotifyAccess=main
TimeoutStopSec=360
```

`ReadWritePaths=/var/mail` is unconditional — it is required only when
`local_mail` is configured, but emitting it in both branches keeps the
unit-rendering logic and the `systemd-analyze` baseline identical. The
empty `CapabilityBoundingSet=` and `DeviceAllow=` clear the daemon's
capability and device allow-lists; without these gcit would inherit
systemd's defaults.

## Socket unit

```ini
[Unit]
Description=gcit control socket

[Socket]
ListenStream=%t/gcit/control.sock
SocketMode=0600
FileDescriptorName=control

[Install]
WantedBy=sockets.target
```

`%t` resolves at runtime to `/run/gcit` for system units and
`$XDG_RUNTIME_DIR/gcit` for user units. `SocketMode=0600` keeps the
control channel restricted to the unit's effective uid.

`FileDescriptorName=control` lets the daemon match the inherited fd
against its expected name. gcit reads the socket-activated fd via
`sd_notify::listen_fds_with_names_and_unset_env()` at startup, before any
threads or the tokio runtime are created.

## `LoadCredential` lines

The install wizard emits one `LoadCredential=<id>:<path>` line per
credential id referenced by the config, sourced from
`<config_dir>/credentials/<id>` (deterministic sorted order for
reproducibility). Operators who want a different source path can write
a drop-in:

```ini
# /etc/systemd/system/gcit.service.d/credentials.conf
[Service]
LoadCredential=github_pat:/run/secrets/github_pat
```

systemd populates `$CREDENTIALS_DIRECTORY` with one file per
`LoadCredential` line; gcit's resolution chain looks there first. See
[Credential management](./credentials.md#resolution-order).

## Install wizard walkthrough

`gcit install --system` (or `--user`) is interactive by default:

1. **Credential walkthrough.** Prints one section per referenced
   credential id with the URL hint, target repo (for GitHub PATs), the
   on-disk install path, and the `chmod 0600 <path>` command. Already-
   configured credentials get a `✓ <id>: configured at <path>` line and
   the long instructions are skipped. Root-owned credentials under
   `--user` scope are annotated `(owned by root — rotate via sudo)` so
   operators know future rotations require `sudo`.
2. **Local mail check** (only when `local_mail` is configured). For
   each user named in a `local_mail` destination, probes
   `/var/mail/<user>` and prints either confirmation or actionable
   warnings (`sudo touch`, `sudo chgrp mail`, `sudo chmod 0660`).
3. **Path preview.** Each file the wizard will write is listed with
   `[exists]` or `[new]`, plus the runtime directories systemd will
   auto-create on first start, plus the user model the unit will
   activate, plus any `useradd` invocation the wizard will run.
4. **Confirmation.** Prompts `Proceed? [y/N]`. Operator changing their
   mind exits 0; only `y` / `Y` / `yes` / `YES` / `Yes` proceed.
   `--non-interactive` skips this prompt.
5. **Atomic write.** Each file is written via tempfile + write +
   `sync_all` + persist + parent dir fsync. The install manifest is
   written last with the same durability.
6. **`daemon-reload`.** Triggered via the user session bus (`zbus`), or
   skipped with a hint when running `--system` without root.
7. **Next steps.** Prints the exact `systemctl daemon-reload &&
   systemctl enable --now gcit.socket gcit.service` command for the
   chosen scope and the `journalctl -u gcit -f` follow-up.

The install refuses to silently overwrite any existing managed file
without `--force` and lists every offending path so the operator can
decide whether to `gcit uninstall` first or pass `--force`.

## Shutdown semantics

On `SIGTERM` (or `SIGINT`) the supervisor cancels its root token, awaits
every per-flow poll/dispatcher/monitor task in the JoinSet, drains the
control server, then signals the state writer to flush and waits for the
state-writer thread to exit. State persisted before shutdown is durable.

`systemctl stop gcit` sends `SIGTERM`. Ctrl-C in foreground sends
`SIGINT`. The unit's `TimeoutStopSec=360` gives gcit up to 6 minutes for
the shutdown sequence, which is enough headroom for in-flight runs to
finish their final notifier fan-out.

For lossless shutdown semantics (specifically around `on_run_start`
fan-outs which are not awaited), see [Notifiers — Run-start delivery
semantics](./notifiers.md#run-start-delivery-semantics).

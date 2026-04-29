# Introduction

gcit ("GitHub CI triggers") is a Linux + systemd daemon that watches one or
more git remotes for new commits on a configured ref. When the tip moves, it
dispatches a GitHub Actions `workflow_dispatch` against a target repository,
correlates the dispatch to its resulting run id, monitors the run to
completion, and notifies operators via Discord webhooks and/or local Unix
mail.

A single gcit daemon hosts many independent flows. Each flow has one source
(git remote + ref), one action (workflow dispatch target), and zero or more
destinations (Discord webhook, local mail). Flows are isolated: a panic,
network failure, or auth error in one flow cannot crash the daemon or affect
other flows.

This book is the operator manual: how to install, configure, and run gcit.
For badges, license, and source layout see the project README.

## Why gcit

- **Polling instead of push.** GitHub Actions can already react to its own
  push events, but many real-world CI triggers come from upstream repos you
  don't control: the kernel mainline, a vendor SDK, a security-advisory feed.
  gcit polls those sources on a configurable cadence and converts SHA changes
  into GitHub Actions runs without relying on webhooks the upstream cannot
  install.
- **One daemon, many flows.** A flow is a (source, action, destinations)
  triple. gcit reloads flows live on `SIGHUP`; flows whose config did not
  change keep their state and credentials, while changed and removed flows
  are cancelled cleanly and the new generation starts a fresh poll cycle.
- **systemd-native.** gcit runs as a Type=notify unit with socket activation
  for the control channel, `LoadCredential=` for secrets, `DynamicUser=yes`
  by default (or `User=gcit` + `Group=mail` when `local_mail` is configured),
  and a full hardening profile.
- **Strict secrets.** Credentials are wrapped in `secrecy::SecretString`,
  redacted in `Debug`/`Display` and in CLI output. Credential files must
  have no group or other access bits set, and must be owned by the daemon's
  effective uid (or root).

## What gcit is not

- Not a generic webhook receiver. gcit polls; it does not accept inbound
  HTTP from GitHub.
- Not a workflow runner. gcit dispatches to GitHub Actions and observes the
  result; the runner side is GitHub's responsibility.
- Not portable. The crate emits a `compile_error!` on non-Linux targets.
  systemd is the supported deployment surface; `--foreground` exists for
  development and is not the production path.

## Status

gcit is pre-1.0. The wire format of the state file, the control protocol,
and the config schema are subject to change before 1.0.

## Requirements

- Linux.
- systemd. `--foreground` mode is intended for development and testing; the
  supported deployment surface is the systemd units installed by `gcit
  install`.
- Rust 1.85 or newer to build from source.

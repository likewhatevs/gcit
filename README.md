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

- Three git polling strategies auto-selected from URL — GitHub API for github.com, grokmirror for git.kernel.org, ls-refs for everything else. See [Polling strategies](https://likewhatevs.github.io/gcit/book/polling.html).
- GitHub Actions dispatch with run-id correlation — see [Polling strategies — Workflow dispatch correlation](https://likewhatevs.github.io/gcit/book/polling.html#workflow-dispatch-correlation).
- Discord webhook and local-mail notifiers — see [Notifiers](https://likewhatevs.github.io/gcit/book/notifiers.html).
- Strict-mode handlebars templates — see [Configuration reference — Templates](https://likewhatevs.github.io/gcit/book/configuration.html#templates).
- systemd-native lifecycle, socket activation, full hardening profile — see [Systemd integration](https://likewhatevs.github.io/gcit/book/systemd.html).
- SIGHUP reload that diffs each flow and preserves state for unchanged ones — see [Configuration reference — Reload behavior](https://likewhatevs.github.io/gcit/book/configuration.html#reload-behavior).
- Credentials wrapped in `secrecy::SecretString`; on-disk files must be `mode & 0o077 == 0`, owned by daemon euid or root. See [Credential management](https://likewhatevs.github.io/gcit/book/credentials.html).

## Documentation

The [gcit book](https://likewhatevs.github.io/gcit/book/) is the operator manual:

- [Introduction](https://likewhatevs.github.io/gcit/book/introduction.html)
- [Quick start](https://likewhatevs.github.io/gcit/book/quick-start.html)
- [Configuration reference](https://likewhatevs.github.io/gcit/book/configuration.html)
- [Credential management](https://likewhatevs.github.io/gcit/book/credentials.html)
- [Polling strategies](https://likewhatevs.github.io/gcit/book/polling.html)
- [Notifiers](https://likewhatevs.github.io/gcit/book/notifiers.html)
- [CLI reference](https://likewhatevs.github.io/gcit/book/cli.html)
- [Systemd integration](https://likewhatevs.github.io/gcit/book/systemd.html)
- [Troubleshooting](https://likewhatevs.github.io/gcit/book/troubleshooting.html)
- [Architecture overview](https://likewhatevs.github.io/gcit/book/architecture.html)

## Status

gcit is pre-1.0. The wire format of the state file, the control protocol, and
config schema are subject to change before 1.0.

## Requirements

- Linux. The crate emits a `compile_error!` on non-Linux targets.
- systemd. `--foreground` mode is intended for development and testing; the
  supported deployment surface is the systemd units installed by `gcit
  install`.
- Rust 1.91 or newer to build from source.

## Install

```sh
git clone https://github.com/likewhatevs/gcit
cd gcit
cargo build --release
sudo install -m 0755 target/release/gcit /usr/local/bin/gcit
```

For the systemd install walkthrough (config skeleton, credentials, units), see
[Quick start](https://likewhatevs.github.io/gcit/book/quick-start.html).

## License

GPL-2.0-only. See [LICENSE](LICENSE).

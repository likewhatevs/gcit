# Changelog

All notable changes to gcit will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- GitHub `X-RateLimit-Reset` headers in the past are now treated as
  missing on both 403/Remaining=0 and 429 responses; the classifier
  defaults to a 60s hold-off so the dispatcher backs off to a real
  future window instead of retrying immediately against an
  already-expired reset.
- `gcit install`: useradd preflight now checks the `mail` group
  exists (NSS-aware via `getent group mail`) before invoking
  `useradd -G mail`, and wraps a `useradd` ENOENT with an actionable
  hint (`install shadow-utils (RHEL/Fedora) or passwd
  (Debian/Ubuntu) and re-run, or remove local_mail destinations from
  config`).
- When `--config` is not passed, gcit defaults to the user-scope
  config (`$XDG_CONFIG_HOME/gcit/config.toml`) under non-root euids;
  the system bottom default `/etc/gcit/config.toml` is used only
  under root or for `--system` install/uninstall. Documented in the
  book's [Configuration reference](https://likewhatevs.github.io/gcit/book/configuration.html#default-config-path)
  and on the `--config` doc comment.
- CI per-PR `cargo mutants` job now uploads `mutants.out/` as a
  `cargo-mutants-output` artifact (14-day retention) so reviewers can
  inspect surviving mutants from the failed-gate annotation.
- Per-flow credential resolution errors recorded in `last_errors`
  (surfaced via `gcit status`) now prefix the operator-visible
  message with `flow `<name>``: ` so a single bad credential blocking
  N flows surfaces as N distinct flow-named entries instead of N
  copies of the same credential-id-only string.
- `tests/poll_github_api.rs::get_ref_5xx_returns_transient_error`
  is no longer `#[ignore]`d; the existing classifier handled both
  the `Error::GitHub` (5xx → status_code arm → Transient) and
  `Error::Json/Serde/Other` (catch-all → Transient) paths once the
  test was relaxed to accept octocrab's internal 5xx retry count.
- `gcit install` path-preview now lists the planned `useradd
  --system --no-create-home --shell /usr/sbin/nologin -G mail gcit`
  invocation under a `# System users gcit will create` block when
  `local_mail` destinations are configured, so operators see the
  side effect before answering `y` at the confirmation prompt.
- Notifier-outcome log records (run-start, run-complete, per-job
  job-complete) now share a single tracing target,
  `gcit::flow::notify`, rather than logging under each call-site's
  own target. Operators with `--log-filter` rules that targeted the
  per-call-site notifier targets must update them to filter on
  `gcit::flow::notify` instead. Each record carries `kind` (e.g.
  `discord`, `local_mail`), `id` (the destination id from config),
  and `label` (`run-start`, `run-complete`, `job-complete`); per-job
  records additionally carry `job_id`. See the book's
  [Troubleshooting — Logging](https://likewhatevs.github.io/gcit/book/troubleshooting.html#logging)
  for the full target hierarchy.
- `gcit status` text output now prefixes daemon-level synthetic keys
  (e.g. `(reload)`, recorded by the supervisor when SIGHUP-driven
  config reload fails to parse) with `[daemon]` so they cannot be
  confused with flow names. The JSON shape is unchanged — the synthetic
  key still appears in the top-level object alongside flow entries; the
  parens-wrapping is the namespace, and `flow.name` validation
  (`[a-zA-Z0-9_-]+`) guarantees no real flow can collide with the
  convention.
- Documented in the book's [Notifiers — Run-start delivery semantics](https://likewhatevs.github.io/gcit/book/notifiers.html#run-start-delivery-semantics):
  `on_run_start` notifier fan-out tasks are spawned without waiting
  for completion (so a slow notifier cannot delay monitor spawn) and
  may be cut off mid-send by tokio runtime teardown when the daemon
  receives `SIGTERM`. By contrast, `on_run_complete` and the per-job
  `on_job_complete` fan-outs are awaited inside the per-run monitor
  task — which the supervisor joins on shutdown — so those deliveries
  either complete or surface their failure in the journal before
  shutdown finishes.
- The supervisor now requires `$CREDENTIALS_DIRECTORY` to resolve to
  an existing directory before walking step 1 of credential
  resolution. Previously the supervisor proceeded with any value of
  the env var and relied on the per-file stat to silently fall
  through; a stale env value pointing at a missing path would mask
  itself as "no such credential" rather than surfacing the
  misconfiguration. Both `gcit check` and the daemon now apply the
  same `is_dir()` gate, matching cli/check's state-3 detection rule.
- A broken credential file at step 1 (`$CREDENTIALS_DIRECTORY/<id>`)
  or step 3 (`<config_dir>/credentials/<id>`) now terminates
  resolution with the probe's error message instead of falling
  through to the next step. Previously a chmod-0644 step-3 file
  would be silently skipped and the lookup would surface as "not
  found", hiding the misconfiguration behind a downstream message.
  Step 2 (env-var) and step 3 are walked only when the previous
  step returns NotPresent; an invariant or stat failure at any step
  terminates resolution immediately.
- `$CREDENTIALS_DIRECTORY/<id>` now enforces the same mode/owner
  invariant as the step-3 `<config_dir>/credentials/<id>` fallback:
  the file must be a regular file (not a symlink), have no
  group/other access bits set (mode `0600` recommended; `0400`,
  `0500`, `0700` also accepted), and be owned by the daemon's
  effective uid or root. Previously the supervisor implicitly trusted whatever
  systemd's `LoadCredential=` deposited in
  `$CREDENTIALS_DIRECTORY` because that directory is private to the
  service unit. The check is now defense-in-depth: it surfaces a
  loud error if a bind-mount, manual override, or future systemd
  change places a non-conforming file there. Operators using
  `LoadCredential=` need not change anything — systemd already
  drops the file at `0400` owned by the service uid, which passes
  the gate.
- Poll-side trigger send now races cancellation. The poll loop's
  `Refreshed`-arm handling reordered to send `TriggerSignal` to the
  dispatcher BEFORE persisting the `PollObservation` to state.json.
  Previously the observation went first; if the supervisor's cancel
  fired between observation and trigger (SIGHUP, panic-respawn, or
  shutdown), state.json persisted the new SHA while the trigger
  never reached the dispatcher's mpsc — the next-generation poll
  task observed no diff and the trigger was permanently lost.
  Reordered: trigger first (cancel-races so a wedged dispatcher
  does not block shutdown), then observation. If cancellation
  races between the two sends, state.json keeps the OLD SHA and
  the next-generation poll re-detects.
- SIGHUP reload now preserves rate-limit state for kept-alive flows.
  Each credential's rate-limit poller, cached `Arc<GithubClient>`, and
  `Arc<RateLimitState>` survive the reload as long as at least one
  unchanged flow still references the credential. New flows added to
  the config that reuse the same credential land on the same shared
  pool entry, so both flows observe each other's API calls under one
  primary-rate-limit budget. Credentials no longer referenced by any
  kept-alive flow are dropped: the poller is cancelled, the entry is
  removed, and the next `acquire_github` call re-reads the credential
  file (picking up a rotated PAT).
- Trade-off: credential rotation propagates per-credential, not
  per-flow. Rotation takes effect only if no kept-alive flow still
  references the credential id. As long as any unchanged flow holds a
  credential, every flow using it (including changed flows that
  respawn during the same reload AND newly-added flows) continues
  with the previously-resolved token via the by_id cache hit. To
  force propagation, change any field in every kept-alive flow that
  references the credential. If rotating because the old token was
  compromised, restart the daemon (`systemctl restart gcit`) rather
  than SIGHUP to ensure all flows use the new token immediately.

### Fixed

- The `LsRemote` polling strategy (used for every source URL that is
  not `github.com` (or `www.github.com`) or `git.kernel.org` —
  GitLab, self-hosted GitLab, Gitea, ssh remotes, file:// remotes,
  anything reached via gix-protocol) was emitting an incorrect V2
  ls-refs request that every real server rejected. Affects all
  0.1.0 deployments using ls-remote sources. gcit constructed the
  `LsRefsCommand::new(...)` `(feature_name, value)` tuple as
  `("gcit", None)`, putting the agent identity string in the
  feature-name slot. gix-protocol's V2 validator special-cases the
  literal feature name `"agent"` as the only unknown name a server
  can accept; any other unknown feature name (including `"gcit"`) is
  rejected with `UnsupportedCapability`. The fix passes
  `("agent", Some(Cow::Borrowed("gcit")))` so the identity string
  travels in the value slot. Operator-visible effect before the fix:
  every poll cycle against a V2-capable `LsRemote` source returned a
  `LsRemoteError::Permanent` error of the form `permanent: ls-refs
  parse/validation for "<url>": ls-refs: capability gcit is not
  supported`. The daemon stayed Ready, but the dispatcher never
  fired. The failure was visible only in journald via the
  `gcit::flow::poll` tracing target's WARN log records; `gcit
  status` showed no last_error for the affected flow. V1-only
  servers (legacy git daemons not advertising V2) continued to work
  because the V1 handshake delivers refs inline and the buggy
  LsRefsCommand path was skipped entirely. The GithubApi and
  Grokmirror strategies were unaffected; only ls-remote-via-gix
  flows speaking V2 shipped broken. After the fix, V2 ls-refs
  round-trips cleanly against any conformant server. Fixture
  coverage in `tests/poll_ls_remote.rs` exercises the file://
  transport path end-to-end and would have caught the original
  defect.
- Poll-cycle errors now surface in `gcit status` as
  `last_error.kind = "git_poll_failed"`. Previously the per-flow
  poll loop logged a `WARN target=gcit::flow::poll` record on every
  failure and continued on cadence, but never recorded the error
  into the `FlowLastError` map — so a flow whose poll returned
  errors every cycle (e.g. a 5xx upstream, a misconfigured URL,
  the V2 ls-refs bug above) appeared healthy in `gcit status`
  while only journald showed the actual failure mode. The fix
  records the strategy's display string into `last_error.message`
  alongside the new kind. The next successful poll observation
  (`Refreshed` or `Unchanged`) clears the entry, matching the
  existing post-respawn-stale-error behavior — the once-per-spawn
  clear flag now re-arms after each recorded error so a recover-
  then-fail-then-recover sequence cycles the entry correctly.
  Operators with monitoring rules keyed on `last_error` being
  absent may see new alerts post-upgrade for flows that were
  already failing silently; this is a visibility improvement, not
  a new failure.
- `PollOutcome::UnbornRef` (configured ref doesn't exist on the
  source — operator typo, branch deleted, branch not yet pushed)
  now surfaces in `gcit status` as `last_error.kind =
  "git_poll_failed"` with a message that names the offending ref,
  the source URL, and the operator's own `flow.<name>.source.ref`
  config key — `"ref refs/heads/main not found at
  https://github.com/o/r — verify the ref exists upstream, wait
  if it's still being pushed, or update flow.<name>.source.ref"`.
  Previously the poll loop logged a `WARN
  target=gcit::flow::poll` record on every UnbornRef cycle but
  never updated the FlowLastError map, so a flow whose configured
  ref was wrong appeared healthy in `gcit status`. The next
  successful poll (the ref appears upstream and resolves to a
  SHA) clears the entry via the existing once-per-recovery latch.
  Operators with monitoring rules keyed on `last_error` being
  absent may see new alerts post-upgrade for flows whose
  `source.ref` doesn't resolve; this is a visibility improvement,
  not a new failure.
- Grokmirror manifest fetches now bound at a 60-second wall-clock
  cap (`gcit::git::grokmirror::POLL_TIMEOUT`), matching
  `gcit::git::ls_remote::POLL_TIMEOUT`. A stalled mirror (TCP
  handshake completes but the server never sends bytes, or the
  path silently drops packets mid-body) previously could hold a
  per-flow poll task open until the supervisor's next reload —
  reqwest's own deadline (`http.request_timeout`, default 30s)
  caught the common case but covered only the reqwest pipeline,
  and operators who increased `http.request_timeout` to tolerate a
  legitimately slow upstream lost the backstop entirely. After the
  fix a stalled fetch surfaces as `last_error.kind =
  "git_poll_failed"` with message `"transient: manifest fetch
  timed out after 60s: <url>"` and the next poll cycle retries on
  the configured `source_interval` cadence.
- `PollOutcome::UnbornRef` cycles now emit a `PollTimestamp` so a
  flow whose configured ref is unborn shows accurate
  `last_poll_at` in `gcit status` instead of the stale spawn
  time. Previously an UnbornRef cycle recorded the `last_error`
  entry but never refreshed `last_poll_at`; the flow rendered as
  both errored AND stale even while it polled on cadence. The new
  `PollTimestamp` send mirrors the existing `Unchanged` arm's
  liveness contract for grokmirror's fingerprint short-circuit.

## [0.1.0] - 2026-04-26

Initial release.

### Added

- Daemon entry (`gcit run`) with `Type=notify` systemd lifecycle, foreground mode,
  signal handling for `SIGTERM`/`SIGINT`/`SIGHUP`, and a per-flow supervisor with
  child `CancellationToken` isolation so a panic in one flow cannot crash the
  daemon or affect other flows.
- TOML config parser with `deny_unknown_fields`, `humantime` durations, jitter
  bounds (`0.0..=0.5`), interval bounds (`15s..=24h`), unique flow-name
  validation, `[a-zA-Z0-9_-]+` length-bounded ids, ref-prefix and `owner/repo`
  validation, and per-error reporting with line numbers, value, and suggested fix.
- Credential resolution chain: `$CREDENTIALS_DIRECTORY` (systemd
  `LoadCredential=`), `GCIT_CREDENTIAL_<ID>` env var, then
  `<config_dir>/credentials/<id>` file. The file fallback requires
  mode `0600` and ownership by either the daemon's effective uid or
  root (root is accepted so an operator on a `DynamicUser=yes` unit
  can drop credential files via `sudo` — the transient daemon uid is
  not knowable in advance). `credential_id` validated against
  `^[a-zA-Z0-9_-]+$` (max 64 chars), with collision detection on the
  env-var mapping.
- Three git polling strategies, auto-selected by remote URL: GitHub API
  (`get_ref` via octocrab) for `github.com`, grokmirror manifest fingerprint
  for `git.kernel.org`, and ls-refs over `gix-protocol`/`gix-transport` for
  everything else.
- GitHub Actions dispatcher: `workflow_dispatch` via octocrab; per-credential
  `RateBucket` with primary + secondary rate-limit awareness; run id
  correlation via injected `gcit_run_id` UUID + `run-name` directive, with a
  `head_sha + created>=` fallback that warns when `run-name` is not set;
  5-minute appearance timeout (30s during drain). Run monitor polls
  `get_run` + `list_jobs` until terminal status.
- Notifiers (`Notifier` trait with native `async fn`):
  - Discord webhook via twilight: programmatic embeds with conclusion
    coloring, configurable handlebars templates with `set_strict_mode(true)`,
    and host allowlist (`discord.com`, `discordapp.com`, `ptb.discord.com`,
    `canary.discord.com`).
  - Local mail: appends mboxrd-formatted messages to `/var/mail/<user>` with
    `O_NOFOLLOW`, `flock(LOCK_EX)`, and `fsync`. Header-injection defense
    replaces control bytes in rendered headers; body capped at 64 KiB.
- State persistence: dedicated `std::thread` writer with bounded mpsc channel,
  order-preserving last-write-wins batching, and tempfile+persist atomic
  writes to `$STATE_DIRECTORY/state.json`. Schema version pinned (`1`);
  unknown versions rejected. Lock at `$RUNTIME_DIRECTORY/gcit.lock` via
  `fd-lock`.
- Control channel: socket-activated Unix socket (`gcit.socket`,
  `SocketMode=0600`) with `LengthDelimitedCodec` framing and JSON request
  bodies. Implemented requests: `Trigger`, `Status`, `Reload`, `Version`.
  Defense in depth via `SO_PEERCRED` peer-uid check and reload rate-limiting
  (1/sec).
- CLI subcommands per design table: `run`, `install`, `uninstall`, `check`,
  `status`, `trigger`, `reload`, `validate-template`, `completions`,
  `--version`. Exit codes mapped to `sysexits.h` (`EX_USAGE`, `EX_DATAERR`,
  `EX_OSERR`, `EX_TEMPFAIL`, `EX_CONFIG`).
- `gcit install`: interactive guided setup with credential walkthrough, local
  mail spool writability check, path-preview confirmation, install manifest
  (sha256 + mode + path) at `$STATE_DIRECTORY/.install-manifest.json`, and
  next-steps print-out.
- `gcit uninstall`: reverse install via the manifest. Files not in the
  manifest are never touched. Operator-modified files (manifest sha mismatch)
  refused without `--force`. Preserves `$STATE_DIRECTORY` data.
- systemd hardening profile: `DynamicUser=yes` (default; switched to
  `User=gcit` + `Group=mail` when any `local_mail` destination is
  configured), `NoNewPrivileges`, `ProtectSystem=strict`, `ProtectHome`,
  `PrivateTmp`, `PrivateDevices`, `ProtectKernelTunables/Modules/Logs`,
  `ProtectControlGroups`, `ProtectClock`, `ProtectHostname`,
  `ProtectProc=invisible`, `ProcSubset=pid`, `RestrictAddressFamilies=AF_UNIX
  AF_INET AF_INET6`, `RestrictNamespaces`, `RestrictRealtime`,
  `RestrictSUIDSGID`, `LockPersonality`, `MemoryDenyWriteExecute`,
  `SystemCallFilter=@system-service`, `SystemCallArchitectures=native`,
  empty `CapabilityBoundingSet=` and `DeviceAllow=`, `UMask=0077`, plus
  `RuntimeDirectory`/`StateDirectory`/`ConfigurationDirectory` with `0700`
  modes (`ConfigurationDirectory` `0750`).
- SIGHUP reload: live config reload with `Reloading` + `MonotonicUsec`
  notifications. Each existing flow is diffed against its previous
  configuration: unchanged flows keep their poll/dispatcher pair (and
  any in-flight monitor) running; changed flows are cancelled,
  drained, and respawned under a fresh `CancellationToken`; removed
  flows additionally emit `FlowRemoved` so persisted state is dropped.
  Source-URL changes also drop persisted state so the freshly-spawned
  poll loop does not seed its baseline from a SHA observed against a
  different repo. Cached credentials are invalidated up front so a
  rotated PAT is picked up without a daemon restart.
- Tracing with `tracing-subscriber` + journald layer. The `--log-filter`
  CLI flag is the only operative source. The config-file `[log] filter`
  field is parsed but not yet wired into log init; gcit does not honour
  any `RUST_LOG`-style env var. The built-in default is `info,gcit=debug`.
- Reproducible release builds: `vergen-gix` embeds the git SHA into
  `gcit --version`. Static binary build target verified in CI
  (`x86_64-unknown-linux-musl`, asserted via `file`).
- CI inline in `.github/workflows/ci.yml`: `cargo fmt --check`, `cargo clippy
  -D warnings`, `cargo llvm-cov nextest --fail-under-lines 90`, codecov
  upload, musl static-binary build, `cargo audit`, `cargo deny check`
  (advisories, bans, sources, licenses), `mdbook` build/test/linkcheck,
  `systemd-analyze security` gate, and per-PR `cargo mutants --in-diff` with
  PR-comment scoring.

### Security

- All credential values wrapped in `secrecy::SecretString`; `Debug` and
  `Display` redact to `[REDACTED]`. CLI flags carrying credential values
  are rejected at parse time.
- `tracing` instrumentation skips credential field names
  (`credential`, `secret`, `token`, `password`).
- `gcit status` and `gcit trigger --dry-run` redact credential values in
  any rendered output.
- HTTP body size cap (16 MiB) across octocrab, twilight-http, reqwest, and
  gix-transport. Per-request timeout from `[http] request_timeout`.
- Handlebars `set_strict_mode(true)` plus deregistered control-flow helpers
  and partials. Untrusted template DATA (sha, ref names, run conclusion,
  job names) renders as inert text and cannot be re-interpreted as
  template fragments. Asserted via unit test with malicious payload.
- Local mail: `O_NOFOLLOW` open refuses symlink redirects; header
  sanitization replaces control bytes (defense against `\r\n`-delimited
  header injection); 64 KiB body cap prevents oversized notifications.
- `Cargo.lock` committed; `cargo audit` and `cargo deny check` run in CI.

[Unreleased]: https://github.com/likewhatevs/gcit/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/likewhatevs/gcit/releases/tag/v0.1.0

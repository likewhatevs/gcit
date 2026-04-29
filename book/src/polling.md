# Polling strategies

gcit picks one of three polling strategies for each flow based on the
source URL. There is no `strategy=` config field; auto-detection is
deterministic and exact. You cannot override the strategy choice — if
you need a different one, change the URL.

## Auto-detection rules

The strategy is selected by host match on the parsed URL:

| host | strategy | default interval |
|---|---|---|
| `github.com` / `www.github.com` | `GithubApi` | 60s |
| `git.kernel.org` | `Grokmirror` | 60s |
| anything else (or an unparseable URL) | `LsRemote` | 5m |

Host comparison is case-insensitive but exact: `api.github.com`,
`github.com.evil.example.com`, and `www.kernel.org` all fall through to
`LsRemote`. URLs that fail to parse fall through to `LsRemote`, which
errors at connect time with a clear message.

Effective interval has a 15-second floor after jitter is applied. Jitter
samples uniformly in `[-jitter, +jitter]` and the resulting duration is
clamped to `MIN_INTERVAL = 15s`.

## `GithubApi`

Uses [octocrab](https://docs.rs/octocrab)'s `get_ref` endpoint
(`GET /repos/{owner}/{repo}/git/ref/{ref_path}`) to resolve a single ref
in one HTTP round trip. Cheapest of the three strategies; lowest default
interval (60s) because GitHub explicitly publishes per-ref endpoints.

- **Auth.** Optional. If `[flow.source]` declares a `credential_id`, the
  resolved token is supplied as a Bearer header. Public repos can be
  polled anonymously.
- **Ref support.** Only `refs/heads/*` and `refs/tags/*`. Any other ref
  syntax is rejected at config load.
- **Errors.**
  - 404 maps to `PollOutcome::UnbornRef` (the ref does not exist).
  - 403 / 429 map to `Transient` and the caller awaits the rate-limit
    reset.
  - 5xx and network errors map to `Transient`.
  - Other 4xx map to `Permanent`.

## `Grokmirror`

Used for `git.kernel.org`. Fetches a single `manifest.js.gz` file that
describes every repo on the mirror. Comparing fingerprints across polls
detects changes for many repos with one HTTP request.

The wire format on kernel.org is a static `.gz` file (no
`Content-Encoding: gzip`), so reqwest's transparent decompression does not
apply. gcit decodes the body explicitly via `flate2::read::GzDecoder`.

- **Caps.**
  - Compressed body: 64 MiB cap (kernel.org's manifest is ~1-2 MiB).
  - Decompressed body: 16 MiB cap.
  - Wall-clock: 60-second timeout per round trip.
- **Cache.** The poll task caches the previous fingerprint in memory.
  Matching fingerprint → `PollOutcome::Unchanged` (no per-ref lookup).
  Mismatch → the strategy returns the new fingerprint and a follow-up
  ls-remote resolution to obtain the per-ref SHA.
- **Errors.**
  - Network / 5xx / decode noise → `Transient`.
  - Configured repo not in the manifest → `RepoNotInManifest` →
    `PollOutcome::UnbornRef`.
  - Configuration / decode errors that won't recover → `Permanent`.

## `LsRemote`

Anything that isn't `github.com` or `git.kernel.org`. Uses
[gix-protocol](https://docs.rs/gix-protocol)'s stateless ls-refs over the
configured transport (`https://`, `git://`, `ssh://`, `file://`, scp-style
`git@host:path`).

- **Auth.** Optional. For HTTPS URLs to private repos, gix-protocol's
  handshake invokes gcit's authenticate callback when the server returns
  401; the callback resolves the credential and replies. For SSH URLs the
  SSH agent / configured key handles auth out of band. For `file://` URLs
  no auth is needed.
- **Ref support.** Any ref name that exists in the remote's ls-refs
  output. Lightweight tags resolve to the commit they point at; annotated
  tags resolve to the tag SHA itself (no peel).
- **Concurrency.** Blocking-IO calls run on the tokio blocking pool.
- **Errors.** Transport-level failures classify as `Transient`; missing
  refs and unborn repos surface as `PollOutcome::UnbornRef`.

## SHA-diff comparator

Every strategy yields a `PollOutcome`. The supervisor pairs each
`Refreshed { sha }` with the previously recorded `last_sha`:

| `last_sha` | `observed` | trigger? |
|---|---|---|
| `None` (first poll) | any | no — record baseline |
| `Some(prev)` | `prev` | no — unchanged |
| `Some(prev)` | `!= prev` | **yes** |

`UnbornRef` and `Unchanged` never fire, but both still trigger a
`PollObservation` so `gcit status` can report "polled <timestamp>" without
showing a stale "no activity" indicator.

## Workflow dispatch correlation

GitHub's `workflow_dispatch` API does not return the resulting run id.
gcit correlates the dispatch to the spawned run by:

1. Generating a UUID at dispatch time and injecting it into the
   `workflow_dispatch` `inputs` payload as `gcit_run_id`.
2. Polling `list_workflow_runs` and matching by
   `Run.name.contains("gcit-<uuid>")`.

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
run. The fallback emits a `WARN` recommending the `run-name` directive.

## Per-flow interval overrides

A flow can override the top-level `[poll]` defaults under `[flow.poll]`.
Resolution order:

1. `[flow.poll].source_interval`
2. `[poll].source_interval`
3. The strategy's built-in default (60s for GithubApi/Grokmirror, 5m for
   LsRemote).

Same fallback chain for `job_interval` and `jitter`. Any value outside the
documented bounds is rejected at config load — the validator does not
silently clamp.

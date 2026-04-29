// Git polling strategies.
//
// Three poll strategies are auto-detected from the source URL:
//   - `GithubApi` (github.com / www.github.com): octocrab `get_ref`
//     returns the SHA for one branch/tag in a single HTTP round
//     trip. Default 60s.
//   - `Grokmirror` (git.kernel.org): one cheap GET against
//     `/manifest.js.gz` returns fingerprints for every repo on the
//     mirror. Default 60s.
//   - `LsRemote` (anything else): gix-protocol stateless ls-refs
//     against the configured transport (https/git/ssh/file).
//     Default 5m, anonymous-only.
//
// All strategies enforce a 15s floor on effective interval after
// jitter is applied.
//
// `PollOutcome` is the strategy-agnostic return shape — see the
// individual modules for the per-strategy error types and the rate
// bucket helper that all three share at the supervisor layer.
//
// Items here are pub for integration-test reachability and treated
// as crate-internal + unstable.

pub mod github_api;
pub mod grokmirror;
pub mod ls_remote;
pub mod rate_bucket;
pub mod strategy;

pub use rate_bucket::RateBucket;
pub use strategy::{
    apply_jitter, auto_detect, compare_sha, default_interval, kind_str, DiffOutcome, PollStrategy,
    MIN_INTERVAL,
};

use gix_hash::ObjectId;

/// Result of a single poll cycle. Strategy-agnostic — every
/// `github_api`, `grokmirror`, `ls_remote` strategy maps its
/// per-implementation success / not-found / no-change signal into
/// one of these variants.
///
/// The dispatcher's per-flow loop pairs `Refreshed { sha }` with the
/// previously recorded `last_sha` (via `compare_sha`) to decide
/// whether to fire a `TriggerSignal`. `UnbornRef` and `Unchanged`
/// never fire, but both still trigger a `PollObservation` so
/// `gcit status` can report "polled <timestamp>" without showing a
/// stale "no activity" indicator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollOutcome {
    /// The source's current SHA was successfully fetched. The caller
    /// performs the LWW SHA diff against the stored last_sha to
    /// decide whether to emit a TriggerSignal.
    Refreshed { sha: ObjectId },
    /// The configured ref does not exist on the remote (404,
    /// `Unborn` from gix-protocol, repo missing from grokmirror's
    /// manifest, etc). The caller logs WARN with the ref + URL,
    /// keeps polling on cadence (no backoff — this is permanent
    /// from a backoff-classification perspective but recoverable if
    /// the operator pushes the branch).
    UnbornRef,
    /// The strategy's cheap-check determined nothing has changed
    /// since the last poll (currently only emitted by the grokmirror
    /// strategy on a fingerprint match). The caller skips the
    /// per-ref lookup entirely. `Refreshed { sha }` never carries
    /// "unchanged" semantics — the caller is responsible for
    /// comparing against the previously recorded last_sha. This
    /// variant exists only for the grokmirror short-circuit.
    Unchanged,
}

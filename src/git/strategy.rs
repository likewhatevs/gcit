// Strategy auto-detection, default intervals, jitter math, SHA diff.
//
// Three strategies (GithubApi 60s, Grokmirror 60s, LsRemote 5m) are
// auto-detected from the URL — there is no `strategy=` config field.
// Effective interval has a 15s floor. Polling is jittered and
// rate-bucketed; a SHA diff produces a TriggerSignal. `jitter` is a
// unitless 0.0..=0.5 fraction of the base interval; effective
// interval = base * (1 ± random_in[0, jitter]).
//
// This file is pure logic — no I/O, no async, no global state. Every
// function is deterministic given (inputs, &mut Rng). Tests under
// `tests/poll_strategy_detect.rs`, `tests/poll_jitter_bounds.rs`, and
// `tests/poll_sha_comparison.rs` pin every documented invariant.

use std::time::Duration;

use gix_hash::ObjectId;

// Single source of truth for the 15s effective-interval floor.
// `config::validate::MIN_INTERVAL` is the canonical declaration —
// strategy.rs re-exports for ergonomics inside `git::*` callers
// (so `git::MIN_INTERVAL` works) without forking the constant.
pub use crate::config::validate::MIN_INTERVAL;

/// Strategy used to poll a source git URL. Auto-detected by
/// `auto_detect`; never configured directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollStrategy {
    /// `github.com` / `www.github.com`. Uses octocrab's `get_ref` to
    /// resolve a single ref. Cheapest of the three; lowest default
    /// interval (60s) because GitHub explicitly publishes per-ref
    /// endpoints.
    GithubApi,
    /// `git.kernel.org`. Uses the per-host `manifest.js.gz` to detect
    /// changes across many repos with one HTTP request. Default 60s.
    Grokmirror,
    /// Anything else. Uses gix-protocol's stateless ls-refs over the
    /// configured transport (https/git/ssh/file). 5m default keeps
    /// politeness against arbitrary servers.
    LsRemote,
}

/// Per-strategy default poll interval, used when neither
/// `flow.poll.source_interval` nor `[poll].source_interval` is set.
pub fn default_interval(strategy: PollStrategy) -> Duration {
    match strategy {
        PollStrategy::GithubApi => Duration::from_secs(60),
        PollStrategy::Grokmirror => Duration::from_secs(60),
        PollStrategy::LsRemote => Duration::from_secs(300),
    }
}

/// Stable string label used by tests + `gcit status`. Snake-case to
/// match the project-wide convention used for serde renames (action
/// kinds, destination kinds, FireEvent variants).
pub fn kind_str(strategy: PollStrategy) -> &'static str {
    match strategy {
        PollStrategy::GithubApi => "github_api",
        PollStrategy::Grokmirror => "grokmirror",
        PollStrategy::LsRemote => "ls_remote",
    }
}

/// Detect the polling strategy from a source URL. Pure function.
/// Match is exact on the host component — `api.github.com`,
/// `github.com.evil.example.com`, `www.kernel.org` all fall through
/// to LsRemote (per `tests/poll_strategy_detect.rs` edge cases).
///
/// `gix_url::parse` handles all the supported forms (https://..., ssh://...,
/// git://..., file://..., scp-style git@host:path). For unparseable
/// inputs we fall back to LsRemote — the LsRemote strategy errors out
/// at connect time with a clear message naming the URL, so callers
/// see actionable diagnostics rather than a silent misroute.
pub fn auto_detect(url: &str) -> PollStrategy {
    // gix_url::parse takes `&BStr`. The transitive bstr dep exposes a
    // From<&[u8]> impl on &BStr, so url.as_bytes().into() is enough
    // without adding a direct bstr dependency.
    let parsed = match gix_url::parse(url.as_bytes().into()) {
        Ok(u) => u,
        Err(_) => return PollStrategy::LsRemote,
    };
    // host() returns the host component as a &str when present;
    // None for transports that don't carry one (e.g. file://path).
    let host = match parsed.host() {
        Some(h) => h,
        None => return PollStrategy::LsRemote,
    };
    if host.eq_ignore_ascii_case("github.com") || host.eq_ignore_ascii_case("www.github.com") {
        PollStrategy::GithubApi
    } else if host.eq_ignore_ascii_case("git.kernel.org") {
        PollStrategy::Grokmirror
    } else {
        PollStrategy::LsRemote
    }
}

/// Apply jitter to a base poll interval. Returns a Duration in the
/// closed interval `[base * (1 - jitter), base * (1 + jitter)]`, then
/// clamped upward to MIN_INTERVAL.
///
/// `jitter` MUST be in `[0.0, 0.5]` (config validation enforces this
/// at parse time). For defense-in-depth, values outside the range
/// are clamped to the nearest endpoint here so a programming error
/// in the caller cannot inflate the actual interval beyond ±50%.
///
/// The RNG is taken by `&mut` so callers can seed it for
/// determinism. Production passes a process-lifetime
/// `fastrand::Rng` (the random walk is cheap; per-call seeding is
/// not necessary).
pub fn apply_jitter(base: Duration, jitter: f64, rng: &mut fastrand::Rng) -> Duration {
    let j = jitter.clamp(0.0, 0.5);
    let base_secs = base.as_secs_f64();
    // Sample uniformly in [-j, +j] so the distribution is symmetric
    // around the base interval. fastrand::Rng::f64 returns [0.0, 1.0).
    let sample = rng.f64() * 2.0 - 1.0; // (-1.0, 1.0)
    let factor = 1.0 + sample * j; // (1 - j, 1 + j)
    let scaled = (base_secs * factor).max(0.0);
    let dur = Duration::from_secs_f64(scaled);
    if dur < MIN_INTERVAL {
        MIN_INTERVAL
    } else {
        dur
    }
}

/// Outcome of comparing the just-observed SHA against the previously
/// recorded one. Pure function of (last, observed) — does not consult
/// any side state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffOutcome {
    /// Always `Some(observed)` — the caller threads this into a
    /// `StateUpdate::PollObservation`. Even when the SHA is unchanged
    /// the observation refreshes `last_poll_at`.
    pub observed: ObjectId,
    /// Whether the dispatcher should fire on this observation. False
    /// on the first poll (`last == None`, baseline) and on equal
    /// SHAs; true only when `last` and `observed` differ.
    pub trigger: bool,
}

/// Compare the just-observed SHA against the previously recorded one
/// (SHA diff -> TriggerSignal). Rules pinned by
/// `tests/poll_sha_comparison.rs`:
///   - `last == None`: baseline; record observation, NO trigger.
///   - `last == Some(observed)`: unchanged; record observation, NO trigger.
///   - `last != observed`: changed; record observation, FIRE trigger.
pub fn compare_sha(last: Option<ObjectId>, observed: ObjectId) -> DiffOutcome {
    let trigger = match last {
        None => false,
        Some(prev) => prev != observed,
    };
    DiffOutcome { observed, trigger }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::util::test_sha as sha;

    #[test]
    fn auto_detect_github_variants() {
        for url in [
            "https://github.com/torvalds/linux.git",
            "ssh://git@github.com/torvalds/linux.git",
            "https://github.com/torvalds/linux",
            "https://www.github.com/torvalds/linux.git",
            "https://github.com/octocat/Hello-World.git",
        ] {
            assert_eq!(auto_detect(url), PollStrategy::GithubApi, "url={url}");
        }
    }

    #[test]
    fn auto_detect_kernel_org() {
        for url in [
            "https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git",
            "https://git.kernel.org",
            "https://git.kernel.org/pub/scm/linux/kernel/git/stable/linux.git",
        ] {
            assert_eq!(auto_detect(url), PollStrategy::Grokmirror, "url={url}");
        }
    }

    #[test]
    fn auto_detect_falls_back_to_ls_remote() {
        for url in [
            "https://gitlab.com/foo/bar.git",
            "https://git.example.com/proj.git",
            "https://git.sr.ht/~user/proj",
            "git://anonscm.example.org/foo.git",
            "ssh://git@example.com/foo.git",
        ] {
            assert_eq!(auto_detect(url), PollStrategy::LsRemote, "url={url}");
        }
    }

    #[test]
    fn auto_detect_subdomain_lookalikes_dont_match() {
        // Per `tests/poll_strategy_detect.rs::not_github_subdomain`
        // and similar: hostname must be exact. api.github.com, www.kernel.org,
        // and github.com.evil.example.com all fall through to LsRemote.
        for url in [
            "https://api.github.com/foo.git",
            "https://www.kernel.org/foo.git",
            "https://github.com.evil.example.com/foo.git",
        ] {
            assert_eq!(auto_detect(url), PollStrategy::LsRemote, "url={url}");
        }
    }

    #[test]
    fn auto_detect_malformed_falls_back_to_ls_remote() {
        for url in ["", "hello world"] {
            assert_eq!(auto_detect(url), PollStrategy::LsRemote, "url={url:?}");
        }
    }

    #[test]
    fn default_intervals_match_documented_values() {
        assert_eq!(
            default_interval(PollStrategy::GithubApi),
            Duration::from_secs(60)
        );
        assert_eq!(
            default_interval(PollStrategy::Grokmirror),
            Duration::from_secs(60)
        );
        assert_eq!(
            default_interval(PollStrategy::LsRemote),
            Duration::from_secs(300)
        );
    }

    #[test]
    fn jitter_zero_returns_base() {
        let mut rng = fastrand::Rng::with_seed(1);
        for _ in 0..1000 {
            let d = apply_jitter(Duration::from_secs(60), 0.0, &mut rng);
            assert_eq!(d, Duration::from_secs(60));
        }
    }

    #[test]
    fn jitter_within_bounds() {
        let mut rng = fastrand::Rng::with_seed(42);
        for _ in 0..10_000 {
            let d = apply_jitter(Duration::from_secs(60), 0.1, &mut rng);
            // Lower bound 54s, upper bound 66s. 15s floor doesn't apply
            // here because 54 > 15.
            assert!(d.as_secs() >= 54, "lower bound: {d:?}");
            assert!(d.as_secs() <= 66, "upper bound: {d:?}");
        }
    }

    #[test]
    fn jitter_floor_clamps_below_15s() {
        let mut rng = fastrand::Rng::with_seed(123);
        // base 20s, jitter 0.5 -> raw lower bound 10s; must clamp to 15s.
        for _ in 0..5_000 {
            let d = apply_jitter(Duration::from_secs(20), 0.5, &mut rng);
            assert!(d >= MIN_INTERVAL, "floor breach: {d:?}");
        }
    }

    #[test]
    fn jitter_clamps_oversized_input() {
        // Defense in depth: jitter > 0.5 should not blow past ±50%.
        let mut rng = fastrand::Rng::with_seed(7);
        for _ in 0..1_000 {
            let d = apply_jitter(Duration::from_secs(60), 1.0, &mut rng);
            assert!(d.as_secs() <= 90, "clamp upper: {d:?}");
            assert!(d.as_secs() >= 30, "clamp lower (post-floor): {d:?}");
        }
    }

    #[test]
    fn compare_sha_first_poll_no_trigger() {
        let observed = sha(0xaa);
        let outcome = compare_sha(None, observed);
        assert_eq!(outcome.observed, observed);
        assert!(!outcome.trigger);
    }

    #[test]
    fn compare_sha_unchanged_no_trigger() {
        let s = sha(0xaa);
        let outcome = compare_sha(Some(s), s);
        assert_eq!(outcome.observed, s);
        assert!(!outcome.trigger);
    }

    #[test]
    fn compare_sha_changed_triggers() {
        let prev = sha(0xaa);
        let curr = sha(0xbb);
        let outcome = compare_sha(Some(prev), curr);
        assert_eq!(outcome.observed, curr);
        assert!(outcome.trigger);
    }

    #[test]
    fn compare_sha_is_pure() {
        let prev = sha(0x01);
        let curr = sha(0x02);
        let a = compare_sha(Some(prev), curr);
        let b = compare_sha(Some(prev), curr);
        assert_eq!(a, b);
    }

    #[test]
    fn kind_str_round_trip() {
        assert_eq!(kind_str(PollStrategy::GithubApi), "github_api");
        assert_eq!(kind_str(PollStrategy::Grokmirror), "grokmirror");
        assert_eq!(kind_str(PollStrategy::LsRemote), "ls_remote");
    }
}

// PollStrategy::auto_detect from URL.
// PollStrategy variants + defaults:
//   GithubApi    <-  github.com / www.github.com  (default 60s)
//   Grokmirror   <-  git.kernel.org              (default 60s)
//   LsRemote     <-  anything else               (default 5m)
// No strategy= config field. URL is source of truth.
// 15s floor.
//
// Pure-logic table test through the public `gcit::git::*` API. No
// filesystem, no async. Mutation target: the hostname-match arm in
// `auto_detect`.

use std::time::Duration;
use std::time::Instant;

use rstest::rstest;

use gcit::git::{auto_detect, default_interval, kind_str};

#[rstest]
// GithubApi: github.com (with/without scheme, with/without www, with/without .git)
#[case::github_https("https://github.com/torvalds/linux.git", "github_api", 60)]
#[case::github_ssh("ssh://git@github.com/torvalds/linux.git", "github_api", 60)]
#[case::github_no_dot_git("https://github.com/torvalds/linux", "github_api", 60)]
#[case::github_www("https://www.github.com/torvalds/linux.git", "github_api", 60)]
#[case::github_with_path("https://github.com/octocat/Hello-World.git", "github_api", 60)]
// Grokmirror: git.kernel.org (its full path forms)
#[case::kernel_https(
    "https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git",
    "grokmirror",
    60
)]
#[case::kernel_no_path("https://git.kernel.org", "grokmirror", 60)]
#[case::kernel_subpath(
    "https://git.kernel.org/pub/scm/linux/kernel/git/stable/linux.git",
    "grokmirror",
    60
)]
// LsRemote: everything else
#[case::gitlab("https://gitlab.com/foo/bar.git", "ls_remote", 300)]
#[case::self_hosted("https://git.example.com/proj.git", "ls_remote", 300)]
#[case::sourcehut("https://git.sr.ht/~user/proj", "ls_remote", 300)]
#[case::git_protocol("git://anonscm.example.org/foo.git", "ls_remote", 300)]
#[case::ssh_other("ssh://git@example.com/foo.git", "ls_remote", 300)]
// Edge cases: hostname must MATCH, not just contain, "github.com" etc.
#[case::not_github_subdomain("https://api.github.com/foo.git", "ls_remote", 300)]
#[case::not_kernel_subdomain("https://www.kernel.org/foo.git", "ls_remote", 300)]
#[case::lookalike_phishing("https://github.com.evil.example.com/foo.git", "ls_remote", 300)]
fn auto_detect_strategy_and_default_interval(
    #[case] url: &str,
    #[case] expect_kind: &str,
    #[case] expect_default_seconds: u64,
) {
    let strategy = auto_detect(url);
    assert_eq!(
        kind_str(strategy),
        expect_kind,
        "URL {url:?} -> {expect_kind}"
    );
    assert_eq!(
        default_interval(strategy),
        Duration::from_secs(expect_default_seconds),
        "URL {url:?} default interval",
    );
}

#[rstest]
#[case::malformed_no_scheme("github.com/foo")]
#[case::empty("")]
#[case::not_a_url("hello world")]
#[case::file_url("file:///var/git/foo.git")]
fn auto_detect_malformed_falls_back_to_ls_remote(#[case] url: &str) {
    // Per src/git/strategy.rs::auto_detect: unparseable URLs and URLs
    // without a host (file://) fall through to LsRemote. The LsRemote
    // strategy then errors at connect time with a clear message naming
    // the URL — operators see actionable diagnostics rather than a
    // silent misroute.
    let s = auto_detect(url);
    assert_eq!(
        kind_str(s),
        "ls_remote",
        "url {url:?} must default to ls_remote"
    );
}

#[test]
fn auto_detect_is_fast_and_does_no_io() {
    // auto_detect must NOT touch the network or filesystem. It's a
    // string match. Run it 1000 times against a representative URL;
    // assert the loop completes in < 100ms wall clock. Catches a
    // regression where someone makes auto_detect "smarter" by probing
    // the URL.
    let start = Instant::now();
    for _ in 0..1_000 {
        std::hint::black_box(auto_detect("https://github.com/torvalds/linux.git"));
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(100),
        "auto_detect must be IO-free; 1000 calls took {elapsed:?}",
    );
}

#[test]
fn min_interval_floor_pinned_at_15s() {
    // 15s floor. config validate enforces this at parse
    // time; the constant is shared via gcit::git::MIN_INTERVAL.
    assert_eq!(gcit::git::MIN_INTERVAL, Duration::from_secs(15));
}

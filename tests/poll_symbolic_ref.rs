// Symbolic ref (HEAD) following.
//
// refs/heads/* are concrete refs; HEAD is a symbolic ref that points
// at one of them (typically refs/heads/main or refs/heads/master).
// gcit's `flow.source.ref` is validated at config-load to start with
// the literal `refs/` prefix, so an operator must spell out the full
// ref path (e.g. `refs/heads/master`) — bare names like `HEAD` are
// rejected before the daemon ever issues a poll.
//
// The interesting question is: when the operator configures
// `source.ref = "refs/heads/main"` and the upstream's `main` is
// SOMETIMES a symbolic ref pointing elsewhere, does gcit follow the
// symref or report on it directly?
//
// This is per-strategy:
//   - GithubApi: octocrab's get_ref returns the resolved SHA regardless
//     of symref; symbolic refs are transparent to the API.
//   - Grokmirror: manifest fingerprints are per-repo, not per-ref; symref
//     question doesn't apply.
//   - LsRemote: gix-protocol ls-refs returns symbolic refs as-is. gix
//     resolves them via the v2 protocol's symref support. gcit's adapter
//     must follow.
//
// gcit always reports the FINAL SHA (the commit). Symref following
// happens transparently. Operators care about "did the commit change",
// not "did the symbolic pointer change".

use std::time::Duration;

mod common;
use common::bare_repo::{commit_with_message, file_url_for, init_bare_repo, update_ref};

use gcit::git::ls_remote;
use gcit::git::PollOutcome;

/// Pathological symref cycle: a flow whose configured ref participates
/// in a cycle (`refs/heads/foo -> refs/heads/main -> refs/heads/foo`)
/// must NOT hang gcit's poll task. The test fixture writes the symref
/// loop directly into the bare repo's `refs/heads/*` files (bypassing
/// `git symbolic-ref`, which doesn't expose a `--no-deref` flag in
/// upstream git for direct cycle creation) and points a separate
/// `refs/heads/safe` at a real commit so the advertisement is not
/// completely empty.
///
/// Empirically (verified against system git via `git ls-remote`),
/// git-upload-pack silently elides the cyclic refs from the
/// advertisement: ls-remote returns refs/heads/safe but no entry for
/// the looping refs. The cycle is detected and stripped at the
/// server, never reaching gcit's wire path. gcit's V2 ls-refs
/// response therefore contains the surviving ref(s); `find_ref`
/// iterates a flat Vec once with no recursion (see
/// src/git/ls_remote.rs::find_ref) and either finds the configured
/// ref directly or returns `NotPresent` -> `UnbornRef`.
///
/// The safety property this test pins:
///   - gcit returns within seconds (well under POLL_TIMEOUT) — there
///     is no client-side cycle-following loop in `find_ref` or in
///     `gix_protocol::ls_refs::LsRefsCommand::invoke_blocking`.
///   - A flow configured against the cycle resolves to UnbornRef
///     (the ref is absent from the elided advertisement), which the
///     supervisor surfaces as `last_error.kind = "git_poll_failed"`
///     with the unborn-ref message — operator gets an actionable
///     diagnostic rather than a hanging poll task.
///
/// Wraps the call in `tokio::time::timeout(5s, ...)` as a paranoia
/// safety net: if a future gix release ever follows symref cycles
/// client-side without bounding (which would itself be a gix bug),
/// the wrapper catches it before the test runner's overall timeout.
#[tokio::test]
async fn ls_remote_circular_symref_handled_safely() {
    let bare = init_bare_repo();
    let path = bare.path();

    // Build a real commit and point `refs/heads/safe` at it so the
    // advertisement contains at least one valid ref. Without this,
    // an empty repo + cyclic refs could be confused with the
    // unborn-empty-repo path that
    // tests/poll_ls_remote.rs::poll_empty_repo_returns_unborn_ref_not_panic
    // already covers.
    let commit_sha = commit_with_message(path, "safe content\n", "safe commit");
    update_ref(path, "refs/heads/safe", &commit_sha);

    // Inject the symref cycle directly: write the canonical
    // `ref: <target>` body into each loose-ref file. `git
    // update-ref` and `git symbolic-ref` both refuse to create
    // cycles by default; writing the files manually bypasses those
    // guards, which is exactly what a corrupted server-side repo
    // would look like in production.
    std::fs::write(path.join("refs/heads/foo"), "ref: refs/heads/main\n")
        .expect("write refs/heads/foo as symref to main");
    std::fs::write(path.join("refs/heads/main"), "ref: refs/heads/foo\n")
        .expect("write refs/heads/main as symref to foo");

    let url = file_url_for(path);

    // Configured ref points INTO the cycle. Server elides cyclic
    // refs from the advertisement, so gcit's poll sees no entry
    // for refs/heads/main and returns UnbornRef. Wrap in an outer
    // 5s timeout — well under POLL_TIMEOUT (60s) — to catch any
    // future regression that lets a cycle wedge gcit's poll task.
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        ls_remote::poll(url.clone(), "refs/heads/main".to_string()),
    )
    .await
    .expect("poll must return well under POLL_TIMEOUT regardless of symref cycle")
    .expect("cyclic ref must surface as Ok(UnbornRef), not Err");
    assert_eq!(
        outcome,
        PollOutcome::UnbornRef,
        "configured ref participates in a server-elided cycle -> UnbornRef",
    );

    // Companion assertion: the non-cyclic ref refs/heads/safe is
    // unaffected and resolves to its commit SHA. A regression that
    // failed the entire ls-refs call on encountering any cyclic ref
    // (rather than just eliding the affected ones) would break this
    // half of the test.
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        ls_remote::poll(url, "refs/heads/safe".to_string()),
    )
    .await
    .expect("poll must return well under POLL_TIMEOUT for the non-cyclic ref")
    .expect("non-cyclic ref must surface as Ok(Refreshed)");
    match outcome {
        PollOutcome::Refreshed { sha } => {
            assert_eq!(
                format!("{sha}"),
                commit_sha,
                "non-cyclic ref must resolve to its commit SHA verbatim",
            );
        }
        other => panic!("expected Refreshed for refs/heads/safe, got {other:?}"),
    }
}

// bare-repo fixture helpers moved to tests/common/bare_repo.rs; both
// tests/poll_ls_remote.rs and this file now share the same canonical
// copy.

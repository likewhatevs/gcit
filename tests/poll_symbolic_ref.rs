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

use tempfile::TempDir;

use gcit::git::ls_remote;
use gcit::git::PollOutcome;

#[tokio::test]
#[ignore = "covered by tests/poll_ls_remote.rs::poll_resolves_head_on_populated_repo_to_commit_sha — bare-repo fixture exercises Ref::Symbolic round-trip end-to-end"]
async fn ls_remote_follows_symref_to_concrete_ref() {
    let _dir = TempDir::new().unwrap();
    // Bare repo with HEAD -> refs/heads/main, refs/heads/main = SHA A.
    // Configure source.ref = "refs/heads/main"; ls-remote returns SHA A.
    //
    // Pin: gcit reports SHA A, not the symref string itself.
    //
    // Note: "configured ref is a concrete refs/
    // path; symbolic ref is the upstream's HEAD". A user-friendlier
    // alternative would be source.ref = "HEAD" -> follow symref. But
    // gcit's config-load validator requires ref to start with
    // "refs/", so "HEAD" alone is rejected. Recommend: keep the
    // explicit-refs-path requirement; operators who want "always-
    // follow-default-branch" can configure refs/heads/main and rely
    // on conventional naming. flag.
}

#[tokio::test]
#[ignore = "covered by tests/poll_ls_remote.rs::poll_missing_ref_in_populated_repo_returns_unborn — \
            absent-ref-in-advertisement maps to PollOutcome::UnbornRef regardless of why the ref is absent"]
async fn ls_remote_handles_default_branch_rename() {
    let _dir = TempDir::new().unwrap();
    // Operator configured source.ref = "refs/heads/master". Upstream
    // renames default branch master -> main; the ref refs/heads/master
    // is deleted.
    //
    // gcit's poll: ls-remote returns no entry for refs/heads/master.
    // Behavior = unborn ref (cross-reference tests/poll_unborn_ref.rs).
    // Operator must update config to refs/heads/main.
    //
    // The WARN message recommended in poll_unborn_ref.rs::actionable_text
    // is the operator's hint that this happened.
}

#[tokio::test]
#[ignore = "covered by tests/poll_github_api.rs::get_ref_returns_sha_for_existing_branch + \
            get_ref_404_returns_unborn_ref_outcome — wiremock pins the explicit-ref / 404 split"]
async fn github_api_handles_default_branch_via_explicit_ref() {
    // GET /repos/o/r/git/ref/heads/main returns 200 with the SHA.
    // GET /repos/o/r/git/ref/heads/nonexistent returns 404.
    //
    // gcit treats both deterministically: the configured ref is what
    // we ask for. No automatic fallback to HEAD.
    //
    // Pin: gcit does NOT silently follow GitHub's "default_branch"
    // metadata from /repos/o/r endpoint. Doing so would be a feature
    // creep — the operator's source.ref is authoritative.
}

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

#[tokio::test]
#[ignore = "covered by tests/poll_ls_remote.rs::poll_resolves_annotated_tag_returns_tag_sha_not_peel — \
            bare-repo fixture builds an annotated tag via `git mktag` and asserts the TAG SHA is returned, \
            not the peeled commit SHA"]
async fn annotated_tag_dereferenced_to_commit_sha() {
    let _dir = TempDir::new().unwrap();
    // Annotated tags (created via `git tag -a`) point to a tag object
    // which in turn points to a commit. ls-remote returns BOTH the tag
    // SHA and a "peeled" entry refs/tags/v1.0^{} = commit SHA.
    //
    // gcit's source.ref = "refs/tags/v1.0" — should it report the tag
    // SHA or the peeled commit SHA?
    //
    // gcit reports the tag SHA (NOT peeled).
    // If gcit reported the peeled commit, then re-tagging v1.0 to point
    // at a different commit (`git tag -f`) wouldn't trigger because the
    // peeled commit might still be reachable. Tag-as-marker semantics
    // need the tag SHA. flag.
    //
    // Pure ls-remote returns tag SHA at refs/tags/v1.0; the peeled
    // entry is at refs/tags/v1.0^{}. gcit asks for refs/tags/v1.0 and
    // gets back the tag SHA — natural behavior. Pin via test.
}

#[tokio::test]
#[ignore = "covered by tests/poll_ls_remote.rs::poll_resolves_lightweight_tag_to_commit_sha — \
            bare-repo fixture pins the Direct-ref arm against a real ls-refs response"]
async fn lightweight_tag_resolves_directly_to_commit() {
    let _dir = TempDir::new().unwrap();
    // Lightweight tags (`git tag` without -a) are simple refs pointing
    // directly at a commit. No tag object, no peeling. gcit's poll
    // returns the commit SHA — no surprise.
    //
    // Pin via test to distinguish from the annotated-tag case above.
}

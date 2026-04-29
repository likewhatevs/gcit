// gix ls-remote via the public `gcit::git::ls_remote` API.
//
// Most ls-remote correctness assertions are pinned at the
// classification boundary (connect / handshake / ls-refs error to
// `LsRemoteError`) by in-module tests in src/git/ls_remote.rs's
// `tests` module. The integration tests here exercise the same
// public API on the unparseable / unreachable paths AND against a
// real bare-repo fixture served over the `file://` transport so the
// end-to-end packetline-v2 handshake + ls-refs path runs unmocked.
//
// Requires `git` (and `git-upload-pack`) on PATH: the fixture
// builder shells out to `git init --bare` / `hash-object` / `mktree`
// / `commit-tree` / `mktag` / `update-ref` to construct repos and
// objects, and gix-transport's local file:// connector spawns
// `git-upload-pack` as the server side of every ls-refs round-trip.
// On a system without git installed, every fixture-driven test
// panics in the helper rather than silently degrading.
//
// The standalone commit-construction path lets the fixture sidestep
// any host-level `user.name` / `user.email` configuration: every git
// invocation that needs an author identity passes it via
// `GIT_AUTHOR_*` / `GIT_COMMITTER_*` env vars.

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use tempfile::TempDir;

use gcit::git::{ls_remote, PollOutcome};

/// Run a git command in the given directory, asserting success and
/// returning trimmed stdout. Panics with the captured stderr on
/// non-zero exit so a fixture failure surfaces a useful error
/// instead of an opaque "subcommand exited with code 128".
fn run_git_in(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .unwrap_or_else(|e| panic!("spawn git in {}: {e}", dir.display()));
    if !output.status.success() {
        panic!(
            "git {args:?} in {} failed: status={:?} stderr={}",
            dir.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr),
        );
    }
    String::from_utf8(output.stdout)
        .expect("git stdout is utf-8")
        .trim()
        .to_string()
}

/// Like `run_git_in`, but also pipes `stdin` to the child. Used by
/// `git hash-object --stdin` and `git mktree`.
fn run_git_in_with_stdin(dir: &Path, args: &[&str], stdin: &str) -> String {
    use std::io::Write;
    let mut child = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn git in {}: {e}", dir.display()));
    child
        .stdin
        .as_mut()
        .expect("piped stdin")
        .write_all(stdin.as_bytes())
        .expect("write stdin to git");
    let output = child.wait_with_output().expect("wait git");
    if !output.status.success() {
        panic!(
            "git {args:?} in {} failed: status={:?} stderr={}",
            dir.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr),
        );
    }
    String::from_utf8(output.stdout)
        .expect("git stdout is utf-8")
        .trim()
        .to_string()
}

/// Initialize an empty bare repository at the returned tempdir's
/// path. The repo has no refs and no objects until callers populate
/// it via `commit_with_message` + `update_ref`.
fn init_bare_repo() -> TempDir {
    let dir = TempDir::new().expect("create tempdir");
    let output = Command::new("git")
        .arg("init")
        .arg("--bare")
        .arg(dir.path())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn git init --bare");
    if !output.status.success() {
        panic!(
            "git init --bare {} failed: status={:?} stderr={}",
            dir.path().display(),
            output.status,
            String::from_utf8_lossy(&output.stderr),
        );
    }
    dir
}

/// Build a single commit in `bare` with the given content under a
/// fixed file name and return the commit SHA. The commit author /
/// committer identity is supplied via env vars so the test does not
/// depend on the host's `~/.gitconfig`. Author / committer date are
/// pinned so a failing assertion can quote a stable SHA when
/// debugging from a re-run.
fn commit_with_message(bare: &Path, blob_content: &str, message: &str) -> String {
    let blob_sha = run_git_in_with_stdin(bare, &["hash-object", "-w", "--stdin"], blob_content);
    let tree_input = format!("100644 blob {blob_sha}\tfile\n");
    let tree_sha = run_git_in_with_stdin(bare, &["mktree"], &tree_input);
    // GIT_AUTHOR_* / GIT_COMMITTER_* env vars supply the identity
    // without needing user.name/user.email in either the bare repo's
    // local config or the host's global config. The pinned dates
    // make the resulting SHA stable across re-runs, which is useful
    // when copying a SHA out of a failed assertion log.
    let output = Command::new("git")
        .arg("-C")
        .arg(bare)
        .args(["commit-tree", &tree_sha, "-m", message])
        .env("GIT_AUTHOR_NAME", "gcit-test")
        .env("GIT_AUTHOR_EMAIL", "gcit-test@example.invalid")
        .env("GIT_AUTHOR_DATE", "1700000000 +0000")
        .env("GIT_COMMITTER_NAME", "gcit-test")
        .env("GIT_COMMITTER_EMAIL", "gcit-test@example.invalid")
        .env("GIT_COMMITTER_DATE", "1700000000 +0000")
        .stdin(Stdio::null())
        .output()
        .expect("spawn git commit-tree");
    if !output.status.success() {
        panic!(
            "git commit-tree in {} failed: status={:?} stderr={}",
            bare.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr),
        );
    }
    String::from_utf8(output.stdout)
        .expect("commit-tree stdout is utf-8")
        .trim()
        .to_string()
}

/// Point `ref_name` at `commit_sha` in the bare repo at `bare`.
fn update_ref(bare: &Path, ref_name: &str, commit_sha: &str) {
    run_git_in(bare, &["update-ref", ref_name, commit_sha]);
}

/// Build an annotated tag object pointing at `commit_sha` and return
/// the tag object's own SHA. Caller is responsible for pointing
/// `refs/tags/<name>` at the returned SHA via `update_ref`.
///
/// Annotated tags are distinct git objects: `refs/tags/v1` points at
/// the tag object, and the wire-format peeled value (`v1^{}`) points
/// at the underlying commit. `find_ref` in src/git/ls_remote.rs maps
/// the gix `Ref::Peeled` variant's `tag` field to
/// `PollOutcome::Refreshed { sha: tag_oid }` so re-tagging is
/// detectable; the annotated-tag round-trip in the integration tests
/// pins that ruling against a real wire-format ls-refs response.
fn mktag_annotated(bare: &Path, commit_sha: &str, name: &str, message: &str) -> String {
    // git mktag reads the canonical tag-object body from stdin and
    // writes the object to the repo's object store, returning the
    // resulting SHA. Pin author / date so the resulting tag SHA is
    // deterministic across test runs.
    let tag_body = format!(
        "object {commit_sha}\n\
         type commit\n\
         tag {name}\n\
         tagger gcit-test <gcit-test@example.invalid> 1700000000 +0000\n\
         \n\
         {message}\n"
    );
    run_git_in_with_stdin(bare, &["mktag"], &tag_body)
}

/// Compose the `file://<absolute-path>` URL gix-transport's local
/// connector accepts for `bare`. Per
/// gix_transport::client::blocking_io::connect::connect, the
/// `file://` arm rejects URLs with host / user / port — the path
/// must be the only component.
fn file_url_for(bare: &Path) -> String {
    let path = bare.to_str().expect("tempdir path is utf-8");
    format!("file://{path}")
}

/// `ls_remote::poll` returns a result; on totally unparseable URLs
/// the gix transport layer surfaces an error that the classifier
/// maps to `Permanent`. We rely on the classifier's coverage in
/// src/git/ls_remote.rs::tests; here we just confirm the public
/// pub-async-fn entry point is reachable from outside the crate.
#[tokio::test]
async fn poll_returns_error_for_unparseable_url() {
    let result = ls_remote::poll(
        "definitely not a url".to_string(),
        "refs/heads/main".to_string(),
    )
    .await;
    assert!(
        result.is_err(),
        "an unparseable URL must surface as Err, not Ok",
    );
}

/// A localhost URL that points at a closed port surfaces as a
/// `Transient` connect error per src/git/ls_remote.rs::
/// classify_connect_error. We bound the test with the function's
/// internal POLL_TIMEOUT so a stuck connect doesn't hang nextest.
#[tokio::test]
async fn poll_against_closed_port_returns_quickly() {
    // Pick a port unlikely to be open. The gix transport's connect
    // will fail with ConnectionRefused; the classifier maps it to
    // `LsRemoteError::Transient` (callers retry on backoff).
    let url = "http://127.0.0.1:1/never-listening".to_string();
    let started = std::time::Instant::now();
    let _ = tokio::time::timeout(
        Duration::from_secs(30),
        ls_remote::poll(url, "refs/heads/main".to_string()),
    )
    .await
    .expect("poll must not hang past 30s wall-clock against a closed port");
    let elapsed = started.elapsed();
    // A real connect error or transport timeout should resolve within
    // seconds. Pin at 30s upper to avoid CI flakes from slow DNS.
    assert!(
        elapsed < Duration::from_secs(30),
        "closed-port poll should resolve within 30s, took {elapsed:?}",
    );
}

/// `ls_remote::poll` is the one async entry point exposed by the
/// module. The function takes ownership of its String args (per the
/// signature on the public `poll` async fn) so callers can pass
/// owned values from a config without lifetime juggling. Pin the
/// signature shape via a compile-only-then-execute smoke run.
#[tokio::test]
async fn poll_signature_accepts_owned_strings() {
    let url: String = String::from("not://valid");
    let ref_name: String = String::from("refs/heads/main");
    let _ = ls_remote::poll(url, ref_name).await;
}

/// End-to-end ref resolution: build a bare repo with a single commit,
/// point `refs/heads/main` at it, and assert `ls_remote::poll` returns
/// `PollOutcome::Refreshed { sha }` carrying the same SHA the fixture
/// recorded. Exercises the real packetline-v2 handshake + ls-refs
/// path through gix-transport's local connector — the in-module
/// `find_*` unit tests cover the post-parse branch table, this one
/// covers the wire round-trip.
#[tokio::test]
async fn poll_resolves_ref_to_sha_from_bare_repo() {
    let bare = init_bare_repo();
    let commit_sha = commit_with_message(bare.path(), "hello\n", "initial");
    update_ref(bare.path(), "refs/heads/main", &commit_sha);

    let url = file_url_for(bare.path());
    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        ls_remote::poll(url, "refs/heads/main".to_string()),
    )
    .await
    .expect("bare-repo poll must not hang past 30s")
    .expect("poll succeeds against bare repo");

    match outcome {
        PollOutcome::Refreshed { sha } => {
            assert_eq!(
                sha.to_hex().to_string(),
                commit_sha,
                "resolved SHA must match the commit recorded by the fixture",
            );
        }
        other => panic!("expected Refreshed, got {other:?}"),
    }
}

/// An empty bare repo (no commits, no branch refs under
/// `refs/heads/*`) must surface as a non-panicking outcome. Against
/// a V2 server an empty repo's ls-refs advertises either a single
/// `Ref::Unborn` entry naming `HEAD` (modern git >= 2.34) or no
/// refs at all (older git). Either way `find_ref` walks the list
/// with the caller's `refs/heads/main` target and falls through to
/// `FoundRef::NotPresent` — which `poll_blocking` then maps to
/// `PollOutcome::UnbornRef`. (The companion test
/// `poll_resolves_head_on_empty_repo_via_unborn_arm` asks for
/// `HEAD` directly; depending on git version the lookup hits
/// either the `FoundRef::Unborn` or `FoundRef::NotPresent` arm
/// — both produce `UnbornRef`.)  The point of this test is to
/// confirm the path through gix-protocol's V2 handshake against a
/// truly empty repository does not crash and surfaces the unborn
/// signal — operators routinely point gcit at fresh repos before
/// pushing the first branch.
#[tokio::test]
async fn poll_empty_repo_returns_unborn_ref_not_panic() {
    let bare = init_bare_repo();
    let url = file_url_for(bare.path());
    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        ls_remote::poll(url, "refs/heads/main".to_string()),
    )
    .await
    .expect("empty-repo poll must not hang past 30s")
    .expect("empty bare repo must not error from the strategy");

    assert_eq!(
        outcome,
        PollOutcome::UnbornRef,
        "requested ref absent from advertisement -> UnbornRef via NotPresent arm",
    );
}

/// With multiple refs in the bare repo, `poll` must return the SHA
/// of the requested ref — not the first one in the advertisement,
/// not HEAD's resolved object. Pins the per-ref filter inside
/// `find_ref` against the wire-format case where the server
/// advertises several refs in one ls-refs response.
#[tokio::test]
async fn poll_resolves_correct_ref_among_multiple() {
    let bare = init_bare_repo();
    let main_commit = commit_with_message(bare.path(), "main branch\n", "main commit");
    let release_commit = commit_with_message(bare.path(), "release branch\n", "release commit");
    let feature_commit = commit_with_message(bare.path(), "feature branch\n", "feature commit");
    update_ref(bare.path(), "refs/heads/main", &main_commit);
    update_ref(bare.path(), "refs/heads/release", &release_commit);
    update_ref(bare.path(), "refs/heads/feature", &feature_commit);

    let url = file_url_for(bare.path());
    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        ls_remote::poll(url.clone(), "refs/heads/release".to_string()),
    )
    .await
    .expect("multi-ref poll must not hang past 30s")
    .expect("poll succeeds for release branch");
    match outcome {
        PollOutcome::Refreshed { sha } => assert_eq!(
            sha.to_hex().to_string(),
            release_commit,
            "must resolve refs/heads/release, not main or feature",
        ),
        other => panic!("expected Refreshed, got {other:?}"),
    }

    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        ls_remote::poll(url.clone(), "refs/heads/feature".to_string()),
    )
    .await
    .expect("multi-ref poll must not hang past 30s")
    .expect("poll succeeds for feature branch");
    match outcome {
        PollOutcome::Refreshed { sha } => assert_eq!(
            sha.to_hex().to_string(),
            feature_commit,
            "must resolve refs/heads/feature, not main or release",
        ),
        other => panic!("expected Refreshed, got {other:?}"),
    }

    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        ls_remote::poll(url, "refs/heads/main".to_string()),
    )
    .await
    .expect("multi-ref poll must not hang past 30s")
    .expect("poll succeeds for main branch");
    match outcome {
        PollOutcome::Refreshed { sha } => assert_eq!(
            sha.to_hex().to_string(),
            main_commit,
            "must resolve refs/heads/main, not release or feature",
        ),
        other => panic!("expected Refreshed, got {other:?}"),
    }
}

/// Asking for a ref that isn't in the bare repo returns `UnbornRef`
/// (the `FoundRef::NotPresent` arm in `poll_blocking`). Distinct
/// from the empty-repo case: here the repo has refs, just not the
/// one the caller asked for. Operator-visible signal is the same —
/// keep polling on cadence, log WARN. Mutation target: a regression
/// that swaps NotPresent into an error path would surface as a
/// Permanent here and the test would fail.
#[tokio::test]
async fn poll_missing_ref_in_populated_repo_returns_unborn() {
    let bare = init_bare_repo();
    let commit = commit_with_message(bare.path(), "main\n", "initial");
    update_ref(bare.path(), "refs/heads/main", &commit);

    let url = file_url_for(bare.path());
    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        ls_remote::poll(url, "refs/heads/never-pushed".to_string()),
    )
    .await
    .expect("missing-ref poll must not hang past 30s")
    .expect("missing-ref must surface as Ok(UnbornRef), not Err");
    assert_eq!(outcome, PollOutcome::UnbornRef);
}

/// Lightweight tags are a `refs/tags/<name>` ref that points DIRECTLY
/// at the commit object — no separate tag object exists in the repo.
/// On the wire, gix sees these as `Ref::Direct`, and `find_ref` maps
/// them to `PollOutcome::Refreshed { sha: commit_oid }`. The contrast
/// with annotated tags below pins the `Direct` vs `Peeled` branch
/// table inside `find_ref` against a real ls-refs response.
#[tokio::test]
async fn poll_resolves_lightweight_tag_to_commit_sha() {
    let bare = init_bare_repo();
    let commit_sha = commit_with_message(bare.path(), "release\n", "release commit");
    update_ref(bare.path(), "refs/tags/v1.0-light", &commit_sha);

    let url = file_url_for(bare.path());
    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        ls_remote::poll(url, "refs/tags/v1.0-light".to_string()),
    )
    .await
    .expect("lightweight-tag poll must not hang past 30s")
    .expect("poll succeeds for lightweight tag");
    match outcome {
        PollOutcome::Refreshed { sha } => assert_eq!(
            sha.to_hex().to_string(),
            commit_sha,
            "lightweight tag is a Direct ref — sha must be the commit SHA",
        ),
        other => panic!("expected Refreshed, got {other:?}"),
    }
}

/// Annotated tags are a separate git object kind: `refs/tags/<name>`
/// points at the tag object, which itself contains the target commit
/// SHA. On the wire, ls-refs surfaces this as a `Ref::Peeled` carrying
/// both the tag SHA and the peeled commit SHA. gcit's behavior
/// (the `FoundRef::PeeledTag` arm in `poll_blocking`, citing
/// tests/poll_symbolic_ref.rs::annotated_tag_dereferenced_to_commit_sha)
/// is that `PollOutcome::Refreshed { sha }` carries the TAG SHA, NOT
/// the peeled commit SHA, so re-tagging the same commit produces a
/// new SHA — and therefore a new dispatch — instead of the daemon
/// silently treating "v1.0 was retagged" as no-change. This test
/// pins that ruling end-to-end against a wire-format ls-refs
/// response: build a commit, build an annotated tag object pointing
/// at it, point `refs/tags/v1.0` at the tag object, poll, and assert
/// the returned SHA equals the tag object SHA — explicitly NOT the
/// commit SHA the tag dereferences to.
#[tokio::test]
async fn poll_resolves_annotated_tag_returns_tag_sha_not_peel() {
    let bare = init_bare_repo();
    let commit_sha = commit_with_message(bare.path(), "release\n", "release commit");
    let tag_sha = mktag_annotated(bare.path(), &commit_sha, "v1.0", "release tag");
    update_ref(bare.path(), "refs/tags/v1.0", &tag_sha);

    assert_ne!(
        commit_sha, tag_sha,
        "fixture sanity: tag object SHA must differ from commit SHA",
    );

    let url = file_url_for(bare.path());
    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        ls_remote::poll(url, "refs/tags/v1.0".to_string()),
    )
    .await
    .expect("annotated-tag poll must not hang past 30s")
    .expect("poll succeeds for annotated tag");
    match outcome {
        PollOutcome::Refreshed { sha } => {
            let resolved = sha.to_hex().to_string();
            assert_eq!(
                resolved, tag_sha,
                "annotated tag must resolve to TAG object SHA so re-tagging \
                 is detectable; resolving to the peeled commit SHA would \
                 mask retags as no-change",
            );
            assert_ne!(
                resolved, commit_sha,
                "annotated tag must NOT resolve to the underlying commit SHA",
            );
        }
        other => panic!("expected Refreshed, got {other:?}"),
    }
}

/// Asking for `HEAD` on a populated bare repo must resolve to the
/// commit SHA that HEAD points at. The exact `find_ref` arm that
/// fires depends on whether the server honors gix-protocol's
/// `symrefs` ls-refs argument: when it does (modern git), gix
/// surfaces HEAD as `handshake::Ref::Symbolic` and `find_ref`
/// returns `FoundRef::Symbolic(oid)`; when the server omits the
/// symref decoration, gix falls back to `Ref::Direct` and
/// `find_ref` returns `FoundRef::Direct(oid)`. Both arms route
/// through `poll_blocking`'s `Refreshed { sha }` outcome with the
/// same commit SHA, so the test asserts the SHA shape rather than
/// the specific arm. The in-module `find_symbolic_ref` unit test
/// covers the post-parse Symbolic arm against a synthetic Ref
/// vector; this test covers the full handshake-then-parse path.
#[tokio::test]
async fn poll_resolves_head_on_populated_repo_to_commit_sha() {
    let bare = init_bare_repo();
    let commit_sha = commit_with_message(bare.path(), "head\n", "initial");
    update_ref(bare.path(), "refs/heads/main", &commit_sha);
    // The bare repo's HEAD already points at refs/heads/main from
    // `git init --bare` (default branch). Promote it explicitly so
    // the test is independent of git's default-branch resolution.
    run_git_in(bare.path(), &["symbolic-ref", "HEAD", "refs/heads/main"]);

    let url = file_url_for(bare.path());
    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        ls_remote::poll(url, "HEAD".to_string()),
    )
    .await
    .expect("HEAD poll must not hang past 30s")
    .expect("poll succeeds for HEAD on populated repo");

    match outcome {
        PollOutcome::Refreshed { sha } => assert_eq!(
            sha.to_hex().to_string(),
            commit_sha,
            "HEAD must resolve to the commit SHA via Ref::Symbolic",
        ),
        other => panic!("expected Refreshed, got {other:?}"),
    }
}

/// Asking for `HEAD` on an EMPTY bare repo must resolve to
/// `PollOutcome::UnbornRef`. This test exercises the empty-bare-repo
/// + HEAD path; on modern git (>= 2.34 with the ls-refs `unborn`
/// capability) `find_ref` takes the `FoundRef::Unborn` arm because
/// the server emits a literal `Ref::Unborn` entry naming HEAD. On
/// older git the server omits HEAD from the empty-repo advertisement
/// and `find_ref` falls through to `FoundRef::NotPresent`. Both
/// paths produce `PollOutcome::UnbornRef`, so the operator-visible
/// signal is identical and the test asserts only that outcome.
/// Distinct from the existing empty-repo test — that one asks for
/// `refs/heads/main`, which forces the NotPresent path on every git
/// version because `refs/heads/main` is never advertised by an
/// empty repo.
#[tokio::test]
async fn poll_resolves_head_on_empty_repo_via_unborn_arm() {
    let bare = init_bare_repo();
    let url = file_url_for(bare.path());
    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        ls_remote::poll(url, "HEAD".to_string()),
    )
    .await
    .expect("HEAD-on-empty poll must not hang past 30s")
    .expect("HEAD on empty repo must surface as Ok(UnbornRef), not Err");

    assert_eq!(
        outcome,
        PollOutcome::UnbornRef,
        "HEAD on an empty bare repo must resolve to UnbornRef",
    );
}

// Shared bare-git-repo test fixture helpers.
//
// `poll_ls_remote.rs` and `poll_symbolic_ref.rs` both drive
// `gix_transport`-backed lookups against on-disk `file://` bare
// repositories built up commit-by-commit via `git` subprocess
// invocations. The fixture surface (init_bare_repo +
// run_git_in[_with_stdin] + commit_with_message + update_ref +
// file_url_for) is identical in both files; this module hosts the
// single canonical copy.

#![allow(dead_code)]

use std::path::Path;
use std::process::{Command, Stdio};

use tempfile::TempDir;

/// Run a git command in the given directory, asserting success and
/// returning trimmed stdout. Panics with the captured stderr on
/// non-zero exit so a fixture failure surfaces a useful error
/// instead of an opaque "subcommand exited with code 128".
pub fn run_git_in(dir: &Path, args: &[&str]) -> String {
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
pub fn run_git_in_with_stdin(dir: &Path, args: &[&str], stdin: &str) -> String {
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
pub fn init_bare_repo() -> TempDir {
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
pub fn commit_with_message(bare: &Path, blob_content: &str, message: &str) -> String {
    let blob_sha = run_git_in_with_stdin(bare, &["hash-object", "-w", "--stdin"], blob_content);
    let tree_input = format!("100644 blob {blob_sha}\tfile\n");
    let tree_sha = run_git_in_with_stdin(bare, &["mktree"], &tree_input);
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
pub fn update_ref(bare: &Path, ref_name: &str, commit_sha: &str) {
    run_git_in(bare, &["update-ref", ref_name, commit_sha]);
}

/// Compose the `file://<absolute-path>` URL gix-transport's local
/// connector accepts for `bare`. Per
/// gix_transport::client::blocking_io::connect::connect, the
/// `file://` arm rejects URLs with host / user / port — the path
/// must be the only component.
pub fn file_url_for(bare: &Path) -> String {
    let path = bare.to_str().expect("tempdir path is utf-8");
    format!("file://{path}")
}

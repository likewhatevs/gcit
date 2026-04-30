// O_NOFOLLOW + create(false) + O_APPEND on the spool open.
//
// The pipeline under test is `LocalMailNotifier::on_run_complete`,
// which:
//   1. Renders subject + body (handlebars).
//   2. Builds an mbox-formatted message.
//   3. Hands off to a blocking task that calls `write_with_lock`,
//      whose open invocation pins the safety contract:
//        OpenOptions::new()
//          .create(false)             // never auto-create the spool file
//          .append(true)              // O_APPEND — atomic seek-to-EOF
//          .custom_flags(libc::O_NOFOLLOW)  // refuse symlink final component
//          .open(path)?
//   4. flock(LOCK_EX) via fd-lock, write, fsync.
//
// These tests drive the public `LocalMailNotifier::for_test` entry
// point with a `tempfile::TempDir` so writes hit a per-test fixture
// instead of /var/mail. The notifier is a test-only constructor
// (`#[doc(hidden)]`) gated `pub` so integration test crates can
// reach it.

use std::fs;
use std::io::Write;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Utc;
use gix_hash::ObjectId;
use handlebars::Handlebars;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use gcit::config::{load_str, ConfigError, FireEvent, LocalMailTemplateConfig};
use gcit::github::{Conclusion, JobResult, RunStatus, RunSummary};
use gcit::mail::LocalMailNotifier;
use gcit::notify::{
    strict_handlebars, ActionInfo, Notifier, NotifyError, NotifyOutcome, RunContext, SourceInfo,
};

fn handlebars() -> Arc<Handlebars<'static>> {
    Arc::new(strict_handlebars())
}

fn ctx() -> RunContext {
    RunContext {
        flow_name: "myflow".into(),
        flow_description: None,
        source: SourceInfo {
            url: "https://example.com/r.git".into(),
            ref_name: "refs/heads/main".into(),
            sha: ObjectId::null(gix_hash::Kind::Sha1),
            sha_short: "0000000".into(),
        },
        action: ActionInfo {
            repo: "owner/repo".into(),
            workflow: "ci.yml".into(),
            run_id: 1,
            run_url: "https://example.com".into(),
            dispatched_at: Utc::now(),
        },
        gcit_run_id: uuid::Uuid::nil(),
    }
}

fn summary() -> RunSummary {
    RunSummary {
        run_id: 1,
        run_url: "https://example.com".into(),
        run_number: 1,
        run_attempt: 1,
        status: RunStatus::Completed,
        conclusion: Some(Conclusion::Success),
        started_at: Some(Utc::now()),
        completed_at: Some(Utc::now()),
        jobs: vec![JobResult {
            job_id: 1,
            name: "build".into(),
            html_url: "https://example.com/job/1".into(),
            conclusion: Some(Conclusion::Success),
            started_at: Some(Utc::now()),
            completed_at: Some(Utc::now()),
            steps: Vec::new(),
            run_attempt: 1,
        }],
    }
}

/// Build a notifier whose spool file is `<spool_dir>/<user>`.
/// Tests pre-create whatever artefact (file/symlink/etc.) is needed
/// at that path before calling `on_run_complete`.
fn notifier(spool_dir: PathBuf, user: &str) -> LocalMailNotifier {
    LocalMailNotifier::for_test(
        "test",
        user,
        Arc::new("host1".to_string()),
        vec![FireEvent::RunComplete],
        LocalMailTemplateConfig::default(),
        handlebars(),
        spool_dir,
    )
    .expect("test fixture user is alphanumeric, valid")
}

#[tokio::test]
async fn open_with_o_nofollow_refuses_symlink_target() {
    // Setup:
    //   tempdir/real-spool       <- regular file, mode 0644
    //   tempdir/<user>           <- symlink to real-spool
    //
    // The notifier's spool path is `tempdir/<user>`. With
    // O_NOFOLLOW, the open call returns ELOOP (errno 40) because
    // the FINAL path component is a symlink. The notifier must
    // surface NotifyError::Permanent — a symlink at the final
    // component is a config or filesystem-poisoning problem; retry
    // won't help. Mutation target: dropping `.custom_flags(O_NOFOLLOW)`
    // on the OpenOptions chain would let the open succeed and write
    // through the symlink to an arbitrary file.
    let tmp = TempDir::new().expect("tempdir");
    let user = "u_symlink";
    let real = tmp.path().join("real-spool");
    fs::write(&real, b"existing\n").expect("create real-spool");
    let spool_path = tmp.path().join(user);
    symlink(&real, &spool_path).expect("create symlink");

    let n = notifier(tmp.path().to_path_buf(), user);
    let result = n
        .on_run_complete(&ctx(), &summary(), &CancellationToken::new())
        .await;
    match result {
        Err(NotifyError::Permanent { source }) => {
            let msg = format!("{source}");
            assert!(
                msg.contains("symlink") || msg.contains("O_NOFOLLOW"),
                "Permanent message must mention symlink/O_NOFOLLOW; got: {msg}",
            );
        }
        other => panic!("expected Permanent on symlink final component; got {other:?}"),
    }

    // Defense-in-depth: the symlink target itself must not have been
    // appended to. If O_NOFOLLOW was bypassed the open would have
    // followed the symlink and the real-spool file's contents would
    // grow.
    let real_contents = fs::read(&real).expect("read real-spool");
    assert_eq!(
        real_contents, b"existing\n",
        "symlink target must be untouched when O_NOFOLLOW refuses the open",
    );
}

#[tokio::test]
async fn open_succeeds_on_regular_file() {
    // Sanity: O_NOFOLLOW only blocks the FINAL component being a
    // symlink. A plain regular file at the spool path opens
    // successfully and the notifier returns Ok(Sent).
    let tmp = TempDir::new().expect("tempdir");
    let user = "u_regular";
    let spool_path = tmp.path().join(user);
    fs::write(&spool_path, b"").expect("create empty spool");

    let n = notifier(tmp.path().to_path_buf(), user);
    let outcome = n
        .on_run_complete(&ctx(), &summary(), &CancellationToken::new())
        .await
        .expect("regular file open + append must succeed");
    match outcome {
        NotifyOutcome::Sent { receipt } => {
            assert!(
                receipt.starts_with("file:") && receipt.contains(user),
                "receipt must name the spool path; got {receipt}",
            );
        }
        other => panic!("expected Sent; got {other:?}"),
    }

    // Spool file must have grown by the message bytes.
    let after = fs::read(&spool_path).expect("read spool");
    assert!(
        !after.is_empty(),
        "spool file must contain the appended message",
    );
}

#[tokio::test]
async fn open_succeeds_when_final_component_is_real_file_through_dir_symlink() {
    // Setup:
    //   tempdir/realdir/<user>       <- regular file
    //   tempdir/spool/dirlink         <- symlink to realdir
    //
    // The notifier's spool_dir is `tempdir/spool/dirlink`; the
    // FINAL component (<user>) is a regular file. O_NOFOLLOW only
    // blocks the FINAL component from being a symlink — intermediate
    // symlinks are still followed by the kernel's path resolver.
    //
    // Pin: the notifier's bare O_NOFOLLOW (not openat2 +
    // RESOLVE_NO_SYMLINKS) accepts intermediate dir symlinks. A
    // mutation that switched to RESOLVE_NO_SYMLINKS would over-
    // harden and block legitimate setups.
    let tmp = TempDir::new().expect("tempdir");
    let user = "u_dirsym";
    let realdir = tmp.path().join("realdir");
    fs::create_dir(&realdir).expect("create realdir");
    let real_spool = realdir.join(user);
    fs::write(&real_spool, b"").expect("create real spool");

    let dirlink_parent = tmp.path().join("spool");
    fs::create_dir(&dirlink_parent).expect("create spool parent");
    let dirlink = dirlink_parent.join("dirlink");
    symlink(&realdir, &dirlink).expect("create dir symlink");

    // spool_dir = tempdir/spool/dirlink — points at realdir via the
    // intermediate symlink. spool_path = tempdir/spool/dirlink/<user>.
    let n = notifier(dirlink, user);
    let outcome = n
        .on_run_complete(&ctx(), &summary(), &CancellationToken::new())
        .await
        .expect("intermediate dir symlink must NOT block O_NOFOLLOW open");
    assert!(matches!(outcome, NotifyOutcome::Sent { .. }));

    // Spool got written via the symlink-traversed path.
    let after = fs::read(&real_spool).expect("read real spool");
    assert!(
        !after.is_empty(),
        "spool file accessed via intermediate dir symlink must receive the append",
    );
}

#[tokio::test]
async fn open_refuses_dangling_symlink() {
    // A dangling symlink (target doesn't exist) STILL trips ELOOP
    // under O_NOFOLLOW: the open call inspects the final-component
    // link itself, regardless of whether the target resolves.
    //
    // Pin: NotifyError::Permanent surfaces. A mutation that special-
    // cased ENOENT but forgot ELOOP would leak a "file not found"
    // message even though the actual condition is "final component
    // is a symlink".
    let tmp = TempDir::new().expect("tempdir");
    let user = "u_dangling";
    let spool_path = tmp.path().join(user);
    let nonexistent = tmp.path().join("does_not_exist");
    symlink(&nonexistent, &spool_path).expect("create dangling symlink");

    let n = notifier(tmp.path().to_path_buf(), user);
    let result = n
        .on_run_complete(&ctx(), &summary(), &CancellationToken::new())
        .await;
    match result {
        Err(NotifyError::Permanent { source }) => {
            let msg = format!("{source}");
            // ELOOP path explicitly mentions symlink/O_NOFOLLOW; it
            // must NOT be confused with the ENOENT path's "does not
            // exist" wording even though the symlink target is
            // missing.
            assert!(
                msg.contains("symlink") || msg.contains("O_NOFOLLOW"),
                "dangling symlink must surface as ELOOP-mapped Permanent, not ENOENT; got: {msg}",
            );
        }
        other => panic!("expected Permanent on dangling symlink; got {other:?}"),
    }
}

#[tokio::test]
async fn append_does_not_create_file() {
    // Production sets `.create(false)` so the open call refuses to
    // create a missing spool file. Operators are expected to create
    // /var/mail/<user> out of band (useradd/touch). The notifier
    // surfaces NotifyError::Permanent with ENOENT mapping; the
    // operator-facing message includes the path AND the remediation
    // commands.
    //
    // Mutation target: flipping create(false) to create(true) would
    // silently produce spool files for arbitrary user names,
    // violating the file-ownership contract.
    let tmp = TempDir::new().expect("tempdir");
    let user = "u_nocreate";
    let spool_path = tmp.path().join(user);
    assert!(
        !spool_path.exists(),
        "precondition: spool file must not exist",
    );

    let n = notifier(tmp.path().to_path_buf(), user);
    let result = n
        .on_run_complete(&ctx(), &summary(), &CancellationToken::new())
        .await;
    match result {
        Err(NotifyError::Permanent { source }) => {
            let msg = format!("{source}");
            assert!(
                msg.contains("does not exist"),
                "Permanent ENOENT message must explain the cause; got: {msg}",
            );
            // Operator remediation guidance per the production
            // map_io_error wording.
            assert!(
                msg.contains("touch") || msg.contains("useradd") || msg.contains("mailx"),
                "ENOENT message must include remediation commands; got: {msg}",
            );
            assert!(
                msg.contains(user),
                "Permanent ENOENT message must include the spool path so the operator knows which user; got: {msg}",
            );
        }
        other => panic!("expected Permanent on missing spool file; got {other:?}"),
    }

    // Defense-in-depth: confirm the file was NOT created as a side
    // effect (a mutation that called .create(true) would have
    // written here).
    assert!(
        !spool_path.exists(),
        "create(false) must NOT auto-create the spool file even on ENOENT path",
    );
}

#[tokio::test]
async fn append_does_not_truncate_existing_content() {
    // OpenOptions::new().append(true) sets O_APPEND, which forces
    // every write to seek to EOF atomically. Pre-existing content at
    // [0..pre_len) must remain byte-identical after the append; the
    // file's new size equals pre_len + len(formatted_message).
    //
    // Mutation target: switching to .write(true).truncate(false)
    // drops O_APPEND. Two concurrent writers would race on
    // seek+write (production guards this with flock too, but
    // O_APPEND is the primary atomicity mechanism per kernel docs).
    let tmp = TempDir::new().expect("tempdir");
    let user = "u_append";
    let spool_path = tmp.path().join(user);
    let preamble = b"From - existing@host\nSubject: prior message\n\nbody\n\n";
    {
        let mut f = fs::File::create(&spool_path).expect("create spool");
        f.write_all(preamble).expect("write preamble");
    }
    let pre_len = preamble.len();
    let pre_bytes = fs::read(&spool_path).expect("read pre");

    let n = notifier(tmp.path().to_path_buf(), user);
    n.on_run_complete(&ctx(), &summary(), &CancellationToken::new())
        .await
        .expect("append on regular file must succeed");

    let post_bytes = fs::read(&spool_path).expect("read post");
    assert!(
        post_bytes.len() > pre_len,
        "spool file must have grown after the append; pre={pre_len} post={post}",
        post = post_bytes.len(),
    );
    assert_eq!(
        &post_bytes[..pre_len],
        pre_bytes.as_slice(),
        "the first {pre_len} bytes must be byte-identical to the pre-existing content",
    );
}

#[test]
fn append_with_path_traversal_in_user_rejected_at_config_load() {
    // The local_mail user charset enforced by validate_local_mail
    // is `[A-Za-z0-9_-]+`; path-traversal sequences (`/`, `..`,
    // etc.) hit the charset gate at config load and surface as
    // ConfigError::Validate on `destination.local_mail.user`.
    //
    // This pins the first line of defense: a malicious config
    // never produces a `LocalMailNotifier` whose `user` would
    // construct a traversal path. A separate runtime defense
    // (rejecting `/` or `..` inside `LocalMailNotifier::new`) is
    // out of scope for this test.
    //
    // Cross-references the broader charset rstest in
    // tests/mail_config_schema.rs::local_mail_user_validation_charset
    // (which covers space/period/at-sign/unicode); this test fixes
    // the wire to the *path-traversal* angle specifically so a
    // future change that loosens the charset for non-traversal
    // characters (e.g. accepting `.`) still has to think about
    // `..` paths.
    let preamble = r#"
[[flow]]
name = "f"
description = "test flow"

[flow.source]
url = "https://example.com/repo.git"
ref = "refs/heads/main"

[flow.action]
kind          = "github_workflow_dispatch"
repo          = "owner/repo"
workflow      = "ci.yml"
ref           = "refs/heads/main"
credential_id = "github_pat"
inputs        = {}
"#;
    // `../../etc/passwd` style — both `.` and `/` are outside the
    // allowed charset.
    let traversal_user = "../../etc/passwd";
    let src = format!(
        "{preamble}\n[[flow.destination]]\nkind = \"local_mail\"\nuser = \"{traversal_user}\"\n",
    );
    let errs = load_str(&src, Path::new("test.toml"))
        .expect_err("path-traversal user must be rejected at config load");
    let saw_validate = errs.iter().any(|e| {
        matches!(
            e,
            ConfigError::Validate { field, .. }
                if field == "destination.local_mail.user"
        )
    });
    assert!(
        saw_validate,
        "path-traversal user {traversal_user:?} must surface ConfigError::Validate on destination.local_mail.user; got: {errs:#?}",
    );
}

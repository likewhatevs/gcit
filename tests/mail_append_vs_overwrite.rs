// Append vs overwrite — second write to a non-empty spool keeps the first
// message intact. gcit's mail path opens the spool with O_APPEND
// (production at src/mail/notifier.rs::write_with_lock) so every
// write atomically lands at EOF — never seeking back to position 0
// and never truncating.
//
// Each test uses `LocalMailNotifier::for_test` with a tempdir spool
// directory. The driver is the production path itself (on_run_complete
// → render → format_message → write_with_lock), so the tests verify
// the full mbox-append contract, not an isolated helper.

use std::sync::Arc;

use chrono::Utc;
use gix_hash::ObjectId;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use gcit::config::{FireEvent, LocalMailTemplateConfig};
use gcit::github::{Conclusion, RunStatus, RunSummary};
use gcit::mail::LocalMailNotifier;
use gcit::notify::{strict_handlebars, ActionInfo, Notifier, RunContext, SourceInfo};

fn ctx() -> RunContext {
    RunContext {
        flow_name: "ci-flow".into(),
        flow_description: None,
        source: SourceInfo {
            url: "https://example.com/repo.git".into(),
            ref_name: "refs/heads/main".into(),
            sha: ObjectId::null(gix_hash::Kind::Sha1),
            sha_short: "0000000".into(),
        },
        action: ActionInfo {
            repo: "owner/repo".into(),
            workflow: "ci.yml".into(),
            run_id: 42,
            run_url: "https://github.com/owner/repo/actions/runs/42".into(),
            dispatched_at: Utc::now(),
        },
        gcit_run_id: Uuid::nil(),
    }
}

fn summary_success() -> RunSummary {
    RunSummary {
        run_id: 42,
        run_url: "https://github.com/owner/repo/actions/runs/42".into(),
        run_number: 7,
        run_attempt: 1,
        status: RunStatus::Completed,
        conclusion: Some(Conclusion::Success),
        started_at: Some(Utc::now()),
        completed_at: Some(Utc::now()),
        jobs: Vec::new(),
    }
}

/// Build a notifier pointed at `<spool_dir>/<user>` with a custom
/// subject so each test can identify its own messages by Subject
/// header substring.
fn notifier_with_subject(
    spool_dir: &std::path::Path,
    user: &str,
    subject: &str,
) -> LocalMailNotifier {
    LocalMailNotifier::for_test(
        "test",
        user,
        Arc::new("h.example.com".to_string()),
        vec![FireEvent::RunComplete],
        LocalMailTemplateConfig {
            subject: Some(subject.to_string()),
            body: None,
        },
        Arc::new(strict_handlebars()),
        spool_dir.to_path_buf(),
    )
    .expect("test fixture user is alphanumeric, valid")
}

#[tokio::test]
async fn second_append_preserves_first_message() {
    // Two on_run_complete invocations against the same spool:
    // each must result in a distinct mbox record (separated by the
    // mbox `From ` separator line, with the full first message
    // preserved verbatim above the second).
    //
    // Mutation target: dropping O_APPEND (uses .write(true)
    // .truncate(false) without .append(true)) — second write seeks
    // to position 0 and overwrites the first message.
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let user = "u";
    let spool = tmp.path().join(user);
    std::fs::write(&spool, b"").expect("seed empty spool");

    let n1 = notifier_with_subject(tmp.path(), user, "subject-MARKER-1");
    n1.on_run_complete(&ctx(), &summary_success(), &CancellationToken::new())
        .await
        .expect("first append must succeed");
    let after_first = std::fs::read(&spool).expect("read after first");
    let first_size = after_first.len();
    assert!(first_size > 0, "first append must write some bytes");

    let n2 = notifier_with_subject(tmp.path(), user, "subject-MARKER-2");
    n2.on_run_complete(&ctx(), &summary_success(), &CancellationToken::new())
        .await
        .expect("second append must succeed");
    let after_second = std::fs::read(&spool).expect("read after second");

    assert!(
        after_second.len() > first_size,
        "second append must grow the spool, not replace it; first={first_size}, after={}",
        after_second.len(),
    );
    // The first message's bytes must appear at the start of the file
    // — byte-identical.
    assert!(
        after_second.starts_with(&after_first),
        "first message must be preserved byte-for-byte at start of spool (no truncate/overwrite)",
    );
    // Both subject markers appear.
    let text = String::from_utf8_lossy(&after_second);
    assert!(
        text.contains("subject-MARKER-1"),
        "first subject marker must survive the second append",
    );
    assert!(
        text.contains("subject-MARKER-2"),
        "second subject marker must be present",
    );
}

#[tokio::test]
async fn append_to_pre_existing_mail_preserves_legacy_content() {
    // Pre-seed the spool with arbitrary mbox content (as if mailx,
    // postfix, or a previous gcit instance had written it). gcit's
    // append must leave that content byte-identical and add its
    // message after.
    //
    // Mutation target: truncating on first-write under
    // the assumption that "spool starts empty" — destroys legacy
    // mail. O_APPEND avoids the truncate.
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let user = "u";
    let spool = tmp.path().join(user);

    let legacy = b"From mailer Wed Jan  1 00:00:00 2025\n\
                   Subject: legacy\n\
                   \n\
                   legacy body\n\
                   \n";
    std::fs::write(&spool, legacy).expect("seed legacy spool");

    let n = notifier_with_subject(tmp.path(), user, "GCIT-APPENDED");
    n.on_run_complete(&ctx(), &summary_success(), &CancellationToken::new())
        .await
        .expect("append to non-empty spool must succeed");

    let after = std::fs::read(&spool).expect("read spool");
    assert!(
        after.starts_with(legacy),
        "legacy bytes must be byte-identical at the start of the spool",
    );
    let text = String::from_utf8_lossy(&after);
    assert!(
        text.contains("GCIT-APPENDED"),
        "gcit-appended subject must follow the legacy content",
    );
    assert!(
        after.len() > legacy.len(),
        "spool must grow by the gcit message size",
    );
}

#[tokio::test]
async fn append_does_not_seek_within_spool() {
    // O_APPEND semantics: every write atomically lands at EOF
    // regardless of fd position. This test pre-populates the spool
    // with N bytes, runs append, and asserts the spool size is
    // exactly N + (gcit-message-size) — proving gcit did NOT seek
    // back to write at position < N.
    //
    // Mutation target: manually seeking to 0 + writing —
    // catastrophic data loss.
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let user = "u";
    let spool = tmp.path().join(user);

    // Compute the gcit-message size independently by running an
    // identical notifier setup against a separate, empty spool. The
    // resulting file size IS the message size (the spool started
    // empty and the notifier appended one message). Without this
    // independent measurement, the size assertion would have to
    // derive `n_appended` from `n_after - n_before`, which is
    // tautological by construction.
    let probe_dir = tempfile::TempDir::new().expect("probe tempdir");
    let probe_spool = probe_dir.path().join(user);
    std::fs::write(&probe_spool, b"").expect("seed empty probe spool");
    let probe = notifier_with_subject(probe_dir.path(), user, "no-seek-marker");
    probe
        .on_run_complete(&ctx(), &summary_success(), &CancellationToken::new())
        .await
        .expect("probe append must succeed");
    let message_size = std::fs::metadata(&probe_spool)
        .expect("stat probe spool")
        .len() as usize;
    assert!(message_size > 0, "probe must produce a non-empty message");

    let prefix = b"X".repeat(1024);
    std::fs::write(&spool, &prefix).expect("seed prefix");
    let n_before = std::fs::metadata(&spool).expect("stat spool").len() as usize;
    assert_eq!(n_before, 1024);

    let n = notifier_with_subject(tmp.path(), user, "no-seek-marker");
    n.on_run_complete(&ctx(), &summary_success(), &CancellationToken::new())
        .await
        .expect("append must succeed on non-empty spool");

    let after = std::fs::read(&spool).expect("read spool");
    let n_after = after.len();

    // Prefix bytes survive at the start.
    assert!(
        after.starts_with(&prefix),
        "no seek-back: prefix bytes must remain at offset 0..1024",
    );
    // Total file size grew by exactly the message size — proves
    // the write didn't overlap any of the prefix. `message_size`
    // comes from an independent measurement against an empty spool
    // (above), so this assertion fails if the production write
    // touched bytes at offset < n_before.
    assert_eq!(
        n_after,
        n_before + message_size,
        "size must equal prefix + independently-measured message size; \
         any overlap with the prefix would shrink n_after below this sum",
    );
}

#[tokio::test]
async fn many_concurrent_appends_preserve_all_messages() {
    // Stress: 32 concurrent appends to a shared spool (the
    // file_lock + O_APPEND combination must serialise them so all
    // messages land intact). Each notifier carries a unique
    // subject marker; after all complete, every marker must appear
    // in the spool exactly once.
    //
    // Mutation target: future refactor that drops the flock or
    // breaks O_APPEND atomicity (e.g. switches to write-at-offset).
    // The flock guarantees mutual exclusion across the gcit
    // notifiers; O_APPEND ensures atomic landing.
    //
    // Note: 32 writers is enough to
    // exercise the serialisation path while keeping the test
    // wallclock low. The file_lock is held during open + write +
    // sync_all, so 32 contenders fully exercise the queue.
    const N_WRITERS: usize = 32;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let user = "u";
    let spool = tmp.path().join(user);
    std::fs::write(&spool, b"").expect("seed empty spool");

    // Spawn N writers, each owning its own notifier (each its own
    // file_lock; the flock is held per-instance so concurrent
    // tasks correctly contend).
    let mut handles = Vec::with_capacity(N_WRITERS);
    for i in 0..N_WRITERS {
        let spool_dir = tmp.path().to_path_buf();
        let subject = format!("test-msg-{i}");
        handles.push(tokio::spawn(async move {
            let n = LocalMailNotifier::for_test(
                "test",
                "u",
                Arc::new("h.example.com".to_string()),
                vec![FireEvent::RunComplete],
                LocalMailTemplateConfig {
                    subject: Some(subject.clone()),
                    body: None,
                },
                Arc::new(strict_handlebars()),
                spool_dir,
            )
            .expect("fixture user 'u' is valid");
            n.on_run_complete(&ctx(), &summary_success(), &CancellationToken::new())
                .await
                .expect("concurrent append must succeed");
        }));
    }
    for h in handles {
        h.await.expect("writer task panicked");
    }

    let written = std::fs::read(&spool).expect("read spool");
    let text = String::from_utf8_lossy(&written);
    // Every marker must appear exactly once. Anchor on a trailing
    // newline so `test-msg-1` does NOT match the prefix of
    // `test-msg-10`, etc. (Subject: <marker>\n surfaces in the
    // header block.)
    for i in 0..N_WRITERS {
        let marker = format!("test-msg-{i}\n");
        let count = text.matches(&marker).count();
        assert_eq!(
            count, 1,
            "marker {marker:?} must appear exactly once in spool; got {count}",
        );
    }
}

#[tokio::test]
async fn append_after_external_concurrent_write_preserves_external() {
    // Setup: external (non-flock) writer pre-populates the spool
    // with message A; gcit then appends message B via the proper
    // path (open + flock + append + sync + drop). gcit's O_APPEND
    // means message B lands AFTER whatever was in the file at fd
    // open time — so message A is preserved.
    //
    // Mutation target: truncating or seeking during open,
    // overwriting external content.
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let user = "u";
    let spool = tmp.path().join(user);

    // External-style write — no flock cooperation, just a raw mbox
    // record dropped into place.
    let external = b"From external Wed Jan  1 00:00:00 2025\n\
                     Subject: EXTERNAL-MESSAGE\n\
                     \n\
                     external body\n\
                     \n";
    std::fs::write(&spool, external).expect("seed external content");

    let n = notifier_with_subject(tmp.path(), user, "GCIT-FOLLOWUP");
    n.on_run_complete(&ctx(), &summary_success(), &CancellationToken::new())
        .await
        .expect("append must cooperate with externally-written content");

    let after = std::fs::read(&spool).expect("read spool");
    assert!(
        after.starts_with(external),
        "external content at offset 0..N must survive byte-identical",
    );
    let text = String::from_utf8_lossy(&after);
    assert!(
        text.contains("EXTERNAL-MESSAGE"),
        "external subject must remain visible",
    );
    assert!(
        text.contains("GCIT-FOLLOWUP"),
        "gcit append must follow the external content",
    );
}

#[tokio::test]
async fn open_creates_no_temp_files() {
    // gcit's append uses O_APPEND directly — no temp-file +
    // rename pattern (that's the state-writer pattern, which is
    // wrong for mbox because rename invalidates flock state and
    // cooperating reader fd-positions).
    //
    // After a successful append, the spool's parent directory
    // must contain only the spool file — no `.tmp`, `.gcit-tmp`,
    // or other intermediate artifacts.
    //
    // Mutation target: copying the state-writer pattern.
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let user = "u";
    let spool = tmp.path().join(user);
    std::fs::write(&spool, b"").expect("seed empty spool");

    let n = notifier_with_subject(tmp.path(), user, "no-temp-marker");
    n.on_run_complete(&ctx(), &summary_success(), &CancellationToken::new())
        .await
        .expect("append must succeed");

    // Read directory entries; only the spool file should exist.
    let entries: Vec<String> = std::fs::read_dir(tmp.path())
        .expect("readdir tempdir")
        .map(|e| e.expect("dirent").file_name().to_string_lossy().to_string())
        .collect();
    assert_eq!(
        entries.len(),
        1,
        "tempdir must contain exactly the spool file; got {entries:?}",
    );
    assert_eq!(
        entries[0], user,
        "the only entry must be the spool file (named after the user)",
    );
}

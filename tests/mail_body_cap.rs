// 64 KiB body cap — pure-function tests for the cap check inside
// `LocalMailNotifier::on_run_complete`.
//
// The cap fires BEFORE any spool I/O — open(), flock(), write(),
// sync_all() are all reached only when `body.len() <= BODY_BYTE_CAP`.
// That ordering is what makes these tests pure: an oversize body
// surfaces as `Err(NotifyError::Permanent)` without touching
// /var/mail at all. Tests in this file therefore do not require
// tempdir spool injection (tracked separately).
//
// Threat model:
//   Attacker-controlled data (long branch name, gigabyte workflow
//   output) flows into the body template via `{{source.ref_name}}`
//   or similar. Without a cap, gcit can be coerced into:
//     a) Large-write DoS — fills /var/mail with attacker data,
//        eventually hitting filesystem ENOSPC.
//     b) Slow-write — fsync of large bodies stalls the daemon.
//     c) Memory exhaustion — handlebars renders the entire string
//        into RAM before write.
//   The 64 KiB cap is a generous bound for legitimate mail content
//   while staying well within memory and disk budgets.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use gix_hash::ObjectId;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use gcit::config::{FireEvent, LocalMailTemplateConfig};
use gcit::github::{Conclusion, RunStatus, RunSummary};
use gcit::mail::mbox::BODY_BYTE_CAP;
use gcit::mail::LocalMailNotifier;
use gcit::notify::{strict_handlebars, ActionInfo, Notifier, NotifyError, RunContext, SourceInfo};

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

fn ctx_with_long_ref_name(ref_bytes: usize) -> RunContext {
    let mut c = ctx();
    c.source.ref_name = "a".repeat(ref_bytes);
    c
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

/// Build a notifier that fires on RunComplete with a literal body
/// template. Because the literal body is the entire rendered output
/// (no template variables to interpolate), tests can pin the
/// rendered byte count directly.
fn notifier_with_literal_body(body: String) -> LocalMailNotifier {
    LocalMailNotifier::new(
        "test",
        "u",
        Arc::new("h".to_string()),
        vec![FireEvent::RunComplete],
        LocalMailTemplateConfig {
            subject: None,
            body: Some(body),
        },
        Arc::new(strict_handlebars()),
    )
    .expect("fixture user 'u' is valid")
}

#[tokio::test]
async fn body_just_over_64_kib_returns_permanent() {
    // Body of BODY_BYTE_CAP+1 bytes. The check inside `on_run_complete`
    // `if body.len() > BODY_BYTE_CAP` fires; permanent error
    // returned with the rendered length, the cap, and operator
    // guidance ("reduce template inputs").
    //
    // Mutation target: flipping Permanent to Transient —
    // gcit would retry forever (template won't shrink under
    // retry), so Permanent is the correct classification.
    let body = "x".repeat(BODY_BYTE_CAP + 1);
    let n = notifier_with_literal_body(body);
    let err = n
        .on_run_complete(&ctx(), &summary_success(), &CancellationToken::new())
        .await
        .expect_err("oversize body must surface as Err");
    let NotifyError::Permanent { source } = err else {
        panic!("expected Permanent for body > BODY_BYTE_CAP; got {err:?}");
    };
    let msg = source.to_string();
    let expected_len = BODY_BYTE_CAP + 1;
    assert!(
        msg.contains(&expected_len.to_string()),
        "error must name the rendered length {expected_len}; got {msg}",
    );
    assert!(
        msg.contains(&BODY_BYTE_CAP.to_string()),
        "error must name the cap {BODY_BYTE_CAP}; got {msg}",
    );
    assert!(
        msg.contains("reduce template inputs") || msg.contains("smaller flows"),
        "error must carry operator guidance from the cap-relief message; got {msg}",
    );
}

#[tokio::test]
async fn body_2x_cap_returns_permanent_without_attempting_spool_io() {
    // 200 KiB body (~3x the cap). The cap check runs BEFORE the
    // spool_path() call — surfacing Permanent without ever opening
    // /var/mail/u.
    //
    // We can't redirect /var/mail/u to a tempdir without a spool-
    // path-injection knob (separate work item). What we CAN
    // assert is: the call returns within a tight time bound that's
    // shorter than the LOCK_WAIT_DEADLINE (5s), proving the cap
    // path ran before the blocking spool task was even spawned. A
    // production /var/mail/u path that didn't exist would surface
    // ENOENT as a Permanent error too (in map_io_error), but only
    // AFTER attempting open(2). The
    // sub-millisecond return is the proxy for "no spool I/O
    // attempted".
    //
    // Mutation target: reordering the cap check to fire
    // post-write. Even if write fails on /var/mail/u (likely on
    // most CI sandboxes), the error message would name the spool
    // path, and the test would observe a longer wall-clock for
    // open()+ENOENT than the pure-fn cap path.
    let body = "x".repeat(BODY_BYTE_CAP * 3);
    let n = notifier_with_literal_body(body);
    let before = std::time::Instant::now();
    let err = n
        .on_run_complete(&ctx(), &summary_success(), &CancellationToken::new())
        .await
        .expect_err("oversize body must surface as Err");
    let elapsed = before.elapsed();

    let NotifyError::Permanent { source } = err else {
        panic!("expected Permanent for 2x cap body; got {err:?}");
    };
    let msg = source.to_string();
    assert!(
        msg.contains("cap"),
        "Permanent error must mention the cap; got {msg}",
    );
    // Must not name a /var/mail path — that would indicate an
    // open(2) attempted before the cap check.
    assert!(
        !msg.contains("/var/mail"),
        "cap path must surface BEFORE any spool I/O; error mentions /var/mail: {msg}",
    );
    // Sub-second wall clock confirms the blocking spool task was
    // never spawned. The cap check is pure CPU work on a 192 KiB
    // string; far below 1s on any realistic runner.
    assert!(
        elapsed < Duration::from_secs(1),
        "cap check should return well under 1s; elapsed {elapsed:?}",
    );
}

#[tokio::test]
async fn body_cap_applied_to_rendered_not_template() {
    // The cap is on the RENDERED body, not on the template source.
    // A short template ({{source.ref_name}}, 19 bytes) renders
    // against a context whose ref_name is BODY_BYTE_CAP+10 bytes;
    // the rendered output exceeds the cap and the check fires.
    //
    // Mutation target: checking template.len() (always
    // tiny in this test, 19 bytes) instead of the post-render
    // body.len() — the test passes the cap as a no-op despite the
    // attacker-controlled context value being well over budget.
    let template = "{{source.ref_name}}".to_string();
    assert!(
        template.len() < BODY_BYTE_CAP,
        "template source itself must be tiny so a wrong .len() check would slip past",
    );
    let n = notifier_with_literal_body(template);
    let big_ctx = ctx_with_long_ref_name(BODY_BYTE_CAP + 10);
    let err = n
        .on_run_complete(&big_ctx, &summary_success(), &CancellationToken::new())
        .await
        .expect_err("rendered body over cap must surface as Err");
    let NotifyError::Permanent { source } = err else {
        panic!("expected Permanent for rendered body > cap; got {err:?}");
    };
    let msg = source.to_string();
    let expected_len = BODY_BYTE_CAP + 10;
    assert!(
        msg.contains(&expected_len.to_string()),
        "error must name the rendered length {expected_len}; got {msg}",
    );
}

#[tokio::test]
async fn body_cap_uses_byte_length_not_char_length() {
    // BODY_BYTE_CAP is 65536 BYTES. UTF-8 multi-byte codepoints
    // count toward the budget per byte: a 4-byte emoji ('🦀' = 4
    // bytes) repeated N times produces 4N bytes. The cap check
    // uses .len() (bytes), not .chars().count() (codepoints).
    //
    // 20000 emoji = 80000 bytes (~25% over the cap). A char-count
    // check would see 20000 chars and pass; the byte-count check
    // catches it.
    //
    // Mutation target: using .chars().count() — body
    // accepted at 80000 bytes, ~25% over the cap. The test pins
    // the byte semantic.
    assert_eq!("🦀".len(), 4, "crab emoji must be 4 bytes (UTF-8)");
    assert_eq!(
        "🦀".chars().count(),
        1,
        "crab emoji must be 1 char — sanity",
    );
    let body = "🦀".repeat(20_000);
    assert_eq!(body.len(), 80_000);
    assert!(
        body.chars().count() < BODY_BYTE_CAP,
        "char-count must be under the cap so a wrong .chars().count() check would slip past",
    );
    let n = notifier_with_literal_body(body);
    let err = n
        .on_run_complete(&ctx(), &summary_success(), &CancellationToken::new())
        .await
        .expect_err("80 KB emoji body must surface as Err");
    let NotifyError::Permanent { source } = err else {
        panic!("expected Permanent for 80 KB byte body; got {err:?}");
    };
    let msg = source.to_string();
    assert!(
        msg.contains("80000"),
        "error must name the byte length 80000; got {msg}",
    );
}

#[tokio::test]
async fn body_cap_warn_log_emits_notifier_and_length_fields() {
    // The cap branch in `on_run_complete` emits:
    //   warn!(notifier = %self.id, rendered_len = body.len(),
    //         cap = BODY_BYTE_CAP, "local_mail body exceeds cap; dropping");
    //
    // Capture the warn event via a thread-local tracing
    // subscriber, then assert the field set surfaces via the fmt
    // layer's text rendering. We pin the message string and the
    // three field names — operator dashboards filter on these.
    //
    // Mutation target: a developer renames the field (e.g.,
    // `len` for `rendered_len`) — operator dashboards built
    // against the documented names lose the entries. The test
    // pins the wire format.
    let buf: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let make_writer = {
        let buf = Arc::clone(&buf);
        move || -> Box<dyn std::io::Write + Send> {
            Box::new(SharedBufferWriter {
                buf: Arc::clone(&buf),
            })
        }
    };
    let subscriber = tracing_subscriber::fmt()
        .with_writer(make_writer)
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .finish();

    let body = "x".repeat(BODY_BYTE_CAP + 1);
    let n = notifier_with_literal_body(body);

    use tracing::dispatcher::Dispatch;
    let dispatch_handle: Dispatch = Dispatch::new(subscriber);
    let result = {
        let _guard = tracing::dispatcher::set_default(&dispatch_handle);
        n.on_run_complete(&ctx(), &summary_success(), &CancellationToken::new())
            .await
    };
    assert!(
        matches!(result, Err(NotifyError::Permanent { .. })),
        "cap path must surface Permanent",
    );

    let captured = buf.lock().expect("buffer mutex").clone();
    let captured_text = String::from_utf8_lossy(&captured);
    assert!(
        captured_text.contains("local_mail body exceeds cap"),
        "warn message text must surface; captured:\n{captured_text}",
    );
    let expected_len = BODY_BYTE_CAP + 1;
    assert!(
        captured_text.contains(&format!("rendered_len={expected_len}")),
        "rendered_len field must carry actual byte count; captured:\n{captured_text}",
    );
    assert!(
        captured_text.contains(&format!("cap={BODY_BYTE_CAP}")),
        "cap field must carry BODY_BYTE_CAP; captured:\n{captured_text}",
    );
    assert!(
        captured_text.contains("notifier=\"test\"") || captured_text.contains("notifier=test"),
        "notifier field must carry the destination id; captured:\n{captured_text}",
    );
}

#[tokio::test]
async fn body_within_cap_succeeds() {
    // Body just under the cap. The cap check inside on_run_complete
    // gates on `body.len() > BODY_BYTE_CAP`; equality + below-cap
    // bodies fall through to the spool open + flock + write +
    // sync_all path. We use `LocalMailNotifier::for_test` to redirect
    // the spool path to a tempdir, pre-create the empty spool file
    // (production does not auto-create the spool file), and confirm
    // the on_run_complete returns NotifyOutcome::Sent + the file
    // contains the rendered mbox bytes.
    let body = "x".repeat(BODY_BYTE_CAP - 100);
    let body_marker_len = body.len();

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let user = "u";
    let spool_file = tmp.path().join(user);
    std::fs::write(&spool_file, b"").expect("seed empty spool file");

    let n = LocalMailNotifier::for_test(
        "test",
        user,
        Arc::new("h".to_string()),
        vec![FireEvent::RunComplete],
        LocalMailTemplateConfig {
            subject: None,
            body: Some(body),
        },
        Arc::new(strict_handlebars()),
        tmp.path().to_path_buf(),
    )
    .expect("fixture user 'u' is valid");
    let outcome = n
        .on_run_complete(&ctx(), &summary_success(), &CancellationToken::new())
        .await
        .expect("body just under cap must succeed");
    let receipt = match outcome {
        gcit::notify::NotifyOutcome::Sent { receipt } => receipt,
        other => panic!("expected Sent, got {other:?}"),
    };
    // Receipt format pinned by `on_run_complete` — `file:<path>`.
    assert_eq!(
        receipt,
        format!("file:{}", spool_file.display()),
        "receipt must reference the redirected spool path",
    );

    // The spool file got the rendered mbox bytes appended. Confirm
    // the rendered body bytes survived the round trip — they are
    // the suffix of the file (mbox headers precede the body).
    let written = std::fs::read(&spool_file).expect("read spool");
    assert!(
        written.len() > body_marker_len,
        "spool must carry headers + body; got {} bytes for {} body",
        written.len(),
        body_marker_len,
    );
    let written_text = String::from_utf8_lossy(&written);
    // The body section in mbox format follows the empty-line that
    // separates headers from body. Pin a substring that's only in
    // the body, not the headers.
    let body_pattern = "x".repeat(100);
    assert!(
        written_text.contains(&body_pattern),
        "spool must contain the rendered body content",
    );
}

#[tokio::test]
async fn body_at_exactly_64_kib_succeeds() {
    // Body of exactly BODY_BYTE_CAP bytes. The check inside
    // `on_run_complete` is `> cap`, so equality passes through to
    // the spool write. Pin the boundary explicitly: the cap is
    // "exceeded" only at cap+1, not at cap.
    let body = "x".repeat(BODY_BYTE_CAP);

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let user = "u";
    let spool_file = tmp.path().join(user);
    std::fs::write(&spool_file, b"").expect("seed empty spool file");

    let n = LocalMailNotifier::for_test(
        "test",
        user,
        Arc::new("h".to_string()),
        vec![FireEvent::RunComplete],
        LocalMailTemplateConfig {
            subject: None,
            body: Some(body),
        },
        Arc::new(strict_handlebars()),
        tmp.path().to_path_buf(),
    )
    .expect("fixture user 'u' is valid");
    n.on_run_complete(&ctx(), &summary_success(), &CancellationToken::new())
        .await
        .expect("body of exactly cap bytes must succeed (cap is `>`, not `>=`)");

    // Sanity: spool grew by exactly headers + body bytes (the
    // append). Pin the body content survived the round trip.
    let written = std::fs::read(&spool_file).expect("read spool");
    assert!(
        written.len() >= BODY_BYTE_CAP,
        "spool must hold at least the body bytes; got {} for cap {}",
        written.len(),
        BODY_BYTE_CAP,
    );
    let pattern = "x".repeat(100);
    let written_text = String::from_utf8_lossy(&written);
    assert!(
        written_text.contains(&pattern),
        "spool must carry the cap-sized body content",
    );
}

#[tokio::test]
async fn body_cap_applies_only_to_body_not_headers() {
    // The cap inside `on_run_complete` measures `body.len()` only
    // — subject + From-line + other headers are NOT counted. This
    // test pins that property: a long custom subject pushes the
    // total mbox record well past BODY_BYTE_CAP without tripping
    // the cap, because the cap measures the rendered body alone.
    //
    // Mutation target: changing the cap to
    // `formatted.len()` (the full mbox bytes including headers) —
    // operators with long subject templates would suddenly see
    // body-cap rejections. The cap is body-only on purpose.
    let body = "y".repeat(BODY_BYTE_CAP - 100);
    // Long custom subject — pushes the total record well past
    // BODY_BYTE_CAP, but body alone is below cap.
    let long_subject = "S".repeat(2000);

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let user = "u";
    let spool_file = tmp.path().join(user);
    std::fs::write(&spool_file, b"").expect("seed empty spool file");

    let n = LocalMailNotifier::for_test(
        "test",
        user,
        Arc::new("h".to_string()),
        vec![FireEvent::RunComplete],
        LocalMailTemplateConfig {
            subject: Some(long_subject.clone()),
            body: Some(body.clone()),
        },
        Arc::new(strict_handlebars()),
        tmp.path().to_path_buf(),
    )
    .expect("fixture user 'u' is valid");
    n.on_run_complete(&ctx(), &summary_success(), &CancellationToken::new())
        .await
        .expect("body below cap must succeed regardless of header length");

    let written = std::fs::read(&spool_file).expect("read spool");
    let written_text = String::from_utf8_lossy(&written);
    // The long subject DID survive into the headers (header
    // sanitization is byte-by-byte; "S" is ASCII 0x53 and stays
    // intact).
    assert!(
        written_text.contains(&long_subject),
        "long subject must survive into the Subject header",
    );
    // Body content also present. Both fit because the cap is on
    // body bytes alone.
    let body_pattern = "y".repeat(100);
    assert!(
        written_text.contains(&body_pattern),
        "body content must survive when total record > cap",
    );
    // Total record size > BODY_BYTE_CAP — the precondition the
    // mutation would catch.
    assert!(
        written.len() > BODY_BYTE_CAP,
        "test invariant: total record must exceed BODY_BYTE_CAP to prove the cap is body-only; got {} ≤ {}",
        written.len(),
        BODY_BYTE_CAP,
    );
}

/// `MakeWriter` implementation used by the warn-log test to route
/// formatted tracing events into a shared `Vec<u8>` buffer. The
/// buffer lives behind `Arc<Mutex<...>>` so both the writer (each
/// event) and the assertion site (after the await returns) share
/// state safely.
struct SharedBufferWriter {
    buf: Arc<Mutex<Vec<u8>>>,
}

impl std::io::Write for SharedBufferWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let mut g = self
            .buf
            .lock()
            .map_err(|_| std::io::Error::other("buffer mutex poisoned"))?;
        g.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

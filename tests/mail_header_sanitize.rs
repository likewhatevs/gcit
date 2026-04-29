// Header sanitization (RFC 5322 injection defense).
//
// Threat model: attacker controls a workflow input or branch name.
// They craft a value like:
//   "release\r\nBcc: attacker@example.com\r\nFrom: gcit@target"
// If a template uses {{source.ref_name}} in the Subject, the
// rendered Subject becomes a multi-line string and downstream
// readers (mailx, postfix) interpret the embedded "\r\n Bcc: ..."
// as additional headers — the message gets BCC'd to the attacker.
//
// Mitigation (src/mail/mbox.rs::sanitize_header): replace every
// byte < 0x20 except literal space (0x20) with a single space.
// The Subject becomes one continuous line with embedded "Bcc:"
// text but NO header separator; the MUA displays it as one line.

use proptest::prelude::*;
use rstest::rstest;

use chrono::{TimeZone, Utc};
use gcit::mail::mbox::{format_message, sanitize_header};

#[rstest]
// Plain ASCII: unchanged.
#[case::plain("Hello World 123!", "Hello World 123!")]
// Single CR: replaced with space.
#[case::cr("Hello\rWorld", "Hello World")]
// Single LF: replaced with space.
#[case::lf("Hello\nWorld", "Hello World")]
// CRLF: each control byte replaced individually -> two spaces.
#[case::crlf("Hello\r\nWorld", "Hello  World")]
// Tab (0x09): replaced.
#[case::tab("Hello\tWorld", "Hello World")]
// Bell (0x07): replaced.
#[case::bell("Hello\x07World", "Hello World")]
// Null (0x00): replaced.
#[case::null("Hello\x00World", "Hello World")]
// Vertical tab (0x0B): replaced.
#[case::vtab("Hello\x0bWorld", "Hello World")]
// Form feed (0x0C): replaced.
#[case::ff("Hello\x0cWorld", "Hello World")]
// DEL (0x7F): NOT replaced — sanitize_header strips bytes < 0x20
// only. 0x7F survives by design (matches the production behaviour
// at src/mail/mbox.rs:79 which gates on `b < 0x20`).
#[case::del_kept("Hello\x7fWorld", "Hello\x7fWorld")]
// Literal space at 0x20: kept.
#[case::literal_space("Hello World", "Hello World")]
// Header injection: \r\n + Bcc: -> two spaces between.
#[case::injection_simple(
    "release\r\nBcc: attacker@example.com",
    "release  Bcc: attacker@example.com"
)]
// LF-only injection -> one space.
#[case::injection_lf_only(
    "release\nBcc: attacker@example.com",
    "release Bcc: attacker@example.com"
)]
// URL-encoded CRLF: not decoded — sanitizer operates on bytes only.
#[case::injection_url_encoded(
    "release%0d%0aBcc:%20attacker@example.com",
    "release%0d%0aBcc:%20attacker@example.com"
)]
// Multiple injection attempts: each control byte -> one space.
#[case::injection_multi("x\r\ny\r\nz", "x  y  z")]
// Empty: empty.
#[case::empty("", "")]
// Whitespace-only: preserved.
#[case::ws_only("   ", "   ")]
// Long string: no truncation by sanitizer (truncation is a separate
// concern in the body cap at src/mail/mbox.rs::BODY_BYTE_CAP).
#[case::long_no_truncate(
    "01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890",
    "01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
)]
fn sanitize_header_value(#[case] input: &str, #[case] expected: &str) {
    assert_eq!(sanitize_header(input), expected);
}

#[test]
fn sanitize_runs_when_format_message_assembles_record() {
    // The header sanitizer must run on every Subject/From/To value
    // routed through format_message — not just on direct callers
    // of sanitize_header. This test feeds attacker-controlled bytes
    // into the subject argument and asserts the assembled mbox
    // record has exactly ONE Subject line (sanitized to spaces) and
    // ZERO Bcc: lines.
    //
    // Mutation target: wiring sanitize_header into a
    // helper that's bypassed on one of the three call sites; the
    // attacker payload would surface as a real Bcc header.
    let now = Utc.with_ymd_and_hms(2026, 1, 2, 15, 4, 5).unwrap();
    let payload = "release\r\nBcc: attacker@example.com";
    let msg = format_message(now, "alice", "host", payload, "body");

    // Split off the headers (everything before the first blank
    // line) and inspect line-by-line.
    let header_block = msg
        .split_once("\n\n")
        .map(|(h, _)| h)
        .expect("header/body separator present");
    let lines: Vec<&str> = header_block.split('\n').collect();
    let bcc_count = lines
        .iter()
        .filter(|l| l.to_ascii_lowercase().starts_with("bcc:"))
        .count();
    assert_eq!(bcc_count, 0, "no Bcc header may be present; got {lines:?}");
    let subject_count = lines.iter().filter(|l| l.starts_with("Subject:")).count();
    assert_eq!(
        subject_count, 1,
        "exactly one Subject header expected; got {subject_count} in {lines:?}",
    );
    let subject_line = lines
        .iter()
        .find(|l| l.starts_with("Subject:"))
        .expect("subject present");
    assert!(
        !subject_line.contains('\r') && !subject_line.contains('\n'),
        "Subject line must not carry CR/LF: {subject_line:?}",
    );
    assert!(
        subject_line.contains("Bcc: attacker"),
        "Subject value must contain the attacker bytes verbatim (with CR/LF replaced by spaces); got {subject_line:?}",
    );
}

#[test]
fn sanitize_applied_to_subject_from_to_via_format_message() {
    // Attacker bytes in user (To: value), hostname (From:/To: value),
    // and subject all surface as sanitized header values when
    // format_message assembles the record. Pin all three call sites.
    //
    // Mutation target: skipping one of the three sanitize
    // calls in format_message (e.g. forgets `user`); the attacker
    // payload in that argument would split the headers.
    let now = Utc.with_ymd_and_hms(2026, 1, 2, 15, 4, 5).unwrap();
    let user = "alice\r\ninjected-user-header: x";
    let hostname = "host\nsmuggled-host-header: y";
    let subject = "subj\rinjected-subject-header: z";
    let msg = format_message(now, user, hostname, subject, "body");

    // No header value may carry a raw CR or LF — every newline in
    // the message body must come from format_message itself, not
    // from leaked user input.
    let header_block = msg
        .split_once("\n\n")
        .map(|(h, _)| h)
        .expect("separator present");
    for line in header_block.split('\n') {
        let lower = line.to_ascii_lowercase();
        assert!(
            !lower.starts_with("injected-user-header:"),
            "user payload bled through: {line:?}",
        );
        assert!(
            !lower.starts_with("smuggled-host-header:"),
            "hostname payload bled through: {line:?}",
        );
        assert!(
            !lower.starts_with("injected-subject-header:"),
            "subject payload bled through: {line:?}",
        );
    }
}

proptest! {
    #[test]
    fn sanitize_byte_length_invariant(input in "[\\x00-\\x7f]{0,256}") {
        // sanitize_header is a 1-to-1 byte mapping: every byte
        // either maps to itself (>= 0x20) or to a single 0x20
        // (< 0x20 and != 0x20). The 1-to-1 property gives the
        // operator a predictable byte budget when combined with
        // the BODY_BYTE_CAP check in src/mail/mbox.rs.
        //
        // Mutation target: using `.trim()` (1-to-zero
        // for leading/trailing whitespace) or escapes control
        // chars to "\\r\\n" (1-to-many). Either change diverges
        // from the invariant.
        let out = sanitize_header(&input);
        prop_assert_eq!(out.len(), input.len());
    }
}

#[test]
fn sanitize_handles_utf8_multibyte_correctly() {
    // The sanitizer operates on bytes (control bytes are 0..0x1F
    // — all ASCII). Multi-byte UTF-8 sequences contain only bytes
    // >= 0x80, so byte-level replacement preserves them intact.
    //
    // Mutation target: iterating char-by-char but
    // mistakenly treats a continuation byte (0x80..0xBF) as its
    // own char — the codepoint splits and the output is invalid
    // UTF-8 (or, worse, corrupts the next codepoint).
    assert_eq!(sanitize_header("中文\nEnglish"), "中文 English");
    // Round-trip: replacing only the LF leaves the codepoints
    // untouched. Confirm the multi-byte sequence survives byte-
    // identical.
    let input = "中文\nEnglish";
    let out = sanitize_header(input);
    let in_bytes = input.as_bytes();
    let out_bytes = out.as_bytes();
    // The CJK bytes (positions 0..3 and 3..6) must survive
    // byte-identical; only the LF at position 6 is replaced.
    assert_eq!(&out_bytes[0..3], &in_bytes[0..3]);
    assert_eq!(&out_bytes[3..6], &in_bytes[3..6]);
    assert_eq!(out_bytes[6], b' ');
    assert_eq!(&out_bytes[7..], &in_bytes[7..]);
}

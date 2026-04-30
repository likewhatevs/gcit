// mbox wire format: From_ separator line, header set, blank-line
// separator, body, trailing blank line.
//
// `gcit::mail::mbox::format_message(now, user, hostname, subject, body)`
// produces the bytes the notifier appends to /var/mail/<user>. All
// rendering (handlebars), sanitization, and mboxrd quoting are done
// inline in `format_message`; tests here exercise the assembly layer
// directly without touching the spool. The handlebars-render path
// for subject/body lives one layer up in `LocalMailNotifier`; tests
// at this layer pin the wire-format invariants only.

use chrono::{TimeZone, Utc};
use rstest::rstest;

use gcit::mail::mbox::{format_from_line_date, format_message};

/// Deterministic UTC timestamp used by every test that doesn't
/// otherwise need a specific date. UTC midday gives a comfortable
/// ±12h margin against local-tz day-boundary crossings (no realistic
/// timezone shifts UTC 12:00 across midnight in either direction —
/// max offsets are -12 and +14).
fn fixed_now() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 15, 12, 0, 0).unwrap()
}

#[test]
fn message_starts_with_from_separator_line() {
    // First line: `From gcit <asctime>\n`. Five-byte prefix is
    // literally "From " (F-r-o-m-space, NO colon). Mailbox readers
    // identify message boundaries by this exact byte sequence at
    // column 0. A "From: " (colon) prefix would parse as a regular
    // header and break message separation.
    //
    // Mutation target: writing "From: gcit ..." (with a
    // colon) — concatenated mboxes lose message boundaries.
    let msg = format_message(fixed_now(), "alice", "host", "subj", "body");
    assert!(
        msg.starts_with("From "),
        "first 5 bytes must be literal 'From ' (no colon); got: {:?}",
        &msg[..msg.len().min(20)],
    );
    // Reject the colon variant explicitly.
    assert!(
        !msg.starts_with("From:"),
        "'From:' (with colon) is a header, not an mbox separator; got: {:?}",
        &msg[..msg.len().min(20)],
    );
    // Sender token is "gcit"; date follows after a space.
    assert!(
        msg.starts_with("From gcit "),
        "first line must start 'From gcit '; got: {:?}",
        &msg[..msg.len().min(30)],
    );
    // First line ends with '\n' before any header. Locate the first
    // newline and confirm what's between "From " and that newline
    // is the asctime-formatted string we'd produce directly.
    let first_nl = msg.find('\n').expect("first line must end with newline");
    let from_line = &msg[..first_nl];
    let expected_date = format_from_line_date(fixed_now());
    assert_eq!(
        from_line,
        format!("From gcit {expected_date}"),
        "first line must be 'From gcit <format_from_line_date>'; got {from_line:?}",
    );
}

#[rstest]
// Single-digit day-of-month: asctime emits double space (space-pad
// the day to width 2). Test passes a UTC midday timestamp — local
// shifts of ±12h still keep the day single-digit (4..6 inclusive).
#[case::single_digit_day(5, true)]
// Two-digit day-of-month: single space between month and day. UTC
// midday on day 15 stays in 14..16 across all real timezones.
#[case::two_digit_day(15, false)]
// Late-month single-digit edge: UTC midday day-of-month=2 (Feb 2)
// stays in 1..3 across timezones — all single-digit.
#[case::leap_window(2, true)]
fn asctime_format_pads_day_with_space(#[case] day: u32, #[case] expect_double_space: bool) {
    // chrono's `%e` formatter is the asctime space-padded day; `%d`
    // is zero-padded ("02"). format_from_line_date uses `%e`, so
    // single-digit days must produce a leading SPACE in the day
    // position (e.g., "Jan  5"), not a leading ZERO ("Jan 05").
    //
    // The assertion strategy is robust to local-timezone shifts:
    // we don't pin an exact day; we pin whether the formatted
    // string contains a "Mon  D" (two spaces, single-digit day)
    // pattern OR a "Mon DD" (single space, two-digit day) pattern.
    let utc_dt = Utc.with_ymd_and_hms(2026, 2, day, 12, 0, 0).unwrap();
    let formatted = format_from_line_date(utc_dt);

    // The asctime token following the month abbreviation: either
    // "Feb  D" (single-digit day, double space) or "Feb DD"
    // (two-digit day, single space). Locate the "Feb " marker and
    // inspect the following byte.
    let feb_marker = formatted.find("Feb ").expect("expected Feb in output");
    let after_feb = &formatted[feb_marker + 4..];
    // The byte immediately after "Feb " is either ' ' (single-digit
    // day case) or a digit (two-digit day case).
    let first_after_byte = after_feb.bytes().next().expect("text after Feb");
    if expect_double_space {
        assert_eq!(
            first_after_byte, b' ',
            "single-digit day (UTC day={day}) must use space-padding (asctime %e); got: {formatted:?}",
        );
        // The byte after that is a single ASCII digit.
        assert!(
            after_feb.as_bytes()[1].is_ascii_digit(),
            "single-digit day must follow the padding space; got: {formatted:?}",
        );
    } else {
        assert!(
            first_after_byte.is_ascii_digit(),
            "two-digit day (UTC day={day}) must NOT have a leading padding space; got: {formatted:?}",
        );
        assert!(
            after_feb.as_bytes()[1].is_ascii_digit(),
            "two-digit day must have two ASCII digits; got: {formatted:?}",
        );
    }
}

#[test]
fn headers_block_separated_from_body_by_single_blank_line() {
    // Single blank line ('\n\n' boundary) divides headers from body.
    // The full message structure (per format_message in mail::mbox):
    //   <From line>\n
    //   Date: <rfc5322>\n
    //   From: gcit@<host>\n
    //   To: <user>@<host>\n
    //   Subject: <subj>\n
    //   Content-Type: text/plain; charset=utf-8\n
    //   MIME-Version: 1.0\n
    //   \n  ← this is the headers/body separator
    //   <body>\n
    //   \n  ← trailing blank line
    //
    // Mutation target: concatenating headers without the
    // separator newline — the body becomes a continuation of the
    // last header.
    let msg = format_message(fixed_now(), "alice", "host", "subj-token", "body-token");
    let sep_idx = msg.find("\n\n").expect("must contain a '\\n\\n' separator");
    let header_section = &msg[..sep_idx];
    let body_and_tail = &msg[sep_idx + 2..];

    // Subject must appear in the header section.
    assert!(
        header_section.contains("Subject: subj-token"),
        "Subject header must precede the separator; got headers:\n{header_section}",
    );
    // Body token must appear AFTER the separator, not before.
    assert!(
        !header_section.contains("body-token"),
        "body must not appear before the headers/body separator",
    );
    assert!(
        body_and_tail.contains("body-token"),
        "body must appear after the separator",
    );
    // No header line in the header section may be preceded by a
    // double newline (which would mean two consecutive blank lines
    // inside the headers — a double-separator).
    assert!(
        !header_section.contains("\n\n"),
        "headers section must not contain a blank line; got:\n{header_section}",
    );
}

#[test]
fn message_ends_with_trailing_blank_line() {
    // Per RFC 4155, each mbox record ends with a blank line so the
    // next "From " separator can start at column 0. format_message
    // ensures this via the explicit trailing '\n' followed by the
    // record terminator '\n'.
    //
    // Mutation target: omitting the trailing newline; two
    // consecutive messages have no separator and the second
    // message's "From " line ends up appended to the first
    // message's body.
    let msg = format_message(fixed_now(), "alice", "host", "subj", "body");
    assert!(
        msg.ends_with("\n\n"),
        "message must end with two newlines (body terminator + record terminator); got tail: {:?}",
        &msg[msg.len().saturating_sub(10)..],
    );
}

#[test]
fn headers_are_canonical_set_in_order() {
    // The header section produces these lines in this order:
    //   Date
    //   From
    //   To
    //   Subject
    //   Content-Type
    //   MIME-Version
    //
    // mailx, postfix, etc. tolerate other orderings, but pinning
    // this canonical order guards against silent reorderings that
    // could break stricter downstream parsers.
    let msg = format_message(fixed_now(), "alice", "host", "subj", "body");
    let header_section = msg
        .split_once("\n\n")
        .map(|(h, _)| h)
        .expect("header/body separator present");
    let header_lines: Vec<&str> = header_section.split('\n').collect();

    // First line is the From_ separator (not a header).
    let header_only: Vec<&str> = header_lines.iter().skip(1).copied().collect();

    // Build the prefix list (header name + ":") in canonical order
    // and verify each appears once at its expected position.
    let canonical_prefixes = [
        "Date:",
        "From:",
        "To:",
        "Subject:",
        "Content-Type:",
        "MIME-Version:",
    ];
    assert_eq!(
        header_only.len(),
        canonical_prefixes.len(),
        "expected exactly {} headers; got {}: {header_only:?}",
        canonical_prefixes.len(),
        header_only.len(),
    );
    for (i, prefix) in canonical_prefixes.iter().enumerate() {
        assert!(
            header_only[i].starts_with(prefix),
            "header position {i} must start with {prefix:?}; got {:?}",
            header_only[i],
        );
    }
}

#[test]
fn date_header_uses_rfc5322_format() {
    // Date header carries an RFC 5322 fixed-format date:
    //   "Mon, 02 Jan 2026 15:04:05 +0000"
    // format_rfc5322_date emits the chrono format string
    //   "%a, %d %b %Y %H:%M:%S %z"
    // Day-of-month is zero-padded (%d) — different from the From_
    // line's space-padded %e.
    //
    // Mutation target: swapping to %T (time without
    // seconds) or omits %z (timezone offset) — RFC violation, some
    // strict MUAs reject the message.
    let dt = Utc.with_ymd_and_hms(2026, 1, 2, 15, 4, 5).unwrap();
    let msg = format_message(dt, "alice", "host", "subj", "body");
    let date_line = msg
        .lines()
        .find(|l| l.starts_with("Date: "))
        .expect("Date header present");
    let date_value = &date_line["Date: ".len()..];
    let expected = gcit::mail::mbox::format_rfc5322_date(dt);
    assert_eq!(
        date_value, expected,
        "Date header must carry the format_rfc5322_date output verbatim",
    );
    // RFC 5322 shape: "Day, DD Mon YYYY HH:MM:SS ±ZZZZ"
    // Pin the structural components (zero-padded day, abbreviated
    // month, four-digit year) without binding to a specific
    // weekday — chrono renders weekday in LOCAL time, so the
    // weekday depends on the test runner's timezone (e.g., UTC-08
    // shifts UTC 15:04 Jan 2 to Local 07:04 Jan 2, but the
    // weekday calc itself is date-agnostic — Jan 2 2026 is
    // Friday everywhere). Pinning ", 02 Jan 2026" covers the
    // zero-pad invariant which is the actual mutation target
    // (vs %e space-pad in the From_ line).
    assert!(
        date_value.contains(", 02 Jan 2026"),
        "Date must contain ', 02 Jan 2026' (zero-padded day, abbrev month, 4-digit year); got {date_value:?}",
    );
    // First three chars are the abbreviated weekday (e.g., "Thu",
    // "Fri") followed by ", ". Pin the comma+space separator
    // shape without committing to a specific weekday.
    assert_eq!(
        &date_value[3..5],
        ", ",
        "RFC 5322 weekday must be followed by ', '; got {date_value:?}",
    );
    // Timezone offset is the last 5 chars (e.g., "+0000" or
    // "-0700"); pin via a sign-and-four-digits check at the end.
    let bytes = date_value.as_bytes();
    let n = bytes.len();
    assert!(
        n >= 5
            && (bytes[n - 5] == b'+' || bytes[n - 5] == b'-')
            && bytes[n - 4..].iter().all(u8::is_ascii_digit),
        "timezone suffix must be ±HHMM (5 chars); got {date_value:?}",
    );
}

#[test]
fn from_header_is_gcit_at_hostname() {
    // From header is `gcit@<hostname>`. format_message passes the
    // hostname argument through `sanitize_header` but ASCII hostnames
    // pass through verbatim.
    let msg = format_message(fixed_now(), "alice", "host.example", "subj", "body");
    let from_line = msg
        .lines()
        .find(|l| l.starts_with("From: "))
        .expect("From header present");
    assert_eq!(from_line, "From: gcit@host.example");
}

#[test]
fn to_header_is_user_at_hostname() {
    // To header is `<user>@<hostname>`. format_message runs both
    // fields through `sanitize_header` and concatenates them around
    // a literal '@'.
    let msg = format_message(fixed_now(), "ops", "host.example", "subj", "body");
    let to_line = msg
        .lines()
        .find(|l| l.starts_with("To: "))
        .expect("To header present");
    assert_eq!(to_line, "To: ops@host.example");
}

#[test]
fn content_type_is_text_plain_utf8() {
    // Pin the literal Content-Type. Mutation target: using
    // uses `charset=us-ascii` and any non-ASCII rendered body bytes
    // show up as "?" in mailx.
    let msg = format_message(fixed_now(), "alice", "host", "subj", "body");
    let ct_line = msg
        .lines()
        .find(|l| l.starts_with("Content-Type:"))
        .expect("Content-Type header present");
    assert_eq!(ct_line, "Content-Type: text/plain; charset=utf-8");
}

#[test]
fn mime_version_header_present() {
    // Without MIME-Version, the Content-Type charset declaration is
    // ignored by some MUAs. Pin the literal "1.0" spelling.
    let msg = format_message(fixed_now(), "alice", "host", "subj", "body");
    let mv_line = msg
        .lines()
        .find(|l| l.starts_with("MIME-Version:"))
        .expect("MIME-Version header present");
    assert_eq!(mv_line, "MIME-Version: 1.0");
}

#[test]
fn body_arg_appears_in_body_section_verbatim() {
    // The handlebars rendering for body lives one layer up
    // (LocalMailNotifier::on_run_complete renders the template
    // before calling format_message). At the format_message layer
    // the body argument is passed through (with mboxrd quoting
    // applied — covered by tests/mail_mboxrd_quoting.rs). For a
    // body with no `>*From ` lines, mboxrd is a byte-identical
    // pass-through, so we can pin the literal body text appears in
    // the wire output.
    //
    // Mutation target: dropping the body argument (using an
    // empty body or substitutes a default).
    let body = "ci-flow: failure\nbuild step 1 failed\n";
    let msg = format_message(fixed_now(), "alice", "host", "subj", body);
    // Body sits after the headers/body separator.
    let (_, body_and_tail) = msg
        .split_once("\n\n")
        .expect("headers/body separator present");
    assert!(
        body_and_tail.starts_with(body),
        "body section must begin with the rendered body bytes verbatim; got:\n{body_and_tail}",
    );
}

#[test]
fn subject_arg_appears_in_subject_header_verbatim() {
    // The handlebars rendering for subject lives one layer up
    // (LocalMailNotifier::on_run_complete renders the template
    // before calling format_message). At the format_message layer
    // the subject argument is passed through `sanitize_header`;
    // ASCII strings pass through verbatim.
    //
    // Note on the original stub's claim of a default
    // "{{flow.name}}: {{run.conclusion}}" template: the production
    // default in mail::notifier::on_run_complete is actually
    // `format!("[gcit] {} {}", flow_name, label)` — a Rust format!
    // call, not a handlebars template. Custom subject handlebars
    // rendering at the notifier layer is covered by the
    // notifier-spool tests gated on tempdir spool injection.
    //
    // Mutation target: hardcoding a Subject value or
    // drops the subject argument.
    let subject = "ci-flow: failure";
    let msg = format_message(fixed_now(), "alice", "host", subject, "body");
    let subject_line = msg
        .lines()
        .find(|l| l.starts_with("Subject: "))
        .expect("Subject header present");
    assert_eq!(subject_line, format!("Subject: {subject}"));
}

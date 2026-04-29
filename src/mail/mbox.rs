// mbox formatting helpers: mboxrd `>From ` quoting, header
// sanitization, asctime date formatting, body cap enforcement.
//
// Wire format produced by `format_message`:
//
//     From gcit <asctime>
//     Date: <rfc5322>
//     From: gcit@<hostname>
//     To: <user>@<hostname>
//     Subject: <sanitized subject>
//     Content-Type: text/plain; charset=utf-8
//     MIME-Version: 1.0
//
//     <mboxrd-quoted body>
//     <trailing blank line>
//
// mboxrd variant (RFC 4155) for body quoting; the leading separator
// is `From gcit ...` (note: no colon between `From` and the next
// space) followed by an asctime date in local time. Header values
// pass through `sanitize_header` which replaces every byte < 0x20
// (other than literal space) with a single space — this blocks
// CRLF injection where attacker-controlled data carries `\r\n`
// followed by additional headers (e.g. `Bcc:` to redirect mail).
//
// All helpers here are pure-data: they take strings/bytes and
// produce bytes. The actual file I/O (open, flock, write,
// sync_all) lives in `notifier.rs`.

use chrono::{DateTime, Local, Utc};

/// Body byte cap. Rendered body exceeding this surfaces
/// `NotifyError::Permanent` at the call site so operators see the
/// failure in `gcit status` last_error. 64 KiB is generous for
/// legitimate mail content while staying well within memory and
/// disk budgets for a notifier hot path.
pub const BODY_BYTE_CAP: usize = 64 * 1024;

/// asctime date format used in the `From ` separator line:
///   `Mon Jan  2 15:04:05 2006`
/// Note the doubled space when the day-of-month is single-digit
/// (asctime's space-padded width-2 day). chrono's `%e` produces
/// the space-padded form.
const ASCTIME_FMT: &str = "%a %b %e %H:%M:%S %Y";

/// RFC 5322 date format used in the `Date:` header. Carries timezone
/// offset; chrono's `to_rfc2822` produces "Mon, 02 Jan 2026 15:04:05 +0000".
const RFC2822_FMT: &str = "%a, %d %b %Y %H:%M:%S %z";

/// Format `dt` as the asctime "From " separator timestamp.
///
/// The separator line embeds this date directly after the `gcit`
/// sender token:
/// ```text
/// From gcit Mon Jan  2 15:04:05 2006
/// ```
///
/// asctime is timezone-naive — we render in local time per Unix
/// mail convention; mailx, postfix, etc. all use the local clock
/// for the From-line. UTC would technically be more reproducible
/// but would break compatibility with downstream consumers that
/// assume local time.
pub fn format_from_line_date(dt: DateTime<Utc>) -> String {
    let local: DateTime<Local> = dt.with_timezone(&Local);
    local.format(ASCTIME_FMT).to_string()
}

/// Format `dt` as the RFC 5322 `Date:` header value. Carries
/// timezone offset.
pub fn format_rfc5322_date(dt: DateTime<Utc>) -> String {
    let local: DateTime<Local> = dt.with_timezone(&Local);
    local.format(RFC2822_FMT).to_string()
}

/// Sanitize a header value. Replaces every byte less than 0x20
/// (excluding the literal space 0x20) with a single space. This
/// blocks the classic CRLF injection where attacker-controlled
/// content carries `\r\n` followed by additional headers (e.g.
/// `Bcc:` to redirect mail).
///
/// The replacement is byte-by-byte rather than codepoint-by-
/// codepoint because the threat is at the wire layer: any 0x0A or
/// 0x0D byte in the rendered header value, regardless of whether
/// it's part of a valid UTF-8 codepoint, ends the header in
/// RFC 5322. We preserve everything ≥ 0x20 (including all multi-
/// byte UTF-8 continuation bytes, which are all ≥ 0x80).
pub fn sanitize_header(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b < 0x20 && b != b' ' {
            out.push(' ');
            i += 1;
            continue;
        }
        // ASCII printable or start of UTF-8 sequence. Find the end
        // of the next codepoint and copy verbatim.
        let mut end = i + 1;
        while end < bytes.len() && (bytes[end] & 0b1100_0000 == 0b1000_0000) {
            end += 1;
        }
        // SAFETY: input is a &str so the byte range [i..end] is a
        // valid UTF-8 codepoint boundary (i was at a codepoint
        // start, and we walked through all continuation bytes).
        let s = std::str::from_utf8(&bytes[i..end]).expect("valid utf-8 slice");
        out.push_str(s);
        i = end;
    }
    out
}

/// mboxrd `>From ` quoting per RFC 4155.
///
/// Body lines whose first bytes match `>*From ` (zero or more `>`
/// followed by `From ` — the trailing space matters) get an
/// additional `>` prepended. This is lossless: stripping exactly
/// one leading `>` from any line whose remainder matches `>*From `
/// recovers the original.
///
/// Operates on lines split by `\n`. Trailing newline is preserved
/// when present in the input.
pub fn escape_mboxrd(body: &str) -> String {
    let mut out = String::with_capacity(body.len());
    for (i, line) in body.split('\n').enumerate() {
        if i > 0 {
            out.push('\n');
        }
        if needs_mboxrd_quote(line) {
            out.push('>');
        }
        out.push_str(line);
    }
    out
}

/// Whether `line` needs an extra `>` prefix per mboxrd. Matches
/// the regex `^>*From ` (zero or more `>`, then literal `From `).
fn needs_mboxrd_quote(line: &str) -> bool {
    let trimmed = line.trim_start_matches('>');
    trimmed.starts_with("From ")
}

/// Build the full mbox-formatted message bytes.
///
/// Layout:
/// ```text
/// From gcit <asctime>
/// Date: <rfc5322>
/// From: gcit@<hostname>
/// To: <user>@<hostname>
/// Subject: <sanitized subject>
/// Content-Type: text/plain; charset=utf-8
/// MIME-Version: 1.0
///
/// <mboxrd-quoted body>
/// <trailing blank line>
/// ```
///
/// All header values are sanitized via `sanitize_header`. The body
/// is mboxrd-quoted via `escape_mboxrd`. The body byte cap is
/// enforced upstream (in the notifier) because exceeding it surfaces
/// as `NotifyError::Permanent`, not a silent truncation.
pub fn format_message(
    now: DateTime<Utc>,
    user: &str,
    hostname: &str,
    subject: &str,
    body: &str,
) -> String {
    let from_line_date = format_from_line_date(now);
    let date_header = format_rfc5322_date(now);
    let mut out = String::with_capacity(256 + body.len());
    out.push_str("From gcit ");
    out.push_str(&from_line_date);
    out.push('\n');

    out.push_str("Date: ");
    out.push_str(&sanitize_header(&date_header));
    out.push('\n');

    out.push_str("From: gcit@");
    out.push_str(&sanitize_header(hostname));
    out.push('\n');

    out.push_str("To: ");
    out.push_str(&sanitize_header(user));
    out.push('@');
    out.push_str(&sanitize_header(hostname));
    out.push('\n');

    out.push_str("Subject: ");
    out.push_str(&sanitize_header(subject));
    out.push('\n');

    out.push_str("Content-Type: text/plain; charset=utf-8\n");
    out.push_str("MIME-Version: 1.0\n");

    // Blank line separating headers from body.
    out.push('\n');

    // mboxrd-quoted body.
    out.push_str(&escape_mboxrd(body));

    // Trailing blank line — terminates the mbox record.
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out.push('\n');

    out
}

/// Read `/etc/hostname` and return the trimmed contents. Falls
/// back to the literal "localhost" with a WARN log entry if the
/// file is unreadable.
///
/// Cached lookup happens at the daemon entry; this function is the
/// single source of truth. Callers should cache the result rather
/// than re-reading on every dispatch.
pub fn read_hostname_or_default() -> String {
    match std::fs::read_to_string("/etc/hostname") {
        Ok(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                tracing::warn!(
                    "/etc/hostname is empty; falling back to 'localhost' for mbox notifier",
                );
                "localhost".to_string()
            } else {
                trimmed.to_string()
            }
        }
        Err(err) => {
            tracing::warn!(
                error = %err,
                "/etc/hostname unreadable; falling back to 'localhost' for mbox notifier",
            );
            "localhost".to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_header_replaces_control_bytes() {
        // CR, LF, NUL, BEL → spaces.
        let input = "subject\r\nBcc: evil@example.com\x00alarm\x07";
        let out = sanitize_header(input);
        assert_eq!(out, "subject  Bcc: evil@example.com alarm ");
        assert!(!out.contains('\r'));
        assert!(!out.contains('\n'));
        assert!(!out.contains('\x00'));
    }

    #[test]
    fn sanitize_header_preserves_space() {
        // 0x20 (space) is NOT replaced.
        let out = sanitize_header("hello world");
        assert_eq!(out, "hello world");
    }

    #[test]
    fn sanitize_header_preserves_utf8_multibyte() {
        // Non-ASCII codepoints (≥ 0x80 bytes) survive intact.
        let input = "café émoji 🎉";
        let out = sanitize_header(input);
        assert_eq!(out, "café émoji 🎉");
    }

    #[test]
    fn sanitize_header_replaces_tab() {
        // Tab is 0x09 < 0x20, replaced with space.
        let out = sanitize_header("a\tb");
        assert_eq!(out, "a b");
    }

    #[test]
    fn escape_mboxrd_quotes_from_lines() {
        let body = "hello\nFrom me\nbye\n";
        let out = escape_mboxrd(body);
        assert_eq!(out, "hello\n>From me\nbye\n");
    }

    #[test]
    fn escape_mboxrd_quotes_already_quoted_lines() {
        // mboxrd is recursive: ">From " also gets quoted (becomes ">>From ").
        let body = ">From here\n>>From there";
        let out = escape_mboxrd(body);
        assert_eq!(out, ">>From here\n>>>From there");
    }

    #[test]
    fn escape_mboxrd_no_op_on_normal_lines() {
        let body = "hello\nworld\n";
        assert_eq!(escape_mboxrd(body), "hello\nworld\n");
    }

    #[test]
    fn escape_mboxrd_no_op_on_from_without_space() {
        // "Fromage" doesn't start with "From " (no trailing space) → no quoting.
        let body = "Fromage cheese";
        assert_eq!(escape_mboxrd(body), "Fromage cheese");
    }

    #[test]
    fn escape_mboxrd_lossless_round_trip() {
        // Stripping one leading > from any line that matches >*From
        // recovers the original.
        let original = "ok\nFrom me\n>From them\nbye";
        let escaped = escape_mboxrd(original);
        // Manual unquoting: drop one leading > from any line that
        // would still match >*From after the drop.
        let unquoted: String = escaped
            .split('\n')
            .map(|line| {
                if let Some(rest) = line.strip_prefix('>') {
                    if rest.trim_start_matches('>').starts_with("From ") {
                        return rest.to_string();
                    }
                }
                line.to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(unquoted, original);
    }

    #[test]
    fn format_message_contains_required_headers() {
        let now: DateTime<Utc> = "2026-04-26T12:00:00Z".parse().unwrap();
        let msg = format_message(now, "alice", "myhost", "Test subject", "Hello");
        // From-line is the mbox separator.
        assert!(msg.starts_with("From gcit "));
        // Required headers.
        assert!(msg.contains("\nDate: "));
        assert!(msg.contains("\nFrom: gcit@myhost\n"));
        assert!(msg.contains("\nTo: alice@myhost\n"));
        assert!(msg.contains("\nSubject: Test subject\n"));
        assert!(msg.contains("\nContent-Type: text/plain; charset=utf-8\n"));
        assert!(msg.contains("\nMIME-Version: 1.0\n"));
        // Headers/body separator.
        assert!(msg.contains("\n\nHello"));
        // Trailing blank line.
        assert!(msg.ends_with("\n\n"));
    }

    #[test]
    fn format_message_quotes_from_lines_in_body() {
        let now: DateTime<Utc> = "2026-04-26T12:00:00Z".parse().unwrap();
        let body = "ok\nFrom them\nbye";
        let msg = format_message(now, "u", "h", "subj", body);
        assert!(msg.contains("\n>From them\n"), "msg: {msg:?}");
    }

    #[test]
    fn format_message_sanitizes_subject() {
        // CRLF in subject must NOT survive as a header break — that
        // would inject a Bcc header. The text "Bcc: ..." is allowed
        // to remain inside the subject; what matters is that
        // there's no actual header separator (CR or LF) preceding
        // it. Verify by counting how many lines start with "Bcc:" —
        // if sanitization fired, the answer is zero (the literal
        // "Bcc: ..." text stays inside the Subject line).
        let now: DateTime<Utc> = "2026-04-26T12:00:00Z".parse().unwrap();
        let msg = format_message(now, "u", "h", "subj\r\nBcc: evil@example.com", "body");
        let bcc_starts = msg.lines().filter(|l| l.starts_with("Bcc:")).count();
        assert_eq!(
            bcc_starts, 0,
            "no line may start with 'Bcc:'; the injected text must \
             be absorbed into the Subject line. msg:\n{msg}",
        );
        // The injected fragment is still in the message body (as
        // part of the Subject line value); CRLF were replaced with
        // spaces.
        let subject_line = msg.lines().find(|l| l.starts_with("Subject:")).unwrap();
        assert!(!subject_line.contains('\r'));
        assert!(!subject_line.contains('\n'));
    }

    #[test]
    fn body_byte_cap_pinned_at_64_kib() {
        assert_eq!(BODY_BYTE_CAP, 64 * 1024);
    }

    #[test]
    fn format_from_line_date_uses_asctime() {
        // asctime example: "Sun Apr 26 12:00:00 2026". The exact
        // local-zone-shifted output depends on the runner — pin
        // structure (length + tokens) rather than literal text.
        let now: DateTime<Utc> = "2026-04-26T12:00:00Z".parse().unwrap();
        let s = format_from_line_date(now);
        assert!(
            s.len() == 24,
            "asctime is fixed-width 24 chars; got {} chars: {s:?}",
            s.len(),
        );
        // Must contain the year as digits.
        assert!(s.contains("2026"), "missing year: {s:?}");
    }

    #[test]
    fn read_hostname_or_default_returns_non_empty_string() {
        // /etc/hostname is universally readable in dev / CI; the
        // function returns *something* either way.
        let h = read_hostname_or_default();
        assert!(!h.is_empty());
    }
}

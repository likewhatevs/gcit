// mboxrd quoting (RFC 4155): >*From escaping in body bytes.
//
// gcit uses the **mboxrd** quoting variant (lossless): body lines
// matching the regex `^>*From ` get an additional `>` prepended.
// The reader recovers the original by stripping exactly one `>`
// from any line matching `^>+From `. Implementation:
// src/mail/mbox.rs::escape_mboxrd.
//
// mboxrd vs mboxo:
//   mboxo : quote ANY line starting with "From " regardless of
//           preceding ">". LOSSY: the reader strips one ">"
//           unconditionally, so ">From" and "From" both round-trip
//           to "From".
//   mboxrd: quote any line matching /^>*From /. LOSSLESS: the
//           reader strips exactly one ">" from any line matching
//           /^>+From /, preserving the original ">" count.

use proptest::prelude::*;
use rstest::rstest;

use gcit::mail::mbox::escape_mboxrd;

#[rstest]
// Bare "From " at column 0: prepend ">"
#[case::bare_from("From hello\n", ">From hello\n")]
// "From" without trailing space: NOT quoted (the trailing space matters).
#[case::from_no_space("From\n", "From\n")]
// "From: " (colon then space): NOT quoted — pattern is "From "
// (literal space at offset 4, no colon).
#[case::from_colon("From: alice@example.com\n", "From: alice@example.com\n")]
// One ">" already present: prepend another.
#[case::single_quote(">From hello\n", ">>From hello\n")]
// Two ">" already present: prepend another.
#[case::double_quote(">>From hello\n", ">>>From hello\n")]
// Pathological deep quoting: still gets one more.
#[case::deep_quote(">>>>>>>>>>>>>>>>From hello\n", ">>>>>>>>>>>>>>>>>From hello\n")]
// Not at column 0: NOT quoted ("Tagged: From hi" doesn't match /^>*From /).
#[case::leading_text("Tagged: From hi\n", "Tagged: From hi\n")]
// Multi-line body: only lines that match the pattern get quoted.
#[case::multiline_no_match("first line\nFrom: second\n", "first line\nFrom: second\n")]
#[case::multiline_actual_quote("first line\nFrom hello\n", "first line\n>From hello\n")]
// CRLF input: gcit splits on '\n' only — '\r' stays attached to
// the prior line. The "From hi" line still gets quoted because
// the regex anchors at the start of the line (post-split), and
// "From hi\r" starts with "From ".
#[case::crlf_input("first\r\nFrom hi\r\n", "first\r\n>From hi\r\n")]
// Empty input: empty output.
#[case::empty("", "")]
// Whitespace-only line: pattern doesn't match -> no quote.
#[case::whitespace_line("   \n", "   \n")]
fn escape_mboxrd_table(#[case] input: &str, #[case] expected: &str) {
    assert_eq!(escape_mboxrd(input), expected);
}

/// Reference inverse of `escape_mboxrd` per RFC 4155: strip
/// exactly one leading `>` from any line matching `^>+From `.
/// All other lines pass through verbatim.
///
/// Used by the round-trip property test below as the canonical
/// reader behaviour. Lives here (not in production code) because
/// gcit only WRITES mboxrd — readers downstream (mailx, postfix,
/// maildrop) supply their own unquoting.
fn unescape_mboxrd(escaped: &str) -> String {
    let mut out = String::with_capacity(escaped.len());
    for (i, line) in escaped.split('\n').enumerate() {
        if i > 0 {
            out.push('\n');
        }
        // Strip one `>` if the line matches `^>+From `.
        if let Some(stripped) = line.strip_prefix('>') {
            let rest = stripped.trim_start_matches('>');
            if rest.starts_with("From ") {
                out.push_str(stripped);
                continue;
            }
        }
        out.push_str(line);
    }
    out
}

proptest! {
    #[test]
    fn escape_mboxrd_lossless_round_trip(input in "[a-zA-Z0-9 >\\n]{0,256}") {
        // For arbitrary body content drawn from a charset that
        // includes the meaningful tokens (`>`, `F`, `r`, `o`, `m`,
        // space, newline), escape then unescape round-trips to the
        // input byte-for-byte.
        //
        // The character class is tight enough that every meaningful
        // mboxrd corner case (>From, >>From, From with no space,
        // empty lines) is reachable; using `.*` would generate
        // mostly-irrelevant unicode that doesn't exercise the
        // actual escape regex.
        let escaped = escape_mboxrd(&input);
        let recovered = unescape_mboxrd(&escaped);
        prop_assert_eq!(recovered, input);
    }
}

#[test]
fn escape_mboxrd_handles_attacker_payload() {
    // Adversarial body that tries to inject a second mbox record:
    let payload = concat!(
        "Content here\n",
        "From evil@attacker Sun Jan  1 00:00:00 2000\n",
        "From: <fake-From-header>\n",
        "Subject: hijack\n",
        "\n",
        "Forged body content\n",
    );
    let escaped = escape_mboxrd(payload);

    // After escaping, no body line may match the bare /^From /
    // pattern — those would be interpreted by an mbox reader as
    // a new message separator.
    for line in escaped.split('\n') {
        assert!(
            !line.starts_with("From "),
            "unquoted From line slipped through: {line:?}",
        );
    }

    // The "From evil@attacker ..." line specifically must now
    // be ">From evil@attacker ...".
    assert!(
        escaped.contains(">From evil@attacker Sun Jan"),
        "attacker From-line must be quoted; got:\n{escaped}",
    );
}

#[test]
fn escape_mboxrd_does_not_modify_non_from_lines() {
    // Lines that don't match /^>*From / pass through byte-for-byte.
    // Cover the easy-to-confuse near-matches: trailing-space
    // omission, leading-text variants, lowercase, non-Latin scripts.
    let cases: &[(&str, &str)] = &[
        ("hello world\n", "hello world\n"),
        (">just a quote\n", ">just a quote\n"),
        (">>nested quote\n", ">>nested quote\n"),
        ("Frommy\n", "Frommy\n"),
        ("from lowercase\n", "from lowercase\n"),
        ("FROM uppercase\n", "FROM uppercase\n"),
        // No trailing newline: pass through.
        ("plain", "plain"),
    ];
    for (input, expected) in cases {
        assert_eq!(
            escape_mboxrd(input),
            *expected,
            "input {input:?} should pass through unchanged",
        );
    }
}

proptest! {
    #[test]
    fn escape_mboxrd_no_panic(input in "[\\x00-\\x7f]{0,1024}") {
        // escape_mboxrd takes &str, so we draw inputs from valid
        // UTF-8 (ASCII subset suffices to exercise every code
        // path — the function operates byte-by-byte on the
        // line-split stream and never branches on codepoints).
        // No panic, no crash, regardless of input shape.
        let _ = escape_mboxrd(&input);
    }
}

#[test]
fn escape_mboxrd_zero_length_lines_not_quoted() {
    // An empty line (just "\n" between content) doesn't match
    // /^>*From / and passes through unchanged. The trailing blank
    // line that mbox requires as the message terminator is
    // preserved.
    assert_eq!(escape_mboxrd("a\n\nb\n"), "a\n\nb\n");
    // Multiple consecutive blank lines: all preserved.
    assert_eq!(escape_mboxrd("\n\n\n"), "\n\n\n");
    // Single newline: preserved as a single empty record.
    assert_eq!(escape_mboxrd("\n"), "\n");
}

#[test]
fn escape_mboxrd_preserves_byte_count_when_no_change_needed() {
    // For inputs with NO line matching /^>*From /, escape_mboxrd's
    // output is byte-identical to the input. This pins the
    // "no allocation hit on the common path" intent without
    // committing to a specific signature (Cow vs String) — the
    // test only asserts byte equality post-escape.
    let safe_inputs = [
        "hello world\n",
        "subject line\nbody line\nfooter\n",
        ">quote\n",
        "From: header-style line\n",
        "",
        "\n",
    ];
    for input in safe_inputs {
        let out = escape_mboxrd(input);
        assert_eq!(
            out, input,
            "input {input:?} contains no /^>*From / line — escape must be a no-op",
        );
        assert_eq!(
            out.len(),
            input.len(),
            "byte count must match for no-op input {input:?}",
        );
    }
}

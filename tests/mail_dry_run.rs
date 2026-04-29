// gcit trigger --dry-run for local_mail destinations.
// `gcit trigger <FLOW> [--dry-run]` manually fires or prints dry-run
// payloads.
//
// Mail dry-run prints the rendered mbox bytes WITHOUT writing to the
// spool. Operators verify subject/body templating and headers before
// going live.
//
// mbox local_mail does NOT trigger redaction in dry-run (no
// credential).
// Cross-references tests/discord_dry_run.rs (Discord-side mirror).

#[tokio::test]
#[ignore = "requires gcit::cli::trigger::run (not yet implemented)"]
async fn dry_run_does_not_write_to_spool() {
    // Setup: tempdir/spool empty.
    // gcit trigger linux-mainline-ci --dry-run.
    // Assert: spool size unchanged (still 0).
    //
    // Mutation target: implementer wires --dry-run only into the
    // dispatcher path but not the mail path. Test catches via the
    // post-condition spool size.
}

#[test]
#[ignore = "requires gcit trigger --dry-run feature (not yet implemented)"]
fn dry_run_prints_mbox_bytes_to_stdout() {
    // Captured stdout contains the mbox-formatted bytes that WOULD
    // have been appended:
    //   destination[N]: local_mail (user=ops)
    //   spool path: /var/mail/ops
    //   ---
    //   From gcit Mon Jan  2 15:04:05 2026
    //   Date: ...
    //   From: gcit@host
    //   To: ops@host
    //   Subject: linux-mainline-ci: Success
    //   Content-Type: text/plain; charset=utf-8
    //   MIME-Version: 1.0
    //
    //   <body>
    //
    //   ---
    //
    // SPEC GAP: spec doesn't pin the exact dry-run output format.
    // Recommend the structure above (header/separator/body/separator).
    // flag.
}

#[test]
#[ignore = "requires gcit trigger --dry-run feature (not yet implemented)"]
fn dry_run_does_not_redact_anything_in_local_mail_payload() {
    // `gcit status` and `gcit trigger --dry-run` redact credential
    // values.
    //
    // local_mail destinations have NO credential (LocalMailConfig has
    // no credential_id field). So there is nothing to redact in the
    // payload.
    //
    // The user value (e.g., "ops") is NOT a credential — it's a public
    // local username. The hostname (from /etc/hostname) is NOT a
    // credential. The rendered body and subject contain only namespaced
    // template variables (flow.name, source.sha, etc.) — none are
    // credential-derived.
    //
    // Pin: dry-run output for a local_mail destination contains NO
    // [REDACTED] markers.
    //
    // Mutation target: implementer's dry-run pipeline applies redaction
    // globally and over-redacts. E.g., the user "ops" matches a
    // credential_id pattern (some implementer might have a
    // overly-broad redactor that flags any string with `_id` in its
    // path).
    //
    // assert!(!stdout.contains("[REDACTED]"));
}

#[test]
#[ignore = "requires gcit trigger --dry-run feature (not yet implemented)"]
fn dry_run_validates_template_at_dry_run_time() {
    // Per discord_dry_run.rs::dry_run_with_template_compile_error_surfaces_at_dry_run_time
    // — same applies for mail.
    //
    // A malformed mail subject template causes dry-run to exit with
    // EX_DATAERR=65 (per spec recommendation in discord_dry_run.rs).
    //
    // Setup: malformed template, call dry-run.
    // Assert exit 65; spool unchanged.
}

#[test]
#[ignore = "requires gcit trigger --dry-run feature (not yet implemented)"]
fn dry_run_for_each_local_mail_destination_in_flow() {
    // A flow with two local_mail destinations: dry-run prints BOTH
    // payloads.
    //
    // Pin via assert_contains for both "user=ops" and "user=alerts"
    // sections in the captured stdout.
    //
    // Mutation target: dry-run iterates only the first destination.
}

#[test]
#[ignore = "requires gcit trigger --dry-run feature (not yet implemented)"]
fn dry_run_runs_writability_check_but_warns_only() {
    // SPEC GAP: should dry-run perform the spool writability check?
    // Recommend YES with WARN-only behavior — operator gets a heads-up
    // that the spool isn't writable, but dry-run still prints the
    // payload that WOULD have been appended.
    //
    // Setup: spool 0444 (read-only).
    // Run dry-run.
    // Assert: stdout contains the mbox bytes AND a stderr WARN
    // "spool /var/mail/ops not writable; live append would fail".
    // Exit code: 0 (dry-run is informational).
    //
    // flag — confirm WARN vs HARD-fail for dry-run.
}

#[test]
#[ignore = "requires gcit trigger --dry-run feature (not yet implemented)"]
fn dry_run_preview_uses_current_time_for_date_header() {
    // The dry-run output's Date and From header (asctime) use NOW
    // (chrono::Utc::now() / chrono::Local::now()), not a fixed value.
    //
    // Mutation target: dry-run uses a hardcoded fake date (e.g., the
    // chrono epoch). Operator can't tell what dry-run "would" emit
    // for the actual message. Test catches via a redaction filter
    // applied to the rendered Date/From headers, then asserts the
    // canonical shape.
}

#[test]
#[ignore = "requires gcit trigger --dry-run feature (not yet implemented)"]
fn dry_run_skips_handlebars_for_destinations_with_template_compile_errors() {
    // SPEC GAP: behavior when ONE destination has a template error and
    // others are fine. Options:
    //   (a) print errors for ALL bad destinations, dry-run successful
    //       destinations normally, exit non-zero.
    //   (b) abort at first error, print only that one.
    // Recommend (a) — operator sees the FULL impact of their config in
    // one dry-run cycle.
    //
    // flag.
}

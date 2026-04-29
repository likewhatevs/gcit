// gcit trigger --dry-run payload output for Discord destinations.
// `gcit trigger <FLOW> [--dry-run]` exits 0, EX_USAGE=64, or
// EX_TEMPFAIL=75.
// `gcit status` and `gcit trigger --dry-run` redact credential values
// in any rendered output (resolved values, dispatch payloads, webhook
// URLs). The redaction substitutes [REDACTED]; the credential_id
// itself is shown so users can identify which entry is in use.
//
// dry-run prints the full request that WOULD have been sent (URL, headers,
// JSON body) without making the network call. Operators use this to verify
// templating, embed shape, and credential resolution before going live.
//
// Behavior: gcit trigger --dry-run prints all outbound payloads.

use rstest::rstest;

#[tokio::test]
#[ignore = "requires gcit::cli::trigger::run (not yet implemented)"]
async fn dry_run_does_not_make_network_call() {
    // wiremock mounted; gcit trigger linux-mainline-ci --dry-run.
    // Assert wiremock receives ZERO POSTs (expect(0)).
    //
    // Mutation target: implementer wires --dry-run only into the
    // dispatcher path but not the notifier path. Test catches.
    //
    // Cross-references discord_wait_param.rs::dry_run_sets_wait_to_false_or_skips_request_entirely.
}

#[test]
#[ignore = "requires gcit trigger --dry-run feature (not yet implemented)"]
fn dry_run_prints_request_url_with_redacted_token() {
    // dry-run output includes the URL, but the token portion (the path
    // segment after /api/webhooks/<id>/) is redacted.
    //
    // Captured stdout contains:
    //   POST https://discord.com/api/webhooks/12345/[REDACTED]?wait=true
    //
    // SPEC GAP: spec says "redact credential values" but
    // doesn't pin the exact rendering for partial-secret URLs. The
    // webhook id is NOT a secret (it's a stable, public-on-discord
    // identifier of the webhook), but the token IS (anyone with the
    // token can post to the webhook). Recommend: print the URL with the
    // token segment replaced by [REDACTED]. flag — test pins the
    // chosen rendering.
    //
    // Related: SecretString redaction in tracing/Display/Debug paths.
}

#[test]
#[ignore = "requires gcit trigger --dry-run feature (not yet implemented)"]
fn dry_run_prints_full_embed_json_body() {
    // dry-run output includes the JSON body that would have been POSTed.
    // Pretty-printed for human reading.
    //
    // Captured stdout contains the embed shape:
    //   {
    //     "embeds": [
    //       {
    //         "title": "linux-mainline-ci: Success",
    //         "description": "...",
    //         ...
    //       }
    //     ]
    //   }
    //
    // SPEC GAP: CLI table row for `gcit trigger` says dry-run
    // prints "all outbound payloads" but doesn't pin format
    // (pretty vs compact JSON,
    // headers in addition to body, etc.). Recommend:
    //   - pretty-printed JSON body
    //   - one section per outbound: METHOD URL\nheaders\nbody
    //   - separator lines between destinations
    // flag.
}

#[test]
#[ignore = "requires gcit trigger --dry-run feature (not yet implemented)"]
fn dry_run_prints_request_headers() {
    // Headers gcit would send:
    //   user-agent: gcit/<version>
    //   content-type: application/json
    //
    // The Authorization header (if any) is redacted. (Webhook URLs
    // don't carry an auth header — the token is in the path — but
    // dry-run output for the GitHub dispatcher path WILL have one.)
    //
    // Mutation target: dry-run prints the header verbatim. Test asserts
    // any header value containing the resolved credential is shown as
    // [REDACTED].
    //
    // Cross-references discord_dispatch.rs (the analogous dry-run test
    // for GitHub dispatcher already lives in github_dispatch.rs).
}

#[test]
#[ignore = "requires gcit trigger --dry-run feature (not yet implemented)"]
fn dry_run_prints_credential_id_alongside_redacted_value() {
    // "the credential_id itself is shown so users can identify which
    // entry is in use."
    //
    // dry-run output includes:
    //   credential: discord_ci_webhook (resolved from $CREDENTIALS_DIRECTORY)
    //   value: [REDACTED]
    //
    // Pin via assert_contains.
    //
    // Mutation target: implementer redacts both the id AND the value.
    // Operator can't tell which credential is in use. Test catches.
}

#[rstest]
#[case::ascii_color("\x1b[31m")] // CSI red
#[case::ascii_reset("\x1b[0m")]
#[ignore = "requires gcit trigger --dry-run feature (not yet implemented)"]
fn dry_run_output_terminal_styling(#[case] csi: &str) {
    let _ = csi;
    // SPEC GAP: dry-run output may use terminal colors (red for
    // [REDACTED] highlighting, gray for headers, etc.). Spec
    // doesn't pin this. Recommend:
    //   - colors when stdout is a TTY (isatty)
    //   - plain text when stdout is redirected (CI logs, file capture)
    //   - --no-color flag honored
    //   - NO_COLOR env var honored (https://no-color.org/)
    // flag.
    //
    // Test asserts that with stdout-not-a-tty, no CSI escape sequences
    // appear in the captured output.
    //
    // Cross-references tests/cli_check_3state.rs which tests gcit check
    // output rendering — same tty-detection logic should apply.
}

#[tokio::test]
#[ignore = "requires gcit trigger --dry-run feature (not yet implemented)"]
async fn dry_run_with_template_compile_error_surfaces_at_dry_run() {
    // gcit trigger --dry-run compiles all templates. If a template has
    // a syntax error, dry-run reports it WITHOUT issuing any wiremock
    // POST. This is the "test before going live" feature.
    //
    // Build a config with a malformed template. Run dry-run. Assert
    // exit 65 (EX_DATAERR) — `gcit validate-template <FILE>` exit
    // code (recommend reusing for dry-run since both check template
    // validity).
    //
    // SPEC GAP: dry-run exit codes are 0/EX_USAGE=64/EX_TEMPFAIL=75 —
    // no EX_DATAERR. But a template-compile error in
    // dry-run mode IS data-error category. Recommend amending exit
    // codes for trigger to include EX_DATAERR=65 for template errors.
    // flag.
}

#[tokio::test]
#[ignore = "requires gcit trigger --dry-run feature (not yet implemented)"]
async fn dry_run_for_each_destination_in_flow() {
    // A flow with two destinations (one discord, one local_mail) prints
    // BOTH outbound payloads in dry-run. Order matches config order.
    //
    // Captured stdout contains:
    //   destination[0]: discord_webhook
    //     POST <url> ...
    //   destination[1]: local_mail
    //     append to /var/mail/ops:
    //     <rendered mbox bytes>
    //
    // Mutation target: dry-run iterates only the first destination.
    // Test catches via assert_contains for both payload sections.
    //
    // Cross-references tests/mail_dry_run.rs (mbox-side).
}

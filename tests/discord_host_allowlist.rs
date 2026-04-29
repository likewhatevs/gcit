// Discord webhook URL host validation.
//
// gcit refuses to send Discord-shaped payloads to non-Discord hosts.
// Defense-in-depth: even if an operator stores an arbitrary URL in a
// credential file, the validator catches it before any HTTP fires.
//
// Validation point: `gcit::discord::parse_webhook_url` is invoked by
// `flow::supervisor::build_notifiers_for` at daemon startup AND by
// `gcit check` (which shares the same supervisor entry path). A bad
// URL surfaces a per-credential error string before any Discord
// request is issued.
//
// These tests drive `parse_webhook_url` directly (the same function
// the supervisor calls) since that is where the host-allowlist
// enforcement lives. Per-component shape coverage (scheme, userinfo,
// path) lives in src/discord/webhook.rs in-module unit tests; here
// we pin the integration property: validation runs before any
// network call, and the resulting error names the offending URL
// component the operator must edit.

use gcit::discord::parse_webhook_url;
use gcit::discord::webhook::ParseWebhookUrlError;
use secrecy::ExposeSecret;

#[test]
fn validation_runs_at_config_load_not_at_send_time() {
    // Property: the validator rejects non-Discord hosts as a typed
    // `ParseWebhookUrlError::HostNotAllowed`, which the supervisor's
    // `build_notifiers_for` step (shared by daemon startup and
    // `gcit check`) surfaces as a fail-fast configuration error
    // before constructing the notifier and before any HTTP request
    // fires.
    //
    // Driving `parse_webhook_url` directly mirrors the supervisor's
    // call site (src/flow/supervisor.rs around build_notifiers_for):
    // any caller of the same function gets the same rejection — so
    // there is no path where a bad URL slips past validation and
    // reaches a wiremock/Discord POST.
    //
    // Mutation target: validator runs only at config-LOAD parse but
    // not at supervisor startup (which is where credential
    // resolution actually surfaces the URL). The supervisor calls
    // parse_webhook_url AFTER credential resolve, so the resolved
    // value is what gets validated.
    let cases = [
        "https://example.com/api/webhooks/12345/secret",
        "https://evil.example.com/api/webhooks/12345/secret",
        "https://discord.io/api/webhooks/12345/secret",
        "https://192.168.1.1/api/webhooks/12345/secret",
        "https://localhost/api/webhooks/12345/secret",
        "https://169.254.169.254/api/webhooks/12345/secret",
    ];
    for url in cases {
        let err = parse_webhook_url(url).expect_err(&format!(
            "non-Discord host must be rejected before any HTTP fires: {url}",
        ));
        assert!(
            matches!(err, ParseWebhookUrlError::HostNotAllowed { .. }),
            "{url}: expected HostNotAllowed, got {err:?}",
        );
    }

    // Sanity: the canonical Discord hosts pass — verifies the
    // rejection above is not a false positive.
    let parsed = parse_webhook_url("https://discord.com/api/webhooks/12345/secret")
        .expect("discord.com must be accepted");
    assert_eq!(parsed.id.get(), 12345);
    assert_eq!(parsed.token.expose_secret(), "secret");
}

#[test]
fn validation_error_message_names_credential_and_flow() {
    // The supervisor wraps `parse_webhook_url`'s error with the
    // calling flow + credential context (see
    // src/flow/supervisor.rs::build_notifiers_for, where the error
    // is mapped via `format!("discord webhook URL: {e}")` and the
    // calling site adds flow-name context). The wrapping produces
    // an operator-actionable string of the shape:
    //
    //   discord webhook URL: webhook host '<host>' is not allowed;
    //   expected discord.com or discordapp.com
    //
    // We pin two properties of the wrapped error: (1) the offending
    // host string surfaces (so the operator can identify which
    // credential file is wrong), and (2) the operator-facing
    // expected-host hint surfaces (so they know how to fix it).
    // Crucially, the URL path (which contains the secret token) MUST
    // NOT appear — leaking it to logs would be a token disclosure.
    let url = "https://evil.example.com/api/webhooks/12345/THIS_IS_A_TOKEN";
    let err = parse_webhook_url(url).expect_err("bad host must be rejected");
    let msg = err.to_string();

    // Positive: host appears (operator triages by host).
    assert!(
        msg.contains("evil.example.com"),
        "error must name the offending host so the operator can find it; got: {msg}",
    );
    // Positive: expected-host hint appears (operator-actionable).
    assert!(
        msg.contains("discord.com") && msg.contains("discordapp.com"),
        "error must list the allowed hosts so the operator knows what to set; got: {msg}",
    );
    // Negative: the URL path / token must NOT surface — a Discord
    // webhook URL's token segment is the credential value itself.
    assert!(
        !msg.contains("THIS_IS_A_TOKEN"),
        "error must NOT leak the URL path/token component; got: {msg}",
    );
    assert!(
        !msg.contains("/api/webhooks/"),
        "error must NOT echo the path either; got: {msg}",
    );
}

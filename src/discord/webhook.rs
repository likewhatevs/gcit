// twilight-http Client wrapper + Discord webhook URL parser.
//
// Construction:
//   - One twilight-http Client per daemon (shared via Arc). Reuses
//     the underlying connection pool across flows.
//   - .ratelimiter(None) — gcit handles 429/Retry-After explicitly
//     in the notifier (see notifier.rs); the in-process global
//     ratelimiter from twilight introduces non-deterministic delays
//     and reorders requests, which breaks per-credential rate
//     buckets.
//   - .timeout(http.request_timeout) — operator-configurable per
//     `[http]` section.
//   - Tests use .proxy(base_uri, true) so wiremock receives the
//     traffic; production paths leave the default.
//
// Webhook URL parsing:
//   - Format: https://discord.com/api/webhooks/{id}/{token}
//     (also discordapp.com, with optional trailing slash).
//   - Extract `(id: u64, token: String)` for execute_webhook.
//   - Reject malformed inputs at parse time so the notifier surfaces
//     a clear NotifyError::Permanent at startup, not on first dispatch.

use std::sync::Arc;
use std::time::Duration;

use secrecy::{ExposeSecret, SecretString};
use twilight_http::Client as TwilightClient;
use twilight_model::id::marker::WebhookMarker;
use twilight_model::id::Id;
use url::Url;

/// Per-daemon Discord HTTP client. Cheap to clone via `Arc`.
///
/// Response body sizing: twilight-http's builder does not expose a
/// response-body cap and gcit does not wrap responses with an
/// external limiter on this path. The load-bearing security boundary
/// for response sizing is the Discord host allowlist (`discord.com`,
/// `discordapp.com`, `ptb.discord.com`, `canary.discord.com`)
/// enforced at webhook-URL parse time by `is_discord_host` /
/// `parse_webhook_url`; combined with `execute_webhook`'s 204 No
/// Content response shape (the notifier does not chain `.wait()` —
/// the variant that returns the posted `Message` body), this leaves
/// no realistic vector for an attacker-sized response body.
#[derive(Debug, Clone)]
pub struct Client {
    inner: Arc<TwilightClient>,
}

/// Errors raised when constructing a `Client`. Distinct from
/// `NotifyError`; these fire at daemon startup.
#[derive(Debug, thiserror::Error)]
pub enum ClientBuildError {
    /// twilight-http's builder rejected the configuration. Rare —
    /// only happens if the daemon-wide proxy/timeout combination is
    /// somehow invalid.
    #[error("twilight-http build failed: {message}")]
    Build { message: String },
}

impl Client {
    /// Construct the production client. `request_timeout` comes from
    /// `[http] request_timeout` in config.
    pub fn new(request_timeout: Duration) -> Result<Self, ClientBuildError> {
        let inner = TwilightClient::builder()
            .ratelimiter(None)
            .timeout(request_timeout)
            .build();
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Construct a client pointed at a test base URI (e.g. wiremock).
    /// `use_http=true` per twilight-http's proxy convention. Used by
    /// tests under tests/discord_*.rs.
    ///
    /// Accepts either a full URI (`http://127.0.0.1:NNNN`) or a bare
    /// `host:port`. twilight-http's `.proxy()` expects only the
    /// `host:port` form — passing a full URI produces a malformed
    /// proxied URL — so this helper strips the `http://` or
    /// `https://` prefix before handing off. Test fixtures typically
    /// pass `wiremock::MockServer::uri()` which returns the full
    /// scheme-prefixed form; this lets callers feed that result in
    /// directly without a per-test strip helper.
    #[doc(hidden)]
    pub fn for_test(
        base_uri: impl Into<String>,
        request_timeout: Duration,
    ) -> Result<Self, ClientBuildError> {
        let raw = base_uri.into();
        let host_port = raw
            .strip_prefix("http://")
            .or_else(|| raw.strip_prefix("https://"))
            .unwrap_or(&raw)
            .to_string();
        let inner = TwilightClient::builder()
            .ratelimiter(None)
            .proxy(host_port, /* use_http */ true)
            .timeout(request_timeout)
            .build();
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Underlying twilight-http handle. The notifier passes this to
    /// `execute_webhook(id, token)`.
    pub fn inner(&self) -> &TwilightClient {
        &self.inner
    }
}

/// Parsed Discord webhook URL: id + token. Tokens never log; the
/// notifier extracts the token at dispatch time and passes it
/// directly to `execute_webhook` without round-tripping through
/// our log layer.
#[derive(Debug, Clone)]
pub struct ParsedWebhookUrl {
    pub id: Id<WebhookMarker>,
    pub token: SecretString,
}

/// Errors returned from `parse_webhook_url`. Each maps to a
/// distinct operator-actionable error message.
#[derive(Debug, thiserror::Error)]
pub enum ParseWebhookUrlError {
    /// The URL is not parseable as a URL at all.
    #[error("malformed webhook URL: {source}")]
    Malformed {
        #[source]
        source: url::ParseError,
    },

    /// The URL scheme is not `https`. Discord webhooks are HTTPS-only;
    /// gcit refuses to send tokens over plaintext or non-HTTPS schemes
    /// to prevent passive token disclosure.
    #[error("webhook scheme '{scheme}' is not allowed; expected https")]
    SchemeNotHttps { scheme: String },

    /// The URL carries a `userinfo` component (`user[:pass]@`).
    /// Discord webhooks do not use HTTP basic auth — a userinfo
    /// component is either an operator typo or a smuggling attempt
    /// (`https://attacker.com@discord.com/...` will dispatch to
    /// `discord.com` only after url::Url normalisation; some HTTP
    /// clients pre-resolve to `attacker.com` instead). gcit rejects
    /// outright.
    #[error("webhook URL must not contain userinfo (user[:pass]@host)")]
    UserinfoNotAllowed,

    /// The URL host is not in the Discord allowlist
    /// (`discord.com`, `discordapp.com`, `ptb.discord.com`,
    /// `canary.discord.com`). Refuse to send webhook payloads to
    /// arbitrary hosts.
    #[error("webhook host '{host}' is not allowed; expected discord.com or discordapp.com")]
    HostNotAllowed { host: String },

    /// The path doesn't have the `/api/webhooks/{id}/{token}` shape.
    #[error("webhook URL path malformed; expected /api/webhooks/<id>/<token>")]
    PathMalformed,

    /// The id segment failed to parse as u64.
    #[error("webhook id '{id}' is not a valid number: {source}")]
    InvalidId {
        id: String,
        #[source]
        source: std::num::ParseIntError,
    },

    /// The id segment parsed to zero. `twilight_model::id::Id` rejects
    /// zero (Discord snowflake ids are non-zero by construction); this
    /// captures the rejection without panicking.
    #[error("webhook id is zero; expected a non-zero Discord snowflake")]
    IdZero,

    /// The token segment is empty.
    #[error("webhook token is empty")]
    EmptyToken,
}

/// Parse `https://discord.com/api/webhooks/{id}/{token}`. Returns
/// the typed id + token; the token is wrapped in `SecretString` so
/// it doesn't appear in `Debug`/`Display` output.
pub fn parse_webhook_url(input: &str) -> Result<ParsedWebhookUrl, ParseWebhookUrlError> {
    let url = Url::parse(input).map_err(|source| ParseWebhookUrlError::Malformed { source })?;

    // Scheme allowlist. Discord webhooks are HTTPS-only; refuse
    // anything else so a typo (`http://`) or smuggled scheme
    // (`javascript:`, `file:`) doesn't leak the token over a
    // non-encrypted channel or to the wrong target.
    if url.scheme() != "https" {
        return Err(ParseWebhookUrlError::SchemeNotHttps {
            scheme: url.scheme().to_string(),
        });
    }

    // Reject userinfo. `url::Url::username()` returns "" when absent;
    // a non-empty value (or a present-but-empty password component)
    // means the URL carries `user[:pass]@host`, which is not part of
    // the Discord webhook URL format and is the canonical shape of
    // a host-spoofing attack.
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ParseWebhookUrlError::UserinfoNotAllowed);
    }

    // Host allowlist. Matches the canonical Discord hosts; reject
    // anything else so a typo'd or attacker-controlled URL can't
    // exfiltrate webhook payloads to a third party.
    let host = url
        .host_str()
        .ok_or_else(|| ParseWebhookUrlError::HostNotAllowed {
            host: String::new(),
        })?;
    if !is_discord_host(host) {
        return Err(ParseWebhookUrlError::HostNotAllowed {
            host: host.to_string(),
        });
    }

    // Path: /api/webhooks/{id}/{token}. Only the canonical plural
    // `webhooks` form is accepted — the singular `webhook` variant
    // is not part of Discord's documented surface and would be a
    // copy-paste error.
    let segments: Vec<&str> = url
        .path_segments()
        .ok_or(ParseWebhookUrlError::PathMalformed)?
        .filter(|s| !s.is_empty())
        .collect();
    if segments.len() != 4 || segments[0] != "api" || segments[1] != "webhooks" {
        return Err(ParseWebhookUrlError::PathMalformed);
    }
    let id_str = segments[2];
    let token_str = segments[3];
    let id_num: u64 = id_str
        .parse()
        .map_err(|source| ParseWebhookUrlError::InvalidId {
            id: id_str.to_string(),
            source,
        })?;
    if token_str.is_empty() {
        return Err(ParseWebhookUrlError::EmptyToken);
    }
    // `Id::new(0)` panics — use the checked variant so a webhook URL
    // carrying `/api/webhooks/0/<token>` surfaces a typed error.
    let id = Id::new_checked(id_num).ok_or(ParseWebhookUrlError::IdZero)?;
    Ok(ParsedWebhookUrl {
        id,
        token: SecretString::from(token_str.to_string()),
    })
}

/// Whether `host` is in the Discord allowlist. Case-insensitive
/// match against the canonical Discord domains. Reject anything else
/// so a typo or attacker-controlled redirect can't exfiltrate webhook
/// payloads.
pub fn is_discord_host(host: &str) -> bool {
    let lower = host.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "discord.com" | "discordapp.com" | "ptb.discord.com" | "canary.discord.com"
    )
}

/// Expose the secret token for the dispatch path. Forces the call
/// site to opt in to viewing the secret bytes (`token.expose()`),
/// matching the secrecy crate's intent.
pub fn expose_token(token: &SecretString) -> &str {
    token.expose_secret()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::util::ensure_crypto_provider;

    #[test]
    fn parse_webhook_url_accepts_discord_com() {
        let url = "https://discord.com/api/webhooks/1234567890/abcDEFxyz";
        let parsed = parse_webhook_url(url).expect("parse");
        assert_eq!(parsed.id.get(), 1234567890);
        assert_eq!(expose_token(&parsed.token), "abcDEFxyz");
    }

    #[test]
    fn parse_webhook_url_accepts_discordapp_com() {
        let url = "https://discordapp.com/api/webhooks/42/tokvalue";
        let parsed = parse_webhook_url(url).expect("parse");
        assert_eq!(parsed.id.get(), 42);
        assert_eq!(expose_token(&parsed.token), "tokvalue");
    }

    #[test]
    fn parse_webhook_url_rejects_non_discord_host() {
        let url = "https://evil.example.com/api/webhooks/1/abc";
        let err = parse_webhook_url(url).expect_err("must reject");
        assert!(matches!(err, ParseWebhookUrlError::HostNotAllowed { .. }));
    }

    #[test]
    fn parse_webhook_url_rejects_malformed_input() {
        let err = parse_webhook_url("not a url").expect_err("must reject");
        assert!(matches!(err, ParseWebhookUrlError::Malformed { .. }));
    }

    #[test]
    fn parse_webhook_url_rejects_short_path() {
        let url = "https://discord.com/api/webhooks/1234";
        let err = parse_webhook_url(url).expect_err("must reject");
        assert!(matches!(err, ParseWebhookUrlError::PathMalformed));
    }

    #[test]
    fn parse_webhook_url_rejects_non_numeric_id() {
        let url = "https://discord.com/api/webhooks/notanumber/token";
        let err = parse_webhook_url(url).expect_err("must reject");
        assert!(matches!(err, ParseWebhookUrlError::InvalidId { .. }));
    }

    #[test]
    fn parse_webhook_url_rejects_wrong_first_segments() {
        let url = "https://discord.com/something/else/1/abc";
        let err = parse_webhook_url(url).expect_err("must reject");
        assert!(matches!(err, ParseWebhookUrlError::PathMalformed));
    }

    #[test]
    fn parse_webhook_url_rejects_http_scheme() {
        let url = "http://discord.com/api/webhooks/1/abc";
        let err = parse_webhook_url(url).expect_err("must reject");
        match err {
            ParseWebhookUrlError::SchemeNotHttps { scheme } => {
                assert_eq!(scheme, "http");
            }
            other => panic!("expected SchemeNotHttps, got {other:?}"),
        }
    }

    #[test]
    fn parse_webhook_url_rejects_userinfo_user_at_host() {
        // Classic host-spoof shape — `username@discord.com`. Regardless
        // of where url::Url ultimately routes, gcit refuses outright.
        let url = "https://attacker@discord.com/api/webhooks/1/abc";
        let err = parse_webhook_url(url).expect_err("must reject");
        assert!(
            matches!(err, ParseWebhookUrlError::UserinfoNotAllowed),
            "expected UserinfoNotAllowed, got {err:?}",
        );
    }

    #[test]
    fn parse_webhook_url_rejects_userinfo_with_password() {
        let url = "https://u:p@discord.com/api/webhooks/1/abc";
        let err = parse_webhook_url(url).expect_err("must reject");
        assert!(
            matches!(err, ParseWebhookUrlError::UserinfoNotAllowed),
            "expected UserinfoNotAllowed, got {err:?}",
        );
    }

    #[test]
    fn parse_webhook_url_rejects_singular_webhook_path() {
        // `/api/webhook/...` (singular) is not a Discord URL shape —
        // operator copy-paste error or attacker probing.
        let url = "https://discord.com/api/webhook/1234567890/tok";
        let err = parse_webhook_url(url).expect_err("must reject");
        assert!(
            matches!(err, ParseWebhookUrlError::PathMalformed),
            "expected PathMalformed, got {err:?}",
        );
    }

    #[test]
    fn parse_webhook_url_rejects_id_zero() {
        // `Id::new(0)` panics; we use `Id::new_checked` and surface
        // a typed error instead.
        let url = "https://discord.com/api/webhooks/0/sometoken";
        let err = parse_webhook_url(url).expect_err("must reject");
        assert!(
            matches!(err, ParseWebhookUrlError::IdZero),
            "expected IdZero, got {err:?}",
        );
    }

    #[test]
    fn is_discord_host_pins_allowlist() {
        assert!(is_discord_host("discord.com"));
        assert!(is_discord_host("discordapp.com"));
        assert!(is_discord_host("ptb.discord.com"));
        assert!(is_discord_host("canary.discord.com"));
        assert!(is_discord_host("DISCORD.COM"), "case-insensitive");

        assert!(!is_discord_host("discord.example.com"));
        assert!(!is_discord_host("evil.com"));
        assert!(!is_discord_host("api.discord.com"));
        assert!(!is_discord_host(""));
    }

    #[test]
    fn parse_webhook_url_token_secret_does_not_leak_in_debug() {
        let url = "https://discord.com/api/webhooks/1/secrettoken";
        let parsed = parse_webhook_url(url).expect("parse");
        let debug = format!("{:?}", parsed.token);
        assert!(
            !debug.contains("secrettoken"),
            "SecretString must hide bytes; got {debug:?}",
        );
    }

    #[tokio::test]
    async fn client_builds_with_default_timeout() {
        ensure_crypto_provider();
        let c = Client::new(Duration::from_secs(30)).expect("build");
        let _ = c.inner();
    }

    #[tokio::test]
    async fn client_for_test_accepts_proxy() {
        ensure_crypto_provider();
        let c =
            Client::for_test("http://127.0.0.1:8080", Duration::from_secs(5)).expect("for_test");
        let _ = c.inner();
    }

    #[tokio::test]
    async fn client_clone_shares_inner_arc() {
        ensure_crypto_provider();
        let c = Client::new(Duration::from_secs(10)).expect("build");
        let c2 = c.clone();
        assert!(Arc::ptr_eq(&c.inner, &c2.inner));
    }
}

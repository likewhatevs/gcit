// `Client` — thin wrapper around an `octocrab::Octocrab` instance
// configured for one credential. The supervisor owns the
// `BTreeMap<CredentialId, Arc<Client>>` keyed by credential id.
//
// Why a wrapper rather than re-using `octocrab::Octocrab` directly:
//
//   - We want to attach the credential id to the client so the
//     dispatcher / monitor / correlator paths can produce
//     `GithubErrorKind` variants that name the credential without
//     threading an extra argument everywhere.
//   - We want a uniform `request_timeout` cap on every API call so a
//     stuck server doesn't make a flow's polling task indefinitely
//     unresponsive. octocrab itself does not surface a per-request
//     timeout knob; we wrap each call site with `tokio::time::timeout`.
//   - We want a single point to enforce the `github_pat_` prefix
//     contract: only fine-grained personal access tokens are
//     supported. Config validation already gates credential strings,
//     but the dispatcher checks the resolved `SecretString` here as
//     defense-in-depth: even if a classic PAT slips through to the
//     dispatcher, the dispatcher refuses to use it and surfaces a
//     clear error.
//
// Construction:
//
//   ```rust,ignore
//   let client = Client::builder()
//       .credential(CredentialId::new("github_pat")?)
//       .token(secret) // SecretString from config resolution
//       .request_timeout(Duration::from_secs(30))
//       .build()?;
//   ```
//
// Tests inject `base_uri` to point at wiremock; production paths use
// the default `https://api.github.com`.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::Uri;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Limited};
use octocrab::Octocrab;
use secrecy::{ExposeSecret, SecretString};
use tokio::time::timeout;

use super::error::{classify, timeout_error, GithubErrorKind};
use super::rate_limit::RateLimitState;
use crate::config::CredentialId;

/// Default per-request timeout when the operator hasn't set
/// `http.request_timeout` in config. 30s.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Cap on the GitHub API response body size, in bytes. Mirrors the
/// 16 MiB cap `git::grokmirror::MAX_DECOMPRESSED_BYTES` enforces on
/// the kernel.org manifest stream — any host upstream of gcit's
/// HTTP path could otherwise serve a multi-GB body and exhaust
/// daemon memory before the operator sees it. The bound is
/// generous: a real workflow run with thousands of jobs renders
/// well under 1 MiB; 16 MiB is roughly two orders of magnitude
/// of headroom.
///
/// Enforcement: applied via `http_body_util::Limited` to every
/// response returned from `Octocrab::_get` (which the correlator
/// and monitor paths reach through `classified_get`). The limit
/// triggers `LengthLimitError` mid-stream as soon as the cap is
/// crossed; the helper maps that to `GithubErrorKind::BodyTooLarge`.
///
/// Coverage gaps (documented for the operator):
///   - The dispatcher's `_post` returns 204 No Content, so the
///     body is empty and the cap is not relevant.
///   - The rate-limit poller uses octocrab's high-level typed
///     `ratelimit().get()` which bypasses `_get`. Body is ~500
///     bytes JSON, and host is the GitHub API allowlist, so the
///     DoS surface is bounded by host trust.
///   - twilight-http (Discord) similarly bypasses any gcit-side
///     wrap; webhook responses are 204 by design and the host is
///     allowlist-restricted to discord.com / discordapp.com.
pub const RESPONSE_BODY_LIMIT: usize = 16 * 1024 * 1024;

/// Errors produced when constructing a `Client`. Distinct from
/// `GithubErrorKind` (which is for runtime API errors) — these fire
/// at startup when the configured credential is wrong.
#[derive(Debug, thiserror::Error)]
pub enum ClientBuildError {
    /// The builder was called without `.credential(...)`. Only
    /// surfaces from in-tree call sites that forgot to set the
    /// field; production code paths thread CredentialId through
    /// from config.
    #[error("github client builder missing required field: credential. Call .credential(CredentialId::new(...)).")]
    MissingCredential,

    /// The builder was called without `.token(...)`. Distinct from
    /// EmptyToken (which means the token *value* was zero-length).
    #[error("github client builder missing required field: token for credential '{credential}'. Call .token(SecretString::from(...)).")]
    MissingToken { credential: CredentialId },

    /// The token doesn't begin with `github_pat_`. Only fine-grained
    /// personal access tokens are supported; classic PATs (`ghp_`)
    /// and OAuth tokens (`gho_`) are rejected.
    #[error("github credential '{credential}' must start with 'github_pat_' (fine-grained PAT). Only fine-grained personal access tokens are supported; classic PATs and OAuth tokens are not accepted. Generate one at https://github.com/settings/personal-access-tokens")]
    NotFineGrained { credential: CredentialId },

    /// The token is empty. Distinct from missing credential
    /// (which surfaces as ConfigError::CredentialNotFound at config
    /// load) — empty here means the resolved value was a zero-byte
    /// string, e.g. an empty file under $CREDENTIALS_DIRECTORY.
    #[error("github credential '{credential}' is empty")]
    EmptyToken { credential: CredentialId },

    /// `base_uri` did not parse. Only happens in tests that build
    /// against wiremock; production paths leave the default unset.
    #[error("invalid base_uri: {source}")]
    InvalidBaseUri {
        #[source]
        source: octocrab::Error,
    },

    /// octocrab's builder rejected the configuration. Bubble up the
    /// underlying error.
    #[error("octocrab build failed: {source}")]
    OctocrabBuild {
        #[source]
        source: octocrab::Error,
    },
}

/// One GitHub client per credential. Cheap to clone via `Arc` —
/// `Client` wraps a single `Arc<Inner>`, so cloning is one
/// `Arc::clone` (one refcount increment).
///
/// Response body sizing: octocrab itself does not expose a body-cap
/// builder knob, so gcit enforces `RESPONSE_BODY_LIMIT` (16 MiB) on
/// the GET paths that route through `classified_get` — the monitor's
/// `get_run` / `list_jobs` and the correlator's `list_workflow_runs`.
/// Paths that bypass `_get` (the dispatcher's `_post`, the rate-limit
/// poller's `ratelimit().get()`, and `git::github_api::poll`'s
/// `repos().get_ref()`) are not body-capped by gcit; their
/// load-bearing security boundary for response sizing is the
/// `api.github.com` host allowlist (octocrab's default base URI;
/// tests override via `ClientBuilder::base_uri`). See
/// `RESPONSE_BODY_LIMIT` for the cap value and an enumeration of the
/// uncapped paths.
#[derive(Debug, Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    octo: Octocrab,
    credential: CredentialId,
    request_timeout: Duration,
}

impl Client {
    /// Start building a new client. The builder pattern matches the
    /// rest of gcit's per-credential constructors and keeps the
    /// argument list ordered (`credential` and `token` always come
    /// first, knobs follow).
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// The underlying octocrab handle. Used by the dispatcher /
    /// monitor / correlator / rate-limit poller paths.
    pub fn octocrab(&self) -> &Octocrab {
        &self.inner.octo
    }

    /// The credential id this client was built for. Used by error
    /// classification to fill in the `credential` field on
    /// `GithubErrorKind::Unauthorized` / `Forbidden` / `RateLimited`.
    pub fn credential(&self) -> &CredentialId {
        &self.inner.credential
    }

    /// Per-request timeout. Callers wrap individual API calls with
    /// `tokio::time::timeout(client.request_timeout(), ...)`.
    pub fn request_timeout(&self) -> Duration {
        self.inner.request_timeout
    }
}

/// Inner error type carrying either an octocrab failure or a
/// body-cap rejection. The body-cap rejections must surface as the
/// typed `GithubErrorKind::BodyTooLarge` (Permanent) — wrapping in
/// `octocrab::Error::Other` (Transient -> Unknown) would cause the
/// correlator's backon loop to retry forever against a misbehaving
/// server.
enum ClassifiedGetInner {
    /// Pre-check fired: server declared Content-Length over the
    /// cap. Carries the declared value for the typed error.
    BodyDeclaredTooLarge(u64),
    /// Streaming `Limited` wrapper fired: actual body bytes
    /// exceeded the cap regardless of Content-Length.
    BodyStreamedTooLarge,
    /// Any other octocrab failure. Routed through the standard
    /// `classify` path for status-code-based classification.
    Octocrab(octocrab::Error),
}

impl From<octocrab::Error> for ClassifiedGetInner {
    fn from(e: octocrab::Error) -> Self {
        // After `limit_response_body`, octocrab errors that carry
        // a `LengthLimitError` somewhere in their `source()` chain
        // are body-cap rejections — `Limited` wrapped its
        // `LengthLimitError` in `octocrab::Error::Other` so the
        // pipeline could carry it. Recognise it here so the
        // outer match emits `BodyTooLarge { declared: None }`
        // instead of treating it as a generic Unknown (which
        // would be Transient and infinite-retry).
        if error_chain_contains::<http_body_util::LengthLimitError>(&e) {
            ClassifiedGetInner::BodyStreamedTooLarge
        } else {
            ClassifiedGetInner::Octocrab(e)
        }
    }
}

/// Issue a raw GET via octocrab's low-level `_get` so the response
/// headers stay accessible. Refresh `state` opportunistically with
/// `X-RateLimit-*` from the response, then route through
/// `map_github_error` + `FromResponse::from_response` to recover the
/// typed body. Per-response observation keeps the rate-limit snapshot
/// fresher than the 60s poller cadence.
///
/// Use this from monitor/correlator GET paths where the high-level
/// builder would discard headers. POST paths (dispatch) call
/// observe_headers directly because the dispatch flow needs custom
/// 403 reclassification anyway.
pub async fn classified_get<T>(
    client: &Client,
    rate_limit: &RateLimitState,
    repo: &str,
    workflow: &str,
    uri: Uri,
) -> Result<T, GithubErrorKind>
where
    T: octocrab::FromResponse,
{
    let octo = client.octocrab();
    let send = async move {
        let response = octo._get(uri).await?;
        let headers = response.headers().clone();
        // Pre-flight `Content-Length` check.
        if let Some(declared) = parse_content_length(&headers) {
            if declared > RESPONSE_BODY_LIMIT as u64 {
                return Err(ClassifiedGetInner::BodyDeclaredTooLarge(declared));
            }
        }
        // Wrap the streaming body in `Limited`. The wrapper
        // surfaces `LengthLimitError` when the actual stream
        // exceeds the cap, regardless of Content-Length (catches
        // missing Content-Length, gzip-expansion, and
        // lying-server cases). The error is wrapped in
        // `octocrab::Error::Other` by `limit_response_body` so it
        // flows through the standard pipeline; the `From` impl
        // above translates it back to the typed
        // `BodyStreamedTooLarge` variant via
        // `error_chain_contains`.
        let response = limit_response_body(response);
        let response = octocrab::map_github_error(response).await?;
        let value = T::from_response(response).await?;
        Ok::<_, ClassifiedGetInner>((headers, value))
    };
    match timeout(client.request_timeout(), send).await {
        Ok(Ok((headers, value))) => {
            rate_limit.observe_headers(&headers).await;
            Ok(value)
        }
        Ok(Err(ClassifiedGetInner::BodyDeclaredTooLarge(declared))) => {
            Err(GithubErrorKind::BodyTooLarge {
                declared: Some(declared),
                limit: RESPONSE_BODY_LIMIT,
            })
        }
        Ok(Err(ClassifiedGetInner::BodyStreamedTooLarge)) => Err(GithubErrorKind::BodyTooLarge {
            declared: None,
            limit: RESPONSE_BODY_LIMIT,
        }),
        Ok(Err(ClassifiedGetInner::Octocrab(e))) => {
            Err(classify(e, client.credential(), repo, workflow, None, None))
        }
        Err(_elapsed) => Err(timeout_error(client.request_timeout())),
    }
}

/// Walk an error's `source()` chain and check whether any layer
/// downcasts to `T`. Used to recognise our own `LengthLimitError`
/// after it's been wrapped in `octocrab::Error::Other` by
/// `limit_response_body`'s `map_err`.
///
/// `Error::is::<T>()` requires the receiver to satisfy
/// `Self: 'static`, which doesn't compose with the `&dyn Error`
/// return shape of `Error::source()`. Use `downcast_ref::<T>()`
/// instead — it has the same `T: 'static` bound but returns
/// `Option<&T>` without needing the receiver to be 'static.
fn error_chain_contains<T: std::error::Error + 'static>(
    err: &(dyn std::error::Error + 'static),
) -> bool {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = cur {
        if e.downcast_ref::<T>().is_some() {
            return true;
        }
        cur = e.source();
    }
    false
}

/// Parse `Content-Length` as a u64. Returns `None` when the header
/// is missing, unparseable, or empty — the caller falls back to the
/// streaming `Limited` wrapper, which catches both the
/// missing-header case and a server lying about the declared length.
fn parse_content_length(headers: &http::HeaderMap) -> Option<u64> {
    headers
        .get(http::header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()
}

/// Wrap the response body with `http_body_util::Limited`. The
/// wrapper triggers `LengthLimitError` when the streamed body
/// exceeds `RESPONSE_BODY_LIMIT`; the wrapper's
/// `Box<dyn Error + Send + Sync>` is wrapped in
/// `octocrab::Error::Other` so the rest of octocrab's pipeline
/// sees a single error type. The `From<octocrab::Error>` impl for
/// `ClassifiedGetInner` calls `error_chain_contains` to recognise
/// the inner `LengthLimitError` and the match in `classified_get`
/// routes the result to the typed `GithubErrorKind::BodyTooLarge`
/// variant.
fn limit_response_body(
    response: http::Response<BoxBody<Bytes, octocrab::Error>>,
) -> http::Response<BoxBody<Bytes, octocrab::Error>> {
    let (parts, body) = response.into_parts();
    let limited = Limited::new(body, RESPONSE_BODY_LIMIT);
    let limited_boxed: BoxBody<Bytes, octocrab::Error> = limited
        .map_err(|e| octocrab::Error::Other {
            source: e,
            backtrace: std::backtrace::Backtrace::capture(),
        })
        .boxed();
    http::Response::from_parts(parts, limited_boxed)
}

#[derive(Default)]
pub struct ClientBuilder {
    credential: Option<CredentialId>,
    token: Option<SecretString>,
    request_timeout: Option<Duration>,
    base_uri: Option<String>,
}

impl ClientBuilder {
    /// The credential id this client serves. Required.
    pub fn credential(mut self, credential: CredentialId) -> Self {
        self.credential = Some(credential);
        self
    }

    /// The PAT to use as `Authorization: Bearer ...`. Required.
    /// Must start with `github_pat_` (fine-grained PAT).
    pub fn token(mut self, token: SecretString) -> Self {
        self.token = Some(token);
        self
    }

    /// Per-request timeout cap. Defaults to `DEFAULT_REQUEST_TIMEOUT`.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Override the API base URI. Tests use this to point at
    /// wiremock; production paths leave it unset (octocrab defaults
    /// to `https://api.github.com`).
    pub fn base_uri(mut self, uri: impl Into<String>) -> Self {
        self.base_uri = Some(uri.into());
        self
    }

    /// Build the client. Errors at construction time on:
    ///   - missing credential (MissingCredential)
    ///   - missing token (MissingToken)
    ///   - empty token (EmptyToken — distinct from missing)
    ///   - non-`github_pat_` token prefix (NotFineGrained)
    ///   - octocrab builder failure (e.g. invalid base_uri)
    pub fn build(self) -> Result<Client, ClientBuildError> {
        let credential = self.credential.ok_or(ClientBuildError::MissingCredential)?;
        let token = self.token.ok_or_else(|| ClientBuildError::MissingToken {
            credential: credential.clone(),
        })?;

        // Defense-in-depth: config load already enforces this, but
        // the dispatcher path inspects again so a Bad Thing in
        // config-load (or test fixture leakage) doesn't slip past.
        let raw = token.expose_secret();
        if raw.is_empty() {
            return Err(ClientBuildError::EmptyToken { credential });
        }
        if !raw.starts_with("github_pat_") {
            return Err(ClientBuildError::NotFineGrained { credential });
        }

        let mut builder = Octocrab::builder().personal_token(token);
        if let Some(uri) = self.base_uri {
            builder = builder
                .base_uri(uri)
                .map_err(|source| ClientBuildError::InvalidBaseUri { source })?;
        }
        let octo = builder
            .build()
            .map_err(|source| ClientBuildError::OctocrabBuild { source })?;

        let inner = Inner {
            octo,
            credential,
            request_timeout: self.request_timeout.unwrap_or(DEFAULT_REQUEST_TIMEOUT),
        };
        Ok(Client {
            inner: Arc::new(inner),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::util::{ensure_crypto_provider, test_cred};

    fn cred() -> CredentialId {
        test_cred("github_pat")
    }

    fn pat() -> SecretString {
        SecretString::from("github_pat_aaaa1111bbbb2222cccc3333ddddeeeeffff".to_string())
    }

    #[tokio::test]
    async fn builder_constructs_with_default_timeout() {
        ensure_crypto_provider();
        let c = Client::builder()
            .credential(cred())
            .token(pat())
            .build()
            .expect("default builder");
        assert_eq!(c.credential().as_str(), "github_pat");
        assert_eq!(c.request_timeout(), DEFAULT_REQUEST_TIMEOUT);
    }

    #[tokio::test]
    async fn builder_overrides_request_timeout() {
        ensure_crypto_provider();
        let c = Client::builder()
            .credential(cred())
            .token(pat())
            .request_timeout(Duration::from_secs(10))
            .build()
            .expect("explicit timeout");
        assert_eq!(c.request_timeout(), Duration::from_secs(10));
    }

    #[test]
    fn builder_rejects_classic_pat_prefix() {
        let classic = SecretString::from("ghp_classicpattoken1234567890".to_string());
        let err = Client::builder()
            .credential(cred())
            .token(classic)
            .build()
            .expect_err("classic PAT must reject");
        assert!(
            matches!(err, ClientBuildError::NotFineGrained { .. }),
            "got {err:?}",
        );
        // Operator-facing message must mention the supported prefix.
        let msg = format!("{err}");
        assert!(
            msg.contains("github_pat_") && msg.contains("fine-grained"),
            "msg: {msg}",
        );
    }

    #[test]
    fn builder_rejects_oauth_token_prefix() {
        let oauth = SecretString::from("gho_oauthtokenexample1234567890".to_string());
        let err = Client::builder()
            .credential(cred())
            .token(oauth)
            .build()
            .expect_err("OAuth must reject");
        assert!(matches!(err, ClientBuildError::NotFineGrained { .. }));
    }

    #[test]
    fn builder_rejects_empty_token() {
        let err = Client::builder()
            .credential(cred())
            .token(SecretString::from(String::new()))
            .build()
            .expect_err("empty must reject");
        assert!(matches!(err, ClientBuildError::EmptyToken { .. }));
    }

    #[test]
    fn builder_rejects_missing_credential() {
        // Distinct error variant — not a sentinel CredentialId.
        let err = Client::builder()
            .token(pat())
            .build()
            .expect_err("missing credential must reject");
        assert!(
            matches!(err, ClientBuildError::MissingCredential),
            "got {err:?}",
        );
        let msg = format!("{err}");
        assert!(
            msg.contains("missing required field: credential"),
            "msg: {msg}"
        );
    }

    #[test]
    fn builder_rejects_missing_token() {
        let err = Client::builder()
            .credential(cred())
            .build()
            .expect_err("missing token must reject");
        match err {
            ClientBuildError::MissingToken { credential } => {
                assert_eq!(credential.as_str(), "github_pat");
            }
            other => panic!("expected MissingToken, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn builder_accepts_custom_base_uri() {
        ensure_crypto_provider();
        // The builder accepts a base_uri override (used by tests
        // pointing at wiremock). Pin the URI shape.
        let c = Client::builder()
            .credential(cred())
            .token(pat())
            .base_uri("http://127.0.0.1:8080")
            .build()
            .expect("base_uri override");
        // No public accessor for the URI; just confirm build
        // succeeded and the credential carried through.
        assert_eq!(c.credential().as_str(), "github_pat");
    }

    #[tokio::test]
    async fn clone_shares_inner_via_arc() {
        ensure_crypto_provider();
        let c = Client::builder()
            .credential(cred())
            .token(pat())
            .build()
            .expect("ok");
        let c2 = c.clone();
        // Same pointer = shared Arc<Inner>.
        assert!(Arc::ptr_eq(&c.inner, &c2.inner));
    }
}

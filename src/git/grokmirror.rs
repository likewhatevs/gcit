// Grokmirror polling: fetch + decompress + parse manifest.js.gz,
// look up the configured repo's fingerprint.
//
// Strategy: Grokmirror (git.kernel.org) uses manifest.js.gz
// fingerprints with a default 60s polling interval. 16 MiB cap on
// the decompressed buffer (gzip-bomb defense).
//
// Wire format: the file lives at https://git.kernel.org/manifest.js.gz
// (and similar layouts on other grokmirror-hosted servers). It is a
// STATIC `.gz` file — kernel.org does NOT serve it with
// `Content-Encoding: gzip`, so reqwest's transparent decompression
// does NOT apply. We must decode the body explicitly via
// flate2::read::GzDecoder.
//
// Manifest shape (per kernel.org):
//   {
//     "/pub/scm/.../linux.git": {
//       "fingerprint": "abcdef...",
//       "modified":   1700000000,
//       "head":       "ref: refs/heads/master",
//       ...
//     },
//     "/pub/scm/.../stable.git": { ... }
//   }
//
// Don't use deny_unknown_fields — kernel.org adds new fields without
// notice; we tolerate them.
//
// Behavior: gcit caches the previous fingerprint in memory (the
// poll task threads it through). On match -> PollOutcome::Unchanged.
// On mismatch (or first observation) -> the strategy returns the
// fingerprint for the caller to record AND a follow-up signal that
// the caller may resolve via ls-remote to get the per-ref SHA.
// Fingerprint -> per-ref SHA resolution is left to the caller; this
// module's job is just to fetch and parse the manifest.

use std::collections::BTreeMap;
use std::time::Duration;

use flate2::read::GzDecoder;
use reqwest::Client;
use serde::Deserialize;

/// Defensive cap on the decompressed manifest body size. kernel.org's
/// manifest is a few MB at writing; 16 MiB is generous enough that we
/// don't reject a real manifest, and tight enough that a hostile
/// gzip-bomb cannot exhaust gcit's address space.
pub const MAX_DECOMPRESSED_BYTES: u64 = 16 * 1024 * 1024;

/// Defensive cap on the COMPRESSED manifest body. Without this, a
/// hostile server can send a multi-GB compressed payload before
/// GzDecoder runs and OOM the daemon. kernel.org's compressed
/// manifest is ~1-2 MiB; 64 MiB is the generous outer limit.
/// Enforced via the Content-Length header (where present) + a
/// streaming counter on the body bytes.
pub const MAX_COMPRESSED_BYTES: u64 = 64 * 1024 * 1024;

/// Wall-clock cap on a single `fetch_manifest` round trip
/// (connect + headers + streamed body). Mirrors `ls_remote::POLL_TIMEOUT`
/// (60s) so the two strategy backstops are symmetric: a stalled mirror
/// (TCP handshake completes but the server never sends bytes, or the
/// path silently drops packets mid-body) is bounded at the strategy
/// layer instead of leaving the supervisor's `CancellationToken` as
/// the only escape hatch. The supervisor cancels on shutdown / SIGHUP,
/// not on slow servers — without this timeout a wedged manifest fetch
/// would hold the per-flow poll task open until the next reload, even
/// while the rest of the daemon stays responsive.
///
/// Layering relative to reqwest's own deadline: reqwest's
/// `ClientBuilder::timeout` (set in the supervisor from
/// `http.request_timeout`, default 30s) is a total request deadline
/// covering the entire send + body-read sequence. For default-config
/// deployments the reqwest deadline at 30s fires before this 60s
/// strategy-layer cap. This 60s wall-clock cap is the operative
/// backstop when an operator configures a longer
/// `http.request_timeout` (e.g. for a slow grokmirror upstream that
/// legitimately needs >30s to respond), and it provides symmetry
/// with `ls_remote::POLL_TIMEOUT` — which wraps a gix-protocol
/// pipeline that runs outside reqwest's scope and therefore has no
/// reqwest deadline at all.
pub const POLL_TIMEOUT: Duration = Duration::from_secs(60);

/// Errors produced by the grokmirror strategy. Classified the same way
/// as `github_api`'s errors so the caller's retry loop can branch on
/// (Transient | Permanent) without inspecting the source error.
#[derive(Debug, thiserror::Error)]
pub enum GrokmirrorError {
    /// Network / 5xx / decode noise — backoff retries.
    #[error("transient: {message}")]
    Transient { message: String },
    /// Manifest doesn't list the configured repo. The caller surfaces
    /// this as `PollOutcome::UnbornRef` and logs a WARN.
    #[error("repo `{repo_path}` not present in manifest")]
    RepoNotInManifest { repo_path: String },
    /// Configuration / decode error that won't recover on retry.
    #[error("permanent: {message}")]
    Permanent { message: String },
}

/// One repo's metadata in the manifest. Only the fields gcit cares
/// about are deserialized; kernel.org adds unknown fields without
/// notice and we tolerate them (no `deny_unknown_fields`).
#[derive(Debug, Clone, Deserialize)]
pub struct RepoEntry {
    /// Per-repo fingerprint. Changes whenever any ref in the repo
    /// moves; comparing fingerprints is the cheap change-detection
    /// signal.
    pub fingerprint: String,
    /// Optional: unix timestamp of last modification. Diagnostic
    /// only; gcit does not use this for change detection.
    #[serde(default)]
    pub modified: Option<u64>,
}

/// The full manifest, keyed by absolute repo path on the mirror
/// (e.g. `/pub/scm/linux/kernel/git/torvalds/linux.git`).
pub type Manifest = BTreeMap<String, RepoEntry>;

/// Fetch + decompress + parse the manifest from `base_url`. Caps the
/// COMPRESSED body at `MAX_COMPRESSED_BYTES` (per Content-Length
/// pre-check + streaming counter) and the DECOMPRESSED body at
/// `MAX_DECOMPRESSED_BYTES`.
///
/// `base_url` is the host root (e.g. `https://git.kernel.org`); the
/// manifest path is appended. The returned manifest is the parsed
/// JSON map, ready for `lookup_fingerprint`.
///
/// Wraps the whole pipeline in `tokio::time::timeout(POLL_TIMEOUT,
/// ...)` so a stalled / slow-drip server cannot wedge the poll task
/// indefinitely (mirrors `ls_remote::poll`'s pattern). On timeout
/// the error is `Transient` so the next backoff cycle retries
/// cleanly.
pub async fn fetch_manifest(client: &Client, base_url: &str) -> Result<Manifest, GrokmirrorError> {
    let url = build_manifest_url(base_url);
    match tokio::time::timeout(POLL_TIMEOUT, fetch_manifest_inner(client, &url)).await {
        Ok(result) => result,
        Err(_) => Err(GrokmirrorError::Transient {
            message: format!("manifest fetch timed out after {:?}: {url}", POLL_TIMEOUT),
        }),
    }
}

/// Inner pipeline (status check + Content-Length pre-check +
/// streamed body read + decompress + parse). Split out so
/// `fetch_manifest` can wrap the whole thing in a single
/// `tokio::time::timeout` without the timeout future escaping
/// across the early-return `?` paths.
async fn fetch_manifest_inner(client: &Client, url: &str) -> Result<Manifest, GrokmirrorError> {
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| GrokmirrorError::Transient {
            message: format!("HTTP request to {url}: {e}"),
        })?;
    let status = response.status();
    if !status.is_success() {
        if status.is_server_error() {
            return Err(GrokmirrorError::Transient {
                message: format!(
                    "manifest fetch returned {} {}",
                    status.as_u16(),
                    status.canonical_reason().unwrap_or("")
                ),
            });
        }
        return Err(GrokmirrorError::Permanent {
            message: format!(
                "manifest fetch returned {} {}",
                status.as_u16(),
                status.canonical_reason().unwrap_or("")
            ),
        });
    }
    // Pre-check Content-Length when the server advertises it. Reject
    // obviously oversized bodies BEFORE allocating a multi-GB buffer.
    // If the server lies (sends a small Content-Length but a large
    // body) the streaming counter below catches it. If Content-Length
    // is absent (chunked encoding), only the streaming counter
    // applies.
    if let Some(len) = response.content_length() {
        if len > MAX_COMPRESSED_BYTES {
            return Err(GrokmirrorError::Permanent {
                message: format!(
                    "manifest Content-Length {len} exceeds cap of {MAX_COMPRESSED_BYTES}",
                ),
            });
        }
    }
    let bytes = read_capped(response).await?;
    parse_manifest_bytes(&bytes)
}

/// Stream the response body into a Vec while enforcing
/// `MAX_COMPRESSED_BYTES`. Returns the full body once read; aborts
/// with Permanent the moment the running counter exceeds the cap.
///
/// This guards against hostile servers that lie about Content-Length
/// (or omit it via chunked encoding). reqwest's `bytes()` reads the
/// whole body into memory with no cap — this OOM vector is closed by
/// streaming with the cap counter.
async fn read_capped(response: reqwest::Response) -> Result<Vec<u8>, GrokmirrorError> {
    use futures_util::StreamExt;
    let mut buf = Vec::new();
    let mut total: u64 = 0;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| GrokmirrorError::Transient {
            message: format!("read manifest body chunk: {e}"),
        })?;
        total = total.saturating_add(chunk.len() as u64);
        if total > MAX_COMPRESSED_BYTES {
            return Err(GrokmirrorError::Permanent {
                message: format!(
                    "manifest body exceeded cap of {MAX_COMPRESSED_BYTES} bytes during stream read",
                ),
            });
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

/// Construct the manifest URL by appending `/manifest.js.gz` to the
/// host root. Tolerates `base_url` already ending with a slash. Pure
/// function so unit tests can pin the produced path independently of
/// the network.
fn build_manifest_url(base_url: &str) -> String {
    let trimmed = base_url.trim_end_matches('/');
    format!("{trimmed}/manifest.js.gz")
}

/// Parse a gzipped manifest body. Caps the decompressed size at
/// `MAX_DECOMPRESSED_BYTES`. Pure function — tests inject the bytes
/// directly without a network round-trip.
///
/// Implementation detail: take(MAX + 1) and reject when the buffer
/// ends up with > MAX bytes. A `.take(MAX)` + `len == MAX` check
/// could not distinguish "cap hit because of a hostile bomb" from
/// "cap hit because the manifest happens to be exactly MAX bytes" —
/// the equality test is fragile. Allowing one byte past the cap and
/// rejecting on `> MAX` means a legitimate manifest at exactly MAX
/// bytes deserializes correctly, while a bomb produces
/// buf.len() == MAX + 1 and fails fast.
pub fn parse_manifest_bytes(bytes: &[u8]) -> Result<Manifest, GrokmirrorError> {
    let mut decoder = GzDecoder::new(bytes).take(MAX_DECOMPRESSED_BYTES + 1);
    let mut buf = Vec::with_capacity(1024 * 1024);
    use std::io::Read;
    decoder
        .read_to_end(&mut buf)
        .map_err(|e| GrokmirrorError::Permanent {
            message: format!("manifest decompression failed: {e}"),
        })?;
    if buf.len() as u64 > MAX_DECOMPRESSED_BYTES {
        return Err(GrokmirrorError::Permanent {
            message: format!(
                "manifest decompressed to over {MAX_DECOMPRESSED_BYTES} bytes; rejecting as potentially hostile",
            ),
        });
    }
    let manifest: Manifest =
        serde_json::from_slice(&buf).map_err(|e| GrokmirrorError::Permanent {
            message: format!("manifest JSON parse: {e}"),
        })?;
    Ok(manifest)
}

/// Look up the configured `repo_path` in the manifest and return its
/// fingerprint. `repo_path` is the absolute path key as stored in the
/// manifest (e.g. `/pub/scm/linux/kernel/git/torvalds/linux.git`).
///
/// Use `extract_repo_path_from_url` to derive the key from the
/// configured `flow.source.url`.
pub fn lookup_fingerprint<'a>(
    manifest: &'a Manifest,
    repo_path: &str,
) -> Result<&'a str, GrokmirrorError> {
    manifest
        .get(repo_path)
        .map(|e| e.fingerprint.as_str())
        .ok_or_else(|| GrokmirrorError::RepoNotInManifest {
            repo_path: repo_path.to_string(),
        })
}

/// Extract the manifest key from a configured source URL. The key is
/// the path component of the URL, including the leading slash.
///
/// Example:
///   `https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git`
///   -> `/pub/scm/linux/kernel/git/torvalds/linux.git`
///
/// Returns `Err(Permanent)` for URLs without a path component.
pub fn extract_repo_path_from_url(url: &str) -> Result<String, GrokmirrorError> {
    let parsed = gix_url::parse(url.as_bytes().into()).map_err(|e| GrokmirrorError::Permanent {
        message: format!("could not parse source URL {url:?}: {e}"),
    })?;
    let path = parsed.path.to_string();
    if path.is_empty() || path == "/" {
        return Err(GrokmirrorError::Permanent {
            message: format!("source URL {url:?} has no path component"),
        });
    }
    let path = if path.starts_with('/') {
        path
    } else {
        format!("/{path}")
    };
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut e = GzEncoder::new(Vec::new(), Compression::default());
        e.write_all(bytes).unwrap();
        e.finish().unwrap()
    }

    #[test]
    fn build_manifest_url_appends_path() {
        assert_eq!(
            build_manifest_url("https://git.kernel.org"),
            "https://git.kernel.org/manifest.js.gz",
        );
        assert_eq!(
            build_manifest_url("https://git.kernel.org/"),
            "https://git.kernel.org/manifest.js.gz",
        );
    }

    #[test]
    fn parse_minimal_manifest() {
        let json = r#"{
            "/pub/linux.git": {"fingerprint": "abc123", "modified": 1700000000}
        }"#;
        let body = gzip(json.as_bytes());
        let m = parse_manifest_bytes(&body).unwrap();
        assert_eq!(m.len(), 1);
        assert_eq!(m["/pub/linux.git"].fingerprint, "abc123");
        assert_eq!(m["/pub/linux.git"].modified, Some(1700000000));
    }

    #[test]
    fn parse_tolerates_unknown_fields() {
        // Don't use deny_unknown_fields. kernel.org adds fields
        // without notice; we ignore them.
        let json = r#"{
            "/r.git": {
                "fingerprint": "xyz",
                "modified": 1,
                "head": "ref: refs/heads/master",
                "owner": "someone",
                "description": "linux kernel",
                "future_field": [1,2,3]
            }
        }"#;
        let body = gzip(json.as_bytes());
        let m = parse_manifest_bytes(&body).unwrap();
        assert_eq!(m["/r.git"].fingerprint, "xyz");
    }

    #[test]
    fn parse_modified_optional() {
        let json = r#"{"/r.git": {"fingerprint": "xyz"}}"#;
        let body = gzip(json.as_bytes());
        let m = parse_manifest_bytes(&body).unwrap();
        assert_eq!(m["/r.git"].fingerprint, "xyz");
        assert_eq!(m["/r.git"].modified, None);
    }

    #[test]
    fn parse_rejects_invalid_gzip() {
        let body = b"not gzip at all";
        let err = parse_manifest_bytes(body).unwrap_err();
        assert!(
            matches!(err, GrokmirrorError::Permanent { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn parse_rejects_invalid_json() {
        let body = gzip(b"not json {{{");
        let err = parse_manifest_bytes(&body).unwrap_err();
        assert!(
            matches!(err, GrokmirrorError::Permanent { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn lookup_fingerprint_hit() {
        let json = r#"{"/r.git": {"fingerprint": "abc"}}"#;
        let body = gzip(json.as_bytes());
        let m = parse_manifest_bytes(&body).unwrap();
        assert_eq!(lookup_fingerprint(&m, "/r.git").unwrap(), "abc");
    }

    #[test]
    fn lookup_fingerprint_miss() {
        let json = r#"{"/r.git": {"fingerprint": "abc"}}"#;
        let body = gzip(json.as_bytes());
        let m = parse_manifest_bytes(&body).unwrap();
        let err = lookup_fingerprint(&m, "/missing.git").unwrap_err();
        assert!(matches!(err, GrokmirrorError::RepoNotInManifest { .. }));
    }

    #[test]
    fn extract_repo_path_from_kernel_url() {
        let p = extract_repo_path_from_url(
            "https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git",
        )
        .unwrap();
        assert_eq!(p, "/pub/scm/linux/kernel/git/torvalds/linux.git");
    }

    #[test]
    fn parse_rejects_decompressed_over_cap() {
        // A body that decompresses to MORE than MAX_DECOMPRESSED_BYTES
        // is rejected. Build a JSON whose
        // decompressed size exceeds the cap by one byte (for the
        // smaller test we use a much-smaller cap-mock by leveraging
        // the public constant: gzip a body that crosses the cap).
        //
        // Constructing a 16 MiB+1 buffer is wasteful in tests. Use a
        // smaller-by-design probe: a body that's 1 byte over the
        // public cap. We don't actually need to compose 16 MiB of
        // valid JSON — anything that decompresses past the cap fails
        // the size check before serde_json runs.
        let oversized = vec![b'a'; (MAX_DECOMPRESSED_BYTES + 1) as usize];
        let body = gzip(&oversized);
        let err = parse_manifest_bytes(&body).unwrap_err();
        match err {
            GrokmirrorError::Permanent { message } => {
                assert!(
                    message.contains("over"),
                    "error message should mention the cap: {message}",
                );
            }
            other => panic!("expected Permanent oversize, got {other:?}"),
        }
    }
}

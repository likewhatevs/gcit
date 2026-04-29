// GitHub API polling: octocrab's `get_ref` to resolve a single ref.
//
// Strategy: GithubApi (github.com) uses octocrab's `get_ref` with a
// default 60s polling interval. Only fine-grained personal access
// tokens (tokens beginning `github_pat_`) are supported. gcit's
// `refs/heads/master` config form converts to octocrab's
// `Reference::Branch("master")` (which renders as `heads/master`).
//
// The endpoint is GET /repos/{owner}/{repo}/git/ref/{ref_path} where
// `ref_path` strips the leading "refs/" (so `refs/heads/master` ->
// `heads/master`). octocrab's `Reference::ref_url()` produces the
// `heads/{branch}` / `tags/{tag}` form already; we pick the variant
// based on the configured ref's prefix.
//
// Error classification:
//   - 404 -> PollOutcome::UnbornRef (Permanent for backoff purposes)
//   - 403/429 -> Transient (caller awaits the rate-limit reset)
//   - 5xx / network errors -> Transient
//   - 4xx other -> Permanent
//
// Auth: octocrab's builder accepts `personal_token(SecretString)` and
// uses it as Bearer in every request. We construct the client once
// per credential at startup and reuse it.

use http::StatusCode;
use octocrab::{models::repos::Object, params::repos::Reference, Octocrab};

use super::PollOutcome;

/// Errors produced by `poll`. Classified so the caller can drive the
/// per-flow retry loop without re-inspecting the source error.
#[derive(Debug, thiserror::Error)]
pub enum GithubError {
    /// Configuration is wrong — gcit will keep polling on the
    /// configured cadence but log a clear WARN. Includes 404 (ref
    /// missing), 401 (bad credentials), 422 (invalid input), other
    /// 4xx that aren't recoverable on retry.
    #[error("permanent: {message}")]
    Permanent { message: String },
    /// Network blip / GitHub indigestion / explicit rate-limit hold.
    /// gcit retries via backon's exponential backoff.
    #[error("transient: {message}")]
    Transient { message: String },
    /// The configured `ref` syntax doesn't fit GitHub's API. gcit
    /// validates `ref starts with refs/` at config load, so the only
    /// path that reaches this branch is a malformed ref that slipped
    /// past validation — defensive.
    #[error("invalid ref `{ref_name}`: {message}")]
    InvalidRef { ref_name: String, message: String },
}

/// Convert `refs/heads/<name>` or `refs/tags/<name>` into octocrab's
/// `Reference` variant. Pure function; rejects any other `refs/...`
/// prefix as the GitHub API only exposes branches and tags through
/// `get_ref`.
fn ref_to_reference(ref_name: &str) -> Result<Reference, GithubError> {
    if let Some(branch) = ref_name.strip_prefix("refs/heads/") {
        if branch.is_empty() {
            return Err(GithubError::InvalidRef {
                ref_name: ref_name.into(),
                message: "branch name is empty after stripping refs/heads/".into(),
            });
        }
        Ok(Reference::Branch(branch.to_string()))
    } else if let Some(tag) = ref_name.strip_prefix("refs/tags/") {
        if tag.is_empty() {
            return Err(GithubError::InvalidRef {
                ref_name: ref_name.into(),
                message: "tag name is empty after stripping refs/tags/".into(),
            });
        }
        Ok(Reference::Tag(tag.to_string()))
    } else {
        Err(GithubError::InvalidRef {
            ref_name: ref_name.into(),
            message: "octocrab get_ref only supports refs/heads/* and refs/tags/* — \
                     pass the full ref path with one of those prefixes"
                .into(),
        })
    }
}

/// Poll `owner/repo` for the SHA of `ref_name`. The ref must use the
/// full `refs/heads/...` or `refs/tags/...` form (config validation
/// already enforces the `refs/` prefix).
///
/// On 404 returns `Ok(PollOutcome::UnbornRef)` so the caller can
/// surface a WARN without driving the backoff path — matching the
/// ls_remote strategy's Unborn handling for cross-strategy
/// consistency. Other errors are classified into
/// `GithubError::{Permanent, Transient}`.
pub async fn poll(
    octo: &Octocrab,
    owner: &str,
    repo: &str,
    ref_name: &str,
) -> Result<PollOutcome, GithubError> {
    let reference = ref_to_reference(ref_name)?;
    let result = octo.repos(owner, repo).get_ref(&reference).await;
    let r = match result {
        Ok(r) => r,
        Err(e) => {
            // Pass-1 fix #2: surface 404 directly as Ok(UnbornRef)
            // rather than Err(Permanent). The doc + PollOutcome
            // contract pin this; the caller logs WARN and keeps
            // polling on cadence.
            if let octocrab::Error::GitHub { source, .. } = &e {
                if source.status_code == StatusCode::NOT_FOUND {
                    return Ok(PollOutcome::UnbornRef);
                }
            }
            return Err(classify_error(e));
        }
    };
    let sha_hex = match r.object {
        Object::Commit { sha, .. } => sha,
        Object::Tag { sha, .. } => sha,
        // octocrab marks Object as #[non_exhaustive]; an unknown
        // discriminant here means GitHub introduced a ref kind gcit
        // doesn't yet handle. Surface as Permanent so the operator
        // sees an actionable message rather than an obscure decode
        // failure.
        _ => {
            return Err(GithubError::Permanent {
                message: format!(
                    "unrecognised ref object type for {ref_name}; gcit only handles commit + tag",
                ),
            });
        }
    };
    let oid =
        gix_hash::ObjectId::from_hex(sha_hex.as_bytes()).map_err(|e| GithubError::Permanent {
            message: format!("could not parse SHA {sha_hex:?} from GitHub: {e}"),
        })?;
    Ok(PollOutcome::Refreshed { sha: oid })
}

/// Classify an octocrab error.
///
/// `poll()` short-circuits 404 into `Ok(PollOutcome::UnbornRef)` per
/// pass-1 fix #2; this function handles every OTHER GitHub error.
/// Non-GitHub errors (transport, hyper, encode) are Transient.
pub fn classify_error(err: octocrab::Error) -> GithubError {
    if let octocrab::Error::GitHub { source, .. } = &err {
        return classify_status(source.status_code, &source.message);
    }
    // Anything else (Hyper, transport, encode/decode) is classified
    // as Transient — these surface from network blips, DNS hiccups,
    // proxy issues, and cleanly retry on the next backoff tick.
    GithubError::Transient {
        message: format!("transport error: {err}"),
    }
}

/// Pure-logic classifier extracted from `classify_error` so unit
/// tests can drive the status-code branches without constructing an
/// octocrab::Error (octocrab's `error` module is private; the inner
/// `GitHubError` type isn't constructible from outside the crate).
/// `classify_error` delegates here for the GitHub error path; tests
/// drive `classify_status` directly.
pub fn classify_status(status: StatusCode, message: &str) -> GithubError {
    if status == StatusCode::NOT_FOUND {
        // poll() short-circuits 404 to UnbornRef; reaching this
        // branch means classify_status was called directly with
        // a 404 (defensive — emit Permanent so upstream callers
        // see a consistent classification).
        return GithubError::Permanent {
            message: format!("ref not found: {message}"),
        };
    }
    if status == StatusCode::TOO_MANY_REQUESTS {
        return GithubError::Transient {
            message: format!("rate-limited (429): {message}"),
        };
    }
    if status == StatusCode::FORBIDDEN {
        // Per pass-1 fix #6: GitHub uses 403 for both rate-limited
        // (Transient — operator should wait for the reset) and
        // permission-denied (Permanent — PAT lacks the required
        // scope; retrying won't help). The message body
        // distinguishes: GitHub's rate-limit body contains "rate
        // limit"; permission errors carry phrases like "Resource not
        // accessible by integration" or "must have admin rights".
        let lower = message.to_ascii_lowercase();
        if lower.contains("rate limit") {
            return GithubError::Transient {
                message: format!("rate-limited (403): {message}"),
            };
        }
        return GithubError::Permanent {
            message: format!(
                "github 403 (permission denied): {message} — \
                 verify the PAT has `Actions: read+write` for the target repo",
            ),
        };
    }
    if status.is_server_error() {
        return GithubError::Transient {
            message: format!("github 5xx: {} {message}", status.as_u16()),
        };
    }
    GithubError::Permanent {
        message: format!("github {}: {message}", status.as_u16()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::StatusCode;

    // Drives classify_status directly — octocrab's error::GitHubError
    // is in a private module, so constructing a full octocrab::Error
    // outside the crate is not possible. classify_status carries the
    // entire branching logic; classify_error is a thin wrapper that
    // pulls (status_code, message) out of the snafu-wrapped variant.

    #[test]
    fn ref_to_reference_branch() {
        let r = ref_to_reference("refs/heads/master").unwrap();
        match r {
            Reference::Branch(b) => assert_eq!(b, "master"),
            other => panic!("expected Branch, got {other:?}"),
        }
    }

    #[test]
    fn ref_to_reference_tag() {
        let r = ref_to_reference("refs/tags/v1.0").unwrap();
        match r {
            Reference::Tag(t) => assert_eq!(t, "v1.0"),
            other => panic!("expected Tag, got {other:?}"),
        }
    }

    #[test]
    fn ref_to_reference_rejects_unknown_namespace() {
        let err = ref_to_reference("refs/notes/commits").unwrap_err();
        assert!(matches!(err, GithubError::InvalidRef { .. }), "got {err:?}",);
    }

    #[test]
    fn ref_to_reference_rejects_empty_branch() {
        let err = ref_to_reference("refs/heads/").unwrap_err();
        assert!(matches!(err, GithubError::InvalidRef { .. }));
    }

    #[test]
    fn ref_to_reference_rejects_bare_name() {
        // Config validation already requires `refs/` prefix but we
        // keep the defensive rejection here.
        let err = ref_to_reference("master").unwrap_err();
        assert!(matches!(err, GithubError::InvalidRef { .. }));
    }

    #[test]
    fn classify_404_is_permanent() {
        let err = classify_status(StatusCode::NOT_FOUND, "Not Found");
        assert!(matches!(err, GithubError::Permanent { .. }), "got {err:?}");
    }

    #[test]
    fn classify_403_with_rate_limit_message_is_transient() {
        // Per pass-1 fix #6: 403 is split by message. A 403 whose
        // body mentions "rate limit" is Transient.
        let err = classify_status(StatusCode::FORBIDDEN, "API rate limit exceeded for user");
        assert!(matches!(err, GithubError::Transient { .. }), "got {err:?}");
    }

    #[test]
    fn classify_403_without_rate_limit_message_is_permanent() {
        // A 403 whose body indicates a permission issue is
        // Permanent — retrying won't help.
        let err = classify_status(
            StatusCode::FORBIDDEN,
            "Resource not accessible by integration",
        );
        match err {
            GithubError::Permanent { message } => {
                assert!(
                    message.contains("Actions: read+write"),
                    "actionable hint missing: {message}",
                );
            }
            other => panic!("expected Permanent, got {other:?}"),
        }
    }

    #[test]
    fn classify_429_is_transient() {
        let err = classify_status(StatusCode::TOO_MANY_REQUESTS, "slow down");
        assert!(matches!(err, GithubError::Transient { .. }), "got {err:?}");
    }

    #[test]
    fn classify_502_is_transient() {
        let err = classify_status(StatusCode::BAD_GATEWAY, "upstream gone");
        assert!(matches!(err, GithubError::Transient { .. }), "got {err:?}");
    }

    #[test]
    fn classify_422_is_permanent() {
        let err = classify_status(StatusCode::UNPROCESSABLE_ENTITY, "bad inputs");
        assert!(matches!(err, GithubError::Permanent { .. }), "got {err:?}");
    }

    #[test]
    fn classify_401_is_permanent() {
        let err = classify_status(StatusCode::UNAUTHORIZED, "bad token");
        assert!(matches!(err, GithubError::Permanent { .. }), "got {err:?}");
    }
}

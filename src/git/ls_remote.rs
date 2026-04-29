// LsRemote polling: gix-protocol stateless ls-refs.
//
// tests/poll_ls_remote.rs covers the file:// transport path against a
// real bare-repo fixture: ref resolution, missing-ref → UnbornRef,
// empty-repo unborn, lightweight tag, annotated tag (tag SHA, not
// peel).
//
// Pipeline:
//   1. gix_transport::client::blocking_io::connect(url, options) builds
//      a `Box<dyn Transport>` for the URL's scheme (http/https/git/
//      ssh/file). All schemes are blocking-IO.
//   2. gix_protocol::handshake(transport, Service::UploadPack, auth_fn,
//      extras, &mut progress) negotiates protocol version + capabilities.
//      For V1 the handshake's `refs` field is `Some(Vec<Ref>)` already
//      — no follow-up ls-refs needed. For V2 we get `None` and must
//      issue an explicit ls-refs.
//   3. gix_protocol::ls_refs::LsRefsCommand::new(prefixes, capabilities,
//      (feature_name, value)).invoke_blocking(transport, &mut progress,
//      trace) issues the V2 ls-refs and returns Vec<Ref>.
//   4. Match the configured `ref_name` against each Ref's full_ref_name,
//      extract the underlying ObjectId per variant (Direct.object,
//      Peeled.tag, Symbolic.object, Unborn -> UnbornRef).
//
// Concurrency: blocking-IO sleep / read calls happen on a dedicated
// thread via `tokio::task::spawn_blocking`. The supervisor holds a
// fixed-size pool. tokio's default blocking thread pool is 512, more
// than enough for v1 deployments.
//
// Auth: source.credential_id is optional. For HTTPS URLs to private
// repos, gix-protocol's handshake calls our `authenticate_fn`
// callback when the server returns 401; the closure then resolves
// the credential and replies. For ssh URLs, the SSH agent /
// configured key handles auth out-of-band; our callback returns
// Empty in that case. For file:// URLs no auth is needed.
//
// gix_features::progress::Discard is a no-op progress reporter.
// We pass it as `&mut Discard` to handshake and invoke_blocking.

use std::borrow::Cow;
use std::time::Duration;

use bstr::ByteSlice;
use gix_features::progress::Discard;
use gix_protocol::{credentials, handshake, LsRefsCommand};
use gix_transport::{client::blocking_io::connect::connect as transport_connect, Service};
use tracing::warn;

use super::PollOutcome;

/// gcit's identity in V2 protocol agent negotiations.
///
/// The agent line is opaque to most servers but kernel.org's
/// grokmirror + GitLab + GitHub all log it; reporting "gcit" makes
/// operator inspection easier than the default "gix".
///
/// `LsRefsCommand::new` accepts a `(feature_name, value)` tuple. The
/// V2 validator at gix-protocol's command.rs special-cases the
/// literal feature name `"agent"` as a fallback when the server's
/// ls-refs allow-list does not list it; any other unknown feature
/// name is rejected with `UnsupportedCapability`. The actual agent
/// identity travels in the value side. Passing the identity string
/// in the name slot causes the validator to reject ls-refs against
/// any V2 server.
const AGENT: &str = "gcit";

/// Errors produced by the LsRemote strategy. Same Transient/Permanent
/// classification as github_api so the caller drives a single retry
/// path.
#[derive(Debug, thiserror::Error)]
pub enum LsRemoteError {
    /// Network blip / transport failure / 5xx — retry under backon.
    #[error("transient: {message}")]
    Transient { message: String },
    /// URL malformed, unknown scheme, auth genuinely rejected after
    /// presenting credentials — won't recover on retry.
    #[error("permanent: {message}")]
    Permanent { message: String },
}

/// Poll the remote at `url` for the SHA of `ref_name`.
///
/// Anonymous-only: the auth callback always returns `Ok(None)`.
/// Private-HTTPS and SSH-with-credential paths are not yet
/// implemented. gcit's primary v1 use case (kernel + Linux mirrors +
/// public Discord workflows) is covered by anonymous polls.
///
/// Wraps the blocking gix-protocol pipeline in `spawn_blocking` so
/// the call returns control to the runtime while the network round
/// trip happens. The closure owns its inputs by move and returns a
/// 'static error type so the spawn_blocking signature is satisfied.
///
/// The spawn_blocking is wrapped in
/// `tokio::time::timeout(POLL_TIMEOUT, ...)`. Without this, a
/// stalled connection (server accepts the TCP handshake but never
/// sends bytes, or the path between gcit and the remote silently
/// drops packets) wedges the blocking thread pool indefinitely and
/// eventually starves the runtime. The timeout returns
/// `Transient` so the next backoff cycle retries cleanly.
///
/// Note: `tokio::time::timeout` aborts the awaiting future but does
/// NOT abort the underlying blocking thread; the blocking task
/// continues to run until the OS unblocks the syscall (TCP keepalive
/// kicks in, kernel times out, etc). This is acceptable for a
/// stalled connection because the thread pool eventually frees up;
/// it is NOT acceptable for an infinite loop, but the gix pipeline
/// is bounded by the transport's own connect/read timeouts.
pub async fn poll(url: String, ref_name: String) -> Result<PollOutcome, LsRemoteError> {
    let task = tokio::task::spawn_blocking(move || poll_blocking(&url, &ref_name));
    match tokio::time::timeout(POLL_TIMEOUT, task).await {
        Err(_) => Err(LsRemoteError::Transient {
            message: format!("ls-remote timed out after {:?}", POLL_TIMEOUT),
        }),
        Ok(Err(join_err)) => Err(LsRemoteError::Transient {
            message: format!("ls-remote task join error: {join_err}"),
        }),
        Ok(Ok(result)) => result,
    }
}

/// Synchronous core of `poll`. Runs entirely on the spawn_blocking
/// thread; returns errors classified into LsRemoteError.
fn poll_blocking(url: &str, ref_name: &str) -> Result<PollOutcome, LsRemoteError> {
    let mut transport =
        transport_connect(url, connect_options()).map_err(|e| classify_connect_error(e, url))?;

    let mut progress = Discard;
    let auth_fn = anonymous_auth_fn();
    let handshake_outcome = gix_protocol::handshake(
        &mut transport,
        Service::UploadPack,
        auth_fn,
        Vec::new(),
        &mut progress,
    )
    .map_err(|e| classify_handshake_error(e, url))?;

    // V1 servers stuff the refs into the handshake response; V2
    // makes us issue an explicit ls-refs.
    let refs: Vec<handshake::Ref> = if let Some(refs) = handshake_outcome.refs {
        refs
    } else {
        let cmd = LsRefsCommand::new(
            None,
            &handshake_outcome.capabilities,
            ("agent", Some(Cow::Borrowed(AGENT))),
        );
        cmd.invoke_blocking(&mut transport, &mut progress, false)
            .map_err(|e| classify_ls_refs_error(e, url))?
    };

    Ok(match find_ref(&refs, ref_name) {
        FoundRef::Direct(oid) => PollOutcome::Refreshed { sha: oid },
        FoundRef::PeeledTag(tag_oid) => {
            // Per the design ruling in tests/poll_symbolic_ref.rs::
            // annotated_tag_dereferenced_to_commit_sha, gcit reports
            // the TAG SHA (not the peeled commit SHA) so re-tagging
            // is detectable. The Peeled variant carries both; we
            // return tag.
            PollOutcome::Refreshed { sha: tag_oid }
        }
        FoundRef::Symbolic(oid) => PollOutcome::Refreshed { sha: oid },
        FoundRef::Unborn => PollOutcome::UnbornRef,
        FoundRef::NotPresent => {
            warn!(
                target: "gcit::git::ls_remote",
                url = %url,
                ref_name = %ref_name,
                ref_count = refs.len(),
                "configured ref not present in remote ref list",
            );
            PollOutcome::UnbornRef
        }
    })
}

fn connect_options() -> gix_transport::client::blocking_io::connect::Options {
    gix_transport::client::blocking_io::connect::Options {
        version: gix_transport::Protocol::V2,
        ..Default::default()
    }
}

/// Anonymous auth callback for the LsRemote pipeline.
///
/// Always returns `Ok(None)` (gix-protocol interprets this as "no
/// credentials available"). For private repos the handshake will
/// fail with PermissionDenied, which classify_handshake_error maps
/// to `LsRemoteError::Permanent`. A future revision will replace
/// this with a real callback that takes the resolved credential and
/// produces a `gix_sec::identity::Account` (HTTP Basic with
/// x-access-token user for fine-grained PATs). gcit's v1 polling
/// targets are public, so anonymous-only is the correct scope.
//
// `gix_credentials::protocol::Error` is ~192 bytes; it never appears
// in this anonymous return path but the closure's signature still
// produces the lint. Boxing the Err variant is incompatible with
// gix-protocol's expected callback signature, so we accept the
// stack-size warning here.
#[allow(clippy::result_large_err)]
fn anonymous_auth_fn() -> impl FnMut(credentials::helper::Action) -> credentials::protocol::Result {
    |_action: credentials::helper::Action| Ok(None)
}

/// Classify the URL/connect error into LsRemoteError. Most
/// connect-stage failures are Permanent (unknown scheme, parse
/// error). Network-level failures fall through to Transient.
fn classify_connect_error(
    e: gix_transport::client::blocking_io::connect::Error,
    url: &str,
) -> LsRemoteError {
    use gix_transport::client::blocking_io::connect::Error as E;
    match e {
        E::Url(_)
        | E::PathConversion(_)
        | E::UnsupportedScheme(_)
        | E::UnsupportedUrlTokens { .. } => LsRemoteError::Permanent {
            message: format!("connect to {url:?}: {e}"),
        },
        E::Connection(_) => LsRemoteError::Transient {
            message: format!("connect to {url:?}: {e}"),
        },
        // Any new variant added by gix in the future falls into the
        // safe Transient bucket so the operator gets retries rather
        // than hard failures on unknown error shapes.
        #[allow(unreachable_patterns)]
        _ => LsRemoteError::Transient {
            message: format!("connect to {url:?}: {e}"),
        },
    }
}

fn classify_handshake_error(e: handshake::Error, url: &str) -> LsRemoteError {
    use handshake::Error as E;
    match e {
        E::InvalidCredentials { .. } => LsRemoteError::Permanent {
            message: format!("invalid credentials for {url:?}: {e}"),
        },
        E::TransportProtocolPolicyViolation { .. } => LsRemoteError::Permanent {
            message: format!("server rejected protocol version for {url:?}: {e}"),
        },
        E::Credentials(_) | E::EmptyCredentials => LsRemoteError::Permanent {
            message: format!("credential helper failure for {url:?}: {e}"),
        },
        E::Transport(_) => LsRemoteError::Transient {
            message: format!("transport failure during handshake to {url:?}: {e}"),
        },
        E::ParseRefs(_) => LsRemoteError::Transient {
            message: format!("ref parsing during handshake to {url:?}: {e}"),
        },
    }
}

fn classify_ls_refs_error(e: gix_protocol::ls_refs::Error, url: &str) -> LsRemoteError {
    use gix_protocol::ls_refs::Error as E;
    match e {
        E::Io(_) | E::Transport(_) => LsRemoteError::Transient {
            message: format!("ls-refs IO/transport for {url:?}: {e}"),
        },
        E::Parse(_) | E::ArgumentValidation(_) => LsRemoteError::Permanent {
            message: format!("ls-refs parse/validation for {url:?}: {e}"),
        },
        #[allow(unreachable_patterns)]
        _ => LsRemoteError::Transient {
            message: format!("unknown ls-refs error for {url:?}: {e}"),
        },
    }
}

/// Outcome of looking up the configured ref in the remote's ref list.
#[derive(Debug)]
enum FoundRef {
    /// Direct (commit-pointing) ref or lightweight tag.
    Direct(gix_hash::ObjectId),
    /// Annotated tag — its own SHA, not the peeled commit's.
    PeeledTag(gix_hash::ObjectId),
    /// Symbolic ref (e.g. HEAD) — the resolved object SHA.
    Symbolic(gix_hash::ObjectId),
    /// Remote reports the ref as unborn (empty repository).
    Unborn,
    /// The ref simply isn't in the returned list.
    NotPresent,
}

/// Match `ref_name` against each ref in the remote's list. Pure
/// function — extracted from poll_blocking for unit testing without
/// the network.
fn find_ref(refs: &[handshake::Ref], ref_name: &str) -> FoundRef {
    let target = ref_name.as_bytes();
    for r in refs {
        match r {
            handshake::Ref::Direct {
                full_ref_name,
                object,
            } if full_ref_name.as_bstr() == target => return FoundRef::Direct(*object),
            handshake::Ref::Peeled {
                full_ref_name, tag, ..
            } if full_ref_name.as_bstr() == target => return FoundRef::PeeledTag(*tag),
            handshake::Ref::Symbolic {
                full_ref_name,
                object,
                ..
            } if full_ref_name.as_bstr() == target => return FoundRef::Symbolic(*object),
            handshake::Ref::Unborn { full_ref_name, .. } if full_ref_name.as_bstr() == target => {
                return FoundRef::Unborn
            }
            _ => continue,
        }
    }
    FoundRef::NotPresent
}

/// Per-strategy default poll timeout. The transport's own connect
/// timeout is shorter; this is the outermost cap on a single poll
/// cycle including handshake + ls-refs.
pub const POLL_TIMEOUT: Duration = Duration::from_secs(60);

#[cfg(test)]
mod tests {
    use super::*;
    use bstr::BString;
    use gix_hash::ObjectId;

    fn sha(byte: u8) -> ObjectId {
        let hex = format!("{byte:02x}").repeat(20);
        ObjectId::from_hex(hex.as_bytes()).unwrap()
    }

    fn make_direct(name: &str, oid: ObjectId) -> handshake::Ref {
        handshake::Ref::Direct {
            full_ref_name: BString::from(name),
            object: oid,
        }
    }

    fn make_peeled(name: &str, tag: ObjectId, object: ObjectId) -> handshake::Ref {
        handshake::Ref::Peeled {
            full_ref_name: BString::from(name),
            tag,
            object,
        }
    }

    fn make_symbolic(name: &str, target: &str, object: ObjectId) -> handshake::Ref {
        handshake::Ref::Symbolic {
            full_ref_name: BString::from(name),
            target: BString::from(target),
            tag: None,
            object,
        }
    }

    fn make_unborn(name: &str, target: &str) -> handshake::Ref {
        handshake::Ref::Unborn {
            full_ref_name: BString::from(name),
            target: BString::from(target),
        }
    }

    #[test]
    fn find_direct_ref() {
        let refs = vec![make_direct("refs/heads/main", sha(0xaa))];
        match find_ref(&refs, "refs/heads/main") {
            FoundRef::Direct(o) => assert_eq!(o, sha(0xaa)),
            other => panic!("expected Direct, got {other:?}"),
        }
    }

    #[test]
    fn find_peeled_tag_returns_tag_sha_not_peel() {
        // Per tests/poll_symbolic_ref.rs::annotated_tag_dereferenced_to_commit_sha
        // ruling: tag SHA wins so re-tagging is detectable.
        let tag = sha(0x10);
        let commit = sha(0x20);
        let refs = vec![make_peeled("refs/tags/v1.0", tag, commit)];
        match find_ref(&refs, "refs/tags/v1.0") {
            FoundRef::PeeledTag(o) => assert_eq!(o, tag),
            other => panic!("expected PeeledTag, got {other:?}"),
        }
    }

    #[test]
    fn find_symbolic_ref() {
        let refs = vec![make_symbolic("HEAD", "refs/heads/main", sha(0xcc))];
        match find_ref(&refs, "HEAD") {
            FoundRef::Symbolic(o) => assert_eq!(o, sha(0xcc)),
            other => panic!("expected Symbolic, got {other:?}"),
        }
    }

    #[test]
    fn find_unborn_ref() {
        let refs = vec![make_unborn("HEAD", "refs/heads/main")];
        match find_ref(&refs, "HEAD") {
            FoundRef::Unborn => {}
            other => panic!("expected Unborn, got {other:?}"),
        }
    }

    #[test]
    fn find_missing_ref() {
        let refs = vec![make_direct("refs/heads/main", sha(0xaa))];
        match find_ref(&refs, "refs/heads/release") {
            FoundRef::NotPresent => {}
            other => panic!("expected NotPresent, got {other:?}"),
        }
    }

    #[test]
    fn find_first_match_wins_in_ordered_list() {
        // gix returns refs in the order the server sent them.
        // find_ref must short-circuit on the first match — important
        // for the symbolic + direct combo where HEAD comes before
        // refs/heads/main in V1 advertisement.
        let refs = vec![
            make_direct("refs/heads/main", sha(0x01)),
            make_direct("refs/heads/main", sha(0x02)), // hypothetical duplicate
        ];
        match find_ref(&refs, "refs/heads/main") {
            FoundRef::Direct(o) => assert_eq!(o, sha(0x01), "first match wins"),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn classify_connect_url_error_is_permanent() {
        // gix_url::parse::Error is publicly constructible only via
        // `parse` failing. Drive via a malformed URL. The conversion
        // into the connect Error is via the From impl declared on
        // the connect Error enum.
        let err = gix_url::parse(b"::not a url::".as_bstr()).unwrap_err();
        let connect_err: gix_transport::client::blocking_io::connect::Error = err.into();
        let classified = classify_connect_error(connect_err, "::not a url::");
        assert!(
            matches!(classified, LsRemoteError::Permanent { .. }),
            "got {classified:?}",
        );
    }
}

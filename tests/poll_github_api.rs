// GitHub API polling via octocrab get_ref.
// GithubApi -> github.com -> octocrab get_ref(); default 60s.
// Testing strategy: wiremock + octocrab base_uri.
//
// gcit's polling entry point is `gcit::git::github_api::poll(octo,
// owner, repo, ref_name)`. The function dispatches to octocrab's
// `repos(owner, repo).get_ref(...)` and classifies any failure into
// `GithubError::{Permanent, Transient, InvalidRef}`. 404 short-
// circuits to `Ok(PollOutcome::UnbornRef)` inside github_api::poll.
//
// The wiremock setup intercepts the GET on
//   /repos/{owner}/{repo}/git/ref/{ref_path}
// where `ref_path` strips the leading "refs/" so e.g.
// `refs/heads/master` becomes `heads/master`. Tests pin the route
// shape and the response-classification arms at the integration-test
// boundary; the in-module unit tests in github_api::tests already
// cover classify_status for every documented status code.

use octocrab::Octocrab;
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use gcit::git::{github_api, PollOutcome};

mod common;

const PAT: &str = "github_pat_test_token_for_wiremock_only_no_real_secret";

async fn build_octocrab(base_uri: &str) -> Octocrab {
    common::ensure_crypto_provider();
    Octocrab::builder()
        .base_uri(base_uri)
        .expect("base_uri parses")
        .personal_token(secrecy::SecretString::from(PAT.to_string()))
        .build()
        .expect("octocrab builds")
}

#[tokio::test]
async fn get_ref_returns_sha_for_existing_branch() {
    let mock = MockServer::start().await;
    let sha = "a".repeat(40);
    Mock::given(method("GET"))
        .and(path("/repos/torvalds/linux/git/ref/heads/master"))
        .and(header("authorization", format!("Bearer {PAT}").as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "ref": "refs/heads/master",
            "node_id": "node-id",
            "url": format!("{}/repos/torvalds/linux/git/refs/heads/master", mock.uri()),
            "object": {
                "sha": sha,
                "type": "commit",
                "url": format!("{}/repos/torvalds/linux/git/commits/{sha}", mock.uri()),
            }
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let octo = build_octocrab(&mock.uri()).await;
    let outcome = github_api::poll(&octo, "torvalds", "linux", "refs/heads/master")
        .await
        .expect("poll succeeds against wiremock");
    match outcome {
        PollOutcome::Refreshed { sha: parsed } => {
            assert_eq!(parsed.to_hex().to_string(), sha);
        }
        other => panic!("expected Refreshed, got {other:?}"),
    }
}

#[tokio::test]
async fn get_ref_returns_sha_for_existing_tag() {
    // gcit accepts both refs/heads/* and refs/tags/* per the
    // ref_to_reference branch table. The path component for a tag
    // becomes `tags/<name>`.
    let mock = MockServer::start().await;
    let sha = "b".repeat(40);
    Mock::given(method("GET"))
        .and(path("/repos/myorg/release/git/ref/tags/v1.2.3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "ref": "refs/tags/v1.2.3",
            "node_id": "tagnode",
            "url": format!("{}/repos/myorg/release/git/refs/tags/v1.2.3", mock.uri()),
            "object": {
                "sha": sha,
                "type": "tag",
                "url": format!("{}/repos/myorg/release/git/tags/{sha}", mock.uri()),
            }
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let octo = build_octocrab(&mock.uri()).await;
    let outcome = github_api::poll(&octo, "myorg", "release", "refs/tags/v1.2.3")
        .await
        .expect("poll succeeds for tag");
    match outcome {
        PollOutcome::Refreshed { sha: parsed } => {
            assert_eq!(parsed.to_hex().to_string(), sha);
        }
        other => panic!("expected Refreshed, got {other:?}"),
    }
}

#[tokio::test]
async fn get_ref_404_returns_unborn_ref_outcome() {
    // github_api::poll short-circuits a 404 from GitHub to
    // `Ok(PollOutcome::UnbornRef)` so the caller logs a WARN and
    // keeps polling on cadence rather than driving the backoff path.
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/owner/repo/git/ref/heads/never-existed"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "message": "Not Found",
            "documentation_url": "https://docs.github.com/rest"
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let octo = build_octocrab(&mock.uri()).await;
    let outcome = github_api::poll(&octo, "owner", "repo", "refs/heads/never-existed")
        .await
        .expect("404 must surface as Ok(UnbornRef), not Err");
    assert!(matches!(outcome, PollOutcome::UnbornRef), "got {outcome:?}");
}

#[tokio::test]
async fn get_ref_5xx_returns_transient_error() {
    // GitHub's 5xx responses sometimes carry a JSON body shaped like
    // its standard `GitHubError`, sometimes not. classify_error
    // handles BOTH:
    //   - octocrab returns Error::GitHub when the body fits → classify_status
    //     hits the `status.is_server_error()` arm → Transient.
    //   - octocrab returns Error::Json / Error::Serde / Error::Other
    //     when the body shape doesn't fit → classify_error's catch-all
    //     also classifies as Transient.
    // Either path produces Transient, so the test no longer needs a
    // body that exactly fits GitHub's error shape; it just asserts the
    // public contract.
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/owner/repo/git/ref/heads/main"))
        .respond_with(ResponseTemplate::new(503).set_body_json(json!({
            "message": "Service Unavailable",
            "documentation_url": "https://docs.github.com/rest"
        })))
        // octocrab's hyper client retries 5xx responses internally
        // before surfacing the error (a few attempts with backoff).
        // The test does not pin the exact attempt count — that's
        // octocrab's contract — so accept any positive count rather
        // than `.expect(1)`. The classification behaviour after the
        // last attempt is what gcit owns.
        .expect(1..)
        .mount(&mock)
        .await;

    let octo = build_octocrab(&mock.uri()).await;
    let err = github_api::poll(&octo, "owner", "repo", "refs/heads/main")
        .await
        .expect_err("503 must classify as error");
    match err {
        github_api::GithubError::Transient { .. } => {}
        other => panic!("expected Transient, got {other:?}"),
    }
}

#[tokio::test]
async fn get_ref_403_with_rate_limit_message_is_transient() {
    // 403 with body containing "rate limit" classifies as Transient
    // (the bucket should defer until the reset). 403 without the
    // rate-limit body is Permanent (PAT scope issue).
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/owner/repo/git/ref/heads/main"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "message": "API rate limit exceeded for user 12345"
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let octo = build_octocrab(&mock.uri()).await;
    let err = github_api::poll(&octo, "owner", "repo", "refs/heads/main")
        .await
        .expect_err("403 must classify as error");
    match err {
        github_api::GithubError::Transient { message } => {
            assert!(
                message.contains("rate"),
                "rate-limit 403 message should mention rate: {message}",
            );
        }
        other => panic!("expected Transient, got {other:?}"),
    }
}

#[tokio::test]
async fn get_ref_403_permission_denied_is_permanent() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/owner/repo/git/ref/heads/main"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "message": "Resource not accessible by integration"
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let octo = build_octocrab(&mock.uri()).await;
    let err = github_api::poll(&octo, "owner", "repo", "refs/heads/main")
        .await
        .expect_err("403 must classify as error");
    match err {
        github_api::GithubError::Permanent { message } => {
            assert!(
                message.contains("403") || message.contains("Resource"),
                "permission 403 must surface message: {message}",
            );
        }
        other => panic!("expected Permanent, got {other:?}"),
    }
}

#[tokio::test]
async fn get_ref_uses_bearer_token_from_personal_token_builder() {
    // Pin the auth shape: octocrab's personal_token() sets
    // Authorization: Bearer <token>. A regression to a different
    // auth scheme (e.g., Basic) would surface as a wiremock
    // unmatched-request because the matcher requires the Bearer
    // header.
    let mock = MockServer::start().await;
    let sha = "c".repeat(40);
    Mock::given(method("GET"))
        .and(path("/repos/owner/repo/git/ref/heads/main"))
        .and(header("authorization", format!("Bearer {PAT}").as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "ref": "refs/heads/main",
            "node_id": "node",
            "url": format!("{}/repos/owner/repo/git/refs/heads/main", mock.uri()),
            "object": {
                "sha": sha,
                "type": "commit",
                "url": format!("{}/repos/owner/repo/git/commits/{sha}", mock.uri()),
            }
        })))
        .expect(1)
        .mount(&mock)
        .await;
    let octo = build_octocrab(&mock.uri()).await;
    let _ = github_api::poll(&octo, "owner", "repo", "refs/heads/main")
        .await
        .expect("authenticated poll succeeds");
}

#[tokio::test]
async fn get_ref_path_strips_refs_prefix() {
    // Pin the URL-shape contract: gcit's adapter sends
    // /repos/o/r/git/ref/<ref-without-refs-prefix>. wiremock's path
    // matcher fails the test if the prefix is left intact.
    let mock = MockServer::start().await;
    let sha = "d".repeat(40);
    Mock::given(method("GET"))
        // octocrab encodes the path component; the request must
        // arrive with `heads/feature%2Fa%2Fb` for nested branches.
        // For this assertion we only confirm the prefix-strip — pick
        // a name without slashes.
        .and(path("/repos/o/r/git/ref/heads/feature-x"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "ref": "refs/heads/feature-x",
            "node_id": "node",
            "url": format!("{}/repos/o/r/git/refs/heads/feature-x", mock.uri()),
            "object": {
                "sha": sha,
                "type": "commit",
                "url": format!("{}/repos/o/r/git/commits/{sha}", mock.uri()),
            }
        })))
        .expect(1)
        .mount(&mock)
        .await;
    let octo = build_octocrab(&mock.uri()).await;
    let _ = github_api::poll(&octo, "o", "r", "refs/heads/feature-x")
        .await
        .expect("path strip must produce the expected URL shape");
}

#[tokio::test]
async fn get_ref_invalid_namespace_returns_invalid_ref_error() {
    // refs/notes/* and refs/pull/* are not supported by github's
    // `get_ref` endpoint; ref_to_reference rejects them at the
    // Reference-construction step before any network round-trip. The
    // mock is mounted but should NOT be hit (.expect(0)).
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(404))
        .expect(0)
        .mount(&mock)
        .await;
    let octo = build_octocrab(&mock.uri()).await;
    let err = github_api::poll(&octo, "o", "r", "refs/notes/commits")
        .await
        .expect_err("notes ref must reject pre-network");
    match err {
        github_api::GithubError::InvalidRef { ref_name, .. } => {
            assert_eq!(ref_name, "refs/notes/commits");
        }
        other => panic!("expected InvalidRef, got {other:?}"),
    }
}

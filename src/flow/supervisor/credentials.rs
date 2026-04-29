// Per-credential resource pool and credential-file resolution.
//
// `CredentialPool` caches the GitHub client + per-credential rate
// bucket + rate-limit poller per credential id so multiple flows that
// share a credential share one backing pool entry. `resolve_secret`
// walks the 3-step credential resolution chain
// ($CREDENTIALS_DIRECTORY -> env var -> <config_dir>/credentials/<id>)
// with the file-invariant probe shared with `cli::check`.
//
// `read_credential_file` is the spawn_blocking wrapper around
// `std::fs::read_to_string` with a 5-second timeout AND a
// process-wide concurrency cap (`CREDENTIAL_READ_CONCURRENCY`) so a
// hung NFS/FUSE backing store cannot stall the supervisor's select!
// loop nor saturate tokio's blocking-pool slots.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use secrecy::SecretString;
use tokio_util::sync::CancellationToken;

use crate::config::credential_file::{self, Probe};
use crate::config::CredentialId;
use crate::git::rate_bucket::RateBucket;
use crate::github::client::Client as GithubClient;
use crate::github::rate_limit::{self as gh_rate_limit, RateLimitState};

/// Per-credential resource pool. The pool caches the Octocrab handle,
/// rate bucket, rate-limit state, and rate-limit poller task per
/// credential id so multiple flows that share a credential share the
/// same backing resources.
///
/// `config_dir` is the canonicalized parent of the daemon's config
/// path; `resolve_secret` reads `<config_dir>/credentials/<id>` as
/// step 3 of the credential resolution chain.
#[derive(Default)]
pub(super) struct CredentialPool {
    by_id: BTreeMap<CredentialId, Arc<GithubCredentialResources>>,
    secrets: BTreeMap<CredentialId, SecretString>,
    config_dir: Option<PathBuf>,
}

impl CredentialPool {
    /// Build a CredentialPool that knows where to look for step-3
    /// credential files. `config_path` is the value of `--config`
    /// (which may be a relative path); the parent of its canonical
    /// form is the search root. Mirrors the same canonicalize + parent
    /// pattern at the top of `cli::check::run`.
    pub(super) fn with_config_path(config_path: &std::path::Path) -> Self {
        let canon = config_path
            .canonicalize()
            .unwrap_or_else(|_| config_path.to_path_buf());
        let config_dir = canon.parent().map(|p| p.to_path_buf());
        Self {
            by_id: BTreeMap::new(),
            secrets: BTreeMap::new(),
            config_dir,
        }
    }

    /// Drop every cached secret + GitHub-credential resource whose
    /// CredentialId is NOT in `keep_credentials`. Used by `run_reload`
    /// to honor rotated PATs without a daemon restart while preserving
    /// rate-limit poller state for kept-alive flows that still
    /// reference an unchanged credential.
    ///
    /// Behaviour per entry:
    ///   - keep: skipped entirely (rate-limit poller continues running,
    ///     `Arc<GithubCredentialResources>` stays in `by_id`, secret
    ///     stays in `secrets`).
    ///   - drop: cancels the rate-limit poller's CancellationToken
    ///     (so the now-orphaned task exits instead of running until
    ///     daemon shutdown), removes the entry from `by_id`, and
    ///     removes the secret from `secrets`.
    ///
    /// **Known limitation:** credential rotation does NOT propagate to
    /// flows whose credential id is still referenced by any kept-alive
    /// flow. The pool entry survives with its old token, and ANY flow
    /// using that credential id — including changed flows that
    /// respawn during the same reload AND newly-added flows — sees
    /// the old value via the `by_id` cache hit at the top of
    /// `acquire_github`. Rotation propagates only when no kept-alive
    /// flow holds the credential (so the entry was dropped and
    /// `acquire_github` falls through to `resolve_secret`, which
    /// re-reads the credential file). To force propagation now,
    /// change any field in EVERY kept-alive flow that references the
    /// credential — that flips them out of `to_keep`, drops them from
    /// `keep_credentials`, and the pool entry is invalidated. If
    /// rotating because the old token was compromised, restart the
    /// daemon (`systemctl restart gcit`) rather than SIGHUP to ensure
    /// every flow uses the new token immediately.
    pub(super) fn invalidate_except(&mut self, keep_credentials: &BTreeSet<CredentialId>) {
        self.by_id.retain(|id, r| {
            if keep_credentials.contains(id) {
                true
            } else {
                r.rate_limit_cancel.cancel();
                false
            }
        });
        self.secrets.retain(|id, _| keep_credentials.contains(id));
    }
}

pub(super) struct GithubCredentialResources {
    pub(super) github_client: Arc<GithubClient>,
    pub(super) rate_bucket: Arc<RateBucket>,
    pub(super) rate_limit: Arc<RateLimitState>,
    /// Octocrab handle reused by the GithubApi poll strategy when the
    /// flow's source URL also points at github.com.
    pub(super) octocrab: Option<Arc<octocrab::Octocrab>>,
    /// Reqwest handle reused by the Grokmirror poll strategy. Built
    /// at boot from `config.http.request_timeout` and shared across
    /// every credential.
    pub(super) reqwest: Option<Arc<reqwest::Client>>,
    /// Cancellation token owned by the rate-limit poller task.
    /// `CredentialPool::invalidate_except` calls `cancel()` on every
    /// dropped entry's token before removing it so an SIGHUP credential
    /// rotation does not orphan the poller (the poller's own loop
    /// checks this token; without cancellation it would keep running
    /// until daemon shutdown even though no flow references the
    /// resources anymore). Entries kept across the reload retain their
    /// uncancelled token and the poller continues running.
    rate_limit_cancel: CancellationToken,
    /// Background rate-limit poll task; held so it can't be dropped
    /// before the credential is.
    _rate_limit_handle: tokio::task::JoinHandle<()>,
}

impl CredentialPool {
    /// Resolve the secret for `id` per the 3-step chain:
    ///   1. $CREDENTIALS_DIRECTORY/<id> (systemd LoadCredential).
    ///   2. GCIT_CREDENTIAL_<UPPER_SNAKE_ID> env var.
    ///   3. <config_dir>/credentials/<id> file.
    ///
    /// Caches the resolved `SecretString` so subsequent lookups don't
    /// re-read the file. The cached value stays wrapped in
    /// `secrecy::SecretString` and the caller exposes it only at the
    /// boundary where the value must be sent (HTTP authorization
    /// headers, octocrab personal token construction, Discord webhook
    /// URL build).
    ///
    /// Step 1 and step 3 share the file-invariant check with
    /// `cli/check` via `credential_file::probe`: the file must be a
    /// regular file (not a symlink, not a fifo / socket / device /
    /// dir), carry no group/other access bits (i.e. `mode & 0o077 == 0`,
    /// so 0o400 / 0o500 / 0o600 / 0o700 all qualify), and be owned by
    /// the resolving process's effective uid OR root (root is accepted
    /// so operators on `DynamicUser=yes` units can drop credentials
    /// via `sudo`). A path that exists but fails any invariant aborts
    /// resolution with the probe's error message — the chain does not
    /// fall through to a later step that might silently mask the
    /// misconfiguration.
    ///
    /// Step 1 only runs when `$CREDENTIALS_DIRECTORY` is set AND
    /// resolves to a real directory: a stale env var value pointing at
    /// a missing or non-directory path is treated as if the variable
    /// were unset (same rule cli/check applies to its state-3
    /// detection).
    ///
    /// Async because the file reads run on the tokio blocking pool
    /// (so a hung NFS/FUSE backing store cannot stall the supervisor's
    /// select! loop). Each read is wrapped in a 5-second timeout —
    /// past that the file-read closure is abandoned to the blocking
    /// pool while the caller proceeds with an error.
    pub(super) async fn resolve_secret(
        &mut self,
        id: &CredentialId,
    ) -> Result<SecretString, String> {
        if let Some(s) = self.secrets.get(id) {
            return Ok(s.clone());
        }
        // Step 1: $CREDENTIALS_DIRECTORY/<id>. systemd's LoadCredential=
        // already drops these into a private namespace at mode 0400
        // owned by the service uid, so the invariant checks here are
        // defense-in-depth — and exact behavioural symmetry with the
        // operator-facing `gcit check` surface so an operator chmoded
        // the file wrong gets the same rejection at boot as in their
        // pre-flight.
        if let Some(dir) = std::env::var_os("CREDENTIALS_DIRECTORY")
            .map(PathBuf::from)
            .filter(|d| d.is_dir())
        {
            let p = dir.join(id.as_str());
            match credential_file::probe(&p) {
                Probe::Ok => {
                    let raw = read_credential_file(&p).await?;
                    let trimmed = raw.trim_end_matches(['\r', '\n']).to_string();
                    let secret = SecretString::from(trimmed);
                    self.secrets.insert(id.clone(), secret.clone());
                    return Ok(secret);
                }
                Probe::NotPresent => {
                    // Fall through to step 2.
                }
                Probe::Invariant(err) => {
                    return Err(err.render(id, &p));
                }
                Probe::StatError(err) => {
                    return Err(credential_file::render_stat_error(
                        &err,
                        id,
                        &p,
                        credential_file::Context::Daemon,
                    ));
                }
            }
        }
        // Step 2: env var.
        let env_name = id.to_env_var();
        if let Ok(v) = std::env::var(&env_name) {
            let secret = SecretString::from(v);
            self.secrets.insert(id.clone(), secret.clone());
            return Ok(secret);
        }
        // Step 3: <config_dir>/credentials/<id> file.
        if let Some(dir) = &self.config_dir {
            let p = dir.join("credentials").join(id.as_str());
            match credential_file::probe(&p) {
                Probe::Ok => {
                    let raw = read_credential_file(&p).await?;
                    let trimmed = raw.trim_end_matches(['\r', '\n']).to_string();
                    let secret = SecretString::from(trimmed);
                    self.secrets.insert(id.clone(), secret.clone());
                    return Ok(secret);
                }
                Probe::NotPresent => {
                    // Fall through to the not-found error below.
                }
                Probe::Invariant(err) => {
                    return Err(err.render(id, &p));
                }
                Probe::StatError(err) => {
                    return Err(credential_file::render_stat_error(
                        &err,
                        id,
                        &p,
                        credential_file::Context::Daemon,
                    ));
                }
            }
        }
        // Step 4: error listing every searched path.
        let config_path_repr = self
            .config_dir
            .as_ref()
            .map(|d| format!("{}/credentials/{}", d.display(), id))
            .unwrap_or_else(|| format!("<config_dir>/credentials/{}", id));
        Err(format!(
            "credential `{}` not found at $CREDENTIALS_DIRECTORY/{} or env ${} or {}",
            id, id, env_name, config_path_repr,
        ))
    }

    pub(super) async fn acquire_github(
        &mut self,
        id: &CredentialId,
        request_timeout: Duration,
        shared_reqwest: Arc<reqwest::Client>,
        root_cancel: CancellationToken,
    ) -> Result<Arc<GithubCredentialResources>, String> {
        if let Some(r) = self.by_id.get(id) {
            return Ok(Arc::clone(r));
        }
        let token_secret = self.resolve_secret(id).await?;
        let client = GithubClient::builder()
            .credential(id.clone())
            .token(token_secret)
            .request_timeout(request_timeout)
            .build()
            .map_err(|e| format!("github client: {e}"))?;
        let github_client = Arc::new(client);
        // Per-credential pacing: 1s between requests is the default.
        let rate_bucket = Arc::new(RateBucket::new(Duration::from_secs(1)));
        let rate_limit = Arc::new(RateLimitState::new());
        // Rate-limit poller task. Owns its own clone of the client +
        // state. Cancellation token is a child of the supervisor's
        // root_cancel so daemon shutdown unwinds the poller cleanly
        // rather than orphaning it.
        let rl_client = (*github_client).clone();
        let rl_state = (*rate_limit).clone();
        let rl_cancel = root_cancel.child_token();
        let rl_cancel_for_task = rl_cancel.clone();
        let handle = tokio::spawn(async move {
            gh_rate_limit::poll_loop(rl_client, rl_state, rl_cancel_for_task).await;
        });
        let octocrab = Some(Arc::new(github_client.octocrab().clone()));
        let reqwest = Some(shared_reqwest);
        let r = Arc::new(GithubCredentialResources {
            github_client,
            rate_bucket,
            rate_limit,
            octocrab,
            reqwest,
            rate_limit_cancel: rl_cancel,
            _rate_limit_handle: handle,
        });
        self.by_id.insert(id.clone(), Arc::clone(&r));
        Ok(r)
    }
}

/// Cap on concurrent credential-file reads via spawn_blocking. Each
/// permit corresponds to one in-flight blocking-pool task; without
/// the cap, a 100-flow config with all credentials behind a slow
/// NFS/FUSE backing store could pin 100 blocking-pool slots
/// simultaneously, starving every other spawn_blocking caller in the
/// daemon (state writer, future cred lookups). 8 is conservative
/// relative to tokio's default blocking-pool size (512) but generous
/// enough that a normal config (under 16 flows) sees no contention.
const CREDENTIAL_READ_CONCURRENCY: usize = 8;

/// Process-wide gate used by `read_credential_file` to bound the
/// number of in-flight `spawn_blocking` reads. Lazily initialized; the
/// permit acquire is async so contended waiters yield rather than
/// busy-spinning.
static CREDENTIAL_READ_SEMAPHORE: std::sync::OnceLock<Arc<tokio::sync::Semaphore>> =
    std::sync::OnceLock::new();

fn credential_read_semaphore() -> Arc<tokio::sync::Semaphore> {
    CREDENTIAL_READ_SEMAPHORE
        .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(CREDENTIAL_READ_CONCURRENCY)))
        .clone()
}

/// Read a credential file off the supervisor's select! loop. Wraps
/// `std::fs::read_to_string` in `tokio::task::spawn_blocking` so the
/// runtime worker stays free during the read, and adds a 5-second
/// timeout so a hung NFS/FUSE backing store cannot stall credential
/// resolution indefinitely. On timeout the spawn_blocking task is
/// abandoned (it continues to completion on the blocking pool —
/// spawn_blocking tasks cannot be cancelled — but the caller proceeds
/// with an error so the supervisor's select! loop stays responsive).
///
/// Concurrency bound: a process-wide semaphore caps in-flight reads
/// at `CREDENTIAL_READ_CONCURRENCY`. A high-flow-count config behind
/// a slow backing store would otherwise saturate tokio's blocking
/// pool with hung reads, denying the rest of the daemon (state
/// writer, future cred lookups) any blocking-pool slots.
async fn read_credential_file(p: &std::path::Path) -> Result<String, String> {
    const READ_TIMEOUT: Duration = Duration::from_secs(5);
    let owned = p.to_path_buf();
    let display = owned.display().to_string();
    // Acquire the concurrency permit BEFORE spawn_blocking. The
    // permit is held for the lifetime of the spawn_blocking handle
    // (including the timeout-abandoned path) so a hung read keeps
    // its slot occupied — but only one slot, not 100. Acquire is
    // also cancel-safe via the timeout below: if the wait for a
    // permit takes longer than READ_TIMEOUT, the timeout fires and
    // the caller surfaces an error rather than blocking the
    // supervisor's select! loop indefinitely.
    let semaphore = credential_read_semaphore();
    let permit = match tokio::time::timeout(READ_TIMEOUT, semaphore.acquire_owned()).await {
        Ok(Ok(p)) => p,
        Ok(Err(_closed)) => {
            // Semaphore is never closed in production paths
            // (process-wide static); this branch exists only for
            // defense-in-depth.
            return Err(format!(
                "read {display}: credential read semaphore closed (bug)"
            ));
        }
        Err(_) => {
            return Err(format!(
                "read {display}: timed out after {}s waiting for credential read slot",
                READ_TIMEOUT.as_secs(),
            ));
        }
    };
    let join = tokio::task::spawn_blocking(move || {
        // Hold the permit for the duration of the read; release on
        // task completion (the move into the closure transfers
        // ownership; permit drops when the closure returns).
        let _permit = permit;
        std::fs::read_to_string(&owned)
    });
    match tokio::time::timeout(READ_TIMEOUT, join).await {
        Ok(Ok(Ok(s))) => Ok(s),
        Ok(Ok(Err(e))) => Err(format!("read {display}: {e}")),
        Ok(Err(e)) => Err(format!("read {display}: blocking task failed: {e}")),
        Err(_) => Err(format!(
            "read {display}: timed out after {}s (slow or hung backing store)",
            READ_TIMEOUT.as_secs(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;
    use serial_test::serial;
    use std::os::unix::fs::PermissionsExt;

    /// Build a CredentialPool whose `config_dir` points at the supplied
    /// path. Skips the canonicalize step (which would touch the real
    /// filesystem); tests pass a `tempfile::TempDir` path directly.
    fn pool_with_dir(dir: std::path::PathBuf) -> CredentialPool {
        CredentialPool {
            by_id: BTreeMap::new(),
            secrets: BTreeMap::new(),
            config_dir: Some(dir),
        }
    }

    /// Write `contents` to `<dir>/credentials/<id>` at mode 0o600 so
    /// `credential_file::probe` accepts it. Returns the file path so
    /// tests can pass it through assertions.
    fn drop_credential_file(dir: &std::path::Path, id: &str, contents: &str) -> std::path::PathBuf {
        let creds = dir.join("credentials");
        std::fs::create_dir_all(&creds).expect("create credentials dir");
        let p = creds.join(id);
        std::fs::write(&p, contents).expect("write credential file");
        let mut perms = std::fs::metadata(&p)
            .expect("stat credential file")
            .permissions();
        perms.set_mode(0o600);
        std::fs::set_permissions(&p, perms).expect("chmod credential file");
        p
    }

    /// Drop env vars the resolution chain consults so each test starts
    /// from a clean slate. Pairs with `#[serial]` to keep concurrent
    /// env mutation off the table.
    fn scrub_env(id: &CredentialId) {
        // SAFETY: Tests are serialized via #[serial], so concurrent
        // env mutation cannot race. Setting/unsetting env vars is
        // safe in a single-threaded context.
        unsafe {
            std::env::remove_var("CREDENTIALS_DIRECTORY");
            std::env::remove_var(id.to_env_var());
        }
    }

    #[tokio::test]
    #[serial]
    async fn resolve_secret_step3_reads_credentials_file_under_config_dir() {
        // Step 3 of the chain: <config_dir>/credentials/<id> file.
        let id = CredentialId::new("step3-cred").expect("valid id");
        scrub_env(&id);
        let tmp = tempfile::tempdir().expect("tempdir");
        drop_credential_file(tmp.path(), id.as_str(), "step3-secret-value");
        let mut pool = pool_with_dir(tmp.path().to_path_buf());
        let s = pool.resolve_secret(&id).await.expect("step3 must resolve");
        assert_eq!(s.expose_secret(), "step3-secret-value");
    }

    #[tokio::test]
    #[serial]
    async fn resolve_secret_step3_trims_trailing_newline() {
        // Operators commonly drop credentials with `printf '...\n' > file`;
        // the trailing newline must be stripped so the secret matches
        // the non-newline value when used in HTTP Authorization.
        let id = CredentialId::new("trim-cred").expect("valid id");
        scrub_env(&id);
        let tmp = tempfile::tempdir().expect("tempdir");
        drop_credential_file(tmp.path(), id.as_str(), "trimmed-secret\n");
        let mut pool = pool_with_dir(tmp.path().to_path_buf());
        let s = pool.resolve_secret(&id).await.expect("must resolve");
        assert_eq!(
            s.expose_secret(),
            "trimmed-secret",
            "trailing newline must be stripped",
        );
    }

    #[tokio::test]
    #[serial]
    async fn resolve_secret_step2_reads_env_var() {
        // Step 2: GCIT_CREDENTIAL_<UPPER_SNAKE_ID>. Skip step 1 by
        // not setting CREDENTIALS_DIRECTORY; skip step 3 by leaving
        // the credentials file absent.
        let id = CredentialId::new("env-cred").expect("valid id");
        scrub_env(&id);
        let env_name = id.to_env_var();
        // SAFETY: serialized; single-thread mutation.
        unsafe {
            std::env::set_var(&env_name, "env-secret-value");
        }
        let mut pool = pool_with_dir(std::path::PathBuf::from("/nonexistent"));
        let s = pool.resolve_secret(&id).await.expect("env must resolve");
        assert_eq!(s.expose_secret(), "env-secret-value");
        // SAFETY: same lock as set_var above.
        unsafe {
            std::env::remove_var(&env_name);
        }
    }

    #[tokio::test]
    #[serial]
    async fn resolve_secret_step1_takes_precedence_over_step2_and_step3() {
        // Step 1 ($CREDENTIALS_DIRECTORY/<id>) MUST win over both
        // env var (step 2) and config-dir file (step 3) when present.
        // Pin the precedence so a regression that swaps the order
        // surfaces here.
        let id = CredentialId::new("prec-cred").expect("valid id");
        scrub_env(&id);
        let creds_dir_tmp = tempfile::tempdir().expect("creds_dir tempdir");
        let config_dir_tmp = tempfile::tempdir().expect("config_dir tempdir");
        // Step 1: drop credential at $CREDENTIALS_DIRECTORY/<id>.
        let cd_path = creds_dir_tmp.path().join(id.as_str());
        std::fs::write(&cd_path, "step1-wins").expect("write step1 file");
        let mut perms = std::fs::metadata(&cd_path).expect("stat").permissions();
        perms.set_mode(0o600);
        std::fs::set_permissions(&cd_path, perms).expect("chmod");
        // Step 3: drop credential at <config_dir>/credentials/<id>.
        drop_credential_file(config_dir_tmp.path(), id.as_str(), "step3-loses");
        let env_name = id.to_env_var();
        // SAFETY: serialized.
        unsafe {
            std::env::set_var("CREDENTIALS_DIRECTORY", creds_dir_tmp.path());
            std::env::set_var(&env_name, "step2-loses");
        }
        let mut pool = pool_with_dir(config_dir_tmp.path().to_path_buf());
        let s = pool.resolve_secret(&id).await.expect("must resolve");
        assert_eq!(
            s.expose_secret(),
            "step1-wins",
            "$CREDENTIALS_DIRECTORY must win over env var and config-dir file",
        );
        // SAFETY: serialized.
        unsafe {
            std::env::remove_var("CREDENTIALS_DIRECTORY");
            std::env::remove_var(&env_name);
        }
    }

    #[tokio::test]
    #[serial]
    async fn resolve_secret_caches_resolved_secret() {
        // Second lookup for the same id must hit the in-memory cache
        // rather than re-reading the file. Verify by reading once,
        // deleting the file, and reading again — the second read
        // succeeds with the same value because the secret is cached.
        let id = CredentialId::new("cache-cred").expect("valid id");
        scrub_env(&id);
        let tmp = tempfile::tempdir().expect("tempdir");
        let p = drop_credential_file(tmp.path(), id.as_str(), "cached-secret");
        let mut pool = pool_with_dir(tmp.path().to_path_buf());
        let s1 = pool.resolve_secret(&id).await.expect("first must resolve");
        assert_eq!(s1.expose_secret(), "cached-secret");
        // Delete the file. A reload-without-cache would now error;
        // the cache must serve the second lookup.
        std::fs::remove_file(&p).expect("remove file");
        let s2 = pool
            .resolve_secret(&id)
            .await
            .expect("second must hit cache");
        assert_eq!(s2.expose_secret(), "cached-secret");
    }

    #[tokio::test]
    #[serial]
    async fn resolve_secret_errors_when_no_resolution_step_succeeds() {
        let id = CredentialId::new("missing-cred").expect("valid id");
        scrub_env(&id);
        let tmp = tempfile::tempdir().expect("tempdir");
        // No file dropped; no env var set; CREDENTIALS_DIRECTORY scrubbed.
        let mut pool = pool_with_dir(tmp.path().to_path_buf());
        let err = pool
            .resolve_secret(&id)
            .await
            .expect_err("missing credential must error");
        // The error message must list every searched path so the
        // operator can fix the config without guessing which step
        // the daemon expected to find the credential at.
        assert!(
            err.contains(id.as_str()),
            "error must name the credential id; got: {err}",
        );
        assert!(
            err.contains("$CREDENTIALS_DIRECTORY"),
            "error must mention step 1 path; got: {err}",
        );
        assert!(
            err.contains(&id.to_env_var()),
            "error must mention step 2 env var; got: {err}",
        );
        assert!(
            err.contains("credentials"),
            "error must mention step 3 path; got: {err}",
        );
    }

    #[test]
    fn invalidate_except_drops_credentials_not_in_keep_set() {
        // CredentialPool's `invalidate_except` retains only the
        // credentials whose ids are in the keep set. The secrets BTreeMap
        // tracks all resolved values; pin its contents directly since
        // by_id requires constructing GithubCredentialResources (which
        // spawns a tokio task via acquire_github — heavy for a pure-
        // logic test).
        let mut pool = CredentialPool::default();
        let id_keep = CredentialId::new("keep-me").expect("valid id");
        let id_drop = CredentialId::new("drop-me").expect("valid id");
        pool.secrets.insert(
            id_keep.clone(),
            SecretString::from("keep-secret".to_string()),
        );
        pool.secrets.insert(
            id_drop.clone(),
            SecretString::from("drop-secret".to_string()),
        );
        let mut keep = BTreeSet::new();
        keep.insert(id_keep.clone());
        pool.invalidate_except(&keep);
        assert!(
            pool.secrets.contains_key(&id_keep),
            "kept credential must remain in secrets",
        );
        assert!(
            !pool.secrets.contains_key(&id_drop),
            "non-kept credential must be dropped from secrets",
        );
    }

    #[test]
    fn with_config_path_extracts_parent_directory_as_config_dir() {
        // The CredentialPool resolves step-3 credentials under
        // <config_dir>/credentials/<id>. `with_config_path(p)` sets
        // `config_dir` to the parent of the canonical form of `p`.
        // Use a known tempdir + config path inside it so the parent
        // matches even on /tmp realpath-traversal symlinks (TMPDIR).
        let tmp = tempfile::tempdir().expect("tempdir");
        let config_path = tmp.path().join("gcit.toml");
        std::fs::write(&config_path, "").expect("touch config file");
        let pool = CredentialPool::with_config_path(&config_path);
        let dir = pool
            .config_dir
            .expect("config_dir set when canonicalize succeeds");
        // Compare canonicalized parents because TempDir on macOS may
        // resolve symlinks differently than the original tmp.path().
        assert_eq!(
            dir.canonicalize().expect("canonicalize"),
            tmp.path().canonicalize().expect("tmp canonicalize"),
        );
    }

    #[tokio::test]
    async fn read_credential_file_returns_file_contents_on_success() {
        // Happy-path through the spawn_blocking + semaphore + timeout
        // wrapping. Drop a file with a known body, call
        // read_credential_file directly, and confirm the bytes round-
        // trip verbatim. The trim-trailing-newline behaviour lives in
        // `resolve_secret`, NOT in `read_credential_file` — this test
        // reads the raw file bytes back, so the contents include any
        // newline the producer wrote.
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("cred-success");
        std::fs::write(&path, "the-secret-bytes").expect("write file");
        let read_back = read_credential_file(&path)
            .await
            .expect("read must succeed");
        assert_eq!(read_back, "the-secret-bytes");
    }

    #[tokio::test]
    async fn read_credential_file_errors_when_path_does_not_exist() {
        // ENOENT path: the spawn_blocking task's inner
        // std::fs::read_to_string returns Err, and read_credential_file
        // formats it under "read {display}: {e}" so the operator can
        // see which file the daemon failed to open. Pin three things:
        //   1. error leads with the production-canonical "read " prefix
        //      so journald `grep '^read '` consumers stay accurate,
        //   2. the missing path is embedded in the message,
        //   3. the error is non-empty (smoke test for the format!).
        let tmp = tempfile::tempdir().expect("tempdir");
        let missing = tmp.path().join("does-not-exist");
        let err = read_credential_file(&missing)
            .await
            .expect_err("missing file must surface error");
        assert!(
            err.starts_with("read "),
            "error must lead with the production 'read ' prefix; got: {err}",
        );
        assert!(
            err.contains(missing.to_str().expect("utf8 path")),
            "error must name the missing path; got: {err}",
        );
    }

    #[tokio::test]
    #[serial]
    async fn resolve_secret_step3_rejects_permissive_mode_credentials_file() {
        // The 3-step chain shares the file-invariant probe with
        // `gcit check`: a credentials file with group/other access
        // bits (e.g. 0o644) fails `credential_file::probe` with
        // `Probe::Invariant(InvariantError::PermissiveMode { .. })`.
        // resolve_secret must surface the rendered invariant message
        // verbatim AND must NOT fall through to a later step that
        // could silently mask the misconfiguration. Pin both: the
        // error contains the canonical "must be 0600" guidance from
        // `InvariantError::render`, and the error names the
        // credential id for `gcit status` parity with operator-facing
        // tools.
        let id = CredentialId::new("permissive-mode-cred").expect("valid id");
        scrub_env(&id);
        let tmp = tempfile::tempdir().expect("tempdir");
        // Use the same path layout as `drop_credential_file`
        // (<dir>/credentials/<id>) but set mode 0o644 so the probe's
        // PermissiveMode invariant triggers.
        let creds = tmp.path().join("credentials");
        std::fs::create_dir_all(&creds).expect("create credentials dir");
        let p = creds.join(id.as_str());
        std::fs::write(&p, "permissive-mode-secret").expect("write credential file");
        let mut perms = std::fs::metadata(&p)
            .expect("stat credential file")
            .permissions();
        perms.set_mode(0o644);
        std::fs::set_permissions(&p, perms).expect("chmod credential file");
        let mut pool = pool_with_dir(tmp.path().to_path_buf());
        let err = pool
            .resolve_secret(&id)
            .await
            .expect_err("permissive-mode credential must reject");
        assert!(
            err.contains("must be 0600"),
            "error must surface the invariant guidance verbatim; got: {err}",
        );
        assert!(
            err.contains(id.as_str()),
            "error must name the credential id; got: {err}",
        );
    }
}

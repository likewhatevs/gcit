// LocalMailNotifier — appends to /var/mail/<user>.
//
// Pipeline (`on_run_complete`):
//   1. Check fire_on; skip if event missing.
//   2. Render subject + body via handlebars.
//   3. Sanitize subject (control bytes → spaces).
//   4. Enforce body byte cap (64 KiB); permanent on overflow.
//   5. Build mbox-formatted message bytes.
//   6. spawn_blocking → open /var/mail/<user> with O_NOFOLLOW +
//      O_APPEND, acquire fd_lock::RwLock::write() advisory exclusive
//      flock, signal lock-acquired via oneshot, write bytes, sync
//      via the Persist seam, drop guard (releases flock).
//
// Two-phase deadline split:
//
//   * LOCK_WAIT_DEADLINE bounds ONLY the flock-acquire phase. The
//     blocking task signals via a oneshot channel the moment the
//     guard is in hand; the outer `tokio::time::timeout` is wrapped
//     around the receiver, not around the task itself. If the
//     deadline elapses before lock-acquired, the notifier surfaces
//     `NotifyError::Transient` so backon retries on the next
//     supervisor cycle. (The blocking task continues to wait on
//     `lock.write()` in the tokio blocking pool until the holder
//     drops; this is an accepted leak that resolves when contention
//     clears.)
//
//   * After the oneshot signal, the post-lock write+sync runs
//     unbounded. A slow fsync on a contended disk does NOT surface
//     as a "lock timeout" once the spool bytes are committed — the
//     deadline applies to lock contention, not to durability.
//
// Cancellation:
//
//   * `cancel: &CancellationToken` is raced against the lock-wait
//     phase via a biased select! arm. Cancellation surfaces as
//     `Transient("cancelled")`. Once the lock is acquired the
//     notifier no longer races against cancel — the spool bytes
//     have to finish committing or the next reader will see a
//     half-written record.
//
// I/O happens inside `spawn_blocking` because:
//   - `fd_lock::RwLock::write()` is a blocking syscall (flock(LOCK_EX)).
//   - `File::sync_all` is a blocking syscall (fsync).
// Wrapping in spawn_blocking yields the tokio runtime so other
// tasks keep making progress while the lock is held.
//
// fsync goes through the `Persist` trait (`RealPersist` in
// production, recorder/fault-injecting impls in tests) so test
// suites can pin sync ordering and propagate sync_all failures
// without a real disk fault.

use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use fd_lock::RwLock as FdLock;
use handlebars::Handlebars;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::mbox::{self, BODY_BYTE_CAP};
use crate::config::{FireEvent, LocalMailTemplateConfig};
use crate::github::{self, RunSummary};
use crate::notify::{self, Notifier, NotifyError, NotifyOutcome, RunContext, SkipReason};

/// Deadline bounding the flock-acquire wait. Beyond this the
/// notifier returns `Transient` so backon retries on the next
/// supervisor cycle. Applies ONLY to the flock-wait phase — write
/// and sync run unbounded once the lock is held.
pub const LOCK_WAIT_DEADLINE: Duration = Duration::from_secs(5);

/// Default spool directory per Unix convention. Production callers
/// of `LocalMailNotifier::new` get this; integration tests can
/// redirect via `LocalMailNotifier::for_test`.
///
/// `pub` so the config validator (`config::validate::validate_local_mail`)
/// resolves the same default when probing spool writability. Single
/// source of truth: a future move (e.g. to `/var/spool/mail`)
/// propagates to both the notifier and the validator simultaneously.
pub const DEFAULT_SPOOL_DIR: &str = "/var/mail";

/// Why a candidate `local_mail.user` value failed the runtime defense
/// check at `LocalMailNotifier::new` / `for_test` construction.
///
/// Config-level validation (`config::validate::validate_local_mail`)
/// already restricts the user to `[A-Za-z0-9_-]`, which would catch
/// every variant below. This enum exists to defend against
/// programmatic construction paths that bypass the validator (a
/// future helper, a refactor, or test code that builds a notifier
/// from raw strings). Mirrors the credential-id defense-in-depth at
/// `config::credential::IdError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserError {
    /// User contains `/`. Would let the spool path escape `/var/mail`.
    ContainsSlash,
    /// User contains `..`. Path traversal up from `/var/mail`.
    ContainsParentDirectory,
    /// User contains a NUL byte. Path APIs treat NUL as a string
    /// terminator on the C side; rejecting it here keeps the
    /// `<spool_dir>/<user>` join unambiguous.
    ContainsNul,
}

impl UserError {
    /// Operator-facing description. Used by the supervisor's
    /// `build_notifiers` error chain when surfacing as
    /// `last_error`.
    pub fn message(&self) -> &'static str {
        match self {
            UserError::ContainsSlash => {
                "local_mail.user contains '/' (path traversal); use only A-Z, a-z, 0-9, '_', and '-'"
            }
            UserError::ContainsParentDirectory => {
                "local_mail.user contains '..' (path traversal); use only A-Z, a-z, 0-9, '_', and '-'"
            }
            UserError::ContainsNul => {
                "local_mail.user contains a NUL byte; use only A-Z, a-z, 0-9, '_', and '-'"
            }
        }
    }
}

impl std::fmt::Display for UserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for UserError {}

/// Defense-in-depth check on the `local_mail.user` value. Rejects
/// substrings that would let the per-user spool path
/// `<spool_dir>/<user>` escape `<spool_dir>`. The order of checks is
/// chosen so the most-specific message surfaces first: `..` is
/// reported as ParentDirectory rather than as the implied two
/// invalid characters; `/` and NUL each get their own arm.
fn validate_user(user: &str) -> Result<(), UserError> {
    if user.contains('\0') {
        return Err(UserError::ContainsNul);
    }
    if user.contains("..") {
        return Err(UserError::ContainsParentDirectory);
    }
    if user.contains('/') {
        return Err(UserError::ContainsSlash);
    }
    Ok(())
}

/// Per-destination local_mail notifier. One instance per
/// `[[flow.destination]]` of `kind = "local_mail"`.
///
/// `persist` is the seam that decouples the I/O path from the
/// fsync(2) call. Production wires `RealPersist` (delegates to
/// `File::sync_all`); test fixtures inject recorders or fault
/// generators via `for_test_with_persist` to pin invocation count,
/// ordering, and error propagation.
pub struct LocalMailNotifier {
    id: String,
    user: String,
    hostname: Arc<String>,
    fire_on: Vec<FireEvent>,
    template: LocalMailTemplateConfig,
    handlebars: Arc<Handlebars<'static>>,
    spool_dir: PathBuf,
    persist: Arc<dyn Persist>,
}

impl LocalMailNotifier {
    /// Construct a notifier from validated config. `hostname` is
    /// shared via `Arc<String>` because every flow's local_mail
    /// destination wants the same value (set once at daemon
    /// startup via `mbox::read_hostname_or_default`).
    ///
    /// The spool directory is fixed at `/var/mail` (the Unix
    /// convention); the per-user spool file is `/var/mail/<user>`.
    /// Tests that need to redirect writes to a tempdir use
    /// `for_test` instead.
    ///
    /// Returns `Err(UserError)` when `user` contains `/`, `..`, or a
    /// NUL byte. Production callers see this only on a programmer
    /// bug (the config validator already restricts the charset);
    /// the check is defense-in-depth so programmatic construction
    /// paths cannot escape the per-user spool.
    pub fn new(
        id: impl Into<String>,
        user: impl Into<String>,
        hostname: Arc<String>,
        fire_on: Vec<FireEvent>,
        template: LocalMailTemplateConfig,
        handlebars: Arc<Handlebars<'static>>,
    ) -> Result<Self, UserError> {
        let user = user.into();
        validate_user(&user)?;
        Ok(Self {
            id: id.into(),
            user,
            hostname,
            fire_on,
            template,
            handlebars,
            spool_dir: PathBuf::from(DEFAULT_SPOOL_DIR),
            persist: Arc::new(RealPersist),
        })
    }

    /// Test-only constructor that redirects the spool path from
    /// `/var/mail/<user>` to `<spool_dir>/<user>`. Integration
    /// tests pass a `tempfile::TempDir` so the write hits a fixture
    /// the test creates and tears down rather than the system spool.
    ///
    /// Mirrors `gcit::discord::webhook::Client::for_test` — kept
    /// `pub` (not `#[cfg(test)]`) so integration test crates,
    /// which compile separately from `#[cfg(test)]`, can reach it.
    /// `#[doc(hidden)]` keeps it out of rustdoc surfaces.
    ///
    /// The same defense-in-depth user check as `new` runs here:
    /// a test fixture that passes `/` or `..` is a bug in the test,
    /// not a probe of the production check. Tests exercising the
    /// production check use `validate_user` directly via the unit
    /// tests in this module.
    ///
    /// Uses `RealPersist` for fsync — tests that want to inject a
    /// recorder or fault impl call `for_test_with_persist` instead.
    #[doc(hidden)]
    pub fn for_test(
        id: impl Into<String>,
        user: impl Into<String>,
        hostname: Arc<String>,
        fire_on: Vec<FireEvent>,
        template: LocalMailTemplateConfig,
        handlebars: Arc<Handlebars<'static>>,
        spool_dir: PathBuf,
    ) -> Result<Self, UserError> {
        Self::for_test_with_persist(
            id,
            user,
            hostname,
            fire_on,
            template,
            handlebars,
            spool_dir,
            Arc::new(RealPersist),
        )
    }

    /// Test-only constructor that overrides the `Persist` impl. Use
    /// when a test needs to record fsync invocations, observe
    /// ordering against the flock guard's drop, or inject a sync
    /// failure.
    ///
    /// `#[doc(hidden)] pub` mirrors the `for_test` pattern: callable
    /// from integration tests, hidden from rustdoc.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn for_test_with_persist(
        id: impl Into<String>,
        user: impl Into<String>,
        hostname: Arc<String>,
        fire_on: Vec<FireEvent>,
        template: LocalMailTemplateConfig,
        handlebars: Arc<Handlebars<'static>>,
        spool_dir: PathBuf,
        persist: Arc<dyn Persist>,
    ) -> Result<Self, UserError> {
        let user = user.into();
        validate_user(&user)?;
        Ok(Self {
            id: id.into(),
            user,
            hostname,
            fire_on,
            template,
            handlebars,
            spool_dir,
            persist,
        })
    }

    fn fires_on(&self, event: FireEvent) -> bool {
        self.fire_on.contains(&event)
    }

    /// Path of the spool file. Production: `/var/mail/<user>` per
    /// the Unix convention (gcit does NOT auto-create it). Tests
    /// using `for_test` get `<spool_dir>/<user>`.
    fn spool_path(&self) -> PathBuf {
        self.spool_dir.join(&self.user)
    }
}

impl Notifier for LocalMailNotifier {
    fn kind(&self) -> &'static str {
        "local_mail"
    }

    fn id(&self) -> &str {
        &self.id
    }

    async fn on_run_complete(
        &self,
        ctx: &RunContext,
        summary: &RunSummary,
        cancel: &CancellationToken,
    ) -> Result<NotifyOutcome, NotifyError> {
        if !self.fires_on(FireEvent::RunComplete) {
            return Ok(NotifyOutcome::Skipped {
                reason: SkipReason::FireOnMismatch,
            });
        }

        // Early bail-out for an already-cancelled token. Skipping
        // this check would still produce Transient at the select!
        // arm below, but only AFTER spawn_blocking has been kicked
        // off — and spawn_blocking is uncancellable, so the OS
        // thread proceeds to acquire the (uncontended) lock and
        // write to the spool. A retry on the next supervisor cycle
        // would then duplicate that write. Returning here BEFORE
        // any spool I/O preserves the invariant: pre-cancel means
        // no bytes touch /var/mail.
        if cancel.is_cancelled() {
            return Err(NotifyError::Transient {
                source: anyhow::anyhow!(
                    "cancelled before lock acquired on {}",
                    self.spool_path().display(),
                ),
                retry_after: None,
            });
        }

        let data = notify::render_context(ctx, summary);

        // Render subject. Default: "[gcit] <flow> <conclusion>".
        let subject =
            match self.template.subject.as_deref() {
                Some(t) => self.handlebars.render_template(t, &data).map_err(|e| {
                    NotifyError::Permanent {
                        source: anyhow::anyhow!("subject render failed: {e}"),
                    }
                })?,
                None => {
                    let label = summary
                        .conclusion
                        .map(github::label_for)
                        .unwrap_or("complete");
                    format!("[gcit] {} {}", ctx.flow_name, label)
                }
            };

        // Render body. Default: structured plaintext with the
        // operator-relevant fields.
        let body =
            match self.template.body.as_deref() {
                Some(t) => self.handlebars.render_template(t, &data).map_err(|e| {
                    NotifyError::Permanent {
                        source: anyhow::anyhow!("body render failed: {e}"),
                    }
                })?,
                None => default_body(ctx, summary),
            };

        // Body byte cap. Permanent — don't retry; surface the
        // cap-relief guidance so operators see how to fix it in
        // `gcit status`.
        if body.len() > BODY_BYTE_CAP {
            warn!(
                notifier = %self.id,
                rendered_len = body.len(),
                cap = BODY_BYTE_CAP,
                "local_mail body exceeds cap; dropping",
            );
            return Err(NotifyError::Permanent {
                source: anyhow::anyhow!(
                    "rendered body is {} bytes; cap is {} bytes; reduce template inputs or split the flow into smaller flows",
                    body.len(),
                    BODY_BYTE_CAP,
                ),
            });
        }

        let now = Utc::now();
        let formatted = mbox::format_message(now, &self.user, &self.hostname, &subject, &body);
        let spool_path = self.spool_path();

        // Two-phase deadline. The blocking task signals via
        // `phase_tx` the moment the flock is acquired; we wrap
        // ONLY the receiver in `tokio::time::timeout`, so the
        // deadline applies to the lock-wait phase only. After the
        // signal, the blocking task continues with write + sync
        // unbounded.
        let bytes = formatted.into_bytes();
        let path_clone = spool_path.clone();
        let persist = Arc::clone(&self.persist);
        let (phase_tx, phase_rx) = oneshot::channel::<()>();
        let blocking = tokio::task::spawn_blocking(move || {
            write_with_lock(&path_clone, &bytes, persist.as_ref(), Some(phase_tx))
        });

        // Race lock-wait against cancel. Cancellation only aborts
        // the lock-wait phase — once the lock is acquired, the
        // notifier no longer races against cancel because the
        // spool bytes must finish committing.
        //
        // `biased` ordering puts phase_rx FIRST so that when both
        // arms are simultaneously ready (lock acquired at the same
        // tokio poll cycle as cancel firing), phase_rx wins and we
        // proceed to await the blocking task's write+sync. This
        // closes the duplicate-message race: if the lock IS
        // acquired the helper has already sent phase_tx and is
        // committed to write the bytes; surfacing Cancelled here
        // would let the supervisor record a Transient and retry on
        // the next cycle, producing a duplicate spool entry once
        // the original blocking task finishes its write. With
        // phase_rx first, cancel only wins when the lock is NOT
        // yet acquired and no bytes have been written.
        let phase_outcome = tokio::select! {
            biased;
            res = tokio::time::timeout(LOCK_WAIT_DEADLINE, phase_rx) => res,
            _ = cancel.cancelled() => {
                // The blocking task is uncancellable: the OS
                // thread continues to wait on `lock.write()` after
                // we surface Cancelled here, and if the holder
                // releases before LOCK_WAIT_DEADLINE elapses the
                // task will still acquire and write the bytes.
                // Surface this in the message so an operator
                // staring at last_error knows a stray spool entry
                // may appear post-cancel.
                return Err(NotifyError::Transient {
                    source: anyhow::anyhow!(
                        "cancelled before lock acquired on {}; the blocking task may still write if the lock becomes available before the deadline",
                        spool_path.display(),
                    ),
                    retry_after: None,
                });
            }
        };

        match phase_outcome {
            Ok(Ok(())) => {
                // Lock acquired. Wait for write+sync unbounded.
                match blocking.await {
                    Ok(Ok(())) => Ok(NotifyOutcome::Sent {
                        receipt: format!("file:{}", spool_path.display()),
                    }),
                    Ok(Err(WriteError::Io(e))) => Err(map_io_error(e, &spool_path)),
                    // LockTimeout cannot reach the post-acquire
                    // arm: the helper only returns it when the
                    // open or try_write fails before the signal.
                    Ok(Err(WriteError::LockTimeout)) => Err(NotifyError::Transient {
                        source: anyhow::anyhow!(
                            "spool lock not acquired within {:?}",
                            LOCK_WAIT_DEADLINE,
                        ),
                        retry_after: None,
                    }),
                    Err(join_err) => Err(NotifyError::Transient {
                        source: anyhow::anyhow!("blocking task join failed: {join_err}"),
                        retry_after: None,
                    }),
                }
            }
            Ok(Err(_dropped)) => {
                // phase_tx dropped without sending — the blocking
                // task errored before the lock was acquired (e.g.
                // open(2) failed). Await the join to surface the
                // underlying io::Error.
                match blocking.await {
                    Ok(Err(WriteError::Io(e))) => Err(map_io_error(e, &spool_path)),
                    Ok(Err(WriteError::LockTimeout)) => Err(NotifyError::Transient {
                        source: anyhow::anyhow!(
                            "spool lock not acquired within {:?}",
                            LOCK_WAIT_DEADLINE,
                        ),
                        retry_after: None,
                    }),
                    Ok(Ok(())) => Err(NotifyError::Transient {
                        source: anyhow::anyhow!(
                            "blocking task signaled phase-error but reported success",
                        ),
                        retry_after: None,
                    }),
                    Err(join_err) => Err(NotifyError::Transient {
                        source: anyhow::anyhow!("blocking task join failed: {join_err}"),
                        retry_after: None,
                    }),
                }
            }
            Err(_elapsed) => Err(NotifyError::Transient {
                source: anyhow::anyhow!("spool lock not acquired within {:?}", LOCK_WAIT_DEADLINE,),
                retry_after: None,
            }),
        }
    }
}

/// Default body when the operator hasn't configured a `body`
/// template. Plain-text summary with the operator-relevant fields.
fn default_body(ctx: &RunContext, summary: &RunSummary) -> String {
    let conclusion = summary
        .conclusion
        .map(github::label_for)
        .unwrap_or("(in progress)");
    let mut s = String::new();
    s.push_str(&format!("flow:       {}\n", ctx.flow_name));
    s.push_str(&format!("source:     {}\n", ctx.source.url));
    s.push_str(&format!("ref:        {}\n", ctx.source.ref_name));
    s.push_str(&format!("sha:        {}\n", ctx.source.sha_short));
    s.push_str(&format!("repo:       {}\n", ctx.action.repo));
    s.push_str(&format!("workflow:   {}\n", ctx.action.workflow));
    s.push_str(&format!("run id:     {}\n", summary.run_id));
    s.push_str(&format!("run url:    {}\n", summary.run_url));
    s.push_str(&format!("attempt:    {}\n", summary.run_attempt));
    s.push_str(&format!("conclusion: {}\n", conclusion));
    s.push('\n');
    s.push_str(&format!("jobs ({}):\n", summary.jobs.len()));
    for j in &summary.jobs {
        let jc = j
            .conclusion
            .map(github::label_for)
            .unwrap_or("(in progress)");
        s.push_str(&format!("  - {} [{}] {}\n", j.name, jc, j.html_url));
    }
    s
}

/// Outcome of `write_with_lock`. Carries the same shape as the old
/// inner-deadline design: `LockTimeout` survives as a defensive
/// variant (the production caller's two-phase deadline never
/// produces it any more, but a future single-phase caller could).
/// Every other io error rides on `Io(io::Error)` so the caller
/// classifies via `map_io_error`.
///
/// `#[doc(hidden)] pub` so integration tests can build the seam in
/// isolation without exposing the type in rustdoc.
#[doc(hidden)]
#[derive(Debug)]
pub enum WriteError {
    /// Reserved for callers that want to surface a lock-wait
    /// timeout from inside the helper. The production
    /// `on_run_complete` does NOT pass a phase signal that fires
    /// this — it bounds the lock-wait phase from outside via
    /// `tokio::time::timeout(phase_rx)` instead — but the variant
    /// is kept so the caller's match is exhaustive even if a
    /// future restructure brings inner deadlines back.
    LockTimeout,
    /// Any io::Error from open / write_all / sync. Routed through
    /// `map_io_error` for Permanent vs Transient classification.
    Io(std::io::Error),
}

/// Test seam for the post-lock fsync(2) call. Production uses
/// `RealPersist`, which calls `std::fs::File::sync_all`. Tests
/// implement their own to record invocation count, observe
/// ordering (write_all → sync_all → drop), or inject failures.
///
/// `#[doc(hidden)] pub` mirrors the `LocalMailNotifier::for_test`
/// test-seam pattern: callable from integration tests, hidden from
/// rustdoc.
///
/// `'static + Send + Sync` so the trait object can be stored as
/// `Arc<dyn Persist>` on the notifier and cloned across the
/// spawn_blocking boundary.
#[doc(hidden)]
pub trait Persist: Send + Sync + 'static {
    /// Persist the spool file's data and metadata. Must succeed
    /// before the lock guard drops or cooperating readers may see
    /// half-written bytes after a crash.
    fn sync_all(&self, file: &std::fs::File) -> std::io::Result<()>;
}

/// Production `Persist`: delegates to `std::fs::File::sync_all`,
/// which calls fsync(2) on Linux and forces the file's data and
/// metadata to the storage device before returning.
#[doc(hidden)]
#[derive(Default, Clone)]
pub struct RealPersist;

impl Persist for RealPersist {
    fn sync_all(&self, file: &std::fs::File) -> std::io::Result<()> {
        file.sync_all()
    }
}

/// Open `path` with O_NOFOLLOW + O_APPEND, take an advisory
/// exclusive flock via fd-lock, signal lock-acquired via
/// `phase_tx` (if provided), write `bytes`, fsync via the Persist
/// seam, drop the guard (releases flock).
///
/// O_NOFOLLOW refuses any open whose final path component is a
/// symlink, so a local attacker who can write inside the spool dir
/// can't redirect the write via a symlink.
///
/// Lock acquisition uses the blocking `lock.write()` call —
/// `flock(LOCK_EX)` — which blocks the OS thread until the holder
/// releases. The caller bounds the lock-wait phase from outside
/// via `tokio::time::timeout` wrapped around the receiver of
/// `phase_tx`; the OS thread continues to wait until the holder
/// drops, but the tokio task that awaits the result has already
/// returned a Transient error to the caller. This is an accepted
/// blocking-pool leak that resolves when contention clears.
///
/// `phase_tx` is a one-shot channel sender. The helper sends
/// `()` immediately after `lock.write()` returns the guard. When
/// `None`, the helper runs the full pipeline (open → flock →
/// write → sync) without phase signalling — useful for tests that
/// drive the helper directly without a tokio runtime.
///
/// `#[doc(hidden)] pub` so integration tests can drive the helper
/// directly with custom `Persist` implementations.
#[doc(hidden)]
pub fn write_with_lock(
    path: &std::path::Path,
    bytes: &[u8],
    persist: &dyn Persist,
    phase_tx: Option<oneshot::Sender<()>>,
) -> Result<(), WriteError> {
    use std::io::Write;
    let file = std::fs::OpenOptions::new()
        .create(false)
        .append(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(WriteError::Io)?;
    let mut lock = FdLock::new(file);
    let mut guard = lock.write().map_err(WriteError::Io)?;
    // Lock acquired — signal the receiver. If the receiver was
    // dropped (caller already returned an error to the operator),
    // the send returns Err and we silently ignore: the helper has
    // no other way to surface the racing-drop, and continuing
    // with write+sync is safe — the spool bytes still need to
    // finish committing for the next reader.
    if let Some(tx) = phase_tx {
        let _ = tx.send(());
    }
    // Lock acquired. write_all + sync run unbounded.
    guard.write_all(bytes).map_err(WriteError::Io)?;
    persist.sync_all(&guard).map_err(WriteError::Io)?;
    drop(guard);
    Ok(())
}

/// Map an io::Error into `NotifyError`. Permanent for failure modes
/// no operator-free retry can resolve (missing spool, mode bits,
/// symlink, directory at the path, programmer-supplied bad args).
/// Everything else is Transient so backon retries on the next
/// supervisor cycle.
///
/// Errno mapping (Linux):
///   ENOENT (NotFound)             → Permanent (no auto-create)
///   EACCES, EPERM (PermissionDenied) → Permanent (mode/ACL/cap)
///   ELOOP (raw_os_error)          → Permanent (symlink, O_NOFOLLOW)
///   EISDIR (raw_os_error)         → Permanent (path is a directory)
///   EINVAL (raw_os_error)         → Permanent (programmer error)
///   *                             → Transient (ENOSPC, EIO, EBUSY, EINTR, EAGAIN, ...)
///
/// `#[doc(hidden)] pub` so integration tests can pin the
/// classification table without reaching into private surface.
/// Mirrors the `LocalMailNotifier::for_test` test-seam pattern.
#[doc(hidden)]
pub fn map_io_error(err: std::io::Error, path: &std::path::Path) -> NotifyError {
    use std::io::ErrorKind;
    match err.kind() {
        ErrorKind::NotFound => NotifyError::Permanent {
            source: anyhow::anyhow!(
                "spool file {} does not exist; create it via `useradd`/`mailx`/`touch {}`",
                path.display(),
                path.display(),
            ),
        },
        ErrorKind::PermissionDenied => NotifyError::Permanent {
            source: anyhow::anyhow!(
                "permission denied writing to {}; ensure BindPaths=/var/mail in the gcit unit and the daemon's user is in the mail group",
                path.display(),
            ),
        },
        // ELOOP from O_NOFOLLOW on a symlink target.
        // ErrorKind::FilesystemLoop is unstable; check raw os
        // error instead.
        _ if err.raw_os_error() == Some(libc::ELOOP) => NotifyError::Permanent {
            source: anyhow::anyhow!(
                "{} is a symlink; refused via O_NOFOLLOW. Resolve and retry.",
                path.display(),
            ),
        },
        // EISDIR: path resolves to a directory. Operator
        // misconfigured the spool path (the `<user>` is somehow a
        // directory). Retrying without operator action will not
        // help.
        _ if err.raw_os_error() == Some(libc::EISDIR) => NotifyError::Permanent {
            source: anyhow::anyhow!(
                "{} is a directory; spool path must be a regular file. Verify the destination user and the /var/mail layout.",
                path.display(),
            ),
        },
        // EINVAL: the open call rejected its arguments. With a
        // fixed argument set in `write_with_lock`, EINVAL signals a
        // programmer-side bug or a filesystem that doesn't accept
        // O_APPEND / O_NOFOLLOW. Retrying yields the same
        // rejection.
        _ if err.raw_os_error() == Some(libc::EINVAL) => NotifyError::Permanent {
            source: anyhow::anyhow!(
                "open({}) returned EINVAL; the underlying filesystem rejected the open flags. Verify /var/mail's filesystem supports O_APPEND + O_NOFOLLOW.",
                path.display(),
            ),
        },
        _ => NotifyError::Transient {
            source: anyhow::anyhow!("io error on {}: {err}", path.display()),
            retry_after: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::github::{Conclusion, JobResult, RunStatus};
    use chrono::Utc;
    use gix_hash::ObjectId;

    fn handlebars() -> Arc<Handlebars<'static>> {
        Arc::new(notify::strict_handlebars())
    }

    fn ctx() -> RunContext {
        RunContext {
            flow_name: "myflow".into(),
            flow_description: None,
            source: crate::notify::SourceInfo {
                url: "https://example.com/r.git".into(),
                ref_name: "refs/heads/main".into(),
                sha: ObjectId::null(gix_hash::Kind::Sha1),
                sha_short: "0000000".into(),
            },
            action: crate::notify::ActionInfo {
                repo: "owner/repo".into(),
                workflow: "ci.yml".into(),
                run_id: 1,
                run_url: "https://example.com".into(),
                dispatched_at: Utc::now(),
            },
            gcit_run_id: uuid::Uuid::nil(),
        }
    }

    fn summary(c: Conclusion, jobs: usize) -> RunSummary {
        RunSummary {
            run_id: 1,
            run_url: "https://example.com".into(),
            run_number: 1,
            run_attempt: 1,
            status: RunStatus::Completed,
            conclusion: Some(c),
            started_at: Some(Utc::now()),
            completed_at: Some(Utc::now()),
            jobs: (0..jobs)
                .map(|i| JobResult {
                    job_id: i as u64,
                    name: format!("job-{i}"),
                    html_url: format!("https://example.com/{i}"),
                    conclusion: Some(Conclusion::Success),
                    started_at: Some(Utc::now()),
                    completed_at: Some(Utc::now()),
                    steps: Vec::new(),
                    run_attempt: 1,
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn skips_when_fire_on_missing() {
        let n = LocalMailNotifier::new(
            "test",
            "user1",
            Arc::new("host1".to_string()),
            vec![FireEvent::JobComplete], // RunComplete missing
            LocalMailTemplateConfig::default(),
            handlebars(),
        )
        .expect("user 'user1' is valid");
        let cancel = CancellationToken::new();
        let outcome = n
            .on_run_complete(&ctx(), &summary(Conclusion::Success, 0), &cancel)
            .await;
        match outcome.unwrap() {
            NotifyOutcome::Skipped {
                reason: SkipReason::FireOnMismatch,
            } => {}
            other => panic!("expected FireOnMismatch, got {other:?}"),
        }
    }

    #[test]
    fn default_body_includes_all_fields() {
        let body = default_body(&ctx(), &summary(Conclusion::Failure, 2));
        assert!(body.contains("flow:       myflow"));
        assert!(body.contains("conclusion: failure"));
        assert!(body.contains("jobs (2):"));
        assert!(body.contains("job-0"));
        assert!(body.contains("job-1"));
    }

    #[test]
    fn lock_wait_deadline_pinned_at_5s() {
        assert_eq!(LOCK_WAIT_DEADLINE, Duration::from_secs(5));
    }

    #[test]
    fn map_io_error_classifies_enoent_as_permanent() {
        let err = std::io::Error::from(std::io::ErrorKind::NotFound);
        let path = PathBuf::from("/var/mail/nobody");
        let mapped = map_io_error(err, &path);
        assert!(!mapped.is_transient(), "ENOENT must be Permanent");
    }

    #[test]
    fn map_io_error_classifies_eacces_as_permanent() {
        let err = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        let path = PathBuf::from("/var/mail/u");
        let mapped = map_io_error(err, &path);
        assert!(!mapped.is_transient(), "EACCES must be Permanent");
    }

    #[test]
    fn map_io_error_classifies_other_as_transient() {
        // ENOSPC, EIO etc. → Transient. We can't easily create a
        // raw_os_error variant, so use a kind-based one.
        let err = std::io::Error::from(std::io::ErrorKind::Other);
        let path = PathBuf::from("/var/mail/u");
        let mapped = map_io_error(err, &path);
        assert!(mapped.is_transient(), "Other io errors must be Transient");
    }

    #[tokio::test]
    async fn body_cap_overflow_is_permanent() {
        // Construct a notifier with a body template that renders to
        // > 64 KiB. The default body fits comfortably, so we configure
        // a custom template.
        let big = format!("{{{{repeat}}}}{}", "x".repeat(BODY_BYTE_CAP + 100));
        let n = LocalMailNotifier::new(
            "test",
            "u",
            Arc::new("h".to_string()),
            vec![FireEvent::RunComplete],
            LocalMailTemplateConfig {
                subject: None,
                body: Some(big),
            },
            handlebars(),
        )
        .expect("user 'u' is valid");
        // Strict-mode handlebars rejects {{repeat}} undefined → we
        // get a Permanent render error before the cap check fires.
        // Test the cap path via a no-template body that's literal.
        let lit_body = "x".repeat(BODY_BYTE_CAP + 100);
        let n2 = LocalMailNotifier::new(
            "test",
            "u",
            Arc::new("h".to_string()),
            vec![FireEvent::RunComplete],
            LocalMailTemplateConfig {
                subject: None,
                body: Some(lit_body),
            },
            handlebars(),
        )
        .expect("user 'u' is valid");
        // Both should fail Permanent. Run the second one; it doesn't
        // touch /var/mail because the cap check fires first.
        let cancel = CancellationToken::new();
        let result = n2
            .on_run_complete(&ctx(), &summary(Conclusion::Success, 0), &cancel)
            .await;
        match result {
            Err(NotifyError::Permanent { source }) => {
                let msg = format!("{source}");
                assert!(msg.contains("cap"), "msg: {msg}");
                // Operator guidance must surface so they see how to
                // fix it.
                assert!(
                    msg.contains("reduce template inputs") || msg.contains("smaller flows"),
                    "missing cap-relief guidance: {msg}",
                );
            }
            other => panic!("expected Permanent, got {other:?}"),
        }
        // Suppress unused warning on `n`.
        let _ = n;
    }

    // ---- defense-in-depth user-validation tests ----
    //
    // These exercise the runtime check at `LocalMailNotifier::new` /
    // `for_test`. Config-level validation already gates the same
    // values; the unit tests here cover the runtime layer in
    // isolation so a regression that drops the check would fail
    // here even if the config validator's tests still pass.

    #[test]
    fn validate_user_accepts_valid_unix_usernames() {
        for ok in ["u", "ops", "alice", "U_user", "user-name", "abc123"] {
            assert!(
                validate_user(ok).is_ok(),
                "{ok:?} must pass the runtime defense check",
            );
        }
    }

    #[test]
    fn validate_user_rejects_slash() {
        let err = validate_user("etc/passwd").unwrap_err();
        assert_eq!(err, UserError::ContainsSlash);
        // Operator-facing message must explain why.
        assert!(err.to_string().contains('/'));
        assert!(err.to_string().contains("path traversal"));
    }

    #[test]
    fn validate_user_rejects_parent_directory_traversal() {
        // The `..` substring is the path-traversal signature; reject
        // it regardless of surrounding chars.
        let err = validate_user("..").unwrap_err();
        assert_eq!(err, UserError::ContainsParentDirectory);
        let err = validate_user("foo..bar").unwrap_err();
        assert_eq!(err, UserError::ContainsParentDirectory);
        // Single dot is fine — only `..` is the traversal signature.
        // (config-level validation rejects `.` anyway via the
        // [A-Za-z0-9_-] charset rule, but the runtime check is
        // narrower-by-design.)
        assert!(validate_user("foo.bar").is_ok());
    }

    #[test]
    fn validate_user_rejects_nul_byte() {
        let err = validate_user("foo\0bar").unwrap_err();
        assert_eq!(err, UserError::ContainsNul);
        assert!(err.to_string().contains("NUL"));
    }

    #[test]
    fn validate_user_check_order_nul_first() {
        // A user containing both NUL and `..` reports ContainsNul:
        // the NUL byte is a stronger signal (path-API termination
        // ambiguity) and gets the most-specific message.
        let err = validate_user("a\0..b").unwrap_err();
        assert_eq!(err, UserError::ContainsNul);
    }

    #[test]
    fn new_returns_err_for_traversal_user() {
        // Bypass the validator: construct directly with a hostile
        // user. The runtime check fires before the struct is built.
        // (Use match rather than `unwrap_err` because
        // `LocalMailNotifier` does not derive `Debug` — adding it
        // just for tests would expand the public surface.)
        let result = LocalMailNotifier::new(
            "test",
            "../etc/passwd",
            Arc::new("h".to_string()),
            vec![FireEvent::RunComplete],
            LocalMailTemplateConfig::default(),
            handlebars(),
        );
        match result {
            Ok(_) => panic!("traversal user must be rejected at construction"),
            Err(e) => assert_eq!(e, UserError::ContainsParentDirectory),
        }
    }

    #[test]
    fn for_test_returns_err_for_traversal_user() {
        // Same defense applies to the test-only constructor: a test
        // fixture passing `/` is a bug in the test, not the
        // production check.
        let tmp = std::env::temp_dir();
        let result = LocalMailNotifier::for_test(
            "test",
            "ops/x",
            Arc::new("h".to_string()),
            vec![FireEvent::RunComplete],
            LocalMailTemplateConfig::default(),
            handlebars(),
            tmp,
        );
        match result {
            Ok(_) => panic!("slash user must be rejected at for_test construction"),
            Err(e) => assert_eq!(e, UserError::ContainsSlash),
        }
    }
}

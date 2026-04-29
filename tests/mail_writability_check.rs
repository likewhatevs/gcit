// Spool writability check at config-load time.
//
// Production state: src/config/validate.rs::validate_local_mail
// (line 968) currently performs charset, length, and missing-user
// checks. It does NOT probe the spool path for writability — the
// `/var/mail/<user>` resolved path is never accessed from the
// validator. Operators learn that a spool isn't writable only at
// the first append attempt at runtime, not at `gcit check` /
// daemon startup.
//
// Every test below depends on a writability-check feature that is
// not yet implemented (see linked production-change task in the
// per-stub `#[ignore]` reason). Skeletons are retained as
// guardrails: they pin the operator-facing error messages, the
// destination-collection semantics, and the W_OK-not-open
// invariant the implementation should adopt.
//
// When the production feature lands (validator gains a writability
// probe + a spool_root override hook for tests), each `#[ignore]`
// can be removed.

#[test]
#[ignore = "spool writability check not implemented in src/config/validate.rs::validate_local_mail; activate once the writability probe + tempdir spool_root override land"]
fn writable_spool_passes_validation() {
    // Setup: tempdir/<user> writable.
    // Validator-with-spool-root-override invoked.
    // Assert: load_str returns Ok.
    //
    // Mutation target (once active): a probe that misclassifies a
    // mode-0660 spool as non-writable would surface a false
    // ConfigError::Validate.
}

#[test]
#[ignore = "spool writability check not implemented in src/config/validate.rs::validate_local_mail; activate once the writability probe lands"]
fn nonwritable_spool_fails_validation_with_clear_error() {
    // Setup: tempdir/<user> mode 0444 (read-only).
    // Validator-with-spool-root-override invoked.
    // Assert: ConfigError::Validate naming destination.local_mail.user
    // with message containing the path AND remediation
    // ("chmod 0660 ... mail group ... ReadWritePaths=/var/mail").
    //
    // The validator should produce a structured suggestion sized to
    // the failure mode (mode bits vs ownership vs ReadWritePaths)
    // — pinning the substrings here defends against vague catch-all
    // wording.
}

#[test]
#[ignore = "spool writability check not implemented; activate once the validator distinguishes ENOENT-on-spool from EACCES"]
fn missing_spool_fails_validation_with_useradd_suggestion() {
    // Setup: tempdir without a spool file at all.
    // Validator. Assert: ConfigError::Validate with suggestion
    // mentioning `useradd`, `mailx`, or `touch <path> + chown
    // <user>:mail + chmod 0660 <path>` so operators can copy the
    // remediation.
    //
    // Mutation target (once active): a probe that only checks
    // access(W_OK) without distinguishing ENOENT — operator gets
    // "permission denied" when the file simply isn't there.
}

#[test]
#[ignore = "spool writability check not implemented; activate once the validator distinguishes /var/mail-parent ENOENT from spool ENOENT"]
fn parent_dir_missing_fails_validation_with_install_mail_suggestion() {
    // Setup: tempdir without any /var/mail subdir.
    // Validator. Assert: ConfigError::Validate with suggestion
    // pointing at installing a mail package (mailutils, postfix)
    // OR `sudo mkdir -m 0755 /var/mail`.
    //
    // Rare on standard Linux distros (most pre-create /var/mail)
    // but plausible on stripped container images.
}

#[test]
#[ignore = "spool writability check not implemented; activate once the validator uses access(W_OK) (or fs::metadata + permissions) rather than open+close"]
fn writability_uses_w_ok_access_check_not_actual_open() {
    // The validator must probe writability without opening or
    // touching the spool's mtime. Operators rely on mtime to
    // detect "new mail" via mailx clients; a startup probe that
    // bumps mtime triggers false-positive new-mail alerts.
    //
    // Pin: pre-record the spool file's mtime, run the validator,
    // assert mtime is unchanged.
}

#[test]
#[ignore = "spool writability check not implemented; activate once the validator iterates per-destination and accumulates collected errors"]
fn writability_check_runs_for_each_local_mail_destination() {
    // A flow with two local_mail destinations (user="ops" + user
    // ="alerts"). Spool for "ops" writable, spool for "alerts"
    // mode 0444. Validator's collected-errors interface must
    // surface both: destination[0] passes, destination[1] fails.
    //
    // Mutation target (once active): a probe that short-circuits
    // on the first failure leaves the operator unable to fix all
    // problems in one pass.
}

#[test]
#[ignore = "spool writability check + DynamicUser-aware soft/hard semantics not implemented; activate once the daemon-startup (soft) vs gcit-check (hard) semantics are split"]
fn writability_check_skipped_for_dynamicuser_when_unrunnable() {
    // Under DynamicUser the daemon's effective uid is unstable
    // (changes per restart). The startup probe runs as the
    // CURRENT effective uid — operationally correct, but in some
    // environments the probe fails for transient reasons (stale
    // ACL, group renegotiation). The validator should:
    //   - HARD-fail at `gcit check` (operator must fix before
    //     shipping)
    //   - SOFT-warn at daemon start (log WARN, continue; runtime
    //     append errors will surface the issue if any)
    //
    // Until landed, the dual semantic isn't testable.
}

#[test]
#[ignore = "spool writability check + dedicated validator helper not implemented; activate once the validator exposes a probe that respects effective uid for both gcit-check and daemon-start contexts"]
fn writability_check_uses_dedicated_validator_not_open_close() {
    // gcit check + daemon startup share the same validator. The
    // probe must use the CURRENT effective uid (not the daemon's
    // eventual uid) — a `gcit check` invocation as the operator
    // shell user runs the probe as that user; the daemon under
    // DynamicUser runs the probe as its dynamic uid.
    //
    // Operator-facing affordance: `gcit check` should warn
    // "this check runs as YOU; the daemon runs as a different
    // uid under DynamicUser; if results differ, use sudo or a
    // simulate-systemd flag" — pin once the helper lands.
}

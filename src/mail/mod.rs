// Local mail (mbox append) notifier. Appends an mbox-formatted
// message to /var/mail/<user> via O_NOFOLLOW + O_APPEND + flock.
// No SMTP, no MTA dependency; mailx, postfix, etc. cooperate via
// the same flock primitive.
//
// Submodules:
//   - mbox: pure-data formatters (mboxrd quoting, header sanitization,
//     asctime, /etc/hostname lookup).
//   - notifier: LocalMailNotifier impl Notifier (the I/O path).
//
// Items here are `pub` for integration-test reachability and
// treated as crate-internal + unstable.

pub mod mbox;
pub mod notifier;

pub use mbox::{
    escape_mboxrd, format_message, read_hostname_or_default, sanitize_header, BODY_BYTE_CAP,
};
// `RealPersist` is intentionally NOT re-exported. Production wires
// it internally inside `LocalMailNotifier::new`; tests that need a
// custom Persist implement the trait themselves and pass it in via
// `for_test_with_persist`. Keeping it module-private prevents
// accidentally turning a production-only struct into part of the
// integration-test surface.
pub use notifier::{
    map_io_error, write_with_lock, LocalMailNotifier, Persist, WriteError, DEFAULT_SPOOL_DIR,
    LOCK_WAIT_DEADLINE,
};

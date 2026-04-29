// Canonical exit codes used by every CLI subcommand.
//
// Values follow sysexits.h convention: EX_OK=0, EX_USAGE=64,
// EX_SOFTWARE=70, EX_OSERR=71, EX_TEMPFAIL=75, EX_CONFIG=78.
// Centralizing these here keeps every subcommand module from
// re-defining its own per-command constant set.

/// Successful exit. EX_OK.
pub const OK: u8 = 0;
/// Command-line usage error (bad arguments, mutex violations, missing
/// required flag). EX_USAGE.
pub const USAGE: u8 = 64;
/// Input-data error. Returned by `gcit validate-template <FILE>` when
/// the supplied template fails to compile or render against the
/// probe context. Distinct from `CONFIG` because the input is a
/// free-form template file, not a parsed gcit config. EX_DATAERR.
pub const DATAERR: u8 = 65;
/// Internal software error (invocation valid, implementation absent or
/// broken). EX_SOFTWARE.
pub const SOFTWARE: u8 = 70;
/// Operating-system error (filesystem failures, sha mismatch on
/// uninstall without --force, manifest schema mismatch). EX_OSERR.
pub const OSERR: u8 = 71;
/// Temporary failure (daemon not reachable, control socket transport
/// error, rate-limited reload). EX_TEMPFAIL.
pub const TEMPFAIL: u8 = 75;
/// Configuration error (parse / validation failures, refused silent
/// overwrite, credential not found). EX_CONFIG.
pub const CONFIG: u8 = 78;

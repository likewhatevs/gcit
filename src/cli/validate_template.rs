// `gcit validate-template <FILE>` — compile a standalone template
// file against the daemon's probe context. Exits 0 on success or
// EX_DATAERR=65 on compile/render failure.
//
// The template source is read verbatim from `FILE` (no TOML wrapping;
// just the raw template string). The pipeline matches
// `config::validate::compile_template_field` so this command stays in
// lockstep with the daemon's config-load validation:
//   1. Register the template string into a `notify::strict_handlebars()`
//      instance — strict_mode + DEREGISTERED_HELPERS + no_escape.
//   2. AST check via `config::validate::find_bare_name` — reject
//      single-segment references like `{{flow}}` or
//      `{{gcit_run_id}}` because the runtime template namespace is
//      dotted only.
//   3. Render against `config::validate::probe_context()` — the same
//      probe shape `gcit check` uses to catch typos in dotted leaves
//      (e.g. `{{flow.naem}}`).
// All three failure paths exit `EX_DATAERR=65` with the underlying
// error on stderr. On success the rendered output is printed to
// stdout (operators routinely pipe it into `jq` or compare against
// expected text in CI).
//
// The probe context (built by `config::validate::probe_context`)
// supplies realistic placeholder values for every namespaced leaf
// (e.g. `flow.name = "linux-mainline-ci"`, a 40-char hex `source.sha`,
// a u64 `action.run_id`) so strict-mode rendering catches typed-shape
// bugs as well as missing fields. Matches the probe `gcit check` runs
// at config load.

use std::path::Path;
use std::process::ExitCode;

use clap::ValueEnum;

use crate::cli::exit;

/// Destination surface a validate-template invocation targets. Used
/// only for operator-facing output today (the underlying probe
/// context is shared across surfaces); surface-specific checks
/// (Discord per-job key set, local_mail body cap, etc.) hang off
/// this enum in future iterations. `--kind discord` and `--kind
/// local-mail` map to the variants below via clap's `ValueEnum`
/// kebab-case rename.
#[derive(Debug, Clone, Copy, ValueEnum)]
#[clap(rename_all = "kebab-case")]
pub enum Kind {
    /// Discord webhook embed (title / description / collapsed_summary
    /// / field_name / field_value templates).
    Discord,
    /// Local mail mboxrd (subject + body templates).
    LocalMail,
}

impl Kind {
    /// Operator-facing label used in the output header so reviewers
    /// reading CI logs see which surface was validated.
    fn label(self) -> &'static str {
        match self {
            Kind::Discord => "discord",
            Kind::LocalMail => "local_mail",
        }
    }
}

/// Stable handlebars-template id used to register the operator's
/// template into a fresh registry. The id is internal and never
/// surfaced to operators; pinning it as a constant prevents drift
/// between `register_template_string` and the corresponding
/// `get_template` lookup.
const VALIDATE_TEMPLATE_ID: &str = "gcit-validate-template";

/// Validate a template file at `path` against the daemon's probe
/// context. Returns:
/// - `EX_OK=0` when the template compiles, passes the bare-name AST
///   check, and renders successfully. The rendered output is printed
///   to stdout.
/// - `EX_DATAERR=65` when the file cannot be read, the template fails
///   to compile, contains a bare (non-namespaced) variable reference,
///   or strict-mode rendering fails (e.g., the template references a
///   typo like `{{flow.naem}}`).
///
/// `kind`, when set, is recorded in a one-line stderr header so
/// operators reading CI logs see which surface (Discord vs
/// local_mail) the template was validated against.
pub fn run(path: &Path, kind: Option<Kind>) -> ExitCode {
    if let Some(k) = kind {
        eprintln!(
            "gcit validate-template: validating {} against the {} surface",
            path.display(),
            k.label(),
        );
    }
    let source = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "gcit validate-template: cannot read {}: {}",
                path.display(),
                e,
            );
            return ExitCode::from(exit::DATAERR);
        }
    };

    let mut hb = crate::notify::strict_handlebars();
    let ctx = crate::config::validate::probe_context();

    // (1) compile.
    if let Err(e) = hb.register_template_string(VALIDATE_TEMPLATE_ID, &source) {
        eprintln!(
            "gcit validate-template: {} failed to compile: {}",
            path.display(),
            e,
        );
        return ExitCode::from(exit::DATAERR);
    }

    // (2) AST-level bare-name check.
    // `gcit check` rejects single-segment references like `{{flow}}`
    // and `{{gcit_run_id}}` at config load via the same helper; this
    // command must agree or operators get inconsistent verdicts.
    if let Some(tpl) = hb.get_template(VALIDATE_TEMPLATE_ID) {
        if let Some(bare) = crate::config::validate::find_bare_name(tpl) {
            eprintln!(
                "gcit validate-template: {} references bare name '{{{{{}}}}}'; \
                 only namespaced variables (e.g. {{{{flow.name}}}}, {{{{source.sha}}}}) are accepted",
                path.display(),
                bare,
            );
            return ExitCode::from(exit::DATAERR);
        }
    }

    // (3) render.
    match hb.render(VALIDATE_TEMPLATE_ID, &ctx) {
        Ok(rendered) => {
            // Stream the rendered output verbatim to stdout. No
            // trailing newline injection — operators that pipe into
            // `jq` or `diff` get exactly what the daemon would
            // produce at notification time.
            print!("{}", rendered);
            ExitCode::from(exit::OK)
        }
        Err(e) => {
            eprintln!(
                "gcit validate-template: {} failed to render: {}",
                path.display(),
                e,
            );
            ExitCode::from(exit::DATAERR)
        }
    }
}

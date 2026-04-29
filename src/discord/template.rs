// Handlebars rendering for Discord embed leaf strings + codepoint-
// safe truncation.
//
// twilight-validate has a known codepoints-vs-bytes inconsistency:
// per-field validators (TITLE_LENGTH, FIELD_NAME_LENGTH, etc.) use
// `.chars().count()` (codepoints), but the EMBED_TOTAL_LENGTH check
// and the chars() helper use `.len()` (bytes). gcit MUST validate
// per-field by codepoints (the real Discord limit) and pre-truncate
// strings at codepoint boundaries before constructing the Embed, so
// twilight-validate never has cause to reject our payload.
//
// Truncation strategy:
//   - Render template with strict_mode (every variable must resolve;
//     undefined variables produce a TemplateError that surfaces as
//     NotifyError::Permanent — this catches operator typos at
//     runtime when probe-context rendering at config-load missed
//     them).
//   - Truncate the rendered string by codepoints (not bytes) to fit
//     the field's per-Discord limit. Append a single "…" (U+2026)
//     ellipsis when truncation actually fires, so operators see the
//     content was cut.
//
// All public helpers here take a pre-rendered string OR an
// already-rendered handlebars template object; we don't expose the
// raw handlebars instance through this module.

use handlebars::{Handlebars, RenderError};
use serde::Serialize;

/// Codepoint-counted length of `s`. Repeated chars().count() in the
/// hot path is a known wart; bench if it becomes a problem.
fn char_len(s: &str) -> usize {
    s.chars().count()
}

/// Truncate `input` to at most `max_chars` codepoints. If truncation
/// fires, append a single ellipsis `…` (U+2026) so operators see
/// content was cut.
///
/// Returns the truncated string. The ellipsis is included in the
/// `max_chars` budget — the result is *always* ≤ `max_chars`
/// codepoints.
///
/// Per the twilight-validate codepoints-vs-bytes note above: we
/// truncate by codepoints (the real Discord limit), so the resulting
/// string passes twilight-validate's per-field check unconditionally.
pub fn truncate_codepoints(input: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    if char_len(input) <= max_chars {
        return input.to_string();
    }
    // Reserve one codepoint for the ellipsis. If max_chars is 1, the
    // result is just the ellipsis — we lose all input but the
    // operator still sees that something was truncated.
    let keep = max_chars.saturating_sub(1);
    let mut out: String = input.chars().take(keep).collect();
    out.push('…');
    out
}

/// Render `template` against `ctx` via the supplied handlebars
/// instance, then truncate the result to `max_chars` codepoints.
/// Used to render leaf strings (title, description, field name,
/// field value, footer) for Discord embeds.
///
/// Errors short-circuit on the first template that fails to render.
/// Config validation has already compiled every template against a
/// probe context; runtime failures here are typically from
/// strict_mode rejecting an undefined variable in a value-shape that
/// the probe didn't exercise.
pub fn render_and_truncate<C: Serialize>(
    handlebars: &Handlebars<'_>,
    template: &str,
    ctx: &C,
    max_chars: usize,
) -> Result<String, RenderError> {
    let rendered = handlebars.render_template(template, ctx)?;
    Ok(truncate_codepoints(&rendered, max_chars))
}

/// Render an optional template; return `None` if the operator did
/// not configure one. Truncation applies only when the template was
/// configured.
pub fn render_optional<C: Serialize>(
    handlebars: &Handlebars<'_>,
    template: Option<&str>,
    ctx: &C,
    max_chars: usize,
) -> Result<Option<String>, RenderError> {
    match template {
        Some(t) => render_and_truncate(handlebars, t, ctx, max_chars).map(Some),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn handlebars() -> Handlebars<'static> {
        crate::notify::strict_handlebars()
    }

    #[test]
    fn truncate_codepoints_no_op_for_short_input() {
        assert_eq!(truncate_codepoints("hello", 10), "hello");
    }

    #[test]
    fn truncate_codepoints_at_exact_limit() {
        // 5-codepoint input, 5-codepoint cap → no truncation.
        assert_eq!(truncate_codepoints("hello", 5), "hello");
    }

    #[test]
    fn truncate_codepoints_beyond_limit_appends_ellipsis() {
        // 6 codepoints → cap at 5: 4 chars + ellipsis.
        let result = truncate_codepoints("abcdef", 5);
        assert_eq!(result, "abcd…");
        assert_eq!(char_len(&result), 5);
    }

    #[test]
    fn truncate_codepoints_handles_multibyte() {
        // 4 emoji = 4 codepoints (each takes 4 bytes in UTF-8).
        // Cap at 3 → 2 emoji + ellipsis = 3 codepoints.
        let input = "😀😀😀😀";
        assert_eq!(char_len(input), 4);
        let result = truncate_codepoints(input, 3);
        assert_eq!(char_len(&result), 3);
        assert!(result.ends_with('…'));
    }

    #[test]
    fn truncate_codepoints_at_zero_returns_empty() {
        assert_eq!(truncate_codepoints("abc", 0), "");
    }

    #[test]
    fn truncate_codepoints_at_one_returns_only_ellipsis() {
        // Edge case: cap=1, input longer → just the ellipsis.
        let result = truncate_codepoints("hello", 1);
        assert_eq!(result, "…");
        assert_eq!(char_len(&result), 1);
    }

    #[test]
    fn render_and_truncate_substitutes_variables() {
        let hb = handlebars();
        let ctx = json!({"flow": {"name": "myflow"}});
        let r = render_and_truncate(&hb, "flow={{flow.name}}", &ctx, 100).unwrap();
        assert_eq!(r, "flow=myflow");
    }

    #[test]
    fn render_and_truncate_truncates_long_output() {
        let hb = handlebars();
        let ctx = json!({"x": "abcdefghij"});
        // Template renders to "abcdefghij" (10 chars); cap at 5.
        let r = render_and_truncate(&hb, "{{x}}", &ctx, 5).unwrap();
        assert_eq!(char_len(&r), 5);
        assert!(r.ends_with('…'));
    }

    #[test]
    fn render_and_truncate_strict_mode_rejects_undefined() {
        // strict_mode + undefined variable → RenderError; the
        // notifier surfaces this as NotifyError::Permanent at runtime.
        let hb = handlebars();
        let ctx = json!({});
        let err = render_and_truncate(&hb, "{{undefined}}", &ctx, 100).unwrap_err();
        assert!(format!("{err}").contains("undefined"));
    }

    #[test]
    fn render_optional_returns_none_for_no_template() {
        let hb = handlebars();
        let ctx = json!({});
        assert_eq!(render_optional(&hb, None, &ctx, 100).unwrap(), None,);
    }

    #[test]
    fn render_optional_renders_when_present() {
        let hb = handlebars();
        let ctx = json!({"name": "alice"});
        assert_eq!(
            render_optional(&hb, Some("hi {{name}}"), &ctx, 100).unwrap(),
            Some("hi alice".to_string()),
        );
    }
}

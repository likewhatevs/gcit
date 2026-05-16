// Handlebars template validation: compile under strict_mode, render
// against a probe context to catch typos in dotted leaves, and AST-
// inspect to reject bare-name expressions (`{{flow}}`, `{{gcit_run_id}}`)
// that strict-mode would otherwise miss.

use std::path::Path;

use toml::Spanned;

use super::super::error::ConfigError;
use super::{span_line, validate_err};

/// Top-level template namespaces. Bare expressions matching one of
/// these (`{{flow}}`) are rejected because the namespace has no
/// string representation; only its dotted leaves are renderable.
/// Single-name expressions outside this set (`{{gcit_run_id}}`) are
/// also rejected — the template variable contract requires the
/// `namespace.field` form.
const TEMPLATE_NAMESPACES: &[&str] = &["flow", "source", "action", "run", "gcit"];

/// Compile a single template field and verify it renders against a
/// probe context that supplies every documented namespaced variable.
/// Returns `None` when compilation OR the bare-name AST check fails
/// so the typed `Config` never carries a template that would explode
/// at notification time.
pub(super) fn compile_template_field(
    template: &Option<Spanned<String>>,
    field: &'static str,
    source: &str,
    path: &Path,
    errors: &mut Vec<ConfigError>,
    flow_name: &str,
) -> Option<String> {
    let spanned = template.as_ref()?;
    let raw = spanned.get_ref().clone();
    let line = span_line(source, spanned);

    let mut hb = crate::notify::strict_handlebars();
    if let Err(e) = hb.register_template_string(field, &raw) {
        errors.push(ConfigError::TemplateCompile {
            path: path.to_path_buf(),
            line,
            flow: flow_name.to_string(),
            field: field.to_string(),
            source: e,
        });
        return None;
    }

    // AST-level check: reject expressions whose path is single-segment
    // (a top-level namespace or a non-dotted name). The probe render
    // cannot catch these because handlebars renders a sub-object as
    // its serialized form rather than raising.
    if let Some(tpl) = hb.get_template(field) {
        if let Some(bare) = find_bare_name(tpl) {
            errors.push(validate_err(
                path,
                vec![line],
                Some(flow_name),
                field,
                raw.clone(),
                format!(
                    "template references bare name '{{{{{}}}}}'; only namespaced variables ({}) are accepted",
                    bare,
                    namespace_form_examples(),
                ),
                format!(
                    "use a namespaced reference like '{{{{flow.name}}}}', '{{{{source.sha_short}}}}' (got {{{{{}}}}})",
                    bare,
                ),
            ));
            return None;
        }
    }

    // Render against the probe context. Strict-mode catches typos in
    // dotted leaves (`{{flow.naem}}`) and missing nested fields.
    let ctx = probe_context();
    if let Err(e) = hb.render(field, &ctx) {
        errors.push(validate_err(
            path,
            vec![line],
            Some(flow_name),
            field,
            raw.clone(),
            format!("template fails to render against the probe context: {}", e),
            "use only documented variables (flow.name, source.sha, source.sha_short, action.repo, action.workflow, action.run_id, action.run_url, action.dispatched_at, run.status, run.conclusion, gcit.run_id, flow.description)",
        ));
        return None;
    }

    Some(raw)
}

/// Inspect a parsed handlebars `Template` for any expression whose
/// reference is a single-segment name (i.e. not in the dotted
/// `namespace.field` form). Returns the offending raw name when one
/// is found; `None` when every reference is a dotted path.
///
/// `pub` so `cli/validate_template.rs` can apply the same AST check
/// the daemon uses at config load — `gcit validate-template` must
/// agree with `gcit check` on what templates are accepted.
pub fn find_bare_name(tpl: &handlebars::Template) -> Option<String> {
    use handlebars::template::TemplateElement;
    for element in &tpl.elements {
        let helper = match element {
            TemplateElement::Expression(h) => h,
            TemplateElement::HtmlExpression(h) => h,
            TemplateElement::HelperBlock(h) => h,
            _ => continue,
        };
        let name = match helper.name.as_name() {
            Some(n) => n.to_string(),
            None => continue,
        };
        // Dotted (`flow.name`) or slashed (`flow/name`) paths are
        // accepted; anything single-segment is rejected.
        if !name.contains('.') && !name.contains('/') {
            return Some(name);
        }
        if let Some(inner) = &helper.template {
            if let Some(inner_bare) = find_bare_name(inner) {
                return Some(inner_bare);
            }
        }
        if let Some(inner) = &helper.inverse {
            if let Some(inner_bare) = find_bare_name(inner) {
                return Some(inner_bare);
            }
        }
    }
    None
}

fn namespace_form_examples() -> String {
    TEMPLATE_NAMESPACES
        .iter()
        .map(|n| format!("{}.<field>", n))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Probe context supplying type-faithful stub values for every
/// documented namespaced template variable. Used by
/// `compile_template_field` to drive a render pass that exercises
/// strict-mode missing-variable detection. `pub` so
/// `cli/validate_template.rs` shares the same probe.
///
/// Stub shapes mirror the runtime values: 40-hex SHA, 12-hex short
/// SHA, full HTTPS run URL, RFC3339 dispatch timestamp, UUID-shaped
/// gcit.run_id, and integer action.run_id (u64 at runtime).
pub fn probe_context() -> serde_json::Value {
    serde_json::json!({
        "flow": {
            "name": "linux-mainline-ci",
            "description": "Watch torvalds/linux master and dispatch our CI builder.",
        },
        "source": {
            "url": "https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git",
            "ref_name": "refs/heads/master",
            "sha": "a".repeat(40),
            "sha_short": "a".repeat(12),
        },
        "action": {
            "repo": "myorg/linux-builder",
            "workflow": "ci.yml",
            "run_id": 1234567890u64,
            "run_url": "https://github.com/myorg/linux-builder/actions/runs/1234567890",
            "dispatched_at": "2026-04-26T12:34:56Z",
        },
        "run": {
            "status": "completed",
            "conclusion": "success",
        },
        "gcit": {
            "run_id": "00000000-0000-0000-0000-000000000000",
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_context_carries_every_documented_namespace() {
        let ctx = probe_context();
        let obj = ctx
            .as_object()
            .expect("probe context must be a JSON object");
        for ns in TEMPLATE_NAMESPACES {
            assert!(
                obj.contains_key(*ns),
                "probe context must carry namespace '{}'; got keys: {:?}",
                ns,
                obj.keys().collect::<Vec<_>>(),
            );
        }
    }

    #[test]
    fn probe_context_source_sha_is_40_hex_chars() {
        let ctx = probe_context();
        let sha = ctx
            .get("source")
            .and_then(|s| s.get("sha"))
            .and_then(|v| v.as_str())
            .expect("probe context source.sha must be a string");
        assert_eq!(sha.len(), 40, "source.sha must be 40 chars; got: {sha}");
    }

    #[test]
    fn probe_context_action_run_id_is_unsigned_integer() {
        // action.run_id is u64 at runtime; the probe must be a JSON
        // integer so handlebars helpers expecting a number see one.
        let ctx = probe_context();
        let run_id = ctx
            .get("action")
            .and_then(|a| a.get("run_id"))
            .expect("probe context action.run_id must exist");
        assert!(run_id.is_u64(), "action.run_id must be u64; got: {run_id}");
    }

    #[test]
    fn probe_context_run_status_and_conclusion_are_strings() {
        let ctx = probe_context();
        let status = ctx.get("run").and_then(|r| r.get("status")).unwrap();
        assert!(status.is_string(), "run.status must be a string");
        let conclusion = ctx.get("run").and_then(|r| r.get("conclusion")).unwrap();
        assert!(conclusion.is_string(), "run.conclusion must be a string");
    }

    #[test]
    fn probe_context_action_dispatched_at_is_rfc3339_string() {
        let ctx = probe_context();
        let v = ctx
            .get("action")
            .and_then(|a| a.get("dispatched_at"))
            .and_then(|v| v.as_str())
            .expect("action.dispatched_at must exist as a string");
        assert!(
            v.contains('T') && v.ends_with('Z'),
            "action.dispatched_at must be RFC3339; got: {v}",
        );
    }

    #[test]
    fn probe_context_gcit_run_id_is_uuid_shaped() {
        let ctx = probe_context();
        let v = ctx
            .get("gcit")
            .and_then(|g| g.get("run_id"))
            .and_then(|v| v.as_str())
            .expect("gcit.run_id must exist as a string");
        assert_eq!(v.len(), 36, "uuid string must be 36 chars; got: {v}");
        assert_eq!(
            v.matches('-').count(),
            4,
            "uuid must have 4 hyphens; got: {v}"
        );
    }

    fn compile_template(s: &str) -> handlebars::Template {
        handlebars::Template::compile(s).expect("template must compile")
    }

    #[test]
    fn find_bare_name_returns_none_for_dotted_path() {
        let tpl = compile_template("hello {{flow.name}}");
        assert_eq!(find_bare_name(&tpl), None);
    }

    #[test]
    fn find_bare_name_returns_none_for_slashed_path() {
        let tpl = compile_template("{{flow/name}}");
        assert_eq!(find_bare_name(&tpl), None);
    }

    #[test]
    fn find_bare_name_returns_some_for_single_segment_namespace() {
        let tpl = compile_template("hello {{flow}}");
        assert_eq!(find_bare_name(&tpl), Some("flow".to_string()));
    }

    #[test]
    fn find_bare_name_returns_some_for_single_segment_unknown_name() {
        let tpl = compile_template("{{gcit_run_id}}");
        assert_eq!(find_bare_name(&tpl), Some("gcit_run_id".to_string()));
    }

    #[test]
    fn find_bare_name_returns_none_for_plain_string_with_no_expressions() {
        let tpl = compile_template("plain text with no template variables");
        assert_eq!(find_bare_name(&tpl), None);
    }

    #[test]
    fn find_bare_name_returns_first_match_when_template_has_multiple_bare_references() {
        // First-match-wins: source-order scan returns the first hit.
        let tpl = compile_template("{{flow}} and {{source}}");
        assert_eq!(find_bare_name(&tpl), Some("flow".to_string()));
    }

    #[test]
    fn find_bare_name_block_helper_with_single_segment_name_returns_helper_name() {
        // The OUTER helper name fires first — even though the body's
        // `{{flow}}` is also bare, "if" wins.
        let tpl = compile_template("{{#if action.repo}}{{flow}}{{/if}}");
        assert_eq!(find_bare_name(&tpl), Some("if".to_string()));
    }

    #[test]
    fn namespace_form_examples_lists_every_documented_namespace() {
        let s = namespace_form_examples();
        for ns in TEMPLATE_NAMESPACES {
            let needle = format!("{}.<field>", ns);
            assert!(s.contains(&needle), "must mention {needle:?}; got: {s}");
        }
        assert!(s.contains(", "), "must use \", \" separator; got: {s}");
    }
}

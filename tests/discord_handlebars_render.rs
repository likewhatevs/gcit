// Discord-side handlebars strict-mode rendering.
//
// **Covered today** by the in-module test suite at
// `src/notify/mod.rs::tests`:
//   * `strict_handlebars_deregisters_block_helpers` — verifies that
//     `crate::notify::strict_handlebars()` rejects templates using
//     `each` / `with` / partials at register-template time.
//   * `strict_handlebars_renders_leaf_substitution` — verifies the
//     happy-path namespaced-leaf substitution
//     (`{{flow.name}}` etc.) actually renders.
//   * `strict_handlebars_does_not_html_escape_special_chars` — verifies
//     `handlebars::no_escape` is registered so plain-text outputs are
//     not HTML-encoded (Discord renders markdown, mbox is plain text).
//
// The Discord notifier shares the same `crate::notify::strict_handlebars()`
// instance via `Arc<Handlebars>` constructed in
// `flow::supervisor::build_notifiers`; the per-call-site Discord
// rendering surface (`src/discord/template.rs`) layers on top of that
// shared instance.
//
// **Not yet tested.** The original speculative skeletons described
// the following contracts but no test currently verifies them anywhere
// in the tree — they remain open work:
//   * Strict-mode missing-variable error at render time
//     (e.g. template `{{flow.unknown_field}}` should produce
//     `RenderError`, not silent empty substitution).
//   * Partial-include `{{> partial}}` rejection at compile time
//     (`Handlebars::register_template_string` should error when no
//     partial is registered, but the contract is not pinned by a test).
//   * Bare-var `{{flow}}` compile-time rejection — the spec calls for
//     bare names to fail at config load with a `ConfigError::TemplateCompile`,
//     but handlebars itself accepts bare names as root-context lookups
//     so an explicit AST walk is required and not currently asserted.
//   * Template compile error message includes flow name + field name —
//     `ConfigError::TemplateCompile { flow, field, source }` is defined
//     but no test pins that the field-name context survives through
//     to the operator-facing message.
//
// The 8 deleted skeletons that lived here were `let _ = ...;`
// placeholders without assertions; activating them as real tests is
// independent work. Deleting the placeholders keeps the test suite
// honest about what is and isn't verified.

// `load_or_init` rejects malformed JSON content with a parse-stage
// `StateError::Load`.
//
// The first deserialize pass in `load_or_init` is the SchemaProbe
// deserialize, which pulls only the `schema` field. Bytes that are
// not parseable JSON at all must surface as `StateError::Load` whose
// message identifies the parse stage AND names the offending file
// path so an operator running `gcit status` or reading journald can
// navigate to the file directly.
//
// This complements the inline unit test
// `load_or_init_rejects_truncated_json` in src/state/mod.rs by
// exercising the public surface from outside the crate (integration
// scope) and by covering several distinct malformations: truncation,
// random bytes, invalid escape, unbalanced delimiters.

use gcit::state::{load_or_init, StateError};
use tempfile::tempdir;

fn assert_load_error_with_parse_stage(err: StateError, expected_path: &std::path::Path) {
    match err {
        StateError::Load { path, message } => {
            assert_eq!(
                path, expected_path,
                "Load.path must echo the offending state.json path",
            );
            // Message format: "parse JSON: <serde_json::Error>" (set
            // by load_or_init's SchemaProbe parse map_err). Pin the
            // literal "parse JSON:" so a wording drift (e.g. dropping
            // the stage prefix or renaming to just "parse:") surfaces
            // here. Operators search journald for this exact
            // substring to disambiguate parse vs schema vs
            // deserialize failures.
            assert!(
                message.contains("parse JSON:"),
                "Load.message must label the parse stage with `parse JSON:`; got {message:?}",
            );
        }
        other => panic!("expected StateError::Load with parse stage, got {other:?}"),
    }
}

#[test]
fn malformed_truncated_object() {
    // Object started but never closed. serde_json reports
    // "EOF while parsing an object".
    let td = tempdir().expect("tempdir");
    let p = td.path().join("state.json");
    std::fs::write(&p, r#"{"schema": 1, "flows": {"#).expect("seed truncated state");
    let err = load_or_init(&p).expect_err("truncated JSON must NOT load as default");
    assert_load_error_with_parse_stage(err, &p);
}

#[test]
fn malformed_random_garbage() {
    // Not even a JSON value — leading byte is '<'. serde_json reports
    // "expected value" at line 1 column 1.
    let td = tempdir().expect("tempdir");
    let p = td.path().join("state.json");
    std::fs::write(&p, b"<html>not json</html>").expect("seed garbage state");
    let err = load_or_init(&p).expect_err("non-JSON content must NOT load as default");
    assert_load_error_with_parse_stage(err, &p);
}

#[test]
fn malformed_invalid_escape_sequence() {
    // String literal contains `\z`, which is not a valid JSON escape.
    // serde_json reports "invalid escape" mid-stream.
    let td = tempdir().expect("tempdir");
    let p = td.path().join("state.json");
    std::fs::write(&p, r#"{"schema": 1, "x": "\z"}"#).expect("seed bad escape state");
    let err = load_or_init(&p).expect_err("invalid escape must NOT load as default");
    assert_load_error_with_parse_stage(err, &p);
}

#[test]
fn malformed_empty_file_is_parse_error_not_default() {
    // An empty file is distinct from "file does not exist". load_or_init
    // returns Ok(State::default()) ONLY on ENOENT; an empty file open
    // succeeds and routes through the parse pass, where serde_json
    // reports "EOF while parsing a value at line 1 column 0".
    //
    // The regression guarded here: a future "empty file => default"
    // shortcut would silently mask state-file truncation (e.g. a
    // half-written atomic_write_json fault) by starting the daemon on
    // a clean slate while the operator's data is gone.
    let td = tempdir().expect("tempdir");
    let p = td.path().join("state.json");
    std::fs::write(&p, b"").expect("seed empty state file");
    let err = load_or_init(&p).expect_err("empty file must NOT load as default");
    assert_load_error_with_parse_stage(err, &p);
}

#[test]
fn malformed_top_level_string_not_object() {
    // Valid JSON, but not a JSON object — `SchemaProbe` requires an
    // object so it can pull the `schema` field. A bare string is
    // type-mismatched at the deserialize level.
    let td = tempdir().expect("tempdir");
    let p = td.path().join("state.json");
    std::fs::write(&p, r#""not an object""#).expect("seed wrong-type state");
    let err = load_or_init(&p).expect_err("top-level string must NOT load as default");
    // Message could be "parse JSON: invalid type: string" — still
    // routed through `parse JSON:`. Use the same assertion helper.
    assert_load_error_with_parse_stage(err, &p);
}

// State schema version enforcement.
// Schema versioned (schema: 1). Refuses unknown versions.
//
// The schema field is the load-time gate. Any unknown version (older or
// newer) must FAIL load with a clear error naming the encountered version
// and the supported version. Operator's fix: back up state.json and let
// the daemon start fresh (old serialized data is disposable per the
// project-wide policy).

use rstest::rstest;
use tempfile::TempDir;

use gcit::state::SCHEMA_VERSION;

#[rstest]
#[case::version_zero(0u64)]
#[case::version_two(2)]
#[case::version_99(99)]
#[case::version_max(u32::MAX as u64)]
fn unknown_schema_version_rejected(#[case] schema: u64) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");
    let body = serde_json::json!({ "schema": schema, "flows": {} }).to_string();
    std::fs::write(&path, body).unwrap();

    let err =
        gcit::state::load_or_init(&path).expect_err(&format!("schema={schema} must be rejected"));
    let msg = err.to_string();
    assert!(
        msg.contains(&schema.to_string()),
        "error must name encountered version: {msg}",
    );
    assert!(
        msg.contains(&SCHEMA_VERSION.to_string()),
        "error must name supported version {SCHEMA_VERSION}: {msg}",
    );
}

#[test]
fn missing_schema_field_rejected() {
    // A JSON file that is otherwise valid but lacks the schema field at
    // all must be rejected — not silently treated as schema 1.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");
    std::fs::write(&path, "{\"flows\":{}}").unwrap();
    let err = gcit::state::load_or_init(&path).expect_err("missing schema must reject");
    let msg = err.to_string();
    assert!(msg.contains("schema"), "error must mention schema: {msg}");
}

#[test]
fn string_schema_field_rejected() {
    // schema: "1" (string) instead of integer — must reject.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");
    std::fs::write(&path, r#"{"schema":"1","flows":{}}"#).unwrap();
    let err = gcit::state::load_or_init(&path).expect_err("string schema must reject");
    let msg = err.to_string();
    assert!(
        msg.contains("schema") && msg.contains("integer"),
        "error must call out schema + integer requirement: {msg}",
    );
}

#[test]
fn known_schema_version_accepted() {
    // schema: 1 with empty flows object must load cleanly. Pins that the
    // rejection is specific to UNKNOWN versions, not over-eager.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");
    let body = serde_json::json!({ "schema": SCHEMA_VERSION, "flows": {} }).to_string();
    std::fs::write(&path, body).unwrap();
    let s = gcit::state::load_or_init(&path).expect("schema 1 must load");
    assert_eq!(s.schema, SCHEMA_VERSION);
    assert!(s.flows.is_empty());
}

#[test]
fn rejection_error_guides_operator_to_recover() {
    // The rejection error message must guide the operator to back up and
    // remove the corrupt file. Pinning this prevents drift in operator-
    // facing prose.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");
    std::fs::write(&path, r#"{"schema":99,"flows":{}}"#).unwrap();
    let err = gcit::state::load_or_init(&path).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("back up") || msg.contains("remove") || msg.contains("fresh"),
        "error must surface recovery guidance: {msg}",
    );
}

#[test]
fn unknown_top_level_field_rejected() {
    // deny_unknown_fields on State catches drift if a future gcit version
    // wrote a sibling field without bumping the schema version.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.json");
    std::fs::write(
        &path,
        r#"{"schema":1,"flows":{},"future_field":"unexpected"}"#,
    )
    .unwrap();
    let err = gcit::state::load_or_init(&path).expect_err("unknown field must reject");
    let msg = err.to_string();
    assert!(
        msg.contains("future_field") || msg.contains("unknown"),
        "error must name unknown field: {msg}",
    );
}

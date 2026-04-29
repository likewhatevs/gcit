// 422 tailored error with YAML fix snippet.
// GithubErrorKind::DispatchInvalid Display impl:
//   "GitHub rejected workflow_dispatch (422 Unprocessable Entity).
//    Most common cause: workflow YAML lacks `workflow_dispatch:` in
//    `on:`. Add this to .github/workflows/{workflow}:
//
//    on:
//      workflow_dispatch:
//        inputs:
//          gcit_run_id:
//            type: string
//
//    run-name: gcit-${{ inputs.gcit_run_id }}
//
//    Then commit, push to default branch, and try again."
//
// If GitHub returns HTTP 422 with body 'Unexpected inputs' (or
// equivalent message indicating an undeclared gcit_run_id input on
// the workflow), gcit surfaces a tailored error.
//

#[tokio::test]
#[ignore = "requires GitHub dispatcher 422-handling implementation"]
async fn dispatch_422_with_unexpected_inputs_returns_dispatch_invalid() {
    // wiremock returns 422 with body shape:
    //   { "message": "Unexpected inputs",
    //     "errors": [{ "resource": "WorkflowRun", "code": "invalid",
    //                  "field": "inputs", ... }],
    //     "documentation_url": "..." }
    //
    // Drive workflow_dispatch attempt; capture err.
    // assert!(matches!(err, GithubErrorKind::DispatchInvalid { workflow }));
    // assert_eq!(workflow, "ci.yml");
}

#[tokio::test]
#[ignore = "requires GitHub dispatcher 422-handling implementation"]
async fn dispatch_invalid_message_contains_yaml_block_with_correct_indentation() {
    // The displayed error MUST contain a YAML-valid snippet that the
    // operator can copy-paste into their workflow file. Indentation is
    // critical (YAML is whitespace-sensitive).
    //
    // Pin the EXACT indentation from the spec:
    //   "on:\n  workflow_dispatch:\n    inputs:\n      gcit_run_id:\n        type: string"
    // (2-space increments at each nesting level)
    //
    // assert!(err.to_string().contains("on:\n  workflow_dispatch:"));
    // assert!(err.to_string().contains("    inputs:\n      gcit_run_id:"));
    // assert!(err.to_string().contains("        type: string"));
    //
    // Mutation target: tab vs space, wrong indent depth. YAML parser
    // would reject the operator's pasted fix and they'd be stuck.
    // Pin via substring match.
}

#[tokio::test]
#[ignore = "requires GitHub dispatcher 422-handling implementation"]
async fn dispatch_invalid_message_contains_run_name_directive() {
    // assert!(err.to_string().contains(
    //     "run-name: gcit-${{ inputs.gcit_run_id }}"
    // ));
    //
    // The literal "${{ inputs.gcit_run_id }}" template syntax is GitHub
    // Actions-specific; gcit must NOT escape the dollar signs or the
    // operator's workflow file will be wrong.
    //
    // Mutation target: gcit treating the message as Display-format
    // template and accidentally replacing $ symbols. Pin via exact
    // substring.
}

#[tokio::test]
#[ignore = "requires GitHub dispatcher 422-handling implementation"]
async fn dispatch_invalid_message_includes_repo_and_workflow_paths() {
    // Beyond the literal text, the message should ALSO name the repo
    // and the workflow filename so the operator knows exactly which
    // file to edit. Per spec ("The error names the repo and workflow
    // file so the user can edit it directly"):
    //
    // assert!(err.to_string().contains(".github/workflows/ci.yml"));
    //
    // SPEC GAP: line 744's display only formats {workflow}; line 778
    // says the message should also name the repo. Recommend:
    //   "GitHub rejected workflow_dispatch (422). Repo: {repo}. Workflow:
    //    .github/workflows/{workflow}. Most common cause: ..."
    // Or extend the variant to carry both repo and workflow. flag.
}

#[tokio::test]
#[ignore = "requires GitHub dispatcher 422-handling implementation"]
async fn dispatch_invalid_does_not_retry() {
    // 422 = configuration error. Retrying without operator action will
    // fail identically. Classifier returns Permanent. backon's .when()
    // predicate returns false.
    //
    // Cross-references tests/github_error_classifier.rs::classifier_dispatches_*.
    // This file pins the SPECIFIC permanent assertion for DispatchInvalid.
}

#[tokio::test]
#[ignore = "requires GitHub dispatcher 422-handling implementation"]
async fn dispatch_invalid_emits_last_error_for_status_command() {
    // After a 422 abort, the flow's last_error must be set with kind=
    // "github_4xx" (per last_error format) and message
    // containing the YAML snippet truncated to a reasonable length.
    //
    // SPEC GAP: spec line 580 caps last_error length implicitly via
    // "operator-friendly text". Recommend cap at 4 KB so `gcit status
    // --format json` doesn't render multi-page error blobs. flag.
}

#[tokio::test]
#[ignore = "requires GitHub dispatcher 422-handling implementation"]
async fn dispatch_422_with_other_error_message_falls_back_to_generic() {
    // 422 can mean things OTHER than missing gcit_run_id. Examples:
    //   "ref does not exist"
    //   "head_sha is required"
    //   "workflow has been disabled"
    //
    // gcit's classifier should ONLY emit the YAML snippet when the
    // message indicates the gcit_run_id case. Other 422s map to a
    // generic DispatchInvalid with the API's message text.
    //
    // SPEC GAP: spec line 778 says "or equivalent message indicating an
    // undeclared gcit_run_id input on the workflow". Pin: detection
    // heuristic = message contains "Unexpected inputs" OR refers to
    // "inputs" + "gcit_run_id". Otherwise generic. flag.
    //
    // assert!(matches!(err, GithubErrorKind::DispatchInvalid { .. }));
    // assert!(!err.to_string().contains("gcit_run_id"));  // generic path
    // assert!(err.to_string().contains("workflow has been disabled"));
}

// Duplicate match abort with both run IDs.
// Duplicate match (more than one run carries the same gcit-<uuid> in
// its name): abort with an error and log both run ids. This indicates
// a misconfigured workflow that runs more than once per dispatch;
// gcit refuses to track an ambiguous correlation rather than picking
// arbitrarily.
//
// This is the "fail loud" path: when the gcit_run_id sentinel appears
// twice (or more), the workflow's run-name template is wrong (e.g., the
// workflow has multiple jobs each with a `run-name` directive that copies
// the input). gcit cannot decide which run is "the dispatch's run", so
// it refuses to track any.

#[tokio::test]
#[ignore = "requires GitHub correlator duplicate-match implementation"]
async fn duplicate_match_aborts_correlation_with_both_run_ids() {
    // Wiremock returns:
    //   { "workflow_runs": [
    //         { "id": 100, "name": "regular" },
    //         { "id": 201, "name": "gcit-<uuid>" },     <- match
    //         { "id": 202, "name": "gcit-<uuid>" },     <- ALSO matches
    //         { "id": 102, "name": "another" }
    //       ] }
    //
    // Correlator MUST return Err(DuplicateMatch { run_ids: vec![201, 202] }).
    // Both run IDs MUST appear in the error AND in the WARN log entry.
    //
    // Mutation target: silently picking the first match (id=201). Test
    // catches by asserting the error variant, not just "is_err".
}

#[tokio::test]
#[ignore = "requires GitHub correlator duplicate-match implementation"]
async fn duplicate_match_log_event_contains_all_run_ids() {
    // Capture tracing events via test layer. Assert exactly one ERROR
    // event with structured fields:
    //   target = "github" (or "github::correlator")
    //   gcit_run_id = "<uuid>"
    //   matched_run_ids = vec![201, 202]  (or stringified)
    //   workflow = "ci.yml"
    //   repo = "owner/repo"
    //
    // SPEC GAP: spec line 380 says "log both run ids" but doesn't pin
    // log level (ERROR? WARN?) or field shape. Recommend ERROR (the
    // dispatch is unrecoverable for this run; operator must fix workflow).
    // Pin field names. flag.
}

#[tokio::test]
#[ignore = "requires GitHub correlator duplicate-match implementation"]
async fn duplicate_match_does_not_emit_run_started_state_update() {
    // Even though we matched runs, gcit MUST NOT emit StateUpdate::
    // RunStarted for either of them. Otherwise the monitor would chase
    // one of them and fire notifications for an arbitrary winner.
    //
    // Capture state-writer channel; assert zero RunStarted updates.
    // Assert ONE last_error update (for the flow) with kind=
    // "github_duplicate_run_match".
    //
    // SPEC GAP: spec doesn't pin which last_error kind. Recommend the
    // string above. flag.
}

#[tokio::test]
#[ignore = "requires GitHub correlator duplicate-match implementation"]
async fn duplicate_match_error_message_explains_workflow_misconfig() {
    // The error message must guide the operator. Recommend:
    //   "workflow {repo}/{workflow} matched gcit_run_id={uuid} on
    //    {N} runs (ids: {200, 201, 202}); this means the workflow
    //    spawned multiple parallel runs with the same input. fix:
    //    ensure exactly one job carries `run-name: gcit-${{ inputs
    //    .gcit_run_id }}` and that no matrix expansion creates
    //    duplicates. gcit refuses to track an ambiguous correlation."
    //
    // Mutation target: vague messages. Pin via assert! contains
    // "matrix" / "run-name" / "duplicate" / "ambiguous" or the literal
    // sentinel uuid.
}

#[tokio::test]
#[ignore = "requires GitHub correlator duplicate-match implementation"]
async fn three_or_more_matches_all_logged() {
    // Edge: 3 duplicates. Recommend: log ALL, not just the first 2.
    //
    // Wiremock body has 4 matching runs (ids 200, 201, 202, 203).
    // Assert error.run_ids has all 4, log event has all 4.
    //
    // Mutation target: an early-break loop that only collects 2 IDs.
    // Test catches.
}

#[tokio::test]
#[ignore = "requires GitHub correlator duplicate-match implementation"]
async fn correlator_does_not_retry_after_duplicate_match() {
    // Duplicate match is Permanent — retrying won't help. The correlator
    // returns immediately, NOT into backon's retry loop.
    //
    // Per the permanent-classification rule from
    // tests/poll_backoff_transient.rs, backon's .when() predicate
    // must return false for DuplicateMatch.
    //
    // Test under start_paused: correlator returns within 100ms, no
    // additional poll attempts.
}

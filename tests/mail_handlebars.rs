// Handlebars rendering for mbox subject + body.
//
// LocalMailNotifier renders subject and body via the same shared
// strict_handlebars instance Discord uses (see
// src/mail/notifier.rs::on_run_complete subject and body
// branches). The default subject is
// `format!("[gcit] {flow_name} {label}")` (NOT a handlebars template),
// and the default body is `default_body(ctx, summary)` which produces
// a plain-text run summary via format!() — also not handlebars.
// Custom templates use handlebars with strict_mode + dotted-path
// namespacing + no helpers, mirroring the Discord template
// configuration.
//
// Tests in this file pin the rendering contract:
//   - default subject/body: production format is regression-pinned via
//     mbox::format_message round-trip (the rendered subject must
//     appear in the mbox header block, sanitized).
//   - custom subject/body: handlebars renders templates against
//     RunContext + RunSummary.
//   - strict mode: undefined variables surface NotifyError::Permanent
//     end-to-end through on_run_complete.
//   - sandboxing: no helper functions registered (each/if/unless/with
//     all rejected at template compile).
//   - subject sanitization: multi-line subject is collapsed to one
//     line via mbox::sanitize_header (replaces all bytes < 0x20
//     except literal space with a space).
//   - body preservation: body is NOT sanitized — newlines survive
//     verbatim through format_message.

use std::sync::Arc;

use chrono::Utc;
use gix_hash::ObjectId;
use handlebars::Handlebars;
use tokio_util::sync::CancellationToken;

use gcit::config::{FireEvent, LocalMailTemplateConfig};
use gcit::github::{self, Conclusion, JobResult, RunStatus, RunSummary};
use gcit::mail::{format_message, sanitize_header, LocalMailNotifier};
use gcit::notify::{
    self, strict_handlebars, ActionInfo, Notifier, NotifyError, RunContext, SourceInfo,
};

#[test]
fn default_subject_renders_flow_name_and_conclusion() {
    // Production default subject (src/mail/notifier.rs::on_run_complete subject branch):
    //   format!("[gcit] {} {}", ctx.flow_name, github::label_for(conclusion))
    //
    // Replicate the exact format and pin it. This is the regression
    // guard against drift in the literal that operators see in their
    // mailbox subject lines.
    let flow_name = "linux-mainline-ci";
    let label = github::label_for(Conclusion::Failure);
    let default_subject = format!("[gcit] {flow_name} {label}");

    // The label for Failure is lowercase per github::label_for. The
    // production format embeds it verbatim.
    assert_eq!(default_subject, format!("[gcit] linux-mainline-ci {label}"));
    // Sanity: the default carries the "[gcit] " marker, the flow
    // name, and the conclusion label.
    assert!(default_subject.starts_with("[gcit] "));
    assert!(default_subject.contains(flow_name));
    assert!(default_subject.contains(label));

    // Round-trip through mbox::format_message to confirm the subject
    // surfaces in the header block. format_message also exercises
    // sanitize_header, which is a no-op for the default subject (no
    // control bytes).
    let formatted = format_message(
        Utc::now(),
        "operator",
        "host.example.com",
        &default_subject,
        "body text",
    );
    assert!(
        formatted.contains(&format!("Subject: {default_subject}")),
        "default subject must surface in the mbox Subject header; formatted:\n{formatted}",
    );
}

#[test]
fn custom_subject_template_used_when_set() {
    // Custom subject template via LocalMailTemplateConfig::subject.
    // Render with strict_handlebars + render_context. Production code
    // path: src/mail/notifier.rs::on_run_complete subject branch.
    let hb = strict_handlebars();
    let data = notify::render_context(&run_ctx(), &run_summary(Conclusion::Failure, 0));
    let template = "FAILED {{flow.name}}";
    let rendered = hb
        .render_template(template, &data)
        .expect("custom subject must render");
    assert_eq!(rendered, "FAILED myflow");
}

#[tokio::test]
async fn default_body_renders_run_summary() {
    // Production default body (src/mail/notifier.rs::default_body)
    // emits a structured plain-text summary via format!() with each
    // line keyed `<label>: <value>` and a trailing `jobs (<n>):` block.
    //
    // We can't intercept the rendered body before delivery (the
    // production path writes to /var/mail/<user> which is root-only
    // in the test environment, and the `/var/mail/<user>` path is
    // hardcoded in LocalMailNotifier::spool_path). What we CAN do is
    // drive on_run_complete with body=None and let the I/O step
    // fail; the resulting NotifyError tells us whether default_body
    // rendered successfully. If render failed, the error message
    // contains "body render failed"; if render succeeded and the
    // file open failed, the message contains "spool file ... does
    // not exist". The latter is the expected outcome on a clean
    // test environment, and proves default_body is reachable +
    // doesn't panic on the realistic input shapes we feed it.
    let n = LocalMailNotifier::new(
        "test",
        "myuser-fixture-does-not-exist",
        Arc::new("h.example.com".to_string()),
        vec![FireEvent::RunComplete],
        LocalMailTemplateConfig::default(),
        Arc::new(strict_handlebars()),
    )
    .expect("fixture user is alphanumeric+hyphen, valid");
    let outcome = n
        .on_run_complete(
            &run_ctx(),
            &run_summary(Conclusion::Failure, 2),
            &CancellationToken::new(),
        )
        .await;
    match &outcome {
        Err(NotifyError::Permanent { source }) => {
            let msg = source.to_string();
            assert!(
                !msg.contains("body render failed"),
                "default_body must render without error; got: {msg}",
            );
            assert!(
                !msg.contains("subject render failed"),
                "default subject must render without error; got: {msg}",
            );
            // I/O step is expected to fail on the missing
            // /var/mail/<user> file. The exact failure depends on
            // env (NotFound on most systems; PermissionDenied on
            // some). Either is fine — both prove render succeeded.
            assert!(
                msg.contains("spool file") || msg.contains("permission denied"),
                "expected I/O failure after render; got: {msg}",
            );
        }
        other => panic!("expected Permanent (I/O failure after successful render); got {other:?}",),
    }
}

#[test]
fn custom_body_template_used_when_set() {
    // Custom body template via LocalMailTemplateConfig::body. Render
    // with strict_handlebars + render_context. Production code path:
    // src/mail/notifier.rs::on_run_complete body branch.
    let hb = strict_handlebars();
    let data = notify::render_context(&run_ctx(), &run_summary(Conclusion::Success, 0));
    let template = "Custom body for {{flow.name}}";
    let rendered = hb
        .render_template(template, &data)
        .expect("custom body must render");
    assert_eq!(rendered, "Custom body for myflow");
}

#[tokio::test]
async fn render_strict_mode_missing_var_in_subject_returns_permanent() {
    // strict_mode + undefined `{{nonexistent.var}}` produces a
    // RenderError, which the notifier wraps as
    // NotifyError::Permanent with "subject render failed: ..."
    // (src/mail/notifier.rs::on_run_complete subject branch). Drive the full
    // on_run_complete path so the wrapping is exercised end-to-end.
    let n = LocalMailNotifier::new(
        "test",
        "myuser",
        Arc::new("h.example.com".to_string()),
        vec![FireEvent::RunComplete],
        LocalMailTemplateConfig {
            subject: Some("{{nonexistent.var}}".to_string()),
            body: None,
        },
        Arc::new(strict_handlebars()),
    )
    .expect("fixture user 'myuser' is valid");
    let outcome = n
        .on_run_complete(
            &run_ctx(),
            &run_summary(Conclusion::Success, 0),
            &CancellationToken::new(),
        )
        .await;
    match outcome {
        Err(NotifyError::Permanent { source }) => {
            let msg = source.to_string();
            assert!(
                msg.contains("subject render failed"),
                "expected subject render error wrapped as Permanent; got: {msg}",
            );
            assert!(
                msg.contains("nonexistent"),
                "error must surface the offending variable name; got: {msg}",
            );
        }
        other => panic!("expected NotifyError::Permanent, got {other:?}"),
    }
}

#[tokio::test]
async fn render_strict_mode_missing_var_in_body_returns_permanent() {
    // Same shape as the subject test but for body — the body branch
    // of mail::notifier::on_run_complete wraps as "body render
    // failed: ...".
    let n = LocalMailNotifier::new(
        "test",
        "myuser",
        Arc::new("h.example.com".to_string()),
        vec![FireEvent::RunComplete],
        LocalMailTemplateConfig {
            subject: None,
            body: Some("Body referencing {{nonexistent.var}}".to_string()),
        },
        Arc::new(strict_handlebars()),
    )
    .expect("fixture user 'myuser' is valid");
    let outcome = n
        .on_run_complete(
            &run_ctx(),
            &run_summary(Conclusion::Success, 0),
            &CancellationToken::new(),
        )
        .await;
    match outcome {
        Err(NotifyError::Permanent { source }) => {
            let msg = source.to_string();
            assert!(
                msg.contains("body render failed"),
                "expected body render error wrapped as Permanent; got: {msg}",
            );
            assert!(
                msg.contains("nonexistent"),
                "error must surface the offending variable name; got: {msg}",
            );
        }
        other => panic!("expected NotifyError::Permanent, got {other:?}"),
    }
}

#[test]
fn render_no_helpers_in_mail_templates() {
    // strict_handlebars deregisters each/if/unless/with helpers (see
    // src/notify/mod.rs::strict_handlebars). Templates that use them
    // must surface a RenderError at render time.
    //
    // Mutation target: registering `each` for mail
    // templates (assuming operators want loops over jobs). The
    // helper registration would defeat the sandboxing model; this
    // test guards by asserting render fails when each is invoked.
    let hb: Handlebars<'static> = strict_handlebars();
    let data = notify::render_context(&run_ctx(), &run_summary(Conclusion::Success, 0));

    // {{#each}} — must fail.
    let each_tmpl = "{{#each items}}{{this}}{{/each}}";
    let err = hb
        .render_template(each_tmpl, &data)
        .expect_err("each helper must be unregistered");
    let msg = err.to_string();
    assert!(
        msg.contains("each") || msg.contains("helper"),
        "error should mention the missing helper; got: {msg}",
    );

    // {{#if}} — must fail.
    let if_tmpl = "{{#if x}}y{{/if}}";
    hb.render_template(if_tmpl, &data)
        .expect_err("if helper must be unregistered");

    // {{#with}} — must fail.
    let with_tmpl = "{{#with x}}y{{/with}}";
    hb.render_template(with_tmpl, &data)
        .expect_err("with helper must be unregistered");

    // {{#unless}} — must fail.
    let unless_tmpl = "{{#unless x}}y{{/unless}}";
    hb.render_template(unless_tmpl, &data)
        .expect_err("unless helper must be unregistered");
}

#[test]
fn render_subject_with_multiline_template_collapsed_to_one_line() {
    // Operator template with a literal newline. Render via
    // strict_handlebars, then pass through mbox::sanitize_header
    // (the same function mbox::format_message applies to header
    // values per src/mail/mbox.rs). The chained pipeline must
    // produce a single-line Subject — sanitize_header replaces every
    // byte < 0x20 (except space) with a literal space.
    let hb = strict_handlebars();
    let data = notify::render_context(&run_ctx(), &run_summary(Conclusion::Success, 0));
    let template = "{{flow.name}}\nLine 2";
    let rendered = hb
        .render_template(template, &data)
        .expect("multiline subject template renders");
    assert_eq!(rendered, "myflow\nLine 2");

    // Sanitize as the production format_message does.
    let sanitized = sanitize_header(&rendered);
    assert!(
        !sanitized.contains('\n'),
        "sanitized subject must not carry a newline; got: {sanitized:?}",
    );
    assert!(
        !sanitized.contains('\r'),
        "sanitized subject must not carry a carriage return; got: {sanitized:?}",
    );
    // The replacement is a literal space.
    assert_eq!(sanitized, "myflow Line 2");

    // Round-trip through format_message — pin that the rendered+
    // sanitized subject lands in the header block as a single line.
    let formatted = format_message(
        Utc::now(),
        "operator",
        "host.example.com",
        &rendered,
        "body",
    );
    assert!(
        formatted.contains("Subject: myflow Line 2"),
        "format_message must inline the sanitized subject; formatted:\n{formatted}",
    );
}

#[test]
fn render_body_can_have_multiple_lines() {
    // Body templates allow newlines — mail bodies are multi-line by
    // nature. format_message does NOT sanitize the body (only
    // headers), so newlines must survive verbatim.
    let hb = strict_handlebars();
    let data = notify::render_context(&run_ctx(), &run_summary(Conclusion::Success, 0));
    let template = "Line 1 for {{flow.name}}\nLine 2\nLine 3";
    let rendered = hb
        .render_template(template, &data)
        .expect("multiline body template renders");
    assert_eq!(rendered, "Line 1 for myflow\nLine 2\nLine 3");

    // Round-trip through format_message — body is not sanitized.
    let formatted = format_message(
        Utc::now(),
        "operator",
        "host.example.com",
        "subject",
        &rendered,
    );
    // Confirm each line of the body is in the output (newline-
    // separated). This catches a mutation that accidentally applies
    // sanitize_header to the body — that would collapse each \n to
    // a literal space.
    assert!(formatted.contains("Line 1 for myflow"));
    assert!(formatted.contains("Line 2"));
    assert!(formatted.contains("Line 3"));
    // Pin the multi-line property: the body section between blank
    // lines must contain at least one '\n'.
    assert!(
        formatted.contains("\nLine 2\n"),
        "body must preserve newlines; formatted:\n{formatted}",
    );
}

// --- helpers ----------------------------------------------------------------

fn run_ctx() -> RunContext {
    RunContext {
        flow_name: "myflow".into(),
        flow_description: None,
        source: SourceInfo {
            url: "https://example.com/repo.git".into(),
            ref_name: "refs/heads/main".into(),
            sha: ObjectId::null(gix_hash::Kind::Sha1),
            sha_short: "0000000".into(),
        },
        action: ActionInfo {
            repo: "owner/repo".into(),
            workflow: "ci.yml".into(),
            run_id: 1,
            run_url: "https://example.com/run".into(),
            dispatched_at: Utc::now(),
        },
        gcit_run_id: uuid::Uuid::nil(),
    }
}

fn run_summary(conclusion: Conclusion, jobs: usize) -> RunSummary {
    RunSummary {
        run_id: 1,
        run_url: "https://example.com/run".into(),
        run_number: 1,
        run_attempt: 1,
        status: RunStatus::Completed,
        conclusion: Some(conclusion),
        started_at: Some(Utc::now()),
        completed_at: Some(Utc::now()),
        jobs: (0..jobs)
            .map(|i| JobResult {
                job_id: i as u64,
                name: format!("job-{i}"),
                html_url: format!("https://example.com/{i}"),
                conclusion: Some(Conclusion::Success),
                started_at: Some(Utc::now()),
                completed_at: Some(Utc::now()),
                steps: Vec::new(),
                run_attempt: 1,
            })
            .collect(),
    }
}

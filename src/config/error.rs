// Config-load error variants.
//
// Validate carries Vec<usize> for multi-line errors (collision pairs,
// duplicate flow names). CredentialNotFound carries the lines where the
// id was referenced. TemplateCompile carries the line of the offending
// template field. Every Display lead with `path:line:` (or `path:l1,l2:`)
// for grep-friendly output.

use std::fmt;
use std::path::PathBuf;

/// Errors raised while loading and validating a gcit config.
///
/// Display strings are operator-facing — they point at the offending
/// file, line, and field, and include a concrete fix suggestion where
/// applicable. Equivalent error categories share a leading
/// `path:line[s]:` prefix so editors and grep can navigate from the
/// terminal output to the source.
#[derive(Debug)]
pub enum ConfigError {
    /// TOML parse failure or an unknown field caught by
    /// `#[serde(deny_unknown_fields)]`. Distinct from `Validate`:
    /// `Parse` means the document is structurally broken,
    /// `Validate` means values are out of policy.
    Parse {
        path: PathBuf,
        line: usize,
        message: String,
    },

    /// A field deserialized successfully but failed a domain rule
    /// (range, regex, uniqueness, cross-field invariant). `lines`
    /// carries every relevant source line — typically a single
    /// element, but pair-wise rules (duplicate flow name,
    /// credential_id env-var collision) carry multiple.
    Validate {
        path: PathBuf,
        lines: Vec<usize>,
        flow: Option<String>,
        field: String,
        value: String,
        message: String,
        suggestion: String,
    },

    /// A `credential_id` referenced by one or more flows could not be
    /// resolved against any of the search paths. The Display lists
    /// which paths were searched and names every consuming flow so
    /// the operator can reproduce the lookup.
    CredentialNotFound {
        path: PathBuf,
        lines: Vec<usize>,
        id: String,
        searched: Vec<PathBuf>,
        consumers: Vec<String>,
    },

    /// A `credential_id` referenced by one or more flows IS present at
    /// one of the search paths but failed an on-disk invariant
    /// (`InvariantError` from the shared `credential_file` probe — bad
    /// mode, wrong owner, symlink, non-regular-file, etc.) OR could not
    /// be stat'd at all (`Probe::StatError` — EACCES on the parent
    /// directory is the common case).
    ///
    /// Distinct from `CredentialNotFound` because the field shape
    /// differs: this is a single concrete file that exists-but-is-
    /// unusable, not a list of search paths the resolver tried.
    /// `reason` carries the operator-facing render of the underlying
    /// `InvariantError` (via `InvariantError::render`) or the stat
    /// error (via `credential_file::render_stat_error` with
    /// `Context::PreFlight`); `consumers` carries flow names only —
    /// the field is no longer overloaded with the failure reason.
    CredentialInvariant {
        path: PathBuf,
        id: String,
        reason: String,
        consumers: Vec<String>,
    },

    /// A handlebars template (Discord title/description/field, mail
    /// subject/body) failed to compile. The wrapped
    /// `handlebars::TemplateError` carries the parser's own message;
    /// `flow` and `field` identify which template in the config.
    TemplateCompile {
        path: PathBuf,
        line: usize,
        flow: String,
        field: String,
        source: handlebars::TemplateError,
    },
}

fn write_lines(f: &mut fmt::Formatter<'_>, lines: &[usize]) -> fmt::Result {
    for (i, l) in lines.iter().enumerate() {
        if i > 0 {
            f.write_str(",")?;
        }
        write!(f, "{}", l)?;
    }
    Ok(())
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Parse {
                path,
                line,
                message,
            } => write!(f, "{}:{}: {}", path.display(), line, message),
            ConfigError::Validate {
                path,
                lines,
                flow,
                field,
                value,
                message,
                suggestion,
            } => {
                write!(f, "{}", path.display())?;
                if !lines.is_empty() {
                    f.write_str(":")?;
                    write_lines(f, lines)?;
                }
                f.write_str(": ")?;
                if let Some(flow) = flow {
                    write!(f, "flow '{}': ", flow)?;
                }
                write!(
                    f,
                    "{} = {:?} -- {} (fix: {})",
                    field, value, message, suggestion
                )
            }
            ConfigError::CredentialNotFound {
                path,
                lines,
                id,
                searched,
                consumers,
            } => {
                write!(f, "{}", path.display())?;
                if !lines.is_empty() {
                    f.write_str(":")?;
                    write_lines(f, lines)?;
                }
                write!(f, ": credential '{}' not found", id)?;
                if !searched.is_empty() {
                    f.write_str("; searched: [")?;
                    for (i, p) in searched.iter().enumerate() {
                        if i > 0 {
                            f.write_str(", ")?;
                        }
                        write!(f, "{}", p.display())?;
                    }
                    f.write_str("]")?;
                }
                if !consumers.is_empty() {
                    f.write_str("; required by flows: [")?;
                    for (i, c) in consumers.iter().enumerate() {
                        if i > 0 {
                            f.write_str(", ")?;
                        }
                        f.write_str(c)?;
                    }
                    f.write_str("]")?;
                }
                // Single source of truth for the id->env-var mapping
                // lives in the credential module; never re-implement
                // the rule inline.
                let env = super::credential::id_to_env(id);
                write!(
                    f,
                    " (fix: create file at <config_dir>/credentials/{} with chmod 0600, OR set {} env var, OR add LoadCredential={}:<source> to systemd unit)",
                    id, env, id
                )
            }
            ConfigError::CredentialInvariant {
                path,
                id: _,
                reason,
                consumers,
            } => {
                // `reason` already carries the credential id, the
                // failed-invariant or stat-error description, and the
                // suggested fix command (chmod / chown / etc.). Do not
                // re-prepend the path — `reason` already names it.
                write!(f, "{}: {}", path.display(), reason)?;
                if !consumers.is_empty() {
                    f.write_str(" (required by flows: [")?;
                    for (i, c) in consumers.iter().enumerate() {
                        if i > 0 {
                            f.write_str(", ")?;
                        }
                        f.write_str(c)?;
                    }
                    f.write_str("])")?;
                }
                Ok(())
            }
            ConfigError::TemplateCompile {
                path,
                line,
                flow,
                field,
                source,
            } => write!(
                f,
                "{}:{}: flow '{}': template '{}' failed to compile: {}",
                path.display(),
                line,
                flow,
                field,
                source
            ),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConfigError::TemplateCompile { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The tests below pin the literal Display output for each
    // ConfigError variant. Operators grep these strings; mutations
    // that change the prefix shape, drop a field, or invert a join
    // character (e.g. comma vs. slash between lines) surface here.

    #[test]
    fn parse_display_shape() {
        let e = ConfigError::Parse {
            path: PathBuf::from("/etc/gcit/config.toml"),
            line: 17,
            message: "expected `,` got `]`".into(),
        };
        let s = format!("{e}");
        assert_eq!(s, "/etc/gcit/config.toml:17: expected `,` got `]`");
    }

    #[test]
    fn validate_display_includes_path_lines_field_value_message_suggestion() {
        let e = ConfigError::Validate {
            path: PathBuf::from("c.toml"),
            lines: vec![3, 7],
            flow: Some("ci".into()),
            field: "source.url".into(),
            value: "not-a-url".into(),
            message: "URL malformed".into(),
            suggestion: "use https://...".into(),
        };
        let s = format!("{e}");
        // Pin every load-bearing piece without locking the exact
        // punctuation between fields.
        assert!(s.contains("c.toml:3,7:"), "path:lines prefix: {s}");
        assert!(s.contains("flow 'ci':"), "flow prefix: {s}");
        assert!(s.contains("source.url"), "field name: {s}");
        assert!(s.contains("\"not-a-url\""), "value (debug-formatted): {s}");
        assert!(s.contains("URL malformed"), "message body: {s}");
        assert!(s.contains("fix: use https://..."), "fix suggestion: {s}");
    }

    #[test]
    fn validate_display_omits_flow_prefix_when_absent() {
        // `flow` is Option — None must NOT emit "flow ':' " noise.
        let e = ConfigError::Validate {
            path: PathBuf::from("c.toml"),
            lines: vec![1],
            flow: None,
            field: "poll.jitter".into(),
            value: "1.5".into(),
            message: "out of range".into(),
            suggestion: "use a value in 0.0..=0.5".into(),
        };
        let s = format!("{e}");
        assert!(
            !s.contains("flow ':'"),
            "must not emit empty flow prefix: {s}"
        );
        assert!(
            !s.contains("flow '':"),
            "must not emit empty flow prefix: {s}"
        );
        assert!(s.contains("poll.jitter"));
        assert!(s.contains("0.0..=0.5"));
    }

    #[test]
    fn validate_display_uses_comma_between_multiple_lines() {
        // write_lines joins lines with a comma — pin the separator
        // so a mutation flipping it to "/" or " " surfaces.
        let e = ConfigError::Validate {
            path: PathBuf::from("c.toml"),
            lines: vec![10, 20, 30],
            flow: None,
            field: "f".into(),
            value: "v".into(),
            message: "m".into(),
            suggestion: "s".into(),
        };
        let s = format!("{e}");
        assert!(s.contains(":10,20,30:"), "lines must be comma-joined: {s}");
    }

    #[test]
    fn validate_display_omits_lines_when_empty() {
        // When `lines` is empty, no `:N:` segment should appear after
        // the path. Pin the absence so a mutation that always emits
        // an empty `:` segment surfaces.
        let e = ConfigError::Validate {
            path: PathBuf::from("c.toml"),
            lines: vec![],
            flow: None,
            field: "f".into(),
            value: "v".into(),
            message: "m".into(),
            suggestion: "s".into(),
        };
        let s = format!("{e}");
        assert!(s.starts_with("c.toml: "), "no lines = path-then-space: {s}");
    }

    #[test]
    fn credential_not_found_display_lists_searched_consumers_and_fix() {
        let e = ConfigError::CredentialNotFound {
            path: PathBuf::from("c.toml"),
            lines: vec![5],
            id: "github_pat".into(),
            searched: vec![
                PathBuf::from("/run/cred/github_pat"),
                PathBuf::from("/etc/gcit/credentials/github_pat"),
            ],
            consumers: vec!["flow-a".into(), "flow-b".into()],
        };
        let s = format!("{e}");
        assert!(s.contains("c.toml:5:"));
        assert!(s.contains("credential 'github_pat' not found"), "msg: {s}");
        assert!(
            s.contains("/run/cred/github_pat"),
            "must list searched paths: {s}"
        );
        assert!(
            s.contains("/etc/gcit/credentials/github_pat"),
            "must list ALL searched paths: {s}",
        );
        assert!(
            s.contains("flow-a") && s.contains("flow-b"),
            "must name every consuming flow: {s}",
        );
        // The fix hint must surface the three resolution channels.
        assert!(
            s.contains("LoadCredential"),
            "must mention LoadCredential: {s}"
        );
        assert!(
            s.contains("GCIT_CREDENTIAL_GITHUB_PAT"),
            "must derive the env var via id_to_env: {s}",
        );
        assert!(
            s.contains("chmod 0600"),
            "must include the perm-set hint: {s}",
        );
    }

    #[test]
    fn credential_invariant_display_combines_path_reason_consumers() {
        let e = ConfigError::CredentialInvariant {
            path: PathBuf::from("c.toml"),
            id: "github_pat".into(),
            reason: "credential file '/etc/gcit/credentials/github_pat' has mode 0644 (group/other readable)".into(),
            consumers: vec!["ci".into()],
        };
        let s = format!("{e}");
        assert!(s.contains("c.toml: "), "must lead with path: {s}");
        // The reason already names the credential id and offending
        // path; the Display must NOT re-prepend either.
        assert!(s.contains("mode 0644"), "must surface the reason: {s}");
        assert!(s.contains("ci"), "must list consuming flows: {s}");
        assert!(
            s.contains("required by flows:"),
            "must label the consumer list: {s}",
        );
    }

    #[test]
    fn template_compile_display_names_flow_field_and_path() {
        // TemplateError doesn't implement a stable formatted string,
        // so we pin the surrounding scaffold instead of the inner
        // body. Construct via parsing a deliberately broken template.
        let inner = handlebars::Handlebars::new()
            .render_template("{{#if x}}", &serde_json::json!({}))
            .unwrap_err();
        // The handlebars `RenderError` is distinct from `TemplateError`,
        // so we synthesize a TemplateError via the parser API directly
        // — handlebars exposes `Template::compile` for this.
        let _ = inner; // unused; we instead use compile below.

        let template_err = handlebars::Template::compile("{{#each").expect_err("compile must fail");

        let e = ConfigError::TemplateCompile {
            path: PathBuf::from("c.toml"),
            line: 42,
            flow: "ci".into(),
            field: "title".into(),
            source: template_err,
        };
        let s = format!("{e}");
        assert!(s.contains("c.toml:42:"), "must lead with path:line: {s}");
        assert!(s.contains("flow 'ci'"), "must name the flow: {s}");
        assert!(s.contains("template 'title'"), "must name the field: {s}");
        assert!(
            s.contains("failed to compile"),
            "must say what went wrong: {s}",
        );
    }

    #[test]
    fn template_compile_carries_source_for_error_chain() {
        // std::error::Error::source() returns Some only for the
        // TemplateCompile variant. Pin both the Some-ness AND the
        // None-ness for other variants so a mutation that swaps them
        // surfaces.
        let template_err = handlebars::Template::compile("{{#each").expect_err("compile fails");
        let e = ConfigError::TemplateCompile {
            path: PathBuf::from("c.toml"),
            line: 1,
            flow: "f".into(),
            field: "x".into(),
            source: template_err,
        };
        use std::error::Error;
        assert!(e.source().is_some(), "TemplateCompile must expose source");

        let parse = ConfigError::Parse {
            path: PathBuf::from("c.toml"),
            line: 1,
            message: "x".into(),
        };
        assert!(parse.source().is_none(), "Parse must NOT expose a source");
    }
}

// Credential identifier newtype + helpers.
//
// `CredentialId` validates the charset, length, and dangerous-substring
// rules at construction. It is the only way to obtain a typed id, so
// every downstream consumer (env-var lookup, file path build, validation
// error reporting) receives a value already known to be safe. Charset
// is checked via `chars().all()` (no regex crate). `id_to_env` maps via
// uppercase + s/-/_/. Leading hyphen is rejected explicitly to avoid
// CLI flag confusion.

use std::fmt;

/// Maximum length of a `credential_id`.
pub const MAX_LEN: usize = 64;

/// A validated credential identifier.
///
/// The inner string is guaranteed to:
/// - be 1..=64 ASCII characters,
/// - contain only `[A-Za-z0-9_-]`,
/// - not start with `-` (avoids confusion with CLI flags),
/// - not contain `..`, `/`, `\`, `~`, or NUL bytes (path-traversal defense),
///
/// The id is NOT a secret. `Debug` and `Display` show the literal value
/// so log lines that reference a credential by id remain useful when
/// diagnosing misconfiguration. The associated SECRET (the resolved
/// token / webhook URL) lives in `secrecy::SecretString`, never in this
/// type.
// No `Deserialize` derive — every `CredentialId` must go through
// `::new()` so the charset / length / path-traversal rules run before
// the value is constructed. The raw schema in `parse.rs` carries
// `Spanned<String>` for credential id fields and the validator calls
// `CredentialId::new` explicitly. Adding `Deserialize` here would let
// invalid ids slip through schema-driven decoders.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize)]
#[serde(transparent)]
pub struct CredentialId(String);

/// Why a candidate string failed `CredentialId::new`.
///
/// The variants are operator-facing — when validation surfaces this
/// inside `ConfigError::Validate`, the caller pulls the variant's
/// `message()` to populate the `message` field and `suggestion()` to
/// populate the fix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdError {
    Empty,
    TooLong { len: usize },
    InvalidChar { ch: char },
    LeadingHyphen,
    PathTraversal,
}

impl IdError {
    pub fn message(&self) -> String {
        match self {
            IdError::Empty => "credential_id must be non-empty".into(),
            IdError::TooLong { len } => {
                format!("credential_id is {} chars; max is {}", len, MAX_LEN)
            }
            IdError::InvalidChar { ch } => format!(
                "credential_id contains invalid character {:?}; allowed: A-Z a-z 0-9 _ -",
                ch
            ),
            IdError::LeadingHyphen => {
                "credential_id must not start with '-' (conflicts with CLI flags)".into()
            }
            IdError::PathTraversal => {
                "credential_id must not contain '..', '/', '\\', '~', or NUL".into()
            }
        }
    }

    pub fn suggestion(&self) -> String {
        match self {
            IdError::Empty | IdError::LeadingHyphen => {
                "use a non-empty id like 'github_pat' or 'discord-ci-webhook'".into()
            }
            IdError::TooLong { .. } => {
                format!("shorten the id to {} characters or fewer", MAX_LEN)
            }
            IdError::InvalidChar { .. } | IdError::PathTraversal => {
                "use only A-Z, a-z, 0-9, '_', and '-'".into()
            }
        }
    }
}

impl fmt::Display for IdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message())
    }
}

impl std::error::Error for IdError {}

impl CredentialId {
    /// Parse and validate a candidate id. Errors carry the specific
    /// failure mode so callers can produce targeted error messages
    /// without re-checking the rules themselves.
    pub fn new(s: impl Into<String>) -> Result<Self, IdError> {
        let s = s.into();
        validate_id_str(&s)?;
        Ok(CredentialId(s))
    }

    /// The validated id as a string slice. Never a secret — the secret
    /// is the resolved value, not the identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Map the id to its `GCIT_CREDENTIAL_*` env var name: uppercase
    /// + s/-/_/ + `GCIT_CREDENTIAL_` prefix. Delegates to the free
    /// `id_to_env` so the rule lives in one place — the error-display
    /// path needs to compute the same env-var name from raw strings
    /// (where no `CredentialId` exists) and must not drift from the
    /// typed-id path.
    pub fn to_env_var(&self) -> String {
        id_to_env(&self.0)
    }
}

impl fmt::Debug for CredentialId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CredentialId({:?})", self.0)
    }
}

impl fmt::Display for CredentialId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Validate a candidate credential id without constructing a
/// `CredentialId`. Used by tests; equivalent to the body of
/// `CredentialId::new` minus the construction.
pub fn validate_id(s: &str) -> Result<(), IdError> {
    validate_id_str(s)
}

fn validate_id_str(s: &str) -> Result<(), IdError> {
    if s.is_empty() {
        return Err(IdError::Empty);
    }
    if s.len() > MAX_LEN {
        return Err(IdError::TooLong { len: s.len() });
    }
    // Path-traversal substrings are checked before the per-char
    // check so the operator gets the more-specific error message.
    // The per-char loop alone would already reject these, but the
    // explicit substring check produces a sharper diagnosis.
    if s.contains("..")
        || s.contains('/')
        || s.contains('\\')
        || s.contains('~')
        || s.contains('\0')
    {
        return Err(IdError::PathTraversal);
    }
    if s.starts_with('-') {
        return Err(IdError::LeadingHyphen);
    }
    for ch in s.chars() {
        let ok = ch.is_ascii_alphanumeric() || ch == '_' || ch == '-';
        if !ok {
            return Err(IdError::InvalidChar { ch });
        }
    }
    Ok(())
}

/// Map a raw id string to its GCIT_CREDENTIAL_* env var name.
///
/// The argument is taken as `&str` so callers can pass raw,
/// already-validated strings (the validator goes id-by-id collecting
/// errors and needs to compute the env var name even before all ids
/// are validated). The transform itself does not validate — the caller
/// is responsible for ensuring the id passed validation first if the
/// result is to be trusted as an env var name.
pub fn id_to_env(id: &str) -> String {
    format!(
        "GCIT_CREDENTIAL_{}",
        id.to_ascii_uppercase().replace('-', "_")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_ids() {
        for id in [
            "github_pat",
            "discord-ci-webhook",
            "MyCred-123",
            "a",
            "12345",
            &"a".repeat(64),
        ] {
            assert!(CredentialId::new(id).is_ok(), "{} should be accepted", id);
        }
    }

    #[test]
    fn rejects_invalid_ids() {
        let cases = [
            ("", IdError::Empty),
            ("foo.bar", IdError::InvalidChar { ch: '.' }),
            ("foo bar", IdError::InvalidChar { ch: ' ' }),
            ("foo/bar", IdError::PathTraversal),
            ("foo\\bar", IdError::PathTraversal),
            ("..", IdError::PathTraversal),
            ("foo/../bar", IdError::PathTraversal),
            ("foo\0bar", IdError::PathTraversal),
            ("-foo", IdError::LeadingHyphen),
            ("~foo", IdError::PathTraversal),
            ("$foo", IdError::InvalidChar { ch: '$' }),
        ];
        for (id, expected) in cases {
            let got = CredentialId::new(id).unwrap_err();
            assert_eq!(got, expected, "for input {:?}", id);
        }
    }

    #[test]
    fn rejects_too_long() {
        let id = "a".repeat(65);
        let err = CredentialId::new(&id).unwrap_err();
        assert_eq!(err, IdError::TooLong { len: 65 });
    }

    #[test]
    fn id_to_env_var() {
        assert_eq!(
            CredentialId::new("github_pat").unwrap().to_env_var(),
            "GCIT_CREDENTIAL_GITHUB_PAT"
        );
        assert_eq!(
            CredentialId::new("discord-ci-webhook")
                .unwrap()
                .to_env_var(),
            "GCIT_CREDENTIAL_DISCORD_CI_WEBHOOK"
        );
        assert_eq!(
            CredentialId::new("MyCred-123").unwrap().to_env_var(),
            "GCIT_CREDENTIAL_MYCRED_123"
        );
    }

    #[test]
    fn collision_pairs_share_env_var() {
        // 'foo-bar' and 'foo_bar' both map to the same env var name.
        // The collision detector relies on this.
        let a = CredentialId::new("foo-bar").unwrap().to_env_var();
        let b = CredentialId::new("foo_bar").unwrap().to_env_var();
        assert_eq!(a, b);
        assert_eq!(a, "GCIT_CREDENTIAL_FOO_BAR");
    }

    // The tests below pin the literal message strings emitted by
    // `IdError::message()` and `IdError::suggestion()` so that
    // mutations replacing the string content (e.g. flipping
    // "non-empty" to "empty", or dropping the allowed-char list)
    // surface as test failures. Pin the pieces operators grep for —
    // not the entire string verbatim, since whitespace-only edits
    // shouldn't break the suite.

    #[test]
    fn id_error_empty_message_names_non_empty_constraint() {
        let m = IdError::Empty.message();
        assert!(m.contains("credential_id"), "msg: {m}");
        assert!(m.contains("non-empty"), "msg: {m}");
    }

    #[test]
    fn id_error_too_long_message_names_actual_length_and_max() {
        let m = IdError::TooLong { len: 99 }.message();
        assert!(m.contains("99"), "must include the offending length: {m}");
        assert!(
            m.contains(&MAX_LEN.to_string()),
            "must include the max constant ({MAX_LEN}): {m}",
        );
    }

    #[test]
    fn id_error_invalid_char_message_names_offending_char_and_charset() {
        let m = IdError::InvalidChar { ch: '$' }.message();
        // The Debug-format of the char includes quotes (`'$'`),
        // matching what handlebars / serde produce. Pinning the bare
        // char survives Debug-format drift; pinning the allowed-set
        // hint guards mutations that drop the suggestion.
        assert!(m.contains('$'), "must name offending char: {m}");
        assert!(
            m.contains("A-Z") && m.contains("a-z") && m.contains("0-9"),
            "must list the allowed character classes: {m}",
        );
    }

    #[test]
    fn id_error_leading_hyphen_message_explains_cli_collision() {
        let m = IdError::LeadingHyphen.message();
        assert!(m.contains("'-'"), "must name the offending char: {m}");
        assert!(
            m.contains("CLI") || m.contains("flag"),
            "must explain why (CLI flag conflict): {m}",
        );
    }

    #[test]
    fn id_error_path_traversal_message_lists_rejected_sequences() {
        let m = IdError::PathTraversal.message();
        // The four traversal sequences gcit blocks. Each must
        // appear in the message so an operator inspecting a
        // rejection knows exactly what to remove.
        assert!(m.contains(".."), "must mention `..`: {m}");
        assert!(m.contains('/'), "must mention `/`: {m}");
        assert!(m.contains('\\'), "must mention `\\`: {m}");
        assert!(m.contains('~'), "must mention `~`: {m}");
        assert!(m.contains("NUL"), "must mention NUL bytes: {m}");
    }

    #[test]
    fn id_error_suggestion_too_long_names_max_len() {
        let s = IdError::TooLong { len: 80 }.suggestion();
        assert!(
            s.contains(&MAX_LEN.to_string()),
            "must include the target length ({MAX_LEN}): {s}",
        );
    }

    #[test]
    fn id_error_suggestion_empty_or_hyphen_offers_examples() {
        // Empty and LeadingHyphen share a suggestion that names two
        // example ids — both must be present so an operator who hits
        // either failure sees concrete shapes to copy.
        for variant in [IdError::Empty, IdError::LeadingHyphen] {
            let s = variant.suggestion();
            assert!(
                s.contains("github_pat") || s.contains("discord-ci-webhook"),
                "suggestion for {variant:?} must include an example id: {s}",
            );
        }
    }

    #[test]
    fn id_error_suggestion_invalid_or_traversal_lists_charset() {
        for variant in [IdError::InvalidChar { ch: '.' }, IdError::PathTraversal] {
            let s = variant.suggestion();
            assert!(
                s.contains("A-Z") && s.contains("a-z") && s.contains("0-9"),
                "suggestion for {variant:?} must list the charset: {s}",
            );
        }
    }

    #[test]
    fn id_error_display_matches_message() {
        // `Display` delegates to `message()`; pinning the equivalence
        // catches mutations that swap the body.
        for variant in [
            IdError::Empty,
            IdError::TooLong { len: 100 },
            IdError::InvalidChar { ch: '@' },
            IdError::LeadingHyphen,
            IdError::PathTraversal,
        ] {
            let display = format!("{variant}");
            assert_eq!(
                display,
                variant.message(),
                "Display must match message() for {variant:?}",
            );
        }
    }

    #[test]
    fn credential_id_debug_includes_inner_string() {
        // Debug emits `CredentialId("foo")` so logs grep cleanly
        // on the type name AND the literal id. Mutations that flip
        // the wrapper or drop the type tag surface here.
        let id = CredentialId::new("github_pat").unwrap();
        let dbg = format!("{id:?}");
        assert!(dbg.contains("CredentialId"), "must include type tag: {dbg}");
        assert!(
            dbg.contains("\"github_pat\""),
            "must include the id value: {dbg}"
        );
    }

    #[test]
    fn credential_id_display_is_inner_string_verbatim() {
        // Display is the bare id (no wrapper, no quotes). Mutations
        // that wrap or quote surface here.
        let id = CredentialId::new("github_pat").unwrap();
        let display = format!("{id}");
        assert_eq!(display, "github_pat");
    }
}

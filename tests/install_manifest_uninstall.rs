// `gcit install` manifest writing + `gcit uninstall` reversal.
//
// **Covered today** by the in-module unit tests at
// `src/cli/install.rs::tests`:
//   * `manifest_round_trip` — verifies the
//     `Manifest { schema_version, files, user_required,
//     user_created_by_install }` shape serializes and round-trips
//     through serde.
//   * `manifest_user_required_omitted_when_none` — verifies the
//     `user_required: None` case is omitted from the JSON via
//     `skip_serializing_if = "Option::is_none"`.
//   * `manifest_user_created_round_trips` — verifies
//     `user_created_by_install: true` round-trips so uninstall can
//     decide whether to run `userdel`.
//
// **Not yet tested.** The original speculative skeletons described
// — and no test currently verifies — these contracts:
//   * Post-install on-disk state: every path listed in the manifest
//     exists on disk, the manifest's sha256 matches the actual file
//     content, and the manifest's mode matches the file mode.
//   * Uninstall happy-path: every path in the manifest is removed,
//     the manifest itself is removed, and `$STATE_DIRECTORY` is
//     preserved (operator may have a state.json from a prior daemon
//     run).
//   * Uninstall refuses to delete operator-modified files
//     (sha-mismatch) without `--force`. The runtime branch lives in
//     `cli::uninstall::run`; that file has no `#[cfg(test)]` block
//     today.
//   * Uninstall with `--force` does delete operator-modified files.
//   * The "files NOT in the manifest are NEVER touched" invariant —
//     the most important manifest contract — is not exercised by any
//     test.
//
// The 5 deleted skeletons that lived here were `let _ = ...;`
// placeholders. Activating them properly requires a tempdir-rooted
// XDG layout to drive the install + uninstall surfaces.

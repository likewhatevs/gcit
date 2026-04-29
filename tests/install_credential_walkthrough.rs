// `gcit install` credential walkthrough.
//
// **Covered today:**
//   * Per-credential print rows (URL/path/chmod blocks) are not
//     unit-tested; the walkthrough output goes through
//     `cli::install::print_credential_walkthrough` and reaches stdout
//     via `println!` calls. No automated test currently captures and
//     asserts the printed text.
//   * The credential-resolution chain that the walkthrough describes
//     to the operator IS covered by `tests/cli_check_3state.rs` —
//     specifically the steps an operator would walk through to fix a
//     missing credential are pinned by:
//       - `state1_clean_config_with_resolvable_credentials_exits_zero`
//       - `state3_declared_in_unit_but_not_currently_resolvable_exits_zero_with_note`
//       - `credential_file_mode_0600_accepted`
//       - `credential_file_mode_0644_rejected`
//       - `credentials_directory_must_be_real_directory`
//       - `step3_eacces_on_parent_dir_exits_78_with_sudo_hint`
//
// **Critical security invariant — not currently tested.** The install
// wizard must never block on stdin for credential secrets (no
// `read_password` prompt, no `read_line` against a blocking stdin in
// the credential branch). Operators copy the secret into the printed
// path out-of-band; the wizard's role is to PRINT instructions, not
// to capture the secret. A regression that adds a stdin prompt would
// silently weaken the credential-handling model — secrets would land
// in shell history or accidentally render to logs. No test in the
// tree currently verifies this property; it should be asserted via
// a stdin-closed run that completes instead of blocking.
//
// **Not yet tested.** The original speculative skeletons described:
//   * Per-kind print exact-string assertions (GitHub PAT URL,
//     Discord webhook navigation breadcrumb, `chmod 0600 <path>`
//     command).
//   * Walkthrough deduplication when two flows share a credential id.
//   * "Already configured" short-circuit when the credential resolves
//     successfully via step 1 / step 3 (the wizard should print a
//     ✓-prefixed one-liner instead of the full block).
//
// The 5 deleted skeletons that lived here were `let _ = ...;`
// placeholders without assertions. Activating them as real tests is
// independent work.

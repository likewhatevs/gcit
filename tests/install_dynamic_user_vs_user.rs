// `gcit install` DynamicUser=yes vs User=gcit/Group=mail switch.
//
// **Covered today** by the in-module unit tests at
// `src/systemd/unit.rs::tests`:
//   * `service_unit_uses_dynamic_user_when_no_local_mail` — verifies
//     that a Discord-only config renders the unit with
//     `DynamicUser=yes`.
//   * `service_unit_uses_static_user_when_local_mail_present` —
//     verifies that a config with at least one `local_mail` destination
//     renders the unit with `User=gcit, Group=mail,
//     SupplementaryGroups=mail`.
//   * `service_unit_emits_load_credential_lines_sorted` — verifies
//     LoadCredential= line emission order.
//   * `service_unit_includes_every_hardening_directive` — verifies
//     the full hardening profile is present.
//   * `service_unit_threads_binary_path_into_exec_lines` — verifies
//     `ExecStart=` and `ExecReload=` use the resolved current-exe path.
//
// The `--user` + `local_mail` combination is rejected up-front by
// `cli::install::run` (returns `EX_USAGE=64` with an explanatory
// error). That branch has no automated test today.
//
// **Not yet tested.** The original speculative skeletons described:
//   * The `useradd --system --no-create-home --shell /usr/sbin/nologin
//     -G mail gcit` invocation and its `E_NAME_IN_USE` (exit 9)
//     short-circuit. The runtime path lives in
//     `cli::install::ensure_static_user`; the mail-group preflight
//     and ENOENT wrap have no automated test.
//   * The uninstall-side `userdel` decision: `cli::uninstall::run`
//     should NOT remove the gcit account when the manifest's
//     `user_created_by_install: false` indicates a pre-existing
//     account. Manifest round-trip IS tested in
//     `cli::install::tests::manifest_user_created_round_trips`, but
//     the uninstall consumer of that field is not exercised by any
//     test.
//   * The path-preview rendering of the chosen service-user model
//     (`print_path_preview`'s `has_local_mail` branch) — the new
//     `# System users gcit will create` block has no automated test.
//
// The 5 deleted skeletons that lived here were `let _ = ...;`
// placeholders. Activating them as real tests requires either a
// pty-driven harness for the install path or refactoring
// `print_path_preview` to be unit-testable; both are independent work.

// `gcit install` path preview + confirmation prompt.
//
// **Covered today:** none of the contracts the deleted skeletons
// described are tested today. The preview surface is owned by
// `cli::install::print_path_preview`; that function emits via
// `println!` calls and has no test that captures and asserts the
// output. The confirmation gate immediately follows in
// `cli::install::run` (read of stdin against the operator's `y`
// reply); that branch has no automated test.
//
// **Not yet tested.** The original speculative skeletons described:
//   * Preview lists every absolute path the install will create
//     before any write happens (file-list -> manifest -> systemd
//     dirs -> user model -> side effects, in deterministic order).
//   * Refusing the confirmation prompt (`n` or stdin closed) results
//     in zero filesystem writes.
//   * `--non-interactive` skips the prompt and writes without
//     printing a `[y/n]` line.
//   * The post-install banner (`cli::install::print_post_install`)
//     prints the exact `systemctl daemon-reload && systemctl
//     enable --now gcit.socket gcit.service` and `journalctl -u gcit
//     -f` commands operators copy-paste — drift in those strings
//     would silently break operator workflows.
//   * The `# System users gcit will create` block lists the planned
//     `useradd` invocation when `has_local_mail` is true, before
//     the confirmation gate.
//
// The 4 deleted skeletons that lived here were `let _ = ...;`
// placeholders. Activating them as real tests requires either a
// pty-driven harness or a refactor of `print_path_preview` to write
// into a `&mut dyn Write` for capture. The `print_post_install`
// exact-string assertion is the cheapest of the bunch to activate
// (no pty needed) and is the most operator-visible drift target.

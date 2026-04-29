// `gcit install` idempotency: refuse silent overwrite without `--force`.
//
// **Covered today:** none of the contracts the deleted skeletons
// described are tested today. The refuse-without-force path lives in
// `cli::install::run` (the path-preview iteration calls
// `o.path.exists()`; `--force = false` against any existing path
// aborts with `EX_CONFIG=78`). The atomic-write durability shared
// with the state writer is exercised by tests in `state_*.rs`, but
// THAT does not exercise the install branch's existence check or the
// exit-78 surface.
//
// **Not yet tested.** The original speculative skeletons described:
//   * Second-install-without-force returns `EX_CONFIG=78` and names
//     the existing file in stderr.
//   * Second-install-with-force overwrites and re-records the
//     manifest's sha to the canonical content (not the operator-
//     edited content).
//   * `--user` and `--system` install scopes are independent: each
//     writes to its own paths and neither triggers the other's
//     refuse-overwrite branch.
//   * Bare `gcit install` (no `--user`/`--system`) exits `EX_USAGE=64`
//     via clap's required-mutex `ArgGroup`.
//
// The 4 deleted skeletons that lived here were `let _ = ...;`
// placeholders. The clap mutex case is the cheapest of the four to
// activate (single `assert_cmd` invocation); the other three need
// a tempdir-rooted XDG layout to drive the install path safely.

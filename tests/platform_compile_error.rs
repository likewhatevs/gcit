// gcit refuses to build on non-Linux/non-systemd targets.
// `lib.rs (compile_error on non-Linux)`.
//
// On Linux this test is a NO-OP smoke check that the crate built. The actual
// non-Linux assertion is enforced by `compile_error!()` in src/lib.rs and
// gated by `#[cfg(not(target_os = "linux"))]` — i.e., builds for macOS or
// Windows fail at compile time, not runtime. There is no way to assert that
// from inside a successful Linux test run, so the proof lives in CI:
// `.github/workflows/ci.yml` runs `cargo check --target x86_64-apple-darwin`
// in a job that MUST FAIL with the compile_error! message.
//
// Skeleton — flip the #[ignore] when CI is wired to mirror the
// linux-only smoke assertion here.

#[test]
#[ignore = "requires CI to run the linux-only build smoke"]
fn linux_target_compiles() {
    // If this test binary built on Linux, the compile_error! on non-Linux
    // didn't fire. That's the postcondition we want.
    assert_eq!(std::env::consts::OS, "linux");
}

// CI-side check (informational; not a Rust test):
//
//   - name: Verify non-Linux build fails
//     run: |
//       output=$(cargo check --target x86_64-apple-darwin 2>&1 || true)
//       echo "$output" | grep -q 'gcit is Linux-only' \
//         || (echo "compile_error did not fire" && exit 1)
//
// The exact compile_error message is owned by src/lib.rs; the CI check
// must be updated together with any change to that string.

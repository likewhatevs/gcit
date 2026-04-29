// Build script: emits VERGEN_GIT_SHA env var for `gcit --version` to consume
// via env!(). Configured to be idempotent — when the gcit source tree is not
// a git repo (e.g. extracted from a tarball), vergen-gix falls back to
// placeholder values via cargo:warning rather than failing the build.

use anyhow::Result;
use vergen_gix::{Emitter, GixBuilder};

fn main() -> Result<()> {
    let gix = GixBuilder::all_git()?;
    Emitter::default().add_instructions(&gix)?.emit()?;
    Ok(())
}

# gcit development recipes

mod scripts

# Run all tests
test:
    cargo nextest run

# Run tests with coverage (90% line gate)
coverage: ci-coverage

# Run mutation testing and print score
mutants:
    cargo mutants --json --output mutants.out
    @just scripts::mutants-score

# Lint (same checks as CI)
lint: ci-fmt ci-clippy

# Build mdbook
book: ci-docs

# Serve mdbook locally
book-serve:
    mdbook serve book --open

# Check config validity
check config="config.toml":
    cargo run -- check --config {{config}}

# Debug build
build:
    cargo build

# --- CI recipes (called by .github/workflows/ci.yml) ---

# Format check
ci-fmt:
    cargo fmt --all -- --check

# Clippy
ci-clippy:
    cargo clippy --workspace --all-targets -- -D warnings

# Test + coverage with 90% region gate (lcov output gitignored)
ci-coverage:
    cargo llvm-cov nextest \
        --workspace \
        --lcov \
        --output-path lcov.info \
        --fail-under-regions 90

# Musl static build + link assertion
ci-musl:
    #!/usr/bin/env bash
    set -euo pipefail
    rustup target add x86_64-unknown-linux-musl
    cargo build --release --target x86_64-unknown-linux-musl
    BIN=target/x86_64-unknown-linux-musl/release/gcit
    file "$BIN"
    file "$BIN" | grep -qE "statically linked|static-pie linked"

# Cargo audit + cargo deny
ci-audit:
    cargo audit --deny warnings
    cargo deny check advisories bans sources licenses

# mdbook build + test
ci-docs:
    mdbook build book
    mdbook test book

# Render systemd unit via `gcit install --dry-run`, score it with systemd-analyze (<= 1.0)
ci-sd-analyze: build
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p target/sd-analyze
    ./target/debug/gcit install --user --dry-run --config tests/resources/config/minimal.toml > target/sd-analyze/unit.service
    SYSTEMD_LOG_LEVEL=warning systemd-analyze --offline=true security --no-pager --root=/ target/sd-analyze/unit.service | tee target/sd-analyze/sd-analyze.txt
    just scripts::sd-analyze-score target/sd-analyze/sd-analyze.txt

# Per-PR mutation testing (--in-diff)
ci-mutants:
    #!/usr/bin/env bash
    set -euo pipefail
    git diff origin/main -- . > pr.diff
    cargo mutants --in-diff pr.diff --json --output mutants.out

# Gate on missed mutants (runs after ci-mutants)
ci-mutants-gate:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -f mutants.out/outcomes.json ]; then
        echo "no outcomes; skipping score computation"
        exit 0
    fi
    just scripts::mutants-score
    missed=$(jq -r '.missed' mutants.out/outcomes.json)
    if [ "$missed" -gt 0 ]; then
        echo "::error::cargo-mutants: $missed missed mutant(s)"
        exit 1
    fi

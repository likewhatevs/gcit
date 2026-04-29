# gcit development recipes

mod scripts

# Run all tests
test:
    cargo nextest run

# Run tests with coverage
coverage:
    cargo llvm-cov nextest --lcov --output-path lcov.info --fail-under-lines 90
    @echo "Coverage report: lcov.info"

# Run mutation testing and print score
mutants:
    cargo mutants --json --output mutants.out
    @just scripts::mutants-score

# Lint (same checks as CI)
lint:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings

# Build mdbook
book:
    mdbook build book

# Serve mdbook locally
book-serve:
    mdbook serve book --open

# Check config validity
check config="config.toml":
    cargo run -- check --config {{config}}

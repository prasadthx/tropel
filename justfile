# ─── Tropel justfile ──────────────────────────────────────
# Recipes: fmt, lint, test, bench, run, build

fmt:
    cargo fmt --check

fmt-fix:
    cargo fmt

lint:
    cargo clippy --workspace -- -D warnings

test:
    cargo test --workspace

test-no-run:
    cargo test --workspace --no-run

bench:
    cargo bench --workspace

build:
    cargo build --release

run collection: build
    ./target/release/tropel run {{collection}}

deny:
    cargo deny check

audit:
    cargo audit

check: fmt lint test-no-run

ci: fmt lint test

default: check

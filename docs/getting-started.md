# Getting Started

## Build

```bash
# Debug build (fast iteration)
cargo build

# Release build (recommended for real tests)
cargo build --release
```

Building Tropel requires a **C toolchain**: the embedded QuickJS runtime is
compiled from C source by `rquickjs-sys` on every build.

- **Linux:** `build-essential` (`gcc`/`clang`)
- **macOS:** Xcode Command Line Tools
- **Windows:** MSVC Build Tools

Everything else is pure Rust. On Windows MSVC the binary links ~300 crates
(wasmtime, oxc, aws-lc-sys, …) — `.cargo/config.toml` disables PDB generation
to stay under the MSVC program-database limit.

## Run your first test

```bash
# Postman collection → load test
./target/release/tropel run examples/collections/simple-api.json -u 10 -d 30s

# HAR file
./target/release/tropel run examples/har/01_get_posts.har -u 5 -d 10s

# k6 script
./target/release/tropel run script.js -u 5 -d 10s
```

## Preview without running

```bash
# Show how the input is resolved and what will execute
./target/release/tropel inspect collection.json

# List the input formats available on this binary
./target/release/tropel extensions
```

## Exit codes

- `0` — run completed and all thresholds passed
- `1` — run failed (error) or a threshold was breached (`abortOnFail` semantics match k6)

## Next steps

- [CLI reference](cli.md)
- [Input formats](inputs.md)
- [Execution modes](executors.md)

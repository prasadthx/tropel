# Contributing to Tropel

We welcome contributions! Here's how to get started.

## Prerequisites

- Rust (stable) — install via [rustup](https://rustup.rs/)
- **C compiler** (QuickJS compiles C source):
  - Linux: `build-essential` (`gcc`, `make`)
  - macOS: `xcode-select --install`
  - Windows: MSVC build tools (`cl.exe`)

## Setting Up

```bash
# Clone the repo
git clone https://github.com/tropel/tropel
cd tropel

# Build
cargo build

# Run tests
cargo test --workspace
```

## Development Workflow

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes
4. Run `cargo fmt` and `cargo clippy`
5. Run `cargo test --workspace`
6. Submit a pull request

## Code Style

- Follow Rust standard formatting (we use `rustfmt`)
- Run `cargo fmt --check` before committing
- All public items should have doc comments
- Add tests for new functionality

## Testing

- Write unit tests in the same file (use `#[cfg(test)] mod tests`)
- Use `insta` for snapshot testing where appropriate
- Use `wiremock` for HTTP integration tests

## License

By contributing, you agree that your contributions will be licensed under the MIT OR Apache-2.0 license.

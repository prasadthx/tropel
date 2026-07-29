# Tropel 🔥

> *Spanish: "a rushing throng; in droves"*

**Tropel** is a high-performance, open-source load-testing framework built in Rust. It runs **Postman collections** as load tests with full `pm.*` fidelity, using a native Rust hot path with an embedded QuickJS engine for script execution.

## Quick Start

```bash
# Build Tropel
cargo build --release

# Run a Postman collection as a load test
./target/release/tropel run examples/collections/simple-api.json \
  --vus 10 \
  --duration 30s \
  --env BASE_URL=https://api.example.com

# Run with JSON output
./target/release/tropel run collection.json \
  --reporter json \
  --output results.json
```

## Features

- ✅ **Postman Collection v2.1/v2.0 support** — full `pm.*` API fidelity
- 🚀 **Native Rust HTTP engine** — connection pooling, HTTP/2, TLS (rustls)
- ⚡ **Embedded QuickJS** — lightweight ES2020+ JavaScript per VU
- 📊 **HDR Histogram metrics** — p50/p90/p95/p99 latencies
- 🎯 **Thresholds** — pass/fail exit codes
- 🔧 **Extensible** — xk6-style extension system (protocols, outputs, JS modules)
- 📤 **Multiple reporters** — stdout summary, JSON, CSV
- 🔒 **Full auth support** — Bearer, Basic, API Key, Digest, OAuth1, OAuth2, AWS SigV4, Hawk

## Architecture

```
                    ┌─────────────────────┐
                    │   tropel (CLI)       │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │   tropel-engine      │
                    │   Orchestration      │
                    └──┬───────┬──────┬───┘
                       │       │      │
              ┌────────▼──┐ ┌──▼───┐ ┌▼─────────┐
              │  Input    │ │Exec. │ │  Report   │
              │ Adapters  │ │Sched.│ │  stdout/  │
              └─────┬─────┘ └──┬───┘ │  json/csv │
                    │          │     └───────────┘
              ┌─────▼─────┐ ┌──▼───────────┐
              │ Postman    │ │  Protocols   │
              │ Collection │ │  (HTTP, ...) │
              │  →Scenario │ └──────┬───────┘
              └────────────┘        │
                           ┌────────▼────────┐
                           │  Native Builtins │
                           │  (crypto/hash/   │
                           │   encode/assert) │
                           └────────┬────────┘
                                    │
                           ┌────────▼────────┐
                           │  tropel-pm      │
                           │  pm.* bridge    │
                           └────────┬────────┘
                                    │
                           ┌────────▼────────┐
                           │  tropel-js      │
                           │  QuickJS per VU │
                           └─────────────────┘
```

## Usage

### CLI Options

```bash
tropel run <input> [options]

Arguments:
  input                     Path to Postman Collection JSON file

Options:
  -u, --vus <N>             Number of virtual users (default: 1)
  -d, --duration <D>        Test duration (e.g., "30s", "5m", "1h")
  -e, --env <KEY=VALUE>     Environment variable (can be repeated)
  -E, --env-file <PATH>     Environment file (JSON)
  -D, --data-file <PATH>    Data file (CSV or JSON)
  -r, --reporter <FORMAT>   Report format: stdout, json, csv (default: stdout)
  -o, --output <PATH>       Output file path
  -t, --threshold <EXPR>    Threshold expression (can be repeated)
  -k, --insecure            Insecure TLS (skip cert verification)
  -m, --mode <MODE>         Run mode: constant-vus, ramping-vus,
                            shared-iterations, arrival-rate (default: constant-vus)
  -v, --verbose             Show verbose output
```

### Execution Modes

| Mode | Description |
|------|-------------|
| `constant-vus` | Fixed number of VUs for a duration |
| `ramping-vus` | Ramp VUs up/down through stages |
| `shared-iterations` | Run a fixed number of iterations across all VUs |
| `arrival-rate` | Maintain a constant request rate |

### Example: Ramping VUs

```bash
tropel run collection.json \
  --mode ramping-vus \
  --start-vus 1 \
  --stages '[{"duration":"30s","target":10},{"duration":"1m","target":10},{"duration":"30s","target":0}]' \
  --reporter stdout
```

### Example: Thresholds

```bash
tropel run collection.json \
  --vus 100 \
  --duration 1m \
  --threshold "http_req_duration p95 < 500" \
  --threshold "checks rate > 0.99"
```

## Project Structure

```
tropel/
├── Cargo.toml                    # Workspace root
├── crates/
│   ├── tropel/                   # CLI binary
│   ├── tropel-engine/            # Orchestration facade
│   ├── tropel-core/              # Shared domain types
│   ├── tropel-collection/        # Postman Collection parser
│   ├── tropel-variables/         # {{var}} resolution
│   ├── tropel-js/                # QuickJS host
│   ├── tropel-native/            # Native builtins
│   ├── tropel-pm/                # pm.* API bridge
│   ├── tropel-http/              # HTTP protocol executor
│   ├── tropel-executor/          # VU scheduler
│   ├── tropel-metrics/           # HDR histogram aggregation
│   ├── tropel-report/            # Reporters
│   ├── tropel-ext/               # Extension SDK
│   ├── tropel-build/             # Custom binary builder
│   ├── inputs/tropel-input-postman/
│   └── extensions/
│       ├── tropel-x-grpc/        # (future) gRPC protocol
│       ├── tropel-x-websocket/   # (future) WebSocket protocol
│       └── tropel-x-prometheus/  # (future) Prometheus output
├── js/                           # Vendored JS libraries
│   ├── pm-api/                   # pm.* JS glue
│   ├── chai/                     # Assertion library
│   ├── lodash/                   # Utility library
│   └── cryptojs-shim/            # CryptoJS-compatible API
└── examples/
    └── collections/              # Sample collections
```

## Build Dependencies

Building Tropel requires a **C toolchain** (QuickJS compiles C source):

- **Linux:** `build-essential` (`gcc`/`clang`)
- **macOS:** Xcode Command Line Tools
- **Windows:** MSVC Build Tools

Everything else is pure Rust.

## Development

```bash
# Format code
cargo fmt

# Lint
cargo clippy --workspace -- -D warnings

# Run tests
cargo test --workspace

# Build release
cargo build --release

# Run benchmarks
cargo bench
```

## License

Licensed under either of:
- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))

at your option.

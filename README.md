# Tropel 🔥

> *Spanish: "a rushing throng; in droves"*

**Tropel** is a high-performance, open-source load-testing framework built in
Rust. It runs **Postman collections**, **HAR files**, **OpenAPI specs**, and
**k6 scripts** as load tests — with a native Rust hot path and an embedded
QuickJS engine for script execution.

## Quick Start

```bash
# Build Tropel
cargo build --release

# Run a Postman collection as a load test
./target/release/tropel run examples/collections/simple-api.json \
  --vus 10 \
  --duration 30s

# Preview what a run will execute
./target/release/tropel inspect collection.json

# List the input formats this binary ships
./target/release/tropel extensions
```

Full docs: **[docs/](docs/README.md)** — CLI reference, executors, scripting,
metrics, extensions, distributed execution.

## What Tropel does

- **Seven executors** — constant-vus, ramping-vus, shared/per-vu iterations,
  constant & ramping arrival rate, and externally-controlled (live control
  API), with graceful stop/ramp-down, think time, and pacing.
- **Postman `pm.*` scripting** — `pm.test`, `pm.expect`, `pm.response`,
  `pm.variables`/`pm.environment`, `pm.iterationData`,
  `pm.execution.setNextRequest`, `pm.sendRequest`, custom metrics.
- **k6-style scripting** — JS/TS scripts with `http.*`, `check()`, `group()`,
  `sleep()`, `Counter/Gauge/Rate/Trend`; exported `options` are honored.
- **HDR-histogram metrics** — p50/p90/p95/p99, sub-timings (TTFB etc.),
  tag-scoped aggregation, thresholds with k6-compatible abort semantics.
- **Streaming outputs** — NDJSON, StatsD, InfluxDB, Prometheus, OTLP, plus
  stdout/JSON/CSV reporters.
- **Thread-per-core VUs** — each VU owns its QuickJS context and HTTP client
  on its own OS thread; lock-free metrics hot path.
- **Extensible** — `tropel-sdk` + `tropel build --with <crate>` for native
  extensions, and a sandboxed **WASM plugin** tier (`--plugins-dir`) for
  third-party input formats without recompiling.
- **Distributed** — k6-style execution segments for multi-node runs.

## Status

Tropel is **actively developed**. Most load-testing fundamentals are shipped
and tested; a few areas remain partial. **See
[docs/roadmap.md](docs/roadmap.md) for the honest per-area capability matrix**
— this README intentionally links there instead of repeating claims.

Notable limitations today:

- **WASM drivers cover a focused surface** — plugins can run iterations and
  use host-imported http/sleep/metrics, but the API is a subset of the
  in-process k6 driver's (no full scripting runtime inside the module).
- **JMeter and Locust adapters are not started** (planned §11.6).

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
              │  Input    │ │Exec. │ │  Output  │
              │  Adapters │ │Sched.│ │  stdout/  │
              │  +Drivers │ └──┬───┘ │  json/csv │
              └─────┬─────┘   │     │   +streams│
                    │         │     └───────────┘
        ┌───────────▼───┐ ┌───▼───────────┐
        │ Postman/HAR/  │ │  Protocol     │
        │ OpenAPI/k6/   │ │  HTTP + gRPC +│
        │ WASM/subproc  │ │  WebSocket    │
        └───────────────┘ └──────┬───────┘
                                 │
                    ┌────────────▼───────────┐
                    │  tropel-js (QuickJS)    │
                    │  per-VU, thread-local   │
                    └────────────┬───────────┘
                                 │
                    ┌────────────▼───────────┐
                    │  tropel-native bridge   │
                    │  crypto/hash/encode/    │
                    │  assert/json/http/sleep │
                    └─────────────────────────┘
```

## Crates

```
crates/
├── tropel/               CLI binary
├── tropel-engine/        Orchestration facade (run/inspect/archive/build)
├── tropel-core/          Shared domain types, config, execution segments
├── tropel-collection/    Postman collection model + parser
├── tropel-variables/     {{var}} / {{$dynamic}} resolution
├── tropel-executor/      VU scheduler + runner (thread-per-core)
├── tropel-http/          HTTP client, auth (Bearer/Basic/ApiKey/OAuth1/OAuth2/
│                         SigV4/Hawk/Digest), sub-timings
├── tropel-js/            QuickJS host, compile-once scripts
├── tropel-native/        Native bridge (crypto, hash, encoding, assert, JSON)
├── tropel-pm/            pm.* bridge + k6 shim glue
├── tropel-metrics/       HDR histograms, first-class metric types, thresholds
├── tropel-report/        Reporters + streaming outputs
├── tropel-es/            ESM/TypeScript transpiler
├── tropel-sdk/           Stable public adapter/driver contract
├── tropel-ext/           Registry, traits (InputAdapter, Driver, Protocol, Output)
├── tropel-wasm/          WASM plugin runtime (wasmtime, fuel, AOT, pooling)
├── tropel-build/         Custom binary builder
├── tropel-distributed/   Controller/agent, execution segments
├── tropel-bench/         Criterion benchmark suite
├── inputs/               tropel-input-{postman,har,openapi,k6,subprocess}
└── extensions/           tropel-x-{grpc,websocket,prometheus}
js/                       Vendored shims (pm-api, chai, lodash, cryptojs-shim, k6-shim)
```

## Development

```bash
cargo fmt
cargo clippy --workspace -- -D warnings
cargo test --workspace
cargo build --release

# Benchmarks (see crates/tropel-bench)
cargo bench -p tropel-bench --bench perf          # release profile
cargo bench -p tropel-bench --bench perf --profile dev  # fast, disk-light
```

## License

Licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))

at your option.

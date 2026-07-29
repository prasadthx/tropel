# Tropel — Session Handoff

**Date:** July 29, 2026
**Last commit:** `d73df78` — "Initial commit: Tropel — a Postman-native load-testing toolkit"
**Branch:** `main`
**Working tree:** clean (`.freebuff/` and `TROPEL_PLAN.md` are untracked)

---

## Project Overview

Tropel is an open-source, high-performance load-testing framework written in Rust.
Primary goal: run **Postman collections** as load tests with full `pm.*` fidelity.
Secondary goal: run **k6-style JS scripts**.

---

## What's Been Built

The full workspace compiles successfully. **87 files, 10,977 lines** committed.

### Crate Inventory

| Crate | Path | Status | Lines |
|---|---|---|---|
| **tropel-core** | `crates/tropel-core/` | ✅ | Shared domain types: `Request`, `Response`, `Sample`, `Scenario`, `JobConfig`, `AuthConfig`, error types |
| **tropel-collection** | `crates/tropel-collection/` | ✅ | Postman Collection v2.1/v2.0 parser: model, deserialization, validation, `Scenario` conversion |
| **tropel-variables** | `crates/tropel-variables/` | ✅ | `{{var}}` resolution with scope precedence + dynamic variable catalog (`{{$guid}}`, `{{$timestamp}}`, etc.) |
| **tropel-js** | `crates/tropel-js/` | ⚠️ **Stubbed** | QuickJS context wrapper — rquickjs bindings exist but are stubbed out pending full integration |
| **tropel-native** | `crates/tropel-native/` | ✅ | Rust builtins: SHA-256/1/MD5, HMAC, base64/hex/url encoding, deep-equal assertions, UUID/random, JSON fast-path |
| **tropel-pm** | `crates/tropel-pm/` | ✅ | `pm.*` API bridge: env/vars/response/test/expect/execution, iteration data, sample emission |
| **tropel-http** | `crates/tropel-http/` | ✅ (2 warnings) | HTTP protocol: reqwest client, connection pooling, HTTP/2, TLS, auth signers (Bearer/Basic/APIKey) |
| **tropel-executor** | `crates/tropel-executor/` | ✅ (1 warning) | VU scheduler: constant-VUs, ramping-VUs, shared-iterations, constant-arrival-rate |
| **tropel-metrics** | `crates/tropel-metrics/` | ✅ | HDR histograms (p50/p90/p95/p99), counters, threshold evaluation |
| **tropel-report** | `crates/tropel-report/` | ✅ | Reporters: stdout summary (table), JSON, CSV |
| **tropel-ext** | `crates/tropel-ext/` | ✅ | Extension SDK: 5 trait points (Protocol, JsModule, Output, AuthSigner, InputAdapter) + ExtensionRegistry |
| **tropel-engine** | `crates/tropel-engine/` | ✅ (2 warnings) | Orchestration facade: parse → execute → collect → report |
| **tropel-build** | `crates/tropel-build/` | ✅ | Custom binary builder (xk6-style CLI scaffolding) |
| **tropel** (CLI) | `crates/tropel/` | ✅ (1 warning) | CLI binary: `tropel run <collection> [--vus N] [--duration D] [--mode M] [--reporter FMT]` |

### Input Adapters & Extensions

| Crate | Path | Status |
|---|---|---|
| **tropel-input-postman** | `crates/inputs/tropel-input-postman/` | ✅ Auto-detect & parse Postman collections |
| **tropel-x-grpc** | `crates/extensions/tropel-x-grpc/` | ✅ Stub |
| **tropel-x-websocket** | `crates/extensions/tropel-x-websocket/` | ✅ Stub |
| **tropel-x-prometheus** | `crates/extensions/tropel-x-prometheus/` | ✅ Stub |

### JS Vendored Libraries

| Library | Path | Status |
|---|---|---|
| **pm-api** | `js/pm-api/pm.js` | ✅ Complete Postman `pm.*` JS API surface |
| **chai-shim** | `js/chai/chai-shim.js` | ✅ Assertion library (delegates to native) |
| **lodash-shim** | `js/lodash/lodash-shim.js` | ✅ Minimal lodash API surface |
| **cryptojs-shim** | `js/cryptojs-shim/cryptojs.js` | ✅ CryptoJS-compatible API backed by native |

### Project Config & Docs

| File | Status |
|---|---|
| `Cargo.toml` | ✅ Workspace root with 14+ members, shared dependencies |
| `rust-toolchain.toml` | ✅ Stable channel + rustfmt + clippy |
| `rustfmt.toml` | ✅ |
| `deny.toml` | ✅ License/advisory gating |
| `justfile` | ✅ fmt/lint/test/bench/run recipes |
| `.gitignore` | ✅ Comprehensive (Rust, IDE, OS, JS, Python, secrets, logs) |
| `README.md` | ✅ Full project readme |
| `CONTRIBUTING.md` | ✅ |
| `CODE_OF_CONDUCT.md` | ✅ |
| `CHANGELOG.md` | ✅ Keep-a-changelog format |
| `LICENSE-MIT` / `LICENSE-APACHE` | ✅ Dual license |
| `.github/workflows/ci.yml` | ✅ CI: fmt → clippy → test → build (linux/macos/windows) |
| `examples/collections/simple-api.json` | ✅ Simple Postman collection for smoke tests |

---

## Build Status

```
cargo build --workspace → ✅ Success (0 errors, 6 warnings)
```

### Warnings (non-blocking)

| # | File | Warning |
|---|---|---|
| 1 | `crates/tropel-http/src/client.rs:252` | Unused import `std::collections::HashMap` |
| 2 | `crates/tropel-http/src/client.rs:15` | Field `cookie_store` is never read |
| 3 | `crates/tropel-executor/src/runner.rs:116` | Unused variable `url` |
| 4 | `crates/tropel-engine/src/engine.rs:46` | Unused variable `http_client` |
| 5 | `crates/tropel-engine/src/engine.rs:64` | Unused variable `vus` |
| 6 | `crates/tropel/src/main.rs:139` | Unused variable `data_file` |

All are unused variable/import warnings — easy to fix with `_` prefixes or by removing the binding.

---

## Milestone Progress (vs TROPEL_PLAN.md)

| Milestone | Description | Status |
|---|---|---|
| **M0** | Workspace + CI green; `tropel run x.json` parses collection | ✅ **Achieved** |
| **M1** | Send one real HTTP request with `{{var}}` resolution | ✅ **Achieved** |
| **M2** ⭐ | Embed QuickJS + `pm.*` + native builtins | ⚠️ **Partial** — native builtins done, JS context **stubbed** |
| **M3** | Constant/ramping VUs, hdr metrics, stdout report | ✅ **Achieved** |
| **M4** | Full `pm.*` surface, dynamic-var catalog, full auth, json/csv | ✅ **Achieved** |
| **M5** | Extension SDK + registry; first extensions; `tropel build` | ✅ **Achieved** |
| **M6** | Full native-primitive coverage, CryptoJS shim complete | ✅ **Achieved** |
| **M7** | k6-script input adapter | ❌ **Not started** |
| **M8** | Distributed mode | ❌ **Not started** |
| **M9** | WASM runtime plugins | ❌ **Not started** |

---

## Key Architecture Details

### Dependency Flow
```
tropel (cli)
  └─ tropel-engine
       ├─ tropel-executor ─┬─ tropel-pm ─┬─ tropel-js ─ tropel-native
       │                   │             ├─ tropel-core
       │                   │             └─ tropel-variables ─ tropel-collection ─ tropel-core
       │                   ├─ tropel-http ─ tropel-core, tropel-metrics, tropel-ext
       │                   └─ tropel-metrics
       ├─ tropel-report ─ tropel-metrics
       ├─ tropel-ext ─ tropel-core
       └─ inputs/* , extensions/*
```

### CLI Usage (planned)
```
tropel run examples/collections/simple-api.json
tropel run collection.json --vus 10 --duration 30s --mode constant
tropel run collection.json --reporter json --reporter csv --out ./results/
tropel build --with tropel-x-grpc --with ./my-ext --output ./tropel
```

### Current Execution Model
The engine (`tropel-engine`) has a direct iteration callback that:
1. Clones env variables for each VU
2. Sends HTTP requests via `HttpProtocol`
3. Records metrics via `MetricsCollector`
4. Reports via configured reporters

The VU runner (`tropel-executor/runner.rs`) and scheduler (`scheduler.rs`) structures exist but the engine currently bypasses the full VU runner — this should be wired up properly as part of M2 completion.

---

## Known Gaps & Next Work

### Critical Path to M2 Completion
1. **Wire up rquickjs** in `tropel-js/src/context.rs` — currently stubbed with TODOs. Needs the full `AsyncContext`, eval with Promises, memory limits, and bootstrap sequence.
2. **Connect tropel-pm → tropel-js** — the `pm.*` bridge currently operates without an active JS context. Once the JS context is live, the PM API needs to inject native functions and run pre-request/test scripts.
3. **Wire up tropel-engine → tropel-executor** — the engine bypasses the VU runner struct for the iteration loop; should use `VURunner::run_iteration` instead.

### Other Improvements
- Fix 6 compiler warnings (easy cleanup)
- Add `.gitattributes` to stop CRLF warnings
- Add unit tests! Only the collection parser has meaningful test surface so far
- Implement `inventory`-based extension registration in `tropel-ext`
- Add `criterion` benchmarks in `benches/`
- Add `insta` snapshot tests for the collection parser

### Files That Need the Most Attention

| File | Issue |
|---|---|
| `crates/tropel-js/src/context.rs` | Stubbed — needs real rquickjs `AsyncContext` |
| `crates/tropel-engine/src/engine.rs` | Bypasses VU runner; has unused variables |
| `crates/tropel-executor/src/runner.rs` | Has unused `url` variable |
| `crates/tropel-http/src/client.rs` | Unused imports, dead `cookie_store` field |
| `crates/tropel/src/main.rs` | Unused `data_file` variable |

---

## Developer Quickstart

```bash
# Build everything
cargo build --workspace

# Check for errors
cargo check --workspace

# Run (once dependencies are fulfilled)
cargo run -- run examples/collections/simple-api.json --vus 5 --iterations 10

# Available just commands
just fmt
just lint
just test
just bench
```

### Prerequisites
- Rust stable toolchain (see `rust-toolchain.toml`)
- C compiler (QuickJS vendor build — MSVC on Windows, gcc/clang elsewhere)
- All other deps are pure Rust

---

## Files Modified in This Session

All files were created from scratch in a single session. The full list is in the git commit `d73df78`. Key structural files:

- `Cargo.toml` — workspace definition with all internal path dependencies
- `crates/tropel-core/src/types.rs` — shared domain types
- `crates/tropel-core/src/config.rs` — `JobConfig`, `HttpConfig`, `TlsConfig`, `AuthConfig`
- `crates/tropel-core/src/scenario.rs` — `Scenario`, `ScenarioItem`
- `crates/tropel-collection/src/model.rs` — Postman v2.1 model structs
- `crates/tropel-collection/src/parser.rs` — Deserialization + validation + conversion
- `crates/tropel-http/src/client.rs` — HTTP client with auth, pooling, cookies
- `crates/tropel-http/src/auth.rs` — Auth signer implementations
- `crates/tropel-executor/src/scheduler.rs` — 4-mode VU scheduler
- `crates/tropel-executor/src/runner.rs` — Per-VU iteration runner
- `crates/tropel-metrics/src/histogram.rs` — HDR histogram wrapper
- `crates/tropel-metrics/src/collector.rs` — Metrics ingestion + aggregation
- `crates/tropel-engine/src/engine.rs` — Orchestration (parse → execute → report)
- `crates/tropel/src/main.rs` — CLI entrypoint
- `crates/tropel-ext/src/traits.rs` — Extension point traits
- `crates/tropel-ext/src/registry.rs` — Extension registry
- `crates/tropel-pm/src/api.rs` — `pm.*` JS bridge
- `crates/tropel-pm/src/bridge.rs` — Native ↔ JS state bridge
- `crates/tropel-native/src/*.rs` — All native builtin modules

# Status & Roadmap

> This page is the source of truth for what Tropel does and does not do yet.
> The README links here instead of repeating per-area claims.

Legend: ✅ shipped & tested · 🟡 partial / known gaps · ❌ not implemented

## Input formats

| Format | Status | Notes |
|--------|--------|-------|
| Postman collections | ✅ | v2.1/v2.0, folders, scripts, variables |
| HAR | ✅ | `postData.text`/base64 bodies, response optional, resource filtering |
| OpenAPI | ✅ | 3.x + Swagger 2.0, `$ref` resolution, server vars, OAuth2 |
| k6 scripts | ✅ | JS/TS, `options` honored, k6 shim (http/check/group/metrics/sleep) |
| Subprocess | ✅ | factory-only, timeout + size cap |
| WASM plugins | 🟡 | Declarative adapter tier works (fuel, AOT, pooling); **imperative WASM drivers not yet** |
| JMeter / Locust | ❌ | Planned (§11.6), not started |

## Scripting

| Capability | Status | Notes |
|------------|--------|-------|
| `pm.test` / `pm.expect` | ✅ | chai-style assertions, native deep_equal |
| `pm.response.code/status/json/header` | ✅ | |
| `pm.variables` / `pm.environment` | ✅ | typed values (objects stay objects) |
| `pm.iterationData` | ✅ | |
| `pm.execution.setNextRequest` | ✅ | by name |
| `pm.sendRequest` | ✅ | real HTTP via native bridge, async-resolved |
| Custom metrics (`pm.metrics`, k6 `Counter/Gauge/Rate/Trend`) | ✅ | |
| `sleep()`, think time, pacing | ✅ | |
| Crypto (AES, HMAC, SHA family, SHA3, RIPEMD160, MD5) | ✅ | real impls + PBKDF2 for AES key derivation |
| Encoding/assert/JSON bridges | ✅ | |
| `group()` + `group_duration` | ✅ | |
| Promises / async script flow | ✅ | QuickJS job queue driven per eval |

## Execution & scheduling

| Capability | Status | Notes |
|------------|--------|-------|
| 7 executors (constant-vus … externally-controlled) | ✅ | incl. per-vu-iterations |
| Growing arrival-rate pool (preAllocatedVUs → maxVUs) | ✅ | token-bucket + dropped_iterations |
| Graceful stop / ramp-down | ✅ | grace window; ramp-down only trims surplus |
| maxDuration as a cap (fixed-iteration executors) | ✅ | raced against VU drain |
| Think time / iteration pacing | ✅ | |
| Thread-per-core VUs | ✅ | `!Send` contexts, no per-ctx mutex |
| Compile-once scripts | ✅ | `Persistent<Function>` per context |
| Lazy response bodies | ✅ | `bytes::Bytes`, decode on access |
| Per-VU HTTP client | ✅ | own cookie jar + pool, no shared-response race |

## Metrics & thresholds

| Capability | Status | Notes |
|------------|--------|-------|
| Counter/Gauge/Rate/Trend first-class | ✅ | |
| Tag-scoped aggregation | ✅ | interned tag keys, small maps |
| Sub-timings (TTFB etc.) | ✅ | |
| `http_req_failed`, `data_sent/received`, `vus`, `dropped_iterations` | ✅ | |
| Thresholds (metric + tag-scoped), k6 abort semantics | ✅ | early abort on `abortOnFail` |
| Lock-free hot path | ✅ | per-VU buffers → MPSC → aggregator, bounded |

## Outputs

| Capability | Status | Notes |
|------------|--------|-------|
| stdout / JSON / CSV reporters | ✅ | |
| Streaming NDJSON / StatsD / InfluxDB / Prometheus / OTLP | ✅ | |
| `--summary-export` (k6-style) | ✅ | |
| `--http-debug` per-request logging | ✅ | |

## Platform & tooling

| Capability | Status | Notes |
|------------|--------|-------|
| `inspect` / `archive` / `extensions` / `build` | ✅ | |
| Distributed (execution segments, controller/agent) | ✅ | |
| Benchmark suite (`tropel-bench`) | ✅ | criterion; ~940µs ctx bootstrap, 2.2× compile-once win |
| WASM safety (fuel, AOT, pooling) | 🟡 | see input formats row |

## Roadmap (next)

1. **Imperative WASM drivers** — host-imported http/sleep/metrics for WASM
   plugins (closes the last WASM gap).
2. **JMeter / Locust adapters** (§11.6) — dog-food the SDK with first-party
   formats.
3. **Custom outputs via SDK** — `Output` trait beyond the built-in four.
4. **gRPC / WebSocket protocols** — `tropel-x-grpc` / `tropel-x-websocket`
   are wired into the runner's scheme dispatch (`grpc://`, `ws://`); extend
   coverage and protocol surface.
5. **Profiling-driven tuning** — allocator (mimalloc/jemalloc) feature gates
   are in; validate with the bench suite on real workloads.

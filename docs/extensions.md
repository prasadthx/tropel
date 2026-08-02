# Extensions

The input-adapter architecture is the heart of Tropel's extensibility: the
engine, load profiles, and reporting are shared; only the **input format** and
**protocol** vary.

## The SDK contract

`tropel-sdk` is the stable public contract for third-party adapters:

- `Scenario`, `ScenarioItem` — the shared read-only scenario model
- `InputAdapter` — declarative adapters: `detect(bytes)` + `parse(bytes) → Scenario`
- `Driver` — imperative drivers: `run_iteration(&VuContext)` with native host
  functions (k6 uses this)
- Registration types (`inventory::submit!`) + the host API for building
  requests and emitting samples

In-tree adapters depend on the SDK (not on internals), which is what makes the
"single stable dependency" promise real.

## Registration

Adapters register at link time via `inventory`. The engine resolves inputs by
asking every registered adapter to `detect()`; `--format` skips detection.
`tropel extensions` lists what the binary ships.

## Tier 1 — compile-time static extensions

```bash
tropel build --with tropel-x-grpc --with ./my-ext --with https://github.com/u/r@v0.2.0
```

Generates a workspace, injects the crates into `Cargo.toml` + `extern crate`,
and builds a custom binary. Names/versions are validated
(`^[a-zA-Z0-9_-]+$`, semver) to prevent build-time code injection.

## Tier 2 — WASM plugins (no recompile)

WASM modules in `--plugins-dir` are loaded by `tropel-wasm` (wasmtime):

- C-ABI exports: `adapter_detect` / `adapter_parse` (writes a JSON `Scenario`
  into host memory)
- **Safety**: fuel-based instruction limits (an infinite loop is trapped, not
  a host hang), epoch interruption, and the plugin's claimed output length is
  clamped to a 4 MiB cap before allocation (no unbounded host alloc)
- **Performance**: AOT `.cwasm` compilation cache, pooling allocation config,
  `InstancePre` — instantiation is cheap, not a fresh JIT+store per call
- WASI-less capability surface: adapters do pure compute over bytes

The WASM tier is currently **declarative-only** (input parsing). Imperative
WASM drivers (host-imported http/sleep/metrics) are planned.

## Subprocess tier

External commands as adapters (factory-only registration, 30s timeout, 16 MiB
cap, concurrent stdin/stdout).

## Shipping extensions

- `tropel-x-grpc` — gRPC protocol (dynamic codec)
- `tropel-x-websocket` — WebSocket protocol
- `tropel-x-prometheus` — Prometheus output

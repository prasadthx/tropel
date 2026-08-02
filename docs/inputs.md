# Input Formats

Tropel resolves any input through an **extension registry**: each adapter
implements `detect(bytes)` + `parse(bytes) → Scenario`, and `tropel run` /
`inspect` auto-detect unless `--format` is given. Run `tropel extensions` to
see what this binary ships.

## Postman Collections

- Collection v2.1 / v2.0, including folders, `request.url` in string or object
  form, urlencoded/multipart/raw bodies, `pm.*` pre-request and test scripts.
- Collection, environment, and data (iteration) variables.
- Structural `detect()` — requires a top-level `info.schema` containing
  `getpostman.com` and `collection`.

## HAR

- Replays the recorded requests (method, URL, headers, bodies — including
  `postData.text` where Chrome stores it, and base64 `postData`).
- Responses are optional (partial HARs parse); duplicate headers collapse via
  a case-insensitive HashMap.
- Resource filtering skips images/CSS/data: URIs by default.
- Structural `detect()` — requires a top-level `log` object with `log.version`.

## OpenAPI

- OpenAPI 3.x and Swagger 2.0.
- Intra-document `$ref` resolution (a `$ref`'d parameter no longer fails the
  whole parse).
- Server variables (`https://{env}…`) resolved; OAuth2 security schemes map to
  the OAuth2 auth config.

## k6 scripts

- JavaScript (and TypeScript via the built-in transpiler) executed with the
  **K6Driver**: a QuickJS context per VU plus a native HTTP bridge.
- `export const options = { vus, duration, stages, scenarios, thresholds }`
  is read via ESM module evaluation and drives the load profile.
- `k6/http`, `k6/metrics`, `check()`, `group()`, `sleep()` are provided by the
  k6 shim.

## Subprocess adapters

External commands can implement `--detect` / parse-on-stdin. Factory-only
registration (no phantom auto-detection), 30s timeout, 16 MiB output cap,
concurrent stdin/stdout to avoid pipe deadlock.

## WASM plugins

WASM modules (C-ABI `adapter_detect` / `adapter_parse`) load from
`--plugins-dir` into wasmtime with fuel limits, AOT `.cwasm` caching, pooling,
and an `InstancePre` — see [Extensions](extensions.md).

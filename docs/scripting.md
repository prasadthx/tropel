# Scripting

Tropel embeds QuickJS (ES2020+) — one context per VU on its own thread.
Scripts are compiled once per context (`Persistent<Function>`); per-iteration
invocation is a cheap call, not a re-eval.

## Postman `pm.*` API

| Area | Functions |
|------|-----------|
| Tests | `pm.test(name, fn)`, `pm.expect(...)` (chai-style `to.have.status` etc.) |
| Response | `pm.response.code()`, `.status`, `.json()`, `.text()`, `.header(name)`, `.to.have.status(n)` |
| Variables | `pm.variables.get/set`, `pm.environment.get/set`, `pm.iterationData` — typed (objects arrive as objects, not JSON strings) |
| Execution | `pm.execution.setNextRequest(name)`, `pm.info`, `pm.iterationData` |
| Metrics | `pm.metrics.add(name, value, tags)` |
| Requests | `pm.sendRequest(url, fn)` — real HTTP via the native bridge, responses resolve for chaining |
| Misc | `pm.visualizer`, `pm.iterationData` |

## k6 API (k6-shim)

`http.get/post/put/del/patch/head/options/request`, `http.batch`,
`check()`, `group()`, `sleep(seconds)`, `Counter/Gauge/Rate/Trend` metrics
with `.add(value, tags)`, and the `k6/http`, `k6/metrics`, `k6` module
interfaces.

## Native bridge

The JS shims call `__tropel_*` host functions registered by `tropel-native`:

- **Crypto** — AES (GCM/CBC with PBKDF2), HmacMD5/SHA1/SHA256/SHA512,
  SHA-1/256/384/512, SHA3, RIPEMD160, base64, hex, MD5
- **Encoding** — base64 encode/decode, hex encode/decode, url encode/decode,
  base64url
- **Assert** — `deep_equal`, `is_string`, `length`, `matches`, and the other
  chai/lodash helpers
- **JSON** — fast parsing/serialization for `pm.response.json()` and friends
- **HTTP** — `pm.sendRequest` and the k6 http bridge (per-VU client, own
  cookie jar)
- **Sleep / metrics** — script-level `sleep()`, custom-metric emission

See [BRIDGE_FUNCTIONS.md](../BRIDGE_FUNCTIONS.md) for the complete list.

## Async / Promises

The QuickJS job queue is driven after each eval, so `await`, Promise-returning
tests, and `pm.sendRequest` callbacks resolve.

# Tropel — Native Bridge Functions

## Architecture

Each VU gets a rquickjs `JsContext` at startup. The engine bootstraps in this order:

1. Create `JsContext` (rquickjs Runtime + Context with memory limits and interrupt handler)
2. Evaluate vendored JS shims: `pm-api/pm.js`, `chai-shim.js`, `lodash-shim.js`, `cryptojs-shim.js`
3. `tropel_native::install_all()` — registers pure-Rust utility functions as JS globals
4. `PmBridge::install()` — registers `__tropel_pm_*` bridge functions that read/write `PmState`

## PmBridge — Registered functions

These functions are registered via `rquickjs::function::Func::from` closures capturing
`Arc<std::sync::Mutex<PmState>>`. All use rquickjs-compatible types only.

| JS global name | Signature | What it does |
|---|---|---|
| `__tropel_pm_environment_get` | `(key: String) -> Option<String>` | Reads env var |
| `__tropel_pm_environment_set` | `(key: String, value: String)` | Writes env var |
| `__tropel_pm_environment_unset` | `(key: String)` | Removes env var |
| `__tropel_pm_environment_clear` | `() -> ()` | Clears all env vars |
| `__tropel_pm_variables_get` | `(key: String) -> Option<String>` | Reads var (env → collection → globals); complex values JSON-encoded |
| `__tropel_pm_variables_set` | `(key: String, value: String)` | Sets collection var (value stored as string) |
| `__tropel_pm_variables_unset` | `(key: String)` | Removes from all scopes |
| `__tropel_pm_response_code` | `() -> u16` | HTTP status code |
| `__tropel_pm_response_status` | `() -> String` | Status text |
| `__tropel_pm_response_body` | `() -> Option<String>` | Raw response body text |
| `__tropel_pm_response_header` | `(key: String) -> Option<String>` | Single response header value |
| `__tropel_pm_response_time` | `() -> f64` | Response time in ms |
| `__tropel_pm_test` | `(name: String, passed: bool)` | Records assertion → emits `checks` sample |
| `__tropel_pm_set_next_request` | `(request_id: String)` | Flow control: jump to request index |
| `__tropel_pm_skip_tests` | `() -> ()` | Skip remaining tests for current request |

## PmBridge — NOT registered (with rationale)

| JS global name | Reason | JS fallback |
|---|---|---|
| `__tropel_pm_response_headers` | Return type `HashMap<String, String>` not supported by `Func::from` | JS returns `{}` |
| `__tropel_pm_response_cookies` | Return type `Vec<Cookie>` not supported by `Func::from` | JS returns `[]` |
| `__tropel_pm_response_json` | Would return `String` (JSON string) but `pm.response.json()` expects parsed object. Silent breakage. | JS returns `null`. Workaround: `JSON.parse(pm.response.text())` |
| `__tropel_pm_iteration_data_get` | Data is injected by VURunner before each iteration, not via bridge | JS returns `null` |
| `__tropel_pm_send_request` | Would require async request chaining (complex) | JS fallback uses XMLHttpRequest (unavailable in QuickJS) |

## Native modules — Registered functions

### Crypto (`crypto.rs`)
| JS global | Rust fn | Signature |
|---|---|---|
| `__tropel_native_sha256` | `sha256(data: &[u8]) -> Vec<u8>` | `(Vec<u8>) -> Vec<u8>` |
| `__tropel_native_sha1` | `sha1(data: &[u8]) -> Vec<u8>` | `(Vec<u8>) -> Vec<u8>` |
| `__tropel_native_md5` | `md5(data: &[u8]) -> Vec<u8>` | `(Vec<u8>) -> Vec<u8>` |
| `__tropel_native_hmac_sha256` | `hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8>` | `(Vec<u8>, Vec<u8>) -> Vec<u8>` |

### Encoding (`encoding.rs`)
| JS global | Rust fn | Signature |
|---|---|---|
| `__tropel_native_base64_encode` | `base64_encode(data: &[u8]) -> String` | `(Vec<u8>) -> String` |
| `__tropel_native_hex_encode` | `hex_encode(data: &[u8]) -> String` | `(Vec<u8>) -> String` |
| `__tropel_native_url_encode` | `url_encode(data: &str) -> String` | `(String) -> String` |

### Hash (`hash.rs`)
| JS global | Rust fn | Signature |
|---|---|---|
| `__tropel_native_hash_uuid` | `uuid::Uuid::new_v4()` | `() -> String` |

### Assert (`assert.rs`)
| JS global | Rust fn | Signature |
|---|---|---|
| `__tropel_native_assert_ready` | Sentinel (returns true) | `() -> bool` |

Note: `__tropel_native_deep_equal` is NOT registered because `serde_json::Value` parameter type is not supported by `Func::from`. The chai-shim.js falls back to `JSON.stringify(a) === JSON.stringify(b)`.

### JSON (`json.rs`)
| JS global | Rust fn | Signature |
|---|---|---|
| `__tropel_native_uuid` | `uuid::Uuid::new_v4()` | `() -> String` |

Note: `json_parse` and `json_stringify` are NOT registered — JS already has native `JSON.parse`/`JSON.stringify`.

### Extra Functions (`fn.rs`)
| JS global | Rust fn | Signature |
|---|---|---|
| `__tropel_native_random_int` | `random_int(0, 1000)` | `() -> i64` |
| `__tropel_native_random_float` | `random_float()` | `() -> f64` |

## Type constraints

`rquickjs::function::Func::from` in rquickjs 0.9 supports these parameter/return types:
- `String`, `bool`, `f64`, `u16`, `i32`/`i64`/`u32`/`u64`
- `Vec<u8>` (binary data from ArrayBuffer/TypedArray)
- `Option<String>`, `Option<T>` for supported T
- `()` (unit)

NOT supported: `serde_json::Value`, `HashMap<K,V>`, custom structs, `Vec<Custom>`.

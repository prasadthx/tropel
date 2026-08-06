use crate::bridge::SharedPmState;
use rquickjs::function::Func;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tropel_core::types::{AuthConfig, Body, Method, Request};
use tropel_core::Result;
use tropel_http::client::HttpClient;
use tropel_js::JsContext;

/// Convert a serde_json::Value to a string suitable for JS consumption.
/// Always JSON-encodes the value so the JS shim can JSON.parse() to
/// restore the correct type. This ensures "123" (string) survives as
/// the string "123" rather than being parsed as the number 123.
/// All variable scopes (env, collection, globals) use this same path
/// for type-safe round-tripping.
fn variable_value_to_string(val: &Value) -> String {
    serde_json::to_string(val).unwrap_or_default()
}

/// Convert a plain string to its JSON-encoded form for type-safe JS round-tripping.
/// `&str` implements `Serialize`, so `serde_json::to_string` produces
/// `'"123"'` which `JSON.parse()` restores as the string `"123"` — not
/// the number `123` or boolean `true`.
fn string_to_json_encoded(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_default()
}

/// Resolve a variable across ALL scopes with Postman precedence:
/// iteration data > environment > collection > globals (backlog line 145).
///
/// Returns the value JSON-encoded for the JS shim to `JSON.parse()` back to
/// the correct type. Extracted as a pure function so the precedence contract
/// is unit-testable without a live JS context.
fn variables_lookup(
    key: &str,
    iteration_data: Option<&HashMap<String, Value>>,
    environment: &HashMap<String, String>,
    collection_vars: &HashMap<String, Value>,
    globals: &HashMap<String, Value>,
) -> Option<String> {
    // Postman precedence: data (iteration) > env > collection > globals.
    // Backlog line 145: iteration data used to be ignored entirely, so a CSV
    // row could never override an environment/collection value.
    if let Some(val) = iteration_data.and_then(|d| d.get(key)) {
        return Some(variable_value_to_string(val));
    }
    // Environment variables are HashMap<String, String>
    if let Some(val) = environment.get(key) {
        return Some(string_to_json_encoded(val));
    }
    // Collection and global variables are serde_json::Value
    if let Some(val) = collection_vars.get(key) {
        return Some(variable_value_to_string(val));
    }
    globals.get(key).map(variable_value_to_string)
}

/// Parse an HTTP method string (case-insensitive) into a `Method`.
/// Falls back to `Custom` for non-standard tokens (PURGE, LINK, …).
fn parse_method(s: &str) -> Method {
    let t = s.trim();
    if t.is_empty() {
        return Method::GET;
    }
    match t.to_uppercase().as_str() {
        "GET" => Method::GET,
        "HEAD" => Method::HEAD,
        "POST" => Method::POST,
        "PUT" => Method::PUT,
        "PATCH" => Method::PATCH,
        "DELETE" => Method::DELETE,
        "OPTIONS" => Method::OPTIONS,
        "TRACE" => Method::TRACE,
        "CONNECT" => Method::CONNECT,
        other => Method::Custom(other.to_string()),
    }
}

/// Return the response body as a JSON string for the `pm.response.json()`
/// bridge, validating that it IS valid JSON first.
///
/// The body is validated against a SCRATCH copy and the ORIGINAL bytes are
/// returned. simd-json parses in-place and REWRITES its input buffer
/// (in-situ de-escaping), so validating-and-returning the same buffer
/// corrupts any body containing `\n`, `\"`, `\\` or `\uXXXX` escapes: the
/// de-escaped bytes overwrite the escape sequences but the buffer length is
/// unchanged, leaving stale bytes — `{"a":"x\ny"}` becomes invalid JSON.
/// The JS shim then JSON.parses the returned text, so it must be pristine.
fn response_json_string(body: &[u8]) -> Option<String> {
    if body.is_empty() {
        return None;
    }
    let mut scratch = body.to_vec();
    simd_json::serde::from_slice::<serde_json::Value>(&mut scratch).ok()?;
    String::from_utf8(body.to_vec()).ok()
}

/// Resolve {{variable}} references in a URL using the current PM state.
/// Searches environment, collection vars, and globals in order.
/// Uses a cursor-based approach that builds the result string by pushing
/// segments — no in-place mutation, no infinite-loop risk.
fn resolve_vars(
    url: &str,
    environment: &HashMap<String, String>,
    collection_vars: &HashMap<String, serde_json::Value>,
    globals: &HashMap<String, serde_json::Value>,
) -> String {
    if !url.contains("{{") {
        return url.to_string();
    }

    let mut result = String::with_capacity(url.len());
    let mut pos = 0;

    while pos < url.len() {
        // Find the next {{ marker
        if let Some(start) = url[pos..].find("{{") {
            let abs_start = pos + start;

            // Copy everything before the marker
            result.push_str(&url[pos..abs_start]);

            // Look for the closing }}
            if let Some(end) = url[abs_start + 2..].find("}}") {
                let key_start = abs_start + 2;
                let key_end = abs_start + 2 + end;

                // Extract and normalize the key — trim whitespace
                let key = url[key_start..key_end].trim();

                // Try to resolve from scopes in order: env → collection → globals
                let resolved = environment
                    .get(key)
                    .cloned()
                    .or_else(|| {
                        collection_vars.get(key).map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        })
                    })
                    .or_else(|| {
                        globals.get(key).map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        })
                    });

                match resolved {
                    Some(val) => result.push_str(&val),
                    None => {
                        // Unresolved — emit the original {{key}} literal
                        result.push_str(&url[abs_start..key_end + 2]);
                    }
                }

                // Advance cursor past the {{key}}
                pos = key_end + 2;
            } else {
                // No closing }} — emit the rest as-is and stop
                result.push_str(&url[abs_start..]);
                break;
            }
        } else {
            // No more {{ markers — emit the tail
            result.push_str(&url[pos..]);
            break;
        }
    }

    result
}

/// Parse headers from a JSON string that may be either:
/// - Object form: {"Content-Type": "application/json"}
/// - Postman array form: [{"key": "Content-Type", "value": "application/json"}]
fn parse_headers(json: &str) -> HashMap<String, String> {
    if json.is_empty() || json == "{}" || json == "[]" {
        return HashMap::new();
    }

    // Try object form first
    if json.trim_start().starts_with('{') {
        if let Ok(map) = serde_json::from_str::<HashMap<String, String>>(json) {
            return map;
        }
    }

    // Try Postman array form: [{"key": ..., "value": ...}]
    if json.trim_start().starts_with('[') {
        if let Ok(arr) = serde_json::from_str::<Vec<HashMap<String, serde_json::Value>>>(json) {
            let mut headers = HashMap::new();
            for entry in arr {
                let key = entry.get("key").and_then(|v| v.as_str()).unwrap_or("");
                let value = entry.get("value").and_then(|v| v.as_str()).unwrap_or("");
                if !key.is_empty() {
                    headers.insert(key.to_string(), value.to_string());
                }
            }
            return headers;
        }
    }

    HashMap::new()
}

/// Register all `pm.*` bridge functions as global JS functions in a JsContext.
/// Functions like `__tropel_pm_test`, `__tropel_pm_environment_get`, etc.
/// are registered so the JS shims in pm-api/pm.js can call them.
///
/// With rquickjs 0.12+, the following complex types are supported as
/// Func::from parameter/return types via FromJs/IntoJs: HashMap<String, String>,
/// Vec<(String, String)>, Option<T>, Vec<T>, and all primitive types.
pub struct PmBridge {
    state: SharedPmState,
    /// Per-VU HTTP client for executing pm.sendRequest synchronously.
    http_client: Arc<HttpClient>,
}

impl PmBridge {
    pub fn new(state: SharedPmState, http_client: Arc<HttpClient>) -> Self {
        Self { state, http_client }
    }

    /// Register all bridge functions into the given JS context.
    pub fn install(&self, ctx: &mut JsContext) -> Result<()> {
        let state = self.state.clone();

        ctx.with_ctx(|rq_ctx| {
            let globals = rq_ctx.globals();

            // ── Environment ──
            // NOTE: environment values are returned RAW (not JSON-encoded) on
            // purpose — Postman environment variables are always strings, and
            // the shim's `pm.environment.get` returns them as-is (no
            // JSON.parse). Do NOT "harmonize" this with variables_get's JSON
            // encoding, or env vars like "123" would round-trip as numbers.
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_environment_get",
                Func::from(move |key: String| -> Option<String> {
                    let st = state_clone.lock().unwrap();
                    st.environment.get(&key).cloned()
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_environment_set",
                Func::from(move |key: String, value: String| {
                    let mut st = state_clone.lock().unwrap();
                    st.environment.insert(key, value);
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_environment_unset",
                Func::from(move |key: String| {
                    let mut st = state_clone.lock().unwrap();
                    st.environment.remove(&key);
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_environment_clear",
                Func::from(move || {
                    let mut st = state_clone.lock().unwrap();
                    st.environment.clear();
                }),
            );

            // ── Variables ──
            // Returns Option<String>: ALL variable scopes return JSON-encoded
            // strings so the JS shim can `JSON.parse()` to restore the correct
            // JS type. Without encoding, an env var like "123" would be parsed
            // as the number 123 instead of the string "123".
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_variables_get",
                Func::from(move |key: String| -> Option<String> {
                    let st = state_clone.lock().unwrap();
                    variables_lookup(
                        &key,
                        st.iteration_data.as_ref(),
                        &st.environment,
                        &st.collection_vars,
                        &st.globals,
                    )
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_variables_set",
                Func::from(move |key: String, value: String| {
                    let mut st = state_clone.lock().unwrap();
                    st.collection_vars
                        .insert(key, serde_json::Value::String(value));
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_variables_unset",
                Func::from(move |key: String| {
                    let mut st = state_clone.lock().unwrap();
                    st.collection_vars.remove(&key);
                    st.environment.remove(&key);
                    st.globals.remove(&key);
                }),
            );

            // ── Environment: has / toObject ──
            // Backlog line 145: Postman's pm.environment exposes has() and
            // toObject() alongside get/set/unset.
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_environment_has",
                Func::from(move |key: String| -> bool {
                    let st = state_clone.lock().unwrap();
                    st.environment.contains_key(&key)
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_environment_to_object",
                Func::from(move || -> HashMap<String, String> {
                    let st = state_clone.lock().unwrap();
                    st.environment.clone()
                }),
            );

            // ── Collection Variables (pm.collectionVariables) ──
            // Backlog line 145: one of the top-3 most-used pm.* members was
            // entirely missing. Values are JSON-encoded for type-safe
            // round-tripping, same as variables.
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_collection_vars_get",
                Func::from(move |key: String| -> Option<String> {
                    let st = state_clone.lock().unwrap();
                    st.collection_vars.get(&key).map(variable_value_to_string)
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_collection_vars_set",
                Func::from(move |key: String, value: String| {
                    let mut st = state_clone.lock().unwrap();
                    st.collection_vars
                        .insert(key, serde_json::Value::String(value));
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_collection_vars_unset",
                Func::from(move |key: String| {
                    let mut st = state_clone.lock().unwrap();
                    st.collection_vars.remove(&key);
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_collection_vars_has",
                Func::from(move |key: String| -> bool {
                    let st = state_clone.lock().unwrap();
                    st.collection_vars.contains_key(&key)
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_collection_vars_to_object",
                Func::from(move || -> HashMap<String, String> {
                    let st = state_clone.lock().unwrap();
                    st.collection_vars
                        .iter()
                        .map(|(k, v)| (k.clone(), variable_value_to_string(v)))
                        .collect()
                }),
            );

            // ── Global Variables (pm.globals) ──
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_globals_get",
                Func::from(move |key: String| -> Option<String> {
                    let st = state_clone.lock().unwrap();
                    st.globals.get(&key).map(variable_value_to_string)
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_globals_set",
                Func::from(move |key: String, value: String| {
                    let mut st = state_clone.lock().unwrap();
                    st.globals.insert(key, serde_json::Value::String(value));
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_globals_unset",
                Func::from(move |key: String| {
                    let mut st = state_clone.lock().unwrap();
                    st.globals.remove(&key);
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_globals_has",
                Func::from(move |key: String| -> bool {
                    let st = state_clone.lock().unwrap();
                    st.globals.contains_key(&key)
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_globals_to_object",
                Func::from(move || -> HashMap<String, String> {
                    let st = state_clone.lock().unwrap();
                    st.globals
                        .iter()
                        .map(|(k, v)| (k.clone(), variable_value_to_string(v)))
                        .collect()
                }),
            );

            // ── Request (pm.request) ──
            // Backlog line 145: prerequest scripts could not add an auth
            // header or sign a request because pm.request didn't exist AND
            // the runner rebuilt the wire request from the static collection
            // item, discarding any state.request mutations. The runner now
            // reads state.request (seeded from item.request before prerequest)
            // when building the outgoing request, so these bridges actually
            // change what goes out on the wire.
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_request_url",
                Func::from(move || -> String {
                    let st = state_clone.lock().unwrap();
                    st.request.as_ref().map(|r| r.url.clone()).unwrap_or_default()
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_request_url_set",
                Func::from(move |url: String| {
                    let mut st = state_clone.lock().unwrap();
                    if let Some(r) = st.request.as_mut() {
                        r.url = url;
                    }
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_request_method",
                Func::from(move || -> String {
                    let st = state_clone.lock().unwrap();
                    st.request
                        .as_ref()
                        .map(|r| r.method.to_string())
                        .unwrap_or_else(|| "GET".to_string())
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_request_method_set",
                Func::from(move |method: String| {
                    let mut st = state_clone.lock().unwrap();
                    if let Some(r) = st.request.as_mut() {
                        r.method = parse_method(&method);
                    }
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_request_headers",
                Func::from(move || -> HashMap<String, String> {
                    let st = state_clone.lock().unwrap();
                    st.request
                        .as_ref()
                        .map(|r| r.headers.clone())
                        .unwrap_or_default()
                }),
            );

            // Case-insensitive read (Postman's HeaderList.get is case-insensitive).
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_request_header_get",
                Func::from(move |key: String| -> Option<String> {
                    let st = state_clone.lock().unwrap();
                    st.request.as_ref().and_then(|r| {
                        r.headers
                            .iter()
                            .find(|(k, _)| k.eq_ignore_ascii_case(&key))
                            .map(|(_, v)| v.clone())
                    })
                }),
            );

            // Upsert case-insensitively: replace an existing differently-cased
            // header rather than creating a duplicate (Postman HeaderList.add).
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_request_header_set",
                Func::from(move |key: String, value: String| {
                    let mut st = state_clone.lock().unwrap();
                    if let Some(r) = st.request.as_mut() {
                        let existing = r
                            .headers
                            .keys()
                            .find(|k| k.eq_ignore_ascii_case(&key))
                            .cloned();
                        match existing {
                            Some(ek) => {
                                r.headers.insert(ek, value);
                            }
                            None => {
                                r.headers.insert(key, value);
                            }
                        }
                    }
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_request_header_unset",
                Func::from(move |key: String| {
                    let mut st = state_clone.lock().unwrap();
                    if let Some(r) = st.request.as_mut() {
                        r.headers.retain(|k, _| !k.eq_ignore_ascii_case(&key));
                    }
                }),
            );

            // Body as raw text (get) / raw text (set).
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_request_body",
                Func::from(move || -> Option<String> {
                    let st = state_clone.lock().unwrap();
                    st.request.as_ref().and_then(|r| match &r.body {
                        Some(Body::Raw(s)) => Some(s.clone()),
                        Some(Body::Json(v)) => Some(serde_json::to_string(v).unwrap_or_default()),
                        _ => None,
                    })
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_request_body_set",
                Func::from(move |body: String| {
                    let mut st = state_clone.lock().unwrap();
                    if let Some(r) = st.request.as_mut() {
                        r.body = Some(Body::Raw(body));
                    }
                }),
            );

            // Auth: accepts the internally-tagged AuthConfig JSON form
            // ({"type":"bearer","token":...}) — the same shape the
            // postman/k6 inputs produce. A prerequest script can therefore
            // sign the outgoing request (the primary purpose of pm.request).
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_request_auth_set",
                Func::from(move |auth_json: String| {
                    let parsed = serde_json::from_str::<AuthConfig>(&auth_json);
                    let mut st = state_clone.lock().unwrap();
                    if let Some(r) = st.request.as_mut() {
                        match parsed {
                            Ok(auth) => r.auth = Some(auth),
                            Err(e) => {
                                tracing::warn!(
                                    "pm.request.auth: could not parse auth config: {}",
                                    e
                                );
                            }
                        }
                    }
                }),
            );

            // ── Test skip (pm.test.skip) ──
            // Backlog line 145: pm.test.skip(name, fn) marks a test skipped
            // WITHOUT running it — a collection using it used to throw
            // "pm.test.skip is not a function" and fail the whole run. Skipped
            // tests are not pass/fail checks, so nothing is recorded.
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_test_skip",
                Func::from(move |name: String| {
                    let mut st = state_clone.lock().unwrap();
                    st.assertions.skipped += 1;
                    tracing::debug!("pm.test.skip: {}", name);
                }),
            );

            // ── Response ──
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_response_code",
                Func::from(move || -> u16 {
                    let st = state_clone.lock().unwrap();
                    st.response.as_ref().map(|r| r.status_code).unwrap_or(0)
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_response_status",
                Func::from(move || -> String {
                    let st = state_clone.lock().unwrap();
                    st.response
                        .as_ref()
                        .map(|r| r.status_text.clone())
                        .unwrap_or_default()
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_response_body",
                Func::from(move || -> Option<String> {
                    let st = state_clone.lock().unwrap();
                    st.response.as_ref().and_then(|r| r.body_text())
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_response_time",
                Func::from(move || -> f64 {
                    let st = state_clone.lock().unwrap();
                    st.response
                        .as_ref()
                        .map(|r| r.response_time.as_secs_f64() * 1000.0)
                        .unwrap_or(0.0)
                }),
            );

            // ── Response Headers (full map) ──
            // rquickjs 0.12+ supports HashMap<String,String> as IntoJs -> JS object
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_response_headers",
                Func::from(move || -> HashMap<String, String> {
                    let st = state_clone.lock().unwrap();
                    st.response
                        .as_ref()
                        .map(|r| r.headers.clone())
                        .unwrap_or_default()
                }),
            );

            // ── Response Header (individual header access, widely used) ──
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_response_header",
                Func::from(move |key: String| -> Option<String> {
                    let st = state_clone.lock().unwrap();
                    st.response.as_ref().and_then(|r| {
                        // Postman semantics: pm.response.header('content-type')
                        // and pm.response.headers.get('Content-Type') are
                        // case-insensitive. The map is canonical (Content-Type)
                        // after the client.rs fix, but a script may ask in any
                        // case — look up case-insensitively.
                        r.headers
                            .iter()
                            .find(|(k, _)| k.eq_ignore_ascii_case(&key))
                            .map(|(_, v)| v.clone())
                    })
                }),
            );

            // ── Response Cookies (name → value map)
            // rquickjs 0.12+ supports HashMap<String,String> as IntoJs -> JS object
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_response_cookies",
                Func::from(move || -> HashMap<String, String> {
                    let st = state_clone.lock().unwrap();
                    st.response
                        .as_ref()
                        .map(|r| {
                            r.cookies
                                .iter()
                                .map(|c| (c.name.clone(), c.value.clone()))
                                .collect()
                        })
                        .unwrap_or_default()
                }),
            );

            // ── Response JSON (returns JSON string, parsed by JS shim) ──
            // rquickjs 0.12 still doesn't support returning serde_json::Value directly,
            // but returning Option<String> (JSON text) works. The pm.js shim parses
            // this string via JSON.parse() to produce the expected object.
            // We validate the body is valid JSON using simd-json (fast, from bytes)
            // before returning, so the JS shim can throw a descriptive error on
            // invalid JSON.
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_response_json",
                Func::from(move || -> Option<String> {
                    let st = state_clone.lock().unwrap();
                    st.response.as_ref().and_then(|r| response_json_string(&r.body))
                }),
            );

            // ── Iteration Data ──
            // Returns Option<String>: JSON-encoded value so the JS shim can
            // JSON.parse() to restore the correct type.
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_iteration_data_get",
                Func::from(move |key: String| -> Option<String> {
                    let st = state_clone.lock().unwrap();
                    st.iteration_data
                        .as_ref()
                        .and_then(|data| data.get(&key).map(variable_value_to_string))
                }),
            );

            // ── Test ──
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_test",
                Func::from(move |name: String, passed: bool| {
                    let mut st = state_clone.lock().unwrap();
                    st.record_test(&name, passed);
                }),
            );

            // ── Flow Control ──
            // setNextRequest accepts either a numeric index (legacy) or a
            // request name. If the argument parses as usize, use it as an
            // index directly. Otherwise, look up the name in request_names
            // (populated from scenario items by the runner).
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_set_next_request",
                Func::from(move |request_id: String| {
                    let mut st = state_clone.lock().unwrap();

                    // skipRequest passes null — clear any pending jump
                    if request_id == "null" || request_id.is_empty() {
                        st.next_request = None;
                        return;
                    }

                    // Try numeric index first (backward compat)
                    if let Ok(index) = request_id.parse::<usize>() {
                        st.next_request = Some(index);
                        return;
                    }

                    // Look up by name in the request list
                    if let Some(pos) = st.request_names.iter().position(|n| n == &request_id) {
                        st.next_request = Some(pos);
                    }
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_skip_tests",
                Func::from(move || {
                    let mut st = state_clone.lock().unwrap();
                    st.skip_tests = true;
                }),
            );

            // ── Group (for nesting groups with group_duration metric) ──
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_group_start",
                Func::from(move |name: String| {
                    let mut st = state_clone.lock().unwrap();
                    st.group_stack.push(name);
                    // Rebuild the current group path from the stack
                    st.current_group = if st.group_stack.is_empty() {
                        None
                    } else {
                        Some(st.group_stack.join("::"))
                    };
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_group_end",
                Func::from(move |name: String, duration_ms: f64| {
                    let mut st = state_clone.lock().unwrap();
                    // Pop the matching group from the stack
                    if st.group_stack.last().map(|n| n == &name).unwrap_or(false) {
                        st.group_stack.pop();
                    }
                    // Rebuild current group path
                    st.current_group = if st.group_stack.is_empty() {
                        None
                    } else {
                        Some(st.group_stack.join("::"))
                    };

                    // Emit group_duration sample (Trend) in microseconds
                    let duration_micros = (duration_ms * 1000.0) as u64;
                    let mut tags = tropel_core::types::TagMap::new();
                    tags.insert("group", name.clone());
                    if let Some(ref path) = st.current_group {
                        tags.insert("group_path", path.clone());
                    }
                    st.samples.push(tropel_core::types::Sample {
                        metric: "group_duration".into(),
                        value: duration_micros as f64,
                        tags: Arc::new(tags),
                        timestamp: std::time::SystemTime::now(),
                        sample_type: tropel_core::types::SampleType::Trend,
                    });
                }),
            );

            // ── Custom Metrics ──
            // Add a custom metric sample (Postman-style, no tags).
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_metrics_add",
                Func::from(move |name: String, value: f64, metric_type_str: String| {
                    let mut st = state_clone.lock().unwrap();
                    // Track current value
                    st.custom_metrics.insert(name.clone(), value);
                    // Emit a metric sample with the appropriate type
                    let sample_type = match metric_type_str.as_str() {
                        "counter" => tropel_core::types::SampleType::Counter,
                        "gauge" => tropel_core::types::SampleType::Point,
                        "rate" => tropel_core::types::SampleType::Rate,
                        _ => tropel_core::types::SampleType::Trend,
                    };
                    st.samples.push(tropel_core::types::Sample {
                        metric: name.into(),
                        value,
                        tags: Arc::new(tropel_core::types::TagMap::new()),
                        timestamp: std::time::SystemTime::now(),
                        sample_type,
                    });
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_metrics_get",
                Func::from(move |name: String| -> Option<f64> {
                    let st = state_clone.lock().unwrap();
                    st.custom_metrics.get(&name).copied()
                }),
            );

            // ── Custom Metric with Tags (k6-style .add(value, tags)) ──
            // Called by Counter/Gauge/Rate/Trend JS constructors.
            // tags_json is a JSON-encoded object like '{"status":"200","method":"GET"}'.
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_custom_metric_add",
                Func::from(
                    move |name: String, value: f64, tags_json: String, metric_type_str: String| {
                        let mut st = state_clone.lock().unwrap();
                        // Parse tags from JSON string
                        let tags = if tags_json.is_empty() || tags_json == "{}" {
                            tropel_core::types::TagMap::new()
                        } else {
                            let parsed: std::collections::HashMap<String, String> =
                                serde_json::from_str(&tags_json).unwrap_or_default();
                            tropel_core::types::TagMap::from_pairs(parsed)
                        };

                        // Track the current value per metric+tags combo
                        st.custom_metrics.insert(name.clone(), value);

                        // Determine sample type from type string
                        let sample_type = match metric_type_str.as_str() {
                            "counter" => tropel_core::types::SampleType::Counter,
                            "gauge" => tropel_core::types::SampleType::Point,
                            "rate" => tropel_core::types::SampleType::Rate,
                            _ => tropel_core::types::SampleType::Trend,
                        };

                        st.samples.push(tropel_core::types::Sample {
                            metric: name.into(),
                            value,
                            tags: Arc::new(tags),
                            timestamp: std::time::SystemTime::now(),
                            sample_type,
                        });
                    },
                ),
            );

            // ═══════════════════════════════════════════════════
            // Execution context (k6 exec.* / test.abort())
            // ═══════════════════════════════════════════════════
            // exec.vu.idInTest — VU ID
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_exec_vu_id",
                Func::from(move || -> u32 { state_clone.lock().unwrap().vu_id }),
            );

            // exec.scenario.name — scenario name
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_exec_scenario_name",
                Func::from(move || -> String { state_clone.lock().unwrap().scenario_name.clone() }),
            );

            // exec.scenario.executor — executor type string (e.g.,
            // "constant-vus", "ramping-vus"). Piped into PmState by the engine
            // via VURunner::with_exec_context. Falls back to "" when the
            // engine hasn't attached it (e.g. script-only test harnesses).
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_exec_scenario_executor",
                Func::from(move || -> String {
                    state_clone.lock().unwrap().executor_name.clone()
                }),
            );

            // exec.vu.iterationInScenario — current iteration index
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_exec_iteration",
                Func::from(move || -> u64 { state_clone.lock().unwrap().iteration_index }),
            );

            // exec.instance.iterationsCompleted — GLOBAL total across all VUs.
            // The engine attaches the scheduler's shared atomic counter to
            // PmState; the closure reads it live (lock-free). Falls back to the
            // per-VU iteration index when no handle is attached.
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_exec_iterations_completed",
                Func::from(move || -> u64 {
                    let st = state_clone.lock().unwrap();
                    st.global_iterations
                        .as_ref()
                        .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
                        .unwrap_or(st.iteration_index)
                }),
            );

            // exec.instance.vusActive — currently active VUs, read live from
            // the scheduler's shared atomic counter (lock-free). Falls back to
            // 0 when no handle is attached.
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_exec_vus_active",
                Func::from(move || -> u32 {
                    let st = state_clone.lock().unwrap();
                    st.active_vus
                        .as_ref()
                        .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
                        .unwrap_or(0)
                }),
            );

            // test.abort(message) — requests engine to abort the test
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_test_abort",
                Func::from(move |message: String| {
                    let mut st = state_clone.lock().unwrap();
                    st.abort_requested = true;
                    st.abort_message = Some(message);
                }),
            );

            // ── sendRequest ──
            // Executes an HTTP request synchronously using the per-VU HTTP client.
            // The bridge closure runs inside ctx.with() (synchronous), so it uses
            // the shared tropel_http::blocking::execute_blocking helper: the
            // caller parks on a plain std channel (no tokio runtime entered on
            // this thread → no "cannot block from within a runtime" panic), while
            // the future runs on the dedicated multi-thread I/O runtime's reactor
            // (no deadlock with the current-thread VU runtime). reqwest's own
            // per-request timeout still fires normally.
            //
            // Supports the auth-token-fetch pattern: scripts can call pm.sendRequest
            // to obtain auth tokens or session data, then store them via pm.variables.set().
            // Variable references ({{var}}) in the URL are resolved against the current
            // environment/collection/global variables.
            //
            // Parameters:
            //   method: HTTP method string (GET, POST, etc.)
            //   url: Request URL with optional {{variable}} references
            //   headers_json: JSON string of headers (supports both object and array formats)
            //   body: Request body string (empty string = no body)
            //   timeout_ms: Request timeout in milliseconds (0 = no timeout, default 30000)
            //   response_type: k6 responseType ("text"/"binary"/"none") — "none"
            //     skips reading the response body (saves bandwidth/memory)
            // Returns: JSON-encoded response with code, statusText, body, headers, responseTime
            let http = self.http_client.clone();
            let state_for_send = self.state.clone();
            let _ = globals.set(
                "__tropel_pm_send_request",
                Func::from(
                    move |method: String,
                          url: String,
                          headers_json: String,
                          body: String,
                          timeout_ms: f64,
                          response_type: String|
                          -> String {
                        // Resolve {{variables}} in the URL using current PM state
                        let resolved_url = {
                            let st = state_for_send.lock().unwrap();
                            resolve_vars(&url, &st.environment, &st.collection_vars, &st.globals)
                        };

                        // Parse headers — supports both object form {"key":"val"} and
                        // Postman array form [{"key":"Content-Type","value":"application/json"}]
                        let headers: HashMap<String, String> = parse_headers(&headers_json);

                        let request_body = if body.is_empty() {
                            None
                        } else {
                            Some(Body::Raw(body))
                        };

                        let timeout = if timeout_ms > 0.0 {
                            Some(std::time::Duration::from_millis(timeout_ms as u64))
                        } else {
                            Some(std::time::Duration::from_secs(30)) // default 30s
                        };

                        // A genuinely invalid method token must not silently
                        // become GET (a write-path "PURGE" must not degrade
                        // into a read-path GET that reports green). Surfaced
                        // as a status-0 error response. Valid-but-uncommon
                        // tokens (PURGE/LINK/…) parse fine via Method::Custom.
                        let Some(method) = Method::parse(&method) else {
                            return serde_json::json!({
                                "code": 0,
                                "statusText": format!("invalid HTTP method {}", method),
                                "body": "",
                                "headers": {},
                                "responseTime": 0,
                            })
                            .to_string();
                        };

                        let req = Request {
                            url: resolved_url,
                            method,
                            headers,
                            query_params: HashMap::new(),
                            body: request_body,
                            auth: None,
                            certificate: None,
                            follow_redirects: true,
                            timeout,
                            response_type: tropel_core::types::ResponseType::from_k6(&response_type),
                        };

                        // Execute on the dedicated I/O runtime via the shared
                        // blocking helper — safe from inside ctx.with on a
                        // current-thread VU runtime.
                        let http_for_io = http.clone();
                        let result = tropel_http::blocking::execute_blocking(async move {
                            http_for_io.execute(&req, None).await
                        });
                        match result {
                            Ok(http_resp) => {
                                let body_text =
                                    String::from_utf8(http_resp.body.clone()).unwrap_or_default();
                                serde_json::json!({
                                    "code": http_resp.status_code,
                                    "statusText": http_resp.status_text,
                                    "body": body_text,
                                    "headers": http_resp.headers,
                                    "responseTime": http_resp.response_time.as_secs_f64() * 1000.0,
                                })
                                .to_string()
                            }
                            Err(e) => serde_json::json!({
                                "code": 0,
                                "statusText": format!("Request failed: {}", e),
                                "body": "",
                                "headers": {},
                                "responseTime": 0,
                            })
                            .to_string(),
                        }
                    },
                ),
            );
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The JS shim round-trips a variable through `JSON.parse`:
    /// bridge returns a JSON-encoded string → shim JSON.parses it. This test
    /// locks the typing contract so an env var "123" stays the STRING "123"
    /// (never the number 123) and a collection/global object survives.
    #[test]
    fn test_variable_json_roundtrip_preserves_type() {
        // Env vars are HashMap<String, String> — the bridge JSON-encodes them.
        let env_str = string_to_json_encoded("123");
        assert_eq!(env_str, "\"123\"");
        // Shim does JSON.parse → must come back as the string "123".
        let parsed: serde_json::Value = serde_json::from_str(&env_str).unwrap();
        assert!(parsed.is_string());
        assert_eq!(parsed.as_str().unwrap(), "123");

        // Boolean-looking and null-looking env values stay strings too.
        assert_eq!(serde_json::from_str::<serde_json::Value>(&string_to_json_encoded("true")).unwrap(),
                   serde_json::Value::String("true".into()));
        assert_eq!(serde_json::from_str::<serde_json::Value>(&string_to_json_encoded("null")).unwrap(),
                   serde_json::Value::String("null".into()));

        // Plain string (the common case) round-trips unchanged.
        assert_eq!(serde_json::from_str::<serde_json::Value>(&string_to_json_encoded("hello")).unwrap(),
                   serde_json::Value::String("hello".into()));

        // Collection/global vars are serde_json::Value — objects round-trip
        // as objects, numbers as numbers, strings as strings.
        let obj = serde_json::json!({ "a": 1, "b": [true, null] });
        let encoded = variable_value_to_string(&obj);
        let parsed: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(parsed, obj);

        let num = serde_json::json!(123);
        let parsed: serde_json::Value = serde_json::from_str(&variable_value_to_string(&num)).unwrap();
        assert!(parsed.is_number());
    }

    /// Regression (backlog line 145): `pm.variables.get` must resolve with
    /// Postman precedence — iteration data beats environment beats collection
    /// beats globals. The old code skipped iteration data entirely, so a CSV
    /// row could never override an environment/collection value.
    #[test]
    fn test_variables_lookup_postman_precedence() {
        let data = HashMap::from([("k".to_string(), Value::String("from-data".into()))]);
        let env = HashMap::from([("k".to_string(), "from-env".to_string())]);
        let collection = HashMap::from([("k".to_string(), Value::String("from-collection".into()))]);
        let globals = HashMap::from([("k".to_string(), Value::String("from-globals".into()))]);

        // Full shadow chain: data wins.
        let got = variables_lookup("k", Some(&data), &env, &collection, &globals);
        assert_eq!(got.as_deref(), Some("\"from-data\""), "iteration data must beat env/collection/globals");

        // No data: env wins.
        let got = variables_lookup("k", None, &env, &collection, &globals);
        assert_eq!(got.as_deref(), Some("\"from-env\""), "environment must beat collection/globals");

        // No data, no env: collection wins.
        let got = variables_lookup("k", None, &HashMap::new(), &collection, &globals);
        assert_eq!(got.as_deref(), Some("\"from-collection\""), "collection must beat globals");

        // Only globals.
        let got = variables_lookup("k", None, &HashMap::new(), &HashMap::new(), &globals);
        assert_eq!(got.as_deref(), Some("\"from-globals\""), "globals last resort");

        // No scope has it.
        let got = variables_lookup("missing", Some(&data), &env, &collection, &globals);
        assert_eq!(got, None, "unknown key resolves to None");
    }

    /// Regression (backlog line 68): `pm.response.json()` returned
    /// simd-json's IN-PLACE scratch buffer. simd-json rewrites the input
    /// bytes while de-escaping, so the returned text contained raw control
    /// chars / stale bytes for any body with `\n`, `\"`, `\\` or `\uXXXX`.
    /// The bridge must validate against a scratch copy and return the
    /// ORIGINAL bytes, which the shim JSON.parses cleanly.
    #[test]
    fn test_response_json_returns_pristine_body() {
        // The classic failing body: a real newline escape. The old code
        // de-escaped it in-place, shortening the string but keeping the
        // buffer length → stale byte after the newline → invalid JSON.
        // NOTE: the \n, \", \\ and \uXXXX here are JSON ESCAPE sequences
        // inside the raw string (backslash + char), NOT real control bytes —
        // a literal newline byte would be invalid JSON.
        let body = br#"{"a":"x\ny","b":"\"q\"","c":"path\\to","d":"\u00e9"}"#;
        let s = response_json_string(body).expect("valid JSON must be returned");
        // The returned text must be EXACTLY the original bytes — pristine
        // for the shim's JSON.parse().
        assert_eq!(s.as_bytes(), body);
        // And it must actually parse.
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["a"], "x\ny");
        assert_eq!(v["b"], "\"q\"");
        assert_eq!(v["c"], "path\\to");
        assert_eq!(v["d"], "é");

        // Non-JSON body → None (shim throws its descriptive error).
        assert!(response_json_string(b"not json").is_none());
        // Empty body → None.
        assert!(response_json_string(b"").is_none());
    }
}

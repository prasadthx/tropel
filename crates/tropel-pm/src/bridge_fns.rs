use std::collections::HashMap;
use crate::bridge::{PendingRequest, SharedPmState};
use rquickjs::function::Func;
use serde_json::Value;
use tropel_core::Result;
use tropel_js::JsContext;

/// Convert a serde_json::Value to a string suitable for JS consumption.
/// Always JSON-encodes the value so the JS shim can JSON.parse() to
/// restore the correct type. This ensures "123" (string) survives as
/// the string "123" rather than being parsed as the number 123.
/// Environment variables (plain HashMap<String, String>) bypass this
/// and return the raw string; the JS try/catch handles those correctly.
fn variable_value_to_string(val: &Value) -> String {
    serde_json::to_string(val).unwrap_or_default()
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
}

impl PmBridge {
    pub fn new(state: SharedPmState) -> Self {
        Self { state }
    }

    /// Register all bridge functions into the given JS context.
    pub fn install(&self, ctx: &JsContext) -> Result<()> {
        let state = self.state.clone();

        ctx.with_ctx(|rq_ctx| {
            let globals = rq_ctx.globals();

            // ── Environment ──
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
            // Returns Option<String>: strings return the inner value directly,
            // non-strings (objects, arrays, numbers, booleans) return JSON-encoded.
            // The JS shim calls JSON.parse() to restore non-string types.
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_variables_get",
                Func::from(move |key: String| -> Option<String> {
                    let st = state_clone.lock().unwrap();
                    // Environment variables are always plain strings
                    if let Some(val) = st.environment.get(&key) {
                        return Some(val.clone());
                    }
                    // Collection and global variables are serde_json::Value
                    if let Some(val) = st.collection_vars.get(&key) {
                        return Some(variable_value_to_string(val));
                    }
                    st.globals.get(&key)
                        .map(|v| variable_value_to_string(v))
                }),
            );

            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_variables_set",
                Func::from(move |key: String, value: String| {
                    let mut st = state_clone.lock().unwrap();
                    st.collection_vars.insert(key, serde_json::Value::String(value));
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
                    st.response.as_ref().map(|r| r.status_text.clone()).unwrap_or_default()
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
                    st.response.as_ref()
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
                    st.response.as_ref().map(|r| r.headers.clone()).unwrap_or_default()
                }),
            );

            // ── Response Header (individual header access, widely used) ──
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_response_header",
                Func::from(move |key: String| -> Option<String> {
                    let st = state_clone.lock().unwrap();
                    st.response.as_ref().and_then(|r| r.headers.get(&key).cloned())
                }),
            );

            // ── Response Cookies (name → value map)
            // rquickjs 0.12+ supports HashMap<String,String> as IntoJs -> JS object
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_response_cookies",
                Func::from(move || -> HashMap<String, String> {
                    let st = state_clone.lock().unwrap();
                    st.response.as_ref().map(|r| {
                        r.cookies.iter().map(|c| (c.name.clone(), c.value.clone())).collect()
                    }).unwrap_or_default()
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
                    st.response.as_ref().and_then(|r| {
                        // Validate JSON directly from raw bytes using simd-json
                        // This avoids the String::from_utf8 and serde_json::from_str steps
                        let mut body_bytes = r.body.clone();
                        if body_bytes.is_empty() {
                            return None;
                        }
                        simd_json::serde::from_slice::<serde_json::Value>(&mut body_bytes).ok()?;
                        // Return body text for JS-side JSON.parse()
                        String::from_utf8(body_bytes).ok()
                    })
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
                    st.iteration_data.as_ref().and_then(|data| {
                        data.get(&key).map(|val| variable_value_to_string(val))
                    })
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

            // ── sendRequest ──
            // Queues a request for later async execution. Returns a JSON error
            // response since true async sendRequest isn't implemented yet.
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_send_request",
                Func::from(
                    move |method: String, url: String, headers_json: String, body: String| -> String {
                        let headers: HashMap<String, String> =
                            serde_json::from_str(&headers_json).unwrap_or_default();
                        {
                            let mut st = state_clone.lock().unwrap();
                            st.pending_requests.push(PendingRequest {
                                method: method.clone(),
                                url: url.clone(),
                                headers,
                                body: if body.is_empty() { None } else { Some(body) },
                            });
                        }
                        // Return a JSON response indicating queued status
                        serde_json::json!({
                            "status": "queued",
                            "code": 0,
                            "statusText": "Request queued — async sendRequest will be processed in a future iteration",
                            "body": "",
                            "headers": {},
                            "responseTime": 0
                        }).to_string()
                    },
                ),
            );
        });

        Ok(())
    }
}

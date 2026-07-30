use std::collections::HashMap;
use std::sync::Arc;
use crate::bridge::SharedPmState;
use rquickjs::function::Func;
use serde_json::Value;
use tropel_core::Result;
use tropel_core::types::{Body, Method, Request};
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
    /// A separate multi-thread tokio runtime for blocking bridge calls.
    /// Required because each VU runs on a current-thread runtime where
    /// Handle::current().block_on() would panic. This independent runtime
    /// is used instead for synchronous FFI bridge calls that need to await
    /// async operations (like pm.sendRequest → reqwest HTTP calls).
    blocking_rt: Arc<tokio::runtime::Runtime>,
}

impl PmBridge {
    pub fn new(
        state: SharedPmState,
        http_client: Arc<HttpClient>,
        blocking_rt: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        Self { state, http_client, blocking_rt }
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
            // Returns Option<String>: ALL variable scopes return JSON-encoded
            // strings so the JS shim can `JSON.parse()` to restore the correct
            // JS type. Without encoding, an env var like "123" would be parsed
            // as the number 123 instead of the string "123".
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_variables_get",
                Func::from(move |key: String| -> Option<String> {
                    let st = state_clone.lock().unwrap();
                    // Environment variables are HashMap<String, String>
                    if let Some(val) = st.environment.get(&key) {
                        return Some(string_to_json_encoded(val));
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
                        metric: "group_duration".to_string(),
                        value: duration_micros as f64,
                        tags,
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
                        metric: name,
                        value,
                        tags: tropel_core::types::TagMap::new(),
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
                Func::from(move |name: String, value: f64, tags_json: String, metric_type_str: String| {
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
                        metric: name,
                        value,
                        tags,
                        timestamp: std::time::SystemTime::now(),
                        sample_type,
                    });
                }),
            );

            // ═══════════════════════════════════════════════════
            // Execution context (k6 exec.* / test.abort())
            // ═══════════════════════════════════════════════════
            // exec.vu.idInTest — VU ID
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_exec_vu_id",
                Func::from(move || -> u32 {
                    state_clone.lock().unwrap().vu_id
                }),
            );

            // exec.scenario.name — scenario name
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_exec_scenario_name",
                Func::from(move || -> String {
                    state_clone.lock().unwrap().scenario_name.clone()
                }),
            );

            // Known limitation: exec.scenario.executor — executor type string
            // (e.g., "constant-vus", "ramping-vus") is not yet piped through to
            // PmState. The ExecutionConfig enum variant is available when the
            // VURunner is created but not stored. Always returns "".
            let _ = globals.set(
                "__tropel_exec_scenario_executor",
                Func::from(|| -> String { String::new() }),
            );

            // exec.vu.iterationInScenario — current iteration index
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_exec_iteration",
                Func::from(move || -> u64 {
                    state_clone.lock().unwrap().iteration_index
                }),
            );

            // Known limitation: exec.instance.iterationsCompleted — returns the
            // per-VU iteration count, NOT a global total across all VUs (which is
            // what k6 provides). Requires a shared atomic counter across VUs in
            // the scheduler to be piped into PmState.
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_exec_iterations_completed",
                Func::from(move || -> u64 {
                    let st = state_clone.lock().unwrap();
                    st.iteration_index
                }),
            );

            // Known limitation: exec.instance.vusActive — currently active VUs.
            // Requires the scheduler's active_vus count to be passed to PmState
            // each iteration. k6 provides this via the instance object, but the
            // connection from VU runner to scheduler is indirect. Always returns 0.
            let _ = globals.set(
                "__tropel_exec_vus_active",
                Func::from(|| -> u32 { 0 }),
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
            // The bridge closure runs inside ctx.with() (synchronous), so we use
            // a separate multi-thread tokio runtime (blocking_rt) to await the async
            // client.execute(). This avoids the "cannot block from within a runtime"
            // panic that would occur with Handle::current().block_on() on a
            // current-thread runtime (thread-per-core architecture).
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
            // Returns: JSON-encoded response with code, statusText, body, headers, responseTime
            let http = self.http_client.clone();
            let state_for_send = self.state.clone();
            let blocking_rt = self.blocking_rt.clone();
            let _ = globals.set(
                "__tropel_pm_send_request",
                Func::from(
                    move |method: String, url: String, headers_json: String, body: String, timeout_ms: f64| -> String {
                        // Resolve {{variables}} in the URL using current PM state
                        let resolved_url = {
                            let st = state_for_send.lock().unwrap();
                            resolve_vars(&url, &st.environment, &st.collection_vars, &st.globals)
                        };

                        // Parse headers — supports both object form {"key":"val"} and
                        // Postman array form [{"key":"Content-Type","value":"application/json"}]
                        let headers: HashMap<String, String> =
                            parse_headers(&headers_json);

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

                        let req = Request {
                            url: resolved_url,
                            method: Method::from_str(&method).unwrap_or(Method::GET),
                            headers,
                            query_params: HashMap::new(),
                            body: request_body,
                            auth: None,
                            certificate: None,
                            follow_redirects: true,
                            timeout,
                        };

                        // Execute the request on the independent blocking runtime.
                        // This runtime is multi-thread, so block_on() works fine
                        // even though we're inside a current-thread VU runtime.
                        match blocking_rt.block_on(http.execute(&req, None)) {
                            Ok(http_resp) => {
                                let body_text = String::from_utf8(http_resp.body.clone()).unwrap_or_default();
                                serde_json::json!({
                                    "code": http_resp.status_code,
                                    "statusText": http_resp.status_text,
                                    "body": body_text,
                                    "headers": http_resp.headers,
                                    "responseTime": http_resp.response_time.as_secs_f64() * 1000.0,
                                }).to_string()
                            }
                            Err(e) => {
                                serde_json::json!({
                                    "code": 0,
                                    "statusText": format!("Request failed: {}", e),
                                    "body": "",
                                    "headers": {},
                                    "responseTime": 0,
                                }).to_string()
                            }
                        }
                    },
                ),
            );
        });

        Ok(())
    }
}

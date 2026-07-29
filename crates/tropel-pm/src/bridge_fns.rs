use std::collections::HashMap;
use crate::bridge::SharedPmState;
use rquickjs::function::Func;
use tropel_core::Result;
use tropel_js::JsContext;

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

            // ── Variables (string-only for JS compat) ──
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_variables_get",
                Func::from(move |key: String| -> Option<String> {
                    let st = state_clone.lock().unwrap();
                    if let Some(val) = st.environment.get(&key) {
                        return Some(val.clone());
                    }
                    if let Some(val) = st.collection_vars.get(&key) {
                        return Some(serde_json::to_string(val).unwrap_or_default());
                    }
                    st.globals.get(&key)
                        .map(|v| serde_json::to_string(v).unwrap_or_default())
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
                    st.response.as_ref().and_then(|r| r.body_text.clone())
                }),
            );

            // __tropel_pm_response_json intentionally omitted: Func::from doesn't
            // support returning serde_json::Value (which is what pm.response.json()
            // expects as a parsed object). Returning a JSON string would break
            // pm.response.json().key access. Users should call:
            //   JSON.parse(pm.response.text())
            // until a typed bridge binding is available.

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
            // but returning Option<String> (JSON text) works. The pm.js shim now parses
            // this string via JSON.parse() to produce the expected object.
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_response_json",
                Func::from(move || -> Option<String> {
                    let st = state_clone.lock().unwrap();
                    st.response.as_ref().and_then(|r| r.body_text.clone())
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
            let state_clone = state.clone();
            let _ = globals.set(
                "__tropel_pm_set_next_request",
                Func::from(move |request_id: String| {
                    let idx: Option<usize> = request_id.parse().ok();
                    if let Some(index) = idx {
                        let mut st = state_clone.lock().unwrap();
                        st.next_request = Some(index);
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
        });

        Ok(())
    }
}

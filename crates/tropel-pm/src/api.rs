use crate::bridge::SharedPmState;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tropel_core::types::{Response, TagMap};

/// The pm.* API surface exposed to JS scripts.
pub struct PmApi {
    state: SharedPmState,
}

impl PmApi {
    pub fn new(state: SharedPmState) -> Self {
        Self { state }
    }

    /// Access the shared state.
    pub fn state(&self) -> &SharedPmState {
        &self.state
    }

    // ── Environment ──

    /// Get an environment variable.
    pub fn environment_get(&self, key: &str) -> Option<String> {
        let state = self.state.lock().unwrap();
        state.environment.get(key).cloned()
    }

    /// Set an environment variable.
    pub fn environment_set(&self, key: &str, value: &str) {
        let mut state = self.state.lock().unwrap();
        state.environment.insert(key.to_string(), value.to_string());
    }

    /// Unset an environment variable.
    pub fn environment_unset(&self, key: &str) {
        let mut state = self.state.lock().unwrap();
        state.environment.remove(key);
    }

    /// Clear all environment variables.
    pub fn environment_clear(&self) {
        let mut state = self.state.lock().unwrap();
        state.environment.clear();
    }

    // ── Variables ──

    /// Get a variable (searches iteration data, then environment, then
    /// collection, then globals — Postman precedence, backlog line 145).
    pub fn variables_get(&self, key: &str) -> Option<Value> {
        let state = self.state.lock().unwrap();
        if let Some(val) = state.iteration_data.as_ref().and_then(|d| d.get(key)) {
            return Some(val.clone());
        }
        if let Some(val) = state.environment.get(key) {
            return Some(Value::String(val.clone()));
        }
        if let Some(val) = state.collection_vars.get(key) {
            return Some(val.clone());
        }
        state.globals.get(key).cloned()
    }

    /// Set a variable (in collection scope).
    pub fn variables_set(&self, key: &str, value: Value) {
        let mut state = self.state.lock().unwrap();
        state.collection_vars.insert(key.to_string(), value);
    }

    /// Unset a variable.
    pub fn variables_unset(&self, key: &str) {
        let mut state = self.state.lock().unwrap();
        state.collection_vars.remove(key);
        state.environment.remove(key);
        state.globals.remove(key);
    }

    // ── Response ──

    /// Get the current response.
    pub fn response_get(&self) -> Option<Response> {
        let state = self.state.lock().unwrap();
        state.response.clone()
    }

    /// Get the response code.
    pub fn response_code(&self) -> Option<u16> {
        let state = self.state.lock().unwrap();
        state.response.as_ref().map(|r| r.status_code)
    }

    /// Get the response body as text (lazy — decodes on access).
    pub fn response_body(&self) -> Option<String> {
        let state = self.state.lock().unwrap();
        state.response.as_ref().and_then(|r| r.body_text())
    }

    /// Get the response body as JSON (lazy — parses on access).
    pub fn response_json(&self) -> Option<Value> {
        let state = self.state.lock().unwrap();
        state.response.as_ref().and_then(|r| r.body_json())
    }

    /// Get response headers.
    pub fn response_headers(&self) -> HashMap<String, String> {
        let state = self.state.lock().unwrap();
        state
            .response
            .as_ref()
            .map(|r| r.headers.clone())
            .unwrap_or_default()
    }

    /// Get a specific response header (case-insensitive — HTTP headers are
    /// case-insensitive, matching Postman and the bridge_fns lookup).
    pub fn response_header(&self, key: &str) -> Option<String> {
        let state = self.state.lock().unwrap();
        state.response.as_ref().and_then(|r| {
            r.headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(key))
                .map(|(_, v)| v.clone())
        })
    }

    /// Get response time in milliseconds.
    pub fn response_time(&self) -> Option<f64> {
        let state = self.state.lock().unwrap();
        state
            .response
            .as_ref()
            .map(|r| r.response_time.as_secs_f64() * 1000.0)
    }

    /// Get response cookies.
    pub fn response_cookies(&self) -> Vec<tropel_core::types::Cookie> {
        let state = self.state.lock().unwrap();
        state
            .response
            .as_ref()
            .map(|r| r.cookies.clone())
            .unwrap_or_default()
    }

    // ── Test ──

    /// Execute a named test assertion.
    pub fn test(&self, name: &str, passed: bool) {
        let mut state = self.state.lock().unwrap();
        state.record_test(name, passed);
    }

    // ── Iteration Data ──

    /// Get the current iteration data value.
    pub fn iteration_data_get(&self, key: &str) -> Option<Value> {
        let state = self.state.lock().unwrap();
        state
            .iteration_data
            .as_ref()
            .and_then(|data| data.get(key).cloned())
    }

    // ── Flow Control ──

    /// Set the next request index (for setNextRequest flow control).
    pub fn set_next_request(&self, index: usize) {
        let mut state = self.state.lock().unwrap();
        state.next_request = Some(index);
    }

    /// Get the pending next request index.
    pub fn get_next_request(&self) -> Option<usize> {
        let mut state = self.state.lock().unwrap();
        state.next_request.take()
    }

    /// Skip remaining tests for this request.
    pub fn skip_tests(&self) {
        let mut state = self.state.lock().unwrap();
        state.skip_tests = true;
    }

    // ── Assertion shortcuts ──

    pub fn expect_true(&self, name: &str, condition: bool) -> bool {
        self.test(name, condition);
        condition
    }

    pub fn expect_equal(&self, name: &str, actual: &str, expected: &str) -> bool {
        let passed = actual == expected;
        self.test(name, passed);
        passed
    }

    pub fn expect_status(&self, expected: u16) -> bool {
        let code = self.response_code();
        let passed = code == Some(expected);
        self.test(&format!("Status is {}", expected), passed);
        passed
    }

    pub fn expect_body_contains(&self, name: &str, substring: &str) -> bool {
        let body = self.response_body();
        let passed = body.is_some_and(|b| b.contains(substring));
        self.test(name, passed);
        passed
    }

    pub fn expect_header(&self, name: &str, key: &str, expected: &str) -> bool {
        let header = self.response_header(key);
        let passed = header.as_deref() == Some(expected);
        self.test(name, passed);
        passed
    }

    // ── Group ──

    /// Start a named group (pushes onto the group stack).
    pub fn group_start(&self, name: &str) {
        let mut state = self.state.lock().unwrap();
        state.group_stack.push(name.to_string());
        state.current_group = Some(state.group_stack.join("::"));
    }

    /// End a named group (pops from stack, records group_duration).
    pub fn group_end(&self, name: &str, duration_ms: f64) {
        let mut state = self.state.lock().unwrap();
        // Pop the matching group
        if state.group_stack.last().map(|n| n == name).unwrap_or(false) {
            state.group_stack.pop();
        }
        // Rebuild the current group path
        state.current_group = if state.group_stack.is_empty() {
            None
        } else {
            Some(state.group_stack.join("::"))
        };

        // Emit group_duration sample (Trend) in microseconds
        let duration_micros = (duration_ms * 1000.0) as u64;
        let mut tags = HashMap::new();
        tags.insert("group".to_string(), name.to_string());
        if let Some(ref path) = state.current_group {
            tags.insert("group_path".to_string(), path.clone());
        }
        state.samples.push(tropel_core::types::Sample {
            metric: "group_duration".into(),
            value: duration_micros as f64,
            tags: Arc::new(TagMap::from_pairs(tags)),
            timestamp: tropel_core::clock::monotonic_wall_now(),
            sample_type: tropel_core::types::SampleType::Trend,
        });
    }

    // ── Check ──

    /// Run a named check (records pass/fail to checks Rate metric).
    /// Returns true if the check passed.
    ///
    /// The recorded name is the RAW check name — no "check " prefix (k6
    /// convention, matching [`crate::bridge::PmState::record_test_tagged`]).
    pub fn check(&self, name: &str, passed: bool) -> bool {
        self.test(name, passed);
        passed
    }

    // ── Sample Emission ──

    pub fn emit_metric(&self, metric: &str, value: f64, tags: HashMap<String, String>) {
        let mut state = self.state.lock().unwrap();
        state.samples.push(tropel_core::types::Sample {
            metric: std::borrow::Cow::Owned(metric.to_string()),
            value,
            tags: Arc::new(TagMap::from_pairs(tags)),
            timestamp: tropel_core::clock::monotonic_wall_now(),
            sample_type: tropel_core::types::SampleType::Point,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::new_pm_state;
    use std::time::Duration;

    /// Build a `Response` fixture (mirrors the `From<&HttpResponse>` shape).
    fn resp(code: u16, body: &str, headers: &[(&str, &str)]) -> Response {
        Response {
            url: "https://api.example.com/users".to_string(),
            status_code: code,
            status_text: if code == 200 { "OK".into() } else { "Error".into() },
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: body.as_bytes().to_vec(),
            text_cache: std::cell::OnceCell::new(),
            json_cache: std::cell::OnceCell::new(),
            response_time: Duration::from_millis(42),
            timings: None,
            cookies: vec![tropel_core::types::Cookie {
                name: "session".into(),
                value: "abc123".into(),
                domain: None,
                path: None,
                http_only: None,
                secure: None,
                same_site: None,
                expires: None,
            }],
            size: body.len() as u64,
            redirects: Vec::new(),
        }
    }

    #[test]
    fn environment_set_get_unset_clear() {
        let state = new_pm_state();
        let api = PmApi::new(state.clone());
        // Set/get round-trip.
        api.environment_set("base_url", "https://example.com");
        assert_eq!(api.environment_get("base_url").as_deref(), Some("https://example.com"));
        // Unknown key → None.
        assert_eq!(api.environment_get("missing"), None);
        // Unset removes.
        api.environment_unset("base_url");
        assert_eq!(api.environment_get("base_url"), None);
        // Clear wipes everything.
        api.environment_set("a", "1");
        api.environment_set("b", "2");
        api.environment_clear();
        assert!(api.environment_get("a").is_none() && api.environment_get("b").is_none());
    }

    #[test]
    fn variables_postman_precedence_data_env_collection_globals() {
        let state = new_pm_state();
        let api = PmApi::new(state.clone());
        api.variables_set("k", Value::String("from-collection".into()));
        assert_eq!(
            api.variables_get("k"),
            Some(Value::String("from-collection".into()))
        );
        // Environment beats collection.
        api.environment_set("k", "from-env");
        assert_eq!(api.variables_get("k"), Some(Value::String("from-env".into())));
        // Globals beat nothing when env present; add globals and drop env.
        state.lock().unwrap().globals.insert("k".into(), Value::String("from-globals".into()));
        assert_eq!(api.variables_get("k"), Some(Value::String("from-env".into())));
        api.environment_unset("k");
        assert_eq!(api.variables_get("k"), Some(Value::String("from-collection".into())));
        // Iteration data beats everything (backlog line 145 precedence).
        state
            .lock()
            .unwrap()
            .set_iteration_data(Some(HashMap::from([("k".into(), Value::String("from-data".into()))])));
        assert_eq!(api.variables_get("k"), Some(Value::String("from-data".into())));
        // Unset removes from collection/environment/globals — but NOT the
        // per-iteration data row (that scope is reset between iterations,
        // not by pm.variables.unset).
        api.variables_unset("k");
        assert_eq!(api.variables_get("k"), Some(Value::String("from-data".into())));
        state.lock().unwrap().set_iteration_data(None);
        assert_eq!(api.variables_get("k"), None);
        // Globals-fallback branch: only globals holds the key.
        state
            .lock()
            .unwrap()
            .globals
            .insert("k".into(), Value::String("from-globals".into()));
        assert_eq!(api.variables_get("k"), Some(Value::String("from-globals".into())));
    }

    #[test]
    fn response_accessors_cover_whole_surface() {
        let state = new_pm_state();
        let api = PmApi::new(state.clone());
        // No response yet → empty accessors.
        assert_eq!(api.response_code(), None);
        assert_eq!(api.response_body(), None);
        assert_eq!(api.response_json(), None);
        assert!(api.response_headers().is_empty());
        assert_eq!(api.response_header("content-type"), None);
        assert_eq!(api.response_time(), None);
        assert!(api.response_cookies().is_empty());

        state.lock().unwrap().response = Some(resp(
            200,
            r#"{"id":42,"name":"Ada"}"#,
            &[("Content-Type", "application/json")],
        ));
        assert_eq!(api.response_code(), Some(200));
        assert_eq!(api.response_body().as_deref(), Some(r#"{"id":42,"name":"Ada"}"#));
        assert_eq!(api.response_json(), Some(serde_json::json!({ "id": 42, "name": "Ada" })));
        assert_eq!(api.response_header("content-type").as_deref(), Some("application/json"));
        assert_eq!(api.response_headers().get("Content-Type").map(String::as_str), Some("application/json"));
        assert_eq!(api.response_time(), Some(42.0));
        let cookies = api.response_cookies();
        assert_eq!(cookies.len(), 1);
        assert_eq!(cookies[0].name, "session");
        assert_eq!(api.response_get().map(|r| r.status_code), Some(200));
    }

    #[test]
    fn test_and_check_record_assertions() {
        let state = new_pm_state();
        let api = PmApi::new(state.clone());
        api.test("status is 200", true);
        api.test("body has id", false);
        api.check("users", true);
        let st = state.lock().unwrap();
        assert_eq!(st.assertions.total, 3);
        assert_eq!(st.assertions.passed, 2);
        assert_eq!(st.assertions.failed, 1);
        // Every test/check emits a `checks` Rate sample.
        let checks: Vec<_> = st
            .samples
            .iter()
            .filter(|s| s.metric == "checks")
            .collect();
        assert_eq!(checks.len(), 3);
        assert_eq!(checks[0].value, 1.0);
        assert_eq!(checks[1].value, 0.0);
        // check() records the RAW name — no "check " prefix (k6 convention).
        assert_eq!(checks[2].tags.get("check").map(|s| s.as_ref()), Some("users"));
    }

    #[test]
    fn expect_shortcuts_chain_into_assertions() {
        let state = new_pm_state();
        let api = PmApi::new(state.clone());
        assert!(api.expect_true("truthy", true));
        assert!(!api.expect_true("falsy", false));
        api.expect_equal("eq", "a", "a");
        state.lock().unwrap().response = Some(resp(201, "created ok", &[("X-Id", "9")]));
        assert!(api.expect_status(201));
        assert!(!api.expect_status(200));
        assert!(api.expect_body_contains("has created", "created"));
        assert!(api.expect_header("has xid", "x-id", "9"));
        let st = state.lock().unwrap();
        assert_eq!(st.assertions.total, 7);
        assert_eq!(st.assertions.passed, 5);
        assert_eq!(st.assertions.failed, 2);
    }

    #[test]
    fn flow_control_next_request_is_take_once() {
        let state = new_pm_state();
        let api = PmApi::new(state.clone());
        assert_eq!(api.get_next_request(), None);
        api.set_next_request(3);
        assert_eq!(api.get_next_request(), Some(3));
        // take() semantics: a second read is None.
        assert_eq!(api.get_next_request(), None);
        api.skip_tests();
        assert!(state.lock().unwrap().skip_tests);
    }

    #[test]
    fn groups_nest_and_emit_group_duration() {
        let state = new_pm_state();
        let api = PmApi::new(state.clone());
        api.group_start("outer");
        api.group_start("inner");
        assert_eq!(state.lock().unwrap().current_group.as_deref(), Some("outer::inner"));
        api.group_end("inner", 10.0);
        assert_eq!(state.lock().unwrap().current_group.as_deref(), Some("outer"));
        let st = state.lock().unwrap();
        let gd = st.samples.iter().find(|s| s.metric == "group_duration").unwrap();
        // 10 ms → 10,000 µs Trend sample tagged with the group name.
        assert_eq!(gd.value, 10_000.0);
        assert_eq!(gd.tags.get("group").map(|s| s.as_ref()), Some("inner"));
        assert_eq!(gd.tags.get("group_path").map(|s| s.as_ref()), Some("outer"));
    }

    #[test]
    fn emit_metric_pushes_point_sample_with_tags() {
        let state = new_pm_state();
        let api = PmApi::new(state.clone());
        api.emit_metric("custom_metric", 3.14, HashMap::from([("env".into(), "prod".into())]));
        let st = state.lock().unwrap();
        let s = st.samples.last().unwrap();
        assert_eq!(s.metric, "custom_metric");
        assert_eq!(s.value, 3.14);
        assert_eq!(s.tags.get("env").map(|v| v.as_ref()), Some("prod"));
    }

    #[test]
    fn iteration_data_accessor_reads_current_row() {
        let state = new_pm_state();
        let api = PmApi::new(state.clone());
        assert_eq!(api.iteration_data_get("id"), None);
        state
            .lock()
            .unwrap()
            .set_iteration_data(Some(HashMap::from([("id".into(), Value::from(7))])));
        assert_eq!(api.iteration_data_get("id"), Some(Value::from(7)));
    }
}

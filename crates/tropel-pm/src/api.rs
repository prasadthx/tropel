use crate::bridge::SharedPmState;
use serde_json::Value;
use std::collections::HashMap;
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

    /// Get a variable (searches environment, then collection, then globals).
    pub fn variables_get(&self, key: &str) -> Option<Value> {
        let state = self.state.lock().unwrap();
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

    /// Get a specific response header.
    pub fn response_header(&self, key: &str) -> Option<String> {
        let state = self.state.lock().unwrap();
        state
            .response
            .as_ref()
            .and_then(|r| r.headers.get(key).cloned())
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

    pub fn expect_true(&self, name: &str, condition: bool) {
        self.test(name, condition);
    }

    pub fn expect_equal(&self, name: &str, actual: &str, expected: &str) {
        self.test(name, actual == expected);
    }

    pub fn expect_status(&self, expected: u16) -> bool {
        let code = self.response_code();
        let passed = code == Some(expected);
        self.test(&format!("Status is {}", expected), passed);
        passed
    }

    pub fn expect_body_contains(&self, name: &str, substring: &str) -> bool {
        let body = self.response_body();
        let passed = body.map_or(false, |b| b.contains(substring));
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
            metric: "group_duration".to_string(),
            value: duration_micros as f64,
            tags: TagMap::from_pairs(tags),
            timestamp: std::time::SystemTime::now(),
            sample_type: tropel_core::types::SampleType::Trend,
        });
    }

    // ── Check ──

    /// Run a named check (records pass/fail to checks Rate metric).
    /// Returns true if the check passed.
    pub fn check(&self, name: &str, passed: bool) -> bool {
        self.test(&format!("check {}", name), passed);
        passed
    }

    // ── Sample Emission ──

    pub fn emit_metric(&self, metric: &str, value: f64, tags: HashMap<String, String>) {
        let mut state = self.state.lock().unwrap();
        state.samples.push(tropel_core::types::Sample {
            metric: metric.to_string(),
            value,
            tags: TagMap::from_pairs(tags),
            timestamp: std::time::SystemTime::now(),
            sample_type: tropel_core::types::SampleType::Point,
        });
    }
}

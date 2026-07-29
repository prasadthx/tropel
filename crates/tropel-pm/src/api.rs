use crate::bridge::SharedPmState;
use serde_json::Value;
use std::collections::HashMap;
use tropel_core::types::Response;


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
    pub async fn environment_get(&self, key: &str) -> Option<String> {
        let state = self.state.lock().await;
        state.environment.get(key).cloned()
    }

    /// Set an environment variable.
    pub async fn environment_set(&self, key: &str, value: &str) {
        let mut state = self.state.lock().await;
        state.environment.insert(key.to_string(), value.to_string());
    }

    /// Unset an environment variable.
    pub async fn environment_unset(&self, key: &str) {
        let mut state = self.state.lock().await;
        state.environment.remove(key);
    }

    /// Clear all environment variables.
    pub async fn environment_clear(&self) {
        let mut state = self.state.lock().await;
        state.environment.clear();
    }

    // ── Variables ──

    /// Get a variable (searches environment, then collection, then globals).
    pub async fn variables_get(&self, key: &str) -> Option<Value> {
        let state = self.state.lock().await;
        if let Some(val) = state.environment.get(key) {
            return Some(Value::String(val.clone()));
        }
        if let Some(val) = state.collection_vars.get(key) {
            return Some(val.clone());
        }
        state.globals.get(key).cloned()
    }

    /// Set a variable (in collection scope).
    pub async fn variables_set(&self, key: &str, value: Value) {
        let mut state = self.state.lock().await;
        state.collection_vars.insert(key.to_string(), value);
    }

    /// Unset a variable.
    pub async fn variables_unset(&self, key: &str) {
        let mut state = self.state.lock().await;
        state.collection_vars.remove(key);
        state.environment.remove(key);
        state.globals.remove(key);
    }

    // ── Response ──

    /// Get the current response.
    pub async fn response_get(&self) -> Option<Response> {
        let state = self.state.lock().await;
        state.response.clone()
    }

    /// Get the response code.
    pub async fn response_code(&self) -> Option<u16> {
        let state = self.state.lock().await;
        state.response.as_ref().map(|r| r.status_code)
    }

    /// Get the response body as text.
    pub async fn response_body(&self) -> Option<String> {
        let state = self.state.lock().await;
        state.response.as_ref().and_then(|r| r.body_text.clone())
    }

    /// Get the response body as JSON.
    pub async fn response_json(&self) -> Option<Value> {
        let state = self.state.lock().await;
        state.response.as_ref().and_then(|r| r.body_json.clone())
    }

    /// Get response headers.
    pub async fn response_headers(&self) -> HashMap<String, String> {
        let state = self.state.lock().await;
        state
            .response
            .as_ref()
            .map(|r| r.headers.clone())
            .unwrap_or_default()
    }

    /// Get a specific response header.
    pub async fn response_header(&self, key: &str) -> Option<String> {
        let state = self.state.lock().await;
        state
            .response
            .as_ref()
            .and_then(|r| r.headers.get(key).cloned())
    }

    /// Get response time in milliseconds.
    pub async fn response_time(&self) -> Option<f64> {
        let state = self.state.lock().await;
        state
            .response
            .as_ref()
            .map(|r| r.response_time.as_secs_f64() * 1000.0)
    }

    /// Get response cookies.
    pub async fn response_cookies(&self) -> Vec<tropel_core::types::Cookie> {
        let state = self.state.lock().await;
        state
            .response
            .as_ref()
            .map(|r| r.cookies.clone())
            .unwrap_or_default()
    }

    // ── Test ──

    /// Execute a named test assertion.
    pub async fn test(&self, name: &str, passed: bool) {
        let mut state = self.state.lock().await;
        state.record_test(name, passed);
    }

    // ── Iteration Data ──

    /// Get the current iteration data value.
    pub async fn iteration_data_get(&self, _key: &str) -> Option<Value> {
        // Handled by the executor before each iteration
        None
    }

    // ── Flow Control ──

    /// Set the next request index (for setNextRequest flow control).
    pub async fn set_next_request(&self, index: usize) {
        let mut state = self.state.lock().await;
        state.next_request = Some(index);
    }

    /// Get the pending next request index.
    pub async fn get_next_request(&self) -> Option<usize> {
        let mut state = self.state.lock().await;
        state.next_request.take()
    }

    /// Skip remaining tests for this request.
    pub async fn skip_tests(&self) {
        let mut state = self.state.lock().await;
        state.skip_tests = true;
    }

    // ── Assertion shortcuts ──

    /// Assert that a condition is true.
    pub async fn expect_true(&self, name: &str, condition: bool) {
        self.test(name, condition).await;
    }

    /// Assert that two values are equal.
    pub async fn expect_equal(&self, name: &str, actual: &str, expected: &str) {
        let passed = actual == expected;
        self.test(name, passed).await;
    }

    /// Assert that a response status code matches.
    pub async fn expect_status(&self, expected: u16) -> bool {
        let code = self.response_code().await;
        let passed = code == Some(expected);
        self.test(&format!("Status is {}", expected), passed).await;
        passed
    }

    /// Assert that the response body contains a string.
    pub async fn expect_body_contains(&self, name: &str, substring: &str) -> bool {
        let body = self.response_body().await;
        let passed = body.map_or(false, |b| b.contains(substring));
        self.test(name, passed).await;
        passed
    }

    /// Assert that the response header contains a value.
    pub async fn expect_header(&self, name: &str, key: &str, expected: &str) -> bool {
        let header = self.response_header(key).await;
        let passed = header.as_deref() == Some(expected);
        self.test(name, passed).await;
        passed
    }

    // ── Sample Emission ──

    /// Emit a custom metric sample.
    pub async fn emit_metric(&self, metric: &str, value: f64, tags: HashMap<String, String>) {
        let mut state = self.state.lock().await;
        state.samples.push(tropel_core::types::Sample {
            metric: metric.to_string(),
            value,
            tags,
            timestamp: std::time::SystemTime::now(),
            sample_type: tropel_core::types::SampleType::Point,
        });
    }
}

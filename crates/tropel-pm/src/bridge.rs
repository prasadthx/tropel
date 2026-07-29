use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use tropel_core::types::{Request, Response, Sample};

/// A request queued by pm.sendRequest for later async execution.
#[derive(Debug, Clone)]
pub struct PendingRequest {
    pub method: String,
    pub url: String,
    pub headers: HashMap<String, String>,
    pub body: Option<String>,
}

/// The mutable state for a single VU's pm.* API.
/// Shared between the JS context and the native executor.
#[derive(Debug, Clone)]
pub struct PmState {
    /// Environment variables.
    pub environment: HashMap<String, String>,
    /// Collection variables.
    pub collection_vars: HashMap<String, Value>,
    /// Global variables.
    pub globals: HashMap<String, Value>,
    /// Current response (set before test script runs).
    pub response: Option<Response>,
    /// Current request being executed.
    pub request: Option<Request>,
    /// Assertion counters.
    pub assertions: AssertionCounters,
    /// Custom metrics/values set by scripts.
    pub custom: HashMap<String, Value>,
    /// Samples emitted by this VU.
    pub samples: Vec<Sample>,
    /// Flow control: next request index to jump to.
    pub next_request: Option<usize>,
    /// Whether to skip the remaining tests.
    pub skip_tests: bool,
    /// Requests queued by pm.sendRequest for async execution.
    pub pending_requests: Vec<PendingRequest>,
}

/// Assertion pass/fail counters (like pm.test results).
#[derive(Debug, Clone, Default)]
pub struct AssertionCounters {
    pub total: u64,
    pub passed: u64,
    pub failed: u64,
}

impl PmState {
    pub fn new() -> Self {
        Self {
            environment: HashMap::new(),
            collection_vars: HashMap::new(),
            globals: HashMap::new(),
            response: None,
            request: None,
            assertions: AssertionCounters::default(),
            custom: HashMap::new(),
            samples: Vec::new(),
            next_request: None,
            skip_tests: false,
            pending_requests: Vec::new(),
        }
    }

    /// Record a test (assertion) result.
    pub fn record_test(&mut self, name: &str, passed: bool) {
        self.assertions.total += 1;
        if passed {
            self.assertions.passed += 1;
        } else {
            self.assertions.failed += 1;
        }

        self.samples.push(Sample {
            metric: "checks".to_string(),
            value: if passed { 1.0 } else { 0.0 },
            tags: HashMap::from([("check".to_string(), name.to_string())]),
            timestamp: std::time::SystemTime::now(),
            sample_type: tropel_core::types::SampleType::Rate,
        });
    }
}

impl Default for PmState {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe shared PM state for passing across async boundaries.
pub type SharedPmState = Arc<Mutex<PmState>>;

/// Create a new shared PM state.
pub fn new_pm_state() -> SharedPmState {
    Arc::new(Mutex::new(PmState::new()))
}

use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Arc;
use std::sync::Mutex;
use tropel_core::types::{Request, Response, Sample, TagMap};

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
    /// Custom metrics counter values (tracked by name for pm.metrics API).
    /// Scripts can create and query custom Counter/Gauge/Trend/Rate metrics.
    pub custom_metrics: HashMap<String, f64>,
    /// Samples emitted by this VU.
    pub samples: Vec<Sample>,
    /// Flow control: next request index to jump to.
    pub next_request: Option<usize>,
    /// Names of all items in order (for setNextRequest by name).
    /// Flattened request names for setNextRequest name lookup. Shared as an
    /// `Arc` so a large collection's names are computed ONCE per scenario
    /// (in the engine) instead of re-cloned into every VU's PmState.
    pub request_names: Arc<Vec<String>>,
    /// Iteration data (from CSV/JSON data file), set per-iteration.
    pub iteration_data: Option<HashMap<String, Value>>,
    /// Whether to skip the remaining tests.
    pub skip_tests: bool,
    /// Group nesting stack — tracks active groups for group_duration metrics.
    /// Innermost group is at the top (last element).
    pub group_stack: Vec<String>,
    /// Current active group path (e.g. "outer::inner") for tagging metrics.
    pub current_group: Option<String>,
    // ── Execution context (k6 exec.* API) ──
    /// Unique VU identifier.
    pub vu_id: u32,
    /// Name of the currently running scenario.
    pub scenario_name: String,
    /// k6-style executor type name (e.g. "constant-vus") — set once per VU
    /// from the scenario's ExecutionConfig. Backs `exec.scenario.executor()`.
    pub executor_name: String,
    /// Shared handle to the scheduler's ACTIVE-VU counter, when one has been
    /// attached. Backs `exec.instance.vusActive`. Atomic so the sync bridge
    /// closure can read it without awaiting an async mutex.
    pub active_vus: Option<Arc<AtomicU32>>,
    /// Shared handle to the scheduler's GLOBAL total-iteration counter, when
    /// one has been attached. Backs `exec.instance.iterationsCompleted` — a
    /// total across ALL VUs, not just this one.
    pub global_iterations: Option<Arc<AtomicU64>>,
    /// Current iteration index (0-based) within this scenario.
    pub iteration_index: u64,
    /// Name of the currently executing request/item.
    pub current_request_name: String,
    // ── Test abort ──
    /// When true, the engine should abort the entire test run.
    /// Set by test.abort() from scripts.
    pub abort_requested: bool,
    /// Optional abort message set by test.abort(message).
    pub abort_message: Option<String>,
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
            custom_metrics: HashMap::new(),
            samples: Vec::new(),
            next_request: None,
            request_names: Arc::new(Vec::new()),
            iteration_data: None,
            skip_tests: false,
            group_stack: Vec::new(),
            current_group: None,
            vu_id: 0,
            scenario_name: String::new(),
            executor_name: String::new(),
            active_vus: None,
            global_iterations: None,
            iteration_index: 0,
            current_request_name: String::new(),
            abort_requested: false,
            abort_message: None,
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
            metric: "checks".into(),
            value: if passed { 1.0 } else { 0.0 },
            tags: Arc::new(TagMap::from_pairs([("check", name.to_string())])),
            timestamp: std::time::SystemTime::now(),
            sample_type: tropel_core::types::SampleType::Rate,
        });
    }

    /// Set the list of request names in order (for resolving setNextRequest by name).
    pub fn set_request_names(&mut self, names: Arc<Vec<String>>) {
        self.request_names = names;
    }

    /// Set the iteration data for the current iteration.
    pub fn set_iteration_data(&mut self, data: Option<HashMap<String, Value>>) {
        self.iteration_data = data;
    }

    /// Attach the shared execution-context handles from the scheduler.
    /// Called once per VU at startup — the executor name is immutable and the
    /// two atomic handles are shared with the scheduler's live counters, so
    /// later reads (from sync JS bridge closures) see up-to-date values.
    pub fn attach_exec_context(
        &mut self,
        executor_name: String,
        active_vus: Arc<AtomicU32>,
        global_iterations: Arc<AtomicU64>,
    ) {
        self.executor_name = executor_name;
        self.active_vus = Some(active_vus);
        self.global_iterations = Some(global_iterations);
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

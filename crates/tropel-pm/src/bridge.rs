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
    /// Backlog line 146: pm.execution.skipRequest() — skip the CURRENT item
    /// (no request send, no test script) and move to the next one.
    pub skip_request: bool,
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
    /// Tests marked skipped via pm.test.skip (backlog line 145) — not
    /// pass/fail, but tracked so reports can show them.
    pub skipped: u64,
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
            skip_request: false,
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
        self.record_test_tagged(name, passed, HashMap::new());
    }

    /// Record a test/check with optional extra tags (backlog line 149:
    /// k6's check() 3rd `tags` arg). The `check` tag always carries the raw
    /// check name — k6 does NOT prefix it with "check ".
    pub fn record_test_tagged(&mut self, name: &str, passed: bool, extra: HashMap<String, String>) {
        self.assertions.total += 1;
        if passed {
            self.assertions.passed += 1;
        } else {
            self.assertions.failed += 1;
        }

        let mut tags = TagMap::with_capacity(extra.len() + 1);
        tags.insert("check", name.to_string());
        for (k, v) in extra {
            tags.insert(k, v);
        }
        self.samples.push(Sample {
            metric: "checks".into(),
            value: if passed { 1.0 } else { 0.0 },
            tags: Arc::new(tags),
            timestamp: tropel_core::clock::monotonic_wall_now(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_test_updates_counters_and_emits_check_sample() {
        let mut st = PmState::new();
        st.record_test("status is 200", true);
        st.record_test("body has id", false);
        assert_eq!(st.assertions.total, 2);
        assert_eq!(st.assertions.passed, 1);
        assert_eq!(st.assertions.failed, 1);
        assert_eq!(st.assertions.skipped, 0);
        // Each test pushes a `checks` Rate sample tagged with the raw name.
        assert_eq!(st.samples.len(), 2);
        let s = &st.samples[0];
        assert_eq!(s.metric, "checks");
        assert_eq!(s.value, 1.0);
        assert_eq!(s.sample_type, tropel_core::types::SampleType::Rate);
        assert_eq!(s.tags.get("check"), Some("status is 200"));
        let s = &st.samples[1];
        assert_eq!(s.value, 0.0);
        assert_eq!(s.tags.get("check"), Some("body has id"));
    }

    #[test]
    fn record_test_tagged_carries_extra_tags() {
        let mut st = PmState::new();
        st.record_test_tagged(
            "check users",
            true,
            HashMap::from([("group".to_string(), "::users".to_string())]),
        );
        let s = &st.samples[0];
        assert_eq!(s.tags.get("check"), Some("check users"));
        assert_eq!(s.tags.get("group"), Some("::users"));
        assert_eq!(st.assertions.passed, 1);
    }

    #[test]
    fn set_request_names_and_iteration_data() {
        let mut st = PmState::new();
        st.set_request_names(Arc::new(vec!["a".into(), "b".into()]));
        assert_eq!(st.request_names.len(), 2);
        assert_eq!(st.request_names[0], "a");
        st.set_iteration_data(Some(HashMap::from([("id".into(), Value::from(7))])));
        assert_eq!(st.iteration_data.as_ref().unwrap()["id"], 7);
        st.set_iteration_data(None);
        assert!(st.iteration_data.is_none());
    }

    #[test]
    fn attach_exec_context_wires_shared_atomics() {
        let mut st = PmState::new();
        let active = Arc::new(AtomicU32::new(2));
        let total = Arc::new(AtomicU64::new(5));
        st.attach_exec_context("constant-vus".into(), active.clone(), total.clone());
        assert_eq!(st.executor_name, "constant-vus");
        // Reads go through the SAME Arc as the caller's — live updates visible.
        assert_eq!(st.active_vus.as_ref().unwrap().load(std::sync::atomic::Ordering::Relaxed), 2);
        active.store(9, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(st.active_vus.as_ref().unwrap().load(std::sync::atomic::Ordering::Relaxed), 9);
        total.store(42, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(st.global_iterations.as_ref().unwrap().load(std::sync::atomic::Ordering::Relaxed), 42);
    }
}

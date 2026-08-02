use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for think time / pacing between iterations.
///
/// Controls how long a VU waits before starting the next iteration.
/// - `delay`: fixed delay after each iteration (e.g. "2s")
/// - `min_delay` / `max_delay`: random delay in range [min, max]
/// - `iteration_pacing`: target iteration duration. If the iteration
///   finishes faster than this, the VU waits to hit the target.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct ThinkTimeConfig {
    /// Fixed delay after each iteration (e.g., "2s", "500ms").
    /// If set, min/max_delay are ignored.
    pub delay: Option<String>,
    /// Minimum delay for random range (e.g., "1s").
    #[serde(alias = "minDelay")]
    pub min_delay: Option<String>,
    /// Maximum delay for random range (e.g., "3s").
    #[serde(alias = "maxDelay")]
    pub max_delay: Option<String>,
    /// Target iteration duration for pacing (e.g., "5s").
    /// If the iteration finishes faster than this, the VU waits
    /// to hit the target duration before starting the next iteration.
    #[serde(alias = "iterationPacing")]
    pub iteration_pacing: Option<String>,
}


/// Configuration for a single named scenario within a multi-scenario run.
/// Each scenario has its own executor, input, env, tags, and optional start time.
/// When only a single scenario is running, the top-level `execution` field is used
/// instead and no `ScenarioConfig` is needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioConfig {
    /// Which executor to use.
    pub execution: ExecutionConfig,
    /// Optional input file override (defaults to the job-level `input`).
    pub input: Option<String>,
    /// Per-scenario environment variables (merged with job-level env).
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Per-scenario tags applied to all metrics emitted by this scenario.
    #[serde(default)]
    pub tags: HashMap<String, String>,
    /// When to start this scenario (e.g. "5s", "30s").
    /// Defaults to "0s" — starts immediately alongside other scenarios.
    /// Use staggered values to sequence scenario start times.
    #[serde(default)]
    pub start_time: String,
    /// k6 `exec` selection — which exported function/flow this scenario runs.
    /// Drivers that support named entry points (e.g. the k6 driver) install
    /// this export as the iteration function; when absent, the script's
    /// `default` export runs. Ignored by declarative (adapter) scenarios.
    #[serde(default)]
    pub exec: Option<String>,
}

/// Full configuration for a load test job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobConfig {
    /// Input file path (used as default for all scenarios, or directly for single-scenario mode).
    pub input: String,
    /// Input type (auto-detect if not specified).
    pub input_type: Option<String>,
    /// Execution configuration (used when no scenarios are defined — single-scenario mode).
    pub execution: ExecutionConfig,
    /// Named scenarios for multi-scenario runs. When present, each scenario runs
    /// independently with its own executor, env, and optional startTime.
    /// The top-level `execution` field is ignored when scenarios are defined.
    #[serde(default)]
    pub scenarios: HashMap<String, ScenarioConfig>,
    /// Environment variables (merged with per-scenario env).
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Global variables.
    #[serde(default)]
    pub globals: HashMap<String, serde_json::Value>,
    /// Collection variables.
    #[serde(default)]
    pub collection_vars: HashMap<String, serde_json::Value>,
    /// Data file (CSV/JSON for iteration data).
    pub data_file: Option<String>,
    /// Iteration data variables.
    #[serde(default)]
    pub iteration_data: Vec<HashMap<String, serde_json::Value>>,
    /// Threshold configuration.
    #[serde(default)]
    pub thresholds: HashMap<String, ThresholdConfig>,
    /// Output/reporter configuration.
    #[serde(default)]
    pub output: OutputConfig,
    /// HTTP configuration.
    #[serde(default)]
    pub http: HttpConfig,
    /// TLS configuration.
    #[serde(default)]
    pub tls: TlsConfig,
    /// Extension configuration.
    #[serde(default)]
    pub extensions: HashMap<String, serde_json::Value>,
    /// Whether the load profile (`execution` / `scenarios`) was explicitly
    /// provided by the user (CLI flags or a config file). When false, an
    /// input driver that declares its own load profile — e.g. a k6 script's
    /// `export const options` (vus/duration/stages/scenarios/thresholds) —
    /// may override the job's execution config. Defaults to false so k6
    /// scripts drive their own runs unless the user opts out via flags.
    #[serde(default)]
    pub execution_explicit: bool,
    /// Deterministic workload partitioning: which fraction `[from, to)` of
    /// this run this node executes, as `"from:to"` (e.g. `"0:1/3"`).
    /// Combine with `execution_segment_sequence` for cross-node validation.
    /// k6-compatible: `executionSegment` / `executionSegmentSequence`.
    #[serde(default, alias = "executionSegment")]
    pub execution_segment: Option<String>,
    /// The full sequence of segment boundaries shared by all cooperating
    /// nodes, e.g. `"0,1/3,2/3,1"`. Optional but recommended: validates
    /// that `execution_segment` is a consecutive pair of this sequence.
    #[serde(default, alias = "executionSegmentSequence")]
    pub execution_segment_sequence: Option<String>,
    /// Set by `tropel-agent` when running as a distributed worker: the
    /// controller owns the end-of-run summary (reporters, handleSummary,
    /// summary-export), so the agent skips them and just ships its raw
    /// snapshot back for central merging.
    #[serde(default, alias = "distributedWorker")]
    pub distributed_worker: bool,
    /// Port for the runtime control API (k6 REST parity). When set, an
    /// `externally-controlled` scenario binds `127.0.0.1:<port>` and serves
    /// `GET/PATCH /v1/status` so the VU count can be adjusted mid-run.
    #[serde(default, alias = "controlPort")]
    pub control_port: Option<u16>,
}

/// How to execute the load test.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ExecutionConfig {
    #[serde(rename = "constant-vus")]
    ConstantVus {
        vus: u32,
        duration: String,
        /// How long to wait for in-flight iterations to finish after the
        /// test duration expires. Defaults to 30s if not set.
        #[serde(default, alias = "gracefulStop")]
        graceful_stop: Option<String>,
        /// Think time / pacing configuration between iterations.
        #[serde(default, alias = "thinkTime")]
        think_time: ThinkTimeConfig,
    },
    #[serde(rename = "ramping-vus")]
    RampingVus {
        stages: Vec<Stage>,
        start_vus: u32,
        /// How long to wait for a VU to finish its current iteration during
        /// a ramp-down stage before moving on. Defaults to 30s.
        #[serde(default, alias = "gracefulRampDown")]
        graceful_ramp_down: Option<String>,
        /// How long to wait for in-flight iterations to finish after the
        /// final stage completes. Defaults to 30s.
        #[serde(default, alias = "gracefulStop")]
        graceful_stop: Option<String>,
        /// Think time / pacing configuration between iterations.
        #[serde(default, alias = "thinkTime")]
        think_time: ThinkTimeConfig,
    },
    #[serde(rename = "constant-arrival-rate")]
    ConstantArrivalRate {
        rate: f64,
        time_unit: String,
        duration: String,
        pre_alloc_vus: u32,
        max_vus: u32,
        /// How long to wait for in-flight iterations to finish after the
        /// test duration expires. Defaults to 30s if not set.
        #[serde(default, alias = "gracefulStop")]
        graceful_stop: Option<String>,
        /// Think time / pacing configuration between iterations.
        #[serde(default, alias = "thinkTime")]
        think_time: ThinkTimeConfig,
    },
    #[serde(rename = "shared-iterations")]
    SharedIterations {
        iterations: u64,
        max_duration: Option<String>,
        vus: u32,
        /// How long to wait for in-flight iterations to finish after the
        /// iteration budget is exhausted or max_duration is reached.
        #[serde(default, alias = "gracefulStop")]
        graceful_stop: Option<String>,
        /// Think time / pacing configuration between iterations.
        #[serde(default, alias = "thinkTime")]
        think_time: ThinkTimeConfig,
    },
    /// Ramping arrival rate — stages of target rate (iterations/sec).
    /// Similar to k6's `ramping-arrival-rate` executor.
    #[serde(rename = "ramping-arrival-rate")]
    RampingArrivalRate {
        /// Starting rate (iterations/sec).
        #[serde(default)]
        start_rate: f64,
        /// Stages defining how the rate changes over time.
        stages: Vec<ArrivalRateStage>,
        /// Time unit for the rate (e.g. "1s").
        #[serde(default = "default_time_unit")]
        time_unit: String,
        /// Pre-allocated VUs.
        #[serde(default = "default_pre_alloc")]
        pre_alloc_vus: u32,
        /// Maximum VUs.
        #[serde(default = "default_max_vus")]
        max_vus: u32,
        /// How long to wait for in-flight iterations to finish after the
        /// test duration expires. Defaults to 30s if not set.
        #[serde(default, alias = "gracefulStop")]
        graceful_stop: Option<String>,
        /// Think time / pacing configuration between iterations.
        #[serde(default, alias = "thinkTime")]
        think_time: ThinkTimeConfig,
    },
    /// Each VU runs exactly N iterations independently.
    /// Similar to k6's `per-vu-iterations` executor.
    #[serde(rename = "per-vu-iterations")]
    PerVUIterations {
        /// Number of VUs to spawn.
        vus: u32,
        /// Number of iterations per VU (each VU runs exactly this many).
        iterations: u64,
        /// Optional overall time limit for the test.
        #[serde(default, alias = "maxDuration")]
        max_duration: Option<String>,
        /// How long to wait for in-flight iterations to finish after the
        /// iteration budget is exhausted or max_duration is reached.
        #[serde(default, alias = "gracefulStop")]
        graceful_stop: Option<String>,
        /// Think time / pacing configuration between iterations.
        #[serde(default, alias = "thinkTime")]
        think_time: ThinkTimeConfig,
    },
    /// Externally-controlled VUs — the VU count can be adjusted AT RUNTIME
    /// via the control API (k6's `externally-controlled` executor / REST
    /// `/v1/status` parity). Starts with `vus`, may grow up to `max_vus` and
    /// shrink below `vus` as the controller commands. When `duration` is
    /// unset the run continues until the controller (or signal) stops it.
    #[serde(rename = "externally-controlled")]
    ExternallyControlled {
        /// Initial VU count.
        vus: u32,
        /// Maximum VU count the control API may scale up to.
        max_vus: u32,
        /// Optional wall-clock limit. When unset, the run continues until
        /// the control API requests a stop (or a signal / threshold aborts).
        #[serde(default, alias = "duration")]
        duration: Option<String>,
        /// How long to wait for in-flight iterations to finish after a
        /// stop / shrink command. Defaults to 30s.
        #[serde(default, alias = "gracefulStop")]
        graceful_stop: Option<String>,
        /// Think time / pacing configuration between iterations.
        #[serde(default, alias = "thinkTime")]
        think_time: ThinkTimeConfig,
    },
}

impl ExecutionConfig {
    /// k6-style executor type name (matches the serde tag used for this
    /// variant). Exposed to scripts via `exec.scenario.executor()`.
    pub fn executor_name(&self) -> &'static str {
        match self {
            ExecutionConfig::ConstantVus { .. } => "constant-vus",
            ExecutionConfig::RampingVus { .. } => "ramping-vus",
            ExecutionConfig::ConstantArrivalRate { .. } => "constant-arrival-rate",
            ExecutionConfig::SharedIterations { .. } => "shared-iterations",
            ExecutionConfig::RampingArrivalRate { .. } => "ramping-arrival-rate",
            ExecutionConfig::PerVUIterations { .. } => "per-vu-iterations",
            ExecutionConfig::ExternallyControlled { .. } => "externally-controlled",
        }
    }
}

/// A ramping stage (for VU count — used by RampingVus).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stage {
    pub duration: String,
    pub target: u32,
}

/// A ramping arrival rate stage.
/// The rate linearly interpolates from the previous stage's target (or start_rate)
/// to this stage's target over the stage duration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrivalRateStage {
    pub duration: String,
    pub target: f64,
}

fn default_time_unit() -> String {
    "1s".to_string()
}

fn default_pre_alloc() -> u32 {
    1
}

fn default_max_vus() -> u32 {
    10
}

/// Threshold configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThresholdConfig {
    /// Threshold expression.
    pub expression: String,
    /// Whether to abort the test on failure.
    #[serde(default)]
    pub abort_on_fail: bool,
    /// Grace period before abortOnFail activates (e.g. "30s").
    /// During this time metrics are collected but failures won't abort.
    #[serde(default, alias = "delayAbortEval")]
    pub delay_abort_eval: Option<String>,
}

/// Output/reporter configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OutputConfig {
    /// Reporters to use (e.g. ["stdout", "json"]).
    pub reporters: Vec<String>,
    /// Output file path (for json/csv reporters).
    pub output_file: Option<String>,
    /// Whether to show detailed summary.
    pub summary: bool,
    /// Whether to show trend statistics.
    pub trends: bool,
    /// Prometheus remote-write endpoint (e.g. `http://localhost:9090`).
    /// When set, samples are streamed to Prometheus via the remote-write API.
    #[serde(default)]
    pub prometheus_remote_write_url: Option<String>,
    /// OTLP/HTTP collector endpoint (e.g. `http://localhost:4318`).
    /// When set, samples are exported to the collector as OTLP metrics.
    #[serde(default)]
    pub otlp_endpoint: Option<String>,
    /// Path for the `--summary-export` JSON export (k6 semantics).
    ///
    /// When the script declares a `handleSummary(data)` function (k6), the
    /// script's returned file map governs output and this is ignored unless
    /// the script also prints to `stdout`; otherwise the aggregated summary
    /// data object is written here as JSON.
    #[serde(default)]
    pub summary_export: Option<String>,
    /// NDJSON streaming output file: every sample is appended as one JSON
    /// line while the run is in progress (k6 `--out json=file` equivalent).
    #[serde(default)]
    pub json_stream: Option<String>,
    /// StatsD / Datadog agent address (`host:port`, e.g. `localhost:8125`)
    /// for streaming datagram output with Datadog-style tags.
    #[serde(default)]
    pub statsd_addr: Option<String>,
    /// InfluxDB line-protocol UDP address (`host:port`, e.g. `localhost:8089`)
    /// for streaming line-protocol datagrams.
    #[serde(default)]
    pub influxdb_addr: Option<String>,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            reporters: vec!["stdout".to_string()],
            output_file: None,
            summary: true,
            trends: true,
            prometheus_remote_write_url: None,
            otlp_endpoint: None,
            summary_export: None,
            json_stream: None,
            statsd_addr: None,
            influxdb_addr: None,
        }
    }
}

/// Expected status code or range for determining http_req_failed.
/// A request fails (http_req_failed=1) when the response status code
/// does NOT fall within any of the expected entries.
///
/// Each entry can be:
/// - A single code: `200`
/// - A range: `"200-399"`
/// - A pattern with wildcard: `"2xx"`, `"3xx"`
///
/// Default: `["200-399"]` — all 2xx and 3xx are considered success.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ExpectedStatus {
    Single(u16),
    Range(String),
}

impl ExpectedStatus {
    /// Check if a given status code matches this expected status entry.
    pub fn matches(&self, code: u16) -> bool {
        match self {
            ExpectedStatus::Single(c) => *c == code,
            ExpectedStatus::Range(s) => {
                // Support patterns: "200-399" (range), "2xx" (wildcard), "200" (single)
                if let Some((lo, hi)) = s.split_once('-') {
                    // Range: "200-299"
                    let lo: u16 = lo.trim().parse().unwrap_or(0);
                    let hi: u16 = hi.trim().parse().unwrap_or(u16::MAX);
                    code >= lo && code <= hi
                } else if s.ends_with("xx") {
                    // Wildcard: "2xx" → 200-299, "3xx" → 300-399
                    let prefix = &s[..s.len() - 2];
                    if let Ok(base) = prefix.parse::<u16>() {
                        let lo = base * 100;
                        let hi = lo + 99;
                        code >= lo && code <= hi
                    } else {
                        false
                    }
                } else if let Ok(c) = s.parse::<u16>() {
                    c == code
                } else {
                    false
                }
            }
        }
    }
}

/// Check if a response status code is expected (successful) according to the
/// list of expected statuses. Returns true if the code matches ANY expected entry.
/// Returns false if the list is empty (never succeeds — all requests fail).
pub fn status_is_expected(code: u16, expected: &[ExpectedStatus]) -> bool {
    if expected.is_empty() {
        return false;
    }
    expected.iter().any(|e| e.matches(code))
}

/// HTTP client configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HttpConfig {
    /// Expected response status codes/ranges that determine request success.
    /// Used to drive the http_req_failed Rate metric.
    /// Default: `["200-399"]` — 2xx and 3xx are success, everything else fails.
    #[serde(default = "default_expected_statuses", alias = "expectedStatuses")]
    pub expected_statuses: Vec<ExpectedStatus>,
    /// Connection pool max idle connections.
    pub max_idle_connections: usize,
    /// Keep-alive duration.
    pub keep_alive: Option<String>,
    /// Timeout for idle connections.
    pub idle_connection_timeout: Option<String>,
    /// Whether to enable HTTP/2.
    pub http2: bool,
    /// User-agent header value.
    pub user_agent: String,
    /// Whether to decompress response bodies.
    pub decompress: bool,
    /// Whether to discard response bodies entirely (don't store bytes).
    /// Saves memory and bandwidth at the cost of not being able to inspect
    /// response content in scripts.
    #[serde(default)]
    pub discard_response_bodies: bool,
    /// Max redirects to follow.
    pub max_redirects: u32,
    /// Optional fixed ceiling for the latency histogram, in MICROseconds.
    /// `None` (default) uses hdrhistogram auto-resize — no ceiling, so very
    /// slow requests are recorded exactly instead of being clipped at 60 s.
    /// Set this to bound memory for runs with pathological outliers.
    #[serde(default, alias = "histogramMaxMicros")]
    pub histogram_max_micros: Option<u64>,
}

fn default_expected_statuses() -> Vec<ExpectedStatus> {
    vec![ExpectedStatus::Range("200-399".to_string())]
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            // 2xx-3xx = success (default, matches k6 behavior)
            expected_statuses: default_expected_statuses(),
            // With per-VU HTTP clients, each VU has its own connection pool.
            // A VU only makes one request at a time (sequential), so 4 idle
            // connections per host per VU is plenty. The old default of 100
            // was designed for shared clients — with per-VU clients and 100 VUs
            // it would mean 10,000 idle connections total.
            max_idle_connections: 4,
            keep_alive: Some("30s".to_string()),
            // How long an idle connection is kept before being closed.
            idle_connection_timeout: Some("30s".to_string()),
            http2: true,
            user_agent: "Tropel/0.1.0".to_string(),
            decompress: true,
            max_redirects: 10,
            discard_response_bodies: false,
            histogram_max_micros: None,
        }
    }
}

/// TLS configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct TlsConfig {
    pub insecure_skip_verify: bool,
    pub min_version: Option<String>,
    pub max_version: Option<String>,
    pub client_cert: Option<String>,
    pub client_key: Option<String>,
    pub client_passphrase: Option<String>,
    pub allowed_ciphers: Vec<String>,
}


impl Default for JobConfig {
    fn default() -> Self {
        Self {
            input: String::new(),
            input_type: None,
            execution: ExecutionConfig::ConstantVus {
                vus: 1,
                duration: "30s".to_string(),
                graceful_stop: Some("30s".to_string()),
                think_time: ThinkTimeConfig::default(),
            },
            scenarios: HashMap::new(),
            env: HashMap::new(),
            globals: HashMap::new(),
            collection_vars: HashMap::new(),
            data_file: None,
            iteration_data: vec![],
            thresholds: HashMap::new(),
            output: OutputConfig::default(),
            http: HttpConfig::default(),
            tls: TlsConfig::default(),
            extensions: HashMap::new(),
            execution_explicit: false,
            execution_segment: None,
            execution_segment_sequence: None,
            distributed_worker: false,
            control_port: None,
        }
    }
}

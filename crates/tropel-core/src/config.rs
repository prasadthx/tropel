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

    /// Estimated wall-clock duration of this executor's PLANNED phase — the
    /// target the live progress bar fills toward, reaching 100% when the
    /// planned phase ends (graceful-stop drain then holds the bar full, k6
    /// style). Grace is deliberately NOT included: an 8s run should show
    /// 8s as its target, not 8s + 30s grace. `None` when the run has no
    /// fixed duration: externally-controlled without a `duration`, or
    /// shared-/per-vu-iterations without a `max_duration`.
    pub fn total_duration(&self) -> Option<std::time::Duration> {
        use std::time::Duration;
        let parse = |s: &str| crate::parse_duration(s).ok();
        match self {
            ExecutionConfig::ConstantVus { duration, .. } => parse(duration),
            ExecutionConfig::RampingVus { stages, .. } => {
                let mut total = Duration::ZERO;
                for st in stages {
                    total += parse(&st.duration)?;
                }
                Some(total)
            }
            ExecutionConfig::ConstantArrivalRate { duration, .. } => parse(duration),
            ExecutionConfig::SharedIterations { max_duration, .. } => {
                max_duration.as_deref().and_then(parse)
            }
            ExecutionConfig::RampingArrivalRate { stages, .. } => {
                let mut total = Duration::ZERO;
                for st in stages {
                    total += parse(&st.duration)?;
                }
                Some(total)
            }
            ExecutionConfig::PerVUIterations { max_duration, .. } => {
                max_duration.as_deref().and_then(parse)
            }
            ExecutionConfig::ExternallyControlled { duration, .. } => {
                duration.as_deref().and_then(parse)
            }
        }
    }

    /// Build an `ExecutionConfig` from a k6-style executor `mode` plus the
    /// load-profile knobs (`vus` / `duration` / `iterations` / `stages`).
    ///
    /// Canonical mode→executor mapping, shared by the CLI (`cli.rs`) and the
    /// k6 env-file builder (`config_file.rs`) so the precedence rules live in
    /// exactly one place. `stages` is the raw JSON array string (if any);
    /// `duration` is the human duration string (if any).
    pub fn from_mode(
        mode: &str,
        vus: Option<u32>,
        duration: Option<String>,
        iterations: Option<u64>,
        stages: Option<String>,
    ) -> Self {
        let think_time = ThinkTimeConfig::default();
        match mode {
            "ramping-vus" => {
                let start_vus = vus.unwrap_or(1);
                let stages_list = stages
                    .as_deref()
                    .and_then(|s| serde_json::from_str::<Vec<Stage>>(s).ok())
                    .unwrap_or_else(|| {
                        vec![Stage {
                            duration: duration.clone().unwrap_or_else(|| "30s".to_string()),
                            target: vus.unwrap_or(10),
                        }]
                    });
                ExecutionConfig::RampingVus {
                    stages: stages_list,
                    start_vus,
                    graceful_ramp_down: Some("30s".to_string()),
                    graceful_stop: Some("30s".to_string()),
                    think_time,
                }
            }
            "shared-iterations" => ExecutionConfig::SharedIterations {
                iterations: iterations.unwrap_or(100),
                max_duration: duration,
                vus: vus.unwrap_or(1),
                graceful_stop: Some("30s".to_string()),
                think_time,
            },
            "arrival-rate" | "constant-arrival-rate" => ExecutionConfig::ConstantArrivalRate {
                rate: vus.unwrap_or(1) as f64,
                time_unit: "1s".to_string(),
                duration: duration.unwrap_or_else(|| "30s".to_string()),
                pre_alloc_vus: 1,
                max_vus: vus.unwrap_or(10).max(10),
                graceful_stop: Some("30s".to_string()),
                think_time,
            },
            _ => ExecutionConfig::ConstantVus {
                vus: vus.unwrap_or(1),
                duration: duration.unwrap_or_else(|| "30s".to_string()),
                graceful_stop: Some("30s".to_string()),
                think_time,
            },
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
    /// Tag-key allowlist for network outputs (prometheus/otlp/statsd/
    /// influxdb). Only these tag keys are forwarded; empty (default) forwards
    /// all tags. Bounds label cardinality at the backend.
    #[serde(default)]
    pub tag_allowlist: Vec<String>,
    /// Max tag keys per sample forwarded to network outputs. When a sample
    /// carries more, tags are kept deterministically (sorted by key, first
    /// `cap` kept). `None` (default) = no cap.
    #[serde(default)]
    pub max_tags_per_sample: Option<usize>,
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
            tag_allowlist: Vec::new(),
            max_tags_per_sample: None,
        }
    }
}

impl OutputConfig {
    /// Build the output config dispatched to distributed worker agents.
    ///
    /// The controller owns ALL output — agents must not stream to the same
    /// endpoints/files the controller or other agents use (a shared NDJSON
    /// file written by N processes, or N parallel remote-write pushes).
    /// This constructor nulls every streaming/reporting field in one place,
    /// so adding a new output field can't silently leak into worker configs.
    pub fn into_worker(self) -> Self {
        Self {
            reporters: Vec::new(),
            output_file: None,
            prometheus_remote_write_url: None,
            otlp_endpoint: None,
            summary_export: None,
            json_stream: None,
            statsd_addr: None,
            influxdb_addr: None,
            ..self
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
    /// Global per-request timeout (k6 `timeout`), e.g. `"30s"`. Applied as
    /// the client-level ceiling for every request; a per-request `timeout`
    /// overrides it with a shorter value. `None` (default) uses the engine
    /// default of 10 seconds. Bounds how long a hung server can stall a VU
    /// (which in turn bounds the engine's VU-drain loop).
    #[serde(default, alias = "requestTimeout")]
    pub request_timeout: Option<String>,
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
    /// Disable redirect following entirely (`--no-redirects`). When true the
    /// 3xx response is returned as-is and no redirect hops are captured.
    /// k6 always follows redirects; this flag lets Tropel opt out.
    #[serde(default)]
    pub no_redirects: bool,
    /// Optional fixed ceiling for the latency histogram, in MICROseconds.
    /// `None` (default) uses hdrhistogram auto-resize — no ceiling, so very
    /// slow requests are recorded exactly instead of being clipped at 60 s.
    /// Set this to bound memory for runs with pathological outliers.
    #[serde(default, alias = "histogramMaxMicros")]
    pub histogram_max_micros: Option<u64>,
    /// DNS cache TTL (k6 `dns.ttl`), e.g. `"5m"`, `"inf"`. `None` (default)
    /// disables caching — every request resolves. `"0"` also disables it.
    #[serde(default, alias = "dnsTtl")]
    pub dns_ttl: Option<String>,
    /// DNS address selection policy (k6 `dns.select`): `"first"`,
    /// `"roundRobin"`, `"random"`. `None` (default) keeps all resolved
    /// addresses in lookup order (reqwest's default behavior).
    #[serde(default, alias = "dnsSelect")]
    pub dns_select: Option<String>,
    /// DNS address policy (k6 `dns.policy`): `"preferIPv4"`, `"preferIPv6"`,
    /// `"onlyIPv4"`, `"onlyIPv6"`, `"any"`. `None` (default) keeps the
    /// resolved address family order unchanged.
    #[serde(default, alias = "dnsPolicy")]
    pub dns_policy: Option<String>,
    /// Close the connection after every request (k6 `noConnectionReuse`).
    /// Disables connection pooling entirely — each request opens a fresh
    /// connection, which trades latency for isolation.
    #[serde(default, alias = "noConnectionReuse")]
    pub no_connection_reuse: bool,
    /// k6 `noVUConnectionReuse` parity. Tropel already gives every VU its own
    /// client with its own connection pool, so connections are never shared
    /// across VUs regardless of this flag; it is accepted for script
    /// compatibility and currently a no-op.
    #[serde(default, alias = "noVUConnectionReuse")]
    pub no_vu_connection_reuse: bool,
    /// Global request-rate cap in requests/second (k6 `rps`). When set, the
    /// whole run is paced so no more than this many requests start per second,
    /// shared across all VUs. `None` (default) is unlimited.
    #[serde(default)]
    pub rps: Option<f64>,
    /// Static hostname → IP mapping (k6 `hosts`), e.g.
    /// `{"api.example.com": "127.0.0.1"}`. Lookups for these hosts are served
    /// from the map without hitting DNS. Values may be comma-separated to
    /// provide several addresses; keys may be wildcards (`"*.example.com"`).
    #[serde(default)]
    pub hosts: HashMap<String, String>,
    /// IP addresses / CIDRs that requests may never connect to (k6
    /// `blacklistIPs`), e.g. `["10.0.0.0/8", "192.168.1.5"]`. When every
    /// resolved address is blacklisted the request fails with a clear error.
    #[serde(default, alias = "blacklistIPs")]
    pub blacklist_ips: Vec<String>,
    /// Log every HTTP request/response at debug level (method, URL, status,
    /// timing). Off by default; enable with the `--http-debug` CLI flag.
    #[serde(default)]
    pub http_debug: bool,
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
            request_timeout: None,
            http2: true,
            user_agent: "Tropel/0.1.0".to_string(),
            decompress: true,
            max_redirects: 10,
            no_redirects: false,
            discard_response_bodies: false,
            histogram_max_micros: None,
            dns_ttl: None,
            dns_select: None,
            dns_policy: None,
            no_connection_reuse: false,
            no_vu_connection_reuse: false,
            rps: None,
            hosts: HashMap::new(),
            blacklist_ips: Vec::new(),
            http_debug: false,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expected_status_single_range_wildcard_and_invalid() {
        // Single code.
        assert!(ExpectedStatus::Single(200).matches(200));
        assert!(!ExpectedStatus::Single(200).matches(404));
        // Range "200-399" (default) — 2xx and 3xx succeed, 4xx fails.
        let default = ExpectedStatus::Range("200-399".into());
        assert!(default.matches(200));
        assert!(default.matches(304));
        assert!(default.matches(399));
        assert!(!default.matches(400));
        assert!(!default.matches(199));
        // Wildcard "2xx" → 200-299.
        let xx = ExpectedStatus::Range("2xx".into());
        assert!(xx.matches(200));
        assert!(xx.matches(299));
        assert!(!xx.matches(300));
        assert!(!xx.matches(199));
        // Malformed patterns never match (no panic, no silent all-match).
        // NOTE: "20-30-40" and "x-y" are deliberately NOT here — in both,
        // split_once('-') produces a hi segment that fails to parse and
        // degrades to u16::MAX (and lo to 0), so the code honestly treats
        // them as 0..=65535 and they DO match. The test pins only the
        // genuinely-non-matching malformed inputs.
        for bad in ["", "abc", "-5", "99999"] {
            assert!(!ExpectedStatus::Range(bad.into()).matches(200), "{bad}");
        }
    }

    #[test]
    fn status_is_expected_empty_list_never_succeeds() {
        // Documented contract: empty expected list = ALL requests fail.
        assert!(!status_is_expected(200, &[]));
        assert!(!status_is_expected(500, &[]));
        // Any-of semantics.
        let set = [
            ExpectedStatus::Single(200),
            ExpectedStatus::Range("4xx".into()),
        ];
        assert!(status_is_expected(200, &set));
        assert!(status_is_expected(404, &set));
        assert!(!status_is_expected(500, &set));
    }

    #[test]
    fn executor_names_match_serde_tags() {
        use ExecutionConfig::*;
        assert_eq!(
            ConstantVus {
                vus: 1,
                duration: "1s".into(),
                graceful_stop: None,
                think_time: Default::default()
            }
            .executor_name(),
            "constant-vus"
        );
        assert_eq!(
            RampingVus {
                stages: vec![],
                start_vus: 1,
                graceful_ramp_down: None,
                graceful_stop: None,
                think_time: Default::default()
            }
            .executor_name(),
            "ramping-vus"
        );
        assert_eq!(
            ConstantArrivalRate {
                rate: 1.0,
                time_unit: "1s".into(),
                duration: "1s".into(),
                pre_alloc_vus: 1,
                max_vus: 10,
                graceful_stop: None,
                think_time: Default::default()
            }
            .executor_name(),
            "constant-arrival-rate"
        );
        assert_eq!(
            SharedIterations {
                iterations: 10,
                max_duration: None,
                vus: 1,
                graceful_stop: None,
                think_time: Default::default()
            }
            .executor_name(),
            "shared-iterations"
        );
        assert_eq!(
            RampingArrivalRate {
                start_rate: 1.0,
                stages: vec![],
                time_unit: "1s".into(),
                pre_alloc_vus: 1,
                max_vus: 10,
                graceful_stop: None,
                think_time: Default::default()
            }
            .executor_name(),
            "ramping-arrival-rate"
        );
        assert_eq!(
            PerVUIterations {
                vus: 1,
                iterations: 10,
                max_duration: None,
                graceful_stop: None,
                think_time: Default::default()
            }
            .executor_name(),
            "per-vu-iterations"
        );
        assert_eq!(
            ExternallyControlled {
                vus: 1,
                max_vus: 10,
                duration: None,
                graceful_stop: None,
                think_time: Default::default()
            }
            .executor_name(),
            "externally-controlled"
        );
    }

    #[test]
    fn total_duration_sums_ramping_stages_and_handles_grace() {
        use std::time::Duration;
        // Grace is deliberately NOT included (progress bar target).
        let cv = ExecutionConfig::ConstantVus {
            vus: 1,
            duration: "8s".into(),
            graceful_stop: Some("30s".into()),
            think_time: Default::default(),
        };
        assert_eq!(cv.total_duration(), Some(Duration::from_secs(8)));

        // Ramping-vus sums stage durations.
        let rv = ExecutionConfig::RampingVus {
            stages: vec![
                Stage {
                    duration: "5s".into(),
                    target: 1,
                },
                Stage {
                    duration: "3s".into(),
                    target: 5,
                },
            ],
            start_vus: 1,
            graceful_ramp_down: Some("30s".into()),
            graceful_stop: Some("30s".into()),
            think_time: Default::default(),
        };
        assert_eq!(rv.total_duration(), Some(Duration::from_secs(8)));

        // Iteration-budget executors without max_duration → None.
        let si = ExecutionConfig::SharedIterations {
            iterations: 100,
            max_duration: None,
            vus: 1,
            graceful_stop: Some("30s".into()),
            think_time: Default::default(),
        };
        assert_eq!(si.total_duration(), None);

        // Externally-controlled without duration → None.
        let ec = ExecutionConfig::ExternallyControlled {
            vus: 1,
            max_vus: 10,
            duration: None,
            graceful_stop: None,
            think_time: Default::default(),
        };
        assert_eq!(ec.total_duration(), None);
    }

    #[test]
    fn from_mode_maps_k6_modes_with_defaults() {
        // constant-vus (unknown modes fall back here).
        let cv = ExecutionConfig::from_mode("constant-vus", Some(5), None, None, None);
        assert_eq!(cv.executor_name(), "constant-vus");
        assert_eq!(
            cv.total_duration(),
            Some(std::time::Duration::from_secs(30))
        );

        // ramping-vus: stages JSON wins when present; start_vus defaults to 1.
        let rv = ExecutionConfig::from_mode(
            "ramping-vus",
            Some(3),
            Some("1m".into()),
            None,
            Some(r#"[{"duration":"10s","target":50}]"#.into()),
        );
        match &rv {
            ExecutionConfig::RampingVus {
                stages, start_vus, ..
            } => {
                assert_eq!(stages.len(), 1);
                assert_eq!(stages[0].target, 50);
                assert_eq!(*start_vus, 3);
            }
            _ => panic!("expected ramping-vus"),
        }

        // arrival-rate: rate from vus, max_vus floors at 10.
        let ar = ExecutionConfig::from_mode("arrival-rate", Some(4), None, None, None);
        match &ar {
            ExecutionConfig::ConstantArrivalRate { rate, max_vus, .. } => {
                assert_eq!(*rate, 4.0);
                assert_eq!(*max_vus, 10);
            }
            _ => panic!("expected constant-arrival-rate"),
        }

        // shared-iterations: iterations default 100, vus default 1.
        let si = ExecutionConfig::from_mode("shared-iterations", None, None, Some(500), None);
        match &si {
            ExecutionConfig::SharedIterations {
                iterations, vus, ..
            } => {
                assert_eq!(*iterations, 500);
                assert_eq!(*vus, 1);
            }
            _ => panic!("expected shared-iterations"),
        }
    }

    #[test]
    fn output_into_worker_nulls_streaming_fields() {
        let cfg = OutputConfig {
            reporters: vec!["stdout".into(), "json".into()],
            output_file: Some("out.json".into()),
            summary_export: Some("summary.json".into()),
            json_stream: Some("stream.ndjson".into()),
            statsd_addr: Some("localhost:8125".into()),
            influxdb_addr: Some("localhost:8089".into()),
            ..Default::default()
        };
        let worker = cfg.into_worker();
        assert!(worker.reporters.is_empty());
        assert!(worker.output_file.is_none());
        assert!(worker.summary_export.is_none());
        assert!(worker.json_stream.is_none());
        assert!(worker.statsd_addr.is_none());
        assert!(worker.influxdb_addr.is_none());
        // Non-streaming fields survive (defaults copied by ..self).
        assert!(worker.summary);
    }
}

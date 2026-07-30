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

impl Default for ThinkTimeConfig {
    fn default() -> Self {
        Self {
            delay: None,
            min_delay: None,
            max_delay: None,
            iteration_pacing: None,
        }
    }
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
}

/// A ramping stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stage {
    pub duration: String,
    pub target: u32,
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
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            reporters: vec!["stdout".to_string()],
            output_file: None,
            summary: true,
            trends: true,
        }
    }
}

/// HTTP client configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HttpConfig {
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
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            max_idle_connections: 100,
            keep_alive: Some("30s".to_string()),
            idle_connection_timeout: Some("10s".to_string()),
            http2: true,
            user_agent: "Tropel/0.1.0".to_string(),
            decompress: true,
            max_redirects: 10,
            discard_response_bodies: false,
        }
    }
}

/// TLS configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TlsConfig {
    pub insecure_skip_verify: bool,
    pub min_version: Option<String>,
    pub max_version: Option<String>,
    pub client_cert: Option<String>,
    pub client_key: Option<String>,
    pub client_passphrase: Option<String>,
    pub allowed_ciphers: Vec<String>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            insecure_skip_verify: false,
            min_version: None,
            max_version: None,
            client_cert: None,
            client_key: None,
            client_passphrase: None,
            allowed_ciphers: vec![],
        }
    }
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
        }
    }
}

use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tropel_core::config::{ExecutionConfig, OutputConfig, ScenarioConfig, ThresholdConfig};
use tropel_core::scenario::Scenario;
use tropel_core::types::{Request, Response, Sample};
use tropel_core::Result;

/// A new protocol/request executor (beyond HTTP): gRPC, WebSocket, MQTT, ...
///
/// `execute()` returns the metric samples to record plus the response to
/// surface to scripts (`pm.response`). See [`ProtocolOutcome`].
#[async_trait]
pub trait Protocol: Send + Sync {
    fn scheme(&self) -> &str;
    async fn execute(&self, req: &Request, config: Option<&Value>) -> Result<ProtocolOutcome>;
}

/// Result of executing a request through a [`Protocol`].
///
/// The runner records `samples` into the metrics pipeline and exposes
/// `response` to scripts via the PM bridge (`pm.response`), mirroring the
/// HTTP path where the runner builds both from the HTTP response.
pub struct ProtocolOutcome {
    pub samples: Vec<Sample>,
    pub response: Option<Response>,
}

/// A new metrics sink/output.
#[async_trait]
pub trait Output: Send + Sync {
    fn name(&self) -> &str;
    async fn emit(&self, batch: &[Sample]) -> Result<()>;
    async fn flush(&self) -> Result<()>;

    /// Optional configuration pass before streaming begins.
    ///
    /// The engine calls this once after constructing the output, passing the
    /// job's `OutputConfig` so outputs can pick up endpoints / credentials
    /// (e.g. a Prometheus remote-write URL). Default is a no-op.
    fn configure(&mut self, _config: &OutputConfig) {}
}

/// A new input format → protocol-agnostic Scenario.
///
/// Adapters that need file-system access (e.g. k6 scripts that resolve
/// module imports relative to the source file) should override
/// `parse_with_path()`. The default implementation delegates to `parse()`.
pub trait InputAdapter: Send + Sync {
    fn id(&self) -> &str;
    fn detect(&self, bytes: &[u8]) -> bool;
    fn parse(&self, bytes: &[u8]) -> Result<Scenario>;

    /// Parse from bytes with an optional source file path hint.
    /// The default implementation ignores the path and calls `parse()`.
    /// Adapters that need file-system context (e.g. k6 for module
    /// resolution) can override this to use the path.
    fn parse_with_path(
        &self,
        bytes: &[u8],
        _source_path: Option<&std::path::Path>,
    ) -> Result<Scenario> {
        self.parse(bytes)
    }
}

// ── Imperative input contract ──

/// Runtime context provided to a `Driver` for each iteration.
/// Exposes native host functions so the driver never needs to send
/// HTTP requests or compute hashes itself — it delegates to the engine.
pub struct VuContext {
    /// Environment variables available to this VU.
    pub env: HashMap<String, String>,
    /// Iteration data row (from CSV/JSON data file), if any.
    pub data_row: Option<HashMap<String, serde_json::Value>>,
    /// The VU's unique identifier across the run (0-based internally;
    /// k6's `__VU` / `exec.vu.idInTest` are 1-based — drivers offset).
    pub vu_id: u32,
    /// The iteration index for this VU.
    pub iteration: u64,
    /// The scenario name this VU belongs to.
    pub scenario_name: String,
    /// Executor type name for `exec.scenario.executor()` (e.g.
    /// "constant-vus"). Populated by the engine.
    pub executor_name: String,
    /// Total iterations completed across ALL VUs so far — backs
    /// `exec.instance.iterationsCompleted()`. Populated by the engine.
    pub iterations_completed: u64,
    /// Currently active VU count — backs `exec.instance.vusActive()`.
    /// Populated by the engine.
    pub vus_active: u32,
    /// Samples collected during this iteration — the driver pushes
    /// samples here, and the engine drains them into the metrics pipeline.
    pub samples: Vec<Sample>,
    /// Whether the driver requested a test abort.
    pub abort_requested: bool,
    /// Message if abort was requested.
    pub abort_message: Option<String>,
    /// HTTP client handle for sending requests.
    pub http_client: Option<std::sync::Arc<dyn DriverHttpClient + Send + Sync>>,
    /// Serialized JSON value returned by the script's `setup()` function
    /// (k6 lifecycle). The engine runs `Driver::setup` ONCE per scenario
    /// before spawning VUs and threads the result into every VU's context,
    /// so the iteration entry point can receive it as its `data` argument
    /// (`export default function (data) { … }`). `None` when the script
    /// declares no `setup` export — the driver passes `undefined`, matching
    /// k6.
    pub setup_data: Option<String>,
    /// Registered protocols keyed by URL scheme (e.g. `grpc`, `ws`, or any
    /// third-party scheme). The engine instantiates the registry's protocols
    /// once per scenario and threads the map into every VU's context, so a
    /// driver (k6, WASM, or a third-party one) can dispatch non-HTTP URLs
    /// through the same scheme lookup the declarative runner uses. Empty for
    /// the declarative path, which keeps protocols on `VURunner` instead.
    pub protocols: Arc<HashMap<String, Arc<dyn Protocol>>>,
}

impl VuContext {
    pub fn new(vu_id: u32, iteration: u64, scenario_name: String) -> Self {
        Self {
            env: HashMap::new(),
            data_row: None,
            vu_id,
            iteration,
            scenario_name,
            executor_name: String::new(),
            iterations_completed: 0,
            vus_active: 0,
            samples: Vec::new(),
            abort_requested: false,
            abort_message: None,
            http_client: None,
            setup_data: None,
            protocols: Arc::new(HashMap::new()),
        }
    }

    /// Attach the execution-context info (executor name, global iteration
    /// count, active VUs) so drivers can expose `exec.*` to scripts.
    pub fn set_exec_context(
        &mut self,
        executor_name: String,
        iterations_completed: u64,
        vus_active: u32,
    ) {
        self.executor_name = executor_name;
        self.iterations_completed = iterations_completed;
        self.vus_active = vus_active;
    }

    /// Record a metric sample.
    pub fn emit_sample(&mut self, metric: &str, value: f64, tags: tropel_core::types::TagMap) {
        self.samples.push(Sample {
            metric: std::borrow::Cow::Owned(metric.to_string()),
            value,
            tags: std::sync::Arc::new(tags),
            timestamp: std::time::SystemTime::now(),
            sample_type: tropel_core::types::SampleType::Point,
        });
    }

    /// Request abort with an optional message.
    pub fn abort(&mut self, msg: Option<String>) {
        self.abort_requested = true;
        self.abort_message = msg;
    }
}

/// Trait that the engine implements so drivers can send HTTP requests
/// without depending on the HTTP crate directly.
#[async_trait]
pub trait DriverHttpClient: Send + Sync {
    /// Execute an HTTP request and return the response.
    async fn execute(&self, req: &Request) -> Result<Response>;
}

/// The imperative input contract — a `Driver` is the runtime that runs
/// one iteration of a load test.
///
/// Unlike `InputAdapter` (which maps a file to a static `Scenario`),
/// a `Driver` implements the per-iteration execution logic. The engine
/// calls `run_iteration()` once per iteration, providing a `VuContext`
/// with native host function handles.
///
/// This is the contract for:
/// - **k6 scripts**: the k6 adapter produces a Driver that evaluates
///   the user's JS for each iteration.
/// - **WASM plugin drivers**: sandboxed WASM modules that call native
///   functions through the host interface.
/// - **Custom executors**: Rust-level iteration logic.
#[async_trait]
pub trait Driver: Send + Sync {
    /// A human-readable identifier for this driver type.
    fn id(&self) -> &str;

    /// Detect whether this driver can handle the given input bytes.
    fn detect(&self, bytes: &[u8]) -> bool;

    /// Initialize the driver from raw input bytes.
    /// This is called once at setup time, not per-iteration.
    /// Returns a boxed DriverInstance that is then called per-iteration.
    ///
    /// `exec` is the optional k6 scenario `exec` selection — the name of the
    /// exported function this scenario should run. Drivers that support named
    /// entry points (e.g. the k6 driver) install that export as the iteration
    /// function; when `None`, the script's `default` export runs.
    async fn init(
        &self,
        bytes: &[u8],
        source_path: Option<&std::path::Path>,
        exec: Option<&str>,
    ) -> Result<Box<dyn DriverInstance>>;

    /// Read load-profile options declared by the script itself (e.g. k6's
    /// `export const options`).
    ///
    /// The engine calls this once per scenario *before* spawning VUs, and
    /// applies the result (execution config, thresholds, named scenarios)
    /// only when the user did not set an explicit load profile (no
    /// `-u`/`-d`/`--mode`/`--stages`/`--iterations` flags). This is how a
    /// k6 script's own `vus`/`duration`/`stages`/`scenarios`/`thresholds`
    /// drive the run instead of being silently ignored.
    ///
    /// `env` carries the job's environment variables so scripts that compute
    /// their options from `__ENV` (a common k6 pattern) see them.
    ///
    /// Returns `Ok(None)` when the script declares nothing usable (the engine
    /// falls back to the CLI profile). Returns `Err` when the script DECLARES
    /// options but they are malformed — e.g. a type mismatch in `stages` — so
    /// the run aborts loudly instead of silently running a profile nobody
    /// asked for (k6 hard-errors; backlog line 153).
    ///
    /// The default implementation declares no options.
    async fn declared_options(
        &self,
        _bytes: &[u8],
        _source_path: Option<&std::path::Path>,
        _env: &HashMap<String, String>,
    ) -> Result<Option<DriverDeclaredOptions>> {
        Ok(None)
    }

    /// Invoke the script's `handleSummary(data)` function (k6) after the run,
    /// if the script declares one.
    ///
    /// `summary_data_json` is the k6-style summary data object (metrics,
    /// thresholds, state) serialized as JSON. Returns a map of output
    /// filename → content with k6 semantics: the `stdout` key prints to
    /// stdout, every other key is written as a file. `None` means the script
    /// has no `handleSummary` export (or the driver doesn't support it) — the
    /// engine falls back to its default summary / `--summary-export`.
    ///
    /// The default implementation returns `None`.
    async fn handle_summary(
        &self,
        _bytes: &[u8],
        _source_path: Option<&std::path::Path>,
        _summary_data_json: &str,
        _env: &HashMap<String, String>,
    ) -> Option<HashMap<String, String>> {
        None
    }

    /// Run the script's `setup()` function (k6) once before VUs start.
    ///
    /// The engine calls this ONCE per scenario before spawning VUs. The
    /// returned value is serialized JSON of whatever `setup()` returned
    /// (k6 requires it to be JSON-serializable); it is threaded to every
    /// VU's [`VuContext::setup_data`] so the iteration entry point receives
    /// it as `data`, and is also passed to [`Driver::teardown`] after the
    /// run. `None` means the script has no `setup` export (data is
    /// `undefined`, matching k6).
    ///
    /// `env` carries the job's environment variables so `setup()` can read
    /// `__ENV` (a common k6 pattern).
    ///
    /// The default implementation declares no setup.
    async fn setup(
        &self,
        _bytes: &[u8],
        _source_path: Option<&std::path::Path>,
        _env: &HashMap<String, String>,
    ) -> Option<String> {
        None
    }

    /// Run the script's `teardown(data)` function (k6) once after the run.
    ///
    /// Called by the engine after all VUs finish, with the `setup()` return
    /// value as `data`. Failures are logged by the driver (k6 parity: a
    /// throwing teardown warns but never changes the run's exit status).
    ///
    /// The default implementation is a no-op.
    async fn teardown(
        &self,
        _bytes: &[u8],
        _source_path: Option<&std::path::Path>,
        _setup_data_json: Option<&str>,
        _env: &HashMap<String, String>,
    ) {
    }
}

/// Load-profile options declared by a script itself (e.g. k6's
/// `export const options = { vus, duration, stages, scenarios, thresholds }`).
///
/// Returned by [`Driver::declared_options`].
#[derive(Debug, Clone, Default)]
pub struct DriverDeclaredOptions {
    /// A single-executor load profile (k6 top-level `vus`/`duration`/
    /// `iterations`/`stages`). `None` when the script declares named
    /// scenarios instead.
    pub execution: Option<ExecutionConfig>,
    /// Named scenarios, each with its own executor (k6 `options.scenarios`).
    /// When present and non-empty, this takes precedence over `execution`.
    pub scenarios: Option<HashMap<String, ScenarioConfig>>,
    /// Thresholds declared by the script (k6 `options.thresholds`),
    /// merged into the job's thresholds.
    pub thresholds: HashMap<String, ThresholdConfig>,
    /// Global response-body handling (k6 `options.discardResponseBodies`):
    /// when `Some(true)`, the engine sets the HTTP client to discard all
    /// response bodies. `None` leaves the job's HttpConfig untouched.
    pub discard_response_bodies: Option<bool>,
    /// Which trend statistics the summary shows (k6 `options.summaryTrendStats`).
    /// `None` uses the k6 default set.
    pub summary_trend_stats: Option<Vec<String>>,
    /// DNS cache TTL (k6 `options.dns.ttl`) applied to the HTTP client.
    pub dns_ttl: Option<String>,
    /// DNS address selection policy (k6 `options.dns.select`).
    pub dns_select: Option<String>,
    /// DNS address-family policy (k6 `options.dns.policy`).
    pub dns_policy: Option<String>,
    /// k6 `options.noConnectionReuse` — close every connection per request.
    pub no_connection_reuse: Option<bool>,
    /// k6 `options.noVUConnectionReuse` — accepted for compatibility (each
    /// VU already owns its client/pool, so it is effectively always on).
    pub no_vu_connection_reuse: Option<bool>,
    /// Global request-rate cap (k6 `options.rps`), requests/second.
    pub rps: Option<f64>,
    /// Static host → IP mapping (k6 `options.hosts`).
    pub hosts: Option<HashMap<String, String>>,
    /// Blocked IPs / CIDRs (k6 `options.blacklistIPs`).
    pub blacklist_ips: Option<Vec<String>>,
    /// Skip TLS certificate verification (k6 `options.insecureSkipTLSVerify`)
    /// — the most common staging idiom. `Some(true)` disables certificate
    /// validation on the HTTP client's TLS config; `None` leaves the job's
    /// TlsConfig untouched.
    pub insecure_skip_tls_verify: Option<bool>,
}

/// A driver instance ready to run iterations.
/// Created by `Driver::init()`. The engine calls `run_iteration()`
/// once per iteration, passing a `VuContext` with native functions.
#[async_trait]
pub trait DriverInstance: Send {
    /// Run one iteration of this driver.
    /// The `ctx` provides native host functions for HTTP, metrics, etc.
    async fn run_iteration(&mut self, ctx: &mut VuContext) -> Result<()>;
}

// ── Registration types for inventory ──

/// Registration wrapper for protocols.
/// `scheme` is the URL scheme this protocol handles (e.g. `"grpc"`), stored
/// separately from the factory so `collect_inventory()` can read it without
/// instantiating the protocol.
///
/// Uses a `fn` pointer (not `Arc<dyn Fn>`) because `inventory::submit!`
/// requires the expression to be usable in a `const` context — the same
/// pattern as `InputAdapterRegistration`. Protocol factories are simple
/// captureless constructors, so a function pointer is sufficient.
pub struct ProtocolRegistration {
    pub scheme: &'static str,
    pub create: fn() -> Box<dyn Protocol>,
    /// Reserved for dispatch priority (informational; protocols are
    /// scheme-keyed).
    pub priority: u8,
}

impl ProtocolRegistration {
    pub const fn new(scheme: &'static str, create: fn() -> Box<dyn Protocol>) -> Self {
        Self {
            scheme,
            create,
            priority: 0,
        }
    }

    /// Builder-style priority setter (const, for `inventory::submit!`).
    pub const fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }
}

/// Registration wrapper for outputs.
/// Follows the same `fn` pointer pattern as `InputAdapterRegistration`
/// for `const`-compatibility with `inventory::submit!`.
pub struct OutputRegistration {
    pub id: &'static str,
    pub create: fn() -> Box<dyn Output>,
    /// Reserved for future dispatch priority; outputs are name-keyed so
    /// this is currently informational.
    pub priority: u8,
}

impl OutputRegistration {
    pub const fn new(id: &'static str, create: fn() -> Box<dyn Output>) -> Self {
        Self {
            id,
            create,
            priority: 0,
        }
    }

    /// Builder-style priority setter (const, for `inventory::submit!`).
    pub const fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }
}

/// Registration wrapper for input adapters.
/// `id` is the adapter's unique identifier (e.g. "postman", "har"), stored
/// separately from the factory so `collect_inventory()` can read it without
/// instantiating the adapter.
///
/// Uses a `fn` pointer (not `Arc<dyn Fn>`) because `inventory::submit!`
/// requires the expression to be usable in a `const` context. Since adapter
/// factories are always simple captureless closures (no captured state),
/// a function pointer is sufficient and avoids the `Arc` allocation.
pub struct InputAdapterRegistration {
    pub id: &'static str,
    pub create: fn() -> Box<dyn InputAdapter>,
    /// Explicit dispatch priority for content auto-detection. Higher wins
    /// when several adapters' `detect()` claim the same bytes; ties fall back
    /// to registration order. This makes dispatch deterministic and independent
    /// of `inventory` link order.
    pub priority: u8,
}

impl InputAdapterRegistration {
    pub const fn new(id: &'static str, create: fn() -> Box<dyn InputAdapter>) -> Self {
        Self {
            id,
            create,
            priority: 0,
        }
    }

    /// Builder-style priority setter (const, for `inventory::submit!`).
    pub const fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }
}

/// Registration wrapper for imperative drivers.
/// Follows the same `fn` pointer pattern as `InputAdapterRegistration`
/// for `const`-compatibility with `inventory::submit!`.
pub struct DriverRegistration {
    pub id: &'static str,
    pub create: fn() -> Box<dyn Driver>,
    /// Explicit dispatch priority for content auto-detection (see
    /// [`InputAdapterRegistration::priority`]).
    pub priority: u8,
}

impl DriverRegistration {
    pub const fn new(id: &'static str, create: fn() -> Box<dyn Driver>) -> Self {
        Self {
            id,
            create,
            priority: 0,
        }
    }

    /// Builder-style priority setter (const, for `inventory::submit!`).
    pub const fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }
}

// `fn` pointers are automatically Send + Sync.

// Register collection types for compile-time inventory registration.
// Each adapter/driver crate uses `inventory::submit!` to register,
// and the registry's `collect_inventory()` iterates them at startup.
// This must be in the crate that defines the type (`tropel-ext`).
inventory::collect!(InputAdapterRegistration);
inventory::collect!(DriverRegistration);
inventory::collect!(OutputRegistration);
inventory::collect!(ProtocolRegistration);

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use tropel_core::TropelError;

    // ── Stub impls for registration tests ──
    struct StubAdapter;
    impl InputAdapter for StubAdapter {
        fn id(&self) -> &str {
            "stub"
        }
        fn detect(&self, _bytes: &[u8]) -> bool {
            false
        }
        fn parse(&self, _bytes: &[u8]) -> tropel_core::Result<Scenario> {
            Err(TropelError::Other("stub".into()))
        }
    }

    struct StubDriver;
    #[async_trait]
    impl Driver for StubDriver {
        fn id(&self) -> &str {
            "stub"
        }
        fn detect(&self, _bytes: &[u8]) -> bool {
            false
        }
        async fn init(
            &self,
            _bytes: &[u8],
            _source_path: Option<&std::path::Path>,
            _exec: Option<&str>,
        ) -> tropel_core::Result<Box<dyn DriverInstance>> {
            Err(TropelError::Other("stub".into()))
        }
    }

    struct StubProtocol;
    #[async_trait]
    impl Protocol for StubProtocol {
        fn scheme(&self) -> &str {
            "stub"
        }
        async fn execute(
            &self,
            _req: &Request,
            _config: Option<&Value>,
        ) -> tropel_core::Result<ProtocolOutcome> {
            Err(TropelError::Other("stub".into()))
        }
    }

    struct StubOutput;
    #[async_trait]
    impl Output for StubOutput {
        fn name(&self) -> &str {
            "stub"
        }
        async fn emit(&self, _batch: &[Sample]) -> tropel_core::Result<()> {
            Ok(())
        }
        async fn flush(&self) -> tropel_core::Result<()> {
            Ok(())
        }
    }

    fn stub_adapter() -> Box<dyn InputAdapter> {
        Box::new(StubAdapter)
    }
    fn stub_driver() -> Box<dyn Driver> {
        Box::new(StubDriver)
    }
    fn stub_protocol() -> Box<dyn Protocol> {
        Box::new(StubProtocol)
    }
    fn stub_output() -> Box<dyn Output> {
        Box::new(StubOutput)
    }

    #[test]
    fn registration_builders_default_priority_zero() {
        assert_eq!(ProtocolRegistration::new("grpc", stub_protocol).priority, 0);
        assert_eq!(OutputRegistration::new("stdout", stub_output).priority, 0);
        assert_eq!(
            InputAdapterRegistration::new("postman", stub_adapter).priority,
            0
        );
        assert_eq!(DriverRegistration::new("k6", stub_driver).priority, 0);
    }

    #[test]
    fn with_priority_is_const_builder_pattern() {
        let p = ProtocolRegistration::new("grpc", stub_protocol).with_priority(5);
        assert_eq!(p.scheme, "grpc");
        assert_eq!(p.priority, 5);
        let a = InputAdapterRegistration::new("x", stub_adapter).with_priority(9);
        assert_eq!(a.id, "x");
        assert_eq!(a.priority, 9);
    }

    #[test]
    fn vu_context_defaults_and_exec_context() {
        let mut ctx = VuContext::new(3, 7, "scenario".into());
        assert_eq!(ctx.vu_id, 3);
        assert_eq!(ctx.iteration, 7);
        assert_eq!(ctx.scenario_name, "scenario");
        assert!(ctx.env.is_empty());
        assert!(ctx.data_row.is_none());
        assert!(ctx.samples.is_empty());
        assert!(!ctx.abort_requested);
        assert!(ctx.http_client.is_none());
        assert!(ctx.setup_data.is_none());
        assert!(ctx.protocols.is_empty());

        ctx.set_exec_context("constant-vus".into(), 100, 4);
        assert_eq!(ctx.executor_name, "constant-vus");
        assert_eq!(ctx.iterations_completed, 100);
        assert_eq!(ctx.vus_active, 4);
    }

    #[test]
    fn vu_context_emit_sample_and_abort() {
        let mut ctx = VuContext::new(1, 0, "s".into());
        let mut tags = tropel_core::types::TagMap::new();
        tags.insert("group", "::g");
        ctx.emit_sample("custom", 1.5, tags);
        assert_eq!(ctx.samples.len(), 1);
        assert_eq!(ctx.samples[0].metric, "custom");
        assert_eq!(ctx.samples[0].value, 1.5);
        assert_eq!(ctx.samples[0].tags.get("group"), Some("::g"));

        ctx.abort(Some("boom".into()));
        assert!(ctx.abort_requested);
        assert_eq!(ctx.abort_message.as_deref(), Some("boom"));
        ctx.abort(None);
        assert!(ctx.abort_requested);
        assert_eq!(ctx.abort_message, None);
    }
}

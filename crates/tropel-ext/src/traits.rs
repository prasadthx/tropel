use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tropel_core::scenario::Scenario;
use tropel_core::types::{Request, Response, Sample};
use tropel_core::Result;

/// A new protocol/request executor (beyond HTTP): gRPC, WebSocket, MQTT, ...
#[async_trait]
pub trait Protocol: Send + Sync {
    fn scheme(&self) -> &str;
    async fn execute(&self, req: &Request, config: Option<&Value>) -> Result<Sample>;
}

/// A new JS module callable from scripts, e.g. `import x from "tropel/x/grpc"`.
pub trait JsModule: Send + Sync {
    fn specifier(&self) -> &str;
    fn register(&self, ctx: &tropel_js::JsContext) -> Result<()>;
}

/// A new metrics sink/output.
#[async_trait]
pub trait Output: Send + Sync {
    fn name(&self) -> &str;
    async fn emit(&self, batch: &[Sample]) -> Result<()>;
    async fn flush(&self) -> Result<()>;
}

/// A new auth signer usable by protocols.
pub trait AuthSigner: Send + Sync {
    fn kind(&self) -> &str;
    fn sign(&self, req: &mut Request, cfg: &Value) -> Result<()>;
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
    fn parse_with_path(&self, bytes: &[u8], _source_path: Option<&std::path::Path>) -> Result<Scenario> {
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
    /// The VU's unique identifier across the run.
    pub vu_id: u32,
    /// The iteration index for this VU.
    pub iteration: u64,
    /// The scenario name this VU belongs to.
    pub scenario_name: String,
    /// Samples collected during this iteration — the driver pushes
    /// samples here, and the engine drains them into the metrics pipeline.
    pub samples: Vec<Sample>,
    /// Whether the driver requested a test abort.
    pub abort_requested: bool,
    /// Message if abort was requested.
    pub abort_message: Option<String>,
    /// HTTP client handle for sending requests.
    pub http_client: Option<std::sync::Arc<dyn DriverHttpClient + Send + Sync>>,
}

impl VuContext {
    pub fn new(vu_id: u32, iteration: u64, scenario_name: String) -> Self {
        Self {
            env: HashMap::new(),
            data_row: None,
            vu_id,
            iteration,
            scenario_name,
            samples: Vec::new(),
            abort_requested: false,
            abort_message: None,
            http_client: None,
        }
    }

    /// Record a metric sample.
    pub fn emit_sample(&mut self, metric: &str, value: f64, tags: tropel_core::types::TagMap) {
        self.samples.push(Sample {
            metric: metric.to_string(),
            value,
            tags,
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
    /// Returns a boxed Driver that is then called per-iteration.
    fn init(&self, bytes: &[u8], source_path: Option<&std::path::Path>) -> Result<Box<dyn DriverInstance>>;
}

/// A driver instance ready to run iterations.
/// Created by `Driver::init()`. The engine calls `run_iteration()`
/// once per iteration, passing a `VuContext` with native functions.
#[async_trait]
pub trait DriverInstance: Send + Sync {
    /// Run one iteration of this driver.
    /// The `ctx` provides native host functions for HTTP, metrics, etc.
    async fn run_iteration(&mut self, ctx: &mut VuContext) -> Result<()>;
}

// ── Registration types for inventory ──

/// Registration wrapper for protocols.
pub struct ProtocolRegistration {
    pub factory: Arc<dyn Fn() -> Box<dyn Protocol> + Send + Sync>,
}

impl ProtocolRegistration {
    pub fn new(factory: impl Fn() -> Box<dyn Protocol> + Send + Sync + 'static) -> Self {
        Self { factory: Arc::new(factory) }
    }
}

/// Registration wrapper for outputs.
pub struct OutputRegistration {
    pub factory: Arc<dyn Fn() -> Box<dyn Output> + Send + Sync>,
}

impl OutputRegistration {
    pub fn new(factory: impl Fn() -> Box<dyn Output> + Send + Sync + 'static) -> Self {
        Self { factory: Arc::new(factory) }
    }
}

/// Registration wrapper for JS modules.
pub struct JsModuleRegistration {
    pub factory: Arc<dyn Fn() -> Box<dyn JsModule> + Send + Sync>,
}

impl JsModuleRegistration {
    pub fn new(factory: impl Fn() -> Box<dyn JsModule> + Send + Sync + 'static) -> Self {
        Self { factory: Arc::new(factory) }
    }
}

/// Registration wrapper for auth signers.
pub struct AuthSignerRegistration {
    pub factory: Arc<dyn Fn() -> Box<dyn AuthSigner> + Send + Sync>,
}

impl AuthSignerRegistration {
    pub fn new(factory: impl Fn() -> Box<dyn AuthSigner> + Send + Sync + 'static) -> Self {
        Self { factory: Arc::new(factory) }
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
}

impl InputAdapterRegistration {
    pub const fn new(id: &'static str, create: fn() -> Box<dyn InputAdapter>) -> Self {
        Self { id, create }
    }
}

/// Registration wrapper for imperative drivers.
/// Follows the same `fn` pointer pattern as `InputAdapterRegistration`
/// for `const`-compatibility with `inventory::submit!`.
pub struct DriverRegistration {
    pub id: &'static str,
    pub create: fn() -> Box<dyn Driver>,
}

impl DriverRegistration {
    pub const fn new(id: &'static str, create: fn() -> Box<dyn Driver>) -> Self {
        Self { id, create }
    }
}

// `fn` pointers are automatically Send + Sync.

// Register collection types for compile-time inventory registration.
// Each adapter/driver crate uses `inventory::submit!` to register,
// and the registry's `collect_inventory()` iterates them at startup.
// This must be in the crate that defines the type (`tropel-ext`).
inventory::collect!(InputAdapterRegistration);
inventory::collect!(DriverRegistration);


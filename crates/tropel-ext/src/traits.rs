use async_trait::async_trait;
use serde_json::Value;
use tropel_core::scenario::Scenario;
use tropel_core::types::{Request, Sample};
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

// ── Registration types for inventory ──

use std::sync::Arc;

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

// `fn` pointers are automatically Send + Sync.

// Register collection types for compile-time inventory registration.
// Each adapter crate uses `inventory::submit!` to register its adapter,
// and the registry's `collect_inventory()` iterates them at startup.
// This must be in the crate that defines the type (`tropel-ext`).
inventory::collect!(InputAdapterRegistration);


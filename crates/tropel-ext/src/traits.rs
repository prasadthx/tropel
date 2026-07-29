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
pub trait InputAdapter: Send + Sync {
    fn id(&self) -> &str;
    fn detect(&self, bytes: &[u8]) -> bool;
    fn parse(&self, bytes: &[u8]) -> Result<Scenario>;
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
pub struct InputAdapterRegistration {
    pub factory: Arc<dyn Fn() -> Box<dyn InputAdapter> + Send + Sync>,
}

impl InputAdapterRegistration {
    pub fn new(factory: impl Fn() -> Box<dyn InputAdapter> + Send + Sync + 'static) -> Self {
        Self { factory: Arc::new(factory) }
    }
}

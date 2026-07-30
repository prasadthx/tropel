//! # Tropel SDK — stable public API for extensions
//!
//! `tropel-sdk` is the **single dependency** a third-party adapter/driver/output
//! author needs. It re-exports the types and traits that form the public contract
//! without exposing internal crate structure. Backing crates (`tropel-core`,
//! `tropel-ext`) can refactor internally as long as the SDK's surface is stable.
//!
//! ## Semver policy
//!
//! This crate follows strict semver. Breaking changes (trait method added or
//! removed, type renamed, field visibility changed) bump the major version.
//! Before 1.0.0, breaking changes bump the minor version (0.x → 0.y).
//!
//! ## Quick start — writing an input adapter
//!
//! ```rust,ignore
//! // Single import: `tropel-sdk` re-exports everything you need.
//! use tropel_sdk::{
//!     InputAdapter, InputAdapterRegistration, Scenario, ScenarioInfo,
//!     ScenarioItem, Request, Method, Result, inventory,
//! };
//!
//! /// Adapter for the (fictional) `.apig` format.
//! pub struct ApiGatewayAdapter;
//!
//! impl InputAdapter for ApiGatewayAdapter {
//!     fn id(&self) -> &str { "apig" }
//!
//!     fn detect(&self, bytes: &[u8]) -> bool {
//!         bytes.starts_with(b"APIG\n")
//!     }
//!
//!     fn parse(&self, bytes: &[u8]) -> Result<Scenario> {
//!         let _text = std::str::from_utf8(bytes)
//!             .map_err(|e| /* ... */)?;
//!         Ok(Scenario {
//!             info: ScenarioInfo {
//!                 name: "my-api".into(),
//!                 description: None,
//!                 schema: None,
//!             },
//!             items: vec![/* request items */],
//!             variables: Default::default(),
//!             auth: None,
//!         })
//!     }
//! }
//!
//! // Register for compile-time discovery by the engine.
//! // The `inventory` crate is re-exported through the SDK.
//! inventory::submit!(InputAdapterRegistration::new(
//!     "apig",
//!     || Box::new(ApiGatewayAdapter),
//! ));
//! ```
//!
//! ## Stability guarantees
//!
//! | Item | Status |
//! |------|--------|
//! | `Scenario`, `ScenarioInfo`, `ScenarioItem` | ✅ Stable — schema is the engine's core IR |
//! | `InputAdapter` trait | ✅ Stable — used by all declarative adapters |
//! | `InputAdapterRegistration` | ✅ Stable — const fn, `fn()` pointer |
//! | `Request`, `Response`, `Method`, `Body` | ✅ Stable — used in scenario items |
//! | `Result<T>` / `TropelError` | ✅ Stable — error handling |
//! | `Protocol` trait | 🚧 Available but not yet wired into engine dispatch |
//! | `Output` trait | 🚧 Available but not yet wired into engine dispatch |
//! | `WASM` / `WIT` interface | 🔜 Planned for Phase 4 |
//! | `VuProgram` / `Driver` trait | 🔜 Planned for future release |

// ═══════════════════════════════════════════════════════════════════
// Core types — the shared Scenario IR used by all adapters
// ═══════════════════════════════════════════════════════════════════

pub use tropel_core::scenario::{Scenario, ScenarioInfo, ScenarioItem};
pub use tropel_core::types::{AuthConfig, Body, Cookie, Method, Request, Response};

// ═══════════════════════════════════════════════════════════════════
// Extension traits
// ═══════════════════════════════════════════════════════════════════

// InputAdapter is the stable, primary extension trait — always available.
pub use tropel_ext::traits::{InputAdapter, InputAdapterRegistration};

// Protocol, Output, and AuthSigner traits are gated behind feature flags.
// They exist in the backing crate but aren't yet wired into engine dispatch.
// Enable with `tropel-sdk = { features = ["unstable-protocol"] }` etc.
// Breaking changes to these traits only require a minor/patch bump.

/// Unstable protocol extension trait (requires `unstable-protocol` feature).
#[cfg(feature = "unstable-protocol")]
pub use tropel_ext::traits::{Protocol, ProtocolRegistration};

/// Unstable output extension trait (requires `unstable-output` feature).
#[cfg(feature = "unstable-output")]
pub use tropel_ext::traits::{Output, OutputRegistration};

/// Unstable auth signer extension trait (requires `unstable-auth` feature).
#[cfg(feature = "unstable-auth")]
pub use tropel_ext::traits::{AuthSigner, AuthSignerRegistration};

// ═══════════════════════════════════════════════════════════════════
// Error types
// ═══════════════════════════════════════════════════════════════════

pub use tropel_core::{Result, TropelError};

// ═══════════════════════════════════════════════════════════════════
// Re-export inventory so adapter crates don't need their own dep
// ═══════════════════════════════════════════════════════════════════

pub use inventory;

// ═══════════════════════════════════════════════════════════════════
// Re-export async-trait so implementors can use #[async_trait]
// ═══════════════════════════════════════════════════════════════════

pub use async_trait::async_trait;

// ═══════════════════════════════════════════════════════════════════
// Registry access for programmatic use (advanced)
// ═══════════════════════════════════════════════════════════════════

pub use tropel_ext::registry::ExtensionRegistry;

// ═══════════════════════════════════════════════════════════════════
// Tag map type for metric tags
// ═══════════════════════════════════════════════════════════════════

pub use tropel_core::types::TagMap;

/// Version string for the SDK — adapter authors can check this at build time
/// to verify compatibility. Bumped whenever the public API changes.
pub const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");

// ═══════════════════════════════════════════════════════════════════
// Test helpers — verify re-exports compile and link correctly
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify all key types are constructible from SDK re-exports.
    #[test]
    fn test_scenario_construction() {
        let scenario = Scenario {
            info: ScenarioInfo {
                name: "test".into(),
                description: Some("SDK test".into()),
                schema: None,
            },
            items: vec![ScenarioItem {
                id: "item-1".into(),
                name: "GET /api".into(),
                request: Some(Request {
                    url: "https://example.com/api".into(),
                    method: Method::GET,
                    headers: Default::default(),
                    query_params: Default::default(),
                    body: None,
                    auth: None,
                    certificate: None,
                    follow_redirects: true,
                    timeout: None,
                }),
                prerequest: None,
                test: None,
                assertions: vec![],
                items: vec![],
            }],
            variables: Default::default(),
            auth: None,
        };
        assert_eq!(scenario.items.len(), 1);
        assert_eq!(scenario.info.name, "test");
    }

    /// Verify InputAdapterRegistration is constructible with const fn + fn pointer.
    #[test]
    fn test_registration_const_compatible() {
        fn factory() -> Box<dyn InputAdapter> {
            Box::new(DummyAdapter)
        }
        let reg = InputAdapterRegistration::new("test-adapter", factory);
        assert_eq!(reg.id, "test-adapter");
        let adapter = (reg.create)();
        assert_eq!(adapter.id(), "test-adapter");
    }

    struct DummyAdapter;
    impl InputAdapter for DummyAdapter {
        fn id(&self) -> &str { "test-adapter" }
        fn detect(&self, _bytes: &[u8]) -> bool { false }
        fn parse(&self, _bytes: &[u8]) -> std::result::Result<Scenario, TropelError> {
            Err(TropelError::Parse("not implemented".into()))
        }
    }

    /// Verify Result type alias works.
    #[test]
    fn test_result_alias() {
        fn returns_result() -> Result<()> {
            Ok(())
        }
        assert!(returns_result().is_ok());
    }

    /// Verify the inventory crate re-export is accessible.
    /// inventory::submit! is a compile-time macro — we just check the module resolves.
    #[test]
    fn test_inventory_re_export() {
        // Verify the inventory crate path works (submit! is compile-time only).
        #[allow(unused_imports)]
        use inventory as _;
    }
}

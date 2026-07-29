//! # tropel-native
//!
//! Native Rust implementations of heavy primitives, installed into the JS
//! context at bootstrap. These provide Rust execution for crypto, hashing,
//! encoding, JSON, and assertions that scripts use.

pub mod crypto;
pub mod hash;
pub mod encoding;
pub mod assert;
pub mod json;
pub mod r#fn;

use tropel_core::Result;
use tropel_js::JsContext;

/// A native module that can be installed into a JS context.
pub trait NativeModule {
    /// Namespace the module installs under (e.g. "__tropel_native").
    fn name(&self) -> &str;
    /// Install native functions into the JS context.
    fn install(&self, ctx: &JsContext) -> Result<()>;
}

/// Install all native builtins into a JS context.
pub async fn install_all(ctx: &JsContext) -> Result<()> {
    let modules: Vec<Box<dyn NativeModule>> = vec![
        Box::new(crypto::CryptoModule),
        Box::new(hash::HashModule),
        Box::new(encoding::EncodingModule),
        Box::new(assert::AssertModule),
        Box::new(json::JsonModule),
        Box::new(r#fn::ExtraFunctionsModule),
    ];

    for module in modules {
        module.install(ctx)?;
    }

    Ok(())
}

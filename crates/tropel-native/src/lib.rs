//! # tropel-native
//!
//! Native Rust implementations of heavy primitives, installed into the JS
//! context at bootstrap. These provide Rust execution for crypto, hashing,
//! encoding, JSON, and assertions that scripts use.

pub mod assert;
pub mod crypto;
pub mod encoding;
pub mod r#fn;
pub mod hash;
pub mod json;

use tropel_core::Result;
use tropel_js::JsContext;

/// A native module that can be installed into a JS context.
pub trait NativeModule {
    /// Namespace the module installs under (e.g. "__tropel_native").
    fn name(&self) -> &str;
    /// Install native functions into the JS context.
    fn install(&self, ctx: &mut JsContext) -> Result<()>;
}

/// Install all native builtins into a JS context.
pub async fn install_all(ctx: &mut JsContext) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Verify that every expected native bridge function is registered as a JS
    /// global after `install_all()`. This is the registration convention test:
    /// any new module must add its expected globals to this list.
    ///
    /// If this test fails, either:
    /// 1. A new native function was added but not registered in `install()`
    /// 2. A function was renamed — update this list to match
    #[tokio::test]
    async fn test_all_native_functions_are_registered() {
        let mut ctx = tropel_js::JsContext::new(Some(1024 * 1024), Some(Duration::from_secs(5)))
            .await
            .unwrap();
        install_all(&mut ctx).await.unwrap();

        // Every expected global. When adding a new native module or function,
        // add its globals here so the registration convention is enforced.
        let expected_globals: &[&str] = &[
            // ── Crypto (crypto.rs) ──
            "__tropel_native_md5",
            "__tropel_native_sha1",
            "__tropel_native_sha256",
            "__tropel_native_sha384",
            "__tropel_native_sha512",
            "__tropel_native_sha3_256",
            "__tropel_native_ripemd160",
            "__tropel_native_hmac_md5",
            "__tropel_native_hmac_sha1",
            "__tropel_native_hmac_sha256",
            "__tropel_native_hmac_sha512",
            "__tropel_native_aes_gcm_encrypt",
            "__tropel_native_aes_gcm_decrypt",
            "__tropel_native_aes_cbc_encrypt",
            "__tropel_native_aes_cbc_decrypt",
            "__tropel_native_random_bytes",
            "__tropel_native_evp_bytes_to_key",
            // ── Hash (hash.rs) ──
            "__tropel_native_hash_uuid",
            // ── Encoding (encoding.rs) ──
            "__tropel_native_base64_encode",
            "__tropel_native_base64_decode",
            "__tropel_native_base64url_encode",
            "__tropel_native_base64url_decode",
            "__tropel_native_hex_encode",
            "__tropel_native_hex_decode",
            "__tropel_native_url_encode",
            "__tropel_native_url_decode",
            // ── Assert (assert.rs) ──
            "__tropel_native_deep_equal",
            "__tropel_native_is_string",
            "__tropel_native_is_number",
            "__tropel_native_is_array",
            "__tropel_native_is_object",
            "__tropel_native_is_null",
            "__tropel_native_is_boolean",
            "__tropel_native_length",
            "__tropel_native_matches",
            // ── JSON (json.rs) ──
            "__tropel_native_json_parse",
            "__tropel_native_json_stringify",
            "__tropel_native_json_get",
            // ── Extra functions (fn.rs) ──
            "__tropel_native_random_int",
            "__tropel_native_random_float",
        ];

        for &name in expected_globals {
            let exists = ctx.with_ctx(|rq_ctx| {
                let globals = rq_ctx.globals();
                // Check if the global exists and is a function
                match globals.get::<_, rquickjs::Value>(name) {
                    Ok(val) => val.is_function(),
                    Err(_) => false,
                }
            });
            assert!(
                exists,
                "Native function '{}' is NOT registered as a JS global. \
                 Either add it to the appropriate NativeModule::install() \
                 or update this test if it was intentionally removed.",
                name
            );
        }

        // Also verify that the total count of registered function globals matches
        // (catches extra unintended registrations)
        let native_count = ctx.with_ctx(|rq_ctx| {
            let globals = rq_ctx.globals();
            let mut count: u32 = 0;
            // Use has() to check specific known globals instead of Object.keys
            // which has complex type issues in rquickjs.
            for &name in expected_globals {
                if let Ok(val) = globals.get::<_, rquickjs::Value>(name) {
                    if val.is_function() {
                        count += 1;
                    }
                }
            }
            count
        });

        assert_eq!(
            native_count as usize,
            expected_globals.len(),
            "Expected {} __tropel_native_* globals but only found {}. \
             Some expected functions are missing.",
            expected_globals.len(),
            native_count
        );
    }
}

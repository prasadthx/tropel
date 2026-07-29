use crate::NativeModule;
use rquickjs::function::Func;
use tropel_core::Result;
use tropel_js::JsContext;

pub struct EncodingModule;

impl NativeModule for EncodingModule {
    fn name(&self) -> &str {
        "__tropel_native_encoding"
    }

    fn install(&self, ctx: &JsContext) -> Result<()> {
        ctx.with_ctx(|rq_ctx| {
            let globals = rq_ctx.globals();

            let _ = globals.set("__tropel_native_base64_encode", Func::from(|data: Vec<u8>| -> String {
                base64_encode(&data)
            }));

            let _ = globals.set("__tropel_native_hex_encode", Func::from(|data: Vec<u8>| -> String {
                hex_encode(&data)
            }));

            let _ = globals.set("__tropel_native_url_encode", Func::from(|data: String| -> String {
                url_encode(&data)
            }));
        });

        tracing::debug!("Installed encoding native module");
        Ok(())
    }
}

/// Base64 encode.
pub fn base64_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

/// Base64 decode.
pub fn base64_decode(data: &str) -> Result<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|e| tropel_core::TropelError::Parse(format!("Base64 decode error: {}", e)))
}

/// Base64 URL-safe encode.
pub fn base64url_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
}

/// Hex encode.
pub fn hex_encode(data: &[u8]) -> String {
    hex::encode(data)
}

/// Hex decode.
pub fn hex_decode(data: &str) -> Result<Vec<u8>> {
    hex::decode(data)
        .map_err(|e| tropel_core::TropelError::Parse(format!("Hex decode error: {}", e)))
}

/// URL encode a string.
pub fn url_encode(data: &str) -> String {
    percent_encoding::utf8_percent_encode(data, percent_encoding::NON_ALPHANUMERIC).to_string()
}

/// URL decode a string.
pub fn url_decode(data: &str) -> Result<String> {
    percent_encoding::percent_decode_str(data)
        .decode_utf8()
        .map(|c| c.to_string())
        .map_err(|e| tropel_core::TropelError::Parse(format!("URL decode error: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base64_roundtrip() {
        let data = b"hello world";
        let encoded = base64_encode(data);
        let decoded = base64_decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_hex_roundtrip() {
        let data = b"hello";
        let encoded = hex_encode(data);
        let decoded = hex_decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_url_encode() {
        let result = url_encode("hello world");
        assert_eq!(result, "hello%20world");
    }

    #[test]
    fn test_base64url() {
        let data = b"hello\xffworld";
        let encoded = base64url_encode(data);
        assert!(!encoded.contains('+')); // no + chars in URL-safe
        assert!(!encoded.contains('/')); // no / chars in URL-safe
    }
}

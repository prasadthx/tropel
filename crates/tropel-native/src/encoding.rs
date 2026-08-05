use crate::NativeModule;
use rquickjs::function::Func;
use tropel_core::Result;
use tropel_js::JsContext;

pub struct EncodingModule;

impl NativeModule for EncodingModule {
    fn name(&self) -> &str {
        "__tropel_native_encoding"
    }

    fn install(&self, ctx: &mut JsContext) -> Result<()> {
        ctx.with_ctx(|rq_ctx| {
            let globals = rq_ctx.globals();

            // ── Base64 encode/decode ──
            let _ = globals.set(
                "__tropel_native_base64_encode",
                Func::from(|data: Vec<u8>| -> String { base64_encode(&data) }),
            );
            let _ = globals.set(
                "__tropel_native_base64_decode",
                Func::from(|data: String| -> Option<Vec<u8>> { base64_decode(&data).ok() }),
            );

            // ── Base64 URL-safe encode/decode ──
            let _ = globals.set(
                "__tropel_native_base64url_encode",
                Func::from(|data: Vec<u8>| -> String { base64url_encode(&data) }),
            );
            let _ = globals.set(
                "__tropel_native_base64url_decode",
                Func::from(|data: String| -> Option<Vec<u8>> { base64url_decode(&data).ok() }),
            );

            // ── Hex encode/decode ──
            let _ = globals.set(
                "__tropel_native_hex_encode",
                Func::from(|data: Vec<u8>| -> String { hex_encode(&data) }),
            );
            let _ = globals.set(
                "__tropel_native_hex_decode",
                Func::from(|data: String| -> Option<Vec<u8>> { hex_decode(&data).ok() }),
            );

            // ── URL encode/decode ──
            let _ = globals.set(
                "__tropel_native_url_encode",
                Func::from(|data: String| -> String { url_encode(&data) }),
            );
            let _ = globals.set(
                "__tropel_native_url_decode",
                Func::from(|data: String| -> Option<String> { url_decode(&data).ok() }),
            );
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

/// Base64 URL-safe decode.
pub fn base64url_decode(data: &str) -> Result<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(data)
        .map_err(|e| tropel_core::TropelError::Parse(format!("Base64 URL decode error: {}", e)))
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
        let decoded = base64url_decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_base64_decode() {
        let data = b"hello world";
        let encoded = base64_encode(data);
        assert_eq!(encoded, "aGVsbG8gd29ybGQ=");
        let decoded = base64_decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_hex_decode() {
        let data = b"hello";
        let encoded = hex_encode(data);
        assert_eq!(encoded, "68656c6c6f");
        let decoded = hex_decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_url_decode() {
        let encoded = "hello%20world%21";
        let decoded = url_decode(encoded).unwrap();
        assert_eq!(decoded, "hello world!");
    }

    #[test]
    fn test_base64url_roundtrip() {
        let data = b"\x00\x01\x02\xff\xfe";
        let encoded = base64url_encode(data);
        assert!(!encoded.contains('+'));
        assert!(!encoded.contains('=')); // no padding in URL-safe
        let decoded = base64url_decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_invalid_base64_decode() {
        assert!(base64_decode("!!!invalid!!!").is_err());
    }

    #[test]
    fn test_invalid_hex_decode() {
        assert!(hex_decode("xyz").is_err());
    }

    #[test]
    fn test_valid_url_decode() {
        let result = url_decode("hello%20world").unwrap();
        assert_eq!(result, "hello world");
    }

    #[test]
    fn test_url_decode_no_encoding() {
        let result = url_decode("hello world").unwrap();
        assert_eq!(result, "hello world");
    }
}

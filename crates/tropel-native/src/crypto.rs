use crate::NativeModule;
use rquickjs::function::Func;
use tropel_core::Result;
use tropel_js::JsContext;

pub struct CryptoModule;

impl NativeModule for CryptoModule {
    fn name(&self) -> &str {
        "__tropel_native_crypto"
    }

    fn install(&self, ctx: &JsContext) -> Result<()> {
        ctx.with_ctx(|rq_ctx| {
            let globals = rq_ctx.globals();

            let _ = globals.set("__tropel_native_sha256", Func::from(|data: Vec<u8>| -> Vec<u8> {
                sha256(&data)
            }));

            let _ = globals.set("__tropel_native_sha1", Func::from(|data: Vec<u8>| -> Vec<u8> {
                sha1(&data)
            }));

            let _ = globals.set("__tropel_native_md5", Func::from(|data: Vec<u8>| -> Vec<u8> {
                md5(&data)
            }));

            let _ = globals.set("__tropel_native_hmac_sha256", Func::from(|key: Vec<u8>, data: Vec<u8>| -> Vec<u8> {
                hmac_sha256(&key, &data)
            }));
        });

        tracing::debug!("Installed crypto native module");
        Ok(())
    }
}

/// Compute SHA-256 hash.
pub fn sha256(data: &[u8]) -> Vec<u8> {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

/// Compute SHA-384 hash.
pub fn sha384(data: &[u8]) -> Vec<u8> {
    use sha2::Digest;
    let mut hasher = sha2::Sha384::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

/// Compute SHA-512 hash.
pub fn sha512(data: &[u8]) -> Vec<u8> {
    use sha2::Digest;
    let mut hasher = sha2::Sha512::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

/// Compute SHA-1 hash.
pub fn sha1(data: &[u8]) -> Vec<u8> {
    use sha1::Digest;
    let mut hasher = sha1::Sha1::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

/// Compute MD5 hash.
pub fn md5(data: &[u8]) -> Vec<u8> {
    use md5::Md5;
    use sha2::Digest;
    let mut hasher = Md5::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

/// Compute SHA3-256 hash.
pub fn sha3_256(data: &[u8]) -> Vec<u8> {
    use sha3::Digest;
    let mut hasher = sha3::Sha3_256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

/// Compute HMAC-SHA256.
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::Mac;
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(key)
        .expect("HMAC key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Compute HMAC-SHA1.
pub fn hmac_sha1(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::Mac;
    let mut mac = hmac::Hmac::<sha1::Sha1>::new_from_slice(key)
        .expect("HMAC key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha256() {
        let result = sha256(b"hello");
        assert_eq!(result.len(), 32);
        let hex = hex::encode(result);
        assert_eq!(
            hex,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn test_md5() {
        let result = md5(b"hello");
        let hex = hex::encode(result);
        assert_eq!(hex, "5d41402abc4b2a76b9719d911017c592");
    }

    #[test]
    fn test_hmac_sha256() {
        let result = hmac_sha256(b"key", b"The quick brown fox jumps over the lazy dog");
        assert_eq!(result.len(), 32);
    }
}

use crate::NativeModule;
use aes::cipher::{BlockDecryptMut, BlockEncryptMut, KeyIvInit};
use aes_gcm::aead::{Aead, KeyInit as _};
use cbc::cipher::block_padding::Pkcs7;
use rquickjs::function::Func;
use tropel_core::Result;
use tropel_js::JsContext;

type Aes256CbcEnc = cbc::Encryptor<aes::Aes256>;
type Aes256CbcDec = cbc::Decryptor<aes::Aes256>;

pub struct CryptoModule;

impl NativeModule for CryptoModule {
    fn name(&self) -> &str {
        "__tropel_native_crypto"
    }

    fn install(&self, ctx: &JsContext) -> Result<()> {
        ctx.with_ctx(|rq_ctx| {
            let globals = rq_ctx.globals();

            // ── Hashes ──
            let _ = globals.set("__tropel_native_sha256", Func::from(|data: Vec<u8>| -> Vec<u8> {
                sha256(&data)
            }));

            let _ = globals.set("__tropel_native_sha384", Func::from(|data: Vec<u8>| -> Vec<u8> {
                sha384(&data)
            }));

            let _ = globals.set("__tropel_native_sha512", Func::from(|data: Vec<u8>| -> Vec<u8> {
                sha512(&data)
            }));

            let _ = globals.set("__tropel_native_sha1", Func::from(|data: Vec<u8>| -> Vec<u8> {
                sha1(&data)
            }));

            let _ = globals.set("__tropel_native_md5", Func::from(|data: Vec<u8>| -> Vec<u8> {
                md5(&data)
            }));

            let _ = globals.set("__tropel_native_sha3_256", Func::from(|data: Vec<u8>| -> Vec<u8> {
                sha3_256(&data)
            }));

            // ── HMACs ──
            let _ = globals.set("__tropel_native_hmac_sha256", Func::from(|key: Vec<u8>, data: Vec<u8>| -> Vec<u8> {
                hmac_sha256(&key, &data)
            }));

            let _ = globals.set("__tropel_native_hmac_sha1", Func::from(|key: Vec<u8>, data: Vec<u8>| -> Vec<u8> {
                hmac_sha1(&key, &data)
            }));

            // ── AES-256-GCM (authenticated encryption) ──
            let _ = globals.set(
                "__tropel_native_aes_gcm_encrypt",
                Func::from(|key: Vec<u8>, nonce: Vec<u8>, plaintext: Vec<u8>| -> Vec<u8> {
                    aes_gcm_encrypt(&key, &nonce, &plaintext)
                        .expect("AES-GCM encrypt failed")
                }),
            );

            let _ = globals.set(
                "__tropel_native_aes_gcm_decrypt",
                Func::from(|key: Vec<u8>, nonce: Vec<u8>, ciphertext: Vec<u8>| -> Vec<u8> {
                    aes_gcm_decrypt(&key, &nonce, &ciphertext)
                        .expect("AES-GCM decrypt failed")
                }),
            );

            // ── AES-256-CBC (PKCS7 padding) ──
            let _ = globals.set(
                "__tropel_native_aes_cbc_encrypt",
                Func::from(|key: Vec<u8>, iv: Vec<u8>, plaintext: Vec<u8>| -> Vec<u8> {
                    aes_cbc_encrypt(&key, &iv, &plaintext)
                        .expect("AES-CBC encrypt failed")
                }),
            );

            let _ = globals.set(
                "__tropel_native_aes_cbc_decrypt",
                Func::from(|key: Vec<u8>, iv: Vec<u8>, ciphertext: Vec<u8>| -> Vec<u8> {
                    aes_cbc_decrypt(&key, &iv, &ciphertext)
                        .expect("AES-CBC decrypt failed")
                }),
            );
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
    let mut mac = <hmac::Hmac::<sha2::Sha256> as Mac>::new_from_slice(key)
        .expect("HMAC key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Compute HMAC-SHA1.
pub fn hmac_sha1(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::Mac;
    let mut mac = <hmac::Hmac::<sha1::Sha1> as Mac>::new_from_slice(key)
        .expect("HMAC key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// AES-256-GCM encrypt.
/// `key` must be 32 bytes, `nonce` must be 12 bytes.
/// Returns ciphertext with 16-byte GCM authentication tag appended.
pub fn aes_gcm_encrypt(key: &[u8], nonce: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
    use aes_gcm::{Aes256Gcm, Nonce};

    if key.len() != 32 {
        return Err(tropel_core::TropelError::Crypto(
            "AES-GCM key must be 32 bytes".into(),
        ));
    }
    if nonce.len() != 12 {
        return Err(tropel_core::TropelError::Crypto(
            "AES-GCM nonce must be 12 bytes".into(),
        ));
    }

    let key_arr = aes_gcm::Key::<Aes256Gcm>::from_slice(key);
    let cipher = Aes256Gcm::new(key_arr);
    let nonce_arr = Nonce::from_slice(nonce);

    cipher
        .encrypt(nonce_arr, plaintext)
        .map_err(|e| tropel_core::TropelError::Crypto(format!("AES-GCM encrypt failed: {}", e)))
}

/// AES-256-GCM decrypt.
/// `ciphertext` must include the 16-byte GCM authentication tag at the end.
pub fn aes_gcm_decrypt(key: &[u8], nonce: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>> {
    use aes_gcm::{Aes256Gcm, Nonce};

    if key.len() != 32 {
        return Err(tropel_core::TropelError::Crypto(
            "AES-GCM key must be 32 bytes".into(),
        ));
    }
    if nonce.len() != 12 {
        return Err(tropel_core::TropelError::Crypto(
            "AES-GCM nonce must be 12 bytes".into(),
        ));
    }
    if ciphertext.len() < 16 {
        return Err(tropel_core::TropelError::Crypto(
            "AES-GCM ciphertext too short (must include 16-byte tag)".into(),
        ));
    }

    let key_arr = aes_gcm::Key::<Aes256Gcm>::from_slice(key);
    let cipher = Aes256Gcm::new(key_arr);
    let nonce_arr = Nonce::from_slice(nonce);

    cipher
        .decrypt(nonce_arr, ciphertext)
        .map_err(|e| tropel_core::TropelError::Crypto(format!("AES-GCM decrypt failed: {}", e)))
}

/// AES-256-CBC encrypt with PKCS7 padding.
/// `key` must be 32 bytes, `iv` must be 16 bytes.
pub fn aes_cbc_encrypt(key: &[u8], iv: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
    use aes::cipher::generic_array::GenericArray;

    if key.len() != 32 {
        return Err(tropel_core::TropelError::Crypto(
            "AES-CBC key must be 32 bytes".into(),
        ));
    }
    if iv.len() != 16 {
        return Err(tropel_core::TropelError::Crypto(
            "AES-CBC iv must be 16 bytes".into(),
        ));
    }

    let key_arr = GenericArray::from_slice(key);
    let iv_arr = GenericArray::from_slice(iv);

    // Buffer needs space for plaintext + one full block of padding
    let block_size = 16;
    let mut buf = vec![0u8; plaintext.len() + block_size];
    buf[..plaintext.len()].copy_from_slice(plaintext);

    let cipher = Aes256CbcEnc::new(key_arr, iv_arr);
    let encrypted = cipher
        .encrypt_padded_mut::<Pkcs7>(&mut buf, plaintext.len())
        .map_err(|e| tropel_core::TropelError::Crypto(format!("AES-CBC encrypt failed: {}", e)))?;

    Ok(encrypted.to_vec())
}

/// AES-256-CBC decrypt with PKCS7 padding.
pub fn aes_cbc_decrypt(key: &[u8], iv: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>> {
    use aes::cipher::generic_array::GenericArray;

    if key.len() != 32 {
        return Err(tropel_core::TropelError::Crypto(
            "AES-CBC key must be 32 bytes".into(),
        ));
    }
    if iv.len() != 16 {
        return Err(tropel_core::TropelError::Crypto(
            "AES-CBC iv must be 16 bytes".into(),
        ));
    }
    if ciphertext.is_empty() || ciphertext.len() % 16 != 0 {
        return Err(tropel_core::TropelError::Crypto(
            "AES-CBC ciphertext must be non-empty and block-aligned (16 bytes)".into(),
        ));
    }

    let key_arr = GenericArray::from_slice(key);
    let iv_arr = GenericArray::from_slice(iv);

    let mut buf = ciphertext.to_vec();
    let cipher = Aes256CbcDec::new(key_arr, iv_arr);
    let decrypted = cipher
        .decrypt_padded_mut::<Pkcs7>(&mut buf)
        .map_err(|e| tropel_core::TropelError::Crypto(format!("AES-CBC decrypt failed: {}", e)))?;

    Ok(decrypted.to_vec())
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

    #[test]
    fn test_aes_gcm_roundtrip() {
        let key = b"01234567890123456789012345678901"; // 32 bytes
        let nonce = b"012345678901"; // 12 bytes
        let plaintext = b"hello world";

        let ciphertext = aes_gcm_encrypt(key, nonce, plaintext).unwrap();
        assert!(ciphertext.len() > plaintext.len()); // includes tag

        let decrypted = aes_gcm_decrypt(key, nonce, &ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_aes_cbc_roundtrip() {
        let key = b"01234567890123456789012345678901"; // 32 bytes
        let iv = b"0123456789abcdef"; // 16 bytes
        let plaintext = b"hello world";

        let ciphertext = aes_cbc_encrypt(key, iv, plaintext).unwrap();
        assert_eq!(ciphertext.len() % 16, 0); // block-aligned

        let decrypted = aes_cbc_decrypt(key, iv, &ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_aes_cbc_wrong_key_fails() {
        let key = b"01234567890123456789012345678901";
        let wrong_key = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let iv = b"0123456789abcdef";
        let plaintext = b"hello world";

        let ciphertext = aes_cbc_encrypt(key, iv, plaintext).unwrap();
        let result = aes_cbc_decrypt(wrong_key, iv, &ciphertext);
        assert!(result.is_err()); // padding error on wrong key
    }
}

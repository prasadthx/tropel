use crate::NativeModule;
use md5::Md5;
use rquickjs::function::Func;
use sha2::Digest;
use tropel_core::Result;
use tropel_js::JsContext;

pub struct CryptoModule;

impl NativeModule for CryptoModule {
    fn name(&self) -> &str {
        "__tropel_native_crypto"
    }

    fn install(&self, ctx: &mut JsContext) -> Result<()> {
        ctx.with_ctx(|rq_ctx| {
            let globals = rq_ctx.globals();

            // ── Hashes ──
            let _ = globals.set(
                "__tropel_native_sha256",
                Func::from(|data: Vec<u8>| -> Vec<u8> { sha256(&data) }),
            );

            let _ = globals.set(
                "__tropel_native_sha384",
                Func::from(|data: Vec<u8>| -> Vec<u8> { sha384(&data) }),
            );

            let _ = globals.set(
                "__tropel_native_sha512",
                Func::from(|data: Vec<u8>| -> Vec<u8> { sha512(&data) }),
            );

            let _ = globals.set(
                "__tropel_native_sha1",
                Func::from(|data: Vec<u8>| -> Vec<u8> { sha1(&data) }),
            );

            let _ = globals.set(
                "__tropel_native_md5",
                Func::from(|data: Vec<u8>| -> Vec<u8> { md5(&data) }),
            );

            let _ = globals.set(
                "__tropel_native_sha3_256",
                Func::from(|data: Vec<u8>| -> Vec<u8> { sha3_256(&data) }),
            );

            // CryptoJS.SHA3 uses Keccak (original padding 0x01), NOT the
            // NIST SHA3 (0x06) — they differ on every message. Backlog line
            // 155: `CryptoJS.SHA3('hello')` is Keccak-512 by default; the
            // output length is a multiple of 32 between 224 and 512.
            let _ = globals.set(
                "__tropel_native_keccak",
                Func::from(|data: Vec<u8>, output_bits: u32| -> Option<Vec<u8>> {
                    keccak(&data, output_bits).ok()
                }),
            );

            let _ = globals.set(
                "__tropel_native_ripemd160",
                Func::from(|data: Vec<u8>| -> Vec<u8> { ripemd160(&data) }),
            );

            // ── HMACs ──
            let _ = globals.set(
                "__tropel_native_hmac_sha256",
                Func::from(|key: Vec<u8>, data: Vec<u8>| -> Vec<u8> { hmac_sha256(&key, &data) }),
            );

            let _ = globals.set(
                "__tropel_native_hmac_sha1",
                Func::from(|key: Vec<u8>, data: Vec<u8>| -> Vec<u8> { hmac_sha1(&key, &data) }),
            );

            let _ = globals.set(
                "__tropel_native_hmac_sha512",
                Func::from(|key: Vec<u8>, data: Vec<u8>| -> Vec<u8> { hmac_sha512(&key, &data) }),
            );

            let _ = globals.set(
                "__tropel_native_hmac_md5",
                Func::from(|key: Vec<u8>, data: Vec<u8>| -> Vec<u8> { hmac_md5(&key, &data) }),
            );

            // ── AES-256-GCM (authenticated encryption) ──
            // Returns None on error (wrong key/nonce length, auth failure)
            // instead of panicking across the FFI boundary.
            let _ = globals.set(
                "__tropel_native_aes_gcm_encrypt",
                Func::from(
                    |key: Vec<u8>, nonce: Vec<u8>, plaintext: Vec<u8>| -> Option<Vec<u8>> {
                        aes_gcm_encrypt(&key, &nonce, &plaintext).ok()
                    },
                ),
            );

            let _ = globals.set(
                "__tropel_native_aes_gcm_decrypt",
                Func::from(
                    |key: Vec<u8>, nonce: Vec<u8>, ciphertext: Vec<u8>| -> Option<Vec<u8>> {
                        aes_gcm_decrypt(&key, &nonce, &ciphertext).ok()
                    },
                ),
            );

            // ── AES-256-CBC (PKCS7 padding) ──
            let _ = globals.set(
                "__tropel_native_aes_cbc_encrypt",
                Func::from(
                    |key: Vec<u8>, iv: Vec<u8>, plaintext: Vec<u8>| -> Option<Vec<u8>> {
                        aes_cbc_encrypt(&key, &iv, &plaintext).ok()
                    },
                ),
            );

            let _ = globals.set(
                "__tropel_native_aes_cbc_decrypt",
                Func::from(
                    |key: Vec<u8>, iv: Vec<u8>, ciphertext: Vec<u8>| -> Option<Vec<u8>> {
                        aes_cbc_decrypt(&key, &iv, &ciphertext).ok()
                    },
                ),
            );

            // ── CSPRNG: generate cryptographically secure random bytes ──
            let _ = globals.set(
                "__tropel_native_random_bytes",
                Func::from(|n: u32| -> Vec<u8> { random_bytes(n as usize) }),
            );

            // ── EVP_BytesToKey (OpenSSL-compatible key derivation for CryptoJS interop) ──
            // Derives a key+iv pair from a passphrase + salt using iterative MD5.
            // Returns JSON: {"key": [...], "iv": [...]}
            let _ = globals.set(
                "__tropel_native_evp_bytes_to_key",
                Func::from(
                    |password: Vec<u8>, salt: Vec<u8>, key_len: u32, iv_len: u32| -> String {
                        let (key, iv) =
                            evp_bytes_to_key(&password, &salt, key_len as usize, iv_len as usize);
                        serde_json::json!({"key": key, "iv": iv}).to_string()
                    },
                ),
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

/// Compute a Keccak (original padding) hash of `output_bits` bits — the
/// algorithm CryptoJS's `SHA3` uses. Output lengths 224/256/384/512 are
/// supported (CryptoJS accepts any multiple of 32 in that range).
pub fn keccak(data: &[u8], output_bits: u32) -> Result<Vec<u8>> {
    use sha3::digest::Digest;
    let digest = match output_bits {
        224 => sha3::Keccak224::digest(data).to_vec(),
        256 => sha3::Keccak256::digest(data).to_vec(),
        384 => sha3::Keccak384::digest(data).to_vec(),
        512 => sha3::Keccak512::digest(data).to_vec(),
        n => {
            return Err(tropel_core::TropelError::Crypto(format!(
                "Keccak output length must be 224, 256, 384, or 512 bits (got {})",
                n
            )))
        }
    };
    Ok(digest)
}

/// Compute RIPEMD-160 hash.
pub fn ripemd160(data: &[u8]) -> Vec<u8> {
    use ripemd::Digest;
    let mut hasher = ripemd::Ripemd160::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

/// Compute HMAC-SHA256.
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::digest::KeyInit;
    use hmac::Mac;
    let mut mac =
        <hmac::Hmac<sha2::Sha256> as KeyInit>::new_from_slice(key).expect("HMAC key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Compute HMAC-SHA1.
pub fn hmac_sha1(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::digest::KeyInit;
    use hmac::Mac;
    let mut mac =
        <hmac::Hmac<sha1::Sha1> as KeyInit>::new_from_slice(key).expect("HMAC key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Compute HMAC-SHA512.
pub fn hmac_sha512(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::digest::KeyInit;
    use hmac::Mac;
    let mut mac =
        <hmac::Hmac<sha2::Sha512> as KeyInit>::new_from_slice(key).expect("HMAC key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Compute HMAC-MD5.
pub fn hmac_md5(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::digest::KeyInit;
    use hmac::Mac;
    let mut mac = <hmac::Hmac<md5::Md5> as KeyInit>::new_from_slice(key).expect("HMAC key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Generate `n` cryptographically secure random bytes using the OS CSPRNG.
pub fn random_bytes(n: usize) -> Vec<u8> {
    use rand::Rng;
    let mut buf = vec![0u8; n];
    rand::rng().fill_bytes(&mut buf);
    buf
}

/// OpenSSL-compatible EVP_BytesToKey key derivation.
///
/// Derives a key+iv pair from a passphrase and salt using iterative MD5,
/// matching the algorithm used by CryptoJS when a string passphrase is
/// provided (and by OpenSSL's `enc` command).
///
/// Algorithm:
///   D_0 = ''
///   D_i = MD5(D_{i-1} || password || salt)
///   Concatenate D_1, D_2, ... until key_len + iv_len bytes are produced
///   key = first key_len bytes, iv = next iv_len bytes
pub fn evp_bytes_to_key(
    password: &[u8],
    salt: &[u8],
    key_len: usize,
    iv_len: usize,
) -> (Vec<u8>, Vec<u8>) {
    let total = key_len + iv_len;
    let mut derived = Vec::with_capacity(total);
    let mut prev_hash: Vec<u8> = Vec::new();

    while derived.len() < total {
        let mut hasher = Md5::new();
        // Prepend previous hash block
        hasher.update(&prev_hash);
        // Append password and salt
        hasher.update(password);
        hasher.update(salt);
        let hash = hasher.finalize().to_vec();
        derived.extend_from_slice(&hash);
        prev_hash = hash;
    }

    let key = derived[..key_len].to_vec();
    let iv = derived[key_len..key_len + iv_len].to_vec();
    (key, iv)
}

/// aes-gcm 0.11 has no `Aes192Gcm` alias (only Aes128/Aes256 under the `aes`
/// feature); build the 192-bit variant from the generic type.
type Aes192Gcm = aes_gcm::AesGcm<aes_gcm::aes::Aes192, aes_gcm::aead::consts::U12>;

/// Dispatch an AES-GCM encrypt across key sizes (16/24/32 bytes =
/// AES-128/192/256). CryptoJS selects the cipher by key length — rejecting
/// 16/24-byte keys broke every AES-128/192 script (backlog line 155).
pub fn aes_gcm_encrypt(key: &[u8], nonce: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
    use aes_gcm::aead::Aead;
    use aes_gcm::KeyInit as _;
    use aes_gcm::{Aes128Gcm, Aes256Gcm, Nonce};

    let nonce_arr = Nonce::try_from(nonce)
        .map_err(|_| tropel_core::TropelError::Crypto("AES-GCM nonce must be 12 bytes".into()))?;
    let err_key = |n: usize| {
        tropel_core::TropelError::Crypto(format!(
            "AES-GCM key must be 16, 24, or 32 bytes (got {})",
            n
        ))
    };
    match key.len() {
        16 => {
            let cipher = Aes128Gcm::new(
                &aes_gcm::Key::<Aes128Gcm>::try_from(key).map_err(|_| err_key(key.len()))?,
            );
            cipher.encrypt(&nonce_arr, plaintext).map_err(|e| {
                tropel_core::TropelError::Crypto(format!("AES-GCM encrypt failed: {}", e))
            })
        }
        24 => {
            let cipher = Aes192Gcm::new(
                &aes_gcm::Key::<Aes192Gcm>::try_from(key).map_err(|_| err_key(key.len()))?,
            );
            cipher.encrypt(&nonce_arr, plaintext).map_err(|e| {
                tropel_core::TropelError::Crypto(format!("AES-GCM encrypt failed: {}", e))
            })
        }
        32 => {
            let cipher = Aes256Gcm::new(
                &aes_gcm::Key::<Aes256Gcm>::try_from(key).map_err(|_| err_key(key.len()))?,
            );
            cipher.encrypt(&nonce_arr, plaintext).map_err(|e| {
                tropel_core::TropelError::Crypto(format!("AES-GCM encrypt failed: {}", e))
            })
        }
        n => Err(err_key(n)),
    }
}

/// AES-GCM decrypt with key-size dispatch (16/24/32 bytes).
pub fn aes_gcm_decrypt(key: &[u8], nonce: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>> {
    use aes_gcm::aead::Aead;
    use aes_gcm::KeyInit as _;
    use aes_gcm::{Aes128Gcm, Aes256Gcm, Nonce};

    if ciphertext.len() < 16 {
        return Err(tropel_core::TropelError::Crypto(
            "AES-GCM ciphertext too short (must include 16-byte tag)".into(),
        ));
    }
    let nonce_arr = Nonce::try_from(nonce)
        .map_err(|_| tropel_core::TropelError::Crypto("AES-GCM nonce must be 12 bytes".into()))?;
    let err_key = |n: usize| {
        tropel_core::TropelError::Crypto(format!(
            "AES-GCM key must be 16, 24, or 32 bytes (got {})",
            n
        ))
    };
    match key.len() {
        16 => {
            let cipher = Aes128Gcm::new(
                &aes_gcm::Key::<Aes128Gcm>::try_from(key).map_err(|_| err_key(key.len()))?,
            );
            cipher.decrypt(&nonce_arr, ciphertext).map_err(|e| {
                tropel_core::TropelError::Crypto(format!("AES-GCM decrypt failed: {}", e))
            })
        }
        24 => {
            let cipher = Aes192Gcm::new(
                &aes_gcm::Key::<Aes192Gcm>::try_from(key).map_err(|_| err_key(key.len()))?,
            );
            cipher.decrypt(&nonce_arr, ciphertext).map_err(|e| {
                tropel_core::TropelError::Crypto(format!("AES-GCM decrypt failed: {}", e))
            })
        }
        32 => {
            let cipher = Aes256Gcm::new(
                &aes_gcm::Key::<Aes256Gcm>::try_from(key).map_err(|_| err_key(key.len()))?,
            );
            cipher.decrypt(&nonce_arr, ciphertext).map_err(|e| {
                tropel_core::TropelError::Crypto(format!("AES-GCM decrypt failed: {}", e))
            })
        }
        n => Err(err_key(n)),
    }
}

/// AES-CBC encrypt with PKCS7 padding, key-size dispatch (16/24/32 bytes).
pub fn aes_cbc_encrypt(key: &[u8], iv: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
    use cbc::cipher::block_padding::Pkcs7;
    use cbc::cipher::{Array, BlockModeEncrypt, KeyIvInit as _};

    if iv.len() != 16 {
        return Err(tropel_core::TropelError::Crypto(
            "AES-CBC iv must be 16 bytes".into(),
        ));
    }
    let iv_arr = Array::<u8, aes::cipher::consts::U16>::try_from(iv)
        .map_err(|_| tropel_core::TropelError::Crypto("AES-CBC iv must be 16 bytes".into()))?;

    let mut buf = vec![0u8; plaintext.len() + 16];
    buf[..plaintext.len()].copy_from_slice(plaintext);
    let err_key = |n: usize| {
        tropel_core::TropelError::Crypto(format!(
            "AES-CBC key must be 16, 24, or 32 bytes (got {})",
            n
        ))
    };
    let encrypted = match key.len() {
        16 => {
            let key_arr = Array::<u8, aes::cipher::consts::U16>::try_from(key)
                .map_err(|_| err_key(key.len()))?;
            cbc::Encryptor::<aes::Aes128>::new(&key_arr, &iv_arr)
                .encrypt_padded::<Pkcs7>(&mut buf, plaintext.len())
        }
        24 => {
            let key_arr = Array::<u8, aes::cipher::consts::U24>::try_from(key)
                .map_err(|_| err_key(key.len()))?;
            cbc::Encryptor::<aes::Aes192>::new(&key_arr, &iv_arr)
                .encrypt_padded::<Pkcs7>(&mut buf, plaintext.len())
        }
        32 => {
            let key_arr = Array::<u8, aes::cipher::consts::U32>::try_from(key)
                .map_err(|_| err_key(key.len()))?;
            cbc::Encryptor::<aes::Aes256>::new(&key_arr, &iv_arr)
                .encrypt_padded::<Pkcs7>(&mut buf, plaintext.len())
        }
        n => return Err(err_key(n)),
    };
    encrypted
        .map(|out| out.to_vec())
        .map_err(|e| tropel_core::TropelError::Crypto(format!("AES-CBC encrypt failed: {}", e)))
}

/// AES-CBC decrypt with PKCS7 padding, key-size dispatch (16/24/32 bytes).
pub fn aes_cbc_decrypt(key: &[u8], iv: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>> {
    use cbc::cipher::block_padding::Pkcs7;
    use cbc::cipher::{Array, BlockModeDecrypt, KeyIvInit as _};

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
    let iv_arr = Array::<u8, aes::cipher::consts::U16>::try_from(iv)
        .map_err(|_| tropel_core::TropelError::Crypto("AES-CBC iv must be 16 bytes".into()))?;
    let mut buf = ciphertext.to_vec();
    let err_key = |n: usize| {
        tropel_core::TropelError::Crypto(format!(
            "AES-CBC key must be 16, 24, or 32 bytes (got {})",
            n
        ))
    };
    let decrypted = match key.len() {
        16 => {
            let key_arr = Array::<u8, aes::cipher::consts::U16>::try_from(key)
                .map_err(|_| err_key(key.len()))?;
            cbc::Decryptor::<aes::Aes128>::new(&key_arr, &iv_arr).decrypt_padded::<Pkcs7>(&mut buf)
        }
        24 => {
            let key_arr = Array::<u8, aes::cipher::consts::U24>::try_from(key)
                .map_err(|_| err_key(key.len()))?;
            cbc::Decryptor::<aes::Aes192>::new(&key_arr, &iv_arr).decrypt_padded::<Pkcs7>(&mut buf)
        }
        32 => {
            let key_arr = Array::<u8, aes::cipher::consts::U32>::try_from(key)
                .map_err(|_| err_key(key.len()))?;
            cbc::Decryptor::<aes::Aes256>::new(&key_arr, &iv_arr).decrypt_padded::<Pkcs7>(&mut buf)
        }
        n => return Err(err_key(n)),
    };
    decrypted
        .map(|out| out.to_vec())
        .map_err(|e| tropel_core::TropelError::Crypto(format!("AES-CBC decrypt failed: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_bytes() {
        let bytes = random_bytes(32);
        assert_eq!(bytes.len(), 32);
        // Two calls should produce different results (CSPRNG)
        let bytes2 = random_bytes(32);
        assert_ne!(bytes, bytes2);
    }

    #[test]
    fn test_random_bytes_zero() {
        let bytes = random_bytes(0);
        assert!(bytes.is_empty());
    }

    #[test]
    fn test_evp_bytes_to_key() {
        // Test vector: known password + salt should produce deterministic output
        let password = b"password";
        let salt = b"12345678";
        let (key, iv) = evp_bytes_to_key(password, salt, 32, 16);
        assert_eq!(key.len(), 32);
        assert_eq!(iv.len(), 16);
        // Deterministic for same inputs
        let (key2, iv2) = evp_bytes_to_key(password, salt, 32, 16);
        assert_eq!(key, key2);
        assert_eq!(iv, iv2);
    }

    #[test]
    fn test_evp_bytes_to_key_different_salt() {
        let password = b"password";
        let (key1, _) = evp_bytes_to_key(password, b"aaaaaaaa", 32, 16);
        let (key2, _) = evp_bytes_to_key(password, b"bbbbbbbb", 32, 16);
        assert_ne!(key1, key2, "Different salts should produce different keys");
    }

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
    fn test_keccak_known_vectors() {
        // Well-known Keccak (original padding) reference vectors — the
        // algorithm CryptoJS.SHA3 uses. Keccak-256("") is the Ethereum
        // empty-account hash; both are published reference values.
        let hex_256 = hex::encode(keccak(b"", 256).unwrap());
        assert_eq!(
            hex_256,
            "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
        );
        let hex_512 = hex::encode(keccak(b"hello", 512).unwrap());
        assert_eq!(
            hex_512,
            "52fa80662e64c128f8389c9ea6c73d4c02368004bf4463491900d11aaadca39d47de1b01361f207c512cfa79f0f92c3395c67ff7928e3f5ce3e3c852b392f976"
        );
        // 224/384 also supported; invalid length errors cleanly.
        assert_eq!(keccak(b"x", 224).unwrap().len(), 28);
        assert_eq!(keccak(b"x", 384).unwrap().len(), 48);
        assert!(keccak(b"x", 288).is_err());
    }

    #[test]
    fn test_aes_gcm_key_sizes() {
        // CryptoJS selects the cipher by key length: 16/24/32 bytes =
        // AES-128/192/256 (backlog line 155: 128/192 were rejected).
        let nonce = b"012345678901";
        let pt = b"hello world";
        for key in [
            b"0123456789abcdef".as_slice(),                 // 16 = AES-128
            b"0123456789abcdef01234567".as_slice(),         // 24 = AES-192
            b"0123456789abcdef0123456789abcdef".as_slice(), // 32 = AES-256
        ] {
            let ct = aes_gcm_encrypt(key, nonce, pt).unwrap();
            assert_eq!(aes_gcm_decrypt(key, nonce, &ct).unwrap(), pt);
        }
    }

    #[test]
    fn test_aes_cbc_key_sizes() {
        let iv = b"0123456789abcdef";
        let pt = b"hello world";
        for key in [
            b"0123456789abcdef".as_slice(),
            b"0123456789abcdef01234567".as_slice(),
            b"0123456789abcdef0123456789abcdef".as_slice(),
        ] {
            let ct = aes_cbc_encrypt(key, iv, pt).unwrap();
            assert_eq!(ct.len() % 16, 0);
            assert_eq!(aes_cbc_decrypt(key, iv, &ct).unwrap(), pt);
        }
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

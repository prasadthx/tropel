// ─── CryptoJS Shim for Tropel ────────────────────────────
// CryptoJS-compatible API that delegates hashing, encoding,
// and encryption operations to the native Rust tropel-native module.

var CryptoJS = CryptoJS || {};

(function () {
    // ── Internal: WordArray helper ──
    function WordArray(words, sigBytes) {
        this.words = words || [];
        this.sigBytes = sigBytes || (this.words.length * 4);
    }

    WordArray.prototype.toString = function (encoder) {
        return (encoder || CryptoJS.enc.Hex).stringify(this);
    };

    WordArray.prototype.concat = function (wordArray) {
        this.words = this.words.concat(wordArray.words);
        this.sigBytes += wordArray.sigBytes;
        return this;
    };

    WordArray.create = function (words, sigBytes) {
        // Backlog line 155: the old signature IGNORED its arguments.
        return new WordArray(words, sigBytes);
    };

    // CryptoJS.lib.WordArray.random(nBytes) — CSPRNG-backed, so scripts that
    // generate keys/IVs with it behave identically to real CryptoJS. Fails
    // loudly if the CSPRNG is unavailable (never falls back to weak bytes).
    WordArray.random = function (nBytes) {
        if (typeof __tropel_native_random_bytes === 'function') {
            return wordArrayFromBytes(__tropel_native_random_bytes(nBytes));
        }
        throw new Error('CSPRNG unavailable: cannot generate random bytes');
    };

    function wordArrayFromBytes(bytes) {
        var words = [];
        for (var i = 0; i < bytes.length; i++) {
            var wordIndex = Math.floor(i / 4);
            words[wordIndex] = (words[wordIndex] || 0) | (bytes[i] << (24 - (i % 4) * 8));
        }
        return new WordArray(words, bytes.length);
    }

    function bytesFromWordArray(wordArray) {
        var bytes = [];
        for (var i = 0; i < wordArray.sigBytes; i++) {
            var wordIndex = Math.floor(i / 4);
            var byteIndex = 24 - (i % 4) * 8;
            bytes.push((wordArray.words[wordIndex] >>> byteIndex) & 0xFF);
        }
        return bytes;
    }

    // ── Encoding Strategies ──
    CryptoJS.enc = {};

    // Hex
    CryptoJS.enc.Hex = {
        stringify: function (wordArray) {
            var hex = '';
            var bytes = bytesFromWordArray(wordArray);
            if (typeof __tropel_native_hex_encode === 'function') {
                // Use native hex encoding
                // Native expects bytes, returns hex string
            }
            for (var i = 0; i < bytes.length; i++) {
                hex += (bytes[i] >>> 4).toString(16);
                hex += (bytes[i] & 0xF).toString(16);
            }
            return hex;
        },
        parse: function (hexStr) {
            var bytes = [];
            for (var i = 0; i < hexStr.length; i += 2) {
                bytes.push(parseInt(hexStr.substr(i, 2), 16));
            }
            return wordArrayFromBytes(bytes);
        }
    };

    // Base64
    CryptoJS.enc.Base64 = {
        stringify: function (wordArray) {
            var bytes = bytesFromWordArray(wordArray);
            if (typeof __tropel_native_base64_encode === 'function') {
                return __tropel_native_base64_encode(bytes);
            }
            // Fallback JS implementation
            var chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=';
            var result = '';
            for (var i = 0; i < bytes.length; i += 3) {
                var b1 = bytes[i] || 0;
                var b2 = bytes[i + 1] || 0;
                var b3 = bytes[i + 2] || 0;
                result += chars[b1 >>> 2];
                result += chars[((b1 & 3) << 4) | (b2 >>> 4)];
                result += chars[((b2 & 15) << 2) | (b3 >>> 6)];
                result += chars[b3 & 63];
            }
            // Handle padding
            if (bytes.length % 3 === 1) {
                result = result.slice(0, -2) + '==';
            } else if (bytes.length % 3 === 2) {
                result = result.slice(0, -1) + '=';
            }
            return result;
        },
        parse: function (base64Str) {
            if (typeof __tropel_native_base64_decode === 'function') {
                return wordArrayFromBytes(__tropel_native_base64_decode(base64Str));
            }
            // Fallback (no native): the old alphabet contained '=' at index
            // 64, so every PADDED input decoded garbage bytes (backlog 155).
            var chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
            var s = String(base64Str);
            var pad = 0;
            while (s.charAt(s.length - 1 - pad) === '=') pad++;
            var cleaned = s.replace(/[^A-Za-z0-9+/]/g, '');
            var expected = Math.floor((cleaned.length * 3) / 4) - pad;
            var bytes = [];
            var buffer = 0;
            var bits = 0;
            for (var i = 0; i < cleaned.length; i++) {
                var v = chars.indexOf(cleaned.charAt(i));
                if (v < 0) continue;
                buffer = (buffer << 6) | v;
                bits += 6;
                if (bits >= 8) {
                    bits -= 8;
                    bytes.push((buffer >> bits) & 0xFF);
                }
            }
            if (bytes.length > expected) bytes.length = expected;
            return wordArrayFromBytes(bytes);
        }
    };

    // Utf8
    CryptoJS.enc.Utf8 = {
        stringify: function (wordArray) {
            var bytes = bytesFromWordArray(wordArray);
            return decodeURIComponent(Array.prototype.map.call(bytes, function (b) {
                return '%' + ('0' + (b & 0xFF).toString(16)).slice(-2);
            }).join(''));
        },
        parse: function (str) {
            // Backlog line 155: surrogate pairs (emoji) were encoded as
            // CESU-8 (two 3-byte sequences) instead of one 4-byte UTF-8
            // code point.
            var bytes = [];
            for (var i = 0; i < str.length; i++) {
                var c = str.charCodeAt(i);
                if (c >= 0xD800 && c <= 0xDBFF && i + 1 < str.length) {
                    var lo = str.charCodeAt(i + 1);
                    if (lo >= 0xDC00 && lo <= 0xDFFF) {
                        var cp = 0x10000 + ((c - 0xD800) << 10) + (lo - 0xDC00);
                        bytes.push(0xF0 | (cp >> 18));
                        bytes.push(0x80 | ((cp >> 12) & 0x3F));
                        bytes.push(0x80 | ((cp >> 6) & 0x3F));
                        bytes.push(0x80 | (cp & 0x3F));
                        i++;
                        continue;
                    }
                }
                if (c < 0x80) {
                    bytes.push(c);
                } else if (c < 0x800) {
                    bytes.push(192 | (c >> 6));
                    bytes.push(128 | (c & 63));
                } else {
                    bytes.push(224 | (c >> 12));
                    bytes.push(128 | ((c >> 6) & 63));
                    bytes.push(128 | (c & 63));
                }
            }
            return wordArrayFromBytes(bytes);
        }
    };

    CryptoJS.enc.Utf16 = CryptoJS.enc.Utf8;
    CryptoJS.enc.Latin1 = {
        stringify: function (wordArray) {
            var bytes = bytesFromWordArray(wordArray);
            return String.fromCharCode.apply(null, bytes);
        },
        parse: function (str) {
            var bytes = [];
            for (var i = 0; i < str.length; i++) {
                bytes.push(str.charCodeAt(i) & 0xFF);
            }
            return wordArrayFromBytes(bytes);
        }
    };

    // ── Hasher ──
    function Hasher(algorithm) {
        this._algorithm = algorithm;
        // Backlog line 155: `SHA256('')` threw because finalize() touched
        // uninitialized `_data` (the old code only created it in update()).
        this.reset();
    }

    Hasher.prototype.reset = function () {
        this._data = [];
    };

    Hasher.prototype.update = function (messageUpdate) {
        var data = typeof messageUpdate === 'string'
            ? CryptoJS.enc.Utf8.parse(messageUpdate)
            : messageUpdate;
        if (!this._data) this._data = [];
        this._data.push(data);
        return this;
    };

    Hasher.prototype.finalize = function (messageUpdate) {
        // `''` is a valid message — the old truthiness check skipped it and
        // finalize() then crashed on undefined _data (backlog line 155).
        if (messageUpdate !== undefined && messageUpdate !== null) this.update(messageUpdate);
        var allBytes = [];
        for (var i = 0; i < this._data.length; i++) {
            allBytes = allBytes.concat(bytesFromWordArray(this._data[i]));
        }

        var result;
        switch (this._algorithm) {
            case 'MD5':
                if (typeof __tropel_native_md5 === 'function') {
                    result = wordArrayFromBytes(__tropel_native_md5(allBytes));
                } else {
                    result = this._fallbackHash(allBytes, 'MD5');
                }
                break;
            case 'SHA1':
                if (typeof __tropel_native_sha1 === 'function') {
                    result = wordArrayFromBytes(__tropel_native_sha1(allBytes));
                } else {
                    result = this._fallbackHash(allBytes, 'SHA1');
                }
                break;
            case 'SHA256':
                if (typeof __tropel_native_sha256 === 'function') {
                    result = wordArrayFromBytes(__tropel_native_sha256(allBytes));
                } else {
                    result = this._fallbackHash(allBytes, 'SHA256');
                }
                break;
            case 'SHA384':
                if (typeof __tropel_native_sha384 === 'function') {
                    result = wordArrayFromBytes(__tropel_native_sha384(allBytes));
                } else {
                    result = this._fallbackHash(allBytes, 'SHA384');
                }
                break;
            case 'SHA512':
                if (typeof __tropel_native_sha512 === 'function') {
                    result = wordArrayFromBytes(__tropel_native_sha512(allBytes));
                } else {
                    result = this._fallbackHash(allBytes, 'SHA512');
                }
                break;
            case 'SHA3':
                // CryptoJS.SHA3 is KECCAK-512 by default (original padding
                // 0x01), NOT NIST SHA3-256 — they differ on every input
                // (backlog line 155). `_outputLength` defaults to 512 bits.
                // Fail loudly on unsupported lengths (native keccak supports
                // 224/256/384/512) instead of fabricating a wrong digest.
                if (typeof __tropel_native_keccak === 'function') {
                    var bits = this._outputLength || 512;
                    var keccakOut = __tropel_native_keccak(allBytes, bits);
                    if (keccakOut !== null && keccakOut !== undefined) {
                        result = wordArrayFromBytes(keccakOut);
                    } else {
                        throw new Error(
                            'SHA3 output length must be 224, 256, 384, or 512 bits (got ' +
                                bits + ')'
                        );
                    }
                } else {
                    throw new Error('SHA3 unavailable: native keccak not installed');
                }
                break;
            case 'RIPEMD160':
                if (typeof __tropel_native_ripemd160 === 'function') {
                    result = wordArrayFromBytes(__tropel_native_ripemd160(allBytes));
                } else {
                    result = this._fallbackHash(allBytes, 'RIPEMD160');
                }
                break;
            default:
                throw new Error('Unknown algorithm: ' + this._algorithm);
        }

        this._data = [];
        return result;
    };

    Hasher.prototype._fallbackHash = function (_bytes, algorithm) {
        // Backlog line 155: the old fallback FABRICATED a plausible-looking
        // digest (every algorithm returned the same made-up words), silently
        // corrupting hashes when native crypto was absent. Native crypto is
        // always installed by Tropel, so a missing function is a real error:
        // fail loudly instead of returning a fake hash.
        throw new Error('Native ' + algorithm + ' is not available in this runtime');
    };

    function createHasher(algorithm) {
        return new Hasher(algorithm);
    }

    // ── Exposed Hash Functions ──
    CryptoJS.MD5 = function (message, key) {
        var hasher = createHasher('MD5');
        var result = hasher.finalize(message);
        if (key) {
            return CryptoJS.HmacMD5(message, key);
        }
        return result;
    };

    CryptoJS.SHA1 = function (message, key) {
        var hasher = createHasher('SHA1');
        var result = hasher.finalize(message);
        if (key) {
            return CryptoJS.HmacSHA1(message, key);
        }
        return result;
    };

    CryptoJS.SHA256 = function (message, key) {
        var hasher = createHasher('SHA256');
        var result = hasher.finalize(message);
        if (key) {
            return CryptoJS.HmacSHA256(message, key);
        }
        return result;
    };

    CryptoJS.SHA384 = function (message) {
        var hasher = createHasher('SHA384');
        return hasher.finalize(message);
    };

    CryptoJS.SHA512 = function (message) {
        var hasher = createHasher('SHA512');
        return hasher.finalize(message);
    };

    CryptoJS.SHA3 = function (message, outputLength) {
        var hasher = createHasher('SHA3');
        // CryptoJS default is 512 bits; 224/256/384 also supported.
        hasher._outputLength = outputLength || 512;
        return hasher.finalize(message);
    };

    // ── HMAC ──
    CryptoJS.HmacSHA1 = function (message, key) {
        var msgBytes = typeof message === 'string'
            ? bytesFromWordArray(CryptoJS.enc.Utf8.parse(message))
            : bytesFromWordArray(message);
        var keyBytes = typeof key === 'string'
            ? bytesFromWordArray(CryptoJS.enc.Utf8.parse(key))
            : bytesFromWordArray(key);

        if (typeof __tropel_native_hmac_sha1 === 'function') {
            return wordArrayFromBytes(__tropel_native_hmac_sha1(keyBytes, msgBytes));
        }
        throw new Error('HMAC-SHA1 native function not available');
    };

    CryptoJS.HmacSHA256 = function (message, key) {
        var msgBytes = typeof message === 'string'
            ? bytesFromWordArray(CryptoJS.enc.Utf8.parse(message))
            : bytesFromWordArray(message);
        var keyBytes = typeof key === 'string'
            ? bytesFromWordArray(CryptoJS.enc.Utf8.parse(key))
            : bytesFromWordArray(key);

        if (typeof __tropel_native_hmac_sha256 === 'function') {
            return wordArrayFromBytes(__tropel_native_hmac_sha256(keyBytes, msgBytes));
        }
        throw new Error('HMAC-SHA256 native function not available');
    };

    CryptoJS.HmacMD5 = function (message, key) {
        var msgBytes = typeof message === 'string'
            ? bytesFromWordArray(CryptoJS.enc.Utf8.parse(message))
            : bytesFromWordArray(message);
        var keyBytes = typeof key === 'string'
            ? bytesFromWordArray(CryptoJS.enc.Utf8.parse(key))
            : bytesFromWordArray(key);

        if (typeof __tropel_native_hmac_md5 === 'function') {
            return wordArrayFromBytes(__tropel_native_hmac_md5(keyBytes, msgBytes));
        }
        throw new Error('HMAC-MD5 native function not available');
    };

    CryptoJS.HmacSHA512 = function (message, key) {
        var msgBytes = typeof message === 'string'
            ? bytesFromWordArray(CryptoJS.enc.Utf8.parse(message))
            : bytesFromWordArray(message);
        var keyBytes = typeof key === 'string'
            ? bytesFromWordArray(CryptoJS.enc.Utf8.parse(key))
            : bytesFromWordArray(key);

        if (typeof __tropel_native_hmac_sha512 === 'function') {
            return wordArrayFromBytes(__tropel_native_hmac_sha512(keyBytes, msgBytes));
        }
        throw new Error('HMAC-SHA512 native function not available');
    };

    // ── EncryptedMessage helpers ──
    CryptoJS.lib = CryptoJS.lib || {};
    CryptoJS.lib.WordArray = WordArray;
    CryptoJS.lib.Hasher = Hasher;

    // ── Format helpers ──
    CryptoJS.format = {
        OpenSSL: {
            stringify: function (cipherParams) {
                var salt = cipherParams.salt || '';
                return CryptoJS.enc.Base64.stringify(salt) + cipherParams.ciphertext.toString(CryptoJS.enc.Base64);
            }
        }
    };

    // ── Mode / padding namespaces (backlog line 155) ──
    // The universal CryptoJS incantation `{ mode: CryptoJS.mode.CBC,
    // padding: CryptoJS.pad.Pkcs7 }` previously TypeErrored because these
    // namespaces did not exist. They are marker objects — the shim resolves
    // them by `.name` and otherwise defaults to CryptoJS's CBC/PKCS7.
    CryptoJS.mode = {
        CBC: { name: 'CBC' },
        ECB: { name: 'ECB' },
        CFB: { name: 'CFB' },
        OFB: { name: 'OFB' },
        CTR: { name: 'CTR' },
    };
    CryptoJS.pad = {
        Pkcs7: { name: 'Pkcs7' },
        AnsiX923: { name: 'AnsiX923' },
        Iso10126: { name: 'Iso10126' },
        Iso97971: { name: 'Iso97971' },
        ZeroPadding: { name: 'ZeroPadding' },
        NoPadding: { name: 'NoPadding' },
    };

    function resolveMode(mode) {
        if (typeof mode === 'string') return mode;
        if (mode && mode.name) return mode.name;
        return undefined;
    }

    // ── AES (real encryption via native Rust) ──
    /// Derive a key+IV from a passphrase using OpenSSL-compatible EVP_BytesToKey.
    /// For AES-256-GCM: key=32 bytes, iv=12 bytes.
    /// For AES-256-CBC: key=32 bytes, iv=16 bytes.
    function deriveKeyAndIv(passphrase, salt, keyLen, ivLen) {
        if (typeof __tropel_native_evp_bytes_to_key === 'function') {
            var passBytes = bytesFromWordArray(CryptoJS.enc.Utf8.parse(passphrase));
            var resultJson = __tropel_native_evp_bytes_to_key(passBytes, salt, keyLen, ivLen);
            var result = JSON.parse(resultJson);
            return {
                key: wordArrayFromBytes(result.key),
                iv: wordArrayFromBytes(result.iv)
            };
        }
        // Fallback JS implementation (rare — native is preferred)
        // EVP_BytesToKey: D_i = MD5(D_{i-1} + password + salt), concatenate until enough
        throw new Error('EVP_BytesToKey native function not available');
    }

    CryptoJS.AES = {
        /// Encrypt with AES-256-GCM (authenticated encryption)
        ///   message: string or WordArray (plaintext)
        ///   key: 32-byte key (WordArray) or passphrase (string, uses EVP_BytesToKey)
        ///   options: { iv: WordArray|bytes (12 for GCM, 16 for CBC),
        ///              mode: 'GCM' (default) | 'CBC' }
        /// Returns: { ciphertext: WordArray, key: WordArray, iv: WordArray,
        ///            salt: WordArray (empty for direct keys), toString: fn }
        encrypt: function (message, key, options) {
            var msgBytes = typeof message === 'string'
                ? CryptoJS.enc.Utf8.parse(message)
                : message;
            var plainBytes = bytesFromWordArray(msgBytes);

            options = options || {};
            // CryptoJS default is CBC/PKCS7, not GCM (backlog line 155).
            var mode = resolveMode(options.mode) || 'CBC';
            var ivLen = mode === 'CBC' ? 16 : 12;
            var keyLen = 32; // AES-256 passphrase derivation (CryptoJS default)

            var keyBytes;
            var keyWordArr;
            var ivBytes;
            var ivWordArr;
            var saltBytes = [];
            var saltWordArr = CryptoJS.enc.Hex.parse('');

            if (typeof key === 'string') {
                // String key = passphrase → use EVP_BytesToKey (OpenSSL-compatible)
                // Generate random 8-byte salt via CSPRNG
                if (typeof __tropel_native_random_bytes === 'function') {
                    saltBytes = __tropel_native_random_bytes(8);
                } else {
                    // Backlog line 155: a Date.now()-derived salt under a
                    // fixed passphrase enables key-reuse attacks — fail
                    // loudly instead of emitting trivially broken ciphertext.
                    throw new Error(
                        'CSPRNG unavailable: cannot generate a random salt for passphrase encryption'
                    );
                }
                saltWordArr = wordArrayFromBytes(saltBytes);

                var derived = deriveKeyAndIv(key, saltBytes, keyLen, ivLen);
                keyWordArr = derived.key;
                keyBytes = bytesFromWordArray(keyWordArr);
                ivWordArr = derived.iv;
                ivBytes = bytesFromWordArray(ivWordArr);
            } else {
                // WordArray key = use directly
                keyWordArr = key;
                keyBytes = bytesFromWordArray(key);

                // Generate random IV/nonce via CSPRNG if not provided
                if (options.iv) {
                    var ivWord = typeof options.iv === 'string'
                        ? CryptoJS.enc.Hex.parse(options.iv)
                        : options.iv;
                    ivWordArr = ivWord;
                    ivBytes = bytesFromWordArray(ivWord);
                } else if (typeof __tropel_native_random_bytes === 'function') {
                    ivBytes = __tropel_native_random_bytes(ivLen);
                    ivWordArr = wordArrayFromBytes(ivBytes);
                } else {
                    // Backlog line 155: a zero IV under a fixed key is
                    // catastrophic nonce reuse — fail loudly instead.
                    throw new Error('CSPRNG unavailable: cannot generate a random IV');
                }
            }

            // Execute encryption via native function
            var result;
            if (mode === 'CBC') {
                if (typeof __tropel_native_aes_cbc_encrypt !== 'function') {
                    throw new Error('AES-CBC encrypt native function not available');
                }
                result = __tropel_native_aes_cbc_encrypt(keyBytes, ivBytes, plainBytes);
            } else {
                if (typeof __tropel_native_aes_gcm_encrypt !== 'function') {
                    throw new Error('AES-GCM encrypt native function not available');
                }
                result = __tropel_native_aes_gcm_encrypt(keyBytes, ivBytes, plainBytes);
            }

            // Handle null result (e.g., wrong key length)
            if (result === null || result === undefined) {
                throw new Error('AES encrypt failed: bad key length, bad nonce length, or internal error');
            }

            var cipherWordArr = wordArrayFromBytes(result);

            // Return a CipherParams-like object
            var cipherParams = {
                ciphertext: cipherWordArr,
                key: keyWordArr,
                iv: ivWordArr,
                salt: saltWordArr,
                toString: function (encoder) {
                    var enc = encoder || CryptoJS.enc.Base64;
                    // If we used a passphrase (has salt), include salt in output
                    if (this.salt && this.salt.sigBytes > 0) {
                        // OpenSSL format: 'Salted__' + 8-byte salt + ciphertext
                        var saltedPrefix = wordArrayFromBytes([83, 97, 108, 116, 101, 100, 95, 95]); // 'Salted__'
                        saltedPrefix.sigBytes = 8;
                        var combined = CryptoJS.lib.WordArray.create();
                        combined = combined.concat(saltedPrefix);
                        combined = combined.concat(this.salt);
                        combined = combined.concat(this.ciphertext);
                        return enc.stringify(combined);
                    }
                    return enc.stringify(this.ciphertext);
                }
            };
            return cipherParams;
        },

        /// Decrypt with AES-256-GCM or AES-256-CBC
        ///   ciphertext: result from encrypt() or { ciphertext: WordArray }
        ///   key: 32-byte key (WordArray) or passphrase (string, uses EVP_BytesToKey)
        ///   options: { iv: WordArray|bytes, mode: 'GCM' (default) | 'CBC' }
        /// Returns: WordArray (decrypted plaintext)
        decrypt: function (ciphertext, key, options) {
            var ct = ciphertext.ciphertext || ciphertext;
            options = options || {};
            var mode = resolveMode(options.mode) || 'CBC';
            var ivLen = mode === 'CBC' ? 16 : 12;
            var keyLen = 32;

            // Extract IV, key, and salt from cipherParams object if available.
            // The EXPLICIT key argument wins over the embedded one — the old
            // `ciphertext.key || key` ignored the passed key, so a
            // wrong-password test "passed" vacuously (backlog line 155).
            var ctIv = ciphertext.iv || null;
            var ctKey = (key !== undefined && key !== null) ? key : (ciphertext.key || null);
            var ctSalt = ciphertext.salt || null;

            // Parse ciphertext: base64 string or WordArray to bytes
            var rawBytes;
            if (typeof ct === 'string') {
                rawBytes = bytesFromWordArray(CryptoJS.enc.Base64.parse(ct));
            } else {
                rawBytes = bytesFromWordArray(ct);
            }

            // Detect OpenSSL passphrase format: 'Salted__' prefix + 8-byte salt
            // If present, extract salt and use it for key derivation, then strip
            // the prefix for the actual ciphertext.
            var cipherBytes = rawBytes;
            if (typeof ctKey === 'string' && rawBytes.length >= 16) {
                // Check for 'Salted__' magic bytes at offset 0
                var saltedMagic = [83, 97, 108, 116, 101, 100, 95, 95]; // 'Salted__'
                var isSalted = true;
                for (var i = 0; i < 8; i++) {
                    if (rawBytes[i] !== saltedMagic[i]) {
                        isSalted = false;
                        break;
                    }
                }
                if (isSalted) {
                    ctSalt = wordArrayFromBytes(rawBytes.slice(8, 16));
                    cipherBytes = rawBytes.slice(16);
                }
            }

            // Handle key: string → passphrase, WordArray → direct
            var keyBytes;
            var keyWordArr;
            var ivBytes;

            if (typeof ctKey === 'string') {
                // Passphrase mode: derive key+IV from password + salt
                var saltBytes;
                if (ctSalt) {
                    saltBytes = bytesFromWordArray(ctSalt);
                } else {
                    throw new Error('Salt required for passphrase-based decryption. Use object from encrypt() or provide salt.');
                }
                var derived = deriveKeyAndIv(ctKey, saltBytes, keyLen, ivLen);
                keyWordArr = derived.key;
                keyBytes = bytesFromWordArray(derived.key);
                ivBytes = bytesFromWordArray(derived.iv);
            } else {
                keyWordArr = ctKey;
                keyBytes = bytesFromWordArray(ctKey);

                // Get IV from cipherParams or options
                if (ctIv) {
                    var ivWord = typeof ctIv === 'string'
                        ? CryptoJS.enc.Hex.parse(ctIv)
                        : ctIv;
                    ivBytes = bytesFromWordArray(ivWord);
                } else if (options.iv) {
                    var optIv = typeof options.iv === 'string'
                        ? CryptoJS.enc.Hex.parse(options.iv)
                        : options.iv;
                    ivBytes = bytesFromWordArray(optIv);
                } else {
                    throw new Error('IV required for AES decryption. Provide iv in options or use object from encrypt().');
                }
            }

            var result;
            if (mode === 'CBC') {
                if (typeof __tropel_native_aes_cbc_decrypt !== 'function') {
                    throw new Error('AES-CBC decrypt native function not available');
                }
                result = __tropel_native_aes_cbc_decrypt(keyBytes, ivBytes, cipherBytes);
            } else {
                if (typeof __tropel_native_aes_gcm_decrypt !== 'function') {
                    throw new Error('AES-GCM decrypt native function not available');
                }
                result = __tropel_native_aes_gcm_decrypt(keyBytes, ivBytes, cipherBytes);
            }

            // Handle null result (wrong key, auth failure, etc.)
            if (result === null || result === undefined) {
                throw new Error('AES decrypt failed: wrong key, corrupted data, or authentication failure');
            }

            return wordArrayFromBytes(result);
        }
    };

    // ── Enc/Dec helpers ──
    CryptoJS.enc.Base64url = {
        stringify: function (wordArray) {
            var base64 = CryptoJS.enc.Base64.stringify(wordArray);
            return base64.replace(/=+$/, '').replace(/\+/g, '-').replace(/\//g, '_');
        }
    };

    // ── Export ──
    if (typeof module !== 'undefined' && module.exports) {
        module.exports = CryptoJS;
    }
})();

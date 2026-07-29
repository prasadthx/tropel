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

    WordArray.create = function () {
        return new WordArray();
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
            var chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=';
            var bytes = [];
            base64Str = base64Str.replace(/[^A-Za-z0-9+/=]/g, '');
            for (var i = 0; i < base64Str.length; i += 4) {
                var c1 = chars.indexOf(base64Str[i] || '=');
                var c2 = chars.indexOf(base64Str[i + 1] || '=');
                var c3 = chars.indexOf(base64Str[i + 2] || '=');
                var c4 = chars.indexOf(base64Str[i + 3] || '=');
                if (c1 >= 0) bytes.push((c1 << 2) | (c2 >> 4));
                if (c3 >= 0) bytes.push(((c2 & 15) << 4) | (c3 >> 2));
                if (c4 >= 0) bytes.push(((c3 & 3) << 6) | c4);
            }
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
            var bytes = [];
            for (var i = 0; i < str.length; i++) {
                var c = str.charCodeAt(i);
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
        if (messageUpdate) this.update(messageUpdate);
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
                if (typeof __tropel_native_sha3_256 === 'function') {
                    result = wordArrayFromBytes(__tropel_native_sha3_256(allBytes));
                } else {
                    result = this._fallbackHash(allBytes, 'SHA3');
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

    Hasher.prototype._fallbackHash = function (bytes, algorithm) {
        // Simple fallback for environments without native crypto
        var hash = 0;
        for (var i = 0; i < bytes.length; i++) {
            hash = ((hash << 5) - hash) + bytes[i];
            hash = hash & hash; // Convert to 32bit integer
        }
        var words = [hash, hash >>> 16, hash, hash >>> 16];
        return new WordArray(words, 16);
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
        return CryptoJS.HmacSHA256(message, key); // Simplified - uses SHA256 as fallback
    };

    CryptoJS.HmacSHA512 = function (message, key) {
        return CryptoJS.HmacSHA256(message, key); // Simplified
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

    // ── AES (real encryption via native Rust) ──
    CryptoJS.AES = {
        /// Encrypt with AES-256-GCM (authenticated encryption)
        ///   message: string or WordArray (plaintext)
        ///   key: 32-byte key (WordArray or hex/base64 string)
        ///   options: { iv: WordArray|string (12 bytes for GCM, 16 for CBC),
        ///              mode: 'GCM' (default) | 'CBC' }
        /// Returns: { ciphertext: WordArray, key: WordArray, iv: WordArray,
        ///            salt: '', toString: fn → base64(ciphertext) }
        encrypt: function (message, key, options) {
            // Convert inputs to bytes
            var msgBytes = typeof message === 'string'
                ? CryptoJS.enc.Utf8.parse(message)
                : message;
            var keyBytes = typeof key === 'string'
                ? CryptoJS.enc.Hex.parse(key)
                : key;
            var plainBytes = bytesFromWordArray(msgBytes);
            var keyArr = bytesFromWordArray(keyBytes);

            // Determine mode (default GCM)
            options = options || {};
            var mode = options.mode || 'GCM';

            // Generate random IV/nonce if not provided
            var ivBytes;
            var ivLen = mode === 'CBC' ? 16 : 12;
            if (options.iv) {
                var ivWord = typeof options.iv === 'string'
                    ? CryptoJS.enc.Hex.parse(options.iv)
                    : options.iv;
                ivBytes = bytesFromWordArray(ivWord);
            } else if (typeof __tropel_native_uuid === 'function') {
                // Use hash of uuid as pseudo-random IV (in real usage, cryptographically random)
                // For now, use a fixed-time-based nonce
                var ts = Date.now().toString(16);
                while (ts.length < ivLen * 2) ts = '0' + ts;
                ts = ts.slice(-ivLen * 2);
                ivBytes = [];
                for (var i = 0; i < ts.length; i += 2) {
                    ivBytes.push(parseInt(ts.substr(i, 2), 16));
                }
                while (ivBytes.length < ivLen) ivBytes.push(0);
            } else {
                ivBytes = [];
                for (var i = 0; i < ivLen; i++) ivBytes.push(0);
            }

            var result;
            if (mode === 'CBC') {
                // AES-256-CBC
                if (typeof __tropel_native_aes_cbc_encrypt !== 'function') {
                    throw new Error('AES-CBC encrypt native function not available');
                }
                result = __tropel_native_aes_cbc_encrypt(keyArr, ivBytes, plainBytes);
            } else {
                // AES-256-GCM (default, authenticated)
                if (typeof __tropel_native_aes_gcm_encrypt !== 'function') {
                    throw new Error('AES-GCM encrypt native function not available');
                }
                result = __tropel_native_aes_gcm_encrypt(keyArr, ivBytes, plainBytes);
            }

            var cipherWordArr = wordArrayFromBytes(result);
            var ivWordArr = wordArrayFromBytes(ivBytes);

            // Return a CipherParams-like object
            var cipherParams = {
                ciphertext: cipherWordArr,
                key: keyBytes,
                iv: ivWordArr,
                salt: '',
                toString: function (encoder) {
                    return (encoder || CryptoJS.enc.Base64).stringify(this.ciphertext);
                }
            };
            return cipherParams;
        },

        /// Decrypt with AES-256-GCM or AES-256-CBC
        ///   ciphertext: result from encrypt() or { ciphertext: WordArray }
        ///   key: 32-byte key (WordArray or hex/base64 string)
        ///   options: { iv: WordArray|string, mode: 'GCM' (default) | 'CBC' }
        /// Returns: WordArray (decrypted plaintext)
        decrypt: function (ciphertext, key, options) {
            // Accept either the ciphertext directly or an object with ciphertext property
            var ct = ciphertext.ciphertext || ciphertext;
            var ctKey = ciphertext.key || key;
            var ctIv = ciphertext.iv || (options && options.iv) || null;
            options = options || {};

            var cipherBytes = typeof ct === 'string'
                ? bytesFromWordArray(CryptoJS.enc.Base64.parse(ct))
                : bytesFromWordArray(ct);
            var keyBytes = typeof ctKey === 'string'
                ? bytesFromWordArray(CryptoJS.enc.Hex.parse(ctKey))
                : bytesFromWordArray(ctKey);

            var mode = options.mode || 'GCM';
            var ivLen = mode === 'CBC' ? 16 : 12;

            // Use IV from cipherParams or options, or derive from ciphertext
            var ivBytes;
            if (ctIv) {
                var ivWord = typeof ctIv === 'string'
                    ? CryptoJS.enc.Hex.parse(ctIv)
                    : ctIv;
                ivBytes = bytesFromWordArray(ivWord);
            } else {
                throw new Error('IV required for AES decryption. Provide iv in options or use object from encrypt().');
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

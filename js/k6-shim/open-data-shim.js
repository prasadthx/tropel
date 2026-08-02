// ══════════════════════════════════════════════════════════════════
// k6 `open()` + `k6/data` SharedArray shim
//
// Native bridges (registered by K6DriverInstance):
//   __tropel_k6_open(path, mode)            -> file contents
//        mode "t" (default) -> UTF-8 string
//        mode "b"           -> base64-encoded bytes
//   __tropel_k6_shared_array_get(name)      -> cached JSON string, or "" if absent
//   __tropel_k6_shared_array_set(name, json)-> store the computed array once
//
// k6 semantics implemented here:
//   - `open()` reads a file relative to the script directory (resolved by the
//     native bridge) and returns its text; mode 'b' returns an ArrayBuffer.
//   - `new SharedArray(name, fn)` runs the factory ONCE per process (the first
//     VU context that constructs it serializes the result into the native
//     cache); every other VU context rebuilds the same read-only view from the
//     cached JSON without re-running the factory. The returned view is
//     read-only and array-like (length, index access, .at(), forEach, map,
//     iteration), matching k6/data.
// ══════════════════════════════════════════════════════════════════

// ── open(path, mode) ──
// NOTE: declared as top-level functions (NOT inside `if` guards) — QuickJS
// block-scopes function declarations, so a declaration inside `if (...) {}
// would be invisible to the proxy-install IIFE at the bottom of this file.
function open(path, mode) {
    if (typeof __tropel_k6_open !== 'function') {
        throw new Error('open() requires the native bridge __tropel_k6_open (k6 driver)');
    }
    var isBinary = mode === 'b';
    var result = __tropel_k6_open(String(path), isBinary ? 'b' : 't');
    if (!isBinary) {
        return result;
    }
    // Binary mode: the native side returns base64; decode into an ArrayBuffer.
    var binary = base64ToBytes(result);
    var buf = new ArrayBuffer(binary.length);
    var view = new Uint8Array(buf);
    for (var i = 0; i < binary.length; i++) {
        view[i] = binary[i];
    }
    return buf;
}

// ── k6/data SharedArray ──
function SharedArray(name, fn) {
    if (typeof name !== 'string' || name === '') {
        throw new Error('SharedArray: name must be a non-empty string');
    }
    if (typeof fn !== 'function') {
        throw new Error('SharedArray: constructor must receive a factory function');
    }

    var json = '';
    if (typeof __tropel_k6_shared_array_get === 'function') {
        json = __tropel_k6_shared_array_get(name) || '';
    }

    var data;
    if (json !== '') {
        data = JSON.parse(json);
    } else {
        data = fn();
        if (data === null || data === undefined || typeof data.length !== 'number') {
            throw new Error('SharedArray: factory function must return an array-like object');
        }
        // Normalize array-like (incl. non-indexed keys) to a plain array.
        var plain = [];
        for (var i = 0; i < data.length; i++) {
            plain.push(data[i]);
        }
        data = plain;
        if (typeof __tropel_k6_shared_array_set === 'function') {
            __tropel_k6_shared_array_set(name, JSON.stringify(data));
        }
    }

    return new SharedArrayView(data);
}

// Read-only, array-like view over the shared payload.
// k6 forbids writes to SharedArray data (it is shared across VUs), so every
// mutator throws.
function SharedArrayView(data) {
    this._data = data;
    this.length = data.length;
}

SharedArrayView.prototype.at = function (index) {
    var i = Number(index);
    if (i < 0) i += this.length;
    if (i < 0 || i >= this.length) return undefined;
    return this._data[i];
};

SharedArrayView.prototype.forEach = function (cb, thisArg) {
    for (var i = 0; i < this.length; i++) {
        cb.call(thisArg, this._data[i], i, this);
    }
};

SharedArrayView.prototype.map = function (cb, thisArg) {
    // k6 maps SharedArray to a plain array of results — return a plain array
    // (k6-like), NOT a SharedArrayView: a raw view would expose data only via
    // the Proxy, so `sa.map(...)[0]` would be undefined.
    var out = [];
    for (var i = 0; i < this.length; i++) {
        out.push(cb.call(thisArg, this._data[i], i, this));
    }
    return out;
};

SharedArrayView.prototype.find = function (cb, thisArg) {
    for (var i = 0; i < this.length; i++) {
        if (cb.call(thisArg, this._data[i], i, this)) return this._data[i];
    }
    return undefined;
};

SharedArrayView.prototype.findIndex = function (cb, thisArg) {
    for (var i = 0; i < this.length; i++) {
        if (cb.call(thisArg, this._data[i], i, this)) return i;
    }
    return -1;
};

SharedArrayView.prototype.includes = function (needle) {
    for (var i = 0; i < this.length; i++) {
        if (this._data[i] === needle) return true;
    }
    return false;
};

SharedArrayView.prototype.indexOf = function (needle) {
    for (var i = 0; i < this.length; i++) {
        if (this._data[i] === needle) return i;
    }
    return -1;
};

SharedArrayView.prototype.join = function (sep) {
    return this._data.join(sep === undefined ? ',' : sep);
};

SharedArrayView.prototype.slice = function (start, end) {
    var out = this._data.slice(start, end);
    var view = new SharedArrayView([]);
    view._data = out;
    view.length = out.length;
    return view;
};

function makeSharedIterator(nextFn) {
    var it = {
        next: nextFn
    };
    // Make the iterator itself iterable so `for...of sa.values()` and
    // `[...sa.values()]` work (k6 returns real iterators).
    it[Symbol.iterator] = function () { return this; };
    return it;
}

SharedArrayView.prototype.keys = function () {
    var i = 0;
    var self = this;
    return makeSharedIterator(function () {
        if (i < self.length) {
            return { value: i++, done: false };
        }
        return { value: undefined, done: true };
    });
};

SharedArrayView.prototype.values = function () {
    var i = 0;
    var self = this;
    return makeSharedIterator(function () {
        if (i < self.length) {
            return { value: self._data[i++], done: false };
        }
        return { value: undefined, done: true };
    });
};

// Symbol.iterator — defined non-enumerable so a SharedArrayView doesn't leak
// its iterator as an own enumerable key.
Object.defineProperty(SharedArrayView.prototype, Symbol.iterator, {
    value: function () {
        return this.values();
    },
    enumerable: false,
    writable: true,
    configurable: true
});

// ── Base64 helper for open(path, 'b') ──
function base64ToBytes(b64) {
    var CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
    var bytes = [];
    var i;
    // Keep the padding chars ('=') in `clean` so the byte-count guards below
    // match the ORIGINAL 4-char quantum exactly. Using the stripped length
    // makes the guards push one fewer byte for padded groups.
    var clean = String(b64).replace(/[^A-Za-z0-9+/=]/g, '');
    // Pad to a multiple of 4 if a trailing group is short (defensive; native
    // base64 always emits padding).
    while (clean.length % 4 !== 0) {
        clean += '=';
    }
    for (i = 0; i < clean.length; i += 4) {
        var c1 = clean.charAt(i);
        var c2 = clean.charAt(i + 1);
        var c3 = clean.charAt(i + 2);
        var c4 = clean.charAt(i + 3);
        var enc1 = CHARS.indexOf(c1);
        var enc2 = CHARS.indexOf(c2);
        // '=' marks a pad byte: skip the corresponding output byte.
        var enc3 = c3 === '=' ? -1 : CHARS.indexOf(c3);
        var enc4 = c4 === '=' ? -1 : CHARS.indexOf(c4);
        if (enc1 < 0 || enc2 < 0 || enc3 < -1 || enc4 < -1) {
            throw new Error('open(): invalid base64 input');
        }
        bytes.push((enc1 << 2) | (enc2 >> 4));
        if (enc3 >= 0) {
            bytes.push(((enc2 & 15) << 4) | (enc3 >> 2));
        }
        if (enc4 >= 0) {
            bytes.push(((enc3 & 3) << 6) | enc4);
        }
    }
    return bytes;
}

// Register a read-only index proxy so `shared[i]` works and writes throw.
// The base `SharedArray` (defined above, keyed on the native cache) is
// wrapped so every constructed view: numeric index access reads `_data`,
// property reads delegate to the view (methods are bound so `this` stays the
// view), `_data` is hidden (never expose the raw array — writes would bypass
// the read-only trap), and writes/deletes throw (k6: data is read-only).
(function installSharedArrayProxy() {
    if (typeof Proxy !== 'function') return;
    var orig = SharedArray;
    SharedArray = function (name, fn) {
        var view = orig(name, fn);
        return new Proxy(view, {
            get: function (target, prop) {
                if (prop === '_data') return undefined;
                if (typeof prop === 'string' && /^\d+$/.test(prop)) {
                    return target._data[Number(prop)];
                }
                var v = target[prop];
                return typeof v === 'function' ? v.bind(target) : v;
            },
            has: function (target, prop) {
                if (prop === '_data') return false;
                if (typeof prop === 'string' && /^\d+$/.test(prop)) {
                    var n = Number(prop);
                    return n >= 0 && n < target.length;
                }
                return prop in target;
            },
            set: function () {
                throw new Error('SharedArray: data is read-only');
            },
            deleteProperty: function () {
                throw new Error('SharedArray: data is read-only');
            }
        });
    };
    SharedArray.prototype = SharedArrayView.prototype;
})();

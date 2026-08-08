// ─── pm.* API for Tropel ─────────────────────────────────
// This JS glue layer provides the Postman pm.* API surface.
// It delegates heavy operations to native Rust functions.

// Global pm object
var pm = pm || {};

// ── pm.environment ──
pm.environment = {
    get: function (key) {
        // Delegates to native environment_get
        if (typeof __tropel_pm_environment_get === 'function') {
            return __tropel_pm_environment_get(key);
        }
        return null;
    },
    set: function (key, value) {
        if (typeof __tropel_pm_environment_set === 'function') {
            __tropel_pm_environment_set(key, String(value));
        }
    },
    unset: function (key) {
        if (typeof __tropel_pm_environment_unset === 'function') {
            __tropel_pm_environment_unset(key);
        }
    },
    clear: function () {
        if (typeof __tropel_pm_environment_clear === 'function') {
            __tropel_pm_environment_clear();
        }
    },
    // Backlog line 145: Postman's pm.environment exposes has()/toObject()/
    // replaceIn() alongside get/set/unset/clear.
    has: function (key) {
        if (typeof __tropel_pm_environment_has === 'function') {
            return __tropel_pm_environment_has(key);
        }
        return pm.environment.get(key) !== null;
    },
    toObject: function () {
        if (typeof __tropel_pm_environment_to_object === 'function') {
            return __tropel_pm_environment_to_object() || {};
        }
        return {};
    },
    replaceIn: function (text) {
        return pm.variables.replaceIn(text);
    }
};

// ── pm.collectionVariables ──
// Backlog line 145: one of the top-3 most-used pm.* members was entirely
// missing. Collection-scoped store; values round-trip JSON-encoded through
// the bridge like pm.variables (JSON.parse restores the correct type).
pm.collectionVariables = {
    get: function (key) {
        if (typeof __tropel_pm_collection_vars_get === 'function') {
            var raw = __tropel_pm_collection_vars_get(key);
            if (raw === null || raw === undefined) return undefined;
            try { return JSON.parse(raw); } catch (e) { return raw; }
        }
        return undefined;
    },
    set: function (key, value) {
        if (typeof __tropel_pm_collection_vars_set === 'function') {
            __tropel_pm_collection_vars_set(key, value === undefined ? '' : String(value));
        }
    },
    unset: function (key) {
        if (typeof __tropel_pm_collection_vars_unset === 'function') {
            __tropel_pm_collection_vars_unset(key);
        }
    },
    has: function (key) {
        if (typeof __tropel_pm_collection_vars_has === 'function') {
            return __tropel_pm_collection_vars_has(key);
        }
        return false;
    },
    toObject: function () {
        if (typeof __tropel_pm_collection_vars_to_object === 'function') {
            var map = __tropel_pm_collection_vars_to_object() || {};
            var out = {};
            for (var k in map) {
                if (map.hasOwnProperty(k)) {
                    try { out[k] = JSON.parse(map[k]); } catch (e) { out[k] = map[k]; }
                }
            }
            return out;
        }
        return {};
    },
    replaceIn: function (text) {
        return pm.variables.replaceIn(text);
    }
};

// ── pm.globals ──
// Backlog line 145: global-scoped variable store, lowest-precedence scope.
pm.globals = {
    get: function (key) {
        if (typeof __tropel_pm_globals_get === 'function') {
            var raw = __tropel_pm_globals_get(key);
            if (raw === null || raw === undefined) return undefined;
            try { return JSON.parse(raw); } catch (e) { return raw; }
        }
        return undefined;
    },
    set: function (key, value) {
        if (typeof __tropel_pm_globals_set === 'function') {
            __tropel_pm_globals_set(key, value === undefined ? '' : String(value));
        }
    },
    unset: function (key) {
        if (typeof __tropel_pm_globals_unset === 'function') {
            __tropel_pm_globals_unset(key);
        }
    },
    has: function (key) {
        if (typeof __tropel_pm_globals_has === 'function') {
            return __tropel_pm_globals_has(key);
        }
        return false;
    },
    toObject: function () {
        if (typeof __tropel_pm_globals_to_object === 'function') {
            var map = __tropel_pm_globals_to_object() || {};
            var out = {};
            for (var k in map) {
                if (map.hasOwnProperty(k)) {
                    try { out[k] = JSON.parse(map[k]); } catch (e) { out[k] = map[k]; }
                }
            }
            return out;
        }
        return {};
    },
    replaceIn: function (text) {
        return pm.variables.replaceIn(text);
    }
};

// ── pm.variables ──
pm.variables = {
    get: function (key) {
        if (typeof __tropel_pm_variables_get === 'function') {
            var raw = __tropel_pm_variables_get(key);
            if (raw === null || raw === undefined) return null;
            // Try JSON.parse — non-string values (objects, arrays, numbers,
            // booleans) come JSON-encoded from the bridge. If parse fails,
            // it's a plain string (return as-is).
            try { return JSON.parse(raw); }
            catch (e) { return raw; }
        }
        return null;
    },
    set: function (key, value) {
        if (typeof __tropel_pm_variables_set === 'function') {
            // Backlog line 146: every other store coerces String(value);
            // variables.set passed the RAW value, so a number like 42 hit
            // the bridge's strict String param and threw TypeError.
            __tropel_pm_variables_set(key, value === undefined ? '' : String(value));
        }
    },
    unset: function (key) {
        if (typeof __tropel_pm_variables_unset === 'function') {
            __tropel_pm_variables_unset(key);
        }
    },
    replaceIn: function (text) {
        // Simple variable replacement
        if (!text) return text;
        return text.replace(/\{\{([^}]+)\}\}/g, function (match, key) {
            var val = pm.variables.get(key.trim());
            return val !== null && val !== undefined ? String(val) : match;
        });
    }
};

// ── pm.response ──
// Backlog line 143: Postman exposes code/status/responseTime/headers/cookies
// as VALUE PROPERTIES, not functions. The old function-object form broke the
// two canonical idioms: `pm.expect(pm.response.code).to.eql(200)` compared a
// Function to 200 (never eql), and `pm.response.headers.get('X')` threw a
// TypeError (headers was a function). Only text()/json() are methods in
// Postman; the rest are values.
//
// The native __tropel_pm_response_* bridges are registered LAZILY (first
// iteration, after this shim bootstraps), so these are GETTERS that re-fetch
// the bridge on every read — same pattern as exec.js. That keeps the read
// shape a plain number/string/object exactly like Postman while staying live.
pm.response = {};

Object.defineProperty(pm.response, 'code', {
    get: function () {
        if (typeof globalThis !== 'undefined' && typeof globalThis.__tropel_pm_response_code === 'function') {
            return globalThis.__tropel_pm_response_code();
        }
        return 0;
    },
    enumerable: true,
    configurable: true
});

Object.defineProperty(pm.response, 'status', {
    get: function () {
        if (typeof globalThis !== 'undefined' && typeof globalThis.__tropel_pm_response_status === 'function') {
            return globalThis.__tropel_pm_response_status();
        }
        return '';
    },
    enumerable: true,
    configurable: true
});

Object.defineProperty(pm.response, 'responseTime', {
    get: function () {
        if (typeof globalThis !== 'undefined' && typeof globalThis.__tropel_pm_response_time === 'function') {
            return globalThis.__tropel_pm_response_time();
        }
        return 0;
    },
    enumerable: true,
    configurable: true
});

// Postman's pm.response.headers is a Headers object: pm.response.headers.get('X')
// is the canonical idiom. Returns a fresh object per read (the underlying map
// can change between iterations; the get() delegate is case-insensitive via the
// __tropel_pm_response_header bridge, matching Postman).
Object.defineProperty(pm.response, 'headers', {
    get: function () {
        var map = {};
        if (typeof globalThis !== 'undefined' && typeof globalThis.__tropel_pm_response_headers === 'function') {
            map = globalThis.__tropel_pm_response_headers() || {};
        }
        return {
            get: function (key) {
                if (typeof globalThis !== 'undefined' && typeof globalThis.__tropel_pm_response_header === 'function') {
                    var v = globalThis.__tropel_pm_response_header(key);
                    return v !== undefined && v !== null ? v : undefined;
                }
                return undefined;
            },
            all: function () { return map; },
            toObject: function () { return map; },
            count: function () {
                var n = 0;
                for (var k in map) { if (map.hasOwnProperty(k)) n++; }
                return n;
            }
        };
    },
    enumerable: true,
    configurable: true
});

// Postman's pm.response.cookies is a list of Cookie objects; scripts also use
// pm.response.cookies.get('name'). Returns an array of {name, value} objects
// with a get() convenience, backed by the name→value bridge map.
Object.defineProperty(pm.response, 'cookies', {
    get: function () {
        var map = {};
        if (typeof globalThis !== 'undefined' && typeof globalThis.__tropel_pm_response_cookies === 'function') {
            map = globalThis.__tropel_pm_response_cookies() || {};
        }
        var list = [];
        for (var ck in map) {
            if (map.hasOwnProperty(ck)) {
                list.push({ name: ck, value: map[ck] });
            }
        }
        list.get = function (key) {
            for (var i = 0; i < list.length; i++) {
                if (list[i].name === key) return list[i];
            }
            return undefined;
        };
        return list;
    },
    enumerable: true,
    configurable: true
});

pm.response.text = function () {
    if (typeof __tropel_pm_response_body === 'function') {
        return __tropel_pm_response_body();
    }
    return '';
};

pm.response.json = function () {
    if (typeof __tropel_pm_response_json === 'function') {
        var raw = __tropel_pm_response_json();
        if (raw) {
            return JSON.parse(raw);
        }
        throw new Error('pm.response.json() — response body is not valid JSON or no response available');
    }
    throw new Error('pm.response.json() is not available in this runtime');
};

pm.response.header = function (key) {
    if (typeof __tropel_pm_response_header === 'function') {
        return __tropel_pm_response_header(key);
    }
    return null;
};

// Backlog line 145: pm.response.to.be.* — Postman's chainable response
// assertions (chai-postman). Each getter/method THROWS on failure so it can
// be used inside pm.test() (the throw fails the single check).
function assertStatusClass(classCode, label) {
    var c = pm.response.code;
    var lo = classCode * 100;
    var hi = lo + 99;
    if (c < lo || c > hi) {
        throw new Error('expected response to be ' + label + ' (' + lo + '-' + hi + '), got ' + c);
    }
}

pm.response.to = {
    be: {
        get success() { assertStatusClass(2, 'success'); },
        get ok() { assertStatusClass(2, 'ok'); },
        get redirection() { assertStatusClass(3, 'redirection'); },
        get clientError() { assertStatusClass(4, 'clientError'); },
        get serverError() { assertStatusClass(5, 'serverError'); },
        get error() {
            var c = pm.response.code;
            if (c < 400) {
                throw new Error('expected response to be an error (>=400), got ' + c);
            }
        },
        get info() { assertStatusClass(1, 'info'); },
        json: function () {
            // Postman parity: to.be.json passes when the body parses as JSON.
            // Content-type is informational — a text/plain body that parses
            // still counts (Postman's chai-postman checks the body first).
            pm.response.json(); // throws on invalid JSON body
        },
        html: function () {
            var ct = String(pm.response.headers.get('content-type') || '').toLowerCase();
            if (ct.indexOf('html') === -1) {
                throw new Error('expected response to be HTML, content-type is ' + ct);
            }
        },
        text: function () {
            var ct = String(pm.response.headers.get('content-type') || '').toLowerCase();
            if (ct.indexOf('text') === -1 && ct.indexOf('json') === -1 && ct.indexOf('xml') === -1) {
                throw new Error('expected response to be text, content-type is ' + ct);
            }
        }
    },
    have: {
        status: function (code) {
            // Backlog line 143: pm.response.code is a VALUE now.
            var actual = pm.response.code;
            if (actual !== code) {
                throw new Error(
                    'expected response to have status ' + code + ' but got ' + actual
                );
            }
        },
        header: function (key, value) {
            var actual = pm.response.header(key);
            if (value === undefined) {
                if (actual === null || actual === undefined) {
                    throw new Error('expected response to have header ' + key);
                }
            } else if (String(actual) !== String(value)) {
                throw new Error(
                    'expected header ' + key + ' to be ' + String(value) + ', got ' + String(actual)
                );
            }
        },
        body: function (substring) {
            var body = pm.response.text();
            if (body.indexOf(substring) === -1) {
                throw new Error('expected response body to contain ' + shortJson(substring));
            }
        },
        jsonBody: function (expected) {
            var body = pm.response.json();
            if (!deepEqual(body, expected)) {
                throw new Error('expected response JSON body to match');
            }
        }
    }
};

// ── pm.test ──
pm.test = function (name, fn) {
    try {
        var result = fn();
        var passed = result !== false;
        if (typeof __tropel_pm_test === 'function') {
            // 3rd arg (tags) always passed — rquickjs enforces arity, so a
            // 2-arg call against the 3-param bridge would throw (line 149).
            __tropel_pm_test(name, passed, '');
        }
        return passed;
    } catch (e) {
        if (typeof __tropel_pm_test === 'function') {
            __tropel_pm_test(name + ' (error)', false, '');
        }
        console.error('pm.test error:', e);
        return false;
    }
};

// Backlog line 145: pm.test.skip(name, fn) marks a test skipped WITHOUT
// running it. Skipped tests are not pass/fail checks.
pm.test.skip = function (name) {
    if (typeof __tropel_pm_test_skip === 'function') {
        __tropel_pm_test_skip(name);
    }
};

// ── pm.expect (wraps chai expect if available, else simple assert) ──
//
// Assertions THROW on failure and never auto-record a check. Postman/chai
// semantics: only `pm.test(name, fn)` records a check — wrapping an expect
// must produce exactly ONE check named by the pm.test call. Auto-recording
// here double-counted every pm.test-wrapped assertion, and embedding
// JSON.stringify(actual) in the recorded name made `pm.expect(pm.response.json())`
// stamp the ENTIRE response body as a metric tag (unbounded cardinality at
// 10 k iterations). When used inside pm.test, the throw is caught and the
// single check fails with the pm.test name. Standalone (outside pm.test)
// expects surface as a script error and do NOT produce a checks-metric
// entry — matching Postman, where check metrics come only from pm.test()
// and check().
//
// Backlog §1: unimplemented assertion PROPERTIES (e.g. `pm.expect(false)
// .to.be.true`, `pm.expect(null).to.exist`) used to read as `undefined` and
// pm.test recorded GREEN — a silent pass. Every chain object is now wrapped
// in a Proxy whose `get` trap THROWS on unknown assertion names, so a typo'd
// or unimplemented assertion fails the check instead of passing silently.
// The common chai-postman property assertions are implemented below.
function guardChain(target) {
    return new Proxy(target, {
        get: function (t, prop, receiver) {
            if (
                typeof prop === 'symbol' ||
                prop === 'then' || prop === 'toJSON' || prop === 'inspect'
            ) {
                return Reflect.get(t, prop, receiver);
            }
            if (prop in t) {
                var v = Reflect.get(t, prop, receiver);
                // Recurse into nested chain objects so `to.be.true` etc. are
                // guarded one level deeper.
                return v !== null && typeof v === 'object' ? guardChain(v) : v;
            }
            throw new Error("unknown assertion property '" + String(prop) + "'");
        }
    });
}

// chai-postman property assertions. Each getter THROWS on mismatch so the
// enclosing pm.test records a failed check (a bare boolean would leave the
// callback's `undefined` statement result recorded as passed).
function addPropAssertions(chain, actual, negated) {
    var checks = {
        true: function () { return actual === true; },
        false: function () { return actual === false; },
        null: function () { return actual === null; },
        undefined: function () { return actual === undefined; },
        ok: function () { return !!actual; },
        empty: function () {
            if (typeof actual === 'string' || Array.isArray(actual)) return actual.length === 0;
            if (actual !== null && typeof actual === 'object') return Object.keys(actual).length === 0;
            return false;
        },
        exist: function () { return actual !== null && actual !== undefined; },
        NaN: function () { return typeof actual === 'number' && isNaN(actual); },
        finite: function () { return typeof actual === 'number' && isFinite(actual); }
    };
    Object.keys(checks).forEach(function (name) {
        Object.defineProperty(chain, name, {
            get: function () {
                // Negated chain: the positive holding means the assertion
                // FAILS — throw when the check passes, not when it fails.
                var holds = checks[name]();
                var passed = negated ? !holds : holds;
                if (!passed) {
                    var label = name === 'ok' ? 'be truthy' : name === 'exist' ? 'exist' : 'be ' + name;
                    throw new Error(
                        'expected ' + shortJson(actual) + (negated ? ' not' : '') + ' to ' + label
                    );
                }
                return this;
            },
            enumerable: true
        });
    });
}

pm.expect = function (actual) {
    var chain = {
        to: {
            // Backlog line 144: chai semantics — .eql is DEEP equality,
            // .equal is strict (===). The old mapping was inverted: eql used
            // === so `pm.expect(pm.response.json()).to.eql({...})` could
            // never pass against a freshly-parsed body, and equal delegated
            // to eql.
            eql: function (expected) {
                if (!deepEqual(actual, expected)) {
                    throw new Error(
                        'expected ' + shortJson(actual) + ' to eql ' + shortJson(expected)
                    );
                }
            },
            equal: function (expected) {
                if (actual !== expected) {
                    throw new Error(
                        'expected ' + shortJson(actual) + ' to equal ' + shortJson(expected)
                    );
                }
            },
            include: function (expected) {
                if (String(actual).indexOf(String(expected)) === -1) {
                    throw new Error('expected value to include ' + shortJson(expected));
                }
            },
            be: {
                // chai-style type assertions: pm.expect(x).to.be.an('array')
                an: function (type) {
                    if (typeOf(actual) !== type) {
                        throw new Error(
                            'expected value to be an ' + type + ', got ' + typeOf(actual)
                        );
                    }
                },
                a: function (type) {
                    return this.an(type);
                }
            },
            have: {
                property: function (prop, value) {
                    var obj = actual;
                    var has = obj && (prop in obj);
                    var passed = has;
                    if (has && value !== undefined) {
                        passed = obj[prop] === value;
                    }
                    if (!passed) {
                        throw new Error('expected value to have property ' + prop);
                    }
                },
                status: function (code) {
                    // Must THROW on mismatch (Postman/chai semantics) — a
                    // boolean return makes `pm.test` treat the callback's
                    // `undefined` statement result as passed.
                    // Backlog line 143: pm.response.code is a VALUE now.
                    var actual = pm.response.code;
                    if (actual !== code) {
                        throw new Error('expected response to have status ' + code + ' but got ' + actual);
                    }
                },
                header: function (key, value) {
                    var header = pm.response.header(key);
                    if (header !== value) {
                        throw new Error('expected header ' + key + ' to be ' + shortJson(value) + ', got ' + shortJson(header));
                    }
                },
                jsonBody: function (expected) {
                    var body = pm.response.json();
                    // Backlog line 144: JSON.stringify comparison was
                    // key-order sensitive — the same failure mode as the old
                    // eql. Deep-compare instead.
                    if (!deepEqual(body, expected)) {
                        throw new Error('expected response body to match');
                    }
                }
            },
            match: function (regex) {
                if (!regex.test(String(actual))) {
                    throw new Error('expected value to match ' + regex);
                }
            }
        },
        not: {
            to: {
                eql: function (expected) {
                    if (deepEqual(actual, expected)) {
                        throw new Error(
                            'expected ' + shortJson(actual) + ' not to eql ' + shortJson(expected)
                        );
                    }
                },
                equal: function (expected) {
                    if (actual === expected) {
                        throw new Error(
                            'expected ' + shortJson(actual) + ' not to equal ' + shortJson(expected)
                        );
                    }
                },
                be: {
                    // Negated property/type assertions:
                    // `pm.expect(x).not.to.be.true` must THROW when the
                    // positive holds (the guard applies here too).
                    an: function (type) {
                        if (typeOf(actual) === type) {
                            throw new Error('expected value not to be an ' + type);
                        }
                    },
                    a: function (type) {
                        return this.an(type);
                    }
                }
            }
        }
    };
    // Install the property assertions on the chain levels where chai-postman
    // exposes them, then guard the whole chain so ANY unknown name throws.
    addPropAssertions(chain.to, actual);
    addPropAssertions(chain.to.be, actual);
    // Negated chain: both `not.to.true` and the canonical `not.to.be.true`
    // must be guarded (the getters live on the `be` object, which is a
    // SEPARATE literal from `not.to`).
    addPropAssertions(chain.not.to, actual, true);
    addPropAssertions(chain.not.to.be, actual, true);
    return guardChain(chain);
};

// ── pm.request ──
// Backlog line 145: prerequest scripts could not add an auth header or sign a
// request because pm.request didn't exist AND the runner rebuilt the wire
// request from the static collection item, discarding mutations. The runner
// now reads PmState.request (seeded from item.request before prerequest) when
// building the outgoing request, so mutations made here go out on the wire.
// Live getters/setters delegate to the __tropel_pm_request_* bridges
// (registered lazily, like exec.js).
pm.request = {};

Object.defineProperty(pm.request, 'url', {
    get: function () {
        if (typeof __tropel_pm_request_url === 'function') return __tropel_pm_request_url();
        return '';
    },
    set: function (url) {
        if (typeof __tropel_pm_request_url_set === 'function') __tropel_pm_request_url_set(String(url));
    },
    enumerable: true,
    configurable: true
});

Object.defineProperty(pm.request, 'method', {
    get: function () {
        if (typeof __tropel_pm_request_method === 'function') return __tropel_pm_request_method();
        return 'GET';
    },
    set: function (method) {
        if (typeof __tropel_pm_request_method_set === 'function') __tropel_pm_request_method_set(String(method));
    },
    enumerable: true,
    configurable: true
});

// Postman's pm.request.headers is a HeaderList: .add({key,value}) is THE
// canonical prerequest idiom for attaching an Authorization header.
pm.request.headers = {
    add: function (header) {
        if (!header || header.key === undefined || header.key === null) return;
        if (typeof __tropel_pm_request_header_set === 'function') {
            __tropel_pm_request_header_set(String(header.key), header.value == null ? '' : String(header.value));
        }
    },
    upsert: function (header) {
        pm.request.headers.add(header);
    },
    get: function (key) {
        if (typeof __tropel_pm_request_header_get === 'function') {
            var v = __tropel_pm_request_header_get(key);
            return v !== null && v !== undefined ? v : undefined;
        }
        return undefined;
    },
    remove: function (key) {
        if (typeof __tropel_pm_request_header_unset === 'function') {
            __tropel_pm_request_header_unset(key);
        }
    },
    all: function () {
        if (typeof __tropel_pm_request_headers === 'function') {
            var map = __tropel_pm_request_headers() || {};
            var arr = [];
            for (var k in map) {
                if (map.hasOwnProperty(k)) arr.push({ key: k, value: map[k] });
            }
            return arr;
        }
        return [];
    },
    each: function (cb) {
        var all = pm.request.headers.all();
        for (var i = 0; i < all.length; i++) cb(all[i]);
    },
    toObject: function () {
        if (typeof __tropel_pm_request_headers === 'function') {
            return __tropel_pm_request_headers() || {};
        }
        return {};
    }
};

// pm.request.body — raw text form backed by the request body bridge.
// Postman's canonical idiom is `pm.request.body.raw = '...'`, which requires
// a STABLE object whose `raw` accessor is bridge-wired (a getter returning a
// fresh object each read would silently swallow the mutation).
var _pmRequestBody = { mode: 'raw' };
Object.defineProperty(_pmRequestBody, 'raw', {
    get: function () {
        if (typeof __tropel_pm_request_body === 'function') {
            var b = __tropel_pm_request_body();
            if (b !== null && b !== undefined) return b;
        }
        return '';
    },
    set: function (raw) {
        if (typeof __tropel_pm_request_body_set === 'function') {
            __tropel_pm_request_body_set(raw == null ? '' : String(raw));
        }
    },
    enumerable: true,
    configurable: true
});
Object.defineProperty(pm.request, 'body', {
    get: function () { return _pmRequestBody; },
    set: function (body) {
        // Accept a plain string OR an object {mode, raw} — both are common
        // Postman spellings.
        var raw = body == null ? '' : (typeof body === 'object' ? (body.raw != null ? String(body.raw) : '') : String(body));
        if (typeof __tropel_pm_request_body_set === 'function') {
            __tropel_pm_request_body_set(raw);
        }
        _pmRequestBody.mode = body && body.mode ? body.mode : 'raw';
    },
    enumerable: true,
    configurable: true
});

// pm.request.auth — accepts the tagged AuthConfig JSON shape so a prerequest
// script can sign the outgoing request (the primary purpose of pm.request).
// Backed by a stored copy so reads echo the last-set config (a collection
// may inspect it in a test script).
var _pmRequestAuth = null;
Object.defineProperty(pm.request, 'auth', {
    get: function () { return _pmRequestAuth; },
    set: function (auth) {
        _pmRequestAuth = auth;
        // JSON.stringify(undefined) is not a string — skip the bridge (and
        // the Rust parse) when clearing auth; use {type:'noauth'} to clear
        // explicitly, exactly like Postman.
        if (auth === undefined || auth === null) return;
        if (typeof __tropel_pm_request_auth_set === 'function') {
            __tropel_pm_request_auth_set(JSON.stringify(auth));
        }
    },
    enumerable: true,
    configurable: true
});

// ── pm.cookies ──
// Backlog line 145: Postman's pm.cookies reads the cookie jar for the current
// domain. In a headless load runner the closest proxy is the response's
// Set-Cookie map (__tropel_pm_response_cookies).
pm.cookies = {
    get: function (name) {
        var jar = pm.cookies.toObject();
        return jar[name];
    },
    has: function (name) {
        return pm.cookies.toObject().hasOwnProperty(name);
    },
    toObject: function () {
        if (typeof __tropel_pm_response_cookies === 'function') {
            return __tropel_pm_response_cookies() || {};
        }
        return {};
    },
    list: function () {
        var jar = pm.cookies.toObject();
        var arr = [];
        for (var k in jar) {
            if (jar.hasOwnProperty(k)) arr.push({ name: k, value: jar[k] });
        }
        return arr;
    }
};

// Backlog line 145: pm.expect.fail — chai's static fail: always throws.
// Used by collections to force a failure inside a pm.test block.
pm.expect.fail = function (message) {
    throw new Error(message || 'expect.fail() called');
};

// Truncate a value for ERROR MESSAGES only (never for metric tags). Keeps
// failure logs readable when the actual value is a large response body.
function shortJson(v) {
    var s;
    try {
        s = JSON.stringify(v);
    } catch (e) {
        s = String(v);
    }
    if (s && s.length > 120) {
        return s.slice(0, 117) + '...';
    }
    return s;
}

// Proper JS deep-equality (chai .eql semantics): handles NaN, undefined,
// key order, nested arrays/objects. Mirrors the jsDeepEqual in
// js/chai/chai-shim.js — the backlog fix for .eql must not depend on chai
// being loaded first (pm.js is bundled standalone).
function deepEqual(a, b) {
    if (a === b) return true;
    if (typeof a === 'number' && typeof b === 'number' && isNaN(a) && isNaN(b)) return true;
    if (a === null || b === null || a === undefined || b === undefined) return a === b;
    if (typeof a !== typeof b) return false;
    if (Array.isArray(a)) {
        if (!Array.isArray(b) || a.length !== b.length) return false;
        for (var i = 0; i < a.length; i++) {
            if (!deepEqual(a[i], b[i])) return false;
        }
        return true;
    }
    if (typeof a === 'object') {
        if (Array.isArray(b) || b === null || b === undefined) return false;
        var keysA = Object.keys(a).sort();
        var keysB = Object.keys(b).sort();
        if (keysA.length !== keysB.length) return false;
        for (var j = 0; j < keysA.length; j++) {
            if (keysA[j] !== keysB[j]) return false;
            if (!deepEqual(a[keysA[j]], b[keysB[j]])) return false;
        }
        return true;
    }
    return a === b;
}

// ── pm.iterationData ──
pm.iterationData = {
    get: function (key) {
        if (typeof __tropel_pm_iteration_data_get === 'function') {
            var raw = __tropel_pm_iteration_data_get(key);
            if (raw === null || raw === undefined) return null;
            // Values come JSON-encoded from the bridge — parse to restore type
            try { return JSON.parse(raw); }
            catch (e) { return raw; }
        }
        return null;
    }
};

// Chai-style type names: 'array', 'object', 'string', 'number', 'boolean',
// 'null', 'undefined', 'function'.
function typeOf(v) {
    if (v === null) return 'null';
    if (Array.isArray(v)) return 'array';
    return typeof v;
}

function buildMultipartBody(formdata) {
    var boundary = '----TropelFormBoundary' + Math.random().toString(36).slice(2);
    var parts = [];
    for (var i = 0; i < formdata.length; i++) {
        var fp = formdata[i];
        if (!fp || !fp.key) continue;
        var value = fp.value == null ? '' : fp.value;
        if (typeof value !== 'string') {
            try {
                value = JSON.stringify(value);
            } catch (e) {
                value = String(value);
            }
        }
        parts.push('--' + boundary + '\r\n');
        parts.push('Content-Disposition: form-data; name="' + escapeMultipartFieldName(fp.key) + '"\r\n\r\n');
        parts.push(value + '\r\n');
    }
    parts.push('--' + boundary + '--\r\n');
    return {
        body: parts.join(''),
        contentType: 'multipart/form-data; boundary=' + boundary
    };
}

function escapeMultipartFieldName(name) {
    return String(name).replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}

// ── pm.sendRequest (for chaining requests within a test) ──
// Supports the auth-token-fetch pattern: send a request to obtain
// an auth token, then store it via pm.variables.set().
// Handles both Postman-style options and simple string URLs.
pm.sendRequest = function (options, callback) {
    // Delegate to native implementation
    if (typeof __tropel_pm_send_request === 'function') {
        // Normalize options
        var url = '';
        var method = 'GET';
        var headers = {};
        var body = '';
        var timeout = 30000; // 30s default

        if (typeof options === 'string') {
            // Simple string URL
            url = options;
        } else if (options && typeof options === 'object') {
            // Postman-style request object
            url = options.url || '';
            method = options.method || 'GET';
            timeout = options.timeout || 30000;

            // Handle Postman-style headers: array of {key, value} or plain object
            if (options.header && Array.isArray(options.header)) {
                // Postman array format: [{key: "Content-Type", value: "application/json"}]
                headers = {};
                for (var i = 0; i < options.header.length; i++) {
                    var h = options.header[i];
                    if (h && h.key) {
                        headers[h.key] = h.value !== undefined ? h.value : '';
                    }
                }
            } else if (options.headers) {
                // Plain object or Postman header object — COPY it: the
                // formdata branch below stamps Content-Type with the
                // generated boundary, and mutating the caller's object
                // (options are often hoisted to module scope) would leak
                // state across iterations.
                headers = {};
                if (!Array.isArray(options.headers)) {
                    for (var hk in options.headers) {
                        if (options.headers.hasOwnProperty(hk)) headers[hk] = options.headers[hk];
                    }
                }
            }

            // Handle Postman-style body
            if (options.body) {
                if (typeof options.body === 'string') {
                    body = options.body;
                } else if (options.body.mode) {
                    // Postman body object: {mode: "raw", raw: "..."}
                    switch (options.body.mode) {
                        case 'raw':
                            body = options.body.raw || '';
                            break;
                        case 'urlencoded':
                            if (options.body.urlencoded && Array.isArray(options.body.urlencoded)) {
                                var pairs = [];
                                for (var j = 0; j < options.body.urlencoded.length; j++) {
                                    var param = options.body.urlencoded[j];
                                    if (param && param.key) {
                                        pairs.push(encodeURIComponent(param.key) + '=' + encodeURIComponent(param.value || ''));
                                    }
                                }
                                body = pairs.join('&');
                            }
                            break;
                        case 'formdata':
                            if (options.body.formdata && Array.isArray(options.body.formdata)) {
                                var multipart = buildMultipartBody(options.body.formdata);
                                body = multipart.body;
                                // ALWAYS stamp the generated boundary — the
                                // old `!headers['Content-Type']` guard was
                                // false whenever the caller declared
                                // multipart/form-data, so the boundary never
                                // reached the request. `headers` is a copy of
                                // options.headers (never the caller's object).
                                // Drop any lowercase variant so only ONE
                                // Content-Type (with the boundary) is sent.
                                delete headers['content-type'];
                                headers['Content-Type'] = multipart.contentType;
                            }
                            break;
                        case 'graphql':
                            if (options.body.query) {
                                body = JSON.stringify({query: options.body.query, variables: options.body.variables || {}});
                            }
                            break;
                        default:
                            body = options.body.raw || JSON.stringify(options.body);
                    }
                } else {
                    // Plain object body — JSON encode
                    try {
                        body = JSON.stringify(options.body);
                    } catch (e) {
                        body = String(options.body);
                    }
                }
            }
        }

        var resultJson = __tropel_pm_send_request(
            method.toUpperCase(),
            url,
            JSON.stringify(headers),
            typeof body === 'string' ? body : JSON.stringify(body),
            timeout,
            // k6-style responseType — Postman sendRequest has no such field,
            // default to "text" (bridge requires the 6th arg)
            (options && options.responseType) || 'text'
        );

        // Fire callback with the response
        if (typeof callback === 'function') {
            try {
                var result = JSON.parse(resultJson);
                // Backlog line 147: transport failures (DNS/conn refused/timeout)
                // used to arrive as callback(null, {code: 0}) — a "success" to
                // the universal `if (err)` guard, so auth-token-fetch retry logic
                // never fired. The bridge now stamps an `error` field; surface it
                // as the first (err) argument so the canonical guard works.
                if (result.error) {
                    callback(new Error(result.error), null);
                    return;
                }
                callback(null, {
                    code: result.code || 0,
                    status: result.statusText || '',
                    text: function () { return result.body || ''; },
                    json: function () {
                        try { return JSON.parse(result.body || '{}'); }
                        catch (e) { return null; }
                    },
                    headers: function () { return result.headers || {}; },
                    responseTime: result.responseTime || 0
                });
            } catch (e) {
                callback(new Error('Failed to parse sendRequest response: ' + e.message), null);
            }
        }
        return;
    }

    // No native function available - throw a clear error
    throw new Error('pm.sendRequest is not available in this runtime (native __tropel_pm_send_request not found)');
};

// ── pm.execution ──
// Backlog line 145: postman.setNextRequest is the LEGACY flow-control global
// that nearly all real collections use (pm.execution.setNextRequest is the
// newer spelling). Both delegate to the same bridge.
var postman = postman || {};
postman.setNextRequest = function (requestName) {
    pm.execution.setNextRequest(requestName);
};

pm.execution = {
    setNextRequest: function (requestName) {
        if (typeof __tropel_pm_set_next_request === 'function') {
            __tropel_pm_set_next_request(requestName);
        }
    },
    skipRequest: function () {
        // Backlog line 146: skipRequest must skip ONLY the current request
        // and move to the next item. Routing it through setNextRequest(null)
        // (a) threw — null into a strict String param — and (b) inherited
        // setNextRequest's "stop the whole run" semantics. Use the dedicated
        // __tropel_pm_skip_request bridge instead.
        if (typeof __tropel_pm_skip_request === 'function') {
            __tropel_pm_skip_request();
        }
    },
    stopOnError: function () {
        if (typeof __tropel_pm_skip_tests === 'function') {
            __tropel_pm_skip_tests();
        }
    }
};

// ── pm.info ──
pm.info = {
    eventName: 'test',
    iteration: 0,
    iterationCount: 1,
    requestName: '',
    requestId: ''
};

// ── pm.metrics (custom metrics) ──
pm.metrics = {
    // Add a value to a custom metric (creates it if it doesn't exist).
    // Metric types: 'counter', 'gauge', 'rate', 'trend' (default: 'trend')
    add: function (name, value, metricType) {
        if (typeof __tropel_pm_metrics_add === 'function') {
            var type = metricType || 'trend';
            __tropel_pm_metrics_add(name, Number(value), type);
        }
    },
    // Get the current value of a custom metric.
    get: function (name) {
        if (typeof __tropel_pm_metrics_get === 'function') {
            return __tropel_pm_metrics_get(name);
        }
        return null;
    },
    // Convenience: add a counter value (always increments by the value).
    counter: function (name, value) {
        pm.metrics.add(name, value, 'counter');
    },
    // Convenience: set a gauge value (records the current value).
    gauge: function (name, value) {
        pm.metrics.add(name, value, 'gauge');
    },
    // Convenience: add a rate event (value = 1.0 for success, 0.0 for failure).
    rate: function (name, value) {
        pm.metrics.add(name, value, 'rate');
    },
    // Convenience: add a trend sample (records the value for percentile analysis).
    trend: function (name, value) {
        pm.metrics.add(name, value, 'trend');
    }
};

// ── group(name, fn) — k6-style grouping ──
// Wraps a block of code in a named group. Emits group_duration
// metric (Trend) showing how long the group took to execute.
// Supports nesting (groups within groups).
function group(name, fn) {
    if (typeof __tropel_pm_group_start === 'function') {
        __tropel_pm_group_start(name);
        var startTime = Date.now();
        try {
            if (typeof fn === 'function') {
                return fn();
            }
        } finally {
            var duration = Date.now() - startTime;
            __tropel_pm_group_end(name, duration);
        }
    } else {
        // No native group support — run the function directly
        if (typeof fn === 'function') {
            return fn();
        }
    }
}

// ── check(val, conds, tags) — k6-style checks ──
// Evaluates conditions against a value. Each condition is a named
// predicate (function) or a boolean constant (ToBoolean-coerced, k6
// parity — NOT val === condition). Records each as a checks Rate metric
// (pass/fail) with the RAW name as the `check` tag (no "check " prefix,
// k6 parity) plus the optional 3rd-arg tags. Returns true if ALL checks
// pass. Backlog line 149: check(1, null)/check(1, 'x') used to return
// true (nonsense-as-success) — k6 throws for a null/non-object conds;
// and a throwing predicate must record a failed check THEN propagate
// (k6 fails the iteration with that error).
function check(val, conds, tags) {
    if (conds === null || conds === undefined || typeof conds !== 'object') {
        throw new TypeError('check() requires an object as its second argument');
    }
    var allPassed = true;
    var tagsJson = '';
    if (tags && typeof tags === 'object') {
        try { tagsJson = JSON.stringify(tags); } catch (e) { tagsJson = ''; }
    }
    var names = Object.keys(conds);
    for (var i = 0; i < names.length; i++) {
        var name = names[i];
        var condition = conds[name];
        var passed = false;

        if (typeof condition === 'function') {
            // Predicate function — call with the value. On throw, record
            // the failed check, then let the error propagate (k6 parity).
            try {
                passed = !!condition(val);
            } catch (e) {
                if (typeof __tropel_pm_test === 'function') {
                    __tropel_pm_test(name, false, tagsJson);
                }
                throw e;
            }
        } else {
            // Non-function condition: boolean constant (k6 ToBoolean).
            passed = !!condition;
        }

        // Record the check pass/fail via the existing test bridge — raw
        // name (k6 does not prefix) + optional tags.
        if (typeof __tropel_pm_test === 'function') {
            __tropel_pm_test(name, passed, tagsJson);
        }

        if (!passed) {
            allPassed = false;
        }
    }
    return allPassed;
}

// ── pm.visualizer ──
pm.visualizer = {
    set: function (template, data) {
        // Visualizer is not supported in CLI mode
        console.log('[visualizer] template:', template, 'data:', data);
    }
};

// ── k6-style Custom Metric Constructors ──
// These provide the k6/metrics API: create a metric object, then
// call .add(value, tags) to record a sample with optional tags.
//
// Usage:
//   var counter = new Counter('my_counter');
//   counter.add(1);
//   counter.add(1, { status: '200' });
//
//   var trend = new Trend('my_trend');
//   trend.add(15.5);
//   trend.add(15.5, { status: '200' });

function Counter(name) {
    if (!name || typeof name !== 'string') {
        throw new Error('Counter requires a metric name');
    }
    this._name = name;
    this._type = 'counter';
    this._isTime = false;
}

Counter.prototype.add = function (value, tags) {
    if (typeof __tropel_pm_custom_metric_add === 'function') {
        var tagsStr = tags ? JSON.stringify(tags) : '{}';
        __tropel_pm_custom_metric_add(this._name, Number(value), tagsStr, this._type, this._isTime);
    }
    return this;
};

function Gauge(name) {
    if (!name || typeof name !== 'string') {
        throw new Error('Gauge requires a metric name');
    }
    this._name = name;
    this._type = 'gauge';
    this._isTime = false;
}

Gauge.prototype.add = function (value, tags) {
    if (typeof __tropel_pm_custom_metric_add === 'function') {
        var tagsStr = tags ? JSON.stringify(tags) : '{}';
        __tropel_pm_custom_metric_add(this._name, Number(value), tagsStr, this._type, this._isTime);
    }
    return this;
};

function Rate(name) {
    if (!name || typeof name !== 'string') {
        throw new Error('Rate requires a metric name');
    }
    this._name = name;
    this._type = 'rate';
    this._isTime = false;
}

Rate.prototype.add = function (value, tags) {
    if (typeof __tropel_pm_custom_metric_add === 'function') {
        var tagsStr = tags ? JSON.stringify(tags) : '{}';
        __tropel_pm_custom_metric_add(this._name, Number(value), tagsStr, this._type, this._isTime);
    }
    return this;
};

function Trend(name, isTime) {
    if (!name || typeof name !== 'string') {
        throw new Error('Trend requires a metric name');
    }
    this._name = name;
    this._type = 'trend';
    this._isTime = isTime === true;
}

Trend.prototype.add = function (value, tags) {
    if (typeof __tropel_pm_custom_metric_add === 'function') {
        var tagsStr = tags ? JSON.stringify(tags) : '{}';
        __tropel_pm_custom_metric_add(this._name, Number(value), tagsStr, this._type, this._isTime);
    }
    return this;
};

// ── Export for module systems ──
if (typeof module !== 'undefined' && module.exports) {
    module.exports = pm;
}

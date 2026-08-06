// ══════════════════════════════════════════════════════════════════
// k6 Shim — provides the standard k6 JavaScript API for Tropel
//
// This shim defines the global symbols that k6 scripts expect:
//   http, check, group, sleep, fail, __VU, __ITER
//   Counter, Gauge, Rate, Trend (k6/metrics)
//
// Under the hood it delegates HTTP calls to __tropel_k6_http_request
// (registered lazily by the K6DriverInstance on first iteration) or
// to __tropel_pm_send_request (if the PM bridge is available).
//
// The preprocessor strips `import ... from "k6/..."` lines, so
// k6 scripts that use module syntax resolve to these globals.
// ══════════════════════════════════════════════════════════════════

// ── Globals set by native host (K6DriverInstance) ──
// __tropel_k6_http_request(method, url, headers_json, body, timeout_ms, response_type) -> native JS object (the PM fallback bridge returns a JSON string)
// __tropel_pm_send_request(method, url, headers_json, body, timeout_ms, response_type) -> JSON string
// __tropel_vu_id, __tropel_iteration_num, __tropel_scenario

// ══════════════════════════════════════════════════════════════════
// HTTP helper — tries native k6 bridge, falls back to PM bridge
// ══════════════════════════════════════════════════════════════════

function k6HTTPRequest(method, url, body, params) {
    var canonical = normalizeK6Request(method, url, body, params);
    var headersJson = JSON.stringify(canonical.headers);
    var resultJson = null;

    // Try the k6-native HTTP bridge first (lazy-registered by K6DriverInstance).
    // Both bridges accept the k6 responseType ("text"/"binary"/"none") as
    // their 6th argument. NOTE: previously this function had a duplicated
    // leftover block calling with undefined `bodyStr`/`timeoutMs` that threw
    // ReferenceError on EVERY request — removed.
    if (typeof __tropel_k6_http_request === 'function') {
        resultJson = __tropel_k6_http_request(
            canonical.method,
            canonical.url,
            headersJson,
            canonical.body,
            canonical.responseType,
            // Backlog line 140: timeout/tags/auth/redirects/compression packed
            // into ONE JSON string — the native bridge closure is arity-
            // capped (rquickjs Func supports ctx + 6 script args), so the
            // per-request params share a single argument. The legacy PM
            // fallback below keeps its own contract (extra params unsupported
            // there).
            JSON.stringify({
                timeoutMs: canonical.timeoutMs,
                tags: canonical.tags,
                auth: canonical.auth,
                redirects: canonical.redirects,
                compression: canonical.compression
            })
        );
    } else if (typeof __tropel_pm_send_request === 'function') {
        resultJson = __tropel_pm_send_request(
            canonical.method,
            canonical.url,
            headersJson,
            canonical.body,
            canonical.timeoutMs,
            canonical.responseType
        );
    } else {
        throw new Error(
            'k6 http.* requires a native HTTP bridge — neither __tropel_k6_http_request ' +
            'nor __tropel_pm_send_request is available. Check that the K6Driver or PM bridge ' +
            'was properly installed.'
        );
    }

    var result;
    if (typeof resultJson === 'string') {
        // Legacy path: the PM fallback bridge returns a JSON string.
        try {
            result = JSON.parse(resultJson);
        } catch (e) {
            throw new Error('k6 http.request: failed to parse native response: ' + e.message);
        }
    } else {
        // Native k6 bridge returns a live JS object — no JSON round trip
        // (avoids the old 3-4 full-body copies per response).
        result = resultJson;
    }

    var respCode = result.code || result.status_code || result.status || 0;
    var respBody = result.body || '';
    var respHeaders = result.headers || {};
    var respTime = result.responseTime || result.response_time || 0;

    // Normalize headers from {key: value} or array format into a fresh
    // object. Keys are kept EXACTLY as the native bridge delivered them (Go
    // MIME canonical form: Content-Type, X-Request-Id) — the old
    // toLowerCase() here made every k6 doc idiom `res.headers['Content-Type']`
    // return undefined (backlog line 139). The copy protects K6Response from
    // sharing the bridge's object (user mutation of res.headers must not leak
    // back into the native response).
    var normalizedHeaders = {};
    if (Array.isArray(respHeaders)) {
        for (var hi = 0; hi < respHeaders.length; hi++) {
            var h = respHeaders[hi];
            if (h && h.key) {
                normalizedHeaders[h.key] = h.value !== undefined ? h.value : '';
            }
        }
    } else if (typeof respHeaders === 'object') {
        for (var hk in respHeaders) {
            if (respHeaders.hasOwnProperty(hk)) {
                normalizedHeaders[hk] = respHeaders[hk];
            }
        }
    }

    var timings = {
        blocked: 0,
        connecting: 0,
        tls_handshaking: 0,
        sending: 0,
        waiting: respTime,
        receiving: 0,
        duration: respTime
    };

    return new K6Response(respCode, respBody, normalizedHeaders, timings, url);
}

function normalizeK6Request(method, url, body, params) {
    method = (method || 'GET').toUpperCase();
    params = params || {};

    // COPY the caller's headers: serializeK6Body stamps Content-Type for
    // object bodies (and the generated boundary for multipart), and real k6
    // scripts hoist `params` to module scope — writing on the caller's
    // object leaked iteration 1's Content-Type into every later iteration
    // (a string body posted on iteration 2 was still labelled
    // application/json). The copy keeps the stamp per-request.
    var headers = {};
    var srcHeaders = params.headers;
    if (srcHeaders && typeof srcHeaders === 'object' && !Array.isArray(srcHeaders)) {
        for (var hk in srcHeaders) {
            if (srcHeaders.hasOwnProperty(hk)) headers[hk] = srcHeaders[hk];
        }
    }
    var timeout = params.timeout || '30s';
    // k6 params.responseType: "text" (default) | "binary" | "none"
    var responseType = params.responseType || 'text';

    var timeoutMs = 30000;
    if (typeof timeout === 'string') {
        var match = timeout.match(/^(\d+)(ms|s|m)?$/);
        if (match) {
            var val = parseInt(match[1], 10);
            var unit = match[2] || 'ms';
            if (unit === 's') timeoutMs = val * 1000;
            else if (unit === 'm') timeoutMs = val * 60000;
            else timeoutMs = val;
        }
    } else if (typeof timeout === 'number') {
        timeoutMs = timeout;
    }

    // k6 params.cookies: {name: value} — merged into the Cookie header
    // (k6 sends cookies as a Cookie header), combined with any explicit
    // Cookie header the script set. `headers` is already a per-request copy,
    // so this can't leak into the caller's module-scope params.
    var cookies = params.cookies;
    if (cookies && typeof cookies === 'object') {
        var cookieParts = [];
        for (var ck in cookies) {
            if (cookies.hasOwnProperty(ck) && cookies[ck] !== undefined && cookies[ck] !== null) {
                cookieParts.push(encodeURIComponent(ck) + '=' + encodeURIComponent(String(cookies[ck])));
            }
        }
        if (cookieParts.length > 0) {
            var existingCookie = headers['Cookie'] || headers['cookie'] || '';
            headers['Cookie'] = existingCookie
                ? existingCookie + '; ' + cookieParts.join('; ')
                : cookieParts.join('; ');
        }
    }

    var serialized = serializeK6Body(body, headers);
    return {
        method: method,
        url: url,
        headers: serialized.headers,
        body: serialized.body,
        timeoutMs: timeoutMs,
        responseType: responseType,
        // Backlog line 140: tags/auth/redirects/compression were silently
        // dropped and timeout was parsed then discarded. The native bridge
        // now receives all of them (auth translated to the tagged AuthConfig
        // form the Rust side deserializes).
        tags: params.tags || {},
        auth: toAuthConfig(params.auth),
        redirects: params.redirects !== undefined ? params.redirects : -1,
        compression: params.compression || '',
    };
}

// Translate k6's `params.auth` object (no type discriminator) into the
// tagged AuthConfig form the native bridge deserializes. k6 infers the type
// from which fields are present: token → bearer, access_token → oauth2,
// access_key → aws-sigv4, username → basic (k6's documented shapes).
function toAuthConfig(auth) {
    if (!auth || typeof auth !== 'object') return null;
    if (auth.token !== undefined) {
        return { type: 'bearer', token: String(auth.token) };
    }
    if (auth.access_token !== undefined) {
        return {
            type: 'oauth2',
            access_token: String(auth.access_token),
            token_type: auth.token_type !== undefined ? String(auth.token_type) : null,
        };
    }
    if (auth.access_key !== undefined) {
        return {
            type: 'aws-sigv4',
            access_key: String(auth.access_key),
            secret_key: auth.secret_key !== undefined ? String(auth.secret_key) : '',
            region: auth.region !== undefined ? String(auth.region) : null,
            service: auth.service !== undefined ? String(auth.service) : null,
            session_token: auth.session_token !== undefined ? String(auth.session_token) : null,
        };
    }
    if (auth.username !== undefined) {
        return {
            type: 'basic',
            username: String(auth.username),
            password: auth.password !== undefined ? String(auth.password) : '',
        };
    }
    return null;
}

function serializeK6Body(body, headers) {
    var bodyStr = '';
    if (body !== null && body !== undefined) {
        if (typeof body === 'string') {
            bodyStr = body;
        } else if (body instanceof ArrayBuffer) {
            bodyStr = '';
        } else {
            var contentType = headers['Content-Type'] || headers['content-type'];
            if (contentType && contentType.indexOf('multipart/form-data') !== -1 && typeof body === 'object') {
                var multipart = buildMultipartFormData(body);
                bodyStr = multipart.body;
                // ALWAYS stamp the full generated content-type. The old
                // `!headers['Content-Type']` guard was false exactly when the
                // user declared multipart/form-data (they set the type but not
                // a boundary), so the generated boundary never reached the
                // header and every multipart request was unparseable. The body
                // was framed with OUR boundary, so the header must advertise
                // exactly that boundary. `headers` is a per-request copy
                // (normalizeK6Request clones params.headers), so this can't
                // leak into the caller's object. Drop any user-declared
                // lowercase variant — leaving it would send TWO Content-Type
                // headers (one boundary-less) and the Rust side's case-
                // sensitive HashMap would keep both.
                delete headers['content-type'];
                headers['Content-Type'] = multipart.contentType;
            } else if (contentType && contentType.indexOf('application/x-www-form-urlencoded') !== -1 && typeof body === 'object') {
                bodyStr = serializeUrlEncoded(body);
            } else {
                try {
                    bodyStr = JSON.stringify(body);
                    if (!headers['Content-Type'] && !headers['content-type']) {
                        headers['Content-Type'] = 'application/json';
                    }
                } catch (e) {
                    bodyStr = String(body);
                }
            }
        }
    }

    return { body: bodyStr, headers: headers };
}

function buildMultipartFormData(object) {
    var boundary = '----TropelFormBoundary' + Math.random().toString(36).slice(2);
    var body = '';

    for (var key in object) {
        if (!object.hasOwnProperty(key)) continue;
        var value = object[key];
        if (value === undefined || value === null) {
            value = '';
        } else if (typeof value !== 'string') {
            try {
                value = JSON.stringify(value);
            } catch (e) {
                value = String(value);
            }
        }
        body += '--' + boundary + '\r\n';
        body += 'Content-Disposition: form-data; name="' + escapeMultipartFieldName(key) + '"\r\n\r\n';
        body += value + '\r\n';
    }
    body += '--' + boundary + '--\r\n';

    return { body: body, contentType: 'multipart/form-data; boundary=' + boundary };
}

function serializeUrlEncoded(object) {
    var parts = [];
    for (var key in object) {
        if (!object.hasOwnProperty(key)) continue;
        var value = object[key];
        if (value === undefined || value === null) {
            value = '';
        }
        parts.push(encodeURIComponent(key) + '=' + encodeURIComponent(String(value)));
    }
    return parts.join('&');
}

function escapeMultipartFieldName(name) {
    return String(name).replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}

// ══════════════════════════════════════════════════════════════════
// Response object (k6-compatible)
// ══════════════════════════════════════════════════════════════════

function K6Response(status, body, headers, timings, url) {
    this.status = status;
    this.body = body;
    this.headers = headers || {};
    this.timings = timings || { blocked: 0, connecting: 0, tls_handshaking: 0, sending: 0, waiting: 0, receiving: 0, duration: 0 };
    this.url = url || '';
    this.status_text = String(status) + ' ' + getStatusText(status);
}

K6Response.prototype.json = function () {
    if (!this.body || this.body === '') {
        throw new Error('Response body is empty — cannot parse JSON');
    }
    // Parse fresh on every call, exactly like k6: a script that mutates the
    // returned object must see a clean re-parse on the next .json() call
    // (k6 does not cache, and caching would persist user mutations). The
    // heavy copy savings for the response envelope itself come from the
    // native-object bridge (driver.rs), not from re-parsing here.
    return JSON.parse(this.body);
};

// ══════════════════════════════════════════════════════════════════
// http.* methods
// ══════════════════════════════════════════════════════════════════

var http = {};

http.get = function (url, params) { return k6HTTPRequest('GET', url, null, params); };
http.post = function (url, body, params) { return k6HTTPRequest('POST', url, body, params); };
http.put = function (url, body, params) { return k6HTTPRequest('PUT', url, body, params); };
http.del = function (url, params) { return k6HTTPRequest('DELETE', url, null, params); };
http.delete = function (url, params) { return k6HTTPRequest('DELETE', url, null, params); };
http.patch = function (url, body, params) { return k6HTTPRequest('PATCH', url, body, params); };
http.head = function (url, params) { return k6HTTPRequest('HEAD', url, null, params); };
http.options = function (url, params) { return k6HTTPRequest('OPTIONS', url, null, params); };
http.request = function (method, url, body, params) { return k6HTTPRequest(method, url, body, params); };

// http.batch — sequential execution (QuickJS has no async)
//
// k6 semantics (backlog line 137): an ARRAY of requests returns an ARRAY of
// responses (index-preserving — Array.isArray true, .forEach/spread/
// destructuring work); an OBJECT of named requests returns an OBJECT keyed
// by name. The old implementation always returned a keyed object, so the
// documented batch idiom (`res[0].status`) silently broke.
http.batch = function (requests) {
    if (!requests) {
        throw new Error('http.batch requires an array or object of requests');
    }

    var isArrayInput = Array.isArray(requests);
    var entries = [];
    var keys = [];

    if (isArrayInput) {
        for (var bi = 0; bi < requests.length; bi++) {
            var e = normalizeBatchEntry(requests[bi], bi);
            entries.push(e);
            keys.push(e.key != null ? e.key : bi);
        }
    } else if (typeof requests === 'object') {
        var names = Object.keys(requests);
        for (var ni = 0; ni < names.length; ni++) {
            var e = normalizeBatchEntry(requests[names[ni]], names[ni]);
            entries.push(e);
            keys.push(e.key != null ? e.key : names[ni]);
        }
    }

    var results = isArrayInput ? [] : {};

    if (typeof __tropel_k6_http_batch === 'function') {
        var normalized = [];
        for (var ei = 0; ei < entries.length; ei++) {
            var entry = entries[ei];
            var canonical = normalizeK6Request(entry.method, entry.url, entry.body, entry.params);
            normalized.push({
                key: String(keys[ei]),
                method: canonical.method,
                url: canonical.url,
                headers_json: JSON.stringify(canonical.headers),
                body: canonical.body,
                timeout_ms: canonical.timeoutMs,
                response_type: canonical.responseType,
                // Backlog line 140: batch entries carry the same per-request
                // params the single-request bridge consumes.
                tags_json: JSON.stringify(canonical.tags),
                auth_json: canonical.auth !== null ? JSON.stringify(canonical.auth) : 'null',
                redirects: canonical.redirects,
                compression: canonical.compression,
            });
        }

        var batchResultJson = __tropel_k6_http_batch(JSON.stringify(normalized));
        var batchResult = JSON.parse(batchResultJson);
        for (var ei = 0; ei < entries.length; ei++) {
            var entry = entries[ei];
            var key = String(keys[ei]);
            // Wrap each entry as a K6Response so `.json()`, `.status`, `.body`
            // behave like the sequential path (k6 returns Response objects).
            var raw = batchResult[key] || {};
            var headers = raw.headers || {};
            var normalizedHeaders = {};
            if (Array.isArray(headers)) {
                for (var hi = 0; hi < headers.length; hi++) {
                    var h = headers[hi];
                    if (h && h.key) {
                        normalizedHeaders[h.key] = h.value !== undefined ? h.value : '';
                    }
                }
            } else {
                for (var hk in headers) {
                    if (headers.hasOwnProperty(hk)) {
                        normalizedHeaders[hk] = headers[hk];
                    }
                }
            }
            var code = raw.code || raw.status_code || raw.status || 0;
            var rtime = raw.responseTime || raw.response_time || 0;
            var timings = {
                blocked: 0,
                connecting: 0,
                tls_handshaking: 0,
                sending: 0,
                waiting: rtime,
                receiving: 0,
                duration: rtime
            };
            var resp = new K6Response(code, raw.body || '', normalizedHeaders, timings, entry.url);
            if (isArrayInput) {
                results.push(resp);
            } else {
                results[key] = resp;
            }
        }
    } else {
        for (var ei = 0; ei < entries.length; ei++) {
            var entry = entries[ei];
            var resp = k6HTTPRequest(entry.method, entry.url, entry.body, entry.params);
            if (isArrayInput) {
                results.push(resp);
            } else {
                results[String(keys[ei])] = resp;
            }
        }
    }

    return results;
};

function normalizeBatchEntry(req, defaultKey) {
    if (typeof req === 'string') {
        return { key: defaultKey, method: 'GET', url: req, body: null, params: {} };
    }
    if (Array.isArray(req) && req.length >= 2) {
        return {
            key: defaultKey,
            method: req[0],
            url: req[1],
            body: req.length > 2 ? req[2] : null,
            params: req.length > 3 ? req[3] : {}
        };
    }
    if (typeof req === 'object') {
        // Preserve the object-form entry's responseType (k6: params.responseType)
        var entryParams = req.params || {};
        return {
            key: req.name != null ? req.name : defaultKey,
            method: req.method || 'GET',
            url: req.url || '',
            body: req.body || null,
            params: {
                headers: req.headers || entryParams.headers || {},
                tags: req.tags || entryParams.tags || {},
                timeout: req.timeout || entryParams.timeout || '30s',
                responseType: entryParams.responseType || req.responseType || 'text'
            }
        };
    }
    throw new Error('Invalid batch request entry: ' + JSON.stringify(req));
}

// ══════════════════════════════════════════════════════════════════
// ws.* — k6/ws parity (event-driven WebSocket)
// ══════════════════════════════════════════════════════════════════
//
// Delegates to native bridges registered by the K6Driver:
//   __tropel_k6_ws_connect(url, headers_json) -> {id, error}
//   __tropel_k6_ws_step(id, timeout_ms)          -> {type, ...}
//   __tropel_k6_ws_send(id, data) / _ping(id) / _close(id, code, reason)
//   __tropel_k6_ws_finish(id)
//
// QuickJS has no async event loop, so ws.connect() runs a synchronous
// event pump: the callback registers handlers, then the pump calls
// __tropel_k6_ws_step() to block for the next event (open/message/close/
// error/ping/pong) and dispatches to the registered socket.on() handlers.
// The pump ends when the socket closes (server close, socket.close(), or an
// error). This mirrors k6's semantics within one iteration: the VU stays on
// the socket until it closes.

var ws = {};

function K6Socket(sessionId) {
    this._id = sessionId;
    this._handlers = {};
    this._timers = [];
    this._closed = false;
}

K6Socket.prototype.on = function (event, handler) {
    if (typeof handler !== 'function') {
        throw new Error('socket.on(event, handler) requires a function handler');
    }
    var list = this._handlers[event] || (this._handlers[event] = []);
    list.push(handler);
    return this;
};

K6Socket.prototype._emit = function (event, arg1, arg2) {
    var list = this._handlers[event];
    if (!list) {
        return;
    }
    for (var i = 0; i < list.length; i++) {
        try {
            list[i].call(this, arg1, arg2);
        } catch (e) {
            fail('ws handler "' + event + '" threw: ' + e);
        }
    }
};

K6Socket.prototype.send = function (data) {
    if (typeof __tropel_k6_ws_send !== 'function') {
        throw new Error('ws.send requires the native ws bridge (__tropel_k6_ws_send)');
    }
    __tropel_k6_ws_send(this._id, String(data));
    return this;
};

K6Socket.prototype.ping = function () {
    if (typeof __tropel_k6_ws_ping !== 'function') {
        throw new Error('ws.ping requires the native ws bridge (__tropel_k6_ws_ping)');
    }
    __tropel_k6_ws_ping(this._id);
    return this;
};

K6Socket.prototype.close = function (code, reason) {
    if (typeof __tropel_k6_ws_close !== 'function') {
        throw new Error('ws.close requires the native ws bridge (__tropel_k6_ws_close)');
    }
    var closeCode = code || 1000;
    var closeReason = reason || '';
    __tropel_k6_ws_close(this._id, closeCode, closeReason);
    // Backlog line 148: a LOCAL close() must still dispatch the 'close'
    // handler. The old code only set _closed, so the synchronous pump
    // (while !settled && !socket._closed) exited at the next iteration and
    // `socket.on('close', ...)` never fired — the k6 idiom of calling
    // socket.close() inside on('open')/'message' leaked the final cleanup
    // callback. Guarded so a server-close that already fired the handler
    // can't double-dispatch.
    if (!this._closed) {
        this._closed = true;
        this._emit('close', closeCode, closeReason);
    }
    return this;
};

K6Socket.prototype.setTimeout = function (fn, ms) {
    this._timers.push({ fn: fn, ms: ms, at: Date.now() + ms, interval: false });
    return this;
};

K6Socket.prototype.setInterval = function (fn, ms) {
    this._timers.push({ fn: fn, ms: ms, at: Date.now() + ms, interval: true });
    return this;
};

// Fire due timers. One-shot timeouts are removed; intervals are rescheduled.
K6Socket.prototype._runTimers = function () {
    var now = Date.now();
    var keep = [];
    for (var i = 0; i < this._timers.length; i++) {
        var t = this._timers[i];
        if (now >= t.at) {
            try {
                t.fn();
            } catch (e) {
                fail('ws timer threw: ' + e);
            }
            if (t.interval) {
                t.at = now + t.ms;
                keep.push(t);
            }
        } else {
            keep.push(t);
        }
    }
    this._timers = keep;
};

ws.connect = function (url, params, callback) {
    params = params || {};
    var headers = params.headers || {};
    if (typeof __tropel_k6_ws_connect !== 'function') {
        throw new Error(
            'ws.connect requires the native ws bridge (__tropel_k6_ws_connect) — ' +
            'check that the K6Driver installed the ws bridges'
        );
    }
    var connectRes = JSON.parse(__tropel_k6_ws_connect(url, JSON.stringify(headers)));
    if (connectRes.error) {
        throw new Error('ws.connect failed: ' + connectRes.error);
    }
    var socket = new K6Socket(connectRes.id);
    // Safety cap: if the peer never closes and the script never calls
    // close(), end the session after params.timeout (ms) so the synchronous
    // pump can never hang the VU. Default 5 minutes.
    var maxSessionMs = (params.timeout > 0) ? params.timeout : 300000;
    var sessionStart = Date.now();
    var settled = false;
    try {
        if (typeof callback === 'function') {
            callback(socket);
        }
        // Synchronous event pump: drive the socket until it closes.
        while (!settled && !socket._closed) {
            if (Date.now() - sessionStart > maxSessionMs) {
                socket.close(1000, 'session timeout');
                settled = true;
                break;
            }
            socket._runTimers();
            var evt = JSON.parse(__tropel_k6_ws_step(connectRes.id, 50));
            if (evt.type === 'open') {
                socket._emit('open');
            } else if (evt.type === 'message') {
                socket._emit('message', evt.data);
            } else if (evt.type === 'ping') {
                socket._emit('ping');
            } else if (evt.type === 'pong') {
                socket._emit('pong');
            } else if (evt.type === 'close') {
                // Mark closed BEFORE dispatching so a defensive
                // socket.close() inside the close handler cannot
                // double-dispatch (backlog line 148: _closed is the
                // authoritative flag for both local and remote closes).
                socket._closed = true;
                socket._emit('close', evt.code, evt.reason);
                settled = true;
            } else if (evt.type === 'error') {
                // Same guard: once errored, a later local close() must not
                // fire 'close' a second time.
                socket._closed = true;
                socket._emit('error', evt.message);
                settled = true;
            }
            // {type:'none'} — step timed out with no event; loop again
            // (timers may fire, or close may arrive on a later step).
        }
    } finally {
        // ALWAYS tear down the native session — even when the user callback
        // or a socket.on handler threw — so the registry entry and its
        // background socket task are not leaked.
        if (typeof __tropel_k6_ws_finish === 'function') {
            try {
                __tropel_k6_ws_finish(connectRes.id);
            } catch (e) { /* teardown must not mask the original error */ }
        }
    }
    return socket;
};

// ══════════════════════════════════════════════════════════════════
// Global functions
// ══════════════════════════════════════════════════════════════════

// fail(msg) — throws an error that aborts the current iteration
function fail(msg) {
    throw new Error('k6 fail: ' + (msg || 'test failed'));
}

// check(val, conds) — defined in pm-api/pm.js if loaded, else here.
// NOTE: uses `var` assignment (NOT `function` inside the guard) — QuickJS
// block-scopes function declarations, so a `function` here would be invisible
// outside the if-block whenever the fallback actually ran.
if (typeof check !== 'function') {
    // Backlog line 149: k6 parity — null/non-object conds throw, raw
    // check names (no "check " prefix), 3rd tags arg forwarded, and a
    // throwing predicate records a failed check then propagates.
    var check = function (val, conds, tags) {
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
                try {
                    passed = !!condition(val);
                } catch (e) {
                    if (typeof __tropel_pm_test === 'function') {
                        __tropel_pm_test(name, false, tagsJson);
                    }
                    throw e;
                }
            } else {
                passed = !!condition;
            }
            if (typeof __tropel_pm_test === 'function') {
                __tropel_pm_test(name, passed, tagsJson);
            }
            if (!passed) {
                allPassed = false;
            }
        }
        return allPassed;
    };
}

// group(name, fn) — defined in pm-api/pm.js if loaded, else here
if (typeof group !== 'function') {
    var group = function (name, fn) {
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
            if (typeof fn === 'function') {
                return fn();
            }
        }
    };
}

// sleep(seconds) — bootstrapped by the engine, but ensure it exists
if (typeof sleep !== 'function') {
    var sleep = function (seconds) {
        if (typeof __tropel_native_sleep === 'function') {
            __tropel_native_sleep(seconds * 1000);
        }
    };
}

// ══════════════════════════════════════════════════════════════════
// k6 globals
// ══════════════════════════════════════════════════════════════════

// __VU and __ITER — updated by K6DriverInstance before each iteration
var __VU = __VU || 0;
var __ITER = __ITER || 0;

// ══════════════════════════════════════════════════════════════════
// k6/metrics — Metric constructors
// ══════════════════════════════════════════════════════════════════

// These are also defined in pm-api/pm.js — only define if missing.
// NOTE: `var` assignments, not `function` declarations — QuickJS block-scopes
// the latter, so a guarded fallback would be invisible outside its if-block.
if (typeof Counter !== 'function') {
    var Counter = function (name) {
        if (!name || typeof name !== 'string') {
            throw new Error('Counter requires a metric name');
        }
        this._name = name;
        this._type = 'counter';
    };
    Counter.prototype.add = function (value, tags) {
        if (typeof __tropel_pm_custom_metric_add === 'function') {
            var tagsStr = tags ? JSON.stringify(tags) : '{}';
            __tropel_pm_custom_metric_add(this._name, Number(value), tagsStr, this._type);
        }
        return this;
    };
}
if (typeof Gauge !== 'function') {
    var Gauge = function (name) {
        if (!name || typeof name !== 'string') {
            throw new Error('Gauge requires a metric name');
        }
        this._name = name;
        this._type = 'gauge';
    };
    Gauge.prototype.add = function (value, tags) {
        if (typeof __tropel_pm_custom_metric_add === 'function') {
            var tagsStr = tags ? JSON.stringify(tags) : '{}';
            __tropel_pm_custom_metric_add(this._name, Number(value), tagsStr, this._type);
        }
        return this;
    };
}
if (typeof Rate !== 'function') {
    var Rate = function (name) {
        if (!name || typeof name !== 'string') {
            throw new Error('Rate requires a metric name');
        }
        this._name = name;
        this._type = 'rate';
    };
    Rate.prototype.add = function (value, tags) {
        if (typeof __tropel_pm_custom_metric_add === 'function') {
            var tagsStr = tags ? JSON.stringify(tags) : '{}';
            __tropel_pm_custom_metric_add(this._name, Number(value), tagsStr, this._type);
        }
        return this;
    };
}
if (typeof Trend !== 'function') {
    var Trend = function (name) {
        if (!name || typeof name !== 'string') {
            throw new Error('Trend requires a metric name');
        }
        this._name = name;
        this._type = 'trend';
    };
    Trend.prototype.add = function (value, tags) {
        if (typeof __tropel_pm_custom_metric_add === 'function') {
            var tagsStr = tags ? JSON.stringify(tags) : '{}';
            __tropel_pm_custom_metric_add(this._name, Number(value), tagsStr, this._type);
        }
        return this;
    };
}

// ══════════════════════════════════════════════════════════════════
// Helpers
// ══════════════════════════════════════════════════════════════════

function getStatusText(code) {
    var texts = {
        200: 'OK', 201: 'Created', 204: 'No Content',
        301: 'Moved Permanently', 302: 'Found', 304: 'Not Modified',
        400: 'Bad Request', 401: 'Unauthorized', 403: 'Forbidden',
        404: 'Not Found', 405: 'Method Not Allowed', 408: 'Request Timeout',
        409: 'Conflict', 413: 'Payload Too Large', 415: 'Unsupported Media Type',
        422: 'Unprocessable Entity', 429: 'Too Many Requests',
        500: 'Internal Server Error', 502: 'Bad Gateway',
        503: 'Service Unavailable', 504: 'Gateway Timeout'
    };
    return texts[code] || '';
}

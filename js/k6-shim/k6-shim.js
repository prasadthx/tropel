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
// __tropel_k6_http_request(method, url, headers_json, body, timeout_ms, response_type) -> JSON string
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
            canonical.timeoutMs,
            canonical.responseType
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
    try {
        result = JSON.parse(resultJson);
    } catch (e) {
        throw new Error('k6 http.request: failed to parse native response: ' + e.message);
    }

    var respCode = result.code || result.status_code || result.status || 0;
    var respBody = result.body || '';
    var respHeaders = result.headers || {};
    var respTime = result.responseTime || result.response_time || 0;

    // Normalize headers from {key: value} or array format
    var normalizedHeaders = {};
    if (Array.isArray(respHeaders)) {
        for (var hi = 0; hi < respHeaders.length; hi++) {
            var h = respHeaders[hi];
            if (h && h.key) {
                normalizedHeaders[h.key.toLowerCase()] = h.value !== undefined ? h.value : '';
            }
        }
    } else if (typeof respHeaders === 'object') {
        for (var hk in respHeaders) {
            if (respHeaders.hasOwnProperty(hk)) {
                normalizedHeaders[hk.toLowerCase()] = respHeaders[hk];
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

    var headers = params.headers || {};
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

    var serialized = serializeK6Body(body, headers);
    return {
        method: method,
        url: url,
        headers: serialized.headers,
        body: serialized.body,
        timeoutMs: timeoutMs,
        responseType: responseType,
    };
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
                if (!headers['Content-Type'] && !headers['content-type']) {
                    headers['Content-Type'] = multipart.contentType;
                }
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
http.batch = function (requests) {
    if (!requests) {
        throw new Error('http.batch requires an array or object of requests');
    }

    var entries = [];

    if (Array.isArray(requests)) {
        for (var bi = 0; bi < requests.length; bi++) {
            entries.push(normalizeBatchEntry(requests[bi], bi));
        }
    } else if (typeof requests === 'object') {
        var names = Object.keys(requests);
        for (var ni = 0; ni < names.length; ni++) {
            entries.push(normalizeBatchEntry(requests[names[ni]], names[ni]));
        }
    }

    var results = {};

    if (typeof __tropel_k6_http_batch === 'function') {
        var normalized = [];
        for (var ei = 0; ei < entries.length; ei++) {
            var entry = entries[ei];
            var canonical = normalizeK6Request(entry.method, entry.url, entry.body, entry.params);
            normalized.push({
                key: entry.key != null ? entry.key : String(ei),
                method: canonical.method,
                url: canonical.url,
                headers_json: JSON.stringify(canonical.headers),
                body: canonical.body,
                timeout_ms: canonical.timeoutMs,
                response_type: canonical.responseType,
            });
        }

        var batchResultJson = __tropel_k6_http_batch(JSON.stringify(normalized));
        var batchResult = JSON.parse(batchResultJson);
        for (var ei = 0; ei < entries.length; ei++) {
            var entry = entries[ei];
            var key = entry.key != null ? String(entry.key) : String(ei);
            // Wrap each entry as a K6Response so `.json()`, `.status`, `.body`
            // behave like the sequential path (k6 returns Response objects).
            var raw = batchResult[key] || {};
            var headers = raw.headers || {};
            var normalizedHeaders = {};
            if (Array.isArray(headers)) {
                for (var hi = 0; hi < headers.length; hi++) {
                    var h = headers[hi];
                    if (h && h.key) {
                        normalizedHeaders[h.key.toLowerCase()] = h.value !== undefined ? h.value : '';
                    }
                }
            } else {
                for (var hk in headers) {
                    if (headers.hasOwnProperty(hk)) {
                        normalizedHeaders[hk.toLowerCase()] = headers[hk];
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
            results[key] = new K6Response(code, raw.body || '', normalizedHeaders, timings, entry.url);
        }
    } else {
        for (var ei = 0; ei < entries.length; ei++) {
            var entry = entries[ei];
            var key = entry.key != null ? String(entry.key) : String(ei);
            var resp = k6HTTPRequest(entry.method, entry.url, entry.body, entry.params);
            results[key] = resp;
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
    var check = function (val, conds) {
        if (!conds || typeof conds !== 'object') {
            return true;
        }
        var allPassed = true;
        var names = Object.keys(conds);
        for (var i = 0; i < names.length; i++) {
            var name = names[i];
            var condition = conds[name];
            var passed = false;
            try {
                if (typeof condition === 'function') {
                    passed = !!condition(val);
                } else {
                    passed = val === condition;
                }
            } catch (e) {
                console.error('check error for "' + name + '":', e);
            }
            if (typeof __tropel_pm_test === 'function') {
                __tropel_pm_test('check ' + name, passed);
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

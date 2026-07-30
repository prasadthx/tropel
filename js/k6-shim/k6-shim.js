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
// __tropel_k6_http_request(method, url, headers_json, body, timeout_ms) -> JSON string
// __tropel_pm_send_request(method, url, headers_json, body, timeout_ms) -> JSON string
// __tropel_vu_id, __tropel_iteration_num, __tropel_scenario

// ══════════════════════════════════════════════════════════════════
// HTTP helper — tries native k6 bridge, falls back to PM bridge
// ══════════════════════════════════════════════════════════════════

function k6HTTPRequest(method, url, body, params) {
    method = (method || 'GET').toUpperCase();
    params = params || {};

    // Process headers
    var headers = params.headers || {};
    var timeout = params.timeout || '30s';

    // Convert timeout string to milliseconds
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

    // Serialize body
    var bodyStr = '';
    if (body !== null && body !== undefined) {
        if (typeof body === 'string') {
            bodyStr = body;
        } else if (body instanceof ArrayBuffer) {
            bodyStr = '';
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

    var headersJson = JSON.stringify(headers);
    var resultJson = null;

    // Try the k6-native HTTP bridge first (lazy-registered by K6DriverInstance)
    if (typeof __tropel_k6_http_request === 'function') {
        resultJson = __tropel_k6_http_request(method, url, headersJson, bodyStr, timeoutMs);
    }
    // Fall back to the PM bridge (if installed)
    else if (typeof __tropel_pm_send_request === 'function') {
        resultJson = __tropel_pm_send_request(method, url, headersJson, bodyStr, timeoutMs);
    }
    else {
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
    for (var ei = 0; ei < entries.length; ei++) {
        var entry = entries[ei];
        var key = entry.key != null ? String(entry.key) : String(ei);
        var resp = k6HTTPRequest(entry.method, entry.url, entry.body, entry.params);
        results[key] = resp;
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
        return {
            key: req.name != null ? req.name : defaultKey,
            method: req.method || 'GET',
            url: req.url || '',
            body: req.body || null,
            params: {
                headers: req.headers || {},
                tags: req.tags || {},
                timeout: req.timeout || '30s'
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

// check(val, conds) — defined in pm-api/pm.js if loaded, else here
if (typeof check !== 'function') {
    function check(val, conds) {
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
    }
}

// group(name, fn) — defined in pm-api/pm.js if loaded, else here
if (typeof group !== 'function') {
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
            if (typeof fn === 'function') {
                return fn();
            }
        }
    }
}

// sleep(seconds) — bootstrapped by the engine, but ensure it exists
if (typeof sleep !== 'function') {
    function sleep(seconds) {
        if (typeof __tropel_native_sleep === 'function') {
            __tropel_native_sleep(seconds * 1000);
        }
    }
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

// These are also defined in pm-api/pm.js — only define if missing
if (typeof Counter !== 'function') {
    function Counter(name) {
        if (!name || typeof name !== 'string') {
            throw new Error('Counter requires a metric name');
        }
        this._name = name;
        this._type = 'counter';
    }
    Counter.prototype.add = function (value, tags) {
        if (typeof __tropel_pm_custom_metric_add === 'function') {
            var tagsStr = tags ? JSON.stringify(tags) : '{}';
            __tropel_pm_custom_metric_add(this._name, Number(value), tagsStr, this._type);
        }
        return this;
    };
}
if (typeof Gauge !== 'function') {
    function Gauge(name) {
        if (!name || typeof name !== 'string') {
            throw new Error('Gauge requires a metric name');
        }
        this._name = name;
        this._type = 'gauge';
    }
    Gauge.prototype.add = function (value, tags) {
        if (typeof __tropel_pm_custom_metric_add === 'function') {
            var tagsStr = tags ? JSON.stringify(tags) : '{}';
            __tropel_pm_custom_metric_add(this._name, Number(value), tagsStr, this._type);
        }
        return this;
    };
}
if (typeof Rate !== 'function') {
    function Rate(name) {
        if (!name || typeof name !== 'string') {
            throw new Error('Rate requires a metric name');
        }
        this._name = name;
        this._type = 'rate';
    }
    Rate.prototype.add = function (value, tags) {
        if (typeof __tropel_pm_custom_metric_add === 'function') {
            var tagsStr = tags ? JSON.stringify(tags) : '{}';
            __tropel_pm_custom_metric_add(this._name, Number(value), tagsStr, this._type);
        }
        return this;
    };
}
if (typeof Trend !== 'function') {
    function Trend(name) {
        if (!name || typeof name !== 'string') {
            throw new Error('Trend requires a metric name');
        }
        this._name = name;
        this._type = 'trend';
    }
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

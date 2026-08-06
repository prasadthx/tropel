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
            __tropel_pm_variables_set(key, value);
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
pm.response = {
    code: function () {
        if (typeof __tropel_pm_response_code === 'function') {
            return __tropel_pm_response_code();
        }
        return 0;
    },
    status: function () {
        if (typeof __tropel_pm_response_status === 'function') {
            return __tropel_pm_response_status();
        }
        return '';
    },
    text: function () {
        if (typeof __tropel_pm_response_body === 'function') {
            return __tropel_pm_response_body();
        }
        return '';
    },
    json: function () {
        if (typeof __tropel_pm_response_json === 'function') {
            var raw = __tropel_pm_response_json();
            if (raw) {
                return JSON.parse(raw);
            }
            throw new Error('pm.response.json() — response body is not valid JSON or no response available');
        }
        throw new Error('pm.response.json() is not available in this runtime');
    },
    headers: function () {
        if (typeof __tropel_pm_response_headers === 'function') {
            return __tropel_pm_response_headers();
        }
        return {};
    },
    header: function (key) {
        if (typeof __tropel_pm_response_header === 'function') {
            return __tropel_pm_response_header(key);
        }
        return null;
    },
    responseTime: function () {
        if (typeof __tropel_pm_response_time === 'function') {
            return __tropel_pm_response_time();
        }
        return 0;
    },
    cookies: function () {
        if (typeof __tropel_pm_response_cookies === 'function') {
            return __tropel_pm_response_cookies();
        }
        return [];
    },
    to: {
        have: {
            status: function (code) {
                var actual = pm.response.code();
                if (actual !== code) {
                    throw new Error(
                        'expected response to have status ' + code + ' but got ' + actual
                    );
                }
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
            __tropel_pm_test(name, passed);
        }
        return passed;
    } catch (e) {
        if (typeof __tropel_pm_test === 'function') {
            __tropel_pm_test(name + ' (error)', false);
        }
        console.error('pm.test error:', e);
        return false;
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
pm.expect = function (actual) {
    return {
        to: {
            eql: function (expected) {
                if (actual !== expected) {
                    throw new Error(
                        'expected ' + shortJson(actual) + ' to eql ' + shortJson(expected)
                    );
                }
            },
            equal: function (expected) {
                return this.eql(expected);
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
                    var actual = pm.response.code();
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
                    if (JSON.stringify(body) !== JSON.stringify(expected)) {
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
                    if (actual === expected) {
                        throw new Error(
                            'expected ' + shortJson(actual) + ' not to eql ' + shortJson(expected)
                        );
                    }
                }
            }
        }
    };
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
                // Plain object or Postman header object
                headers = options.headers;
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
                                if (!headers['Content-Type'] && !headers['content-type']) {
                                    headers['Content-Type'] = multipart.contentType;
                                }
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
pm.execution = {
    setNextRequest: function (requestName) {
        if (typeof __tropel_pm_set_next_request === 'function') {
            __tropel_pm_set_next_request(requestName);
        }
    },
    skipRequest: function () {
        pm.execution.setNextRequest(null);
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

// ── check(val, conds) — k6-style checks ──
// Evaluates conditions against a value. Each condition is a named
// predicate (function) or expected value. Records each as a checks
// Rate metric (pass/fail). Returns true if ALL checks pass.
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
                // Predicate function — call with the value
                passed = !!condition(val);
            } else {
                // Direct comparison
                passed = val === condition;
            }
        } catch (e) {
            // Error during evaluation — count as failed
            console.error('check error for "' + name + '":', e);
        }

        // Record the check pass/fail via the existing test bridge
        if (typeof __tropel_pm_test === 'function') {
            __tropel_pm_test('check ' + name, passed);
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
}

Counter.prototype.add = function (value, tags) {
    if (typeof __tropel_pm_custom_metric_add === 'function') {
        var tagsStr = tags ? JSON.stringify(tags) : '{}';
        __tropel_pm_custom_metric_add(this._name, Number(value), tagsStr, this._type);
    }
    return this;
};

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

// ── Export for module systems ──
if (typeof module !== 'undefined' && module.exports) {
    module.exports = pm;
}

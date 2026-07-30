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
                return pm.response.code() === code;
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
pm.expect = function (actual) {
    return {
        to: {
            eql: function (expected) {
                var passed = actual === expected;
                var name = 'expect ' + JSON.stringify(actual) + ' to eql ' + JSON.stringify(expected);
                if (typeof __tropel_pm_test === 'function') {
                    __tropel_pm_test(name, passed);
                }
                if (!passed) throw new Error(name + ' failed');
            },
            equal: function (expected) {
                return this.eql(expected);
            },
            include: function (expected) {
                var passed = String(actual).indexOf(String(expected)) !== -1;
                var name = 'expect to include ' + JSON.stringify(expected);
                if (typeof __tropel_pm_test === 'function') {
                    __tropel_pm_test(name, passed);
                }
                if (!passed) throw new Error(name + ' failed');
            },
            have: {
                property: function (prop, value) {
                    var obj = actual;
                    var has = obj && (prop in obj);
                    var passed = has;
                    if (has && value !== undefined) {
                        passed = obj[prop] === value;
                    }
                    var name = 'expect to have property ' + prop;
                    if (typeof __tropel_pm_test === 'function') {
                        __tropel_pm_test(name, passed);
                    }
                    if (!passed) throw new Error(name + ' failed');
                    return passed;
                },
                status: function (code) {
                    return pm.response.to.have.status(code);
                },
                header: function (key, value) {
                    var header = pm.response.header(key);
                    var passed = header === value;
                    var name = 'expect header ' + key + ' = ' + value;
                    if (typeof __tropel_pm_test === 'function') {
                        __tropel_pm_test(name, passed);
                    }
                    if (!passed) throw new Error(name + ' failed');
                    return passed;
                },
                jsonBody: function (expected) {
                    var body = pm.response.json();
                    var passed = JSON.stringify(body) === JSON.stringify(expected);
                    var name = 'expect response body to match';
                    if (typeof __tropel_pm_test === 'function') {
                        __tropel_pm_test(name, passed);
                    }
                    if (!passed) throw new Error(name + ' failed');
                    return passed;
                }
            },
            match: function (regex) {
                var passed = regex.test(String(actual));
                var name = 'expect to match ' + regex;
                if (typeof __tropel_pm_test === 'function') {
                    __tropel_pm_test(name, passed);
                }
                if (!passed) throw new Error(name + ' failed');
            }
        },
        not: {
            to: {
                eql: function (expected) {
                    var passed = actual !== expected;
                    var name = 'expect ' + JSON.stringify(actual) + ' not to eql ' + JSON.stringify(expected);
                    if (typeof __tropel_pm_test === 'function') {
                        __tropel_pm_test(name, passed);
                    }
                    if (!passed) throw new Error(name + ' failed');
                }
            }
        }
    };
};

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
                                var formPairs = [];
                                for (var k = 0; k < options.body.formdata.length; k++) {
                                    var fp = options.body.formdata[k];
                                    if (fp && fp.key) {
                                        formPairs.push(encodeURIComponent(fp.key) + '=' + encodeURIComponent(fp.value || ''));
                                    }
                                }
                                body = formPairs.join('&');
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
            timeout
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

// ── Export for module systems ──
if (typeof module !== 'undefined' && module.exports) {
    module.exports = pm;
}

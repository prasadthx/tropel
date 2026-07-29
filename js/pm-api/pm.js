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
            return __tropel_pm_iteration_data_get(key);
        }
        return null;
    }
};

// ── pm.sendRequest (for chaining requests within a test) ──
pm.sendRequest = function (options, callback) {
    // Delegate to native implementation
    if (typeof __tropel_pm_send_request === 'function') {
        // Extract request fields as strings (bridge doesn't accept JS objects)
        var url = (options && options.url) || (typeof options === 'string' ? options : '');
        var method = (options && options.method) || 'GET';
        var headers = (options && options.headers) || {};
        var body = (options && options.body) || '';

        var resultJson = __tropel_pm_send_request(
            method.toUpperCase(),
            url,
            JSON.stringify(headers),
            typeof body === 'string' ? body : JSON.stringify(body)
        );

        // Fire callback with the response
        if (callback) {
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

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
            return __tropel_pm_variables_get(key);
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
            return __tropel_pm_response_json();
        }
        return null;
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
    // Delegate to native implementation if available
    if (typeof __tropel_pm_send_request === 'function') {
        return __tropel_pm_send_request(options, callback);
    }

    // Fallback: attempt a simple XMLHttpRequest
    try {
        var xhr = new XMLHttpRequest();
        var method = (options.method || 'GET').toUpperCase();
        var url = options.url || options;

        xhr.open(method, url, true);

        if (options.headers) {
            for (var key in options.headers) {
                if (options.headers.hasOwnProperty(key)) {
                    xhr.setRequestHeader(key, options.headers[key]);
                }
            }
        }

        if (options.body) {
            xhr.send(options.body);
        } else {
            xhr.send();
        }

        xhr.onload = function () {
            if (callback) {
                callback(null, {
                    code: xhr.status,
                    text: function () { return xhr.responseText; },
                    json: function () {
                        try { return JSON.parse(xhr.responseText); }
                        catch (e) { return null; }
                    },
                    headers: function () {
                        var h = {};
                        xhr.getAllResponseHeaders().split('\r\n').forEach(function (line) {
                            var parts = line.split(': ');
                            if (parts.length >= 2) {
                                h[parts[0].toLowerCase()] = parts.slice(1).join(': ');
                            }
                        });
                        return h;
                    }
                });
            }
        };

        xhr.onerror = function () {
            if (callback) {
                callback(new Error('Request failed'), null);
            }
        };
    } catch (e) {
        if (callback) {
            callback(e, null);
        }
    }
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

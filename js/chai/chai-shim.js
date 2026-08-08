// ─── Chai Assertion Library Shim for Tropel ──────────────
// A simplified chai-compatible assertion library that delegates
// heavy operations to the native Rust assert module.

// Global chai
var chai = chai || {};

(function () {
    // Proper JS deep-equal (handles NaN, undefined, key-order)
    function jsDeepEqual(a, b) {
        if (a === b) return true;
        if (typeof a === 'number' && typeof b === 'number' && isNaN(a) && isNaN(b)) return true;
        if (a === null || b === null || a === undefined || b === undefined) return a === b;
        if (typeof a !== typeof b) return false;
        if (Array.isArray(a)) {
            if (!Array.isArray(b) || a.length !== b.length) return false;
            for (var i = 0; i < a.length; i++) {
                if (!jsDeepEqual(a[i], b[i])) return false;
            }
            return true;
        }
        if (typeof a === 'object') {
            if (Array.isArray(b) || b === null || b === undefined) return false;
            var keysA = Object.keys(a).sort();
            var keysB = Object.keys(b).sort();
            if (keysA.length !== keysB.length) return false;
            for (var i = 0; i < keysA.length; i++) {
                if (keysA[i] !== keysB[i]) return false;
                if (!jsDeepEqual(a[keysA[i]], b[keysB[i]])) return false;
            }
            return true;
        }
        return a === b;
    }

    // Use native deep-equal via JSON-string bridge if available
    var nativeDeepEqual = (typeof __tropel_native_deep_equal === 'function')
        ? function (a, b) {
            // Handle NaN/undefined in JS before calling native bridge
            if (typeof a === 'number' && typeof b === 'number' && isNaN(a) && isNaN(b)) return true;
            if (a === b) return true;
            if (a === null || a === undefined || b === null || b === undefined) return a === b;
            return __tropel_native_deep_equal(JSON.stringify(a), JSON.stringify(b));
        }
        : jsDeepEqual;

    // ── Assertion Constructor ──
    function Assertion(obj, msg, ssfi) {
        this._obj = obj;
        this._msg = msg;
        this._ssfi = ssfi || Assertion;
        // Initialize the flags bag up front. Without this, a plain chain
        // (no `.not`/`.deep` accessed) leaves `__flags` undefined and the
        // `var negate = this.__flags && this.__flags.negate` idiom below
        // yields `undefined` — and `(false) !== undefined` is TRUE, so every
        // positive assertion silently passed. The `!!(...)` conversions below
        // are the actual fix; this init keeps the getters' `|| {}` harmless.
        this.__flags = {};
    }

    // ── Chainable properties ──
    Object.defineProperties(Assertion.prototype, {
        to: { get: function () { return this; }, enumerable: true },
        be: { get: function () { return this; }, enumerable: true },
        been: { get: function () { return this; }, enumerable: true },
        is: { get: function () { return this; }, enumerable: true },
        that: { get: function () { return this; }, enumerable: true },
        which: { get: function () { return this; }, enumerable: true },
        and: { get: function () { return this; }, enumerable: true },
        has: { get: function () { return this; }, enumerable: true },
        have: { get: function () { return this; }, enumerable: true },
        with: { get: function () { return this; }, enumerable: true },
        at: { get: function () { return this; }, enumerable: true },
        of: { get: function () { return this; }, enumerable: true },
        same: { get: function () { return this; }, enumerable: true },
        not: {
            get: function () {
                this.__flags = this.__flags || {};
                this.__flags.negate = true;
                return this;
            },
            enumerable: true
        },
        deep: {
            get: function () {
                this.__flags = this.__flags || {};
                this.__flags.deep = true;
                return this;
            },
            enumerable: true
        },
        a: {
            get: function () { return this; },
            enumerable: true
        },
        an: {
            get: function () { return this; },
            enumerable: true
        }
    });

    // ── Assertion Methods ──

    // .equal(expected)
    Assertion.prototype.equal = function (value) {
        var negate = !!(this.__flags && this.__flags.negate);
        var passed = (this._obj === value) !== negate;
        if (!passed) {
            throw new Error(
                (this._msg ? this._msg + ': ' : '') +
                'expected ' + JSON.stringify(this._obj) +
                (negate ? ' not' : '') + ' to equal ' + JSON.stringify(value)
            );
        }
        return this;
    };

    // .eql(expected) — deep equality
    Assertion.prototype.eql = function (value) {
        var negate = !!(this.__flags && this.__flags.negate);
        var deep = !!(this.__flags && this.__flags.deep);
        var passed;

        if (deep || true) {
            // Always use deep comparison for .eql
            passed = nativeDeepEqual(this._obj, value) !== negate;
        } else {
            passed = (this._obj === value) !== negate;
        }

        if (!passed) {
            throw new Error(
                (this._msg ? this._msg + ': ' : '') +
                'expected ' + JSON.stringify(this._obj) +
                (negate ? ' not' : '') + ' to deeply equal ' + JSON.stringify(value)
            );
        }
        return this;
    };

    // .include(value)
    Assertion.prototype.include = function (value) {
        var obj = this._obj;
        var negate = !!(this.__flags && this.__flags.negate);
        var passed;

        if (typeof obj === 'string') {
            passed = (obj.indexOf(value) !== -1) !== negate;
        } else if (Array.isArray(obj)) {
            passed = (obj.indexOf(value) !== -1) !== negate;
        } else if (typeof obj === 'object' && obj !== null) {
            passed = (value in obj) !== negate;
        } else {
            passed = false;
        }

        if (!passed) {
            throw new Error(
                (this._msg ? this._msg + ': ' : '') +
                'expected ' + JSON.stringify(obj) +
                (negate ? ' not' : '') + ' to include ' + JSON.stringify(value)
            );
        }
        return this;
    };

    // .ok
    Object.defineProperty(Assertion.prototype, 'ok', {
        get: function () {
            var negate = !!(this.__flags && this.__flags.negate);
            var passed = !!this._obj !== negate;
            if (!passed) {
                throw new Error(
                    (this._msg ? this._msg + ': ' : '') +
                    'expected ' + JSON.stringify(this._obj) +
                    (negate ? ' not' : '') + ' to be truthy'
                );
            }
            return this;
        },
        enumerable: true
    });

    // .true
    Object.defineProperty(Assertion.prototype, 'true', {
        get: function () {
            var negate = !!(this.__flags && this.__flags.negate);
            var passed = (this._obj === true) !== negate;
            if (!passed) {
                throw new Error(
                    'expected ' + this._obj +
                    (negate ? ' not' : '') + ' to be true'
                );
            }
            return this;
        },
        enumerable: true
    });

    // .false
    Object.defineProperty(Assertion.prototype, 'false', {
        get: function () {
            var negate = !!(this.__flags && this.__flags.negate);
            var passed = (this._obj === false) !== negate;
            if (!passed) {
                throw new Error(
                    'expected ' + this._obj +
                    (negate ? ' not' : '') + ' to be false'
                );
            }
            return this;
        },
        enumerable: true
    });

    // .null
    Object.defineProperty(Assertion.prototype, 'null', {
        get: function () {
            var negate = !!(this.__flags && this.__flags.negate);
            var passed = (this._obj === null) !== negate;
            if (!passed) {
                throw new Error(
                    'expected ' + JSON.stringify(this._obj) +
                    (negate ? ' not' : '') + ' to be null'
                );
            }
            return this;
        },
        enumerable: true
    });

    // .undefined
    Object.defineProperty(Assertion.prototype, 'undefined', {
        get: function () {
            var negate = !!(this.__flags && this.__flags.negate);
            var passed = (this._obj === undefined) !== negate;
            if (!passed) {
                throw new Error(
                    'expected ' + JSON.stringify(this._obj) +
                    (negate ? ' not' : '') + ' to be undefined'
                );
            }
            return this;
        },
        enumerable: true
    });

    // .property(name[, value])
    Assertion.prototype.property = function (name, value) {
        var obj = this._obj;
        var negate = !!(this.__flags && this.__flags.negate);
        var has = obj !== null && obj !== undefined && name in obj;
        var passed = has !== negate;

        if (passed && value !== undefined) {
            passed = (obj[name] === value) !== negate;
        }

        if (!passed) {
            throw new Error(
                'expected ' + JSON.stringify(obj) +
                (negate ? ' not' : '') + ' to have property ' + name
            );
        }
        return this;
    };

    // .lengthOf(n)
    Assertion.prototype.lengthOf = function (n) {
        var obj = this._obj;
        var negate = !!(this.__flags && this.__flags.negate);
        var passed;

        if (typeof obj === 'string' || Array.isArray(obj)) {
            passed = (obj.length === n) !== negate;
        } else if (typeof obj === 'object' && obj !== null) {
            passed = (Object.keys(obj).length === n) !== negate;
        } else {
            passed = false;
        }

        if (!passed) {
            throw new Error(
                'expected ' + JSON.stringify(obj) +
                (negate ? ' not' : '') + ' to have length ' + n
            );
        }
        return this;
    };

    // .match(regexp)
    Assertion.prototype.match = function (re) {
        var obj = String(this._obj);
        var negate = !!(this.__flags && this.__flags.negate);
        var passed = re.test(obj) !== negate;
        if (!passed) {
            throw new Error(
                'expected ' + JSON.stringify(obj) +
                (negate ? ' not' : '') + ' to match ' + re
            );
        }
        return this;
    };

    // .string(string)
    Assertion.prototype.string = function (str) {
        var obj = String(this._obj);
        var negate = !!(this.__flags && this.__flags.negate);
        var passed = (obj.indexOf(str) !== -1) !== negate;
        if (!passed) {
            throw new Error(
                'expected ' + JSON.stringify(obj) +
                (negate ? ' not' : '') + ' to contain ' + JSON.stringify(str)
            );
        }
        return this;
    };

    // .keys(key1, key2, ...)
    Assertion.prototype.keys = function () {
        var obj = this._obj;
        var expectedKeys = Array.prototype.slice.call(arguments);
        var negate = !!(this.__flags && this.__flags.negate);
        var passed;

        if (expectedKeys.length === 1 && Array.isArray(expectedKeys[0])) {
            expectedKeys = expectedKeys[0];
        }

        if (obj && typeof obj === 'object') {
            var objKeys = Object.keys(obj);
            passed = expectedKeys.every(function (k) { return objKeys.indexOf(k) !== -1; }) !== negate;
        } else {
            passed = false;
        }

        if (!passed) {
            throw new Error(
                'expected ' + JSON.stringify(obj) +
                (negate ? ' not' : '') + ' to have keys ' + JSON.stringify(expectedKeys)
            );
        }
        return this;
    };

    // ── chai.expect ──
    // Backlog §1: unimplemented assertion PROPERTIES (e.g. `.to.be.empty`,
    // `.to.exist`, `.NaN`, `.finite`) used to read as `undefined` and the
    // enclosing pm.test/check recorded GREEN — a silent pass. The instance is
    // wrapped in a Proxy whose `get` trap THROWS on unknown assertion names,
    // so a typo'd or unimplemented assertion fails instead of passing
    // silently. Known names resolve normally through the prototype chain.
    chai.expect = function (val, msg) {
        var assertion = new Assertion(val, msg, chai.expect);
        return new Proxy(assertion, {
            get: function (target, prop, receiver) {
                // Symbols (Symbol.toPrimitive etc.) and the standard
                // inspection/promise interop names must resolve normally — a
                // `console.log(assertion)` or `JSON.stringify(assertion)`
                // would otherwise hit the guard and throw spuriously.
                if (
                    typeof prop === 'symbol' ||
                    prop === 'then' || prop === 'toJSON' || prop === 'inspect'
                ) {
                    return Reflect.get(target, prop, receiver);
                }
                if (prop in Assertion.prototype || prop in target) {
                    return Reflect.get(target, prop, receiver);
                }
                throw new Error("unknown assertion property '" + String(prop) + "'");
            }
        });
    };

    // .empty
    Object.defineProperty(Assertion.prototype, 'empty', {
        get: function () {
            var negate = !!(this.__flags && this.__flags.negate);
            var obj = this._obj;
            var empty = (typeof obj === 'string' || Array.isArray(obj))
                ? obj.length === 0
                : (obj !== null && typeof obj === 'object') && Object.keys(obj).length === 0;
            var passed = empty !== negate;
            if (!passed) {
                throw new Error(
                    (this._msg ? this._msg + ': ' : '') +
                    'expected ' + JSON.stringify(obj) + (negate ? ' not' : '') + ' to be empty'
                );
            }
            return this;
        },
        enumerable: true
    });

    // .exist
    Object.defineProperty(Assertion.prototype, 'exist', {
        get: function () {
            var negate = !!(this.__flags && this.__flags.negate);
            var exists = this._obj !== null && this._obj !== undefined;
            var passed = exists !== negate;
            if (!passed) {
                throw new Error(
                    (this._msg ? this._msg + ': ' : '') +
                    'expected ' + JSON.stringify(this._obj) + (negate ? ' not' : '') + ' to exist'
                );
            }
            return this;
        },
        enumerable: true
    });

    // .NaN
    Object.defineProperty(Assertion.prototype, 'NaN', {
        get: function () {
            var negate = !!(this.__flags && this.__flags.negate);
            var nan = typeof this._obj === 'number' && isNaN(this._obj);
            var passed = nan !== negate;
            if (!passed) {
                throw new Error(
                    (this._msg ? this._msg + ': ' : '') +
                    'expected ' + JSON.stringify(this._obj) + (negate ? ' not' : '') + ' to be NaN'
                );
            }
            return this;
        },
        enumerable: true
    });

    // .finite
    Object.defineProperty(Assertion.prototype, 'finite', {
        get: function () {
            var negate = !!(this.__flags && this.__flags.negate);
            var finite = typeof this._obj === 'number' && isFinite(this._obj);
            var passed = finite !== negate;
            if (!passed) {
                throw new Error(
                    (this._msg ? this._msg + ': ' : '') +
                    'expected ' + JSON.stringify(this._obj) + (negate ? ' not' : '') + ' to be finite'
                );
            }
            return this;
        },
        enumerable: true
    });

    // ── chai.assert ──
    chai.assert = {
        isOk: function (val, msg) {
            if (!val) throw new Error(msg || 'expected ' + JSON.stringify(val) + ' to be truthy');
        },
        isNotOk: function (val, msg) {
            if (val) throw new Error(msg || 'expected ' + JSON.stringify(val) + ' to be falsy');
        },
        equal: function (act, exp, msg) {
            if (act !== exp) throw new Error(msg || 'expected ' + JSON.stringify(act) + ' to equal ' + JSON.stringify(exp));
        },
        notEqual: function (act, exp, msg) {
            if (act === exp) throw new Error(msg || 'expected ' + JSON.stringify(act) + ' not to equal ' + JSON.stringify(exp));
        },
        deepEqual: function (act, exp, msg) {
            if (!nativeDeepEqual(act, exp)) throw new Error(msg || 'expected deep equality');
        },
        isTrue: function (val, msg) {
            if (val !== true) throw new Error(msg || 'expected ' + JSON.stringify(val) + ' to be true');
        },
        isFalse: function (val, msg) {
            if (val !== false) throw new Error(msg || 'expected ' + JSON.stringify(val) + ' to be false');
        },
        isNull: function (val, msg) {
            if (val !== null) throw new Error(msg || 'expected null');
        },
        isNotNull: function (val, msg) {
            if (val === null) throw new Error(msg || 'expected not null');
        },
        isUndefined: function (val, msg) {
            if (val !== undefined) throw new Error(msg || 'expected undefined');
        },
        isDefined: function (val, msg) {
            if (val === undefined) throw new Error(msg || 'expected defined');
        },
        isString: function (val, msg) {
            if (typeof val !== 'string') throw new Error(msg || 'expected a string');
        },
        isNumber: function (val, msg) {
            if (typeof val !== 'number') throw new Error(msg || 'expected a number');
        },
        isBoolean: function (val, msg) {
            if (typeof val !== 'boolean') throw new Error(msg || 'expected a boolean');
        },
        isArray: function (val, msg) {
            if (!Array.isArray(val)) throw new Error(msg || 'expected an array');
        },
        isObject: function (val, msg) {
            if (typeof val !== 'object' || val === null || Array.isArray(val)) throw new Error(msg || 'expected an object');
        },
        isFunction: function (val, msg) {
            if (typeof val !== 'function') throw new Error(msg || 'expected a function');
        },
        include: function (haystack, needle, msg) {
            if (haystack.indexOf(needle) === -1) throw new Error(msg || 'expected to include ' + JSON.stringify(needle));
        },
        match: function (val, re, msg) {
            if (!re.test(val)) throw new Error(msg || 'expected to match ' + re);
        },
        lengthOf: function (val, n, msg) {
            if (val.length !== n) throw new Error(msg || 'expected length ' + n + ' got ' + val.length);
        },
        fail: function (msg) {
            throw new Error(msg || 'Assertion failed');
        },
        throws: function (fn, err, msg) {
            // The "expected function to throw" error must be thrown AFTER the
            // try/catch — previously it was thrown inside the try and then
            // caught by its own catch, so assert.throws() always passed even
            // when nothing threw (the error fell through and returned).
            var threw = false;
            var caught;
            try {
                fn();
            } catch (e) {
                threw = true;
                caught = e;
            }
            if (!threw) {
                throw new Error(msg || 'expected function to throw');
            }
            if (err && typeof err === 'function' && !(caught instanceof err)) {
                throw new Error(
                    msg || 'expected function to throw ' + err.name + ' but threw ' + (caught && caught.name)
                );
            }
            if (typeof err === 'string' && caught.message !== err) {
                throw new Error(msg || 'expected error message ' + err + ' got ' + caught.message);
            }
        },
        doesNotThrow: function (fn, msg) {
            try {
                fn();
            } catch (e) {
                throw new Error(msg || 'expected function not to throw: ' + e.message);
            }
        }
    };

    // ── chai.should (minimal) ──
    chai.should = function () {
        Object.defineProperty(Object.prototype, 'should', {
            get: function () {
                return new Assertion(this);
            },
            set: function () {},
            configurable: true,
            enumerable: false
        });
    };
})();

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = chai;
}

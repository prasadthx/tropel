// ─── Chai Assertion Library Shim for Tropel ──────────────
// A simplified chai-compatible assertion library that delegates
// heavy operations to the native Rust assert module.

// Global chai
var chai = chai || {};

(function () {
    // Use native deep-equal if available
    var nativeDeepEqual = (typeof __tropel_native_deep_equal === 'function')
        ? __tropel_native_deep_equal
        : function (a, b) { return JSON.stringify(a) === JSON.stringify(b); };

    // ── Assertion Constructor ──
    function Assertion(obj, msg, ssfi) {
        this._obj = obj;
        this._msg = msg;
        this._ssfi = ssfi || Assertion;
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
        var negate = this.__flags && this.__flags.negate;
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
        var negate = this.__flags && this.__flags.negate;
        var deep = this.__flags && this.__flags.deep;
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
        var negate = this.__flags && this.__flags.negate;
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
            var negate = this.__flags && this.__flags.negate;
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
            var negate = this.__flags && this.__flags.negate;
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
            var negate = this.__flags && this.__flags.negate;
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
            var negate = this.__flags && this.__flags.negate;
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
            var negate = this.__flags && this.__flags.negate;
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
        var negate = this.__flags && this.__flags.negate;
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
        var negate = this.__flags && this.__flags.negate;
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
        var negate = this.__flags && this.__flags.negate;
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
        var negate = this.__flags && this.__flags.negate;
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
        var negate = this.__flags && this.__flags.negate;
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
    chai.expect = function (val, msg) {
        return new Assertion(val, msg, chai.expect);
    };

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
            try {
                fn();
                throw new Error(msg || 'expected function to throw');
            } catch (e) {
                if (err && e instanceof err) return;
                if (typeof err === 'string' && e.message !== err) throw new Error(msg || 'expected error message ' + err + ' got ' + e.message);
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

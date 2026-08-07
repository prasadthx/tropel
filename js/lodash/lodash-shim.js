// ─── Lodash Shim for Tropel ──────────────────────────────
// A minimal lodash-compatible library for common operations.
// Heavy operations delegate to native Rust functions when available.

var _ = _ || {};

(function () {
    // ── Array ──
    _.chunk = function (array, size) {
        // n===0 off-by-one (backlog line 155): size 0 -> [] (not 1-element
        // chunks), and undefined -> lodash default 1.
        size = size === undefined ? 1 : size;
        if (size <= 0) return [];
        var result = [];
        for (var i = 0; i < array.length; i += size) {
            result.push(array.slice(i, i + size));
        }
        return result;
    };

    _.compact = function (array) {
        return array.filter(function (x) { return x; });
    };

    _.concat = function () {
        var args = Array.prototype.slice.call(arguments);
        return args.reduce(function (acc, val) {
            return acc.concat(val);
        }, []);
    };

    _.difference = function (array, values) {
        return array.filter(function (x) { return values.indexOf(x) === -1; });
    };

    _.drop = function (array, n) {
        n = n === undefined ? 1 : n;
        return array.slice(n);
    };

    _.dropRight = function (array, n) {
        n = n === undefined ? 1 : n;
        return array.slice(0, Math.max(0, array.length - n));
    };

    _.fill = function (array, value, start, end) {
        start = start || 0;
        end = end || array.length;
        for (var i = start; i < end; i++) {
            array[i] = value;
        }
        return array;
    };

    _.findIndex = function (array, predicate, fromIndex) {
        fromIndex = fromIndex || 0;
        for (var i = fromIndex; i < array.length; i++) {
            if (typeof predicate === 'function' && predicate(array[i])) return i;
            if (typeof predicate === 'object' && predicate !== null) {
                var match = true;
                for (var k in predicate) {
                    if (array[i][k] !== predicate[k]) { match = false; break; }
                }
                if (match) return i;
            }
        }
        return -1;
    };

    _.first = function (array) { return array[0]; };
    _.head = function (array) { return array[0]; };
    _.last = function (array) { return array[array.length - 1]; };

    _.flatten = function (array) {
        var result = [];
        array.forEach(function (item) {
            if (Array.isArray(item)) {
                result = result.concat(item);
            } else {
                result.push(item);
            }
        });
        return result;
    };

    _.flattenDeep = function (array) {
        var result = [];
        function flatten(arr) {
            arr.forEach(function (item) {
                if (Array.isArray(item)) {
                    flatten(item);
                } else {
                    result.push(item);
                }
            });
        }
        flatten(array);
        return result;
    };

    _.fromPairs = function (pairs) {
        var result = {};
        pairs.forEach(function (pair) {
            result[pair[0]] = pair[1];
        });
        return result;
    };

    _.indexOf = function (array, value, fromIndex) {
        fromIndex = fromIndex || 0;
        return array.indexOf(value, fromIndex);
    };

    _.initial = function (array) {
        return array.slice(0, array.length - 1);
    };

    _.intersection = function () {
        var args = Array.prototype.slice.call(arguments);
        var first = args[0] || [];
        return first.filter(function (x) {
            return args.every(function (arr) { return arr.indexOf(x) !== -1; });
        });
    };

    _.nth = function (array, n) {
        n = n || 0;
        return n >= 0 ? array[n] : array[array.length + n];
    };

    _.pull = function (array) {
        var values = Array.prototype.slice.call(arguments, 1);
        for (var i = array.length - 1; i >= 0; i--) {
            if (values.indexOf(array[i]) !== -1) {
                array.splice(i, 1);
            }
        }
        return array;
    };

    _.pullAll = function (array, values) {
        return _.pull.apply(null, [array].concat(values));
    };

    _.remove = function (array, predicate) {
        var removed = [];
        for (var i = array.length - 1; i >= 0; i--) {
            if (predicate(array[i], i, array)) {
                removed.unshift(array.splice(i, 1)[0]);
            }
        }
        return removed;
    };

    _.slice = function (array, start, end) {
        return array.slice(start, end);
    };

    _.sortedIndex = function (array, value) {
        var low = 0, high = array.length;
        while (low < high) {
            var mid = (low + high) >>> 1;
            if (array[mid] < value) low = mid + 1;
            else high = mid;
        }
        return low;
    };

    _.sortedUniq = function (array) {
        var result = [];
        for (var i = 0; i < array.length; i++) {
            if (i === 0 || array[i] !== array[i - 1]) {
                result.push(array[i]);
            }
        }
        return result;
    };

    _.tail = function (array) { return array.slice(1); };
    _.take = function (array, n) { n = n === undefined ? 1 : n; return array.slice(0, n); };
    _.takeRight = function (array, n) { n = n === undefined ? 1 : n; return array.slice(Math.max(0, array.length - n)); };
    _.union = function () {
        var args = Array.prototype.slice.call(arguments);
        var result = [];
        args.forEach(function (arr) {
            arr.forEach(function (x) {
                if (result.indexOf(x) === -1) result.push(x);
            });
        });
        return result;
    };

    _.uniq = function (array) {
        return array.filter(function (x, i) { return array.indexOf(x) === i; });
    };

    _.without = function (array) {
        var values = Array.prototype.slice.call(arguments, 1);
        return array.filter(function (x) { return values.indexOf(x) === -1; });
    };

    _.zip = function () {
        var args = Array.prototype.slice.call(arguments);
        var maxLen = 0;
        args.forEach(function (a) { if (a.length > maxLen) maxLen = a.length; });
        var result = [];
        for (var i = 0; i < maxLen; i++) {
            result.push(args.map(function (a) { return a[i]; }));
        }
        return result;
    };

    // ── Collection ──
    _.each = function (collection, iteratee) {
        if (Array.isArray(collection)) {
            for (var i = 0; i < collection.length; i++) {
                if (iteratee(collection[i], i, collection) === false) break;
            }
        } else {
            for (var key in collection) {
                if (iteratee(collection[key], key, collection) === false) break;
            }
        }
        return collection;
    };

    _.forEach = _.each;

    _.every = function (collection, predicate) {
        // Iterate objects by key too, and honor object/matcher predicates
        // (backlog line 155): `_.every([{active:false}],{active:true})` must
        // be false — the old truthiness branch returned true.
        var keys = Array.isArray(collection) ? null : Object.keys(collection || {});
        var len = keys ? keys.length : (collection ? collection.length : 0);
        for (var i = 0; i < len; i++) {
            var item = keys ? collection[keys[i]] : collection[i];
            if (typeof predicate === 'function') {
                if (!predicate(item, keys ? keys[i] : i, collection)) return false;
            } else if (predicate !== null && typeof predicate === 'object') {
                var match = true;
                for (var k in predicate) {
                    if (item[k] !== predicate[k]) { match = false; break; }
                }
                if (!match) return false;
            } else {
                if (!item) return false;
            }
        }
        return true;
    };

    _.filter = function (collection, predicate) {
        // Object collections iterate by key (backlog line 155) — the old
        // `collection.length` loop returned empty for objects.
        var keys = Array.isArray(collection) ? null : Object.keys(collection || {});
        var len = keys ? keys.length : (collection ? collection.length : 0);
        var result = [];
        for (var i = 0; i < len; i++) {
            var item = keys ? collection[keys[i]] : collection[i];
            if (typeof predicate === 'function') {
                if (predicate(item, keys ? keys[i] : i, collection)) result.push(item);
            } else if (predicate !== null && typeof predicate === 'object') {
                var match = true;
                for (var k in predicate) {
                    if (item[k] !== predicate[k]) { match = false; break; }
                }
                if (match) result.push(item);
            } else {
                if (item) result.push(item);
            }
        }
        return result;
    };

    _.find = function (collection, predicate) {
        var keys = Array.isArray(collection) ? null : Object.keys(collection || {});
        var len = keys ? keys.length : (collection ? collection.length : 0);
        for (var i = 0; i < len; i++) {
            var item = keys ? collection[keys[i]] : collection[i];
            if (typeof predicate === 'function') {
                if (predicate(item, keys ? keys[i] : i, collection)) return item;
            } else if (predicate !== null && typeof predicate === 'object') {
                var match = true;
                for (var k in predicate) {
                    if (item[k] !== predicate[k]) { match = false; break; }
                }
                if (match) return item;
            } else if (predicate === undefined) {
                if (item) return item;
            }
        }
        return undefined;
    };

    _.includes = function (collection, value) {
        if (typeof collection === 'string') return collection.indexOf(value) !== -1;
        if (Array.isArray(collection)) return collection.indexOf(value) !== -1;
        if (typeof collection === 'object') {
            for (var key in collection) {
                if (collection[key] === value) return true;
            }
        }
        return false;
    };

    _.map = function (collection, iteratee) {
        // Object collections iterate by key (backlog line 155).
        var keys = Array.isArray(collection) ? null : Object.keys(collection || {});
        var len = keys ? keys.length : (collection ? collection.length : 0);
        var result = [];
        for (var i = 0; i < len; i++) {
            var item = keys ? collection[keys[i]] : collection[i];
            if (typeof iteratee === 'function') {
                result.push(iteratee(item, keys ? keys[i] : i, collection));
            } else if (typeof iteratee === 'string') {
                result.push(item[iteratee]);
            } else if (iteratee !== null && typeof iteratee === 'object') {
                var match = true;
                for (var k in iteratee) {
                    if (item[k] !== iteratee[k]) { match = false; break; }
                }
                result.push(!!match);
            } else {
                result.push(item);
            }
        }
        return result;
    };

    _.reject = function (collection, predicate) {
        return _.filter(collection, function (x) { return !predicate(x); });
    };

    _.size = function (collection) {
        if (typeof collection === 'string' || Array.isArray(collection)) return collection.length;
        if (typeof collection === 'object') return Object.keys(collection).length;
        return 0;
    };

    _.some = function (collection, predicate) {
        if (typeof predicate === 'function') {
            for (var i = 0; i < collection.length; i++) {
                if (predicate(collection[i], i, collection)) return true;
            }
        } else {
            for (var i = 0; i < collection.length; i++) {
                if (collection[i]) return true;
            }
        }
        return false;
    };

    _.sortBy = function (collection, iteratee) {
        var arr = collection.slice();
        var key = typeof iteratee === 'string' ? iteratee : null;
        arr.sort(function (a, b) {
            var va = key ? a[key] : iteratee(a);
            var vb = key ? b[key] : iteratee(b);
            if (va < vb) return -1;
            if (va > vb) return 1;
            return 0;
        });
        return arr;
    };

    // ── Function ──
    _.bind = function (func, thisArg) {
        var partials = Array.prototype.slice.call(arguments, 2);
        return function () {
            var args = partials.concat(Array.prototype.slice.call(arguments));
            return func.apply(thisArg, args);
        };
    };

    function scheduleTimeout(fn, delay) {
        if (typeof setTimeout === 'function') {
            return setTimeout(fn, delay);
        }
        if (typeof Promise === 'function' && typeof Promise.resolve === 'function') {
            Promise.resolve().then(fn);
            return null;
        }
        fn();
        return null;
    }

    function cancelTimeout(handle) {
        if (typeof clearTimeout === 'function') {
            clearTimeout(handle);
        }
    }

    _.debounce = function (func, wait) {
        var timeout;
        var pending = null; // { ctx, args } — latest call wins
        var scheduled = false;
        return function () {
            var context = this;
            var args = Array.prototype.slice.call(arguments);
            if (typeof clearTimeout === 'function') cancelTimeout(timeout);
            if (typeof setTimeout === 'function' && wait !== undefined) {
                timeout = setTimeout(function () { func.apply(context, args); }, wait);
                return;
            }
            // Timer-less runtime (backlog line 155): 3 sync calls must NOT
            // invoke 3 times. Coalesce into ONE trailing microtask flush
            // using the LATEST call's context/args.
            pending = { ctx: context, args: args };
            if (!scheduled) {
                scheduled = true;
                Promise.resolve().then(function () {
                    scheduled = false;
                    if (pending) {
                        var p = pending;
                        pending = null;
                        func.apply(p.ctx, p.args);
                    }
                });
            }
        };
    };

    _.throttle = function (func, wait) {
        // wait===undefined defaults to 0 (lodash) — the old `>= undefined`
        // comparison was always false, so a bare throttle NEVER fired
        // (backlog line 155).
        wait = wait || 0;
        var lastCall = 0;
        return function () {
            var now = Date.now();
            if (now - lastCall >= wait) {
                lastCall = now;
                func.apply(this, arguments);
            }
        };
    };

    // ── Lang ──
    _.clone = function (value) {
        if (value === null || typeof value !== 'object') return value;
        if (Array.isArray(value)) return value.slice();
        var result = {};
        for (var k in value) result[k] = value[k];
        return result;
    };

    _.cloneDeep = function (value) {
        // Real recursive deep clone — JSON round-trip threw on undefined /
        // cycles and converted Dates to strings (backlog line 155). Cycles
        // resolve to the in-progress clone; Dates/RegExps are preserved.
        var seen = [];
        function deep(v) {
            if (v === null || typeof v !== 'object') return v;
            if (v instanceof Date) return new Date(v.getTime());
            if (v instanceof RegExp) return new RegExp(v.source, v.flags);
            for (var s = 0; s < seen.length; s++) {
                if (seen[s].src === v) return seen[s].dst;
            }
            var out = Array.isArray(v) ? [] : {};
            seen.push({ src: v, dst: out });
            if (Array.isArray(v)) {
                for (var i = 0; i < v.length; i++) out[i] = deep(v[i]);
            } else {
                for (var k in v) {
                    if (Object.prototype.hasOwnProperty.call(v, k)) out[k] = deep(v[k]);
                }
            }
            return out;
        }
        return deep(value);
    };

    function isEqualDeep(a, b) {
        if (a === b) return true;
        if (typeof a === 'number' && typeof b === 'number' && isNaN(a) && isNaN(b)) return true;
        if (a === null || b === null || a === undefined || b === undefined) return a === b;
        if (typeof a !== typeof b) return false;
        if (Array.isArray(a)) {
            if (!Array.isArray(b) || a.length !== b.length) return false;
            for (var i = 0; i < a.length; i++) {
                if (!isEqualDeep(a[i], b[i])) return false;
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
                if (!isEqualDeep(a[keysA[i]], b[keysB[i]])) return false;
            }
            return true;
        }
        return a === b;
    }

    _.isEqual = function (a, b) {
        // Use native deep-equal via JSON-string bridge if available
        if (typeof __tropel_native_deep_equal === 'function') {
            if (typeof a === 'number' && typeof b === 'number' && isNaN(a) && isNaN(b)) return true;
            if (a === b) return true;
            if (a === null || a === undefined || b === null || b === undefined) return a === b;
            return __tropel_native_deep_equal(JSON.stringify(a), JSON.stringify(b));
        }
        return isEqualDeep(a, b);
    };

    _.isEmpty = function (value) {
        if (value === null || value === undefined) return true;
        if (typeof value === 'string' || Array.isArray(value)) return value.length === 0;
        return Object.keys(value).length === 0;
    };

    _.isNil = function (value) { return value === null || value === undefined; };
    _.isNull = function (value) { return value === null; };
    _.isUndefined = function (value) { return value === undefined; };
    _.isNumber = function (value) { return typeof value === 'number'; };
    _.isString = function (value) { return typeof value === 'string'; };
    _.isBoolean = function (value) { return typeof value === 'boolean'; };
    _.isArray = Array.isArray;
    _.isObject = function (value) { return value !== null && typeof value === 'object'; };
    _.isFunction = function (value) { return typeof value === 'function'; };
    _.isDate = function (value) { return value instanceof Date; };
    _.isRegExp = function (value) { return value instanceof RegExp; };
    _.toArray = function (value) {
        if (value === null || value === undefined) return [];
        if (Array.isArray(value)) return value.slice();
        if (typeof value === 'string') return value.split('');
        var result = [];
        for (var k in value) result.push(value[k]);
        return result;
    };

    _.toString = function (value) {
        if (value === null) return 'null';
        if (value === undefined) return 'undefined';
        return String(value);
    };

    // ── Math ──
    _.max = function (array) {
        return Math.max.apply(null, array);
    };
    _.min = function (array) {
        return Math.min.apply(null, array);
    };
    _.sum = function (array) {
        return array.reduce(function (a, b) { return a + b; }, 0);
    };

    // Collection reduce with optional accumulator; works on objects too
    // (backlog line 155: `_.reduce` did not exist).
    _.reduce = function (collection, iteratee, accumulator) {
        var keys = Array.isArray(collection) ? null : Object.keys(collection || {});
        var len = keys ? keys.length : (collection ? collection.length : 0);
        var hasAcc = arguments.length >= 3;
        var acc = hasAcc ? accumulator : undefined;
        var first = true;
        for (var i = 0; i < len; i++) {
            var item = keys ? collection[keys[i]] : collection[i];
            var idx = keys ? keys[i] : i;
            if (!hasAcc && first) { acc = item; first = false; continue; }
            acc = iteratee(acc, item, idx, collection);
        }
        return acc;
    };
    _.mean = function (array) {
        return _.sum(array) / array.length;
    };
    _.clamp = function (num, lower, upper) {
        return Math.min(Math.max(num, lower), upper);
    };
    _.random = function (lower, upper, floating) {
        if (upper === undefined) { upper = lower; lower = 0; }
        if (floating) {
            return lower + Math.random() * (upper - lower);
        }
        return Math.floor(lower + Math.random() * (upper - lower + 1));
    };

    // ── Number ──
    _.inRange = function (num, start, end) {
        if (end === undefined) { end = start; start = 0; }
        return num >= start && num < end;
    };

    // ── Object ──
    _.assign = function (object) {
        var sources = Array.prototype.slice.call(arguments, 1);
        sources.forEach(function (src) {
            if (src) {
                for (var k in src) {
                    if (src.hasOwnProperty(k) && !unsafeKey(k)) object[k] = src[k];
                }
            }
        });
        return object;
    };

    _.defaults = function (object) {
        var sources = Array.prototype.slice.call(arguments, 1);
        sources.forEach(function (src) {
            if (src) {
                for (var k in src) {
                    if (object[k] === undefined && !unsafeKey(k)) object[k] = src[k];
                }
            }
        });
        return object;
    };

    _.extend = _.assign;

    // Deep merge (backlog line 155: `_.merge` did not exist). Arrays and
    // plain objects merge recursively; prototype-pollution keys are skipped.
    _.merge = function (object) {
        var sources = Array.prototype.slice.call(arguments, 1);
        function mergeInto(target, src) {
            if (src === null || typeof src !== 'object') return src;
            if (Array.isArray(src)) {
                var arr = Array.isArray(target) ? target.slice() : [];
                for (var i = 0; i < src.length; i++) {
                    arr[i] = (typeof src[i] === 'object' && src[i] !== null && i < arr.length)
                        ? mergeInto(arr[i], src[i])
                        : src[i];
                }
                return arr;
            }
            if (src instanceof Date) return new Date(src.getTime());
            var out = (target !== null && typeof target === 'object' && !Array.isArray(target))
                ? target
                : {};
            for (var k in src) {
                if (src.hasOwnProperty(k) && !unsafeKey(k)) {
                    if (src[k] !== null && typeof src[k] === 'object' && !(src[k] instanceof Date)) {
                        out[k] = mergeInto(out[k], src[k]);
                    } else {
                        out[k] = src[k];
                    }
                }
            }
            return out;
        }
        sources.forEach(function (src) {
            if (src !== null && typeof src === 'object') {
                for (var k in src) {
                    if (src.hasOwnProperty(k) && !unsafeKey(k)) {
                        object[k] = mergeInto(object[k], src[k]);
                    }
                }
            }
        });
        return object;
    };

    // Parse lodash-style paths incl. bracket notation: 'a[0].b' -> ['a','0','b']
    // (backlog line 155: _.get on such paths returned undefined).
    function toPath(path) {
        if (Array.isArray(path)) return path;
        var parts = [];
        var re = /\.?([^.\[\]]+)|\[(\d+)\]/g;
        var m;
        while ((m = re.exec(String(path))) !== null) {
            if (m[1] !== undefined) parts.push(m[1]);
            else parts.push(parseInt(m[2], 10));
        }
        return parts;
    }

    function unsafeKey(k) {
        return k === '__proto__' || k === 'constructor' || k === 'prototype';
    }

    _.has = function (object, path) {
        var parts = toPath(path);
        var current = object;
        for (var i = 0; i < parts.length; i++) {
            if (current === null || current === undefined) return false;
            // `in` on primitives ('length' in 'bob') THROWS — box it.
            if (!(parts[i] in Object(current))) return false;
            current = current[parts[i]];
        }
        return true;
    };

    _.get = function (object, path, defaultValue) {
        var parts = toPath(path);
        var current = object;
        for (var i = 0; i < parts.length; i++) {
            if (current === null || current === undefined) return defaultValue;
            if (!(parts[i] in Object(current))) return defaultValue;
            current = current[parts[i]];
        }
        return current !== undefined ? current : defaultValue;
    };

    _.set = function (object, path, value) {
        // Block prototype-pollution keys (backlog line 155): setting
        // '__proto__.polluted' must not mutate Object.prototype.
        var parts = toPath(path);
        var current = object;
        for (var i = 0; i < parts.length - 1; i++) {
            if (unsafeKey(parts[i])) return object;
            if (!(parts[i] in Object(current))) current[parts[i]] = {};
            current = current[parts[i]];
        }
        var last = parts[parts.length - 1];
        if (!unsafeKey(last)) current[last] = value;
        return object;
    };

    _.keys = function (object) {
        if (object === null || object === undefined) return [];
        return Object.keys(object);
    };

    _.values = function (object) {
        if (object === null || object === undefined) return [];
        return Object.keys(object).map(function (k) { return object[k]; });
    };

    _.pairs = function (object) {
        if (object === null || object === undefined) return [];
        return Object.keys(object).map(function (k) { return [k, object[k]]; });
    };

    _.pick = function (object, paths) {
        if (typeof paths === 'string') paths = Array.prototype.slice.call(arguments, 1);
        var result = {};
        paths.forEach(function (path) {
            if (path in object) result[path] = object[path];
        });
        return result;
    };

    _.omit = function (object, paths) {
        if (typeof paths === 'string') paths = Array.prototype.slice.call(arguments, 1);
        var result = {};
        for (var k in object) {
            if (paths.indexOf(k) === -1) result[k] = object[k];
        }
        return result;
    };

    _.result = function (object, path, defaultValue) {
        var val = _.get(object, path);
        return val !== undefined ? (typeof val === 'function' ? val() : val) : defaultValue;
    };

    _.toPairs = _.pairs;
    _.fromPairs = function (pairs) {
        var result = {};
        pairs.forEach(function (p) { result[p[0]] = p[1]; });
        return result;
    };

    // ── String ──
    _.camelCase = function (str) {
        return str.replace(/[_-]+/g, ' ').replace(/\b\w/g, function (c, i) {
            return i === 0 ? c.toLowerCase() : c.toUpperCase();
        }).replace(/\s+/g, '');
    };

    _.capitalize = function (str) {
        str = String(str).toLowerCase();
        return str.charAt(0).toUpperCase() + str.slice(1);
    };

    _.endsWith = function (str, target, position) {
        position = position || str.length;
        return str.slice(0, position).slice(-target.length) === target;
    };

    _.escape = function (str) {
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    };

    _.kebabCase = function (str) {
        return str.replace(/([A-Z])/g, '-$1').toLowerCase().replace(/^[-_]+/, '').replace(/[_-]+/g, '-');
    };

    _.lowerCase = function (str) {
        return String(str).toLowerCase();
    };

    _.lowerFirst = function (str) {
        return str.charAt(0).toLowerCase() + str.slice(1);
    };

    _.pad = function (str, len, chars) {
        chars = chars || ' ';
        var totalPad = len - String(str).length;
        if (totalPad <= 0) return String(str);
        var left = Math.floor(totalPad / 2);
        var right = totalPad - left;
        return _.repeat(chars, Math.ceil(left / chars.length)).slice(0, left)
            + str
            + _.repeat(chars, Math.ceil(right / chars.length)).slice(0, right);
    };

    _.repeat = function (str, n) {
        var result = '';
        for (var i = 0; i < n; i++) result += str;
        return result;
    };

    _.replace = function (str, pattern, replacement) {
        return String(str).replace(pattern, replacement);
    };

    _.snakeCase = function (str) {
        return str.replace(/([A-Z])/g, '_$1').toLowerCase().replace(/^_/, '').replace(/[_-]+/g, '_');
    };

    _.split = function (str, separator, limit) {
        return String(str).split(separator, limit);
    };

    _.startsWith = function (str, target, position) {
        position = position || 0;
        return str.slice(position, position + target.length) === target;
    };

    _.toLower = function (str) { return String(str).toLowerCase(); };
    _.toUpper = function (str) { return String(str).toUpperCase(); };

    _.trim = function (str, chars) {
        if (chars) {
            var re = new RegExp('^[' + chars.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ']+|[' + chars.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ']+$', 'g');
            return String(str).replace(re, '');
        }
        return String(str).trim();
    };

    _.trimEnd = function (str, chars) {
        if (chars) {
            var re = new RegExp('[' + chars.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ']+$', 'g');
            return String(str).replace(re, '');
        }
        return String(str).trimEnd();
    };

    _.trimStart = function (str, chars) {
        if (chars) {
            var re = new RegExp('^[' + chars.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ']+', 'g');
            return String(str).replace(re, '');
        }
        return String(str).trimStart();
    };

    _.truncate = function (str, options) {
        options = options || {};
        var len = options.length || 30;
        var omission = options.omission || '...';
        var separator = options.separator;

        if (str.length <= len) return str;

        var result = str.slice(0, len - omission.length);
        if (separator) {
            var lastSep = result.lastIndexOf(separator);
            if (lastSep > 0) result = result.slice(0, lastSep);
        }

        return result + omission;
    };

    _.unescape = function (str) {
        return String(str)
            .replace(/&amp;/g, '&')
            .replace(/&lt;/g, '<')
            .replace(/&gt;/g, '>')
            .replace(/&quot;/g, '"')
            .replace(/&#39;/g, "'");
    };

    _.upperCase = function (str) { return String(str).toUpperCase(); };
    _.upperFirst = function (str) {
        return str.charAt(0).toUpperCase() + str.slice(1);
    };

    _.words = function (str, pattern) {
        if (pattern) return str.match(pattern) || [];
        return str.match(/[A-Z][a-z]+|[a-z]+|\d+/g) || [];
    };

    // ── Util ──
    _.constant = function (value) { return function () { return value; }; };
    _.identity = function (value) { return value; };
    _.noop = function () {};
    _.times = function (n, iteratee) {
        var result = [];
        for (var i = 0; i < n; i++) result.push(iteratee(i));
        return result;
    };
    _.uniqueId = function () {
        var id = 0;
        return function (prefix) {
            return (prefix || '') + (++id);
        };
    }();

    // ── Export ──
    if (typeof module !== 'undefined' && module.exports) {
        module.exports = _;
    }
})();

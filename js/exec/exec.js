// ─── exec.* / test.* API for Tropel ──────────────────
// Provides k6-compatible execution-context objects:
//   exec.scenario.name               — scenario name (string)
//   exec.scenario.executor           — executor type (string)
//   exec.vu.idInTest                 — unique VU identifier (number)
//   exec.vu.idInInstance             — VU id within this instance (number)
//   exec.vu.iterationInScenario      — current iteration (number)
//   exec.vu.iterationInInstance      — current iteration in instance (number)
//   exec.instance.iterationsCompleted — total completed iterations (number)
//   exec.instance.vusActive          — currently active VUs (number)
//   exec.test.abort([message])       — abort the test run
//
// Backlog line 141: in k6 these are VALUE properties, not functions. The old
// exec.js exposed functions (`exec.vu.idInTest` was a function object), so the
// two most common k6 idioms silently broke:
//   - `if (exec.vu.iterationInScenario === 0)` never fired (a function is
//     always truthy);
//   - `data[exec.vu.idInTest % len]` produced NaN → `undefined`.
// And `exec.test` did not exist, so `exec.test.abort()` threw a TypeError.
//
// The members are implemented as GETTERS, not plain values, because the native
// __tropel_exec_* bridges are registered LAZILY (on the first iteration, after
// this shim bootstraps). A getter re-fetches the bridge on every read, so
// per-iteration values (iteration count, VUs active) stay current while the
// READ shape is a plain number/string exactly like k6.

var exec = exec || {};

exec.scenario = {};
exec.vu = {};
exec.instance = {};

// Define a live value property backed by a native bridge function. Reads
// return a plain number/string (k6 shape), re-fetching the bridge each access
// so lazy registration + per-iteration state both work. `fallback` is used
// while the bridge is absent (init context, pre-registration).
function __tropel_liveProp(obj, key, bridgeName, fallback) {
    Object.defineProperty(obj, key, {
        get: function () {
            if (typeof globalThis !== 'undefined' && typeof globalThis[bridgeName] === 'function') {
                return globalThis[bridgeName]();
            }
            return fallback;
        },
        enumerable: true,
        configurable: true,
    });
}

__tropel_liveProp(exec.scenario, 'name', '__tropel_exec_scenario_name', '');
__tropel_liveProp(exec.scenario, 'executor', '__tropel_exec_scenario_executor', '');
__tropel_liveProp(exec.vu, 'idInTest', '__tropel_exec_vu_id', 0);
__tropel_liveProp(exec.vu, 'idInInstance', '__tropel_exec_vu_id', 0);
__tropel_liveProp(exec.vu, 'iterationInScenario', '__tropel_exec_iteration', 0);
__tropel_liveProp(exec.vu, 'iterationInInstance', '__tropel_exec_iteration', 0);
__tropel_liveProp(exec.instance, 'iterationsCompleted', '__tropel_exec_iterations_completed', 0);
__tropel_liveProp(exec.instance, 'vusActive', '__tropel_exec_vus_active', 0);

// ── Global test object ──
// k6 exposes the abort API as `exec.test.abort([message])` (NOT a bare `test`
// global). Both are provided here for compatibility: the PM path and some
// scripts use `test.abort`, while k6 scripts use `exec.test.abort`.
var test = test || {};

exec.test = {
    abort: function (message) {
        if (typeof __tropel_test_abort === 'function') {
            if (message === undefined || message === null) {
                message = 'Test aborted by script';
            }
            __tropel_test_abort(String(message));
        }
    }
};

test.abort = exec.test.abort;

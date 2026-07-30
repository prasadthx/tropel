// ─── exec.* / test.* API for Tropel ──────────────────
// Provides k6-compatible execution-context objects:
//   exec.scenario.name       — scenario name
//   exec.scenario.executor    — executor type
//   exec.vu.idInTest         — unique VU identifier
//   exec.vu.iterationInScenario — current iteration
//   exec.instance.iterationsCompleted — total iterations
//   exec.instance.vusActive  — currently active VUs
//   test.abort([message])    — abort the test run

// ── Global exec object ──
var exec = exec || {};

exec.scenario = {
    name: function () {
        if (typeof __tropel_exec_scenario_name === 'function') {
            return __tropel_exec_scenario_name();
        }
        return '';
    },
    executor: function () {
        if (typeof __tropel_exec_scenario_executor === 'function') {
            return __tropel_exec_scenario_executor();
        }
        return '';
    }
};

exec.vu = {
    idInTest: function () {
        if (typeof __tropel_exec_vu_id === 'function') {
            return __tropel_exec_vu_id();
        }
        return 0;
    },
    idInInstance: function () {
        // Same as idInTest for single-instance runs
        if (typeof __tropel_exec_vu_id === 'function') {
            return __tropel_exec_vu_id();
        }
        return 0;
    },
    iterationInScenario: function () {
        if (typeof __tropel_exec_iteration === 'function') {
            return __tropel_exec_iteration();
        }
        return 0;
    },
    iterationInInstance: function () {
        // Same as iterationInScenario for single-instance runs
        if (typeof __tropel_exec_iteration === 'function') {
            return __tropel_exec_iteration();
        }
        return 0;
    }
};

exec.instance = {
    iterationsCompleted: function () {
        if (typeof __tropel_exec_iterations_completed === 'function') {
            return __tropel_exec_iterations_completed();
        }
        return 0;
    },
    vusActive: function () {
        if (typeof __tropel_exec_vus_active === 'function') {
            return __tropel_exec_vus_active();
        }
        return 0;
    }
};

// ── Global test object ──
var test = test || {};

test.abort = function (message) {
    if (typeof __tropel_test_abort === 'function') {
        if (message === undefined || message === null) {
            message = 'Test aborted by script';
        }
        __tropel_test_abort(String(message));
    }
};

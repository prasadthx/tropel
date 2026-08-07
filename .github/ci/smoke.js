// Tropel CI smoke script — commit to: .github/ci/smoke.js
//
// Purpose: exercise the real binary end-to-end against a local server and
// produce metrics the workflow can assert on. A run that completes with zero
// http_reqs is the exact failure this guards against.
//
// The `check` import is deliberately MULTI-LINE to pin `strip_k6_virtual_imports`
// (an AST splice that must handle any syntactic form — the old line-anchored
// regex missed `import {\n check\n}` and killed init).
//
// GET-only: the CI target is python http.server, which serves GET/HEAD only.

import http from 'k6/http';
import {
  check,
  sleep,
} from 'k6';

const BASE = __ENV.TROPEL_SMOKE_BASE || 'http://127.0.0.1:8787';

export const options = {
  // Kept modest: CI runners are shared and this is a correctness gate,
  // not a benchmark.
  vus: 4,
  duration: '5s',
  thresholds: {
    // Achievable on localhost — if this ever breaches, something is wrong
    // with the HTTP path, not with the threshold.
    http_req_duration: ['p(95)<2000'],
    checks: ['rate>0.99'],
  },
};

export default function () {
  const res = http.get(`${BASE}/ok.json`);

  check(res, {
    'status is 200': (r) => r.status === 200,
    'body is non-empty': (r) => !!r.body && r.body.length > 0,
    'parses as json': (r) => {
      try {
        return r.json() !== undefined;
      } catch (e) {
        return false;
      }
    },
  });

  sleep(0.1);
}

// Tropel CI negative-control script — commit to: .github/ci/smoke-threshold-fail.js
//
// Purpose: prove the exit-code contract. Every threshold here is physically
// impossible to satisfy, so the process MUST exit non-zero. If it exits 0,
// thresholds are inert — a failure mode that has shipped before (a
// double-emitted metric silently made p(95) resolve to 0 and always pass).
//
// Two independent impossible thresholds are declared on purpose: if one form
// isn't parsed, it only warns, and the other still trips the gate.

import http from 'k6/http';

const BASE = __ENV.TROPEL_SMOKE_BASE || 'http://127.0.0.1:8787';

export const options = {
  vus: 2,
  duration: '3s',
  thresholds: {
    // Any completed request makes count >= 1, so "< 1" cannot hold.
    http_reqs: ['count<1'],
    // No real request completes in under a microsecond.
    http_req_duration: ['max<0.001'],
  },
};

export default function () {
  http.get(`${BASE}/ok.json`);
}

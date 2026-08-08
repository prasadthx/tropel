# Metrics & Thresholds

## Built-in metrics

| Metric | Type | Notes |
|--------|------|-------|
| `http_req_duration` | Trend | HDR histogram; percentiles p50/p90/p95/p99 |
| `http_req_failed` | Rate | Driven by expected status (2xx–3xx = success) |
| `http_req_blocked/connecting/tls_handshaking/sending/waiting/receiving` | Trend | Sub-timings (TTFB etc.) |
| `data_sent` / `data_received` | Counter | Wired from real byte counts |
| `iterations` / `iteration_duration` | Counter / Trend | |
| `vus` / `vus_max` | Gauge | Sampled over time |
| `dropped_iterations` | Counter | Open-model pool can't keep up |
| `checks` | Rate | From `check()` / `pm.test()` / `pm.response.to.have.status` |

Metrics are first-class types (Counter / Gauge / Rate / Trend) and custom
metrics can be added from scripts:

```js
const errors = new Counter("errors");
errors.add(1, { service: "api" });
```

## Tagged aggregation

Every sample carries tags (`status`, `method`, `name`, …) and metrics
aggregate per tag set — so `http_req_duration{status:200}` and
`{status:500}` are separate series. Tag keys are interned (Arc<str>) and tag
sets use small maps to keep allocation churn off the hot path.

## Thresholds

```bash
tropel run c.json --threshold "http_req_duration p95 < 500" \
                  --threshold "checks rate > 0.99"
```

**All duration threshold values are in MILLISECONDS** (k6's native unit —
backlog §0). `http_req_duration p95 < 500` means p95 < 500 ms, matching how
the same threshold behaves inside a k6 script.

- Metric-only expressions (`http_req_duration p95 < 500`) and tag-scoped
  expressions (`http_req_duration{status:200} p95 < 500`).
- **k6-compatible abort semantics**: thresholds are evaluated during the run;
  with `abortOnFail` the run aborts early on the first breach (k6 behavior),
  otherwise it completes and the final exit code reflects pass/fail.

## Output

See [Outputs & reporters](outputs.md). The hot path is lock-free — per-VU
thread-local sample buffers flushed over an MPSC channel to a single
aggregator with bounded buffering (no unbounded queue growth).

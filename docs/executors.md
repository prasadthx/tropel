# Execution Modes

Tropel ships six executors. They are selected by the scenario config (from a
collection, a k6 script's `options`, or a `--config` overlay).

| Executor | Description |
|----------|-------------|
| `constant-vus` | Fixed number of VUs, each looping for a duration |
| `ramping-vus` | VUs ramped up/down through stages (`startVUs`, `stages`) |
| `shared-iterations` | A total number of iterations distributed across VUs |
| `per-vu-iterations` | Each VU runs exactly N iterations |
| `constant-arrival-rate` | Open model: iterations started at a fixed rate from a growing VU pool (`preAllocatedVUs` → `maxVUs`); drops are counted in `dropped_iterations` when the pool can't keep up |
| `ramping-arrival-rate` | Arrival rate ramped through target-rate stages |

## Shared semantics

- **Graceful stop / ramp-down** — in-flight iterations finish within a grace
  window (`gracefulStop`); ramp-down only reduces the surplus VUs instead of
  stopping everyone.
- **maxDuration** — a cap for the fixed-iteration executors, raced against VU
  drain (`select!`), so a short run never blocks for the full cap.
- **Think time / pacing** — fixed `delay`, random `[min_delay, max_delay]`, or
  `iteration_pacing` (target iteration duration) between iterations.
- **Cancel propagation** — level-triggered stop (AtomicBool/CancellationToken
  checked each iteration), so a VU in the gap between iterations still stops;
  no missed one-shot broadcasts, no hangs in constant/arrival mode.

## Thread-per-core

Each VU runs on its own OS thread with a tokio current-thread runtime and its
own QuickJS context (contexts are `!Send` and thread-local, so there is no
per-context mutex). The orchestrator coordinates VUs and collects metrics via
an MPSC channel.

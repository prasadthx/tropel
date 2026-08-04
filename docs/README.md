# Tropel Documentation

> *Spanish: "a rushing throng; in droves"*

Tropel is a high-performance, open-source load-testing framework built in Rust.
It runs **Postman collections**, **HAR files**, **OpenAPI specs**, and **k6
scripts** as load tests with a native Rust hot path and an embedded QuickJS
engine for script execution.

## Getting started

- [Installation & first run](getting-started.md)
- [CLI reference](cli.md)

## Core concepts

- [Input formats](inputs.md) — Postman, HAR, OpenAPI, k6, subprocess, WASM
- [Execution modes](executors.md) — seven executors, from constant-VUs to ramping arrival rate and externally-controlled
- [Scripting](scripting.md) — `pm.*` API, k6 shim, native bridge, crypto/encoding/assert helpers
- [Metrics & thresholds](metrics.md) — HDR histograms, checks, threshold expressions

## Operating

- [Outputs & reporters](outputs.md) — stdout, JSON, CSV, Prometheus, OTLP, StatsD, InfluxDB, NDJSON
- [Extensions](extensions.md) — the SDK, `tropel build`, and WASM plugins
- [Distributed execution](distributed.md) — execution segments for multi-node runs

## Reference

- [Status & roadmap](roadmap.md) — honest, per-area capability matrix
- [Bridge functions](../BRIDGE_FUNCTIONS.md) — the complete native/JS bridge list
- [Architecture decisions](../TROPEL_CHALLENGES.md) — design challenges and how they were resolved

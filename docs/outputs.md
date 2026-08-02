# Outputs & Reporters

## Reporters (end-of-run)

| Reporter | Flags |
|----------|-------|
| stdout (default) | `-r stdout` |
| JSON | `-r json -o results.json` |
| CSV | `-r csv -o results.csv` |

## Streaming outputs (during the run)

An `Output` trait consumes an MPSC stream of samples on a dedicated task, so
metrics flow out as the test runs, not just at the end:

| Output | Flag |
|--------|------|
| NDJSON stream (k6 `--out json=`) | `--json-stream <file>` |
| StatsD / Datadog UDP | `--statsd-addr <host:port>` |
| InfluxDB line protocol UDP | `--influxdb-addr <host:port>` |
| Prometheus remote-write | `--prometheus-url <url>` |
| OTLP/HTTP | `--otlp-endpoint <url>` |
| k6-style summary object | `--summary-export <file>` |

## Per-request debugging

```bash
tropel run collection.json --http-debug
```

Logs `HTTP >>>` (method/url/body bytes/header count) before each send and
`HTTP <<<` (status/bytes/duration) after — at info level, so no `RUST_LOG`
tuning is needed.

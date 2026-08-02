# CLI Reference

```
Tropel — A high-performance load-testing framework

Usage: tropel <COMMAND>

Commands:
  run         Run a load test
  extensions  List available input formats and their capabilities
  inspect     Inspect an input file without running it
  archive     Bundle a test (input + deps + manifest) into a self-contained dir
  build       Build a custom Tropel binary with extensions
  version     Print the version and build information
```

## `tropel run`

```
Usage: tropel run [OPTIONS] <INPUT>

Arguments:
  <INPUT>  Path to the input file (collection, HAR, script, spec, …)

Options:
      --format <FORMAT>            Input format (auto-detect if omitted)
  -u, --vus <VUS>                  Number of virtual users
  -d, --duration <DURATION>        Test duration ("30s", "5m", "1h")
  -e, --env <KEY=VALUE>            Environment variable (repeatable)
  -E, --env-file <ENV_FILE>        Environment file (JSON)
  -D, --data-file <DATA_FILE>      Data file (CSV or JSON)
      --config <CONFIG>            Partial JobConfig JSON overlay. Precedence:
                                   CLI flags > config file > K6_* env > defaults
  -r, --reporter <REPORTER>        stdout, json, csv   [default: stdout]
  -o, --output <OUTPUT>            Output file path (json/csv reporters)
      --prometheus-url <URL>       Prometheus remote-write endpoint
      --otlp-endpoint <URL>        OTLP/HTTP collector endpoint
      --summary-export <PATH>      k6-style summary object as JSON
      --json-stream <PATH>         NDJSON stream of every sample (k6 --out json=)
      --statsd-addr <ADDR>         StatsD/Datadog UDP agent
      --influxdb-addr <ADDR>       InfluxDB line-protocol UDP
      --execution-segment <F:T>    Workload partition for this node (k6 executionSegment)
      --execution-segment-sequence <SEQ>
                                   Segment boundaries for a multi-node run
      --plugins-dir <DIR>          Directory of WASM input plugins
      --http-debug                 Log every HTTP request/response line
      --threshold <EXPR>           Threshold expression (repeatable)
  -k, --insecure                   Skip TLS certificate verification
```

### Multi-node (distributed) run

The distributed controller is a separate binary (see
[Distributed execution](distributed.md)):

```bash
tropel-distributed controller --agents 3 --config test.json
```

## `tropel inspect`

Shows the resolved adapter, scenario summary (name, request count, methods,
variables, auth), and any script-declared options — a dry-run of what a run
will execute.

## `tropel archive`

Bundles the input file plus referenced dependencies (data file, env file,
config file) and a `tropel-archive.json` manifest into a directory you can
replay on another machine:

```bash
tropel archive collection.json -o ./bundle
cd ./bundle && tropel run collection.json
```

## `tropel build`

Compile a custom binary with extension crates:

```bash
tropel build --with tropel-x-grpc --with ./my-ext --with https://github.com/u/r@v0.2.0
```

Names and versions are validated before being injected into the generated
`Cargo.toml` (`^[a-zA-Z0-9_-]+$`, semver for versions) to prevent build-time
code injection.

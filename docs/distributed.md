# Distributed Execution

`tropel-distributed` provides multi-node load tests. The workload is
partitioned deterministically with k6-style **execution segments**.

## How it works

1. A **controller** computes segment boundaries from the requested agent count
   (or an explicit `execution_segment_sequence`) and assigns each agent its
   `from:to` segment.
2. Each **agent** runs only its fraction of the workload — VUs, iterations,
   and arrival rate are scaled deterministically from the segment.
3. Agents report samples to the controller, which aggregates and evaluates
   thresholds.

## Binaries

- `tropel-distributed controller --agents N --config test.json`
- `tropel-distributed agent --controller <addr>`

## Single-binary run

```bash
# One node, split into two segments (e.g. two processes on one machine)
tropel run test.json --execution-segment 0:1/2 --execution-segment-sequence 0,1/2,1
tropel run test.json --execution-segment 1/2:1 --execution-segment-sequence 0,1/2,1
```

## Cloud mode

`run_cloud(config, agents)` plus Kubernetes manifest generation
(`generate_k8s_manifests`) for deploying agent pods.

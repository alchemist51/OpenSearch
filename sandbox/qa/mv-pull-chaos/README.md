# Separate-index pull-MV chaos harness

This harness repeatedly runs the production-path integration test with explicit
randomized-testing seeds and captures enough evidence to reproduce every run.
It is intended for the larger A/B machine after the small local smoke gate.

## What A/B means

- **A:** DataFusion Final over the `mv_state` Arrow files currently referenced
  by the target's primary and replica `CatalogSnapshot`s.
- **B:** The deterministic source-side group model accumulated from every
  indexed source document.

Every phase requires exact per-group count/sum equality. The test also checks
that files and watermark W are published in one catalog snapshot, replicas
preserve W, only the primary polls, and the target translog remains empty.

## Fault and lifecycle matrix

Each seed executes the following bounded sequence:

1. multiple explicit source checkpoint publications and target-owned pulls;
2. DFA NRT replica convergence using replica-local files;
3. deterministic primary relocation and build-service handoff;
4. full node-process restart with durable catalog/W recovery;
5. a new source generation after restart;
6. DataFusion state-to-state compaction to one immutable Arrow artifact;
7. exactness and replica convergence after compaction;
8. deletion of all superseded state artifacts.

Kernel-level packet loss, host power loss, and filesystem exhaustion are not
performed by this portable script. Add those around an executing run on the A/B
host only when the host's isolation and cleanup mechanism are known; never use
root-level fault injection against a shared or production machine.

## Local smoke gate

```bash
export CARGO_PROFILE_RELEASE_LTO=false
sandbox/qa/mv-pull-chaos/run.sh --runs 1 --timeout-seconds 900
```

## A/B run

Use a clean checkout of the same commit and preserve the output directory:

```bash
export CARGO_PROFILE_RELEASE_LTO=false
sandbox/qa/mv-pull-chaos/run.sh \
  --runs 20 \
  --timeout-seconds 1200 \
  --base-seed 5A17E1C500000000 \
  --results /absolute/path/to/persistent/mv-chaos-results
```

The harness never pushes, deploys, deletes cluster data, or changes host
network/disk configuration.

## Captured artifacts

Each run directory contains:

- `environment.txt`: UTC start, OS/JDK, Git commit/branch/status, and LTO mode;
- `matrix.txt`: A/B definitions, injected lifecycle faults, and invariants;
- `commands.log` and `run-NNN.command`: exact commands, seeds, exit codes, and timing;
- `logs/`: complete Gradle and test-cluster output per seed;
- `run-NNN.json`: per-seed status and duration;
- `results.json`: aggregate pass/fail summary.

A timed-out run exits `124`, is recorded as `TIMEOUT`, and its complete partial
log remains available. Reproduce a failed seed directly with the command file
or with Gradle's `REPRODUCE WITH` line in the captured log.

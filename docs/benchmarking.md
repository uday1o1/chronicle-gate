# Benchmarking

ChronicleGate performance comparison is deliberately separate from semantic qualification.
`chronicle run` owns correctness, fault injection, observers, confirmation, and minimization.
`chronicle bench` owns a fresh benchmark network, one hardened HTTP service at a time, raw timings, resource observations, a standalone report, and its own exit status.

## Authored contract

The public `BenchmarkWorkload` schema is `schemas/benchmark-workload.schema.json`.
Unknown fields fail before Docker access.
The contract bounds operations, headers, bodies, rates, durations, rounds, concurrency, response sizes, resource sampling, bootstrap work, request inventory, run duration, and artifact sizes.

The V1 schedule algorithm is `chronicle-schedule-v1`.
It uses SplitMix64 with documented fixed constants, rejection-sampled bounded integers, descending in-place Fisher-Yates treatment shuffling, cumulative integer-weight operation selection, and integer offsets `floor(ordinal * 1e9 / rate)` with the first send at offset zero.
The complete plan is generated and hashed before any measured container starts.
Each round executes baseline and candidate once, and exactly half the rounds are baseline-first.
Both members of a pair use the same precomputed request methods, paths, headers, bodies, IDs, operation selection, count, and send offsets.

## Open-loop execution

The sender launches each request at its precomputed monotonic offset without waiting for a prior response.
A bounded in-flight limit prevents a valid workload from creating unbounded goroutines.
Warmup and measurement are separate, and warmup samples never enter latency analysis.

The HTTP client permits only the exact Docker-assigned `127.0.0.1` endpoint.
Its dialer rejects every other address.
Environment proxies, redirects, HTTP/2 negotiation, automatic compression, and authored `Accept-Encoding` headers are disabled.
Response headers and bodies have independent hard limits with limit-plus-one detection.
Idle connections close before the measured container is removed.

## Trial validity and classification

Every valid trial must complete the exact authored measurement inventory, remain within the schedule-lag bound, retain its expected HTTP status, stay healthy before warmup and after measurement, and produce the required resource samples.
The effective container image, network, port binding, security profile, Docker healthcheck disablement, restart count, OOM state, and mount inventory are inspected before measurement.

Schema or semantic contract failure returns exit code `3` without creating output.
Docker, network, image, hardening, health, transport, sampler, artifact, or cleanup failures return `INFRASTRUCTURE_ERROR` with exit code `4`.
Request or overall deadlines return `TIMEOUT` with exit code `5`.
Schedule lag, in-flight exhaustion, response-size excess, unexpected status, or incomplete evidence returns `UNRESOLVED` with exit code `5`.
External cancellation produces the `INTERRUPTED` state and exit code `130`, unless cleanup or artifact failure overrides it with infrastructure precedence.
Only a complete valid matrix can return `PASS` with exit code `0` or `PERFORMANCE_REGRESSION` with exit code `2`.

## Locked analysis

Each trial sorts integer request latencies and uses nearest-rank p50, p95, and p99 with index `ceil(q * n) - 1`.
Throughput is successful measurement requests divided by the authored measurement duration.
Error rate is unsuccessful measurement requests divided by the exact measurement inventory.
The standalone result also pools all measurement latencies for each target to report descriptive p50, p95, p99, throughput, error rate, CPU delta, peak sampled memory, peak PID count, and throttling counters.
Every resource counter must remain monotonic within a trial, and a changed memory limit invalidates the evidence.

For round `r`, baseline p95 is `B_r`, candidate p95 is `C_r`, absolute delta is `A_r = C_r - B_r`, and relative delta is `D_r = A_r / B_r`.
`B_r` must be positive.
The absolute point estimate is the arithmetic mean of `A_r`, while the relative point estimate is the arithmetic mean of `D_r`.
Pooled target quantiles are descriptive and never drive classification.

The V1 analysis algorithm is `paired-percentile-bootstrap-v1` with block size one.
Every bootstrap replicate resamples complete round pairs with replacement and computes the mean relative delta.
The fixed-seed statistics are sorted, and the two-sided percentile endpoints use nearest-rank indices from the authored confidence level.
Displayed values never feed the decision.

A performance regression requires both conditions below.

- The exact integer sum of absolute deltas is at least the authored absolute threshold multiplied by the round count.
- The lower unrounded relative confidence bound is strictly greater than the authored practical relative threshold.

This conjunction prevents a statistically stable but negligible change from becoming a regression claim.

## Measured-path isolation

The benchmark launcher does not provision PostgreSQL or Redpanda.
It injects no correctness, broker, database, secret, probe, OpenTelemetry, debug, or profiler environment, mount, port, or sidecar.
It disables any image-authored Docker healthcheck and runs harness health requests only outside the measurement window.
All target containers retain the Milestone 8 non-root, read-only-root, dropped-capability, no-new-privileges, CPU, memory, PID, loopback-port, and Docker-socket controls.

Container inspection proves only that harness-side correctness instrumentation is absent from an arbitrary trusted image.
It cannot prove the internals of an arbitrary binary.
The repository reference image has a stronger trusted-source gate: its Docker build rejects probe, OpenTelemetry, Kafka, database, and profiler dependencies, and the live image identity and provenance label are verified.

## Artifacts

A benchmark output directory contains these private mode `0600` files.

- `execution-plan.json` preserves the complete request and treatment plan plus its hash.
- `raw-timings.ndjson` preserves scheduled, start, first-byte, and end monotonic offsets for every warmup and measurement request.
- `resource-samples.ndjson` preserves Docker CPU, memory, PID, and available throttling counters.
- `environment.json` preserves bounded host and Docker provenance without the process environment.
- `benchmark.json` is the standalone machine-readable result governed by `schemas/benchmark-result.schema.json`.
- `report.txt` and `report.html` are standalone human-readable views.
- `checksums.sha256` uses streaming SHA-256 over the immutable artifact inventory.

## Development and publication boundaries

Apple silicon with Colima is supported for local comparative development.
Local results do not establish production capacity and must not be presented as generally reproducible performance numbers.
GitHub-hosted runners may execute the functional benchmark gate but cannot produce portfolio performance claims.

Publication evidence requires the `publication` workload scope, an explicit `--dedicated-host` attestation, a native non-containerized Linux CLI host, a local Unix-socket Docker daemon, no shared CI environment, no other running Docker containers, and the required resource evidence.
Named immutable OCI references are resolved by inspecting the authored digest, selecting exactly one matching runtime descriptor while ignoring non-runtime attestations, pulling that child digest, and verifying that Docker retains the selected platform digest and matching OS and architecture for the executed config image.
Bare local config-image IDs are accepted only with `--development-local-images` and are always rejected for publication evidence.
Publication preflight records a 30-second `/proc/stat` and `/proc/loadavg` idle sample and requires at least 90 percent idle CPU with normalized one-minute load no greater than 0.25.
Before every target trial it repeats a two-second idle window and verifies that no other Docker container is running.
If those conditions are absent, ChronicleGate refuses publication-scoped evidence before measurement.

## Acceptance commands

The non-performance verification gate remains separate.

```sh
make verify
make test-benchmark
```

`make test-benchmark` runs the public CLI twice for A/A and twice for the seeded slowdown.
It checks the complete paired inventory, raw artifacts, checksums, instrumentation exclusions, expected classifications, and exact Docker cleanup.

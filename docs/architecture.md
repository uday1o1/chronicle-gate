# Architecture

ChronicleGate is a local release-qualification CLI for trusted Kafka-style stateful services.
It compares a baseline target with a candidate under the same authored scenario, infrastructure locks, database state, broker inputs, and observation contract.
The product boundary ends at the local CLI, Docker daemon, generated artifacts, and repository-trusted reference workload.

## System boundary

```text
scenario + targets + schemas + queries
                  |
                  v
          chronicle validate
                  |
                  v
        isolated attempt runtime
      /          |             \
 Redpanda    PostgreSQL     target services
      \          |             /
       typed observations + invariants
                  |
                  v
     comparison -> confirmation -> reduction
                  |
                  v
 result + reports + journal + verified bundle
```

`chronicle validate` performs schema and semantic validation without contacting Docker.
`chronicle run` provisions isolated attempt resources, executes the declared schedule, waits for declared quiescence, collects the complete observation inventory, and compares baseline with candidate.
`chronicle bench` uses a separate HTTP-only runtime and never reuses the correctness engine or its instrumentation.

## Authored contracts

Scenarios declare events, actions, fault checkpoints, quiescence, observers, invariants, and normalization.
Targets declare exact images, commands, resources, health checks, capabilities, and service dependencies.
Workload-owned JSON Schemas and SQL queries are versioned inputs and are included in the reproduction closure.
Draft 2020-12 JSON Schemas validate the raw documents before strict typed decoding and semantic checks.

Only an explicit JSON Pointer allowlist can differ between baseline and candidate target contracts.
The workload database schema version must match across both targets.
Every other command, environment, resource, dependency, health, and capability difference fails preflight.

## Attempt isolation

Every attempt receives a unique run identifier, network, topic namespace, consumer groups, and database clone.
PostgreSQL clones originate from one frozen template whose canonical schema fingerprint is checked before and after observation.
Redpanda exposes separate internal and loopback host listeners so containers and the host-side orchestrator use valid advertised addresses.
Cleanup owns exact resource handles and never enumerates broad Docker, broker, or database targets for deletion.

## Deterministic control

The opt-in [`pkg/probe`](../pkg/probe) package exposes authenticated checkpoints, delivery receipts, work accounting, and a logical clock.
Checkpoint arms use an opaque handle and a per-process instance identity, so a stale release cannot affect a restarted process.
The orchestrator independently verifies committed offsets and exact broker receipts before it treats a controlled step as complete.

R1 uses a broker-admin offset rewind after the consumer group becomes empty.
R2 and R7 use `SIGKILL` at a durable checkpoint and verify physical redelivery after restart.
R3, R5, and R6 use exact release schedules whose receipts, transitions, offsets, and clock sequence must be complete before comparison.

## Observation and comparison

SQL, Kafka, HTTP, effect-ledger, and invariant observers produce typed `chronicle.dev/observation/v1alpha1` records.
An observation is joined across baseline and candidate by its logical step, observer, and occurrence identity.
Attempt-specific database names, topics, offsets, subjects, and endpoints remain diagnostic metadata rather than comparison keys.

Normalization is restricted to declared JSON Pointer operations.
Ordered, set, multiset, and keyed comparison modes have fail-closed pairing rules.
Candidate-only payload schema failure after a valid baseline is a schema regression, while invalid baseline evidence remains unresolved.

## Confirmation, reduction, and replay

A first semantic difference is evidence, not yet a confirmed regression.
ChronicleGate repeats fresh candidate attempts and returns a regression only when the required attempts reproduce the same exact failure signature.
Different completed semantic outcomes are flaky, infrastructure failures remain infrastructure failures, bounded operation deadlines remain timeouts, and incomplete healthy evidence remains unresolved.

The reducer evaluates each proposal with a fresh baseline and candidate under the same predicate.
It accepts a transform only when repeated trials preserve the exact target signature.
One-minimality is reported as proven only after a complete final pass has no removable transform and no unresolved trial.

The reproduction ZIP has a signed manifest, bounded expansion, strict path rules, verified embedded local-image archives, and a separate extraction staging directory.
The append-only journal is authoritative for completion, and `chronicle report` refuses to treat an artifact as complete without a valid terminal record.

## Security boundary

Target containers run as non-root with read-only root filesystems, dropped capabilities, `no-new-privileges`, bounded CPU, memory, and PIDs, loopback-only host ports, and private read-only credential mounts.
The Docker socket is available only to the host CLI and is never mounted into a target.
The model assumes trusted authored inputs, trusted target images, and a trusted local Docker daemon.
See [Security model](security-model.md) for the detailed trust and archive boundaries.

## Source map

- [`cmd/chronicle`](../cmd/chronicle) wires the public CLI.
- [`internal/spec`](../internal/spec) owns authored schemas, typed models, and semantic validation.
- [`internal/engine`](../internal/engine) owns semantic execution and comparison.
- [`internal/observe`](../internal/observe) owns observation collection and normalization.
- [`internal/minimize`](../internal/minimize) owns signature-preserving reduction.
- [`internal/bundle`](../internal/bundle) owns safe bundle creation, verification, and replay staging.
- [`internal/bench`](../internal/bench) owns the isolated performance engine.
- [`internal/evidence`](../internal/evidence) owns source-authenticated release evidence and repository release checks.

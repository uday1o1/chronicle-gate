# ChronicleGate

ChronicleGate is a local release-qualification CLI for Kafka-style stateful services.
It makes broker redelivery, precise crash windows, schema-compatible behavior drift, cross-stream schedules, event-time lateness, and transactional-outbox retries reproducible before a candidate release ships.

Extended V1 is complete through Milestone 10.

## Problem

Stateful consumer defects often live between systems rather than inside one function.
A handler can commit a database write and crash before its Kafka offset advances, a compatible schema can change a business default, or an outbox relay can publish successfully and die before marking its row complete.
Ordinary unit tests and happy-path integration tests rarely control those boundaries precisely enough to reproduce the same failure twice.

ChronicleGate turns each boundary into an authored qualification contract.
It gives the baseline and candidate equivalent state and inputs, controls the chosen fault or schedule, waits for explicit quiescence, compares complete typed observations, confirms an exact signature, and reduces only when fresh baseline-candidate trials preserve that signature.

## Boundary

ChronicleGate V1 is a trusted local engineering tool, not a hostile-container sandbox or a production traffic system.
Its trust boundary includes authored scenarios, target images, the Docker daemon, the host kernel, pinned Redpanda and PostgreSQL images, and the local artifact directory.
The included order lifecycle is synthetic, and passing it does not prove universal service correctness.

Semantic qualification and performance comparison are separate products.
`chronicle run` owns correctness, fault control, observers, confirmation, reduction, and replay bundles.
`chronicle bench` owns an HTTP-only open-loop runtime, paired trial policy, raw timings, resource samples, and a standalone result.

See [Limitations](docs/limitations.md) and [Security model](docs/security-model.md) before applying the results outside the checked-in workload and environment.

## Architecture

```text
authored scenario + exact targets + schemas + queries
                         |
                         v
               offline contract validation
                         |
                         v
        isolated baseline and candidate attempts
         Redpanda + PostgreSQL + target services
                         |
                         v
          quiescence + typed observation inventory
                         |
                         v
       comparison + confirmation + safe reduction
                         |
                         v
  result + reports + journal + checksums + replay bundle
```

Every attempt receives a unique network, topic namespace, consumer groups, and database clone from one fingerprinted template.
The opt-in authenticated [`pkg/probe`](pkg/probe) package supplies exact checkpoints, delivery receipts, work accounting, and logical time for controlled cases.
SQL, Kafka, HTTP, effect-ledger, and invariant observers join baseline and candidate by stable logical identity rather than attempt-specific physical names.

The append-only journal is authoritative for completion.
Cleanup owns exact recorded handles and never performs global Docker, Kafka, or PostgreSQL deletion.
Verified reproduction bundles enforce bounded expansion, strict paths, signed content hashes, and exact embedded local-image identity before replay.

Read the [Architecture](docs/architecture.md), [Qualification methodology](docs/methodology.md), and [Observer model](docs/observer-model.md) for the complete design.

## Quick demo

Docker is the only required host toolchain dependency.
The Makefile uses the locked Go toolchain container when a compatible native Go installation is unavailable.

```sh
make build
./bin/chronicle doctor --json
make reference-images
./bin/chronicle run \
  --scenario examples/order-lifecycle/scenarios/r1-offset-rewind.yaml \
  --baseline examples/order-lifecycle/targets/generated/baseline.yaml \
  --candidate examples/order-lifecycle/targets/generated/candidate.yaml \
  --out run/r1 \
  --development-local-images \
  --json
```

The seeded R1 candidate returns exit code `2` after two matching confirmation attempts.
<!-- measured: evidence/results/r1-offset-rewind.json#/outcome/exitCode -->
<!-- measured: evidence/results/r1-offset-rewind.json#/outcome/confirmations -->
The baseline retains one reservation after the same physical Kafka record is delivered twice, while the candidate retains two and matches the checked-in failure signature.

Inspect or replay the completed run with:

```sh
./bin/chronicle report --result run/r1 --format text
./bin/chronicle replay \
  --bundle run/r1/reproduction.zip \
  --out run/r1-replay \
  --json
```

The [R1 transcript](demo/r1-transcript.md) and deterministic [asciinema v2 cast](demo/r1.cast) are derived from the source-authenticated release capture.
The full clean-checkout procedure is in the [Reproduction guide](docs/reproduction.md).

## Measured results

The public evidence was captured by the exact source commit, executable digest, arguments, and input closure recorded in each JSON document.
All seven seeded regressions reproduced their checked-in signatures, and all seven nearby controls passed.

| Fault family | Seeded case | Nearby control |
| --- | --- | --- |
| R1 offset rewind | `SEMANTIC_REGRESSION` | `PASS` |
| R2 crash after effect | `EXTERNAL_EFFECT_REGRESSION` | `PASS` |
| R3 stale aggregate version | `SEMANTIC_REGRESSION` | `PASS` |
| R4 schema-default drift | `SEMANTIC_REGRESSION` | `PASS` |
| R5 inventory-first release | `SEMANTIC_REGRESSION` | `PASS` |
| R6 late cancellation | `SEMANTIC_REGRESSION` | `PASS` |
| R7 outbox crash after acknowledgement | `EXTERNAL_EFFECT_REGRESSION` | `PASS` |

<!-- measured: evidence/results/r1-offset-rewind.json#/outcome/classification -->
<!-- measured: evidence/results/r1-single-delivery-control.json#/outcome/classification -->
<!-- measured: evidence/results/r2-crash-after-effect.json#/outcome/classification -->
<!-- measured: evidence/results/r2-manual-commit-control.json#/outcome/classification -->
<!-- measured: evidence/results/r3-stale-aggregate.json#/outcome/classification -->
<!-- measured: evidence/results/r3-monotonic-control.json#/outcome/classification -->
<!-- measured: evidence/results/r4-schema-default-drift.json#/outcome/classification -->
<!-- measured: evidence/results/r4-defaulting-control.json#/outcome/classification -->
<!-- measured: evidence/results/r5-inventory-first.json#/outcome/classification -->
<!-- measured: evidence/results/r5-payment-first-control.json#/outcome/classification -->
<!-- measured: evidence/results/r6-late-cancellation.json#/outcome/classification -->
<!-- measured: evidence/results/r6-on-time-control.json#/outcome/classification -->
<!-- measured: evidence/results/r7-outbox-crash.json#/outcome/classification -->
<!-- measured: evidence/results/r7-unrelated-orders-control.json#/outcome/classification -->

R1 reduction removed one of two events and one of seven actions in eight fresh-pair trials, then proved relative one-minimality.
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/originalEvents -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/finalEvents -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/originalActions -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/finalActions -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/trials -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/minimality -->

The isolated benchmark gate passed its A/A control and detected the seeded practical slowdown under the locked paired-bootstrap policy.
<!-- measured: evidence/results/benchmark.json#/comparisons/0/outcome/classification -->
<!-- measured: evidence/results/benchmark.json#/comparisons/1/outcome/classification -->
<!-- measured: evidence/results/benchmark.json#/comparisons/1/outcome/analysis/regression -->

See [Results and evidence](docs/results.md) for exact signatures, observation counts, reduction facts, raw benchmark aggregates, source provenance, and claim boundaries.

## Limitations

- Precise faults require the target to opt into the probe contract.
- V1 controls declared checkpoints and selected schedules, not arbitrary thread interleavings.
- Development-local image IDs and their embedded replay bundles are nonportable across Docker platforms.
- A `PASS` means the declared observations matched under one locked scenario, not that both services are universally equivalent.
- `UNRESOLVED`, `TIMEOUT`, `FLAKY`, `INFRASTRUCTURE_ERROR`, and `INTERRUPTED` are non-passing outcomes.
- Local macOS, Colima, and shared-CI benchmark runs are comparative development evidence, not production capacity claims.

## Build and internals

Validate an authored contract without Docker:

```sh
./bin/chronicle validate \
  --scenario examples/order-lifecycle/scenarios/r1-offset-rewind.yaml \
  --target examples/order-lifecycle/targets/baseline.yaml \
  --json
```

Run the complete local gates:

```sh
make verify
make test-e2e
make test-benchmark
```

The public schemas live in [`schemas`](schemas), the synthetic workload lives in [`examples/order-lifecycle`](examples/order-lifecycle), and the exact release inventory lives in [`evidence/corpus.json`](evidence/corpus.json).
Implementation details are organized under [`internal/spec`](internal/spec), [`internal/engine`](internal/engine), [`internal/observe`](internal/observe), [`internal/minimize`](internal/minimize), [`internal/bundle`](internal/bundle), [`internal/bench`](internal/bench), and [`internal/evidence`](internal/evidence).

## License

ChronicleGate is licensed under Apache-2.0.

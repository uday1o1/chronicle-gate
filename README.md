# ChronicleGate

ChronicleGate is a local release-qualification framework for instrumented Kafka-style stateful consumers.
It is being implemented milestone by milestone according to [BUILD_PLAN.md](BUILD_PLAN.md).

Milestones 0 through 7 are complete, including the portfolio-ready core checkpoint, the complete V1 observer model, the controlled cross-stream and event-time corpus, and the connected transactional-outbox corpus.
Reproducible bootstrap, immutable image locks, environment diagnostics, typed authored contracts, offline validation, broker-realistic R1, stable failure confirmation, dependency-safe reduction, multi-format reports, verified replay bundles, authenticated precise checkpoints, R2 crash recovery, manual synchronous offset-commit proof, schema-compatible R4 default drift, stale-version R3, cross-stream R5, late-event R6, and duplicate-publication R7 pass their local acceptance gates.

## Bootstrap

Docker is the only required local toolchain dependency.
The Makefile uses immutable Go and golangci-lint container references when a native Go installation is unavailable.

```sh
make build
./bin/chronicle version
./bin/chronicle doctor --json
make verify
```

`doctor` checks host workspace capacity and loopback port allocation, then verifies the Docker server and every locked OCI image index.
The disk and loopback checks describe host-visible resources only.

## Validate authored contracts

Validation is deliberately offline and does not contact Docker or any target service.
It applies the bundled Draft 2020-12 JSON Schemas before strict Go decoding, then checks dependency graphs, runtime references, immutable images, fault ordering, observer contracts, normalization rules, and bounded execution limits.

```sh
./bin/chronicle validate \
  --scenario examples/order-lifecycle/scenarios/r1-offset-rewind.yaml \
  --target examples/order-lifecycle/targets/baseline.yaml \
  --json
```

The public schemas live in [`schemas/`](schemas/).
The R1 authored-contract example lives in [`examples/order-lifecycle/`](examples/order-lifecycle/).
Invalid contracts return exit code `3` before any Docker access.

## Status

The native macOS ARM64 binary and cross-built Linux ARM64 and AMD64 binaries execute successfully.
The checked-in Redpanda, PostgreSQL, Go bootstrap, and golangci-lint OCI indexes contain the exact locked Linux ARM64 and AMD64 child manifests.
All checked-in scenario, target, workload, result, and bundle examples pass both schema and semantic validation and typed-model round trips.

## Run the broker-realistic R1 slice

The R1 walkthrough requires Docker with at least 4 CPUs, 6 GiB of memory, and the locked Redpanda and PostgreSQL images available for the local architecture.
The reference image build is a repository-trusted development workflow and runs before `chronicle run`.
It creates distinct correct and seeded-defect images for R1 through R7, creates the local effect sink, and generates ignored target manifests containing exact image IDs.

```sh
make reference-images
make build
./bin/chronicle run \
  --scenario examples/order-lifecycle/scenarios/r1-offset-rewind.yaml \
  --baseline examples/order-lifecycle/targets/generated/baseline.yaml \
  --candidate examples/order-lifecycle/targets/generated/candidate.yaml \
  --out run/r1 \
  --development-local-images \
  --json
```

Exit code `2` is expected because the seeded candidate regression is confirmed twice after its first failure.
The baseline processes the same physical Kafka record twice but retains one reservation.
The candidate processes that same record twice and produces the checked-in `no-duplicate-reservations` signature.
The default run confirms the original failure twice, minimizes optional scenario noise with fresh baseline and candidate executions, and creates text, JSON, JUnit XML, static HTML, checksums, and `reproduction.zip`.
See [`examples/order-lifecycle/README.md`](examples/order-lifecycle/README.md) for the evidence contract and current boundaries.

`--development-local-images` is intentionally nonportable.
Publication and later reproduction workflows require named OCI digest references.
Development replay bundles declare themselves nonportable and embed structurally verified archives for the exact local Docker image IDs.

Render a completed run in any supported format with:

```sh
./bin/chronicle report --result run/r1 --format text
./bin/chronicle report --result run/r1 --format json
./bin/chronicle report --result run/r1 --format junit
./bin/chronicle report --result run/r1 --format html
```

Replay verifies the ZIP manifest, paths, expanded-size limits, checksums, authored contracts, and embedded image identities before extraction or Docker access.
It extracts inputs into a separate private staging directory and requires the replay output directory not to exist.

```sh
./bin/chronicle replay \
  --bundle run/r1/reproduction.zip \
  --out run/r1-replay \
  --json
```

`events.ndjson` is the authoritative append-only audit log and is intentionally excluded from `checksums.sha256` because its final `COMPLETE` record is the last write in a successful run.
The `report` command refuses to treat `result.json` as complete when the journal has no valid terminal record.

Run the repeatable success and injected-failure cleanup gate with:

```sh
make test-integration
```

## Run the precise R2 crash qualification

R2 uses the opt-in public [`pkg/probe`](pkg/probe) package to block the workflow at the exact `after_external_effect` checkpoint.
The harness proves the group offset remains `0`, verifies the first delivery's topic, partition, offset, key, and event digest, sends `SIGKILL`, waits for the group to become empty, restarts the same declared image and group, and verifies the same physical record is delivered again.

```sh
./bin/chronicle run \
  --scenario examples/order-lifecycle/scenarios/r2-crash-after-effect.yaml \
  --baseline examples/order-lifecycle/targets/generated/r2-baseline.yaml \
  --candidate examples/order-lifecycle/targets/generated/r2-candidate.yaml \
  --out run/r2 \
  --development-local-images \
  --json
```

Exit code `2` is expected.
The correct baseline uses the CloudEvent ID as its effect idempotency key and records one effect across the crash.
The seeded candidate creates a new key for each delivery and produces the stable checked-in `EXTERNAL_EFFECT_REGRESSION` signature with two effects for one business operation.
Every completed attempt proves the full declared quiescence contract continuously for two seconds.

The nearby manual-commit control must pass with exit code `0`:

```sh
./bin/chronicle run \
  --scenario examples/order-lifecycle/scenarios/manual-offset-commit-control.yaml \
  --baseline examples/order-lifecycle/targets/generated/r2-baseline.yaml \
  --candidate examples/order-lifecycle/targets/generated/r2-candidate.yaml \
  --out run/manual-commit \
  --development-local-images \
  --no-minimize \
  --json
```

The control blocks at `before_offset_commit` while the committed position remains `0`.
After release, the service performs a synchronous record commit and independently reads the group offset until it is exactly `1` before exposing `after_offset_commit`.
See [`docs/portfolio-core.md`](docs/portfolio-core.md) for the measured checkpoint evidence and clean-source reproduction procedure.

## Run the R4 schema-default qualification

R4 registers a predecessor and current JSON Schema under a fresh Registry subject and requires a positive `BACKWARD` compatibility result.
The new optional `fulfillmentMode` field is structurally compatible, but the seeded candidate applies `expedited` when the field is absent while the baseline preserves the declared `standard` behavior.

```sh
./bin/chronicle run \
  --scenario examples/order-lifecycle/scenarios/r4-schema-default-drift.yaml \
  --baseline examples/order-lifecycle/targets/generated/r4-baseline.yaml \
  --candidate examples/order-lifecycle/targets/generated/r4-candidate.yaml \
  --out run/r4 \
  --development-local-images \
  --no-minimize \
  --json
```

Exit code `2` is expected with the checked-in `/rows/0/fulfillment_mode` signature.
Each attempt executes the exact declared SQL, Kafka, and HTTP observation inventory once.
Reports include every timestamp normalization with its logical observation identity, authored pointer, and affected count.
The Registry evidence retains source and self-contained schema hashes, assigned versions and IDs, predecessor coverage, and compatibility responses.
The integration matrix also proves that a candidate-only invalid runtime output becomes a confirmed `SCHEMA_REGRESSION`, while the same invalid output from the baseline becomes `UNRESOLVED` before candidate execution.

The explicit-default control must pass with exit code `0` even though it includes a compatible optional logging field and uses an equivalent refactored SQL projection:

```sh
./bin/chronicle run \
  --scenario examples/order-lifecycle/scenarios/r4-explicit-default-control.yaml \
  --baseline examples/order-lifecycle/targets/generated/r4-baseline.yaml \
  --candidate examples/order-lifecycle/targets/generated/r4-candidate.yaml \
  --out run/r4-control \
  --development-local-images \
  --no-minimize \
  --json
```

See [`docs/observer-model.md`](docs/observer-model.md) for observer comparison, Kafka range, normalization, and schema-classification boundaries.

## Run the controlled R3, R5, and R6 corpus

The controlled executor initializes every empty consumer group before service launch, proves each expected client assignment, and executes exact `before_handler` checkpoint schedules.
Each consumer has one-record processing capacity, while the shared probe advertises capacity two so two separate-topic handlers can be blocked concurrently.
Every release is followed by an independent committed-offset read before the next controlled action.

R3 publishes version `2` at offset `0`, waits for its commit, and then publishes stale version `1` at offset `1` on the same partition.
No broker reorder is requested or claimed.

```sh
./bin/chronicle run \
  --scenario examples/order-lifecycle/scenarios/r3-stale-aggregate-overwrite.yaml \
  --baseline examples/order-lifecycle/targets/generated/state-baseline.yaml \
  --candidate examples/order-lifecycle/targets/generated/state-r3-candidate.yaml \
  --out run/r3 \
  --development-local-images \
  --no-minimize \
  --json
```

R5 blocks payment and inventory handlers on two distinct logical topics, proves both are blocked, and releases them in each authored legal order.
Payment-first is the nearby passing control.
Inventory-first exposes the seeded lost-transition defect without claiming control over partitions within one topic.

```sh
./bin/chronicle run \
  --scenario examples/order-lifecycle/scenarios/r5-inventory-first-regression.yaml \
  --baseline examples/order-lifecycle/targets/generated/state-baseline.yaml \
  --candidate examples/order-lifecycle/targets/generated/state-r5-candidate.yaml \
  --out run/r5 \
  --development-local-images \
  --no-minimize \
  --json
```

R6 commits an active state, advances the authenticated logical clock from `12:00Z` to `13:00Z`, and then physically publishes a version `2` cancellation whose CloudEvent time is `11:00Z`.
The version is monotonically valid, so the result isolates event-time lateness from R3 stale-version handling.
Text, JSON, JUnit, and HTML reports show physical delivery sequence, CloudEvent event time, and logical delivery time as separate evidence.

```sh
./bin/chronicle run \
  --scenario examples/order-lifecycle/scenarios/r6-late-cancellation.yaml \
  --baseline examples/order-lifecycle/targets/generated/state-baseline.yaml \
  --candidate examples/order-lifecycle/targets/generated/state-r6-candidate.yaml \
  --out run/r6 \
  --development-local-images \
  --no-minimize \
  --json
```

All three seeded defects return exit code `2` only after two matching confirmation attempts.
The R3 monotonic-version, R5 payment-first, and R6 on-time cancellation controls return exit code `0`.
See [`docs/controlled-schedules.md`](docs/controlled-schedules.md) for the schedule validity and evidence contract.

## Run the connected R7 outbox qualification

R7 exercises the complete repository-trusted order lifecycle through the real public API and connected services.
The flow is `POST /orders`, one orders-and-outbox transaction, synchronous relay publication, payment and inventory request and outcome streams, fulfillment projection, and a local synthetic external effect.

The harness blocks the relay only after `ProduceSync` returns an exact broker identity and the acknowledgement is written to durable evidence, then sends `SIGKILL` before the outbox row is marked published.
The restarted baseline republishes the same logical CloudEvent ID, so downstream idempotency retains one business effect.
The seeded candidate derives a deterministic new ID only for the retry, so the same crash produces two business effects and the checked-in R7 signature.

```sh
./bin/chronicle run \
  --scenario examples/order-lifecycle/scenarios/r7-outbox-crash-after-ack.yaml \
  --baseline examples/order-lifecycle/targets/generated/r7-baseline.yaml \
  --candidate examples/order-lifecycle/targets/generated/r7-candidate.yaml \
  --out run/r7 \
  --development-local-images \
  --no-minimize \
  --json
```

Exit code `2` is expected with `EXTERNAL_EFFECT_REGRESSION` and the checked-in `/entries/count` signature.
Every attempt retains the five exact executed service images, both acknowledged outbox publications, the logical and emitted event IDs, physical topic offsets, final group positions, full quiescence proof, effect projection, and database schema fingerprints.

The unrelated-orders control proves that the R7 candidate passes when two independent outbox rows each publish once:

```sh
./bin/chronicle run \
  --scenario examples/order-lifecycle/scenarios/r7-unrelated-orders-control.yaml \
  --baseline examples/order-lifecycle/targets/generated/r7-baseline.yaml \
  --candidate examples/order-lifecycle/targets/generated/r7-candidate.yaml \
  --out run/r7-control \
  --development-local-images \
  --no-minimize \
  --json
```

See [`docs/outbox-qualification.md`](docs/outbox-qualification.md) for the crash-window contract, retained evidence, control matrix, and measured functional acceptance runs.

## License

ChronicleGate is licensed under Apache-2.0.

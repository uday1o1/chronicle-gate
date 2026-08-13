# ChronicleGate

ChronicleGate is a local release-qualification framework for instrumented Kafka-style stateful consumers.
It is being implemented milestone by milestone according to [BUILD_PLAN.md](BUILD_PLAN.md).

Milestones 0 through 2 are complete.
Reproducible bootstrap, immutable image locks, environment diagnostics, typed authored contracts, offline validation, and the broker-realistic R1 vertical slice pass their local acceptance gates.
Failure reduction, reproduction bundles, and precise crash-window claims remain unavailable until their later milestones pass.

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
It creates two distinct local content-addressed Docker images and generates ignored target manifests containing their exact image IDs.

```sh
make reference-images
make build
./bin/chronicle run \
  --scenario examples/order-lifecycle/scenarios/r1-offset-rewind.yaml \
  --baseline examples/order-lifecycle/targets/generated/baseline.yaml \
  --candidate examples/order-lifecycle/targets/generated/candidate.yaml \
  --out run/r1 \
  --development-local-images \
  --no-minimize \
  --json
```

Exit code `2` is expected because the seeded candidate regression is confirmed twice after its first failure.
The baseline processes the same physical Kafka record twice but retains one reservation.
The candidate processes that same record twice and produces the checked-in `no-duplicate-reservations` signature.
See [`examples/order-lifecycle/README.md`](examples/order-lifecycle/README.md) for the evidence contract and current boundaries.

`--development-local-images` is intentionally nonportable.
Publication and later reproduction workflows require named OCI digest references.

Run the repeatable success and injected-failure cleanup gate with:

```sh
make test-integration
```

## License

ChronicleGate is licensed under Apache-2.0.

# Reproduction guide

This guide reproduces the primary R1 walkthrough from a clean checkout using the public CLI.
It uses repository-trusted development images and produces a nonportable local bundle for the same Docker platform.

## Prerequisites

- Git with complete repository history.
- Docker with Linux containers enabled.
- At least 4 CPUs, 6 GiB of memory, and 10 GiB of free workspace storage available to the Docker-backed run.
- Network access when the locked build and infrastructure images are not already present.

No host Go installation is required.
The Makefile uses the exact locked Go container when a compatible native toolchain is unavailable.

## 1. Inspect the environment

```sh
git status --short
make build
./bin/chronicle version --json
./bin/chronicle doctor --json
```

Start from a clean tracked worktree.
`doctor` verifies workspace capacity, loopback port allocation, Docker server compatibility, architecture normalization, and the locked image index-to-child linkage.

## 2. Build the trusted reference images

```sh
make reference-images
make build
```

The first command builds separate baseline and seeded-candidate images outside `chronicle run` and writes ignored generated target manifests with exact local image IDs.
The second command builds the native CLI with source version metadata.

## 3. Run the R1 regression

```sh
./bin/chronicle run \
  --scenario examples/order-lifecycle/scenarios/r1-offset-rewind.yaml \
  --baseline examples/order-lifecycle/targets/generated/baseline.yaml \
  --candidate examples/order-lifecycle/targets/generated/candidate.yaml \
  --out run/r1 \
  --development-local-images \
  --json
```

The seeded candidate is expected to return exit code `2` after stable confirmation.
The baseline retains one reservation after the same physical record is delivered twice.
The candidate retains two reservations and matches the checked-in R1 signature.

## 4. Inspect and verify artifacts

```sh
./bin/chronicle report --result run/r1 --format text
./bin/chronicle report --result run/r1 --format json
./bin/chronicle report --result run/r1 --format junit
./bin/chronicle report --result run/r1 --format html
sha256sum -c run/r1/checksums.sha256
```

On macOS, use `shasum -a 256 -c run/r1/checksums.sha256` if `sha256sum` is unavailable.
The run directory contains the machine-readable result, reports, checksums, authoritative journal, attempts, minimized scenario, and reproduction bundle.

## 5. Replay the verified bundle

```sh
./bin/chronicle replay \
  --bundle run/r1/reproduction.zip \
  --out run/r1-replay \
  --json
```

Replay requires `run/r1-replay` not to exist.
It verifies archive paths, manifest checksums, expanded-size limits, authored contracts, and exact embedded local-image identities before extraction or Docker access.
The development bundle is portable only to the same Docker OS and architecture because a local config-image ID is not a registry-resolvable OCI reference.

## 6. Run the nearby passing control

```sh
./bin/chronicle run \
  --scenario examples/order-lifecycle/scenarios/r1-single-delivery-control.yaml \
  --baseline examples/order-lifecycle/targets/generated/baseline.yaml \
  --candidate examples/order-lifecycle/targets/generated/candidate.yaml \
  --out run/r1-control \
  --development-local-images \
  --no-minimize \
  --json
```

The control returns exit code `0` because the seeded candidate receives the record once and retains one reservation.

## 7. Run repository verification

```sh
make verify
make test-e2e
make test-benchmark
```

`make verify` covers formatting, unit and property tests, static analysis, race tests, dependency and security checks, fuzz smoke tests, native and cross-platform builds, pinned lint, and Docker integration.
`make test-e2e` is the exact public integration alias.
`make test-benchmark` runs repeated A/A and seeded-slowdown functional benchmark gates separately from correctness qualification.

## Cleanup scope

Successful runs clean their attempt containers, networks, topics, databases, and recorded volumes automatically.
Generated CLI binaries, target manifests, and run artifacts are ignored by Git.
ChronicleGate never performs global Docker, Kafka, or PostgreSQL cleanup.

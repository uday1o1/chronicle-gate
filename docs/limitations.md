# Limitations

ChronicleGate V1 makes bounded local qualification claims for trusted authored scenarios and trusted target images.
It does not establish general correctness, production capacity, or hostile-workload isolation.

## Trust and containment

- Docker daemon, host kernel, authored inputs, target images, and pinned infrastructure images are trusted.
- Target hardening reduces accidental privilege but does not turn a shared-kernel container into a security boundary for hostile code.
- Container networking can use host-provided outbound NAT, so V1 does not claim general egress denial.
- Unknown images should run on a disposable virtual machine with separate credentials and no sensitive host mounts.

## Workload scope

- The included order lifecycle is a synthetic reference workload, not production business logic.
- The seven seeded defects demonstrate specific broker, crash, schema, ordering, event-time, and outbox failure modes.
- Passing the corpus does not prove the absence of other stateful-service defects.
- V1 controls declared checkpoints and selected cross-stream schedules, not arbitrary thread interleavings or partitions within one topic.
- Kafka-visible offsets can contain legitimate gaps, and observers compare visible records under frozen bounds rather than assuming contiguity.

## Instrumentation boundary

- Precise faults require services to opt into the authenticated [`pkg/probe`](../pkg/probe) contract.
- Probe state and receipts are qualification evidence, not production telemetry.
- Semantic correctness never depends on OpenTelemetry or profiler availability.
- Container inspection proves only that the harness did not inject correctness instrumentation into an arbitrary benchmark image.
- The stronger instrumentation-absence claim for the included benchmark image depends on repository source and image-provenance checks.

## Reproducibility boundary

- The repository locks toolchain and infrastructure OCI indexes plus Linux ARM64 and AMD64 child digests.
- Development target manifests use local config-image IDs and are not registry-resolvable or cross-platform identities.
- Development replay bundles embed verified local image archives and remain portable only to the same Docker OS and architecture.
- Portable publication should use named OCI digest references and a clean isolated environment.
- Complete Git history is required to verify that public evidence source commits and input hashes remain reachable.

## Performance boundary

- `chronicle bench` compares two targets on one controlled host and reports a paired local effect under one predeclared workload.
- Local macOS, Colima, and other non-dedicated results are functional development evidence, not production capacity measurements.
- Publication-scoped measurement requires a dedicated native Linux host, local Unix-socket Docker daemon, idle-host checks, no competing containers, and immutable named OCI targets.
- The locked bootstrap confidence interval describes the sampled paired trials and does not guarantee future latency.
- Tail latency outside the authored request rate, duration, response bounds, and endpoint is not measured.

## Data and integration boundary

- V1 supports the built-in SQL, Kafka, HTTP, effect-ledger, and invariant observer contracts.
- It does not discover arbitrary third-party services, schemas, databases, or side effects automatically.
- The reference data is synthetic and contains no restricted dataset.
- Credentials are per-run local secrets and are excluded from reports and bundles, but Docker daemon or host compromise remains out of scope.

## Result interpretation

- A confirmed regression means fresh complete attempts reproduced the same exact signature under the locked scenario.
- `PASS` means the declared observations matched under that contract, not that baseline and candidate are universally equivalent.
- `UNRESOLVED`, `TIMEOUT`, `FLAKY`, `INFRASTRUCTURE_ERROR`, and `INTERRUPTED` are non-passing outcomes and must not be reported as semantic success.
- Cleanup failure overrides an otherwise successful semantic result because the complete qualification gate did not pass safely.

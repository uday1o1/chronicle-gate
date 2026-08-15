# ChronicleGate Build Plan

## 1. Document purpose and authority

This file is the implementation authority for ChronicleGate V1.
An implementation agent should read this entire file before changing the repository.
The agent should implement milestones in order and should not silently widen the scope.
Every design decision marked as required is an acceptance condition rather than a suggestion.
If an external dependency has changed, the agent should update the lock data and cite the current authoritative documentation in the dependency record or affected project documentation.
The paths and commands below describe the implemented V1 repository.

### 1.1 Current verified status

ChronicleGate Extended V1 is complete through Milestone 10 within the trusted synthetic local Docker scope defined in this plan.
The checked-in release inventory contains fourteen semantic records covering seven seeded regressions and seven nearby passing controls, plus one benchmark record containing an A/A control and an injected-slowdown comparison.
The benchmark report keeps pooled descriptive p95 values separate from paired absolute and relative estimators, and each confidence interval corresponds to the paired relative estimate it describes.
The complete local verification path is `make verify`, `make test-e2e`, `make test-benchmark`, and `make release-check`.
The implementation does not claim production reliability, production capacity, hostile-container isolation, or benchmark portability beyond the recorded local environment.
No hardware, external service, or human-validation gate remains for the accepted V1 scope.
Dedicated-host publication benchmarking remains an optional future activity and is not required to reproduce the checked-in development-scoped evidence.

## 2. Product definition

ChronicleGate is a local release-qualification framework for instrumented Kafka-style stateful consumers.
It runs a baseline and candidate against equivalent initial state, injects realistic delivery and crash-window faults, compares workload-defined semantic observations, and reduces a stable failure to a small reproduction bundle.

The defensible one-sentence claim is:

> ChronicleGate provides repeatable semantic release qualification for stateful Kafka consumers through offset-based redelivery, opt-in execution checkpoints, explicit state oracles, and dependency-aware failure reduction.

ChronicleGate is primarily a software engineering and distributed-systems portfolio project.
Its strongest interview evidence should be precise failure semantics, clean orchestration, fault isolation, typed contracts, reproducibility, and high-quality testing.

## 3. Problem being solved

Ordinary unit tests rarely exercise the dangerous interval between an external side effect, a database commit, and a Kafka offset commit.
Load tests may reveal symptoms without proving whether a release changed business semantics.
Generic chaos tools disrupt infrastructure but do not know which order state, projection row, emitted event, or external effect is correct.
Production replay systems often depend on company-specific infrastructure and are not portable portfolio artifacts.

ChronicleGate addresses this gap by combining realistic Kafka offset behavior with workload-supplied semantic observations.
The harness does not guess whether two databases are equivalent.
The workload declares the SQL, Kafka, HTTP, and side-effect evidence that constitutes equivalence.

## 4. Scope contract

### 4.1 Required V1 capabilities

- Run baseline and candidate container images sequentially against equivalent PostgreSQL state.
- Use one Redpanda broker through Kafka-compatible APIs.
- Publish locally validated CloudEvents structured JSON records.
- Create attempt-specific Kafka topic prefixes and consumer groups.
- Re-deliver an existing broker record by rewinding a stopped consumer group's committed offset.
- Inject precise crash windows through an opt-in Go probe package.
- Control selected cross-stream handler release orders through named probe checkpoints.
- Advance deterministic logical application time through the probe clock.
- Observe state through read-only SQL, bounded Kafka output ranges, local JSON HTTP endpoints, and a fake external-effect ledger.
- Evaluate explicit invariants and semantic comparisons.
- Classify infrastructure, timeout, flakiness, schema, semantic, and external-effect failures separately.
- Confirm failure stability before minimization.
- Reduce a failing scenario under dependency and schema constraints.
- Export JSON, JUnit XML, text, static HTML, and a hash-verified reproduction bundle.
- Provide a complete synthetic order-lifecycle reference workload with seeded defects and passing controls.

### 4.2 Explicit non-goals

- V1 will not claim deterministic replay of arbitrary black-box services.
- V1 will not claim deterministic Go scheduling, database execution, or broker internals.
- V1 will not reorder records within a Kafka partition.
- V1 will not compare divergent database schemas or qualify migrations.
- V1 will not capture production traffic or attempt generic PII redaction.
- V1 will not execute untrusted third-party images on a developer workstation.
- V1 will not implement Kubernetes, multi-broker failover, leader-election faults, or a hosted control plane.
- V1 will not implement generic network chaos or require Toxiproxy.
- V1 will not promise exactly-once effects across an external system.
- V1 will not implement language SDKs beyond the Go probe.
- V1 will not call a reduced case globally minimal.
- V1 will not combine performance and correctness conclusions in one gate.

## 5. Feasibility and environment assumptions

The development workstation is Apple silicon and currently has Docker through Colima but no host Go installation.
The Docker server is Linux ARM64.
The implementation must therefore support local ARM64 development and avoid assuming an x86-only image.
The project should use the current stable Go toolchain available at implementation time, with Go 1.26.5 as the researched starting point on 2026-08-13.
A pinned Go development container is an acceptable fallback if a host toolchain is not installed.

The initial dependency locks researched on 2026-08-13 are:

| Concern | Initial lock target |
| --- | --- |
| Go | 1.26.5 |
| Testcontainers for Go | 0.44.0 |
| Redpanda | 26.2.1 |
| PostgreSQL | 18.4 Alpine |
| franz-go | 1.21.6 |
| pgx | 5.10.0 |
| OpenTelemetry Go | 1.45.0 |
| Cobra | 1.10.2 |
| JSON Schema library | santhosh-tekuri/jsonschema v6.0.3 |
| golangci-lint | 2.12.2 |

The implementation agent must verify current tags and security advisories before writing `go.mod` or container locks.
Container tags alone are not reproducible and must resolve to OCI digests in `config/images.lock.json`.
The Redpanda image must be selected explicitly rather than inherited from a Testcontainers module default.

## 6. Core design decisions

### 6.1 Baseline and candidate execute sequentially

ChronicleGate will provision one Redpanda and PostgreSQL environment for a run.
It will execute the baseline, stop every baseline service, restore PostgreSQL from a template snapshot, allocate new Kafka namespaces, and then execute the candidate.
Sequential execution avoids cross-run resource contention and makes local requirements feasible.
It also prevents a baseline and candidate from consuming each other's records.

### 6.2 PostgreSQL is restored and Redpanda is namespaced

The harness will create a PostgreSQL template database after migrations and fixture seeding.
Each attempt will clone or restore from that template only while application connections are stopped.
Redpanda will not be snapshotted or destructively reset.
Each baseline, candidate, confirmation, and minimization attempt will receive unique topic and consumer-group prefixes.
Attempt cleanup will delete those namespaced topics only after artifacts are finalized.

### 6.3 Real redelivery uses offsets

The canonical redelivery fault will stop every member of a consumer group, inspect the committed offset with `kadm`, commit an earlier valid offset, restart the service, and verify delivery of the same topic, partition, and offset.
Publishing a second physical record is a distinct producer-duplicate fault and must never be described as consumer redelivery.

### 6.4 Exact crash windows require the probe

An arbitrary container can participate in coarse start, stop, health, and observation workflows.
Precise faults such as killing after an external effect but before offset commit require the service to call `pkg/probe` checkpoints.
The UI and report must label whether a result used coarse control or an opt-in precise checkpoint.

### 6.5 Precise commit faults require manual synchronous commits

Every instrumented consumer used for an offset-commit fault must disable automatic Kafka commits.
The reference franz-go consumer must use the library's manual commit mode and process exactly one controlled record at a time for a precise fault scenario.
Its required sequence is handler entry, workload state and effect processing, `before_offset_commit`, synchronous commit of that record, administrative verification that the committed position advanced to `record.offset + 1`, and `after_offset_commit`.
The probe capability handshake reports `commitMode`, maximum in-flight controlled records, checkpoint set, and probe protocol version.
Precise commit-window scenarios require `commitMode: manual_sync` and `maxControlledInFlight: 1`.
The validator rejects the scenario before publication when the target cannot prove those capabilities.
Being blocked at `before_offset_commit` must leave the group's committed offset unchanged.

### 6.6 Correctness is workload-owned

Every workload must declare its observations, comparison mode, normalization, invariants, and quiescence conditions.
The harness may provide reusable observer implementations but must not infer semantic equivalence from raw database equality.

### 6.7 Performance is isolated

`chronicle run` is the semantic correctness path.
`chronicle bench` is a separate later path with a separate environment, report, exit status, and statistical policy.
OpenTelemetry, debug logging, minimization, and fault probes may distort performance and therefore are excluded from the core benchmark path unless explicitly measured.

## 7. End-to-end execution state machine

The correctness engine must persist every state transition so an interrupted run can explain how far it progressed.

```text
VALIDATING
  -> PROVISIONING
  -> SEEDING
  -> SNAPSHOTTING
  -> BASELINE_STARTING
  -> BASELINE_EXECUTING
  -> BASELINE_OBSERVING
  -> BASELINE_STOPPING
  -> RESTORING
  -> CANDIDATE_STARTING
  -> CANDIDATE_EXECUTING
  -> CANDIDATE_OBSERVING
  -> COMPARING
  -> CONFIRMING_FAILURE, when a failure exists
  -> MINIMIZING, when the failure is stable
  -> REPORTING
  -> CLEANING
  -> COMPLETE
```

Any state may transition to `INFRASTRUCTURE_ERROR`, `TIMEOUT`, `UNRESOLVED`, or `INTERRUPTED` with a recorded cause.
Cleanup must be idempotent and must run after normal completion, error, timeout, and signal interruption.
Run state should be appended atomically to `run/events.ndjson` before and after every externally visible operation.

## 8. Public CLI contract

The command name is `chronicle`.

```text
chronicle version
chronicle doctor [--json]
chronicle validate --scenario FILE --target FILE [--json]
chronicle run --scenario FILE --baseline FILE --candidate FILE --out DIR [--no-minimize] [--json]
chronicle replay --bundle FILE --out DIR [--json]
chronicle report --result DIR --format text|json|junit|html
chronicle bench --workload FILE --baseline FILE --candidate FILE --out DIR [--json]
```

`run` performs bounded minimization by default after a stable failure.
`--no-minimize` disables minimization but does not disable confirmation.
Every command must support machine-readable errors when `--json` is present.
No command may silently modify an authored scenario or target file.

Exit codes are fixed:

| Code | Meaning |
| --- | --- |
| 0 | All requested gates passed. |
| 2 | A semantic, schema, external-effect, or performance regression was confirmed. |
| 3 | Input contracts were invalid. |
| 4 | Infrastructure provisioning or execution failed. |
| 5 | The result was flaky, timed out, or remained unresolved. |
| 130 | The process was interrupted. |

## 9. Authored contracts

### 9.1 Scenario contract

The scenario API version is `chronicle.dev/v1alpha1` until the complete seeded corpus is stable.
YAML is the authoring format, but it must decode into strict typed Go structures.
Unknown fields must fail validation.
All durations must include units.
Every schema must use JSON Schema Draft 2020-12.

The top-level scenario fields are:

```yaml
apiVersion: chronicle.dev/v1alpha1
kind: Scenario
metadata:
  name: offset-replay-does-not-double-reserve
  description: Replayed delivery must not apply a reservation twice.
spec:
  seed: {}
  clock: {}
  events: {}
  steps: []
  quiescence: {}
  observations: []
  invariants: []
  normalization: []
  limits: {}
```

Validation must reject duplicate step IDs, unresolved dependencies, dependency cycles, undeclared services, undeclared observations, invalid partitions, invalid checkpoint names, illegal fault order, external schema references, and unbounded limits.
Semantic validation happens before any container starts.

### 9.2 Step DAG

Each step has a unique `id`, optional `dependsOn`, one action, and a timeout where waiting is possible.
Supported actions are `publish`, `await`, `stop`, `restart`, `rewindOffset`, `armCheckpoint`, `releaseCheckpoint`, `advanceClock`, and `observe`.
The executor runs ready independent steps concurrently only when their actions and declared resources do not conflict.
The reference implementation should begin with deterministic topological execution and add declared concurrency only for cross-stream fixtures.

### 9.3 Target manifest

A target manifest describes runtime topology rather than test behavior.

```yaml
apiVersion: chronicle.dev/v1alpha1
kind: Target
metadata:
  name: order-lifecycle-baseline
spec:
  services:
    - name: fulfillment-projector
      image: ghcr.io/example/projector@sha256:...
      command: []
      args: []
      environment: {}
      health: {}
      probe: {}
      resources: {}
      dependencies: []
```

Images must be immutable digest references in publication mode.
V1 will not build arbitrary Docker contexts inside `chronicle run`.
The target schema must allow only explicitly documented environment keys and must redact secret-valued fields in artifacts.
Baseline and candidate manifests must declare the same database schema version for V1.
Baseline and candidate may differ only in image digest by default.
Any additional intended change requires an exact JSON Pointer and rationale under `comparison.allowedTargetDifferences`.
Unlisted differences in command, arguments, resources, dependencies, health policy, environment, or runtime capability make validation fail.

The engine resolves each authored target into an immutable `ResolvedTarget` and each execution into an `AttemptRuntime`.
Reserved nonsecret injection fields are `CHRONICLE_BROKERS`, `CHRONICLE_TOPIC_PREFIX`, `CHRONICLE_GROUP_PREFIX`, `CHRONICLE_RUN_ID`, `CHRONICLE_ATTEMPT_ID`, and `CHRONICLE_LOGICAL_CLOCK_SEED`.
Reserved secret-bearing inputs use mounted files referenced by `CHRONICLE_DATABASE_DSN_FILE` and `CHRONICLE_PROBE_TOKEN_FILE`.
Authored manifests cannot set or override reserved names.
The resolved contract records broker endpoints, database identity, topic and group prefixes, probe endpoint, run identity, logical-clock seed, resource limits, and the source of each injected field.

Literal nonsecret environment values and named secret references are separate manifest fields.
A secret reference names a runtime provider entry but never embeds its value.
Artifacts preserve only the reference name and whether resolution succeeded.
Replay lists missing secret references and stops before launching containers.

Before the baseline starts, the engine creates canonical fingerprints of PostgreSQL schemas, extensions, functions, indexes, constraints, and the workload migration table.
It recomputes the fingerprint after each target becomes healthy and rejects unexpected DDL or migration divergence.
It fingerprints again after quiescence and observation collection but before stopping each attempt.
It also verifies the restored template fingerprint before starting the candidate or a minimizer trial.
Unexpected runtime DDL invalidates the comparison rather than becoming an ordinary state difference.
Application data rows are deliberately excluded from this schema fingerprint.
When Schema Registry is enabled, every subject receives the attempt prefix and the resolved attempt lock records subject, compatibility mode, schema ID, and schema hash.

### 9.4 Event contract

Events use CloudEvents 1.0 structured JSON.
Required attributes are `specversion`, `id`, `source`, `type`, `subject`, `time`, `datacontenttype`, and `data`.
Recommended extension attributes are `aggregateid`, `aggregateversion`, `correlationid`, `causationid`, and `schemaversion`.
The Kafka record key holds the aggregate identifier.
Broker offset, partition, and delivery attempt are execution metadata and do not belong in domain payloads.

Deterministic event IDs use canonical JSON with length-preserving string encoding:

```text
SHA-256(canonical_json({"version":1,"runSeed":seed,"stepID":step_id,"occurrence":n}))
```

Trace context belongs in Kafka headers and is excluded from semantic comparison.
Every payload is validated locally before publication even when it is registered with Redpanda Schema Registry.

## 10. Probe package contract

`pkg/probe` is the only public Go package in V1.
It provides logical time, named checkpoint gates, in-flight work accounting, readiness, quiescence evidence, and a bounded private administration endpoint.

```go
type Clock interface {
	Now() time.Time
}

type Checkpoint struct {
	Name       string
	Service    string
	EventID    string
	StepID     string
	Occurrence int
}

type WorkLabels struct {
	Service string
	Kind    string
	EventID string
}

func New(opts ...Option) *Probe
func (p *Probe) Clock() Clock
func (p *Probe) Enter(ctx context.Context, cp Checkpoint) error
func (p *Probe) BeginWork(labels WorkLabels) func()
func (p *Probe) Handler() http.Handler
func (p *Probe) Ready() bool
```

The reference consumer must expose these checkpoint names:

- `before_handler`
- `after_state_load`
- `after_external_effect`
- `after_db_commit`
- `before_offset_commit`
- `after_offset_commit`

Its capability response also includes manual commit mode, maximum controlled in-flight records, event-ID framing version, and logical-clock support.

The orchestrator arms a checkpoint by service, checkpoint name, event ID, and occurrence number.
The service blocks only when the exact tuple is reached.
The orchestrator observes the blocked state before applying the requested fault.

The administration endpoint must bind only to the private test network or a random loopback mapping.
It must require a cryptographically random per-run bearer token.
It must impose body-size, connection, and request-time limits.
It must not expose process environment variables, secrets, or arbitrary commands.
It must be disabled by default outside a ChronicleGate run.

## 11. Fault semantics

### 11.1 Offset rewind

The executor stops every consumer in the group, fetches committed offsets with `kadm`, validates the requested rewind against topic start and end offsets, commits the earlier position, verifies the new committed position, and restarts consumers.
The evidence records both old and new positions and the replayed record identity.
The scenario is rejected if group membership is not empty before the rewind.

### 11.2 Crash before offset commit

The executor arms `before_offset_commit`, publishes the target record, waits for the exact checkpoint, kills the service container with `SIGKILL`, restarts it with the same group, and verifies the same broker record is delivered again.
It also proves through `kadm` that the committed offset did not move while the handler was blocked.

### 11.3 Crash after an external effect

The executor arms `after_external_effect`, waits until the fake sink recorded the effect, kills the service before completion or offset commit, restarts it, and compares the effect ledger for duplicate business effects or unstable idempotency keys.

### 11.4 Cross-stream interleaving

Two records on separate topics or partitions may block at `before_handler`.
The executor releases them in each declared order.
The result is a controlled handler interleaving, not a claim about global broker ordering.

### 11.5 Event-time lateness

A later-published record may contain an older CloudEvent time and aggregate version.
The logical clock or declared watermark advances before publication.
This fault is event-time lateness and not within-partition reordering.

### 11.6 Physical duplicate

A scenario may explicitly publish the same domain event twice to model an upstream retry.
Reports must label this `PHYSICAL_DUPLICATE` and distinguish it from `OFFSET_REDELIVERY`.

## 12. Quiescence contract

A fixed sleep is never proof that execution completed.
The reference workload is quiescent only when all declared conditions remain true for a stability window.

Required reference conditions are:

- Kafka lag is zero for every declared consumer group.
- Probe in-flight counts are zero.
- No checkpoint is armed or blocked.
- The PostgreSQL outbox has no unpublished rows.
- The fake effect sink has no pending calls.
- Required terminal aggregate states have been reached.

Each condition records its observation time and evidence.
The stability window defaults to two seconds for local tests but is explicit in the scenario.
A quiescence timeout produces `UNRESOLVED` rather than a semantic regression.

## 13. Observation and oracle model

Every observer emits canonical JSON plus source metadata and a SHA-256 digest.

### 13.1 SQL snapshot

SQL observers use a dedicated read-only PostgreSQL role.
Queries are checked-in files with explicit columns and stable ordering keys.
The database role must reject writes.
An observation records the query hash, row count, column types, and normalized result.

### 13.2 SQL invariant

An invariant query returns zero rows when it passes.
Every returned row is a violation and becomes structured failure evidence.

### 13.3 Kafka output

Kafka observers read an explicit topic and bounded offset range.
Broker metadata is preserved separately from the normalized business record.
The compare mode must be exactly one of `ordered`, `set`, `multiset`, or `keyed`.
A list must never be silently sorted.

### 13.4 HTTP observation

HTTP observers call an explicitly declared local endpoint and require a JSON response.
External hosts are forbidden in V1.

### 13.5 External-effect ledger

The fake sink compares effect kind, business key, amount, and idempotency key.
Transport timestamps are ignored unless an explicit invariant uses them.

### 13.6 Normalization

Normalization supports exact JSON Pointer removal, exact-value replacement with a fixed token, stable ordering by declared keys, explicit numeric tolerance, and explicit nonsemantic timestamp normalization.
V1 does not support arbitrary scripts, regular expressions, templates, or expressions.
Every applied rule appears in the report beside the affected observer.

## 14. Failure classification

The result classification is one of:

- `PASS`
- `SEMANTIC_REGRESSION`
- `SCHEMA_REGRESSION`
- `EXTERNAL_EFFECT_REGRESSION`
- `PERFORMANCE_REGRESSION`
- `INFRASTRUCTURE_ERROR`
- `TIMEOUT`
- `FLAKY`
- `UNRESOLVED`

A stable failure signature contains the classification, observer or invariant ID, canonical JSON Pointer or row key, expected normalized value, actual normalized value, and a signature hash.
All violations are retained and sorted by classification precedence, observer ID, canonical pointer or row key, expected-value hash, and actual-value hash.
Regression precedence is `SCHEMA_REGRESSION`, `EXTERNAL_EFFECT_REGRESSION`, then `SEMANTIC_REGRESSION`.
Run-level states take precedence in this order: baseline infrastructure error, candidate infrastructure error, timeout or unresolved, flaky, then the ordered regression set.
The primary failure signature is derived from the first sorted violation and is independent of observer completion order.
The minimizer must preserve that exact primary signature rather than accepting any failing outcome.
Secondary violations remain in the report but may disappear when unrelated actions are removed.
Baseline infrastructure failure stops the run.
Candidate infrastructure failure is not misreported as a semantic mismatch.

## 15. Dependency-aware minimization

The minimizer uses a bounded hierarchical delta-debugging strategy over the scenario DAG.
It returns the smallest stable case found within the declared transformations and budget.
It may describe a result as 1-minimal relative to those transformations only.

Reduction order is:

1. Remove independent aggregate groups.
2. Remove concurrency branches.
3. Remove contiguous topological chunks.
4. Remove individual optional actions.
5. Remove optional faults.
6. Remove optional event fields while preserving schema validity.
7. Replace scalar values with schema-valid boundary candidates.

The reducer must preserve prerequisite closure and partition order.
It must not move an action across a dependency edge.
It must cache trials by target image digest, environment lock hash, and canonical scenario hash.
It must reproduce the original failure twice before reduction.
It must use a tri-state predicate of `PASS`, `SAME_FAILURE`, or `UNRESOLVED`.
It must stop as `FLAKY` when the original signature is unstable.
It must respect both trial-count and wall-clock budgets.
The report must show event-count and executable-action reduction.

Go fuzzing is reserved for parsers, schema handling, canonicalization, bundle reading, normalization, and DAG validation.
Go fuzzing is not the scenario minimizer.

## 16. Reference order-lifecycle workload

The repository includes a synthetic multi-service workload so the portfolio demonstrates the product rather than only its framework code.

Services are:

- `order-api`
- `outbox-relay`
- `order-workflow`
- `fulfillment-projector`
- `effect-sink`

Tables are:

- `orders`
- `order_state`
- `payments`
- `inventory_reservations`
- `fulfillment_projection`
- `processed_events`
- `outbox`
- `external_effects`

The core flow is:

```text
POST /orders
  -> orders and outbox transaction
  -> outbox relay publishes OrderCreated
  -> workflow emits PaymentRequested and InventoryRequested
  -> independent outcomes arrive in either legal order
  -> workflow advances the order
  -> projector updates the public view
  -> effect sink records local fake calls
```

The outbox relay may publish the same event after a crash between publish and marking the row complete.
Consumers therefore use stable event IDs and an idempotency table.
All effects are local and synthetic.

## 17. Seeded regression and control corpus

Each defect requires a correct baseline image, defective candidate image, triggering scenario, expected failure signature, and nearby passing control.

| ID | Seeded defect | Trigger | Expected evidence |
| --- | --- | --- | --- |
| R1 | Missing processed-event guard | Offset rewind after success | Duplicate inventory transition and invariant row. |
| R2 | Unstable external-effect idempotency | Kill after effect and before offset commit | Two capture ledger rows for one business operation. |
| R3 | Stale aggregate overwrite | Deliver older aggregate version later | Persisted version decreases. |
| R4 | Schema-default drift | Omit a compatible optional field | Baseline and candidate choose different fulfillment behavior. |
| R5 | Lost cross-stream transition | Release payment and inventory handlers in both orders | One legal ordering fails to reach terminal state. |
| R6 | Late cancellation mishandling | Advance watermark before old cancellation | Candidate applies an invalid late transition. |
| R7 | Duplicate outbox publication | Crash relay after publish | Candidate emits duplicate downstream business effects. |

Required passing controls include an optional logging field, different trace IDs, different transport timestamps, different JSON field order, a SQL refactor with the same projection, an unrelated aggregate, and a compatible schema addition with unchanged application defaulting.
The corpus is incomplete until both defects and controls pass their expected classification.

## 18. Repository layout

```text
chronicle-gate/
  BUILD_PLAN.md
  README.md
  LICENSE
  SECURITY.md
  CONTRIBUTING.md
  Makefile
  go.mod
  go.sum
  cmd/chronicle/main.go
  benchmarks/workloads/
  config/
    dependencies.lock.json
    images.lock.json
    images.lock.schema.json
  demo/
  docs/
  evidence/
    corpus.json
    results/
  examples/order-lifecycle/
    expected/
    scenarios/
    services/
    targets/
  internal/
    app/
    artifact/
    bench/
    broker/
    buildinfo/
    bundle/
    controlcontract/
    database/
    doctor/
    effects/
    engine/
    evidence/
    imagelock/
    minimize/
    observe/
    probeclient/
    registry/
    report/
    runlog/
    runtime/
    spec/
  pkg/probe/
  schemas/
  tests/
    benchmark/
    integration/
    fixtures/
  tools/
```

Only `pkg/probe` is a supported public Go API in V1.
Everything under `internal` may change without compatibility guarantees.

## 19. Artifact and reproduction format

A run directory contains:

```text
run.json
events.ndjson
scenario.authored.yaml
scenario.resolved.json
baseline.target.json
candidate.target.json
environment.lock.json
result.json
report.html
junit.xml
observations/
logs/
traces/
schemas/
minimized/
checksums.sha256
```

A reproduction ZIP contains only the minimum required source contracts, selected evidence, exact immutable image references, and checksums.
ZIP creation and extraction must reject absolute paths, `..` traversal, symlinks, device files, excessive file count, excessive expanded size, and duplicate paths.
Archive verification occurs before any image is started.
The replay command must display the images and requested resources before execution.

## 20. Security and privacy model

Docker socket access grants host-level authority and is a documented trust boundary.
ChronicleGate accepts trusted local scenarios and images only.
Third-party images should be evaluated in an ephemeral VM rather than a developer workstation.
Every container receives a private network, non-root user where supported, read-only root filesystem where feasible, dropped Linux capabilities, CPU and memory limits, and only required mounts.
No service receives the Docker socket.
No run command is accepted from a scenario.
No external network endpoint is allowed in V1 observers or fake effects.

SQL observers use a read-only role with statement timeouts.
Probe endpoints use random per-run credentials and bounded requests.
Logs must redact declared secret fields and must never dump the process environment.
Artifact directories use mode `0700` and sensitive files use `0600` on supporting platforms.
Dependency lock verification, `govulncheck`, `go vet`, and static analysis are required local release gates.

All examples and captured data are synthetic.
The repository must contain no production credentials, customer payloads, or personal data.

## 21. Telemetry policy

The accepted V1 evidence does not require a telemetry collector or profiler.
Correctness does not depend on external tracing or metrics availability.
OpenTelemetry exporter integration remains outside the accepted synthetic local scope.
Any future collector output must remain opaque diagnostic evidence and must not alter semantic classification.
Trace IDs, span IDs, timestamps, and container IDs are excluded from semantic comparison.

## 22. Performance benchmark policy

Performance work begins only after the complete correctness corpus is stable.
The benchmark uses fixed open-loop arrival schedules and a declared workload distribution.
Baseline and candidate trials alternate in randomized balanced order.
Warmup and measurement are separate.
The report preserves raw request timings and computes p50, p95, p99, throughput, error rate, and resource observations.
A regression requires both a practical threshold and a paired confidence interval.
The exact bootstrap method, block size, trial count, and rejection criteria must be locked before candidate data is examined.

Apple silicon with Colima is acceptable for local comparative development.
Publication performance evidence must run on a dedicated or verified idle environment.
Shared or hosted runners must not produce portfolio performance claims.
The benchmark must state that local results do not generalize to production clusters.

## 23. Verification strategy

### 23.1 Unit tests

- Test strict YAML decoding and unknown-field rejection.
- Test every semantic validation rule.
- Test deterministic canonical JSON and hashing.
- Test normalization by exact JSON Pointer.
- Test every comparison mode.
- Test failure signatures and classifications.
- Test offset-bound validation with fake admin responses.
- Test comparison allowlists and reserved runtime injection.
- Test schema fingerprint stability and unexpected DDL detection.
- Test post-scenario DDL detection and restored-template fingerprint verification.
- Test deterministic multi-violation ordering and primary signatures.
- Test missing secret references and artifact redaction.
- Test DAG closure and reduction transforms.
- Test cleanup idempotence.
- Test bundle path and size safety.
- Test secret redaction.
- Test command exit codes and JSON error shape.

### 23.2 Property and fuzz tests

- Any accepted scenario has an acyclic dependency graph.
- Removing actions through the reducer never leaves an unresolved dependency.
- Canonicalization is stable across map insertion order.
- Applying normalization twice is idempotent.
- Bundle verification never writes outside its destination.
- A successful encode and decode preserves supported contract fields.
- Invalid byte input cannot panic schema or bundle parsers.

### 23.3 Integration tests

- Start Redpanda and PostgreSQL through Testcontainers on each host architecture used for a runtime-compatibility claim.
- Publish and consume a CloudEvent through franz-go.
- Rewind a stopped group and observe the same broker record again.
- Create and restore the PostgreSQL template with active connections rejected.
- Enforce the SQL observer read-only role.
- Arm, observe, release, and time out a probe checkpoint.
- Prove a group offset cannot advance while the reference consumer is blocked at `before_offset_commit`.
- Reject a precise fault against an automatic-commit or multi-in-flight capability response.
- Kill and restart a service at the offset-commit window.
- Collect each built-in observer type.
- Clean all containers and networks after an injected test failure.

### 23.4 End-to-end tests

- R1 detects the missing idempotency guard and minimizes noise events.
- R2 detects the duplicate external effect after a crash window.
- R3 detects the stale aggregate overwrite.
- R4 detects semantic default drift despite schema compatibility.
- R5 exercises both legal cross-stream handler orders.
- Every control scenario passes.
- An unstable candidate becomes `FLAKY` rather than a regression.
- A quiescence failure becomes `UNRESOLVED`.
- A fresh checkout reproduces a saved bundle using only documented prerequisites.

### 23.5 Local validation commands

The repository should expose stable top-level commands:

```text
make fmt
make lint
make test
make test-integration
make test-e2e
make fuzz-smoke
make build
make verify
```

`make verify` is the authoritative local non-performance verification gate.

## 24. Ordered implementation milestones

### Milestone 0 - Bootstrap and locks

Create the Go module, license, contribution policy, Makefile, image lock schema, and `chronicle version` and `chronicle doctor` commands.
`doctor` verifies Docker reachability, Linux container support, required architecture, disk space, port allocation, and image digest availability.

Gate:

- The repository builds on macOS and Linux.
- `doctor --json` returns stable structured checks.
- Redpanda and PostgreSQL locks resolve for ARM64 and AMD64.
- No mutable image is accepted in publication mode.

### Milestone 1 - Typed contracts and validation

Implement scenario, target, workload, result, and bundle schemas plus strict YAML decoding and semantic validation.
Add valid and invalid golden fixtures for every rule.

Gate:

- Unknown fields and cycles fail before Docker access.
- Every example validates against both JSON Schema and Go semantic validation.
- Schema and typed-model round trips have golden tests.

### Milestone 2 - Broker-realistic vertical slice

Implement Testcontainers provisioning, Redpanda administration, PostgreSQL seeding and restore, the smallest projection consumer, SQL observation, and offset rewind.
Use R1 as the first failing candidate.

Gate:

- The exact same broker topic, partition, and offset is processed again.
- The baseline passes and the candidate produces the expected invariant signature.
- Cleanup leaves no ChronicleGate containers or networks.

### Milestone 3 - Stable reporting and minimization

Implement run state, structured observations, failure signatures, confirmation, DAG-aware reduction, JSON, JUnit, text, static HTML, and reproduction ZIP creation.

Gate:

- A noisy R1 scenario is reduced while preserving its exact signature.
- The original failure reproduces twice before minimization.
- An intentionally flaky fixture is never minimized.
- A bundle verifies and replays from a clean output directory.

### Milestone 4 - Probe and crash faults

Implement `pkg/probe`, private authenticated endpoints, logical clock, checkpoint client, and container kill and restart orchestration.
Instrument the reference workload.

Gate:

- R2 reliably stops at `after_external_effect`.
- The service restart reprocesses the original broker record.
- Incorrect token, checkpoint, event ID, or occurrence fails closed.
- Probe unavailability is a clear capability error rather than a hang.
- The reference consumer proves manual synchronous commit behavior through its capability handshake.
- The committed offset remains fixed at `before_offset_commit` and advances only after the explicit commit.

### Milestone 5 - Complete observer and schema model

Implement Kafka, HTTP, effect-ledger, SQL snapshot, SQL invariant, normalization, local JSON Schema validation, and registry metadata recording.

Gate:

- R4 demonstrates that structural compatibility can still produce a semantic regression.
- Volatile control fields normalize without hiding a business-field mismatch.
- Every applied normalization rule is visible in the report.

### Milestone 6 - Cross-stream and late-event corpus

Implement controlled handler release, logical clock advancement, aggregate version evidence, R3, R5, and R6.

Gate:

- Both declared legal interleavings run repeatably.
- No within-partition reordering is implemented or claimed.
- The late-event report distinguishes event time from delivery order.

### Milestone 7 - Outbox and complete seeded corpus

Implement the reference outbox relay, R7, all required controls, and the full end-to-end matrix.

Gate:

- Every seeded defect maps to its expected signature.
- Every approved nearby control passes.
- Repeating the full corpus does not leak state across attempts.

### Milestone 8 - Robustness and security

Add resource limits, signal handling, atomic artifacts, safe archives, read-only observers, fuzzing, dependency scanning, and documented trust boundaries.

Gate:

- Abrupt CLI interruption cleans resources and preserves an intelligible partial result.
- Archive traversal and expansion-bomb fixtures are rejected.
- Security, static analysis, and race tests pass.

### Milestone 9 - Performance command

Add the separately configured open-loop benchmark engine, raw timings, balanced trial order, bootstrap analysis, infrastructure validity checks, and a standalone report.

Gate:

- An unchanged A/A comparison does not falsely fail under the locked policy.
- A seeded practical slowdown is detected.
- Correctness instrumentation is demonstrably absent from the measured path.

### Milestone 10 - Portfolio release

Write public documentation, record a short demonstration, publish one reduced fault case and one control, and include raw evidence supporting every numerical claim.

Gate:

- A new user can reproduce the R1 walkthrough from a clean checkout.
- The README explains the problem, boundary, architecture, demo, results, and limitations before internal implementation details.
- Resume bullets contain only measured results.
- No generated container, database, raw trace, or secret is committed.

## 25. Portfolio-ready core and extended V1

The portfolio-ready core is complete at Milestone 4 when R1 and R2 work through the public CLI, failure confirmation and reduction are stable, a safe reproduction bundle replays, and the manual offset-commit claim is proven.
At that point the project is suitable for a truthful SWE resume bullet and demonstration even if later observer, corpus, performance, and release work remains.
The extended V1 definition of done remains Milestones 0 through 10.
Documentation must label which gate the current repository has reached.

## 26. Local verification design

`make verify` runs formatting, lint, unit, property, race, schema, dependency, security, release-integrity, fuzz-smoke, build, cross-build, and trusted Docker integration checks.
`make test-e2e` is the exact public alias for the complete Docker integration suite.
`make test-benchmark` runs the A/A and injected-slowdown functional benchmark gates separately from correctness qualification.
`make release-check` validates the tracked evidence inventory, historical source provenance, documentation claims, schemas, tracked-file policy, and exact result counts.
All release decisions are made from these local commands and their tracked evidence.

Tests must use unique resource labels and a run identifier.
Cleanup may remove only resources with the exact current test label.
No cleanup command may target all Docker containers, networks, volumes, topics, or databases.

## 27. Definition of done

ChronicleGate V1 is complete only when all conditions below are true.

- The broker-realistic offset rewind is demonstrated against the same stored Kafka record.
- Precise crash-window behavior is implemented through the documented opt-in probe.
- Baseline and candidate start from equivalent declared PostgreSQL state and isolated Kafka namespaces.
- The full seven-defect corpus and all controls produce their expected results.
- Stable failures are confirmed and reduced without changing the failure signature.
- Semantic correctness never depends on telemetry availability.
- Invalid, infrastructure, timeout, flaky, and semantic outcomes remain distinct.
- Reproduction bundles are hash-verified and safe to extract.
- Local ARM64 Docker development and Linux AMD64 and ARM64 cross-build paths are documented and tested within their stated boundaries.
- The performance report is separate and statistically defensible.
- Security and trust boundaries are prominent.
- A clean-checkout walkthrough succeeds using only the documented prerequisites.

## 28. Implementation-agent rules

The implementation agent should work on one milestone at a time.
It should begin each milestone by reproducing the preceding gate from the real CLI path.
It should add tests with every contract or behavior change.
It should preserve authored fixtures and never update expected outputs merely to make a failure disappear.
It should record dependency and image changes in lock files with their reason.
It should not add Kubernetes, a web service, a dashboard framework, Toxiproxy, or additional language SDKs before V1 is complete.
It should stop and document the evidence if a required Kafka or PostgreSQL semantic differs from this plan.
It should keep README claims narrower than the evidence.

## 29. Authoritative references

- [Apache Kafka design and delivery semantics](https://kafka.apache.org/41/design/design/)
- [Redpanda consumer offsets](https://docs.redpanda.com/current/develop/consume-data/consumer-offsets/)
- [Redpanda idempotent producers](https://docs.redpanda.com/current/develop/produce-data/idempotent-producers/)
- [Redpanda Schema Registry overview](https://docs.redpanda.com/current/manage/schema-reg/schema-reg-overview/)
- [Redpanda topic consumption reference](https://docs.redpanda.com/current/reference/rpk/rpk-topic/rpk-topic-consume/)
- [Redpanda quick start](https://docs.redpanda.com/current/get-started/quick-start/)
- [Redpanda 26.2.1 release](https://github.com/redpanda-data/redpanda/releases/tag/v26.2.1)
- [Testcontainers for Go Redpanda module](https://golang.testcontainers.org/modules/redpanda/)
- [Testcontainers for Go PostgreSQL module](https://golang.testcontainers.org/modules/postgres/)
- [Testcontainers for Go networking](https://golang.testcontainers.org/features/networking/)
- [Testcontainers for Go 0.44.0 release](https://github.com/testcontainers/testcontainers-go/releases/tag/v0.44.0)
- [franz-go repository](https://github.com/twmb/franz-go)
- [OpenTelemetry Go](https://opentelemetry.io/docs/languages/go/)
- [CloudEvents 1.0 specification](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md)
- [JSON Schema Draft 2020-12](https://json-schema.org/draft/2020-12)
- [Delta debugging](https://www.st.cs.uni-saarland.de/dd/)

Source snapshots inspected during planning include franz-go commit `c7ff0052662ac77241f6b5f1c6c5511479a22055` and Testcontainers for Go commit `4b21f2f07deef622fbe041c867454116f83038fa`.
These commit identifiers document the research basis and do not replace implementation dependency locks.

## 30. Verified milestone ledger and maintenance handoff

| Milestone | Status | Primary acceptance evidence |
| --- | --- | --- |
| M0 | Complete | `cmd/chronicle`, `internal/doctor`, `config/images.lock.json`, `make build`, and `make build-cross` |
| M1 | Complete | `internal/spec`, `schemas`, validation fixtures, and `make test` |
| M2 | Complete | `tests/integration/r1_test.go`, `internal/broker`, and `evidence/results/r1-offset-rewind.json` |
| M3 | Complete | `internal/minimize`, `internal/bundle`, R1 reduction evidence, and offline replay integration tests |
| M4 | Complete | `pkg/probe`, `internal/probeclient`, precise engine paths, and the R2 regression and control evidence |
| M5 | Complete | `internal/observe`, `internal/registry`, and the R4 regression and control evidence |
| M6 | Complete | `tests/integration/m6_test.go` and the R3, R5, and R6 regression and control evidence |
| M7 | Complete | `tests/integration/m7_test.go`, the R7 regression and control evidence, and `evidence/corpus.json` |
| M8 | Complete | `internal/artifact`, archive safety tests, interruption tests, `tools/security_check`, and `make verify` |
| M9 | Complete | `internal/bench`, `tests/benchmark`, `docs/benchmarking.md`, and `evidence/results/benchmark.json` |
| M10 | Complete | `README.md`, `docs/results.md`, `docs/reproduction.md`, `demo/r1-transcript.md`, and `make release-check` |

The accepted result is the complete Extended V1 implementation for trusted synthetic workloads on a local Docker environment.
The fourteen semantic evidence records contain seven intended regressions and seven nearby passing controls.
The fifteenth record contains the passing A/A benchmark control and the detected injected slowdown.
The benchmark point estimates, units, paired relative confidence intervals, and classifications are defined in `docs/benchmarking.md` and recorded in `evidence/results/benchmark.json`.
All checked-in numerical claims are validated against those machine-readable records by `make release-check`.

Production reliability, production capacity, arbitrary service qualification, hostile-container isolation, and cross-platform portability of development-local images remain explicit non-claims.
Dedicated-host publication benchmarking is optional future evidence and is not a missing V1 gate.
No tag, hosted release, deployment, or external publication is required for the accepted repository state.
There is no current implementation blocker or required continuation task for V1.

### 30.1 Maintenance verification boundary

The 2026-08-14 maintenance cleanup changes local repository policy documentation and removes obsolete hosted-automation validators without changing product runtime code, Dockerfiles, scenarios, expected signatures, or tracked evidence.
The current tree passed the non-integration verification gates and the compiled benchmark suite, including both A/A controls and both injected-slowdown trials.
A fresh current-tree Docker integration rerun did not reach a semantic assertion because the shared Docker backing store reached capacity and the run returned `INFRASTRUCTURE_ERROR` while waiting for the first delivery record.
That infrastructure-only rerun is not presented as an integration pass and does not replace the source-bound historical acceptance records in `evidence/results/`.
Before making any new runtime claim from a later maintenance change, rerun `make test-e2e` on a Docker environment with the documented free-space margin.

For any later maintenance change, run the complete local gate before preserving it:

```sh
make verify
make test-e2e
make test-benchmark
make release-check
git status --short
```

Stage only the intended files, use a focused Conventional Commit, and push the verified commit directly to `main`:

```sh
git add -- path/to/changed-file
git commit -m "type(scope): concise change"
git push origin main
test "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)"
```

If a future change alters a scenario, expected signature, benchmark estimator, evidence schema, or documentation claim, regenerate and validate the affected evidence rather than editing an expected result to hide a failure.

# Controlled schedule and event-time evidence

ChronicleGate Milestone 6 implements exact handler-release schedules for R3, R5, and R6.
The executor accepts only the typed `aggregate-version`, `cross-stream`, and `event-time` shapes declared by `spec.control`.
Any scenario that contains a controlled action but does not satisfy one exact shape is rejected before Docker access.

## Runtime topology

The target contains one repository-trusted `order-workflow` service with an authenticated `chronicle-probe/v1alpha1` endpoint.
The runtime supplies a deterministic private JSON contract through the same read-only secret mount used by the service credentials.
The contract is keyed by logical topic and event ID, maps to the exact authored step ID, and records its SHA-256 digest in attempt evidence.

The service runs one franz-go client for each logical topic.
Every client polls one record, blocks rebalancing during processing, commits synchronously, and allows rebalancing only after an independent administrative read proves the exact next offset.
The probe-wide controlled capacity is two.
Every consumer has processing capacity one.
These two capacity claims are recorded separately.

Every target container runs as the invoking numeric non-root user with a read-only root filesystem, all capabilities dropped, `no-new-privileges`, bounded CPU, memory, and PID resources, one private read-only mount, no Docker socket, and loopback-only host mappings.
The executor inspects these controls before publishing.

## Schedule validity

R3 and its control use two records on the same logical topic and partition.
The first handler is released, its transition evidence is visible, and its group offset is committed before the second checkpoint is armed.
The second record therefore has a later physical offset and a later transition sequence.
This is ordinary append and processing order, not broker reordering.

R5 uses two distinct logical topics, each on partition `0`, and one independently assigned consumer per topic.
Both exact `before_handler` checkpoints must be visibly blocked before either release.
The executor then releases payment-first or inventory-first according to the authored dependency graph.
The implementation deliberately rejects two partitions of the same topic because that topology is not independently controlled in V1.

R6 releases and commits the active-state event, proves the declared stability window, records a clock-advance intent in the run journal, and requires an exact acknowledgement.
Only then does it arm and publish the cancellation.
The cancellation has aggregate version `2`, so the late-event defect cannot be explained by the stale-version rule tested by R3.

## Evidence required before comparison

An attempt is comparable only when all of the following evidence is complete.

- Every fresh group was initialized while empty and later contained exactly the expected client assignment.
- Every publication has one exact probe receipt with matching topic, partition, offset, key, event ID, and payload digest.
- Every release appears in the authored order and reaches committed offset plus one before the next controlled action.
- Append-only aggregate transition sequences match the release schedule and source broker identities.
- Every declared observation executes exactly once with the same logical inventory for baseline and candidate.
- Kafka lag, probe work, checkpoints, outbox state, and aggregate evidence remain quiescent through the stability window.
- The database schema fingerprint matches the frozen template after health and after observation.

Missing or contradictory healthy evidence is `UNRESOLVED`.
Provisioning, broker administration, service, database, and cleanup errors are `INFRASTRUCTURE_ERROR`.
Bounded operation deadlines are `TIMEOUT`.
Only complete semantic attempts participate in confirmation.

## Time-domain reporting

R6 evidence records physical delivery sequence, source offset, CloudEvent event time, acknowledged logical watermark, and logical time captured at receipt before checkpoint release.
The late-event predicate is `eventTime < acknowledgedWatermark <= deliveryLogicalTime`.
The report renders these values separately in JSON, text, JUnit, and static HTML.

The measured late case uses event time `2026-08-13T11:00:00Z`, watermark `2026-08-13T13:00:00Z`, delivery logical time `2026-08-13T13:00:00Z`, and source offsets `0` then `1`.
The baseline records `ignored_late` and remains `active`.
The seeded candidate records `applied` and becomes `cancelled`.

## Measured milestone evidence

The M6 integration gate passed on 2026-08-13 in 312.99 seconds on the documented local ARM64 environment.
It executed R3, the R3 monotonic control, both R5 legal release orders, R6, and the R6 on-time control with one baseline and three candidate attempts each.
It then deleted source images and reproduced the exact checked-in R3, R5, and R6 signatures from three separately verified bundles.
The final complete M0 through M6 integration matrix passed in 897.8 seconds, with its M6 corpus and replay case completing in 358.95 seconds.
These durations describe functional acceptance execution and are not product performance claims.

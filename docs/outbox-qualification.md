# Transactional outbox qualification evidence

ChronicleGate Milestone 7 implements the connected order-lifecycle topology, the R7 duplicate-publication defect, its nearby controls, and the complete R1 through R7 corpus gate.

## Connected topology

The qualification uses five repository-trusted services.

```text
POST /orders
  -> orders and outbox transaction
  -> outbox relay publishes OrderCreated
  -> order workflow emits PaymentRequested -> effect sink records one local synthetic business effect
  -> order workflow emits InventoryRequested
  -> independent payment and inventory outcomes
  -> fulfillment projector emits FulfillmentReady
```

The order API and relay use separate least-privilege PostgreSQL roles.
The order API can create orders and their initial outbox rows but cannot modify relay publication evidence.
The relay can claim, publish, and complete outbox rows but cannot insert or alter orders.
The observer role remains read-only with a statement timeout.

All five containers run as a numeric non-root identity with read-only root filesystems, dropped Linux capabilities, `no-new-privileges`, bounded CPU, memory, and PID resources, no Docker socket, and loopback-only host mappings.
The relay and effect sink receive only their required private read-only token mounts.

## Exact R7 crash window

The relay increments the durable outbox attempt counter, publishes one record with `ProduceSync`, and requires the broker to return one exact nonnegative offset.
It then writes an append-only acknowledgement row containing the outbox row ID, logical event ID, emitted event ID, attempt, physical topic, partition, offset, payload SHA-256 digest, and observation time.

Only after that durable evidence exists does the relay enter the authenticated `after_outbox_publish` checkpoint.
The harness verifies the connected flow has reached its terminal state, the effect sink is idle, the trigger delivery is exact, and the relay trigger offset is committed.
It sends `SIGKILL` before the relay marks the outbox row published, waits until the relay group is empty, and restarts the same exact image.
The restarted probe must have a fresh cryptographically random process identity and no inherited checkpoint or receipt state.

The baseline emits the same logical CloudEvent ID for attempts one and two.
The downstream workflow treats the second physical record as the same logical operation and retains one effect.
The R7 candidate emits the logical ID on attempt one and a deterministic content-derived retry ID on attempt two.
The downstream workflow therefore records two business effects for one order and produces `EXTERNAL_EFFECT_REGRESSION` at `/entries/count`.

## Comparable evidence

An R7 attempt participates in semantic comparison only after all required evidence is complete.

- Every fresh group is initialized at offset zero while empty and later has exactly its expected assigned client.
- Both acknowledged outbox publications have contiguous sequence, attempt, and physical offset evidence.
- The baseline keeps the logical event ID stable across both publications.
- The candidate changes only the second emitted event ID in the seeded crash case.
- All eight topic bounds and all seven final consumer-group positions are retained.
- Every group position equals the corresponding topic end.
- The relay probe has zero work and no armed or blocked checkpoint.
- The outbox has no unpublished row and the effect sink has no pending operation.
- Required order, payment, inventory, projection, and effect states remain stable for the authored window.
- Every declared observation executes exactly once with the same logical identity for baseline and candidate.
- The database fingerprint matches the frozen `order-lifecycle-v3` template after health and after observation.

Contradictory or incomplete healthy evidence is `UNRESOLVED`.
Provisioning, broker, database, service, artifact, or cleanup failures are `INFRASTRUCTURE_ERROR`.
Bounded operation deadlines are `TIMEOUT`.
Only complete semantic attempts can confirm R7.

## Nearby controls

The Milestone 7 control matrix adds or strengthens these public-CLI checks.

| Control | Expected outcome | Evidence boundary |
| --- | --- | --- |
| R7 unrelated orders | `PASS`, exit `0` | Two independent outbox rows each publish once with stable IDs and produce two independent effects. |
| R1 single delivery | `PASS`, exit `0` | The R1 candidate produces one reservation when no offset rewind occurs. |
| R2 no crash | `PASS`, exit `0` | The R2 candidate uses one effect when the original physical record is delivered once. |
| R4 transport metadata | `PASS`, exit `0` | Broker timestamps, trace context, and top-level JSON key order differ while the normalized business observation remains equal. |
| R4 explicit default and SQL refactor | `PASS`, exit `0` | Compatible schema addition, optional logging field, and equivalent SQL projection do not hide or create a business difference. |
| R3 monotonic version | `PASS`, exit `0` | An increasing version is accepted by the R3 candidate. |
| R5 payment first | `PASS`, exit `0` | The alternative legal cross-stream release order reaches the terminal state. |
| R6 on-time cancellation | `PASS`, exit `0` | A cancellation after the watermark rule remains applicable. |

## Public release evidence

The source-authenticated R7 crash and unrelated-orders control records are published in [`evidence/results`](../evidence/results).
The sanitized records retain exact outcomes, signatures, bounded observation summaries, artifact digests, and source provenance.
The publisher emits a record only after its private capture proves exact cleanup with no retained attempt resources.
The private checksummed runs retain acknowledged publications, topic bounds, group positions, quiescence samples, database fingerprints, and verified embedded-image bundles.

## Claim boundary

The evidence supports deterministic local qualification of the repository-trusted synthetic topology under the locked environment and documented resource assumptions.
It does not claim arbitrary third-party service discovery, production data safety, cross-platform portability for development-local image bundles, or production benchmark capacity.

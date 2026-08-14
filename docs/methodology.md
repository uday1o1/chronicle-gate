# Qualification methodology

ChronicleGate treats a release claim as valid only when authored intent, runtime identity, execution evidence, comparison, and cleanup all pass their own gates.
This document describes the method used by the checked-in seven-defect reference corpus.

## 1. Validate before side effects

Raw YAML is validated against bundled Draft 2020-12 schemas before strict typed decoding.
Semantic validation then checks bounded sizes and durations, dependency closure, runtime references, immutable image rules, fault ordering, observer identity, normalization pointers, capability declarations, and baseline-candidate equivalence.
Input errors return before Docker access.

## 2. Resolve exact identities

Named images resolve from an OCI index digest to exactly one runtime child manifest for the Docker platform.
Attestation descriptors are excluded from runtime selection.
Development-local image IDs are content-addressed but explicitly nonportable and cannot support publication-scoped benchmark evidence.

The result records the source target contract, executed config-image identity, workload and schema inputs, environment lock, Docker architecture, and database template fingerprint.
The release evidence layer additionally binds the executable digest and embedded source commit to the exact Git source tree.

## 3. Establish equivalent state

Each baseline and candidate attempt begins with a fresh PostgreSQL clone from the same frozen template and fresh attempt-prefixed Kafka resources.
Consumer positions are initialized only while their groups are empty.
The harness checks database schema fingerprints after health and after observation so a candidate cannot change the observation contract silently.

## 4. Execute the authored schedule

The orchestrator follows the explicit scenario dependency graph.
Faults and controlled releases require authenticated probe state plus independent broker or database evidence.
Every published CloudEvent is locally schema-validated and retains its exact topic, partition, offset, key, and payload digest.
Kafka record keys must equal the event aggregate identifier.

Controlled cases have a single accepted schedule shape.
Any controlled action that cannot match that shape is rejected instead of falling through to a less precise executor.

## 5. Prove quiescence

Observation begins only after the declared stability window proves all required terminal conditions continuously.
The condition set can include zero consumer lag, zero probe work, no armed or blocked checkpoint, no pending sink work, no unpublished outbox row, required database state, and exact committed positions.
Failure to establish complete healthy quiescence is unresolved rather than a pass.

## 6. Collect a complete inventory

Every declared observation has a logical identity composed of observe-step ID, observer ID, and occurrence.
It executes exactly once per attempt.
Baseline and candidate must complete the same inventory before comparison.
Missing, duplicate, or unexpected observations cannot be omitted from the result.

Collectors retain raw schema validity, source metadata, canonical value, normalization evidence, and a digest.
Secret paths are redacted before canonicalization or hashing.

## 7. Compare and classify

Comparison modes are ordered, set, multiset, and keyed.
Normalization is declarative and records every affected pointer.
Correctness and performance remain separate products with separate schemas, artifacts, and decision policies.

The semantic classifier preserves these boundaries.

| Condition | Classification | Exit |
| --- | --- | ---: |
| Confirmed exact semantic signature | Regression class from the observer | `2` |
| Authored contract invalid | Input error | `3` |
| Provision, service, broker, database, artifact, or cleanup failure | `INFRASTRUCTURE_ERROR` | `4` |
| Complete semantic attempts disagree | `FLAKY` | `5` |
| Bounded operation deadline | `TIMEOUT` | `5` |
| Healthy execution lacks complete semantic evidence | `UNRESOLVED` | `5` |
| External cancellation with successful cleanup | `INTERRUPTED` state | `130` |

Infrastructure and timeout outcomes never masquerade as candidate instability.
Cleanup failure has infrastructure precedence while preserving the observed evidence.

## 8. Confirm and reduce

Regression confirmation repeats the original scenario from equivalent fresh state.
Only matching complete semantic signatures count toward confirmation.
The public reference corpus requires the original failure plus two matching confirmations for seeded regressions.

Reduction proposals are dependency-safe scenario transforms.
Every proposal obtains fresh baseline and candidate evidence twice and tests the exact original signature.
The cache key includes both resolved targets, environment and workload inputs, the complete referenced closure, and canonical scenario content.
Operationally unresolved trials are never accepted.

## 9. Finalize evidence and clean up

Per-attempt evidence is atomically finalized before its topic is deleted.
Exact-scope cleanup runs before the top-level result becomes final.
The append-only journal records ordered acquisition, execution, cleanup, and terminal state transitions.
A valid `COMPLETE` journal record is required for a completed run.

Checksums cover the immutable artifact inventory.
The journal is intentionally excluded because its terminal record is the final successful write.
Bundle replay verifies the archive, input closure, schemas, target identities, and embedded image identity before Docker access.

## 10. Publish only bounded claims

The public release evidence contains an exact corpus inventory, source commit and tree, binary digest, exact CLI arguments, input hashes, outcomes, bounded observation summaries, reduction facts, artifact digests, and cleanup counts.
Raw runs, image archives, database data, tokens, process environment, and traces stay outside Git.
Every public numerical result points to a machine-readable JSON value in [`evidence/results`](../evidence/results).

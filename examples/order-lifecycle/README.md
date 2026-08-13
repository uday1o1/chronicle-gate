# Order lifecycle R1 and R2 vertical slices

This example qualifies an inventory projection invariant and a precise external-effect crash invariant against correct baselines and seeded candidates.

## What the run proves

ChronicleGate publishes one CloudEvent as one Kafka record with key `order-1`.
The reference consumer uses franz-go with automatic commits disabled, one-record polling, blocked rebalance during processing, and synchronous record commits.
The harness waits for confirmed processing, stops the only expected group member, and requires the group to become empty.
It then validates the requested offset against the broker's `[start,end)` range, changes the committed offset from `1` to `0`, reads it back, and restarts the same target with the same group.
Both recorded deliveries must contain the same topic, partition `0`, offset `0`, key, event ID, and locally validated CloudEvent digest.
The final committed offset must return to `1`.

Each attempt starts from a PostgreSQL database cloned from a frozen migrated template.
The template fingerprint is verified before every clone.
The attempt fingerprint must match after service health and again after observation.
SQL observation uses a dedicated login whose default transactions are read-only and whose statement timeout is five seconds.
The live gate proves that both `INSERT` and DDL fail for this observer.

The correct baseline records two deliveries and one reservation.
The seeded candidate records two deliveries and two reservations.
The candidate result is accepted as `SEMANTIC_REGRESSION` only when the first failure and two fresh confirmation attempts produce the exact signature in [`expected/r1-signature.json`](expected/r1-signature.json).

## Isolation and cleanup

The baseline and every candidate confirmation receive different database, topic, and consumer-group names.
Only resources with the exact current run label and handles are cleaned.
Attempt evidence is atomically finalized before its topic is deleted.
The top-level result is atomically finalized after cleanup, so a cleanup failure overrides a semantic conclusion with `INFRASTRUCTURE_ERROR`.
The integration suite queries Docker after both a successful run and an injected missing-image failure and requires zero matching containers and networks.

## Precise R2 crash window

`r2-crash-after-effect.yaml` initializes offset `0` for a fresh empty group before the consumer starts.
The workflow proves the `chronicle-probe/v1alpha1` capability handshake, including manual synchronous commits, one controlled in-flight record, named checkpoints, and deterministic logical time.
The harness arms the exact first `after_external_effect` occurrence before publication and waits until both the checkpoint and the first canonical sink entry are visible.
It verifies that the committed position is still `0`, checks that the group contains exactly the expected member, sends `SIGKILL`, and waits for group membership to become empty.
After restart, a new probe process instance must report the same orchestrator-owned logical time and a different process identifier.
The second probe receipt must identify the same topic, partition, offset, key, event ID, and event digest as the first receipt.

The baseline sink observation contains one stable effect entry.
The seeded R2 candidate contains two entries with different idempotency keys for one business key.
The exact count-based failure signature remains stable even though the defective random keys differ between confirmation attempts.
All effect entries retain amount and source broker metadata, and the observer verifies the canonical ledger digest before comparison.

`manual-offset-commit-control.yaml` arms both `before_offset_commit` and `after_offset_commit`.
It proves the group position remains `0` at the first gate, releases processing, and proves the position is `1` before the second gate becomes visible.
Completion additionally requires lag zero, no in-flight work, no armed or blocked checkpoints, an empty outbox, no pending sink calls, and the terminal database state for a continuous two-second stability window.

## Isolation and security

The workflow and effect sink run as a numeric non-root identity with a read-only root filesystem, all Linux capabilities dropped, `no-new-privileges`, CPU, memory, and PID limits, and loopback-only host mappings.
Each service receives one private read-only directory containing only its required `0600` token files.
The sink writer credential cannot observe the ledger, and the observer credential cannot append effects.
Neither service receives the Docker socket.

## Current boundaries

The generated local image IDs are content-addressed but not registry-resolvable or cross-platform portable.
They are accepted only with `--development-local-images`.
Milestone 5 will complete the broader observer and schema-default corpus beyond the portfolio-ready core.

## Stable reduction and replay

`r1-offset-rewind-noisy.yaml` adds one optional, independent diagnostic event on an unsubscribed topic.
The minimizer first confirms the original candidate signature twice, then tests the optional closure with two fresh baseline and candidate comparisons.
The accepted reduction removes one event and one executable action while preserving the exact checked-in signature.
The result reports whether relative 1-minimality was proven, not proven, or unavailable.

An intentionally flaky projector alternates between defective and guarded behavior by attempt identity.
Its completed semantic outcomes disagree, so the run returns `FLAKY` with exit code `5`, performs zero reducer trials, and creates no reproduction bundle.

Every confirmed regression writes `report.json`, `report.txt`, `junit.xml`, `report.html`, `checksums.sha256`, and `reproduction.zip`.
Local development bundles embed exact image archives and remain explicitly nonportable.
Before replay, ChronicleGate verifies normalized unique paths, regular-file types, file and expanded-size limits, SHA-256 records, strict contracts, exact target identities, OCI descriptors, layer digests, and uncompressed layer DiffIDs.
The integration gate deletes both source images before replay to prove that the verified embedded archives restore the exact content-addressed images.

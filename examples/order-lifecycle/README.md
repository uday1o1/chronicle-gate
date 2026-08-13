# Order lifecycle R1 vertical slice

This example qualifies a single inventory projection invariant against a correct baseline and a candidate with a seeded missing-idempotency guard.

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

## Current boundaries

The generated local image IDs are content-addressed but not registry-resolvable or cross-platform portable.
They are accepted only with `--development-local-images`.
Milestone 3 will add generalized confirmation, minimization, reports, and reproduction bundles.
Milestone 4 will add the authenticated probe and precise crash checkpoints.

# Observer and schema evidence model

ChronicleGate executes every authored observe step exactly once per attempt after the declared quiescence gate passes.
The logical identity is the observe-step ID, observer ID, and one-based occurrence.
Baseline and candidate attempts must complete the same ordered inventory with no missing, duplicate, or unexpected identity before comparison begins.

Every observation uses `chronicle.dev/observation/v1alpha1` and retains its source metadata, canonical normalized JSON, SHA-256 digest, compare mode, record count, implicit metadata exclusions, and applied normalization evidence.
Physical database names, attempt-prefixed topics, offsets, endpoints, and Registry subjects diagnose execution but are not semantic equality inputs.

## Built-in observers

SQL snapshot observers execute checked-in queries with a dedicated login whose transactions are read-only and whose statement timeout is bounded.
Each result retains the query hash, declared ordering keys, PostgreSQL column OIDs, row count, and normalized rows.
The harness checks the complete database schema fingerprint after service health and after observation.

SQL invariant queries pass only when they return zero rows.
Every returned row is structured violation evidence rather than a rewritten expected output.

Kafka observers use a direct non-group consumer and never commit an offset.
The harness freezes the partition's log bounds at the declared observation point and consumes visible records in the intersection with the authored half-open range.
Kafka offsets need not be contiguous, and a range shortfall remains semantic evidence rather than becoming a collector timeout.
Record keys must equal the CloudEvent `aggregateid`.
Broker timestamps and leader epochs remain source metadata.
Recognized W3C trace-context headers may be excluded with a recorded reason, while all other headers remain semantic unless an exact nonsemantic header declaration says otherwise.
Header names and values use reversible base64 encoding and preserve duplicate wire order, including invalid UTF-8 bytes.

HTTP observers resolve only an explicitly declared target service, container port, and path through a loopback-only host mapping.
Redirects are disabled, response size and content type are bounded, and JSON duplicate keys or trailing values are rejected.

Effect-ledger observers verify the sink's canonical digest and compare effect kind, business key, amount, and idempotency key.
Source topic, partition, and offset remain retained evidence.
Transport timing is not a business comparison field unless a later invariant declares it.

## Comparison and normalization

The exact compare mode is `ordered`, `set`, `multiset`, or `keyed`.
Ordered Kafka records cannot be top-level sorted.
Stable ordering requires unique declared composite keys.
Numeric tolerance is allowed only after deterministic ordered or keyed pairing because tolerance is not transitive.

Normalization supports exact JSON Pointer removal, fixed-token replacement, stable ordering by explicit keys, bounded numeric tolerance, and explicit RFC 3339 timestamp replacement.
No scripts, regular expressions, templates, or implicit business-field exclusions are supported.
Each rule records the logical observation identity, authored pointer, affected pointers and count, and before and after digests.
These records appear in JSON, text, JUnit, and static HTML reports.

## Schema evidence and classification

Authored payload schemas and their complete local `$ref` graph are validated before Docker access.
External schema references and references that escape the scenario root are rejected.
Registry registration uses fresh attempt-prefixed subjects and records the effective compatibility mode, source and self-contained schema hashes, assigned IDs and versions, predecessor versions, and compatibility responses.
Generated self-contained schemas are compiled locally before registration.

Invalid authored schemas are input errors.
Registry request, configuration, or read-back corruption is an infrastructure error.
An invalid baseline runtime payload makes the comparison unresolved.
A candidate-only payload validation failure after a valid baseline is a confirmable `SCHEMA_REGRESSION`.
Equal valid schemas with different business behavior are a confirmable `SEMANTIC_REGRESSION`, as demonstrated by R4.

## Measured milestone evidence

The complete Docker-backed M0-M5 integration suite passed on 2026-08-13 in 455 seconds on the documented local ARM64 environment.
The R4 semantic regression, explicit-default control, candidate-only schema regression, invalid-baseline control, source-image removal, verified offline replay, and four-format normalization report checks completed in 129.24 seconds within that run.
These durations describe functional acceptance execution and are not product performance claims.

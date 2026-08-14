# Results and evidence

ChronicleGate publishes a sanitized machine-readable record for every reference regression and nearby control.
Each record binds the observed result to an exact Git commit and tree, executable digest and embedded commit, CLI argument vector, complete input closure, and artifact hashes.
The publisher emits a record only after its private capture proves exact cleanup with no retained attempt resources.
Raw runs, journals, reports, bundles, image archives, service logs, and database data remain outside Git.

## Semantic corpus

The release inventory covers seven seeded defect families and seven nearby passing controls.
Every seeded defect reached its declared complete regression classification after two matching confirmations, and every control completed with `PASS`.

| Claim | Regression result | Control result |
| --- | --- | --- |
| R1 offset rewind | `SEMANTIC_REGRESSION`, `344a2c61a869abdfc6a272ea4a35eb247158d41b619049cb8b6618ba71b6b29c` | Single delivery: `PASS` |
| R2 crash after effect | `EXTERNAL_EFFECT_REGRESSION`, `34904ab76052369ba67bbc30ae12855b0665c884c24b92ec31c081388176fc17` | Manual synchronous commit: `PASS` |
| R3 stale aggregate | `SEMANTIC_REGRESSION`, `299ffe3cd73f28980cc07879aaf96082b1f2b9216ba5b33325c37704cb84524a` | Monotonic version: `PASS` |
| R4 schema-default drift | `SEMANTIC_REGRESSION`, `e63414cbd4034d53a4f5d0b569981a70ec041c488b20a4a256adcad650c594e9` | Explicit default and equivalent SQL projection: `PASS` |
| R5 inventory-first release | `SEMANTIC_REGRESSION`, `c2fad8b52cce868bcd572f8d8e0defc8ef1e6cf6abddf00fb1008f44c630c4c2` | Payment-first release: `PASS` |
| R6 late cancellation | `SEMANTIC_REGRESSION`, `022eb460fefdb25079faba076eae57fbe98ddad6b51d4950cab87ffd088685d0` | On-time cancellation: `PASS` |
| R7 outbox crash after acknowledgement | `EXTERNAL_EFFECT_REGRESSION`, `b6a2655ffc67568d7255c2d10fd7f70c1fb037cf206c68d82bde024e7aa823c4` | Unrelated orders: `PASS` |

<!-- measured: evidence/results/r1-offset-rewind.json#/outcome/classification -->
<!-- measured: evidence/results/r1-offset-rewind.json#/outcome/failureSignature/digest -->
<!-- measured: evidence/results/r1-offset-rewind.json#/outcome/confirmations -->
<!-- measured: evidence/results/r1-single-delivery-control.json#/outcome/classification -->
<!-- measured: evidence/results/r2-crash-after-effect.json#/outcome/classification -->
<!-- measured: evidence/results/r2-crash-after-effect.json#/outcome/failureSignature/digest -->
<!-- measured: evidence/results/r2-crash-after-effect.json#/outcome/confirmations -->
<!-- measured: evidence/results/r2-manual-commit-control.json#/outcome/classification -->
<!-- measured: evidence/results/r3-stale-aggregate.json#/outcome/classification -->
<!-- measured: evidence/results/r3-stale-aggregate.json#/outcome/failureSignature/digest -->
<!-- measured: evidence/results/r3-stale-aggregate.json#/outcome/confirmations -->
<!-- measured: evidence/results/r3-monotonic-control.json#/outcome/classification -->
<!-- measured: evidence/results/r4-schema-default-drift.json#/outcome/classification -->
<!-- measured: evidence/results/r4-schema-default-drift.json#/outcome/failureSignature/digest -->
<!-- measured: evidence/results/r4-schema-default-drift.json#/outcome/confirmations -->
<!-- measured: evidence/results/r4-defaulting-control.json#/outcome/classification -->
<!-- measured: evidence/results/r5-inventory-first.json#/outcome/classification -->
<!-- measured: evidence/results/r5-inventory-first.json#/outcome/failureSignature/digest -->
<!-- measured: evidence/results/r5-inventory-first.json#/outcome/confirmations -->
<!-- measured: evidence/results/r5-payment-first-control.json#/outcome/classification -->
<!-- measured: evidence/results/r6-late-cancellation.json#/outcome/classification -->
<!-- measured: evidence/results/r6-late-cancellation.json#/outcome/failureSignature/digest -->
<!-- measured: evidence/results/r6-late-cancellation.json#/outcome/confirmations -->
<!-- measured: evidence/results/r6-on-time-control.json#/outcome/classification -->
<!-- measured: evidence/results/r7-outbox-crash.json#/outcome/classification -->
<!-- measured: evidence/results/r7-outbox-crash.json#/outcome/failureSignature/digest -->
<!-- measured: evidence/results/r7-outbox-crash.json#/outcome/confirmations -->
<!-- measured: evidence/results/r7-unrelated-orders-control.json#/outcome/classification -->

The R1 noisy scenario began with two events and seven actions.
The reducer retained the exact signature while reducing it to one event and six actions in eight fresh baseline-candidate trial pairs, then reported relative one-minimality as proven.
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/originalEvents -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/finalEvents -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/originalActions -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/finalActions -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/trials -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/minimality -->

## Performance gate

The performance evidence contains one A/A control and one seeded-slowdown comparison under the same authored request plan and locked paired-bootstrap policy.
The A/A comparison completed with `PASS` and no regression decision.
The seeded slowdown completed with `PERFORMANCE_REGRESSION` after crossing both the absolute p95 delta threshold and the lower relative confidence-bound threshold.
<!-- measured: evidence/results/benchmark.json#/comparisons/0/outcome/classification -->
<!-- measured: evidence/results/benchmark.json#/comparisons/0/outcome/regression -->
<!-- measured: evidence/results/benchmark.json#/comparisons/1/outcome/classification -->
<!-- measured: evidence/results/benchmark.json#/comparisons/1/outcome/regression -->

| Comparison | Rounds | Requests per target | Baseline p95 | Candidate p95 | Mean paired p95 delta | Relative confidence interval | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| A/A | 4 | 160 | 2,847,375 ns | 2,874,875 ns | -720,500.25 ns | -0.373703 to 0.050282 | `PASS` |
| Seeded slowdown | 4 | 160 | 6,286,625 ns | 29,865,292 ns | 25,425,229 ns | 3.682243 to 9.914886 | `PERFORMANCE_REGRESSION` |

<!-- measured: evidence/results/benchmark.json#/comparisons/0/outcome/rounds -->
<!-- measured: evidence/results/benchmark.json#/comparisons/0/outcome/baselineRequests -->
<!-- measured: evidence/results/benchmark.json#/comparisons/0/outcome/candidateRequests -->
<!-- measured: evidence/results/benchmark.json#/comparisons/0/outcome/baselineP95Nanos -->
<!-- measured: evidence/results/benchmark.json#/comparisons/0/outcome/candidateP95Nanos -->
<!-- measured: evidence/results/benchmark.json#/comparisons/0/outcome/meanAbsoluteP95DeltaNanos -->
<!-- measured: evidence/results/benchmark.json#/comparisons/0/outcome/lowerRelativeCI -->
<!-- measured: evidence/results/benchmark.json#/comparisons/0/outcome/upperRelativeCI -->
<!-- measured: evidence/results/benchmark.json#/comparisons/1/outcome/rounds -->
<!-- measured: evidence/results/benchmark.json#/comparisons/1/outcome/baselineRequests -->
<!-- measured: evidence/results/benchmark.json#/comparisons/1/outcome/candidateRequests -->
<!-- measured: evidence/results/benchmark.json#/comparisons/1/outcome/baselineP95Nanos -->
<!-- measured: evidence/results/benchmark.json#/comparisons/1/outcome/candidateP95Nanos -->
<!-- measured: evidence/results/benchmark.json#/comparisons/1/outcome/meanAbsoluteP95DeltaNanos -->
<!-- measured: evidence/results/benchmark.json#/comparisons/1/outcome/lowerRelativeCI -->
<!-- measured: evidence/results/benchmark.json#/comparisons/1/outcome/upperRelativeCI -->

The public benchmark JSON retains the paired round count, request inventories, baseline and candidate p95 aggregates, mean absolute p95 delta, and lower and upper relative confidence bounds shown above.
These are development-scoped comparative measurements from the locked local environment, not production capacity claims.
See [Benchmarking](benchmarking.md) for the exact estimator and validity rules.

## Evidence interpretation

- `PASS` means the complete declared baseline and candidate observations matched under that scenario.
- A regression means fresh complete attempts reproduced the same exact checked-in failure signature.
- A bundle digest proves the captured reproduction ZIP bytes, not portability of development-local image IDs to another platform.
- Observation summaries publish type, count, and raw-schema validity without publishing business payloads or secret-derived values.
- Source provenance remains verifiable because the complete evidence source commit and tree are retained in Git history.
- Infrastructure, timeout, flaky, unresolved, interrupted, and cleanup-failed outcomes remain non-passing and are absent from this successful release inventory.

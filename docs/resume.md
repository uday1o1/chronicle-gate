# Evidence-backed resume bullets

The bullets below are bounded to the checked-in synthetic workload, locked local Docker environment, and source-authenticated public evidence.

- Built a Go release-qualification CLI that reproduced the checked-in signatures for R1 through R7 stateful-service defects while every paired nearby control completed with `PASS`.
<!-- measured: evidence/results/r1-offset-rewind.json#/outcome/classification -->
<!-- measured: evidence/results/r1-single-delivery-control.json#/outcome/classification -->
<!-- measured: evidence/results/r2-crash-after-effect.json#/outcome/classification -->
<!-- measured: evidence/results/r2-manual-commit-control.json#/outcome/classification -->
<!-- measured: evidence/results/r3-stale-aggregate.json#/outcome/classification -->
<!-- measured: evidence/results/r3-monotonic-control.json#/outcome/classification -->
<!-- measured: evidence/results/r4-schema-default-drift.json#/outcome/classification -->
<!-- measured: evidence/results/r4-defaulting-control.json#/outcome/classification -->
<!-- measured: evidence/results/r5-inventory-first.json#/outcome/classification -->
<!-- measured: evidence/results/r5-payment-first-control.json#/outcome/classification -->
<!-- measured: evidence/results/r6-late-cancellation.json#/outcome/classification -->
<!-- measured: evidence/results/r6-on-time-control.json#/outcome/classification -->
<!-- measured: evidence/results/r7-outbox-crash.json#/outcome/classification -->
<!-- measured: evidence/results/r7-unrelated-orders-control.json#/outcome/classification -->

- Implemented signature-preserving reduction that shrank the R1 scenario from two events and seven actions to one event and six actions across eight fresh-pair trials, with relative one-minimality proven.
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/originalEvents -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/finalEvents -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/originalActions -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/finalActions -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/trials -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/minimality -->

- Built an isolated paired open-loop benchmark gate whose A/A control passed and whose seeded slowdown produced a 25,425,229 ns mean paired p95 increase with a 3.682243 lower relative confidence bound.
<!-- measured: evidence/results/benchmark.json#/comparisons/0/outcome/classification -->
<!-- measured: evidence/results/benchmark.json#/comparisons/0/outcome/regression -->
<!-- measured: evidence/results/benchmark.json#/comparisons/1/outcome/classification -->
<!-- measured: evidence/results/benchmark.json#/comparisons/1/outcome/regression -->
<!-- measured: evidence/results/benchmark.json#/comparisons/1/outcome/meanAbsoluteP95DeltaNanos -->
<!-- measured: evidence/results/benchmark.json#/comparisons/1/outcome/lowerRelativeCI -->

# R1 verified demonstration

This short transcript and its [asciinema v2 cast](r1.cast) are deterministic renderings of the source-authenticated R1 regression and nearby control captures.
The displayed argument vectors, exit codes, classifications, signature, confirmations, and reduction facts are checked against the public JSON evidence during every release verification.

<!-- demo-output:start -->
```text
ChronicleGate R1 verified demo
$ bin/chronicle run --scenario examples/order-lifecycle/scenarios/r1-offset-rewind-noisy.yaml --baseline examples/order-lifecycle/targets/generated/baseline.yaml --candidate examples/order-lifecycle/targets/generated/candidate.yaml --out run/release-evidence/r1-offset-rewind/raw --development-local-images --json
exit: 2
classification: SEMANTIC_REGRESSION
signature: 344a2c61a869abdfc6a272ea4a35eb247158d41b619049cb8b6618ba71b6b29c
confirmations: 2
reduction: events 2 -> 1, actions 7 -> 6, trials 8, minimality proven
$ bin/chronicle run --scenario examples/order-lifecycle/scenarios/r1-single-delivery-control.yaml --baseline examples/order-lifecycle/targets/generated/baseline.yaml --candidate examples/order-lifecycle/targets/generated/candidate.yaml --out run/release-evidence/r1-single-delivery-control/raw --development-local-images --json --no-minimize
exit: 0
classification: PASS
artifacts: checksummed reports, authoritative journal, verified regression bundle
boundary: trusted synthetic workload, locked local Docker environment, development-local images
```
<!-- demo-output:end -->

<!-- measured: evidence/results/r1-offset-rewind.json#/source/argv -->
<!-- measured: evidence/results/r1-offset-rewind.json#/outcome/exitCode -->
<!-- measured: evidence/results/r1-offset-rewind.json#/outcome/classification -->
<!-- measured: evidence/results/r1-offset-rewind.json#/outcome/failureSignature/digest -->
<!-- measured: evidence/results/r1-offset-rewind.json#/outcome/confirmations -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/originalEvents -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/finalEvents -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/originalActions -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/finalActions -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/trials -->
<!-- measured: evidence/results/r1-offset-rewind.json#/reduction/minimality -->
<!-- measured: evidence/results/r1-single-delivery-control.json#/source/argv -->
<!-- measured: evidence/results/r1-single-delivery-control.json#/outcome/exitCode -->
<!-- measured: evidence/results/r1-single-delivery-control.json#/outcome/classification -->

The cast does not include raw service logs, tokens, database contents, attempt identifiers, or embedded images.
Follow the [clean-checkout reproduction guide](../docs/reproduction.md) to generate a fresh local run and replay bundle.

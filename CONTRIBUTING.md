# Contributing

ChronicleGate accepts focused changes that preserve the contracts in [BUILD_PLAN.md](BUILD_PLAN.md).

Before submitting a change, run:

```sh
make fmt
make verify
```

Tests should exercise the closest public CLI or documented workflow that owns the changed behavior.
Do not weaken expected outputs to hide a regression.
Do not commit credentials, generated run data, model artifacts, container state, profiler output, or restricted datasets.
Third-party actions and container images must use immutable references where the build plan requires them.

Use Conventional Commits for commit subjects.
Keep commits focused on one verified change.

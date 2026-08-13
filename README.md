# ChronicleGate

ChronicleGate is a local release-qualification framework for instrumented Kafka-style stateful consumers.
It is being implemented milestone by milestone according to [BUILD_PLAN.md](BUILD_PLAN.md).

Milestone 0 is complete: reproducible bootstrap, immutable image locks, and environment diagnostics pass their local acceptance gate.
No semantic replay or release-qualification claim is made until its corresponding acceptance gate passes.

## Bootstrap

Docker is the only required local toolchain dependency.
The Makefile uses immutable Go and golangci-lint container references when a native Go installation is unavailable.

```sh
make build
./bin/chronicle version
./bin/chronicle doctor --json
make verify
```

`doctor` checks host workspace capacity and loopback port allocation, then verifies the Docker server and every locked OCI image index.
The disk and loopback checks describe host-visible resources only.

## Status

The native macOS ARM64 binary and cross-built Linux ARM64 and AMD64 binaries execute successfully.
The checked-in Redpanda, PostgreSQL, Go bootstrap, and golangci-lint OCI indexes contain the exact locked Linux ARM64 and AMD64 child manifests.

## License

ChronicleGate is licensed under Apache-2.0.

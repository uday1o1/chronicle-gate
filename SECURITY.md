# Security Policy

## Supported versions

ChronicleGate is under active V1 development.
Security fixes are applied to the default branch until the first stable release defines a broader support policy.

## Reporting a vulnerability

Please use GitHub private vulnerability reporting for this repository.
Do not include credentials, production payloads, customer data, or exploit details in a public issue.

## Trust boundary

Docker socket access grants host-level authority.
ChronicleGate is intended for trusted local scenarios and trusted images only.
Evaluate unknown third-party images in an isolated disposable virtual machine.

Authored scenarios, observer SQL, target images, the Docker daemon, the host kernel, the local run filesystem, and the exact pinned infrastructure images are trusted components in V1.
Container restrictions are defense in depth and are not a sandbox for hostile code.
Docker networking can provide outbound NAT, and ChronicleGate does not claim host-level egress isolation.

Target containers receive a non-root identity, read-only root filesystem, dropped capabilities, `no-new-privileges`, bounded CPUs, memory, and PIDs, loopback-only host ports, and one private read-only secret mount.
Infrastructure capability allowlists are locked by image digest and architecture in `config/images.lock.json`.
No workload container receives the Docker socket.

Reproduction archives are verified before extraction or image loading.
The verifier rejects traversal aliases, symlinks, device files, duplicate and case-colliding paths, unsafe Unicode and control names, excessive file counts, excessive expansion, and excessive compression ratios.

See [`docs/security-model.md`](docs/security-model.md) for the complete threat model, control inventory, archive limits, interruption behavior, and residual risks.

The Milestone 0 `doctor` command performs read-only Docker inspection and remote manifest resolution.
It does not start workload containers.

## Dependency and workflow policy

Every GitHub Action reference is pinned to a full immutable commit SHA.
Pull requests run the pinned GitHub dependency-review action separately from the pinned `govulncheck` reachable-code scan.
`make security-check` validates the Go dependency lock in both directions, workflow pins and triggers, image pins and capability policies, and credential-shaped repository content.

## Data handling

Use synthetic data only.
Do not place production credentials, customer payloads, personal data, resolved secret values, raw private traces, or database exports in scenarios, evidence, bundles, or issues.
Run directories are private mode `0700`, and artifact and token files are mode `0600` on supported filesystems.

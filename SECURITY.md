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

The Milestone 0 `doctor` command performs read-only Docker inspection and remote manifest resolution.
It does not start workload containers.

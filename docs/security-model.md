# Security model

ChronicleGate qualifies trusted local stateful services against trusted authored scenarios.
It is not a hostile-container sandbox.

## Trust boundaries

The V1 trust boundary includes the authored scenario, observer SQL and schemas, baseline and candidate images, Docker daemon, pinned Redpanda and PostgreSQL images, host kernel, and local run filesystem.
An unknown or third-party image must run inside an ephemeral virtual machine with separate credentials and no sensitive host mounts.
Docker containers share the daemon's host kernel, and container restrictions do not remove that boundary.
Docker networking can provide outbound NAT, so the V1 implementation does not claim general egress denial.

The Docker socket grants host-level authority to the ChronicleGate CLI.
No target or infrastructure container receives the Docker socket.
Cleanup uses only exact run, attempt, container, network, topic, database, and recorded volume handles.
ChronicleGate never performs a global container, network, volume, topic, or database cleanup.

## Container controls

Every target service starts with these controls and is inspected after every initial start and restart.

- The process uses a fixed or invoking non-root UID and GID.
- The root filesystem is read-only.
- Linux capabilities are dropped with `ALL` and none are added.
- `no-new-privileges` is set.
- CPU, memory, and PID limits come from the validated target contract.
- Every host port binds only to `127.0.0.1`.
- The only mount is the runtime-owned private secret directory, mounted read-only.
- Docker socket mounts are rejected.

The infrastructure policy is tied to the exact OCI index and ARM64 or AMD64 child digest in `config/images.lock.json`.
Redpanda drops all capabilities and adds only `DAC_OVERRIDE`, which the pinned Testcontainers module needs because it starts its injected entrypoint as root while accessing files owned by the image's `redpanda` user.
PostgreSQL drops all capabilities and adds only `CHOWN`, `DAC_OVERRIDE`, `FOWNER`, `SETGID`, and `SETUID` for its pinned root-to-postgres initialization path.
Both infrastructure containers have explicit CPU, memory, and PID limits, `no-new-privileges`, loopback-only published ports, and no Docker socket mount.
The same capability policy is locked for Linux ARM64 and AMD64 and cannot broaden automatically when one platform fails.

PostgreSQL data and Redpanda state must remain writable, so their image-defined writable data paths are not treated as read-only target roots.
This is a documented infrastructure exception, not a claim that the entire infrastructure filesystem is immutable.

## Secret and observer controls

Runtime credentials are random per run and are written to a private `0700` directory as `0600` files.
The invoking non-root UID and GID are used so bind-mounted files remain readable without world-readable permissions.
Targets receive only the secret files required by their declared endpoint privileges.
Artifacts retain secret reference names and resolution status, never resolved values or the process environment.
Every public renderer and reproduction bundle passes the same secret-value rejection check before publication.

SQL observers use the dedicated `chronicle_observer` role with read-only transactions and a statement timeout.
Live role tests prove that data writes and DDL fail.
HTTP and effect observers use separate bounded observer credentials that cannot append effects.
The service-side effect credential cannot read or reset the observer ledger.

## Reproduction archive policy

Archive creation requires the destination to be absent and publishes through an atomic same-filesystem hard link.
It never overwrites an existing artifact.
The private temporary ZIP is mode `0600`, is synced before publication, and its parent directory is synced after publication or cancellation cleanup.
Cancellation cleanup compares the retained temporary inode with the destination before deleting the published name.

Creation retains at most 256 MiB of input data in memory and streams compressed ZIP output directly to disk.
Outer bundle verification permits at most 1,000 manifested files plus `bundle.json`, 512 MiB per entry, and 1 GiB total expanded content.
Entries of at least 1 MiB may not exceed a 200-to-1 expansion ratio.
Embedded local-image archives permit at most 512 MiB of tar content and 256 regular members.
Each expanded image layer is limited to 512 MiB and the same 200-to-1 ratio.

Creation, opening, extraction, replay staging, and embedded-image verification share normalized relative path rules.
Absolute paths, dot components, `..`, repeated separators, backslashes, colons, invalid UTF-8, control characters, duplicates, case-fold collisions, symlinks, device files, encrypted entries, unreferenced image members, and malformed JSON metadata are rejected.
Actual decompressed byte counts must equal both archive headers and signed manifest records.
The verified archive file remains open through extraction to prevent a verify-replace-extract race.

## Interruption and partial evidence

The first terminal context cause is authoritative.
An internal maximum-run deadline remains `TIMEOUT` if it occurs before an external signal.
An external `SIGINT` or `SIGTERM` remains `INTERRUPTED` if it occurs first.
A cleanup failure overrides either as `INFRASTRUCTURE_ERROR` with exit code `4`.
An interrupted run with successful cleanup exits `130`.
A second signal restores the operating system's default behavior so an operator can force termination.

Cleanup uses a bounded independent context and retains failed handles for an exact retry.
Topics and attempt databases are deleted and verified while their broker and database remain reachable.
Container-owned volume names are recorded before teardown, removed by exact name, and verified absent before the network is removed.
The append-only journal records cleanup and the final effective state.
An interrupted journal never contains a `COMPLETE` terminal record.

## Supply-chain gates

`config/dependencies.lock.json` records every direct Go module, selected reviewed transitive modules, the Go toolchain, and pinned analysis tools.
The repository checker rejects missing direct modules, stale locked modules, duplicate entries, wrong versions, and direct-versus-indirect kind mismatches.
`go mod tidy` must leave both module files unchanged.

The normal CI workflow runs the common format, unit, property, `go vet`, race, repository security, `govulncheck`, bounded fuzz, and build gate on Ubuntu and macOS.
The pinned Docker golangci-lint image provides the broader lint and staticcheck gate.
Trusted Docker integration runs on a GitHub-hosted Ubuntu runner and never on a privileged self-hosted pull-request runner.
The pinned dependency-review action runs only for pull requests.

## Verified Milestone 8 evidence

The complete local `make verify` gate passed on 2026-08-13 in 1,768 seconds on the development host.
The gate included formatting, unit and property tests, `go vet`, race tests, the repository security policy, `govulncheck`, six bounded fuzz targets, native and cross-platform builds, pinned golangci-lint, and the complete Docker integration suite.
`govulncheck` found no reachable vulnerabilities and no vulnerabilities in imported packages.
It reported three advisories in required modules whose vulnerable symbols are not called.

All eleven live integrations passed in that gate.
The abrupt-interruption test delivered `SIGINT`, observed exit code `130`, preserved an `INTERRUPTED` journal without a `COMPLETE` record, and verified exact cleanup in 26.33 seconds.
The R4 schema and observer run plus offline bundle replay passed in 321.46 seconds.
The controlled R3, R5, and R6 corpus plus offline replay passed in 495.95 seconds.
The R7 connected-outbox fault and replay passed in 158.27 seconds, and its nearby control matrix passed in 185.01 seconds.

The archive unit and fuzz corpus rejects traversal variants, unsafe aliases, duplicate and case-colliding paths, malformed local-image metadata, direct expansion bombs, creation-side expansion bombs, and oversized image members.
The cancellation tests prove that a published bundle is removed only when it is the exact retained temporary inode and that an existing destination is never overwritten.

## Residual risks

Docker daemon compromise, malicious trusted inputs, a compromised pinned image, host-kernel vulnerabilities, and unrestricted host or container outbound NAT remain outside the V1 containment claim.
Archive size limits reduce denial-of-service risk but do not make arbitrary archives trustworthy beyond the implemented verifier.
Local image-ID bundles are development-only and are not portable registry identities.
Publication should use named OCI digest references and an isolated clean environment.

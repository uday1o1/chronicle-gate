# Portfolio-ready core evidence

ChronicleGate reached the portfolio-ready core checkpoint defined by `BUILD_PLAN.md` at Milestone 4.
This checkpoint covers working public-CLI paths for R1 and R2, stable confirmation and reduction, safe offline bundle replay, and an independently verified manual synchronous offset commit.

## Demonstrated behavior

| Qualification | Public outcome | Acceptance evidence |
| --- | --- | --- |
| R1 offset redelivery | `SEMANTIC_REGRESSION`, exit `2` | Baseline and candidate each consume the same topic, partition, and offset twice; the candidate produces the checked-in duplicate-reservation signature in three matching semantic attempts. |
| Noisy R1 | `SEMANTIC_REGRESSION`, exit `2` | The optional diagnostic closure is removed, the exact signature is preserved, and relative one-minimality is proven. |
| R1 offline replay | `SEMANTIC_REGRESSION`, exit `2` | Source local images are deleted before the verified bundle restores exact embedded identities and reproduces the signature. |
| R2 crash after effect | `EXTERNAL_EFFECT_REGRESSION`, exit `2` | The committed offset remains `0` at `after_external_effect`; `SIGKILL` and restart redeliver the same physical record; the candidate records two canonical effects and all three candidate attempts match the checked-in signature. |
| R2 offline replay | `EXTERNAL_EFFECT_REGRESSION`, exit `2` | Baseline, candidate, and sink images are absent before replay; verified embedded archives restore them and reproduce the exact R2 signature. |
| Manual commit control | `PASS`, exit `0` | The position is `0` while blocked at `before_offset_commit` and `1` before `after_offset_commit` is exposed, followed by the full stability-window proof. |
| Flaky R1 control | `FLAKY`, exit `5` | Completed semantic outcomes disagree, so no minimization or bundle is produced. |
| Missing-image control | `INFRASTRUCTURE_ERROR`, exit `4` | A nonexistent exact image identity remains an infrastructure failure and exact-scope cleanup leaves no run resources. |

## Measured local integration evidence

The complete repository Docker integration command passed on 2026-08-13.
These timings describe that acceptance run and are not product performance claims.

| Test | Elapsed time |
| --- | ---: |
| R1 public CLI | 31.41 s |
| Noisy R1 minimization and offline replay | 91.09 s |
| R2 public CLI and offline replay | 131.28 s |
| Manual synchronous commit control | 28.37 s |
| Flaky candidate control | 30.57 s |
| Exact-scope failure cleanup | 3.16 s |
| Complete `make test-integration` command, including cached image and test setup | 319.8 s |

## Reproduction

Docker is the only required host toolchain dependency.
The repository pins its Go, lint, Redpanda, and PostgreSQL container inputs.

```sh
make reference-images
make build
make test-integration
```

For a direct demonstration, run the R1 and R2 commands from the root README, inspect their generated HTML reports, then run the manual-commit control.
Reproduction bundles are deliberately nonportable when development-local image IDs are used, but they embed and verify the exact local images needed for offline replay on the same platform.

## Claim boundary

The evidence supports deterministic local qualification of the implemented R1 and R2 reference defects under the locked environment and documented resource assumptions.
Milestone 5 now completes the V1 observer and schema-default model beyond this checkpoint.
This checkpoint document still makes only the narrower M4 claim measured above.
The remaining cross-stream, outbox, complete corpus, performance, release, and hostile third-party image isolation work is governed by Milestones 6 through 10.

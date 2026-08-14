# Portfolio-ready core evidence

ChronicleGate reached the portfolio-ready core checkpoint defined by `BUILD_PLAN.md` at Milestone 4.
This checkpoint covers working public-CLI paths for R1 and R2, stable confirmation and reduction, safe offline bundle replay, and an independently verified manual synchronous offset commit.

## Demonstrated behavior

| Qualification | Public outcome | Acceptance evidence |
| --- | --- | --- |
| R1 offset redelivery | `SEMANTIC_REGRESSION`, exit `2` | Baseline and candidate each consume the same topic, partition, and offset on the original delivery and redelivery; the candidate produces the checked-in duplicate-reservation signature after stable confirmation. |
| Noisy R1 | `SEMANTIC_REGRESSION`, exit `2` | The optional diagnostic closure is removed, the exact signature is preserved, and relative one-minimality is proven. |
| R1 offline replay | `SEMANTIC_REGRESSION`, exit `2` | Source local images are deleted before the verified bundle restores exact embedded identities and reproduces the signature. |
| R2 crash after effect | `EXTERNAL_EFFECT_REGRESSION`, exit `2` | The committed offset remains at the original position at `after_external_effect`; `SIGKILL` and restart redeliver the same physical record; the candidate duplicates the canonical effect after stable confirmation. |
| R2 offline replay | `EXTERNAL_EFFECT_REGRESSION`, exit `2` | Baseline, candidate, and sink images are absent before replay; verified embedded archives restore them and reproduce the exact R2 signature. |
| Manual commit control | `PASS`, exit `0` | The position is `0` while blocked at `before_offset_commit` and `1` before `after_offset_commit` is exposed, followed by the full stability-window proof. |
| Flaky R1 control | `FLAKY`, exit `5` | Completed semantic outcomes disagree, so no minimization or bundle is produced. |
| Missing-image control | `INFRASTRUCTURE_ERROR`, exit `4` | A nonexistent exact image identity remains an infrastructure failure and exact-scope cleanup leaves no run resources. |

## Public release evidence

The source-authenticated R1 and R2 release records are published in [`evidence/results`](../evidence/results).
They retain exact classifications, signatures, confirmation counts, bounded observation summaries, artifact digests, and source provenance without committing raw runs or embedded images.
The publisher emits a record only after its private capture proves exact cleanup with no retained attempt resources.

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
This checkpoint document intentionally retains the narrower Milestone 4 boundary even though extended V1 is complete through Milestone 10.

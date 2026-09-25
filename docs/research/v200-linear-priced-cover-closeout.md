# V200 exact linear priced-cover fast-path closeout

## Decision and authority

**Keep the generic two-state Rust fast path and implement a capped fallback
for the remaining queries.** On the complete, already-used ReLAION-1M D768
validation-1000 GT-blind feature/plan panel, 861/1,000 unconstrained priced
solutions satisfied the same 672-unit and 32-GET caps. Rust reproduced
**all 861** V198 physical interval lists, modeled mass, unit counts and GET
counts exactly. The remaining 139 were correctly rejected by this fast
path; V200 does not replace their exact capped plans.

The Causality Spot `c7i.12xlarge` attempt `a0003` used source commit
`b573a2fcf6300d2d37acd400521d9a1a504f8b61`, source archive SHA-256
`71ca1aca41584b76acde0a240361866cbaf5f5e48c9b6c7a766ae92a74b88693`,
and instance `i-00aedf7afc2fc51c0`. The complete terminal SHA-256 is
`1520355b548e1f1d9322a34a19b8cc44ee70efb54f3964c1f8dd2a1e42cd2429`
at `s3://borsuk-bench-453182569524-euc1/research/v200-uncapped-cover/b573a2fcf6300d2d37acd400521d9a1a504f8b61/runs/a0003/terminal.json`.
The launcher streamed back and hashed every artifact, verified the 861 exact
Rust witnesses, and terminated the instance. The checked-in
`scripts/check_v200_uncapped_cover.py` independently regenerated the
GT-blind model weights from the V192 fit and V198 features, then replayed
all 1,000 covers in Python and compared every admitted interval witness.
It passed. The weight file SHA-256 was
`797a83a7d830afd5cec491e70de3f49023a52c2f972af2045704b68df6f76313`.

## Measured planner-core result

| Rust linear-cover group | Queries | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: |
| All unconstrained solves | 1,000 | 45.886 µs | 55.303 µs | 60.988 µs |
| Cap-admitted exact witnesses | 861 | 44.889 µs | **54.026 µs** | 60.572 µs |

Of the 139 fallback queries, 135 unconstrained plans exceeded the unit cap
and 41 exceeded the GET cap; the counts overlap. The benchmark process
wall time was 0.09 s and peak RSS **20,971,520 bytes**. The Rust timings
exclude JSON parsing, prediction-weight generation, router/features and
live S3. Python 3.12 weight generation from the sealed model took 2.56 s
for 1,000 queries, with 116,488 KiB peak RSS; it too needs a production
Rust path. V198's full Python planner phase took 74.55 ms/query averaged,
but it includes weight generation and the capped NumPy DP for every query,
so this is not a paired 1,000-query latency speedup claim.

`scripts/test_unconstrained_priced_interval.py` exhaustively checked small
weighted/mandatory geometries against all physical covers and passed.
`formal/PredictedIntervalGuarantees.lean` compiled and proves that an
unconstrained optimum remains optimal once its witness satisfies the caps,
plus a conditional linear state-work/time implication. The theorem assumes
the executable finds the unconstrained optimum; exhaustive tests and exact
V198 witnesses are separate implementation evidence, not a proof of all
inputs. None of these facts proves actual unseen-query recall or a fixed
wall-time bound without further premises and measurements.

Attempts `a0001` and `a0002` are preserved negative engineering evidence:
the first used the AMI's too-old default Python for `zip(strict=True)`;
the second sealed the correct weights but hit two Rust benchmark `usize +=
bool` compile errors. Neither produced timing or parity measurements.
Both Spot instances were terminated. No architecture or model parameters
were changed to obtain `a0003`.

## Next qualification

Implement an exact capped solver for the remaining 139. A generic hierarchy
can try a unit-constrained or GET-constrained relaxation and accept its
optimum only if it satisfies the other cap; otherwise use the two-dimensional
hard-cap solver. Prove the relaxation-admission implication and compare
every emitted interval witness against the frozen V198 reference before
including planner time in a concurrent live end-to-end gate. Then port
GT-blind model-weight calculation and feature construction into the
production serving path, qualify on a distinct real-query embedding dataset,
and continue 10M/100M scale work with RAM sized by recall tier and live
generation count.

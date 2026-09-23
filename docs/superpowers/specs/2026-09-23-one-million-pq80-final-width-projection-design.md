# One-million PQ80 final width projection

## One fixed decision

The completed PQ96 locality projection at source
`1a1d2e7380c4d6635a2b370784ea009aa3af0851` reached 98.151% mean
GT100 and 99.28% GT10 but 89% p05 GT100: 51 of 1,000 queries remained
below 90 GT100 hits, two more than the fixed p05 gate permits. Its
16-MiB code-wave cap was saturated. Test one final narrower format before
stopping width-only projections: 80 PQ code bytes per row, representing
80 contiguous rotated-coordinate subspaces, with the first 48 subspaces
10D and the last 32 subspaces 9D (48×10 + 32×9 = 768). This screen
projects locality only; it does not train or score PQ80 codes.

## Frozen source, routing and projection

Rebuild the identical seven-input source-only page-centroid seal and
require these prior terminal-closed SHA-256 values before downloading any
query/truth object: page centroids
`757fbe7c6b2112ea5904a5bba0e26fb4ac929cc39ca1fc470b12dee35b61a704`,
membership
`55e36613b293ffc08c11b9da8c2f2293cf89e43310551f316f2c5b57a84e0d13`,
and physical page order
`bb8ebb3642de174a338621f08d4a739b265d91914eb25ebe41831ffed0f33b4b`.
Keep 910 eight-page groups, page-centroid squared-L2 scores, stable
`(score, role, group ordinal)` ties, complete one-pass ranking,
role-separated adjacent intervals and the previous 32-GET/16,777,216-byte
range planner unchanged. Change only the projected group payload length to
`4 + 4 * member page count + 80 * live rows`. Source IDs would come from
sealed page-row order; a future PQ80 book would be resident and separately
authenticated. Do not test any other row width, codebook count, group
layout or routing score on this cohort.

Record 1,000 ordered group plans, intervals, projected GETs/bytes and
GT10/GT100 containment. The independent validator must recompute the
80-byte lengths, source seal, scores, admissions and aggregate metrics
without calling the producer planner. Results state `row_bytes=80` and
`claim_eligible=false`.

## Stop gate

Run one immutable Causality Spot attempt with source/query separation,
networkless unprivileged evaluation, terminal on worker/controller failure,
immediate instance termination, artifact readback, <3-GiB per-phase RSS
including a 64-MiB allowance and zero swap. Read only terminal and
infrastructure health while incomplete.

Advance only if mean GT100 ≥97.5%, p05 GT100 ≥90%, GT10 ≥96%, at most
32 projected GETs and at most 16,777,216 projected code bytes per query.
A miss is `pq80-locality-projection-killed`; stop width-only projections
and change routing or group layout. A pass is
`pq80-locality-projection-feasible` and authorizes **only** a real paired
100k PQ80 code-quality/read/resource test against the successful 200-byte
rotated two-bit baseline on identical source groups. Train PQ80 solely
from the 100k source after source-only construction, use the fixed signed
Hadamard rotation seed `20260923` before the 48×10/32×9 partition, and
preregister its scoring and training method before that actual test.
No 1M code plane or product serving claim follows from this projection.

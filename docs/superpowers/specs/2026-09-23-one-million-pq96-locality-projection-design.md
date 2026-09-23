# One-million PQ96 row-width locality projection

## Decision

The terminal-closed 1M adjacent-range screen at source
`1626e78507dbc6531e9e9ff3b51ec79459f26564` reached 97.541% mean
GT100 and 98.96% GT10 but only 86% p05 GT100 at 200 bytes per row and
an essentially full 16-MiB code wave. Its score-ranked byte-only diagnostic
also had 86% p05. Test one materially narrower row-format hypothesis:
a 96-byte PQ96 code, with no per-row scale, norm or ID in the code payload.
This screen projects candidate locality only; it does not train PQ96 or
measure PQ96 ranking quality.

## Frozen projection

Authenticate and rebuild the same seven-input ReLAION-1M source-only page
centroids, physical page membership and 910 eight-page groups. Require the
previous completed screen's centroid, membership and page-order SHA-256
values before query/truth download:
`757fbe7c6b2112ea5904a5bba0e26fb4ac929cc39ca1fc470b12dee35b61a704`,
`55e36613b293ffc08c11b9da8c2f2293cf89e43310551f316f2c5b57a84e0d13`,
and `bb8ebb3642de174a338621f08d4a739b265d91914eb25ebe41831ffed0f33b4b`.
Keep all page scores, `(score, role, group ordinal)` ties, one-pass group
admission, role-separated adjacent intervals, 32-GET cap and
16,777,216-byte cap exactly as in the prior range screen. Change only each
group's projected payload length to
`4 + 4 * member page count + 96 * live rows`. The 96 bytes are one PQ code
byte per 8D subspace for 768D rows. Codebooks would be resident and
separately authenticated in a future real format; the page-row order supplies
source IDs, so the code payload needs no per-row ID. Do not test other widths,
group sizes, scores, layouts or range policies on this cohort.

Record all 1,000 ordered admitted groups, final intervals, projected GETs,
bytes, GT10/GT100 containment and exact p05. The independent validator must
rebuild source artifacts and recompute the 96-byte lengths, scores,
admissions, intervals and samples without the producer planner. The output
must state `row_bytes=96` and `claim_eligible=false`.

## Gate and follow-up

Run one immutable Causality Spot attempt with source/query phase separation,
unprivileged networkless evaluation, terminal and controller failure
recovery, immediate instance termination, all-artifact readback, <3-GiB
per-phase RSS including 64-MiB wrapper allowance and zero swap. Incomplete
campaigns may be observed through terminal and infrastructure health only.

The fixed locality gate is mean GT100 ≥97.5%, p05 GT100 ≥90%, GT10 ≥96%,
at most 32 projected code-range GETs and at most 16,777,216 projected bytes
per query. A miss is `pq96-locality-projection-killed`; no PQ96 code needs
to be built for this routing plan. A pass is
`pq96-locality-projection-feasible` and authorizes **only** a real source-only
PQ96 training/encoding and paired 100k quality/read/resource gate against
the winning 200-byte rotated two-bit arm on identical candidate groups.
The actual PQ96 row-score gate must reach the same 97.5% mean/90% p05/96%
GT10 thresholds before any 1M PQ96 code-plane build. Actual S3 reads,
native final-page serving, fresh holdout, two-generation 100M memory and
matched product performance remain separate requirements.

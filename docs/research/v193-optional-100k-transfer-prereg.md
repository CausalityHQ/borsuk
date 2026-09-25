# V193 optional utility cross-scale 100k transfer: preregistration

## Decision and evidence scope

Test whether the V192 ReLAION-1M fit-trained optional utility transfers
without refitting to the **already used** ReLAION-100k D768
development-1000 real queries. This is the cheapest paired
cross-scale rejection gate. It is not a fresh holdout or a production
default. Its strongest matched BORSUK comparator is V170's direct
PQ64 32-row field on these same requests: **99,357/100,000 SQ8
returned GT100 hits**, p05 98, 99,747 GT100 rows in fetched ranges,
14,785,704,960 planned bytes and 13,888 GETs. V163's older
same-layout returned baseline was 99,322 hits at
15,657,408,000 bytes and 15,275 GETs.

## Frozen method and inputs

Use the V192 completed source commit
`a22a0d7c6f9cfc71f627bdafa85875750c2048f9` and complete
`a0001` result SHA-256
`b79683695350b4bc21eb4cad14f3588ed5ebaef088a1dd9443b3cc429ef62a01`.
Freeze the optional model trained on V189 fit ordinals 2432–2495,
price `(unit=1000, GET=50000)`, and the comparable full-rank curve
from the same 64 model-fit queries at its own V192 price
`(2000, 50000)`. A constant-risk ablation uses the optional rank
curve and the first-64 mean candidate-optional truth, with the
optional price. No ReLAION-100k labels fit a model, price, threshold,
candidate radius or exception.

Authenticate V113 PQ64 books/codes/source IDs, V163 order and
contiguous SQ8 object, V170's GT-blind plans and its completed
terminal, and V114 request/reference/manifest identities exactly as
the V170 transfer checker did. The V170 `pq_ranges` and per-query
`pq_bytes`/`pq_gets` form the paired cap. Only after all V193
features, model identity, interval plans and seal are durably written
to distinct S3 keys may the worker download ReLAION-100k GT100 and
V170 returned rows.

For each frozen request, preserve V114's 512 PQ nominees and 100 SQ8
primary rows. Map them through V163's authenticated physical order.
Compute V168 reconstructed-cosine PQ64 scores on each candidate
32-row unit within **radius 32 physical units** of a nominee unit,
as in V189. Rank units by minimum PQ row score, tie breaking by unit
ordinal. Force every primary unit. For each V193 arm, run the exact
hard-capped priced interval reference with
`page_count=3125`,
`max_units=min(672, V170 pq_bytes / 24960)`, and
`max_gets=min(32, V170 pq_gets)`. Use a caller trace allowance of
512 MiB. The units and GET caps are per query; each V193 plan must
also stay at or below the paired V170 planned bytes and GETs.
Any mandatory-infeasible or trace-budget failure is an explicit
failed query, never a zero-recall result.

The four reported arms are optional-risk, refitted full-rank,
constant-risk and frozen V170 direct PQ control. V170 is not
recomputed or retuned. Score the three V193 fetched SQ8 ranges with
the same authenticated V163 quantizer and deterministic
`score_sq8_ranges(..., top_k=100)` used by V170. Count both exact
GT100 IDs inside fetched ranges and returned top-100 intersections.
Report per-query candidate omissions, mandatory floors,
interval witnesses, returned hit lists, planned bytes/GETs,
offline CPU/wall/RSS and any infeasibility.

## Decision gate

The optional-risk arm advances to a fresh 1M source gate only if all
1,000 plans are feasible, no per-query V170 byte/GET cap is exceeded,
its aggregate planned bytes and GETs are no higher than V170's, its
SQ8 returned hits are at least **99,357/100,000**, its p05 is at
least **98**, and its fetched GT100 coverage is at least **99,747**.
Report paired wins/ties/losses against V170 and the full-rank arm.
An equal-quality, lower-resource result is a transfer-screen pass;
this used cohort supports no new-query significance claim.

If the gate fails, separate candidate omission, mandatory
infeasibility, optional allocation, SQ8 quantization and return
ranking losses before changing the responsible layer. A pass leads
to a new ReLAION-1M source holdout of at least 512 queries with an
uncertainty interval for the tail and then live S3 returned quality,
latency, charged RAM and cost, plus a distinct dataset transfer.

Run one immutable source archive on Causality Spot. Seal GT-blind
artifacts before opening truth, upload terminal artifacts even on
failure, discard an interrupted cell and restart under a new
attempt ID, replay the completed terminal and terminate the worker
immediately. Keep heavy execution off the swap-pressured devbox.

# 100k source-trained OPQ8 row router

## Decision and boundary

The terminal-closed 1M width, geometric-order and diagonal-dispersion
screens all missed the fixed p05 containment gate. Stop modifying those
scores on the same development cohort. Test one material replacement for
page-summary routing: an **8-byte source-trained row code** resident in the
router, used only to rank the existing four-page code groups. Keep the
successful 200-byte rotated two-bit code and its exact group contents for
scoring fetched rows. This is an OPQ8 routing hypothesis, not a row-score
format change or a claim of production throughput.

Use the frozen 100k source, physical membership, 166 pages, four-page
groups, 1,000 development queries and GT100 in
`scripts/native_page_microcluster_cell.py:FROZEN_INPUTS`. Compare against
the terminal-closed rotated two-bit confirmation at
`80ddf40533aefd3c24b7d3ea4539887aa94f91bb`. No query or truth object
may be read before source-only model, codes, physical row ownership and
training recipe are sealed. Production defaults are not frozen by this
screen.

## One fixed source-only model

Compute the global 768D source mean in source-ordinal order using float64,
and store it as float32. Select 8,192 training rows by smallest SHA-256 of
`b"borsuk-route-opq8-v1" || LE32(20260923) || LE64(source_ordinal)`, ties
by ordinal. Center source rows on the stored float32 mean. Train one full
768×768 orthogonal transform and eight consecutive 96D subspaces with
256 8-bit centroids each. Start transform at identity. Initialize each
subspace by farthest-first selection from the lowest-hash training row,
breaking distance ties by source ordinal. Run exactly eight outer steps:
four Lloyd assignment/mean updates, then orthogonal Procrustes minimizing
`||X R - reconstruction||_F`; keep empty centers. Warm-start books across
outer steps. Pin a single-threaded float64 training implementation, seed,
tie order and SVD driver. Store final transform as float32; run four final
Lloyd updates against that stored transform, then store centers as float32.
Encode all source rows against that exact stored model, center ties by index.
Seal mean, transform, books, exactly eight bytes per row in physical
membership order, source/membership identities, page/group boundaries and
training recipe under a new incompatible format marker. No resident per-row
ID, scale or norm is permitted. The route's source records remain separate
from immutable object-storage two-bit row codes.

For query `q`, calculate `(q-mean)R`, construct eight 256-entry float64
squared-distance lookup tables and score each row as the sum of its eight
lookups, rounded once to float32. For each group, retain four smallest row
distances, ties by physical row ordinal. Rank groups by
`(mean(top four), minimum row distance, role, original group ordinal)`.
Do not blend with the old tree, normalize by bytes, change top-four,
change seed or sweep code width after seeing the cohort.

## Staged 100k gate

Run heavy training and evaluation on one immutable Causality Spot attempt,
not on the swap-constrained devbox. Use create-only source/archive/artifact
and terminal publication with readback, terminal/infrastructure-only
monitoring while incomplete, immediate instance termination, process-tree
RSS plus 64 MiB below 3 GiB and zero swap for each phase. Use 32
individual code-group reads and at most 16,777,216 bytes per query,
skipping ranked groups that cannot fit. Keep exact group payload sizes.

The first stop is source-only selected-group containment. If even perfect
downstream scoring cannot reach the final quality thresholds below, stop
before decoding or fetching two-bit row codes. This is a strict upper
bound for the fixed group plan, not a product quality claim. Preserve
all 1,000 plans, containment samples, exact resource receipts and an
independent reconstruction of model/codes/ADC/groups.

If containment passes, run a separate preregistered paired 100k actual
two-bit code-quality/read/resource cell: read exactly the selected groups,
score rows with the unchanged two-bit primary scorer and final-page
nomination, replay the historical tree router and two-bit scorer as a
control, and include exact row scoring on the challenger's groups as a
diagnostic. Require the historical control's full per-query plans and
outcomes to match its terminal-closed evidence; aggregate equality alone
is insufficient. A diagnostic pass cannot rescue primary failure.

Advance only if the challenger has at least 98,418 GT100 hits across
100,000 ordered truth positions, at least 92% p05 GT100 (control 91%),
at least 9,951 GT10 hits across 10,000 positions, fewer sub-90 queries
than the paired control, at most 32 code GETs and 16 MiB code bytes per
query, at most 32 final data pages and 16 MiB planned data bytes per query,
and all resource/authentication/replay gates pass. A valid miss kills this
fixed OPQ8/top-four hypothesis. Provenance or replay disagreement makes
an attempt invalid, not a scientific failure; preserve it and repair under
a new source revision and attempt.

## Scale qualification after a pass

The 8-byte route plane occupies 800,000,000 bytes per 100M rows, before
models, liveness, mutation metadata, two-generation overlap and runtime
reserves. A tentative two-full-generation allocation is about 2.87 GiB
under a 3-GiB cap; it remains unmeasured and leaves limited headroom.
Source-only 100k quality does not establish acceptable 100M scan
throughput (800 MB and eight ADC contributions per row per query), native
S3 latency, mutation/compaction correctness or packaging. On a valid
100k pass, the next gate is unchanged 1M original physical order and
eight-page groups, with the fixed 96-byte locality projection: at least
98.151% mean GT100, 90% p05, 99.28% GT10 and at most 49 sub-90 queries
under 32 GETs/16 MiB. Real code plane, fresh holdout, two-generation
memory and serving performance remain separate gates.

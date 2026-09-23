# ReLAION 96-byte row-score fidelity gate

Status: preregistered design, no 96-byte quality measurement or Spot launch.

## Decision and authority

The terminal-closed 1M source-range diagnostic at source commit
`d4e46abe1ebd62ef18d54486eb4d3d77f2e3776f` reached 98,920 of
100,000 GT100 positions and 9,956 of 10,000 GT10 positions using exact
source-vector row scores. The same selected groups and 32-range/16-MiB
planner with OPQ8 row scores reached only 91,282 GT100 positions. This
isolates final row-score fidelity as the main observed loss. It does not
establish that any compressed scorer will pass, nor any serving latency.

The architecture decision is whether a **96-byte** fetched row-score
record can preserve enough of the source plan to pass the fixed 1M gate.
The primary hypothesis is a rotated one-bit code; standard PQ96 is the
predeclared rescue if the primary fails its cheap 100k screen. This
sequential order avoids training PQ96 when sign96 already qualifies.
The earlier 100k rotated two-bit cell lost 97
GT100 positions relative to exact scoring on the same grouped rows at
200 bytes per row. That result motivates the sign-code family but cannot
be extrapolated numerically to one bit or to the new range planner.

## Immutable inputs and split

Use the source, query, truth, OPQ8 selected-group plans and physical layout
identities in the closed source-range attempt. Freeze source commit, package
versions, CPU architecture/SIMD identity, random seeds, code format, scoring expression, page tie rule,
range rule, validation method and thresholds before each run. The same
1,000 development queries may select an architecture; an untouched query
cohort is required before product qualification. Neither arm may use truth,
the source arm's priority list or the source arm's selected ranges to plan
its own ranges. The **selected OPQ8 candidate groups and their rows** are
shared; each compressed arm computes its own row scores, derives its own
top-100-row page priority and runs the same range admission rule. Exact
source scores are the paired diagnostic only.

The closed source-range `plans.json` stores page priority order, not
numeric page-score margins. A margin certificate would require a new
exact-score pass and is not a free pre-build check from that file.

## Records and scores

**Primary sign96:** apply the already specified three-block signed
256-point normalized Hadamard transform to each mean-centered source row.
Use the two-bit family's SHA-256 sign derivation and rotation seed
`20260923`. Fit the per-corpus mean in a separate externally authenticated
source pass: traverse source rows in Parquet order in **exactly 4,096-row
batches with only the last batch shorter**,
sum each batch in float64, add batch sums in traversal order, divide by
the exact row count and store once as little-endian float32. The
mean receipt binds its hash, row count, batch size and SHA-256 of the
raw source-vector stream. The encoding pass verifies the same stream
hash before sealing groups and uses only that stored mean. This streaming
reduction is a new format rule and may differ in low bits from the
historical full-array two-bit mean. Retain rotated
coordinate `j` exactly when `j % 48 != 47`, leaving 752 coordinates and
omitting 16 evenly spaced positions. Seal that coordinate rule. Encode one
sign bit per retained coordinate, with zero taking positive sign, packed
little-bit-first into exactly 94 bytes. Let `a` be the mean absolute value
of those 752 rotated coordinates, rounded once to binary16. The remaining
two record bytes store that scale. There is no stored per-row norm or ID.
For query rotation `z`, score by
`||query-mean||² + 752*a*a - 2*a*sum(z_j*sign_j)` over retained
coordinates. The first term is constant across rows and expresses the
reconstruction score in squared-distance units. Reconstruction can have
row-dependent bias; report signed score-error tails as well as rank and
page outcomes. Decode the stored binary16 scale before scoring. Precompute
94 by 256 float64 byte-dot table entries per query by reducing each
eight-coordinate signed block; score each record with 94 table lookups
and a float64 reduction. Break equal float32 scores by
physical row ordinal in the caller. Reject
nonfinite inputs and invalid scales. Compare this stored-scale score with
the exact source-distance arm, because scale rounding is part of the
representation. The 94+2 record is 96 bytes; 94+2+2 would be 98.

**Contingent PQ96 rescue:** 96 disjoint eight-coordinate subspaces, one byte per
subspace, 256 centers per subspace, with train sample, seed, centroid
dtype, iteration rule and ADC reduction pinned in the seal. Reuse the
previous PQ96 construction method only where its exact inputs and
codebook contract can be authenticated; no results from a layout-only
projection count as PQ96 row-score evidence.

Write each arm in authenticated four-page groups with per-page row-count
headers, a versioned format marker, source/order/mean/model hashes and
complete group SHA-256 identities. Seal a hash of the coordinate, bit,
scale, rotation and scoring rules. The writer returns the seal hash from
its in-memory canonical bytes; the campaign manifest must authenticate
that hash independently of later disk reads. A reader must receive the
independently authenticated seal SHA-256 and reject an unknown format,
seal or group identity mismatch. The caller must authenticate source
Parquet bytes, membership, physical order and count inputs before using
the artifact writer; those claimed hashes are not verified by the writer
itself. The sign96 kernel accepts at most 4,096 materialized
source rows per encoding call; its caller must stream source batches into
that interface. Score in 2,048-row batches and measure process-tree RSS;
no code path may materialize the whole 1M source matrix.

## Paired gates and stop rules

First run one sign96-versus-source 100k source-only screen under the **same** top-100-row
page-priority **rule** and 32 merged data ranges/16,777,216 encoded data
bytes intended for 1M. The 100k run has one base object; use the same
range algorithm with a single `base` role, while the 1M run has its
sealed `base` and `delta` roles. Score the rows in exactly the same selected groups
with sign96 and source vectors; recompute each arm's page priority
and data ranges independently. Use the same physical layout, route, queries and truth
for both arms. Report GT100 and GT10 hit counts, p05, sub-90 query count,
per-query deltas, selected-page and interval deltas, score-error tails,
**projected code-group GETs/bytes and planned data-range GETs/bytes**, runtime and
peak process-tree RSS/swap. The old
32-page 100k result is historical context, not the screen's control.

A scorer advances from 100k only if the paired source arm reaches at least
98,000/100,000 GT100 positions, the scorer loses at most 300 GT100 positions
relative to that arm, has p05 GT100 at least 90, creates at most ten additional sub-90
queries, stays within both **data-range** I/O caps, and stays under 3 GiB minus 64 MiB
peak process-tree RSS with zero swap. These are conservative screening
rules, not proof of a 1M pass. If sign96 passes, advance it alone to 1M
without training PQ96. If sign96 fails, run **one** separately sealed PQ96
rescue on the identical 100k inputs, fixed group plans, source-score
control and thresholds. PQ96 may advance alone if it passes; otherwise
stop this width and revise representation or budget before another
campaign. Do not tune either scorer on these development queries.

At 1M, hold the sealed OPQ8 selected-group plans, original page layout,
page-priority **algorithm** and 32-data-range/16-MiB admission rule fixed;
do not reuse the source arm's priority values or selected ranges. Each advancing
scorer constructs and reads its own authenticated 96-byte code groups and
replays all 1,000 queries. The fixed candidate floors are GT100 at least
98,151/100,000, GT10 at least 9,928/10,000, p05 at least 90,
sub-90 count at most 49, GETs at most 32 and encoded data bytes at most
16,777,216 **for the data-range wave**. Count and report code-group
GETs and bytes separately; this source-only gate does not impose a code
GET latency limit or imply that the code wave shares the data-wave caps.
Compare against the 98,920/9,956 source-score arm, the
historical OPQ8 arm, and a page-centroid control scored with the **same**
candidate representation. A scorer that fails any floor is rejected.
Only a passing scorer can proceed to actual authenticated code and data
reads on the untouched cohort and serving measurements. Do not claim
recall, latency, throughput or 100M viability from source-only passes.

Use Causality Spot for the sealed interruptible cells, record instance
identity, upload each terminal artifact create-only, discard and restart
an interrupted measurement cell, independently authenticate and recount
the closed attempt, and terminate compute immediately at its terminal
marker. Monitor incomplete cells only via infrastructure health and
terminal markers; never inspect incomplete measurement CSV.

## Formal obligations

`formal/SourceRangeFidelity.lean` proves that the closed source count plus
a bounded paired hit loss implies the aggregate 1M GT100 and GT10 floors.
It also proves that a 100k source arm with at least 98,000 hits and at most
300 paired lost hits leaves the compressed arm with at least 97,700 hits.
It also proves the 96-byte record and conditional code-payload row
arithmetic (which must not be inferred from the separate data-range cap) and a
per-query sufficient condition for retaining 90 GT100 hits. Future work
must bind the input hit masks to authenticated artifacts, prove that the
Python merged-range planner refines its Lean model, and check the p05 and
sub-90 conditions. Conditional service-rate premises can produce latency
bounds; S3 tail behavior, unseen-query recall and 100M route work still
need measurements and an adequately scaled routing architecture.

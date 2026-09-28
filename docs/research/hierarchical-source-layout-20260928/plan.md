# Reuse hierarchical fitting for capped semantic extents

One material change from failed flat native source order: reuse existing
train_logical_cell_centroids (fanout32, proportional sample quotas,12 updates)
and CentroidHnsw assignment instead of flat all-centroid training/assignment and
quadratic centroid chaining. Target ceil(N/1024) source cells, reservoir<=64
samples/cell (same seed8201). Input is already unit-normalized; squared-Euclidean
training retains mean geometry. If the sampled source is entirely identical,
train one cell. Other trainer failures reject rather than silently changing the
recipe. Stable cell/radius/ordinal order, hierarchy output cell order; split an
oversized final cell's ordered rows into extents of at most1024. This is a row
cap, not a claim of balanced full-corpus density or adaptive semantic splitting.

Public candidate returns order+extent ranges. Existing failed flat function is
retained only as the current research control until a candidate survives; no
migration/reader/alias or production default is added. CLI explicit `hier-fit`
seals the new order and names recipeborsuk-hierarchical-extents-chacha8-v1 with
extent boundaries. Serving must authenticate new extent metadata before use;
current generation queries do not use these extents and remain unqualified.

Authenticate source on both sequential passes, validate norms/geometry/hash,
admit modeled sample/geometry/trainer packing/centroid graph/order/range payload
before allocation. No whole source matrix. Source fitting memory scales with
source cells and dimension under an explicit caller cap; assignment traversal
has no worst-case fixed-work guarantee. A100M D768 build64GiB payload envelope
is provisional (model, not measured), with extra runtime/OS headroom required.
Keep32GET/16773120B query caps. Current generation's conservative metadata-open
model may require128GiB at100M; no new RSS/QPS/p90/lifecycle claim is made.
The final100M Pareto/latency/cost envelope remains to be measured against both
vendors; do not freeze defaults from a theoretical admission number.

Checks: one red/green fixture on32/2048/8192 rows (including identical and varied
unit vectors), stable complete ordinal permutation, contiguous nonempty1024-row
extents, repeatability, hash/geometry/cap/overflow rejection; run affected source tests (which exercise the reused trainer) on existing Spark. One optimized CLI build.

First source-only corpus falsifier: CoHere first100k D768 normalized source,
development0–63 GT100, all original hashes from native preparation. One new fit
and metadata-only GT-aware extent diagnostic. Top32 extents ignores byte cost
and is optimistic at32GET: KILL if mean<98.9 or p05<96. Top21 extents are a
feasible witness (<=21504 rows/16773120B,<=21GET): if mean>=98.9/p05>=96, GO only
to actual nomination/returned-SQ8 development check. Otherwise HOLD for exact
metadata-only byte/GET knapsack; no training sweep or cloud. Neither result is
actual recall or vendor superiority. ReLAION only after CoHere survives. Cap
fit+oracle300s/4GiB/2CPU; one owned job, no paid run.

Prior failures: V149's contiguous query hierarchy is not reused; this trains
source-semantic cells. V139 threshold admission/V146 flat query scan/V150 unit
truncation remain rejected. Fable418e158ae6644129 containment warning and native
CoHere32-page p05 89 motivate multi-page extents. Both new traversal variants
remain KILL. Spill replicas are not implemented without a separate causal gate.

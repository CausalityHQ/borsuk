# V152 cached page-score gate

**Decision:** does reusing graph-computed unit-centroid distances remove
V151's duplicate work and pass paired CPU without losing its returned
quality or transport advantage? V151 failed only the CPU gate on the
deep-image-96-angular random100k subset: 0.644371 ms p95 versus
paired flat 0.570292 ms, despite 99.646% versus 99.720% source
Recall@100. Its p95 total of 1,933 computed unit distances contained
only 1,325 distinct units.

## Frozen input and one change

Use the same authenticated V122 100k subset, SQ8 layout, source-only
router, V146 f16 unit-centroid plane, V150 graph adjacency and β=4
planner. Development queries are publication test ordinals 3000–3999;
their V151 GT has already been opened, so this is an engineering
comparison, not a fresh validation. Reserve 4000–4999 for confirmation
only after a development winner. Flat, unchanged V150, unchanged V151
and V152 process the same query and primary roster. The even order is
flat, V151, V150, V152; the odd order reverses it. This puts V150
between the identical V151/V152 walks. V150 and V151 timings are
diagnostic.

V152 runs exactly V151's primary-seeded 16P graph search and 4P
nonprimary page roster. It carries each evaluated unit's squared
Euclidean distance into a query-local cache. For every candidate page,
reuse those distances and compute only its unevaluated units, then
take the page minimum. This changes arithmetic from V146's norm-dot
formula to direct squared Euclidean accumulation for page scoring;
both compute distance on the same authenticated f16 centroids. Record
the maximum absolute page-score difference against paired flat and
reject if it exceeds 0.0001. This is a generic per-unit cache with no
dataset-specific parameter or vector-count threshold.
The cache is the search's query-local map of at most 16P evaluated
units, moved into page scoring without copying; it does not allocate
or initialize one slot per corpus unit. V151 uses the
original search path without exporting scored distances, so its
diagnostic timing includes no V152 cache export work.

## Measurement and decision

Seal source-only query/routing inputs and exact V122 GT100 as in V151.
Measure per-arm page scoring/search/planning wall time, graph and new
unit work, scored and selected pages, ranges, GETs, planned bytes,
resource peaks and graph build. Replay each arm's SQ8 top-512 and
512-router-nominee source union, seal ordered source top-100 before
opening GT, and run a separate GT-blind returned-ID audit. Authenticate
all terminal artifacts and independently recount page plans, work,
score parity, returned quality and verdict after the terminal closes.
Record search, candidate-page scoring and planner phase times. Report
flat and V152 p95 separately by even/odd query order. The GT-blind
audit recomputes all 391 flat page scores for each query from the
authenticated centroid plane, accepting at most 0.001 absolute
cross-arithmetic difference. Pin the regenerated queries, primary
rosters and GT digests to V151's terminal-closed artifacts.

Pass requires: all 1,000 queries retain primary pages and obey the
16P search, 4P additional-page, 32 GET and 16,777,216 B caps;
maximum page-score difference versus flat at most 0.0001; V152 source
Recall@100 within 100 hits of paired flat, p05 at least 98 hits, zero
queries below 90; V152 p95 page-search-plus-planner wall time strictly
below paired flat; and aggregate planned bytes and GETs no greater
than paired flat. Report V151 as a paired diagnostic and compare V152
work against the distinct-union count. One Causality Spot attempt and
immutable source archive are preregistered. On interruption, discard
and rerun the whole cell under a new attempt ID; terminate compute
immediately after its terminal marker. Monitor incomplete runs only
through terminal markers and instance health.
The one-shot p95 inequality is a development screen: report its
order-parity spread and treat a narrow win as provisional until the
reserved-cohort confirmation. The node-distance kernel remains V151's
strict sequential float32 accumulation so the graph roster is an
unchanged control; vectorized arithmetic is a separate design change.

The 100k corpus has only 391 physical pages and fits under 16 MiB, so
even a V152 pass is an implementation screen. A passing result must
confirm on reserved ordinals 4000–4999, then face the frozen paired
ReLAION-1M sparse-discovery gate. Lean can bound cache work under
explicit scored/evaluated-set premises; it cannot prove observed
recall, wall time, GET cost or charged RAM without measured inputs.

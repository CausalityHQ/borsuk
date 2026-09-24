# V150 semantic unit-centroid graph GT-free 100k preregistration

**Decision:** can a navigable graph over the existing 32-row unit
centroids discover the β=4 BORSUK page plan more faithfully and faster
than V149's contiguous physical-page summaries, while bounding query
work independently of the corpus vector count? This gate contains no
GT, returned IDs or live S3 fetches and cannot qualify recall.

Use the V146 authenticated `BORSUCP1` centroid file and flat Rust score
matrix, V140 β=4 raw page plans, V122 deep-image-96-angular random100k
queries, and V138 primary rosters on the already-used publication-test
source ordinals 9000–9999. Authenticate all inputs by length and SHA-256.
One frozen source archive and one Causality Spot attempt produce a
terminal, per-query raw records, resource logs and graph artifact; an
interrupted attempt is discarded and restarted under a new attempt ID.
Monitor only terminal markers and instance health while incomplete.
Stop the instance immediately after a terminal marker.

## Frozen method

Build deterministic HNSW adjacency over the f32-decoded f16 unit
centroids, keeping the existing `centroid_hnsw.rs` defaults `M=16`,
`M0=32`, `ef_construction=64`. Keep vectors in the authenticated V146
centroid plane; serialize adjacency and version its format rather than
writing a second vector copy. The builder must reuse one epoch-marked
visited array across insertion searches. Record build wall time, peak
RSS, cgroup peak and artifact bytes; these 100k figures do not project
100M build performance.

For each query let `P` be the number of distinct primary physical
pages and β=4. Search the graph for up to `2βP=8P` nearest unit
centroids, with a hard cap of `4βP=16P` distinct unit-distance
evaluations. Count upper-layer navigation in that cap. Deduplicate the
returned units to physical pages in nearest-unit order; retain at most
`βP=4P` nonprimary pages and add every primary page. Evaluate the
V146 exact eight-unit minimum score on every retained page, then run
the unchanged primary-first sparse planner with 32 GET and
16,777,216 B limits. The graph may stop early at its work cap. The
β-derived multipliers apply unchanged to D96, ReLAION D768, 10M and
100M; a requested recall or transport budget may later enlarge β
after fresh evidence, with no vector-count knee.

Record visited unit IDs, exact page scores, selected pages, GETs,
bytes, all work counters, early stops, build time, CPU p50/p95/p99,
RSS and cgroup memory. Compare visited-page exact scores to V146:
maximum absolute difference ≤0.0001. Include every primary physical
page and respect all planner/work caps on all 1,000 queries. Recount
the terminal from raw records independently.

The quality proxy is the aggregate fraction of V140 β=4 selected pages
retained by the new sparse plan: **≥95%** and at least **5 percentage
points** above the page-ID control policy used in V149, recomputed for
each query at V150's own number of scored pages. Every query's target
shortfall must be no worse than V140.
The work gate is p95 distinct unit-distance evaluations **<3,125**
(V146 flat unit count). The CPU gate is p95 graph search plus exact
page scores plus planner **<0.530383 ms/query**, V146's verified flat
Rust score-plus-planner p95 on this same used D96 cohort. V149's
0.641749 ms p95 and 77.0956% page capture are the rejected route
baselines; the page-ID control captured 39.8580%.

One failure rejects this frozen graph policy for the next production
promotion. Diagnose whether the cause is neighbor discovery, graph
build, bounded work, page-score interface or serving overhead before
a materially revised architecture or method. Do not tune `M`, `ef`,
the multipliers or a corpus-size threshold from this used cohort.
A pass licenses a fresh held-out 100k returned Recall@100 and live S3
latency/cost comparison against the strongest current β=4 BORSUK arm.
Only a quality and cost winner advances to frozen ReLAION-1M, then
10M/100M. The 1M D768 p95 ≤10 ms local CPU gate remains unchanged.

Lean may prove conditional graph work, adjacency size, planner byte
and GET bounds from the validated format and caps. Graph neighbor
quality, returned recall, measured CPU/S3 latency and charged RAM
still require experiments or explicit measured assumptions.

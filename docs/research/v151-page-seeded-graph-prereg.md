# V151 primary-seeded, page-diverse graph gate

**Decision:** does ranking distinct pages from a primary-seeded walk over
the authenticated V150 unit graph restore returned quality at lower
paired CPU and object-store work than the flat V146 scorer? V150's
73.35% page-plan overlap failed its frozen proxy, but a postterminal
GT-cover diagnostic found 99,523/100,000 truth positions in its actual
fetched ranges. Page-plan overlap is therefore diagnostic here, not a
quality gate.

## Frozen source and fresh query split

Keep the V122 deep-image-96-angular random100k subset, SQ8 layout,
f16 unit-centroid plane, V150 HNSW adjacency and β=4 primary-first
sparse planner unchanged. The new development cohort is publication-v3
deep-image `test.parquet` ordinals **3000–3999**, which are absent from
the checked-in research ledger's used 0–999 and 9000–9999 cohorts.
Authenticate the test object, V122 subset, SQ8, router and graph inputs
by length and SHA-256. Recompute exact GT100 within the fixed subset by
the V122 float32 dot-product and stable ID-tie method; seal its digest
before arm evaluation. Derive fresh primary rosters and 512 router
nominees by the unchanged V122 source-only router. Retain ordinals
4000–4999 for a later confirmatory winner gate; do not read them here.

## One changed search policy

For each query, let P be distinct primary physical pages and keep the
same 32-row unit/256-row page geometry. Seed a min-priority unit
frontier with every unit on all P primary pages. Cache each distinct
evaluated unit distance. Pop the nearest frontier unit and evaluate
its base-layer neighbors in stored graph order, pushing every newly
evaluated neighbor. Stop at **16P** distinct graph unit evaluations,
including seeds. Record work exhaustion. Do not use V150's arbitrary
top-layer entry or an `8P` unit-result heap.

For every evaluated unit, keep the minimum observed distance for its
physical page. Rank distinct **nonprimary pages** by that provisional
score and page ID; admit up to **4P** as exact-score candidates, plus
every primary page. Score each candidate page with the unchanged
V146 eight-unit minimum, then run the unchanged β=4 sparse planner.
The provisional page minimum is an upper bound on the exact page
minimum, but graph traversal has no certified unseen-page lower bound;
the bound alone does not guarantee recall. The cap and page mapping
depend on P, requested β and page geometry, with no vector-count knee.

The comparison arms are (1) V146 flat exact unit scores plus the dense
planner, (2) unchanged V150 single-entry/unit-result search, and (3)
V151 primary-seeded/page-result search. They share the same query,
primary roster, graph artifact, page geometry, 32 GET and 16,777,216 B
limits. Alternate flat-first and graph-first query timing; keep raw
nanoseconds, evaluated unit/page IDs, provisional and exact page
scores, selected pages, ranges, GETs, bytes, RSS, cgroup peak and build
time. Independently replay the page plans and work accounting from
terminal-closed records.

Seal each arm's ordered top-100 returned source IDs **before** opening
GT100. Use the V141 source-union procedure: 512 router nominees plus
rows in fetched SQ8 ranges, then float64 cosine on authenticated
original float32 source vectors. Score returned Recall@100, p05,
queries below 90 and paired outcomes from the sealed IDs.
Before opening GT, a separate audit recomputes each arm's ordered
SQ8 top-512, nominee union and source top-100 from the authenticated
inputs; its sealed receipt is required for a complete terminal.
The postterminal recount independently recomputes scored centroid-page
distances and V151 provisional page minima/ranking from the frozen
centroid plane, and replays the planner and returned-quality reductions.

## Decision gates and promotion

All 1,000 queries must retain every primary page, obey the 16P search
cap and transport caps, and match V146 exact scores on scored pages
within 0.0001. V151's aggregate exact-source Recall@100 must be
within **0.1 percentage point** of the paired flat arm, with p05
**≥98/100** and zero queries below 90. Its paired p95 graph search,
exact page scoring and planning CPU must be below the paired flat
score-plus-planner p95. Report p95 distinct graph, exact-page, union
and total unit work; no artificial 100k graph-work ratio is a pass
gate, because P and the 16P cap can make it tautological. Candidate
bytes/GETs must be no greater than the paired flat arm in aggregate,
and all planned ranges remain under the per-query caps.
Report target-shortfall count and p95. The 100k corpus is only 391
physical pages; at typical P around 40, the 5P candidate budget can
exact-score about half the corpus and the entire SQ8 corpus is below
16 MiB. A pass is a necessary implementation screen, while the frozen
1M comparison is the first decisive sparse-discovery quality gate.
The 8P seeded distances are deliberately counted within 16P; scoring
primary pages again is included in the timed CPU and total work.
V150 arm timing is diagnostic because it always runs between the
alternating V151 and flat arms. CPU timing is single-threaded wall time.

One frozen source archive and Causality Spot attempt produce a
terminal, raw per-query records and resource logs. Interrupted cells
are discarded and restarted under a new attempt ID; terminal artifacts
are synced to S3, independently authenticated and recounted, then
compute is terminated. Monitor incomplete work by terminal marker and
infrastructure health only. If the policy fails, distinguish graph
non-discovery, page-result ranking, page admission, representation,
source rerank and serving overhead. Do not tune `M`, `ef`, 16P, 4P or
a corpus-size threshold on this split. A pass permits a frozen
confirmation on 4000–4999, then paired ReLAION-1M and 10M/100M gates;
it does not by itself qualify S3 latency or publication claims.

Lean can prove that the result map has at most one entry per page,
that a provisional minimum over evaluated units upper-bounds the
exact page minimum, that graph distance work is at most 16P units,
and that the planner enforces bytes and GETs under its stated input
premises. Returned recall, graph discovery, elapsed latency and
charged RAM require authenticated data and hardware measurements.

# V292 bounded centroid-graph precursor

Status: frozen before V292 query outcomes. V291's flat 159-page centroid
precursor plus resident rotated two-bit page scoring and one SQ8 page wave
passes CoHere first100k D768 cosine k100 development0–63: exact Rust return
98.1875% mean/p05 96, fetched99.765625/p05 99, <=32 GET/16 MiB. The
precursor scans all 3,125 centroids, so it is not a scale-qualified route.

Change **only first-stage page discovery**. Reuse the authenticated V283
`UnitCentroidGraph` and f16 source-unit means. Find one nearest unit using
`search(nearest_units=1,max_evaluations=128)`; its physical page is the seed.
Run `search_pages_seeded` with that one page, at most 158 additional pages
and 1,272 centroid evaluations (8 times the fixed 159-page candidate count).
Use the seed plus the best discovered pages, stable IDs. The total bound is
1,400 centroid evaluations/query; repeat evaluations in the two calls are
charged. Graph edges and centroids are resident routing metadata, never
S3 pointer reads or the full source-vector graph. No source SQ8 payload is
used by page discovery; an authenticated local SQ8 ID map is used only by
the separate offline coverage checker.

Use the unchanged V283 generation and CoHere development0–63 requests/GT.
KILL before two-bit refinement if any query cannot produce exactly159
distinct pages, exceeds either visit bound, mean candidate GT100 <98.9, or
p05 <96. A pass permits one replay of the *unchanged* V291 two-bit and exact
SQ8 scorer on these candidate sets, then paired 100k validation if returned
quality passes. No panel, paid job or 1M gate yet. Record candidate sets,
coverage, evaluation counts and routing wall p50/p90/p95/p99 from the same
64 samples, explicitly offline/planner-only. One local run <=120 seconds,
<=2 GiB process RSS.

The unmeasured 100M arithmetic from `ceil(8 sqrt(page_count))` is 5,000
candidate pages and 40,128 total unit visits for 390,625 pages. That is a
preregistered work envelope, not evidence of recall, memory or latency at
100M. The prior V282/V283 PQ-seeded graph arm failed upstream nomination;
this uses no PQ primary roster and refines a larger candidate set with the
new high-fidelity code plane. It does not repeat the failed final-page graph
route or promote full resident vectors as the default.

## Completed development precursor (2026-09-27 UTC)

**GO to the unchanged two-bit/SQ8 development replay, no cloud.** The frozen
`fec3842d` debug Rust executable completed exit 0 in 10.98 s with 102,800 KiB
peak process RSS. Its [result](v292-bounded-centroid-route-result.json) SHA is
`5c14ee60fb9e01fc618e56e691b6bbf6be3b1a7dbfb4599d079c27c6fc197c40`;
[raw candidate samples](v292-candidates.jsonl) SHA is
`c24e53f8e740ad569331c774f46ef3161873e3a765d20a345290af50a1b974d0`.
All 64 CoHere first100k D768 cosine k100 development queries returned exactly
159 candidate pages, at most 1,400 centroid evaluations. Mean candidate
GT100 is 99.578125/p05 98, versus flat V290's 99.8125/p05 99. The bounded
precursor loses 0.234375 mean hits to flat but passes 98.9/p05 96. Planner-only
wall p50/p90/p95/p99 are 25.979/33.063/33.545/33.685 ms from the same 64
debug-build samples; they are neither service latency nor a scale result.
The next single gate retains these exact candidate sets and V291's two-bit
score, final physical planner and production-f32 SQ8 rank; returned 98%/
p05 95 remains required before any paired validation or 1M gate.

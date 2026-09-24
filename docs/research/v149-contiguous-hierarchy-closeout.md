# V149 contiguous hierarchy closeout

**Decision: reject the physical-page hierarchy as the production candidate
route.** Its bounded traversal finds substantially more V140 pages than a
page-ID control, but misses too many of the strongest flat-route pages and
is slower than the V146 flat Rust scorer on this D96 cohort. Do not tune
the fanout or beam on these already-used queries; the next falsifier is a
semantic unit-centroid graph with bounded query work and a build path that
does not allocate a corpus-sized visited bitmap per insertion.

## Frozen evidence

The worker used source commit `052bdb9785d405a3da621ff95582d2ed64ea18b6`
and archive SHA-256
`8601e9913bfddc15393cc0c183fffb4b1a2c317b8027d6b0253294b7b293aebb`.
One Causality Spot `c7i.8xlarge`, `i-0cde30293b9b92bca`, completed
uninterrupted in 105 seconds and is terminated. Its terminal is
`s3://borsuk-bench-453182569524-euc1/research/v149-contiguous-hierarchy/052bdb9785d405a3da621ff95582d2ed64ea18b6/runs/v149-20260924T114152Z/a0001/terminal.json`
(SHA-256 `42d054f0dd0c4ae4dd754bd505a85eeab9f7e11e51d258a52262a022dab0a8f4`).
All 14 terminal artifacts were independently authenticated by length and
SHA-256 after the terminal, and the 1,000 per-query records were recounted
against the authenticated V140 selected pages and V146 flat Rust score
matrix. The first local postterminal verifier stopped on a one-character
typo in its hardcoded V146 SHA. The corrected verifier authenticated and
recounted this **same** completed attempt; no benchmark was rerun.

| Metric | V149 measured | Relevant baseline / gate |
|---|---:|---:|
| Cohort | deep-image-96-angular random100k train subset; 1,000 previously used publication-test queries, source ordinals 9000–9999 | Frozen V140/V146 cohort |
| V140 β=4 selected-page capture | 65,374 / 84,796 = **77.0956%** | Preregistered ≥95%; page-ID control 33,798 / 84,796 = 39.8580% |
| V140 pages visited but rejected by planner | **0** | Candidate discovery caused all 19,422 missing pages |
| Score on visited pages | max absolute difference **0.0** | V146 flat Rust matrix; gate ≤0.0001 |
| All primary pages retained; GET/byte caps; target shortfall | **1,000/1,000**, all caps hold, no shortfall worse than V140 | 32 GET and 16,777,216 B |
| Coordinate-vector evaluations | p50 **1,219**, p95 **2,016**, p99 **2,256** per query | V146 flat 3,125 units/query; the p95 work gate was determined by this roster and beam cap, so it is not independent route-quality evidence |
| Search CPU | p50/p95/p99 **0.120770/0.149107/0.159776 ms/query** | V146 flat score p95 0.038115 ms/query |
| Search + planner CPU | p50/p95/p99 **0.261143/0.641749/0.790504 ms/query** | V146 flat score + planner p95 **0.530383 ms/query**; V145 planner-only p95 0.474496 ms/query |
| Hierarchy build and artifact | **1.231978 ms**, **90,568 B** | 16-way, height 4; no corpus payload included |
| Centroid arrays, process RSS, science cgroup peak | **1,212,500 B**, **52,396 KiB**, **66,215,936 B** | Different resource measures; RSS and cgroup include overhead |

Six queries exhausted the node budget. The planner admitted every
baseline-selected page that traversal visited, so neither sparse
admission nor the GET/byte cap explains the miss. The broad, overlapping
summaries of consecutive physical pages provide weak semantic guidance;
query CPU also includes scalar f64 summary distances, heap work and
repeated query normalization for exact page scores. V149 did not measure
new returned Recall@100 or live S3 latency. Previously verified V141
100k D96 returned Recall@100 was 99.739% for the current β=4 BORSUK arm
versus 99.942% for its broad arm on a used development split; these are
baseline context, not V149 quality measurements.

## Next gate

Implement one dimension-generic graph over unit centroids. Retain the
V146 exact score definition and sparse planner, but navigate semantic
neighbors across physical page boundaries. Use reusable epoch-marked
visited state or an equivalent bounded scratch design in construction;
the existing `centroid_hnsw.rs` builder copies vectors and allocates an
O(node-count) bitmap per insertion, so its 100M build scaling is not
assumed. Preregister a GT-free 100k discovery/cost comparison against
V146 flat and this V149 route with the same V140 page-capture gate and
page-ID control, then run fresh held-out returned quality/live S3 only
for a winner. Keep the 1M D768 ≤10 ms p95 CPU gate and eventual
10M/100M recall, latency, resource and cost gates unchanged. RAM may
rise with measured recall and latency needs; use no vector-count knee.
Lean can prove conditional work and memory bounds for the graph policy,
but no proof of hardware latency or empirical recall follows without
measured assumptions.

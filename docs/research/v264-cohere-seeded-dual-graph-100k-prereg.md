# V264 CoHere 100k: PQ-seeded exact graph search

V260 achieved 99,948/100,000 exact GT100 hits but its loaded eight-worker
p95 was 53.510 ms. Its exact FP16 path starts from the graph's global
entry after the PQ path has already identified a query-specific neighbor.
V264 tests whether using that neighbor as the exact path's start reduces
navigation work without losing coverage. This is a generic same-generation
graph method, with no dataset-specific tuning or additional index.

Dataset: CoHere-large-10M canonical first100,000 source rows, D768 cosine,
k100, authenticated prior-used test ordinals0–999; development0–255,
validation256–999. Rebuild the byte-identical V250 M32/M0 64,
ef_construction128, eight-worker source-only diverse graph, PQ64 books and
resident FP16 plane. Fixed arm: PQ-cosine graph ef4096 and FP16
shortlist4096→k100; use its best source ID mapped to the authenticated
physical row as the exact FP16 graph seed; exact graph ef2048→k100; union
both final lists and FP16-rerank to k100. No fallback, sweep, extra graph,
coarse list or query training. Sequential and eight-worker loaded paths
must produce identical ordered IDs. Seal IDs before exact F32 truth.

Pass requires byte-identical source, graph, requests and truth to V260,
valid graph geometry and reachability, combined exact GT100 hits≥99,800/
100,000, split mean R@100≥0.995 and split p05≥98. Loaded in-process
eight-worker p95≤42 ms, p99≤50 ms, throughput≥200 QPS, serving peak
RSS≤300 MiB and zero query vector-body GETs. Report same-sample loaded
p50/p90/p95/p99, split quality, sequential latency, PQ/exact visited
scores, distinct union size, build and Spot costs. These timings are not
HTTP service timings or vendor comparisons.

If every gate passes, freeze one same-method first1M quality/resource cell,
then a same-revision resident HTTP gate before any vendor claim. If quality
or latency fails, stop seed-based navigation and choose one material
algorithm or format redesign, with its own cheap falsifier, rather than
tuning ef or reporting a projection.

One `causality` c7i.4xlarge Spot attempt; immutable source archive and S3
prefix; interrupted cell discarded and restarted under a new attempt;
terminal plus artifact size/SHA-256 readback; terminate immediately at
terminal. Monitor only terminal and infrastructure before closure.

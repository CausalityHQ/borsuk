# V265 CoHere 100k: cached-norm SIMD FP16 graph scorer

V264 showed that changing the exact graph's start row preserved every
V260 result and nearly all its score work. V265 changes the cost of each
exact graph score. During authenticated plane hydration, it validates that
every decoded FP16 row has positive finite norm. For this arm it caches one
FP64 norm per row, uses `half` slice conversion plus safe SIMD FP32 dot
products during navigation, then FP64-ranks the complete retained exact
beam before k100 truncation. The PQ graph, global entry, adjacency,
ef4096/shortlist4096 PQ path, ef2048 exact path, final two-list union and
FP64 FP16 rerank remain fixed. The derived norm cache adds 8 bytes/row;
there is no new persisted artifact or dataset-trained parameter.

Dataset: CoHere-large-10M canonical first100,000 source rows, D768 cosine,
k100, authenticated prior-used test ordinals0–999; development0–255,
validation256–999. Rebuild and byte-verify the same V250 graph, source,
plane, books/codes, map, requests and exact GT100 truth as V260. One fixed
candidate arm, no sweep or fallback. Seal IDs and loaded eight-worker
timings before reading exact F32 truth. V260 is the prior immutable
baseline from a different source revision, not a same-revision paired
control.

GO only if all hold: combined GT100 hits≥99,900/100,000; each split
mean R@100≥0.995 and p05≥98; loaded in-process eight-worker p95≤26.8 ms,
p99≤32 ms and throughput≥330 QPS; peak serving RSS≤300 MiB, zero swap,
query errors and vector-body GETs. Require authenticated artifact parity,
complete graph reachability and ordered sequential/loaded ID parity.
Report same-sample loaded p50/p90/p95/p99, split quality, sequential
latency, PQ/exact graph visit counts, distinct union size, build/hydration
resources and Spot cost. The FP64 rerank of the retained exact beam is
additional work beyond the graph visit counter; report that explicitly.
The 26.8 ms threshold is a decision gate at roughly half V260's 53.510
ms p95, not a performance forecast or a product latency claim.

If GO, freeze one same-method first1M quality/resource cell and a
same-revision resident HTTP gate with empty-cache hydration and first/
repeat client samples. Require first1M combined GT100≥99,500 and each
split mean≥0.995, p05≥98. A direct vendor win requires a matched
end-to-end service comparison; historical S3 Vectors repeat p95 66.394
ms is a target only, under different transport/cache semantics. If NO-GO,
stop this scoring kernel series and attribute the residual whole-query
cost before choosing a compact navigation format.

One `causality` c7i.4xlarge Spot attempt, immutable source archive/S3
prefix, interrupted cell discarded and restarted under a new attempt,
terminal and artifact size/SHA-256 readback, immediate termination.
Monitor only terminal and infrastructure before closure. The remote
release build must pass narrow graph, SIMD+tail and FP16 underflow tests
before measurement.

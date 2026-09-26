# V266 CoHere 100k: block-scaled SQ8 exact-branch navigation

V265 preserved every V260 ordered result while reducing loaded p95 to
29.318 ms, but missed its 26.8 ms/330 QPS promotion gate. Its p95
graph work remained33,637 PQ and21,236 exact visits. V266 changes the
exact branch's navigation representation: derive signed SQ8 codes from
the authenticated FP16 plane, with one scale per 32-coordinate block
and cosine normalization per row. Each exact-branch graph visit reads
768 code bytes and24 scales instead of repeatedly converting 1,536
FP16 payload bytes. The 2,048 retained exact candidates are reranked
against the original FP16 plane in FP64 before k100 truncation. The PQ
path, graph topology/global entry, ef values, final list union and
final FP16 rerank remain fixed. No trained parameter, sweep or fallback.
The cache is deterministic derived state, not a persisted artifact; its
resident bytes are charged before allocation. This is a representation
falsifier, not a kernel retuning of V265.

Dataset: CoHere-large-10M canonical first100,000 source rows, D768
cosine k100; authenticated prior-used test ordinals0–999, development
0–255 and validation256–999. Rebuild and verify the same V250 graph,
source, FP16 plane, PQ books/codes, map, requests and exact GT100 truth
as V260/V265. Seal raw IDs and loaded eight-worker timings before F32
truth scoring. V265 is an immutable prior-revision baseline, not a
same-revision paired repetition.

GO only if combined exact GT100 hits≥99,900/100,000, each split mean
R@100≥0.995 and p05≥98; loaded eight-worker p95≤26.8 ms, p99≤32 ms,
throughput≥330 QPS; peak serving RSS≤400 MiB, zero swap, query errors
and vector-body GETs. Report same-sample loaded p50/p90/p95/p99,
sequential latency, split quality, ordered ID parity versus V265,
PQ/SQ8 graph visit counts, final union size, cache bytes, cold
hydration, graph-build cost and Spot cost. The FP64 exact-beam rerank
is additional score work beyond graph visit counts. The 100k memory
allowance reflects the representation's per-row cost; future scale
admission must grow with shard rows and measured recall, without an
arbitrary corpus-size knee. The latency/QPS limits are decision gates,
not forecasts or HTTP service measurements.

If GO, freeze one same-method first1M quality/resource cell, then a
same-revision resident HTTP gate. First1M quality requires combined
GT100≥99,500, each split mean≥0.995 and p05≥98. Compare vendor
latency only under disclosed end-to-end cache and transport conditions.
If NO-GO, stop the SQ8 navigation representation and decide a different
graph-work or index-format redesign from the terminal evidence.

One `causality` c7i.4xlarge Spot attempt, immutable source archive and
S3 prefix, interrupted cell discarded and restarted under a new attempt,
terminal and artifact size/SHA-256 replay, immediate termination.
Monitor only terminal and infrastructure before closure. Remote release
build must pass narrow graph, SQ8 SIMD/tail and FP16-underflow tests
before measurement.

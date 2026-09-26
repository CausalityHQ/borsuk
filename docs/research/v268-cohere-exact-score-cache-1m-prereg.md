# V268 CoHere first1M: cached dual graph transfer

V267 passed the frozen first100k exact-score reuse gate with all1000
ordered result lists unchanged. V268 changes only corpus scale to
CoHere-large-10M canonical first1,000,000 source rows, D768 cosine
k100. Reuse the authenticated V257 a0001 source, FP16 plane, graph,
PQ books/codes, map and requests. Run the V267 bound PQ shortlist
ef4096/4096 and fast FP16 exact graph ef2048, reusing exact shortlist
scores in beam rerank; retain the same final FP16 union rerank.
One fixed arm, no parameter sweep. V261's immutable first1M run is
the strongest quality-matched in-process baseline from another
source revision, not a paired same-revision control.

Panel: prior-used test ordinals0–999, development0–255,
validation256–999. Seal raw IDs and eight-worker loaded timings
before reading exact GT100 truth. GO only if 1000/1000 ordered k100
lists match V261, combined GT100 hits≥99,500, each split mean R@100
≥0.995 and p05≥98; loaded p95≤105 ms and throughput≥95 QPS
(material improvement over V261's 116.782 ms/86.05 QPS); p99≤125 ms;
peak serving RSS≤3 GiB, zero swap, errors and query vector GETs.
Report same-sample p50/p90/p95/p99, score cache hits, PQ/exact visit
counts, hydration, bytes, build/reuse resources and Spot cost. The
loaded in-process timings are not client HTTP timings.

If GO, put this method through one same-revision authenticated
empty-cache hydration and VPC-peer HTTP first/repeat cell before
claiming product latency. Compare S3 Vectors and Turbopuffer only
under disclosed matched quality, transport, cache, hardware, region,
concurrency and cost. If NO-GO, stop this cache promotion and choose a
material routing/index-format change from the measured bottleneck.

One `causality` c7i.4xlarge Spot attempt with immutable source archive
and S3 prefix. Remote release build and narrow graph/SIMD/underflow
tests precede measurement. Discard and restart an interrupted cell
under a new attempt. Replay terminal artifacts' sizes/SHA-256, then
terminate immediately. Before terminal, monitor only infrastructure
and terminal marker; do not inspect incomplete measurements.

# V267 CoHere 100k: reuse authenticated exact scores

V265's PQ shortlist scores 4096 FP16 rows exactly; its independent fast
graph then reranks its retained ef2048 beam with the same exact scorer.
V267 keeps both routes and all beams fixed. Within one bound graph, it
keeps the PQ shortlist scores for the duration of that query and reuses
overlapping scores during exact graph beam reranking. The cache is local
to a query and cannot cross plane generations. There is no new persisted
format, trained parameter, or dataset-specific branch. V266 SQ8 is a
rejected prior arm. V265 is the strongest relevant baseline.

Dataset: CoHere-large-10M canonical first100,000 source rows, D768
cosine k100, fixed authenticated prior-used test ordinals0–999;
development0–255, validation256–999. Rebuild and byte-verify the V250
graph, source plane, PQ books/codes, map, requests and GT100 truth.
One arm, no tuning sweep. Seal raw IDs and eight-worker loaded timings
before reading truth. V265 is an immutable prior-revision baseline,
not a same-revision paired repetition.

GO only if combined GT100 hits≥99,900/100,000; each split mean R@100
≥0.995 and p05≥98; all1000 ordered k100 lists match V265; cache hits
are positive; loaded eight-worker in-process p95≤26.8 ms, p99≤32 ms,
throughput≥330 QPS; peak RSS≤300 MiB; zero swap, query errors and
vector-body GETs. Report same-sample p50/p90/p95/p99, exact cache hits,
PQ/exact visits, final union size, sequential timing, build/hydration
resources and approximate Spot cost. These are decision thresholds,
not forecasts or end-to-end service claims.

If GO, run one frozen first1M quality/resource and same-revision HTTP
service cell before any vendor claim. If NO-GO, stop score-cache micro
optimization and choose a material routing or index-format redesign
from the measured work distribution before another 1M campaign.

One `causality` c7i.4xlarge Spot attempt, immutable source archive/S3
prefix, remote release build plus narrow graph/SIMD/underflow tests
before measurement. Discard and restart any interrupted measurement
cell under a new attempt. Sync terminal artifacts to S3, replay every
size/SHA-256, terminate immediately. Before terminal, monitor only
infrastructure and terminal marker; do not inspect incomplete CSV/JSONL.

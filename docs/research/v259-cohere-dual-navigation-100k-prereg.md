# V259 CoHere 100k: dual navigation quality and work falsifier

V257 PQ graph plus coarse routing and V258 exact FP16 graph navigation
each failed the frozen CoHere-first1M recall gate, but the union of
their closed final k100 lists contained 99,859/100,000 exact GT100
IDs. That number is a post-terminal candidate ceiling, not a measured
dual-method result. On the prior-used first100k panel, a read-only
replay of the closed V256 hybrid final k100 and V251 exact ef2048 final
k100, followed by the authenticated FP16 plane's cosine scorer,
returned 99,955/100,000 exact GT100 hits. This was a **post-hoc
diagnostic** (not a timed or frozen serving result), so V259 must
measure the implementation independently and cannot claim holdout
validation from it.

Dataset: CoHere-large-10M canonical first100,000 source rows, D768
cosine, k100, authenticated prior-used test ordinals0–999,
development0–255 and validation256–999. Build the source-only diverse
graph and source-only spherical coarse index exactly as V256: graph
M32/M0 64/ef_construction128/eight workers; coarse K391, two
assignments per row, seed254, six Lloyd iterations. One fixed query
arm runs the V256 hybrid: PQ-cosine graph ef4096/FP16 shortlist4096
returns k100, top32 coarse lists supply PQ top8192, and their union is
FP16-reranked to a hybrid k100. Separately, V251 exact FP16 graph
ef2048 returns k100. Union the **two final k100 lists** and FP16-rerank
their distinct IDs to k100. The two navigation paths run sequentially
in this falsifier; no concurrency or latency projection is assumed.
No ef/probe sweep, query-trained links or fallback.

Pass requires authenticated source/graph/coarse identities, graph
reachability and degree gates; mean exact R@100≥0.995 and p05 hits≥98
in each split, combined≥99,800; loaded eight-worker in-process
p95≤80 ms, p99≤95 ms, ≥100 QPS, peak serving RSS≤300 MiB, zero
query vector-body GETs. Report same-sample loaded p50/p90/p95/p99,
sequential timings, PQ and FP16 navigation score work separately,
distinct union size, build/preparation costs and Spot elapsed cost.
Seal IDs before reading exact truth. If it passes, freeze one 1M
dual-navigation arm using the same ef, probe, shortlist and rerank
policy, then require 1M exact quality and resource gates before HTTP.
If it fails, stop this dual method and choose a materially different
candidate-generation format. This 100k in-process gate cannot be a
product-latency or vendor win.

One `causality` c7i.4xlarge Spot attempt, immutable source archive and
S3 prefix, interruption discard/restart, terminal SHA-256 readback,
artifact size/hash replay and immediate instance termination.

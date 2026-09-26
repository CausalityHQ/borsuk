# V260 CoHere 100k: two graph scorers, no coarse route

V259 failed its frozen loaded p95 gate by 2.909 ms. The V259 coarse
route cost p95 17,254 PQ row scores, yet its final dual-candidate
union had p95 101 distinct IDs. A post-terminal replay of the
**closed** V250 PQ graph and V251 exact FP16 graph final k100 lists,
FP16-reranked on the authenticated first100k plane, returned
99,948/100,000 exact GT100 hits. This diagnostic was neither timed
nor a frozen serving result. At first1M, the same two closed graph
final lists have a 99,733/100,000 **candidate ceiling only**. V260
tests the simpler graph-only candidate format before another1M run.

Dataset: CoHere-large-10M canonical first100,000 source rows, D768
cosine k100, authenticated prior-used test ordinals0–999;
development0–255, validation256–999. Build the V250 source-only
diverse graph M32/M0 64/ef_construction128/eight workers, PQ64 books
and resident FP16 plane. Fixed query arm: navigate the graph with
PQ-cosine ef4096, FP16 shortlist4096→k100; navigate the **same** graph
with exact FP16 ef2048→k100; union their two final k100 lists and
FP16-rerank distinct IDs to final k100. Paths execute sequentially.
No coarse lists, global PQ scan, extra graph, ef sweep, query-trained
parameters or fallback. Seal IDs before reading exact F32 truth.

Pass requires authenticated source/graph/request/truth identities,
complete graph reachability and prior degree gates; mean exact
R@100≥0.995 and p05≥98 in each split, combined≥99,800/100,000.
Loaded eight-worker in-process p95≤75 ms, p99≤90 ms, throughput≥110
QPS, peak RSS≤300 MiB and zero query vector-body GETs. Report
same-sample loaded p50/p90/p95/p99, sequential timings, separate
PQ/FP16 navigation score counts, final distinct union size, build
costs, Spot elapsed cost and all artifact hashes. These are not
product HTTP timings. If all gates pass, freeze one same-method 1M
quality/resource cell before any HTTP promotion. If latency or quality
fails, stop this two-graph-scorer series and choose a different index
or representation format rather than tuning ef or adding routes.

One `causality` c7i.4xlarge Spot attempt, immutable source archive and
S3 prefix, interruption discard/restart, terminal hash readback,
artifact size/hash replay and immediate termination.

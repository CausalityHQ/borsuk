# V257 CoHere first 1M: frozen hybrid quality and resource falsifier

V256's graph plus coarse-PQ union passed CoHere-100k quality and
in-process resource gates. V255's graph alone failed CoHere-1M exact
quality. **Decision question:** Does the unchanged hybrid method repair
that 1M recall loss with bounded work? This is an intermediate quality
and resource falsifier before the more expensive persisted HTTP gate.
It cannot itself establish product or vendor service latency.

Dataset: CoHere-large-10M canonical first1,000,000 source rows, D768
cosine, k100, authenticated test query ordinals0–999; development
0–255 and validation256–999 were previously used. Build the same
source-only diverse graph M32/M0 64/ef_construction128/eight workers,
PQ64 books/codes and FP16 plane as V255. Train the same V254 source-only
spherical coarse policy at N=1M: K=ceil(N/256)=3907, seed254, six
Lloyd iterations, two nearest-list assignments per row. For every
query, graph PQ-cosine ef4096 with FP16 shortlist4096 returns k100;
probe top32 coarse lists, PQ-rank unique rows to8192; union the graph
k100 with those8192, FP16-rerank the distinct union to final k100.
No query/truth-trained parameters, ef/probe sweep, alternate arm or
fallback. Seal returned IDs before exact F32 GT100 is read.

Pass requires authenticated source/graph/coarse identity, full graph
reachability, minimum in-degree four and maximum degree≤256. Each
query split mean R@100≥0.995 and p05 exact hits≥98, with combined
hits≥99,500/100,000. Loaded eight-worker in-process p95≤60 ms,
p99≤70 ms, throughput≥160 QPS, p95 total PQ scores≤150,000
(≤15% of N), peak serving RSS≤3 GiB and zero query vector-body GETs.
Report p50/p90/p95/p99 from the same sealed loaded per-query samples,
sequential raw timings and per-query work. The algorithm itself
limits FP16 row scores to12,388/query. Graph build≤2,400 s, peak
builder RSS≤10 GiB; coarse preparation≤1,800 s, peak≤12 GiB.
The builder time cap is newly frozen above V255's verified 1,951.5 s;
V255's old 1,800 s failure remains a failure. Record separate source
preparation and exact-truth resources, Spot quote and elapsed cost.

If all gates pass, freeze these exact V257 artifact identities for a
persisted end-to-end HTTP first/repeat campaign with client-observed
p50/p90/p95/p99, recall parity, throughput, bytes/GETs, cache state,
RSS and cost, followed by a direct same-dataset S3 Vectors comparator
if access/cost permit. A later serving-code revision must be disclosed;
do not present separate revisions as one same-revision product result.
If the quality or work gate fails, stop hybrid promotion and make a
material algorithm/format decision at 100k before another 1M run.
Keep 10M closed until 1M exact quality and HTTP resource gates pass.

One `causality` c7i.4xlarge Spot attempt, immutable source archive and
S3 prefix, terminal SHA-256 readback, interruption discard/restart,
immediate termination. Do not inspect incomplete measurement files.

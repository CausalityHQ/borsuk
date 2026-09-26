# V253 CoHere 100k: global PQ candidate representation gate

**Decision question:** Can source-trained PQ64 cosine choose a candidate
set that passes exact GT100 quality with ≤10,000 FP16 row scores, without
the V250 graph's costly traversal? V248's every-tenth-query diagnostic
found 9,996/10,000 GT100 IDs in the global PQ top8192; that sample is
insufficient for the full frozen 1,000-query gate. V251's smallest
quality-passing graph arm scored 14,508 FP16 rows at p95. V252 changed
entry routing and gained only one GT100 hit at the smaller beam.

On the authenticated V248/V250 CoHere-large-10M first100k D768 cosine
source, score **all 100,000 PQ codes** per query by reconstructed cosine,
select the global best 8,192 with row-ordinal tie break, and rerank those
8,192 rows by authenticated FP16 cosine for k100. Freeze that one arm.
The full PQ scan is explicitly charged and timed; it is an O(N) 100k
representation falsifier, not a proposed 100M serving architecture.
PQ books/codes, FP16 plane, map, 1,000-query panel and independently
computed exact GT100 truth must match V248–V252 by SHA-256. The old-to-new
physical map is inverted before FP16 reranking. Keep the same loaded
eight-worker conditions and conservative memory accounting, including
the V250 graph even though this path does not query it. Rebuild that
graph and require its prior SHA-256 as a paired-source guard.

Development is ordinals 0–255, validation 256–999; both previously used.
Seal returned IDs before scoring. Require each split mean R@100≥0.995
and p05 hits≥98, p95 FP16 rows≤10,000, loaded p95≤35 ms, peak RSS
≤256 MiB, and zero vector-body GETs. Record p50/p90/p95/p99, QPS,
PQ rows scored per query, FP16 rows, RSS and build/preparation resources.
Loaded timings are in-process, not vendor-service latency. If quality
passes but full-scan latency fails, the next design must partition PQ
codes and retain candidate completeness. If quality fails, revise PQ
representation before coarse routing. Neither outcome promotes 10M
without a new bounded-route and 1M end-to-end gate.

One `causality` c7i.4xlarge Spot attempt, immutable reservation/source
archive, narrow PQ cosine unit test, closed terminal with artifact
SHA-256 readback, interruption discard and immediate termination.

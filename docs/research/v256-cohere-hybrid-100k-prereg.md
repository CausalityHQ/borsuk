# V256 CoHere 100k: graph plus coarse-PQ candidate union

**Decision question:** Can a query-aware coarse PQ route repair the
diverse graph's candidate-coverage misses without a full PQ scan?
V255's CoHere first1M diverse graph failed exact quality despite
passing loaded latency and query-work gates. On the same closed 100k
panel, the union of V250 graph ef4096 and V254 coarse-PQ returned
top100 sets contains 99,902/100,000 exact GT100 IDs, development
25,565/25,600 and validation 74,337/74,400, both p05 99. This is
only candidate-coverage evidence: no hybrid rerank or latency has
been measured.

Freeze one source-only 100k treatment, CoHere-large-10M first100k
D768 cosine, k100, query ordinals0–255 development and256–999
validation, both prior-used. Keep V250's authenticated diverse graph,
FP16 plane, PQ64 books/codes and V254's authenticated K391/two-copy
coarse lists byte-identical. For each query, graph PQ-cosine search
ef4096 with FP16 shortlist4096 returns k100. Independently probe the
V254 top32 lists, PQ-score their distinct rows, retain the best8192.
Take the distinct union of graph's returned k100 and those8192 PQ
nominees, then FP16-rerank the union for final k100. No learned query
parameters, alternate ef, probe-count change or fallback. Seal final
IDs before exact truth. The graph and coarse route can each fail
alone; this gate measures their fixed union rather than retuning
either one on the used panel.

Pass requires each split mean R@100≥0.995 and p05 GT100 hits≥98,
combined hits≥99,800/100,000; p95 total PQ row scores (graph visits
plus distinct coarse rows)≤60,000; at most12,388 FP16 row scores per
query (4096 graph rerank plus≤8292 union rerank); loaded eight-worker
in-process p95≤35 ms and throughput≥250 QPS; peak serving RSS≤300
MiB and zero vector-body GETs. Report p50/p90/p95/p99 from the same
loaded raw query samples and sequential raw samples, per-query work,
split quality, build and serving resources, and cost. V253 global PQ
top8192 (99,922 hits, p95 19.157 ms but 100k PQ scores/query) and
V250 graph (99,689 hits, p95 13.421 ms, p95 33,637 PQ scores) are
descriptive same-panel baselines from separate Spot attempts.

This 100k pass would authorize one frozen CoHere-1M hybrid end-to-end
serving gate with newly measured exact recall, p50/p90/p95/p99,
throughput, bytes/GETs, RSS, build time and cost. It would not prove
1M quality or a vendor win. On failure, stop the graph/coarse union
branch and make a different algorithm/format decision. No 10M run
before a 1M quality and serving pass.

One `causality` c7i.4xlarge Spot attempt, immutable source archive and
S3 prefix, closed terminal and all artifact SHA-256 readback,
interruption discard/restart, immediate termination. No overlapping
benchmark instance. Query/truth data are excluded from construction.

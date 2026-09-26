# V251 CoHere 100k: exact FP16 graph navigation

**Decision question:** Can exact FP16 cosine navigation on the V250 diverse
graph meet the same quality target while scoring fewer base rows? V250 passed
development and validation R@100 at ef4096 but its p95 33,637 base scores
failed the frozen 10,000-row work gate. PQ reconstruction may misorder graph
frontier expansion; this experiment changes only the navigation score.

Rebuild the identical V250 graph from the same CoHere-large-10M first 100k
D768 cosine F32 rows with M=32, M0=64, ef_construction=128, alpha=1.2 and
eight workers. Require its SHA-256 to equal V250's authenticated
`688941c7c61c89a39739909af14cb2b4a935a7a4967a168f9503ac34e170d0e7`.
Require V248-identical F32 source, FP16 plane, PQ books/codes, physical map,
1,000 queries and exact GT100 truth by SHA-256. Use the same generation and
authenticated single-root loader. PQ remains loaded for a conservative RSS
comparison, but navigation and output ranking use FP16 cosine; no vector GETs.
The comparator is V250 ef4096 on exactly this graph and panel. The change is
general to cosine graph search, with no CoHere-derived training or links.

Freeze ef arms 512, 1024, 2048, each with k=100. Seal all returned IDs before
scoring. Development is query ordinals 0–255 and validation 256–999; both
were previously used. Choose the smallest arm with development mean R@100
≥0.995 and p05 GT100 hits≥98; then require validation mean≥0.995 and
p05≥98 without retuning. For that arm require p95 base-row scores≤10,000,
eight-worker loaded p95≤35 ms, peak serving RSS≤256 MiB and zero vector
body GETs before scale promotion. Report all arms' exact hits, visited rows,
p50/p90/p95/p99 in milliseconds, QPS, RSS, build time/RSS and preparation
time/RSS. Timings are in-process, not service or vendor latency.

One `causality` c7i.4xlarge Spot attempt, immutable source and reservation,
closed terminal with artifact SHA-256 readback, interruption discard and
immediate compute termination. If quality passes but base-row work does not,
keep 10M closed and choose a materially different coarse-routing architecture
after a cheap 100k falsifier. If quality fails, reject FP16 navigation on this
graph as a scale path. Lean can prove exact top-k over visited candidates and
termination under bounded graph assumptions; recall and latency remain measured.

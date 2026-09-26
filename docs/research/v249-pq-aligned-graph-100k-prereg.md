# V249 CoHere 100k: align graph topology with PQ navigation

**Decision question:** V248 failed its CoHere first-100k D768 cosine
quality gate at every frozen search arm. Does constructing the same
owner-partitioned graph from the PQ reconstructed directions used at
search time close the quality gap without a material resource regression?
The V248 exact-source topology and its complete raw evidence are the
paired baseline, terminal SHA-256
`9e45f55216518fec13bba3c40002aedb8d356ce8fe1fa943d6ac4caf9ac817f6`.

Keep **identical** authenticated first-100k source rows, source-only PQ64
books/codes, FP16 plane, physical map, CoHere test queries, exact float64
GT100, M=32, M0=64, ef_construction=128, eight graph builders, and the
2048/2048, 4096/4096, 8192/8192 serving arms. Reject the attempt if
the prepared source, plane, books, codes, map, query panel or exact truth
hash differs from V248. Build topology from PQ codeword reconstructions
normalized to cosine; continue exact FP16 reranking. This changes the
graph geometry, not the compression, corpus, query panel, or serving
algorithm. Seal all returned IDs to S3 before scoring truth.

Score development queries 0–255 and validation 256–999 separately,
using the same already-used CoHere panel. Select the smallest arm whose
development mean R@100≥0.995 and p05 hits≥98; require the same on
validation without retuning. Require reachable100,000/100,000, minimum
in-degree four, maximum degree≤256, graph build≤120 s and≤1.5 GB peak
RSS. For the selected arm, require eight-worker in-process loaded p95
≤35 ms, process peak RSS≤256 MiB, and zero query vector-body GETs.
Report all arms, p50/p90/p95/p99, QPS, source/PQ preparation time and
RSS separately, and paired per-query hit changes against V248. The
latency numbers are in-process and not product or vendor service claims.

Use one new `causality` c7i.4xlarge Spot attempt, immutable source
archive/reservation and instance identity. Discard an interrupted cell,
authenticate complete terminal artifacts, and terminate compute. A
pass permits a 10M CoHere scale gate at the same frozen method revision,
with RAM provisioned from the measured per-vector planes and recall
work. A fail rejects PQ-aligned topology and requires a material
navigation/format decision at 100k before any 10M run. No further ef
sweep is authorized by this preregistration.

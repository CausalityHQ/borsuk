# V250 CoHere 100k: diverse graph links

**Decision question:** can source-only link diversity repair the anti-hub
access failure observed after V248 and V249? V249 changed graph vectors
to PQ reconstructions but gained only six GT100 hits. On the identical
100k CoHere D768 cosine panel, its ef8192 search scored 50,212 base
rows at p95 and returned 99,254/100,000 GT100 hits, below the frozen
≥99.5%/p05≥98 quality gate. A read-only audit of its authenticated
graph, raw IDs and truth found that 91.8% of missed GT IDs had
in-degree≤8, versus 0.49% of retrieved GT IDs. This supports an
access-path problem but does not by itself prove the specific cause.

The builder currently keeps nearest candidates and evicts reverse links
by nearest distance. Change only link selection: during insertion and
reverse-edge overflow, consider source-cosine candidates in distance
order; keep a candidate unless an already-kept link is closer to it
than the owner is, using alpha=1.2 (factor 1.44 on squared distance).
Fill unused degree slots from deferred nearest candidates. Keep the
existing one-pass deterministic owner-partitioned build, exact F32
source directions, M=32, M0=64, ef_construction=128 and eight workers.
No query or truth data enters construction. The search and exact FP16
reranker remain unchanged. This is one material graph construction
change, not an ef sweep.

Reproduce V248's source, FP16 plane, PQ books/codes, map, query panel
and independently computed exact 100k GT100 byte for byte; reject an
identity mismatch. The comparator is V248's exact-source graph, with
V249 included as negative evidence. Freeze ef/shortlist arms
2048/2048, 4096/4096 and 8192/8192. Seal all returned IDs before
scoring. Development is queries 0–255, validation 256–999; both are
previously used panels. Select the smallest arm whose development
mean R@100≥0.995 and p05 hits≥98, then require the same on validation
without retuning. Require the selected arm's p95 base-row scores≤10,000,
eight-worker loaded p95≤35 ms, serving peak RSS≤256 MiB and zero
query vector-body GETs before scale promotion. Record p50/p90/p95/p99,
QPS, per-query hits and visited rows for all arms. Loaded timings are
in-process, not service or vendor latencies.

The structure must reach all100,000 rows, minimum in-degree four and
maximum degree≤256. The build falsification limit is≤300 s and≤1.5 GB
peak RSS; report source/PQ preparation separately. Also report the
fraction of all nodes and missed GT IDs with in-degree≤8. A quality
pass with more than10,000 p95 base scores is a scalability miss: keep
10M closed and revise navigation/work accounting at100k. A quality
failure rejects this one-pass diverse graph and requires a different
navigation or coarse-routing architecture. One `causality` c7i.4xlarge
Spot attempt, immutable reservation/source archive, full terminal
artifact authentication, interruption discard and immediate termination.

Lean may prove the degree bound, deterministic tie/order behavior,
termination and top-k correctness over the candidate set under stated
assumptions. It cannot turn this fast batched graph plus PQ navigation
into a proof of R@100 or measured p95; those remain empirical gates.

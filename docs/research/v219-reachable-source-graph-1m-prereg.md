# V219 reachable source graph ReLAION-1M validation

V218 passed the frozen ReLAION-100k D768 gate: all 100,000 rows
reachable, minimum in-degree four, 99,771/100,000 GT100 hits at
ef/shortlist 2,048/2,048 versus paired V215/V216 99,521, p05 99,
loaded eight-worker p95 6.915 ms, RSS 212,135,936 bytes and zero
vector-body GETs. Its closed terminal SHA-256 is
`cf44ecb95a9d1cc07ac895a222a407c37db090c499f15a887e887d74c8492efd`.
V219 is one validation of that same source-only construction policy at
1M; it does not tune graph degree, topology or PQ books to labels.

Freeze ReLAION-1M D768, validation query ordinals 0–999 already used
in earlier BORSUK work, k=100. Reuse the V217 authenticated source,
source-only physical order, PQ64 books/codes, FP16 plane, requests and
V198 paired GT100 witness. Build the graph with M=32, M0=64 and
ef_construction=128. Add the directed physical-order cycle and repair
incoming base edges to floor four by the V218 policy. Abort on a node
with more than 256 outgoing edges or any structural violation.

Measure exactly the four V217 search arms (ef/FP16-shortlist):
2,048/2,048, 4,096/4,096, 8,192/8,192, 16,384/16,384. No arm,
threshold or graph parameter may be selected after truth opens. Use
PQ64-reconstructed cosine to navigate and the bound FP16 plane to
rerank. Run sequential and loaded eight-worker whole in-process
queries; report p50/p90/p95/p99 from the same 1,000 samples for each
arm, completed QPS, p95 base visits, graph/plane/PQ/workspace bytes,
peak RSS, build time and RSS, artifact bytes and zero vector-body
GETs. Seal every returned ID in S3 before downloading GT100/V199.

The structural gate is **1,000,000/1,000,000 reachable**, minimum
in-degree four, no invalid/self/duplicate edge and maximum outgoing
degree at most 256. A serving arm passes only if it reaches at least
**99,605/100,000 GT100 hits** and per-query p05 at least **98**, the
paired V199 quality floor, while loaded p95 is below 92.23 ms, loaded
p99 below 137.87 ms, eight-worker completed throughput at least 100
queries/s, peak serving RSS at most 3 GiB and zero vector-body GETs.
The latency values are internal advancement ceilings taken from a
different V199 serving scope; they do not create a matched product
comparison. Report all arms even if none pass. The smallest passing
arm by ef is the frozen candidate for the next gate.

Use one `causality` c7i.4xlarge Spot attempt in eu-central-1c, with
source archive and terminal identity, 10,800 second wall cap, sealed
raw IDs, final artifact hashes and immediate instance termination.
An interruption discards the entire cell; restart only under a new
attempt ID after confirming the original is terminal or terminated.

If a V219 arm passes, move directly to one same-revision, same-panel
end-to-end cold/no-cache serving comparison including recall@k,
p50/p90/p95/p99 from raw per-query samples, concurrency, throughput,
bytes/GETs, RSS, hardware, region and compute/storage cost. Direct
S3 Vectors measurements must match split, k and cache state;
Turbopuffer published cold numbers are dated context unless direct
matched access is available. No 100M memory cap or vector-count knee
is inferred from this 1M gate; memory is reported as a function of
the graph, planes and workspaces actually resident. If V219 fails
quality, reject this representation and choose one material graph or
score-format redesign on a cheaper 100k falsifier before another 1M
attempt.

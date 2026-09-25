# V217 frozen source graph/PQ-cosine/FP16 ReLAION-1M gate

V216's frozen ef=2048/FP16-shortlist=2048 method passed paired
ReLAION-100k D768 method-held-out quality, p95 and memory gates.
Evaluate the same source-trained cosine HNSW, PQ64 cosine navigation,
generation-bound resident FP16 rerank and once-bound row mapping on
ReLAION-1M D768 **validation-1000, already used**. This is one
source-only build and one frozen serving campaign, not iterative
per-dataset tuning. The corpus/recall work ladder doubles search and
shortlist together: (2048,2048), (4096,4096), (8192,8192),
(16384,16384). The first arm transfers the 100k setting; the others
are generic higher-recall choices with proportionally higher query
work. The graph construction remains M=32, M0=64,
ef_construction=128. Report all arms and select the lowest-work one
that passes; do not alter them after opening truth.

Authenticate the historical source Parquet, old physical order,
V164 relaid order, V115 source PQ64 books/codes, V196 FP16 plane and
V116 query requests by complete SHA-256 and exact geometry. Build
the graph from float32 source vectors in the FP16 plane's physical
order; verify every stable ID and FP16 row against source before
serving. Persist a versioned, generation-bound graph and read it in
a fresh serving process. Validate the old/new PQ row-map permutation
once, then give each of eight serving workers an epoch-marked search
workspace. Measure sequential p50/p95/p99 and completed QPS per arm,
plus eight-worker loaded p50/p95/p99 and QPS. Record p95 base visits,
graph build wall/peak RSS, cold hydration, peak serving RSS, resident
plane/graph/PQ/map/workspace bytes, source artifacts, and vector-body
GETs. Round-robin sequential arm order by query ordinal to reduce
cache-order bias. Raw returned IDs for all arms are sealed to S3
before V198's closed GT100/V199/V155 witness is downloaded.

The paired quality gate is ≥**99,605/100,000 GT100 hits**, V199's
same-query strongest BORSUK returned result, and per-query p05≥98.
The internal performance/resource gate is eight-worker loaded
p95<92.23 ms and p99<137.87 ms, completed throughput≥100 QPS,
peak serving RSS≤3 GiB, and zero vector-body GETs. V199's timing
contains live S3 transport, SQ8 and FP16 but excludes routing and
planning, so these are conservative advancement ceilings, **not a
like-for-like latency win**. Report sequential whole-request timings
separately. V155's 99,567 hits and V209's 96,813 hits are additional
same-panel context, with their distinct latency scopes disclosed.

The 3 GiB cap is this 1M cell's generation and eight-worker envelope;
it is not a 100M vector-count knee. For N rows, D dimensions and W
workers, the admitted resident components grow as FP16
N(8+2D), PQ64 64N, reconstructed norms 4N, physical map 8N,
visit marks 4NW and source graph adjacency O(N·degree), plus
bounded query heaps and allocator overhead. Higher recall may choose
a larger ef/shortlist and memory admission; no hard row threshold is
part of the method. These are conditional resource bounds, while
actual recall and latency require this measurement.

Run one Causality c7i.4xlarge Spot cell with a 3-hour hard stop,
discard interrupted measurements and restart at a new attempt,
sync terminal artifacts and hashes, and terminate at terminal.
Monitor incomplete work by terminal/infrastructure health only.
A pass authorizes production generation/API integration and a matched
end-to-end Turbopuffer/S3 Vectors comparison. A fail requires one
algorithm/format decision from measured loss, not another V20x
planner micro-optimization. No 100M or external-product claim follows
from this cell alone.

# V213 source-built resident vector graph 100k gate

V212's fixed neighborhood/PQ/FP16 kernel passed paired ReLAION-100k
quality and missed its 10 ms whole-kernel p95 gate. Test one materially
different index: build a navigable cosine graph over authenticated source
vectors in physical row order, release the float32 build vectors, and
navigate with scores from the matching resident FP16 generation. Do not
expand the V20x locality/PQ series. Use the same already-used ReLAION-100k
D768 development queries 0–255 and paired V193 full-rank SQ8 baseline.

Freeze HNSW construction at M=32, base degree=64, ef_construction=128.
Test ef_search=256, 512, 1024, and 2048, returning 100 stable IDs per
query. These are generic recall/work settings, not row-count thresholds or
dataset-fitted coefficients. For each arm record returned GT100 hits,
per-query p05 hits, whole Rust in-process p50/p95/p99, sequential QPS,
base-layer visited nodes, graph/plane resident bytes, peak serving RSS, graph-build
wall/RSS, cold hydration, and zero vector-body GETs. Record generation
identity and graph source hash. Seal all raw IDs to S3 before downloading
truth or V193 output. The first qualifying arm is the smallest ef with
aggregate hits at least V193's paired 25,440/25,600, p05>=98,
whole-query p95<=10 ms, and zero vector-body GETs. No arm is selected
using the truth after this gate; a pass requires a fresh-query check
before 1M promotion.

Run one Causality Spot cell with a distinct attempt prefix. The worker
authenticates inputs and source archive, writes a terminal receipt with
artifact hashes, and terminates at the terminal marker. An interruption
invalidates that cell; restart under a new attempt. Read only terminal
health until complete; verify every artifact hash after termination.
No 1M result, network latency, competitor latency, or 100M bound follows
from this 100k in-process experiment. A pass authorizes a production
format and fresh-query gate; failure rejects this graph configuration
and forces a production architecture decision from measured bottlenecks.

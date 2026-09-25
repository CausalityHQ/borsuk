# V214 compact-navigation resident graph 100k gate

V213 closed with 25,471/25,600 GT hits and p05 98 at ef=2048,
but 42.593 ms whole in-process p95 and 22,393 dense FP16 base-layer
visits. Test one material format change: persist its source-built graph,
then launch a fresh process that navigates on source-trained PQ64 codes
and reranks a bounded candidate list against the generation-pinned
resident FP16 plane. This avoids D768 FP16 scores during navigation
and releases the graph builder's float32 heap before serving.

Use the same authenticated ReLAION-100k D768 source, V113 PQ64
books/codes, V163 physical order, V212 FP16 plane, and already-used
development query ordinals 0–255. Construction remains cosine HNSW
M=32, M0=64, ef_construction=128. Freeze these generic
(ef_search, FP16 shortlist) settings: (1024,1024), (2048,1024),
(2048,2048), (4096,1024), and (4096,2048). The graph file has a new
version marker and binds to source, plane and generation identities;
the fresh serving process authenticates its SHA-256 and validates
all edges before query traffic.

For each arm measure GT100 hits, p05 hits/query, full in-process Rust
p50/p95/p99, sequential QPS and p95 base-layer visits. Also record
graph build wall/RSS, cold graph+plane/PQ hydration, serving RSS,
graph/PQ/plane bytes, and vector-body GETs. Seal all raw returned IDs
to S3 before downloading truth and paired V193 output. The frozen
gate is ≥25,440 GT hits (V193 full-rank SQ8 on these 256 queries),
p05≥98, whole p95≤10 ms, serving RSS≤320,000,000 bytes, and zero
vector-body GETs. Select the lowest-cost qualifying arm by whole
p95, then ef, then shortlist; a pass requires fresh-query confirmation
before 1M promotion. The 320 MB cap is for this 100k cell's measured
serving process, not an arbitrary 100M row-count knee.

Run one Causality Spot attempt. Authenticate source archive and inputs,
discard an interrupted cell, sync a terminal artifact/hash roster, and
terminate compute at the terminal marker. Monitor only terminal and
infrastructure health until complete. These measurements are in-process,
not end-to-end product latency or matched Turbopuffer/S3 Vectors evidence.

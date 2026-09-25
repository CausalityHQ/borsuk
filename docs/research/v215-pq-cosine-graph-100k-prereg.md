# V215 metric-consistent PQ graph 100k gate

V214's persisted source graph, source PQ64 codes and resident FP16
plane made p95 and serving RAM feasible, but its squared-L2 PQ
navigation missed paired cosine quality by 10 aggregate GT100 hits
and one p05 hit/query at ef=2048, shortlist=2048. Test one generic
metric correction: reconstruct each PQ row's direction from its
source-trained codewords, prepare row norms once, and navigate on
approximate cosine. Keep the same source graph, physical mapping,
FP16 plane, and five frozen V214 (ef, shortlist) arms exactly:
(1024,1024), (2048,1024), (2048,2048), (4096,1024), (4096,2048).
No parameter is fitted to truth or changed for ReLAION.

Reuse the complete, independently read-back V214 graph/plane/PQ/map
artifacts with their exact SHA-256 identities. Use ReLAION-100k D768
already-used development queries 0–255 and the paired V193 full-rank
SQ8 baseline. For each arm record GT100 hits, p05 hits/query,
whole in-process Rust p50/p95/p99, sequential QPS, p95 base visits,
serving peak RSS, PQ cosine sidecar bytes, hydration and zero vector
body GETs. Seal raw returned IDs to S3 before truth and V193 download.

The frozen gate is hits≥25,440/25,600, p05≥98, whole p95≤10 ms,
serving peak RSS≤320,000,000 bytes, and zero vector-body GETs.
Choose the passing arm by lowest p95, then ef, then shortlist. A
pass only authorizes a fresh-query gate; it is not 1M or end-to-end
product evidence. Failure rejects the existing source-PQ64 codebook
as a high-recall graph navigator at this quality/work point.

Run one Causality Spot cell. An interruption discards the cell and
requires a new attempt. Sync terminal artifacts and hashes to S3,
terminate compute at the terminal marker, and monitor incomplete work
through terminal and infrastructure health only.

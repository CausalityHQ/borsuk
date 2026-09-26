# V262 CoHere first1M: authenticated dual-graph HTTP gate

V261 passed the frozen first1M exact quality and loaded in-process
resource gates. **Decision question:** Does the same dual-graph method
retain exact ID parity and useful client-observed latency after
authenticated object-store publication, cold startup hydration and
VPC-peer HTTP? This is a product serving gate, not a direct vendor
comparison.

Dataset: CoHere-large-10M canonical first1,000,000 source rows,
D768 cosine, k100, authenticated prior-used test ordinals0–999,
development0–255 and validation256–999. Source/graph/plane/PQ/map
come from the closed V261 `a0001` terminal SHA-256
`00c7d4803354f15d5f62bea6e53fd0672a0f5fc91cc482e1fa4b20b8951a9f82`.
The frozen generation root SHA-256 is
`1e483859b96f5270209678e0f76f7cc9e26a80162a24c7fa48a942cf010ec92e`.
Publish the five authenticated blobs under a fresh conditional S3
generation head, read back the root and all blob identities, then
hydrate an empty server cache before queries. Its 8 workers use the
V261 method: PQ-cosine graph ef4096/FP16 shortlist4096→k100, exact
FP16 graph ef2048→k100, final FP16 union rerank. One client in the
same AZ uses eight persistent HTTP/1.1 connections in fixed ordinal
order, first then immediate repeat; no response cache. Seal each
pass's 1,000 raw ID and timing records before loading reference IDs
or exact GT100 truth. Both passes must return **all 1,000 ordered V261
ID lists exactly**, including 99,717/100,000 GT100 hits (development
25,511, validation74,206, p05 98/99).

Each pass must have client p95≤150 ms, p99≤175 ms and completed
throughput≥65 QPS from its own sealed per-query samples; report
p50/p90/p95/p99, request/response bytes, peak server RSS≤3 GiB,
zero query vector-body GETs, startup blob GET count and response
bytes, cache state, hardware/region and complete Spot compute cost.
Expected blob bytes from the root sum to1,880,914,634 B; require
exactly five cold blob GETs and those bytes, with metadata GETs
reported separately. One first/repeat cell, no parameter sweep or
fallback. This resident-after-hydration VPC HTTP service is not
transport/cache matched to regional S3 Vectors HTTPS, and no
Turbopuffer tenant result exists. Do not claim a vendor win from it.

If both passes qualify, make the production-code decision from this
frozen V262 serving revision and proceed to a separate direct CoHere S3 Vectors
comparison under disclosed cache and transport semantics, then a
10M scale gate. If HTTP parity or resources fail, repair the
responsible production layer and rerun only the failed frozen gate;
do not modify ANN search parameters to mask a serving defect.

V262 uses V261's exact five artifacts and search parameters, with a
new production loader and HTTP server revision. The first and repeat
passes share that V262 revision; in-process V261 timing remains
historical and is never added to HTTP timing.

One two-host `causality` c7i.4xlarge Spot cell in eu-central-1c,
immutable source archive, exact instance identity, interruption
discard/restart for the entire cell, terminal hash/readback for both
hosts, immediate termination. Monitor incomplete work by terminal
markers and infrastructure health only.

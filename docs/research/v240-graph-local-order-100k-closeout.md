# V240 graph-local order 100k closeout

**Decision: stop the sparse remote SQ8 layout branch.** One query-blind
reverse Cuthill-McKee order over the authenticated reachable graph made
the page-read lower bound worse. At the V239 quality-passing 1,024-row
shortlist, the V163 k-means order touched 147 pages at p95; the graph
order touched **218 pages**, or **43,530,240 minimum bytes**, against
the frozen 16,777,216-byte budget. No further physical-order parameter
search is justified by these two failed source-only layouts. Continue
from V237's authenticated resident FP16/PQ graph serving line and address
scalable graph construction and actual memory at larger N.

The sole `causality` c7i.4xlarge Spot attempt `a0001` ran on
`i-006a7880452e0ef7e` in `eu-central-1c`; it is **terminated**.
Frozen source `1e3055819e8303b92cb68793f6fd1a819e844884`, archive
SHA-256 `42a97c51c1d86f025c6ba178528c1a4fda11848577069a23f35c8402c272353a`.
Terminal SHA-256 `fc9d65accf50003c064f41942121fdcd49395e67004ddae2f8bb26758b5bc1ab`,
closeout SHA-256 `fafebde2150f3e83d044486a7380af3fb622a79892ce75d06e09c0142f7b9042`.
The terminal reports complete, exit zero. All eight artifact sizes and
SHA-256 digests passed replay. The layout seal SHA-256
`eaed57bbd2d652afcc223ac50ba894a1db9167ce6715431c290596c7ff9bcd6b`
was uploaded **before** V239 candidates were fetched. The order file
SHA-256 is `9cd257f9bb507c95ad04bc352ce776d9ca5b1f1bf5fd34578196a13183b64338`.
The launcher independently replayed all page counts from the sealed
V239 raw stream and this order. Immutable evidence:
`s3://borsuk-bench-453182569524-euc1/research/v240-graph-order-100k/1e3055819e8303b92cb68793f6fd1a819e844884/runs/a0001/`.

Dataset: **ReLAION-100k D768**, cosine k100; prior-used development
0–255 and method-held-out 256–999, same 1,000 sealed V239 queries.
The V239 graph's 1,024 candidate list contained 99,894/100,000 GT100
IDs (p05 99); V240 changed only physical row order and did not rescore
vectors or change IDs. Distinct 256-row SQ8 page counts from the same
1,000 query samples:

| Layout at 1,024 candidates | p50 | p90 | p95 | p99 | p95 minimum bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| V163 k-means source order | 75 | 129 | 147 | 173 | 29,352,960 |
| V240 graph RCM order | 155 | 205 | **218** | 235 | **43,530,240** |

These bytes are full-page lower bounds, not observed S3 transfer.
GET counts, returned SQ8 recall, end-to-end latency and competitor
performance were not measured. Compute was estimated at **$0.00445**
to terminal, excluding S3, EBS and billing tail. The instance was
terminated before independent local replay.

Next single gate: retain V237's working resident architecture and test
one source-only sharded graph build/search construction at 100k before
any 10M/100M run. It must preserve all-node reachability, paired GT100
quality and a bounded build-memory model. Scale memory from row count,
precision needed for the recall target and active generation count, not
from a hard vector-count switch. No 1M/10M promotion follows from V240.

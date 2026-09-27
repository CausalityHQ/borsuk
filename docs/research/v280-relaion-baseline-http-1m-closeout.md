# V280 ReLAION-1M baseline HTTP serving closeout

**Frozen decision: pass the serving gate.** The shipped Rust generation route
served the unchanged V279 baseline with exact ID-list parity and measured
client p95 near62 ms on both passes. This is a verified BORSUK service result,
not a matched S3 Vectors or Turbopuffer win.

Source commit `a1dea4e668ab53f5e202c60c7baff99315e5a53f`, source archive
SHA-256 `94706590175f24403681558745d463004e5b84f32e4e9ac2b1450faf9321f76c`.
Attempt `a0001` used eu-central-1c c7i.4xlarge Spot server
`i-063b7f30c3a96953b` and same-class client `i-00755fc34cd2c7994`;
both are terminated. Server/client terminal SHA-256 were
`1efc24edf0fdfff552cd2c8d62f7aba869fed09ce28692a0d0814418d56401a2`
and `07654dd5b4e36eeaaacbe8f524eff38f044ff3053e258fcf28c7f269cc83334a`.
Every terminal artifact size/SHA-256 and both sealed raw client files were
replayed by the original launcher. Closeout SHA-256
`6c6255745b65f6c9c4a2e6170dfd9bd18fae3a0aee8a5a7365084e614a1d532d`
was read back. Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v280-relaion-baseline-http-1m/a1dea4e668ab53f5e202c60c7baff99315e5a53f/runs/a0001/`.

Dataset ReLAION-1M, D768 cosine, k100, historically used validation
ordinals0–999. The server published and authenticated V279 baseline root
SHA-256 `bdc994e1f58c817ebfd95c7ddaa3aca37fe182f7dc73a569a61ad39a73ec2565`.
The client used the V279 normalized FP32 query panel and FAISS exact cosine
GT100. Eight persistent HTTP/1.1 VPC-peer connections sent1,000 queries
once and immediately again, with no response cache. Client latency includes
JSON encode, HTTP exchange and decode. Each raw result was independently
re-scored against V279 local IDs and exact truth.

| Measured pass | GT100 hits / 100,000 | Exact local ID-list parity | Client p50/p90/p95/p99, ms | QPS |
| --- | ---: | ---: | ---: | ---: |
| First | 99,989 | 1,000/1,000 | 40.844 / 56.485 / 61.694 / 70.231 | 177.69 |
| Immediate repeat | 99,989 | 1,000/1,000 | 41.019 / 56.998 / 61.285 / 70.529 | 179.82 |

Both passes have R@100=0.99989. First/repeat sealed raw SHA-256 were
`459e4d84d16396edf4af9d2731e32f44ff6fcefd50e033d68d215d25c7014fae`
and `bdfdb3ed0d5654574742f370bd337b7f61b5eef51e3a2ecd7d111e5cc2bcebea`.
Each pass sent16,325,386 request bytes and received738,918 response bytes.
The empty-cache server fetched five authenticated blobs totaling1,879,999,990
bytes in37.073 s. Peak server RSS was2,058,260,480 B. Query-time vector
GETs are zero by construction of the resident serving route; the client
response reports that value, rather than an independently instrumented
object-store counter. The Spot quote was $0.3686 per host-hour; estimated
two-host compute through terminal was $0.04516, excluding storage, data
transfer, index build and cleanup tail. This is not billed lifecycle cost.

V279 local resident Rust baseline p95 was59.362 ms, but it used sequential
in-process queries. V280 measures the product HTTP path under eight-way
concurrency, so these percentiles must not be added or treated as a matched
latency delta. V269 CoHere first1M HTTP and direct S3 Vectors V263 are
different dataset/panel or cache/transport conditions. No authenticated
matched Turbopuffer result exists.

The next single gate is a frozen CoHere10M quality/resource transfer of the
bounded extra reverse-edge option, with memory sized to the graph, vectors
and worker count. V276/V277 100k and V278 1M supported that method on
CoHere; V279 rejected it as a generic default on ReLAION. V272 already
showed the baseline misses its 10M quality gate, so merely repeating it
would not answer a new question. An authenticated HTTP scale run follows
only if the 10M quality/resource transfer passes. Mutation/restart/failure
API checks remain release work, and none of these gates imply a vendor win.

# V226 FP16 graph source, ReLAION-100k

**Decision:** Promote the authenticated FP16 plane as a candidate canonical
source for graph compaction. At 100k it rebuilt the selected V218 graph
byte for byte. Verify that identity at 1M before fixing the source contract.

The sole Causality c7i.4xlarge Spot attempt `a0001` on
`i-060bad327b3467649` is terminated. Source commit
`17f855b7768060c7e4850f9c68aff23c4a459058`, source archive SHA-256
`1a9aefcc07608a543ba4dea93ee77fae5a9c997e5dc749a230e74c2e722ed3a7`,
terminal SHA-256 `24d71d8b97b369c1b50df72ec546b2c467cf58702408e264c982539350e4c32f`.
The complete terminal and all ten artifacts passed length and SHA-256
readback. Immutable evidence is under
`s3://borsuk-bench-453182569524-euc1/research/v226-fp16-graph-source-100k/17f855b7768060c7e4850f9c68aff23c4a459058/runs/a0001/`.

Dataset: ReLAION-100k D768, development queries 0–255 and previously
used method-held-out queries 256–999, k=100, exact GT100. V226 kept
V218's M32/M0=64/ef_construction=128 and PQ ef/shortlist=2,048/2,048.
Only the graph builder's vector source changed from authenticated F32
to dequantized rows of the authenticated FP16 plane.

| Paired result | V218 F32 source | V226 FP16 source |
| --- | ---: | ---: |
| Graph SHA-256 | `d8b70919243a7cd6ecb9448ce23f776374738476c1882cbc6a651fb34753af2f` | identical |
| Development GT100 hits / 25,600 | 25,537 | 25,537 |
| Method-held-out GT100 hits / 74,400 | 74,234 | 74,234 |
| Combined GT100 hits / 100,000 | 99,771 | 99,771 |
| Paired returned-ID list ties / 1,000 | — | 1,000 |
| p05 GT100 hits/query | 99 | 99 |
| Loaded eight-worker p50/p90/p95/p99, ms | 5.750/6.673/6.915/7.380 | 6.619/7.600/7.906/8.397 |
| Loaded completed throughput, queries/s | 1,353.3 | 1,181.6 |
| Serving peak RSS, bytes | 212,135,936 | 211,542,016 |
| Graph build, seconds | 177.702 | 196.353 |
| Graph build peak RSS, bytes | 842,149,888 | 842,162,176 |

Both timing rows are verified whole in-process measurements from
different runs on the same instance type, not paired per-query service
latency. The difference is not attributed to FP16 construction: serving
the bit-identical graph with the same plane, PQ inputs, and query code
cannot causally change query work. The frozen V226 quality, loaded p95,
RSS and zero-vector-GET gates passed. Build resource and serving JSON
hashes are in the terminal.

**Next single source-format gate:** rebuild the selected ReLAION-1M graph
from its authenticated FP16 plane on Spot, compare its artifact SHA-256
to V219's frozen graph SHA-256
`a2805a97c1955adf1cdc0b0da43b4ff205eb1d4646916c09d3c4d2b9f7c1ee0b`.
If identical, V219's sealed returned IDs and quality remain valid for
the rebuilt graph. If different, run a same-revision paired quality
measurement before accepting or rejecting FP16 as the compaction source.

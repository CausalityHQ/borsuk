# V256 CoHere 100k: hybrid candidate union passed

Source commit `1ba78eaf8322ad47b9553ede6f1d9f8848277678`; the sole
`causality` c7i.4xlarge Spot attempt `a0001` ran on
`i-011ad2c77b69a7217` in `eu-central-1c`, now **terminated**.
Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v256-cohere-hybrid-100k/1ba78eaf8322ad47b9553ede6f1d9f8848277678/runs/a0001/`.
Source archive SHA-256
`338b8ae23c9bb3f7052acdbbcc8b2e5d7064b6d47a490cfba070caf97a80ab89`;
terminal SHA-256
`b4ce612b6bf3a4b636b23586fd795c7d418dd669adf3078d9f3e4eedcc3243f6`.
The terminal exited zero. The original launcher replayed every
artifact's size and SHA-256, confirmed EC2 termination, and the narrow
remote PQ unit test passed before measurement. Source F32, FP16 plane,
PQ books/codes, map, graph, centroids, offsets, postings, requests and
exact truth match the authenticated V250/V254 100k artifacts byte for
byte. Raw IDs SHA-256
`4cb0ed720dc2344cfeb0b3621a30cc73c4b7e0dc7d07366410e0e85e457aed62`;
loaded per-query timing SHA-256
`7f9ae76302434de6d57a8474b55416aaa72021276f9169478876340e06ce6e52`.

CoHere-large-10M first100k D768 cosine, k100, prior-used development
query ordinals0–255 and validation256–999. One frozen arm unions
V250 graph ef4096's returned k100 with V254 top32 coarse-list PQ
nominees8192, then FP16-reranks the distinct union for final k100.
The graph itself FP16-reranks shortlist4096. No tuning or fallback.

| Same-panel method | Dev hits / 25,600; p05 | Val hits / 74,400; p05 | Combined / 100,000 | Loaded p50/p90/p95/p99 ms | QPS | p95 PQ scores |
|---|---:|---:|---:|---:|---:|---:|
| V250 diverse graph | 25,508; 98 | 74,181; 98 | 99,689 | 12.709 / 13.274 / 13.421 / 13.717 | 627.1 | 33,637 |
| V253 global PQ | 25,575; 99 | 74,347; 99 | 99,922 | 18.560 / 18.946 / 19.157 / 19.505 | 418.9 | 100,000 |
| **V256 hybrid** | **25,558; 99** | **74,320; 99** | **99,878** | **30.026 / 30.621 / 30.777 / 31.077** | **266.6** | **48,859** |

V256 development mean R@100=0.998359 and validation=0.998925;
combined=0.998780. It passes the frozen ≥0.995/p05≥98 per-split
gate and ≥99,800 combined gate. Independent local replay of all 1,000
sealed IDs against the exact GT100 truth reproduced every count.
Replaying the **same 1,000 loaded raw samples** reproduced all four
loaded percentiles above. Sequential raw p50/p90/p95/p99 was
29.807/30.485/30.637/30.932 ms. Total PQ scores at p50/p90/p95/p99
were 42,914/47,686/48,859/50,498, passing the ≤60,000 p95 gate.
The method has an explicit ≤12,388 FP16-score/query upper bound.
Peak serving RSS was 226,529,280 B (≤300 MiB), with zero query
vector-body GETs; loaded throughput exceeds 250 QPS. Cold hydration
was 328.131 ms. These are loaded in-process results, not HTTP or
vendor service latency.

The graph identity and structure match V250. Its rebuild took
170.193 s at 536,338,432 B peak RSS. Source/PQ preparation took
47.96 s at 1,803,120 KiB; coarse list preparation took 5.34 s at
933,504 KiB, all zero swap. The instance launched 11:04:22 UTC and
terminal landed 11:15:41 UTC (11m19s). At the recorded Spot quote
of $0.3663/hour, compute through terminal is about **$0.0691**,
excluding EBS, S3 and cleanup tail.

**Decision:** V256 qualifies **one** frozen CoHere-first1M hybrid
quality and end-to-end serving/resource gate. Its 100k quality is
near V253's exhaustive PQ baseline while scoring about half as many
PQ rows, but V256 is slower than both V250 and V253 at this size.
The branch remains provisional: V255 showed the graph alone loses
recall at 1M, and V254 showed the coarse route alone loses recall at
100k. Neither their complementarity nor query work is proven at 1M.
No 10M promotion or S3 Vectors/Turbopuffer win follows from this
100k in-process gate. Use the same source-only graph/coarse policies,
fixed top32/8192 and ef4096/4096, and measure exact recall plus
client-observed HTTP latency, throughput, RSS, bytes/GETs and cost
before a production-code decision.

# V265 CoHere 100k: faster scorer, frozen promotion gate missed

**Decision: retain the verified scoring and FP16 validation increment, but
do not promote this arm to 1M.** Cached row norms plus safe SIMD FP32
navigation and FP64 beam reranking cut loaded eight-worker p95 from
V260's 53.510 ms to 29.318 ms (45.2%). It retained exactly the same
1000/1000 ordered k100 lists and 99,948/100,000 GT100 hits. The frozen
p95≤26.8 ms and throughput≥330 QPS gates failed: measured p95 was
29.318 ms and throughput296.74 QPS. No product HTTP or vendor win is
inferred. Stop this scoring-kernel series; the next falsifier must change
the navigation representation or graph work materially.

The sole `causality` c7i.4xlarge Spot attempt `a0001` ran on
`i-0b2e117bfccc6e932` in `eu-central-1c`, now terminated. Source
commit `5db6ac8f1beb9e9318e95fb2fd297d9d64a12b2f`, archive SHA-256
`a36bff813205b8b277c2ad0124ae4aec9be8f6ed05396bf6aefe25b8dbbbb95c`.
Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v265-cohere-fast-dual-graph-100k/5db6ac8f1beb9e9318e95fb2fd297d9d64a12b2f/runs/a0001/`.
Terminal status `complete`, exit0, SHA-256
`1af22e907e3b7cb9fac0687ec3bedc2bf0aea4c3a427b1c731b7df67c2119d29`.
The original launcher replayed every artifact size/SHA-256 and confirmed
termination. Remote release tests passed for graph search, SIMD chunk+
tail scoring and FP16 underflow rejection. Source, graph, plane, PQ
books/codes, map, requests and exact truth match V260 byte for byte.

Dataset and split: CoHere-large-10M canonical first100,000 source rows,
D768 cosine, k100, authenticated prior-used test ordinals0–999;
development0–255 and validation256–999. The baseline is immutable V260
from a different source revision; conditions and artifact identities are
matched, but these are not same-revision paired repetitions.

| Same 100k panel, loaded eight-worker in-process | Dev GT100 / 25,600; p05 | Val GT100 / 74,400; p05 | Combined / 100,000 | p50/p90/p95/p99, ms | QPS |
|---|---:|---:|---:|---:|---:|
| V260 FP64 navigation | 25,583; 99 | 74,365; 100 | 99,948 | 47.756 / 52.109 / 53.510 / 55.730 | 166.32 |
| V265 cached-norm SIMD navigation | 25,583; 99 | 74,365; 100 | 99,948 | 26.889 / 28.795 / 29.318 / 30.298 | 296.74 |

The sealed raw IDs and loaded timing samples independently replayed all
four percentiles and ordered ID parity. V265 sequential p50/p90/p95/p99
was29.010/31.537/32.111/33.461 ms. Both cells had p95 PQ graph
scores33,637, exact graph scores21,236, final distinct union102 and
base visits54,519. V265's FP64 rerank of the retained exact beam is
additional score work outside that visit count; it did not change graph
coverage. Serving peak RSS was215,863,296 B, including800,000 B of
new cached norms, versus V260's214,740,992 B; zero query vector-body
GETs and zero remote swap. Graph rebuild took172.41 s and peaked at
522,748 KiB. Cold hydration was0.592 s.

The instance launched18:08:26 UTC and terminal landed18:20:09 UTC,
elapsed703 s. At the previously recorded $0.3663/hour Spot quote,
compute through terminal was approximately $0.0715, excluding EBS,
S3 and cleanup tail; this is not an observed bill. Sealed quality SHA-256
`8e4ab2449ac19d6ae70169598b3c3b36986b2c2f40b401981a67016910a65a32`,
raw IDs `ae213d834a5bfc29bf89299d3a428e9a7bc7f334dc31020e9f7b756a20841357`,
loaded timing `00133a5575b5afca97f2a9e8da6b7d787d12fbec75c729ad7af1acdab2fd1c8c`,
serving summary `bedf7dd52fa8e7c6db2d1e19738856416653a795d473b7529a1e2199641ddd35`.

V262 HTTP first1M p95 149.749 ms and V263 direct S3 Vectors repeat
p95 66.394 ms remain measured on their own frozen revisions and
different transport/cache semantics. V265's 100k in-process timing
must not be added to or substituted for either product latency.

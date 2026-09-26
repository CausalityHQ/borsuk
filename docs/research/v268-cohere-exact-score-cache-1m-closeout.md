# V268 CoHere first1M: cached dual graph transfers

**Decision: promote to one authenticated resident HTTP cell.** The
V267 fast graph plus exact-score cache transfers to first1M with all
1000 ordered k100 result lists equal to V261 and the same99,717
GT100 hits. It clears the frozen loaded latency, throughput and memory
gates. It is an in-process result, not client HTTP latency.

One `causality` c7i.4xlarge Spot attempt `a0001` ran on
`i-030a1856b9759b8aa` in `eu-central-1c`, now terminated. Source
commit `5b83f6c3ba50bb09b2cb65ca063df925c284a555`, archive SHA-256
`80478066361867ba40aaa9db49f21f2f506a068f09b9d3dcbf36fc11059cc93d`.
Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v268-cohere-cached-dual-graph-1m/5b83f6c3ba50bb09b2cb65ca063df925c284a555/runs/a0001/`.
Terminal `complete`, exit0, SHA-256
`22843310d0cb599ba2141cbb38cfe71c448359d97720c4975b7239370d4e7530`.
The original launcher replayed every artifact size/SHA-256 and
confirmed termination. Remote release graph, SIMD/tail and FP16
underflow tests passed before measurement. The authenticated V257
source, FP16 plane, graph, PQ books/codes, map, requests and exact
GT100 truth were reused byte for byte; no 1M graph was rebuilt.

Dataset: CoHere-large-10M canonical first1,000,000 source rows,
D768 cosine k100; prior-used test ordinals0–999, development0–255,
validation256–999. The eight-worker loaded process has zero query
vector-body GETs. V261 is an immutable prior-revision baseline.

| Arm | Dev hits / 25,600; p05 | Val hits / 74,400; p05 | Combined / 100,000 | Loaded p50/p90/p95/p99, ms | QPS | Peak RSS, B |
|---|---:|---:|---:|---:|---:|---:|
| V261 FP64 dual graph | 25,511; 98 | 74,206; 99 | 99,717 | 90.843 / 110.735 / 116.782 / 122.924 | 86.05 | 2,014,588,928 |
| V268 fast cached dual graph | 25,511; 98 | 74,206; 99 | 99,717 | 50.154 / 60.136 / 62.586 / 66.146 | 151.07 | 2,024,177,664 |

Independent replay of sealed raw IDs and loaded timing reproduced all
four percentiles and 1000/1000 ordered-list parity. V268 meets p95≤105
ms, p99≤125 ms, throughput≥95 QPS, RSS≤3 GiB, split quality, and
zero swap/error/GET gates. It reused a mean1,426.849 exact scores per
query (minimum694, p95 1,746) from the PQ shortlist during graph beam
rerank. PQ and exact graph p95 visits were72,353 and44,787; distinct
final union p95 was106. The visit counts omit the retained-beam FP64
rerank. The p95 reduction of46.4% and throughput gain75.6% against
V261 combine V265's fast FP16 navigation and V267's score cache; this
campaign does not isolate each component at 1M. V267 versus V265
isolated the cache at 100k.

In-process cold hydration took6.017 s; serving peak1,975,676 KiB by
`/usr/bin/time`, no swaps. Spot launched19:10:40 UTC and terminal
landed19:20:59 UTC, about619 s. At the prior $0.3663/hour Spot quote,
compute through terminal is approximately $0.0630, excluding EBS, S3
and cleanup tail; it is an estimate, not a bill. Sealed quality SHA-256
`8943700e15fb1e7c9a9b554b79ac078bbef9382977ae906ee7fef62497a2cbd8`,
raw IDs `6a2a1621c43b31d6dc89bdba4d800cd530eb455f0adfe67842fbcc1e4fd9ee86`,
loaded timing `5bee45f2a6f76a5ae5c46171799d98fcea7151029f8c6e7c1dc0fb8bd9995a90`,
serving summary `f231e5948954b823d95e7ce1ab4747211431ffc7621cf2c020753e2f539f559f`.

Next single gate: route the authenticated generation and eight-worker
VPC-peer HTTP server through this same cached search method. Reuse the
exact V268 source artifacts and panel, verify first/repeat ordered IDs
and GT100 truth, then report client p50/p90/p95/p99, QPS, resident
RSS, cold hydration bytes/GETs and cost. Do not compare in-process
V268 percentiles to service percentiles or claim a matched vendor win.

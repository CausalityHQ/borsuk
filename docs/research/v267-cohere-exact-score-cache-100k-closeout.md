# V267 CoHere 100k: exact score cache passes the gate

**Decision: promote this authenticated dual-route search to one frozen
first1M quality/resource cell and a same-revision resident HTTP cell.**
The cache reuses exact FP16 scores already computed for the PQ shortlist
when reranking the fast graph's retained beam. It changed no graph,
plane, PQ artifact, trained value, beam width, or final union ranking.

The one `causality` c7i.4xlarge Spot attempt `a0001` ran on
`i-0a8f5e46f117fa0ea` in `eu-central-1c` and is terminated. Source
commit `e8e5f465f4a1a0ddc254d8c783179a89ea94e27f`, archive SHA-256
`a7a69e7bb05aa0918e25cea422bbfbc6372a51815723913d9308bc8dfc83582e`.
Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v267-cohere-cached-dual-graph-100k/e8e5f465f4a1a0ddc254d8c783179a89ea94e27f/runs/a0001/`.
Terminal `complete`, exit0, SHA-256
`bbd611940df42689fc623c8e8dbc4551fe7744b8c7964369ad15dc98c2be2cf7`.
The original launcher replayed every artifact size/SHA-256 and confirmed
termination. Remote release graph, SIMD/tail, and FP16-underflow tests
passed before measurement. The source, graph, FP16 plane, PQ books/codes,
map, requests, and GT100 truth match V265 byte for byte.

Dataset: CoHere-large-10M canonical first100,000 rows, D768 cosine
k100; authenticated prior-used test ordinals0–999, development0–255,
validation256–999. The eight-worker in-process loaded query panel uses
one prehydrated process, zero query vector-body GETs. V265 is an immutable
prior-revision baseline, not a same-revision paired repetition.

| Arm | Dev hits / 25,600; p05 | Val hits / 74,400; p05 | Combined hits / 100,000 | Loaded p50/p90/p95/p99, ms | QPS | Peak RSS, B |
|---|---:|---:|---:|---:|---:|---:|
| V265 fast FP16 | 25,583; 99 | 74,365; 100 | 99,948 | 26.889 / 28.795 / 29.318 / 30.298 | 296.74 | 215,863,296 |
| V267 score cache | 25,583; 99 | 74,365; 100 | 99,948 | 23.067 / 25.155 / 25.827 / 26.975 | 334.87 | 216,129,536 |

Independent replay of sealed raw IDs and loaded timings reproduced all
four percentiles and 1000/1000 ordered k100 parity against V265. V267
meets p95≤26.8 ms, p99≤32 ms, throughput≥330 QPS, RSS≤300 MiB,
quality and error/GET gates. Its mean exact-score cache hits were
1,579.915/query (minimum1,159; p95 1,840), out of an ef2048 retained
beam. PQ and fast graph p95 visits were33,637 and21,236, unchanged
from V265; distinct final union p95 was102. The cache saves exact
rerank work, not graph navigation work. Loaded p95 fell11.9% and
throughput rose12.8% against the historical V265 run. The result is
in-process latency, not product HTTP latency.

Hydration took0.586 s. Graph build wall time168.86 s with peak522,628
KiB RSS; serving peak211,064 KiB by `/usr/bin/time`, no swaps. Spot
launched18:53:24 UTC and terminal landed19:04:35 UTC, about671 s.
At the prior $0.3663/hour Spot quote, compute through terminal is
approximately $0.0683, excluding EBS, S3, and cleanup tail; this is
an estimate, not a bill. Sealed quality SHA-256
`1cc271fc72129651372b4787b8fdf4ba0e0d62a03fd15dccc3680a918284c86f`,
raw IDs `277bf36bf14988876b2affd3db93b5fff8148a6a3c98f2323f10e208099af1ec`,
loaded timing `fef530e1d264e4a71f4b88656f02c46d87b7f2ad6a5ead1cd9d418d36b70a11e`,
serving summary `da249874d6ba0c9256b212f620869fee4813d8c6f9dd4d5c8cda265847247a98`.

Next single gate: freeze this search method on CoHere first1M with
authenticated V257 graph reuse, exact quality, loaded latency,
throughput and memory, then measure same-revision resident HTTP first
and repeat latency. Only compare S3 Vectors or Turbopuffer as a product
claim under disclosed matched quality, cache, transport, hardware,
region, concurrency, and cost conditions.

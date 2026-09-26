# V261 CoHere first1M: dual graph passes frozen falsifier

**Decision: qualify one persisted client HTTP gate on this exact
method.** V261 passed all preregistered first1M exact quality and
loaded in-process resource gates. These timings are not client HTTP
or a matched S3 Vectors/Turbopuffer result; 10M remains closed until
the product serving gate qualifies.

The sole `causality` c7i.4xlarge Spot attempt `a0001` ran on
`i-00d837ba615a657be` in `eu-central-1c`, now terminated. Source
commit `2dee58896e42f84d74e2653dc0960e19d6defc63`, archive SHA-256
`f68ac7332fa591885631d8dd64f804fac4043f7303561ecab3652a8e8bd7228b`.
Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v261-cohere-dual-graph-1m/2dee58896e42f84d74e2653dc0960e19d6defc63/runs/a0001/`.
Terminal SHA-256
`00c7d4803354f15d5f62bea6e53fd0672a0f5fc91cc482e1fa4b20b8951a9f82`,
status `complete`, exit0. The original launcher replayed every
terminal artifact size/SHA-256 and confirmed EC2 termination. The
remote graph-schema and graph-workspace tests each passed one test
with zero failures before measurement. All source, plane, graph,
books/codes, map and query artifacts were verified against the
closed V257 `a0001` terminal before reuse. No 1M graph or coarse
index was rebuilt.

Dataset and split: CoHere-large-10M canonical first1,000,000 source
rows, D768 cosine, k100; authenticated prior-used test ordinals0–999,
development0–255 and validation256–999. The method is identical to
V260: PQ-cosine graph ef4096 with FP16 shortlist4096→k100, exact FP16
graph ef2048→k100, then FP16-rerank the distinct two-list union to
final k100. No coarse route, additional graph, ef sweep or fallback.

| Same CoHere-1M panel | Development GT100 hits / 25,600; p05 | Validation GT100 hits / 74,400; p05 | Combined / 100,000 | Loaded p50/p90/p95/p99, ms | QPS |
|---|---:|---:|---:|---:|---:|
| V255 PQ graph | 25,213; 93 | 73,504; 94 | 98,717 | 21.888 / 24.977 / 25.952 / 27.456 | 356.1 |
| V258 exact FP16 graph | 25,379; 96 | 73,930; 97 | 99,309 | 68.876 / 85.790 / 90.440 / 95.505 | 113.23 |
| V257 graph + coarse hybrid | 25,404; 96 | 73,971; 97 | 99,375 | 44.320 / 47.924 / 49.140 / 50.871 | 178.98 |
| **V261 dual graph** | **25,511; 98** | **74,206; 99** | **99,717** | **90.843 / 110.735 / 116.782 / 122.924** | **86.05** |

V255 loaded percentiles are its authenticated summary only; that
campaign did not seal loaded per-query samples. V257, V258 and V261
loaded percentiles were independently replayed from sealed samples.
V261 mean exact R@100 was0.99652344 development and0.99739247
validation. It passed each split's ≥0.995 mean and p05≥98, combined
≥99,500; loaded p95≤135 ms, p99≤155 ms, QPS≥65, peak RSS≤3 GiB
and zero query vector-body GETs. Peak serving RSS was2,014,588,928 B;
cold hydration3.317 s. Sequential p50/p90/p95/p99 was
90.681/110.987/117.115/123.038 ms.

Sealed raw IDs SHA-256
`d8566803b49197ae454d8ecc3775adc8bd4e9a48629885db546a043b9ae94246`,
truth SHA-256
`62e14eba043fafb8d8ec7c833d7d320c5d823c549683d15e5eacdff365a87f39`
and sealed loaded timing SHA-256
`bdee30d7a414d3838255c310bdb9081427eda393043b77d687b7403c48ed3729`
were independently replayed. Replay reproduced every split count and
all four same-sample loaded percentiles. PQ graph score p50/p90/p95/p99
was52,519/67,989/72,353/81,332; exact FP16 graph score count was
33,866/42,590/44,787/47,082; distinct final union size was
100/104/106/110. Component percentiles are not additive. The
pre-measurement candidate ceiling from V255+V258 final lists was
99,733 hits; V261's final rerank returned99,717, a loss of16.

The instance launched13:43:06 UTC and terminal landed13:54:07 UTC,
elapsed661 s. At the previously recorded $0.3663/hour Spot quote,
compute through terminal was about $0.0673, excluding EBS, S3 and
cleanup tail. V257's inherited preparation/graph-build costs are
separate and were not repeated.

**Next gate:** publish a versioned authenticated generation containing
this exact source/FP16/PQ/graph/map identity, hydrate it from an empty
server cache, and run one same-revision VPC-peer HTTP client cell on
the same 1,000 queries. Seal client raw samples; require exact parity
to V261, report p50/p90/p95/p99, throughput, GETs/bytes, cache state,
RSS and full cost. Do not call the V261 in-process result a product
latency win or compare it directly to S3 Vectors' opaque HTTPS cache.

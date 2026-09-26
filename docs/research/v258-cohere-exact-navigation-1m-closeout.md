# V258 CoHere first 1M: exact graph navigation fails quality

**Decision: no HTTP promotion.** The one frozen exact FP16 graph
navigation arm at ef2048 failed the development, validation, p05 and
combined exact-recall gates. Its loaded resource gates passed. This is
an in-process falsifier, not client-observed product latency or a
matched vendor comparison.

Dataset: CoHere-large-10M canonical first1,000,000 source rows, D768
cosine, k100, authenticated prior-used test ordinals0–999; development
0–255, validation256–999. Source commit
`804e630833e740baf11231f2d57be8483218990d`; source archive SHA-256
`6de8b0bce8ae25b0f5b57dc854b95ae6bf3da6f31b26a118bb42522afbd2ba2f`.
The sole `causality` c7i.4xlarge Spot attempt `a0001` ran on
`i-0b63f1055307835ff` in `eu-central-1c`, now **terminated**.
Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v258-cohere-exact-navigation-1m/804e630833e740baf11231f2d57be8483218990d/runs/a0001/`.
Terminal SHA-256
`d93a1510bbe49a0391b17e532d42f855d8958cb305b07216cbc0360ce57a9ede`,
status `complete`, exit 0. The original launcher replayed every
terminal artifact's size and SHA-256, then confirmed instance
termination. It authenticated and reused the closed V257 `a0001`
source/plane/graph/map/books/codes/requests; no graph or coarse index
was rebuilt. The narrow remote graph-schema and graph-workspace tests
each passed one test with zero failures before measurement.

| Same CoHere-1M panel | Development hits / 25,600; p05 | Validation hits / 74,400; p05 | Combined / 100,000 | Loaded p50/p90/p95/p99, ms | QPS | p95 graph scores |
|---|---:|---:|---:|---:|---:|---:|
| V255 PQ graph baseline | 25,213; 93 | 73,504; 94 | 98,717 | 21.888 / 24.977 / 25.952 / 27.456 | 356.1 | 72,353 PQ |
| V257 PQ graph + coarse baseline | 25,404; 96 | 73,971; 97 | 99,375 | 44.320 / 47.924 / 49.140 / 50.871 | 178.98 | 90,311 PQ |
| **V258 exact FP16 graph** | **25,379; 96** | **73,930; 97** | **99,309** | **68.876 / 85.790 / 90.440 / 95.505** | **113.23** | **44,787 FP16** |

V255 loaded percentiles are from its authenticated summary; that
campaign did not seal loaded raw samples. V257 and V258 loaded
percentiles were independently replayed from sealed per-query samples.

V258 mean exact R@100 was 0.99136719 on development and 0.99368280
on validation; frozen minimum was 0.995 in each, p05≥98 and combined
≥99,500. It returned 66 fewer exact GT100 hits than V257. Its sealed
raw IDs SHA-256
`e76207366d352243eb2b66c3658c9bb1612ee8185e4f6ad4fa40056bb5dc5daa`,
same-panel truth SHA-256
`62e14eba043fafb8d8ec7c833d7d320c5d823c549683d15e5eacdff365a87f39`,
and sealed loaded timing SHA-256
`07be469d048bc8a84c64f8d2a61f0cc63a1d044bca372293fa0d697127964954`
were independently replayed. Replay reproduced every split count and
all four loaded percentiles from the same 1,000 samples. Sequential
p50/p90/p95/p99 was 69.288/86.547/91.122/96.048 ms. Graph FP16
score counts at p50/p90/p95/p99 were 33,866/42,590/44,787/47,082,
or 4.48% of N at p95. Loaded p95≤120 ms, p99≤150 ms, ≥70 QPS, peak
RSS≤3 GiB and zero query vector-body GETs all passed. Peak serving
RSS was 2,011,226,112 B; cold hydration 3.506 s. These resource
numbers cannot rescue the failed quality gate.

The instance launched 12:44:35 UTC and terminal landed 12:55:32 UTC,
elapsed 657 s. At the previously recorded $0.3663/hour Spot quote,
approximate compute through terminal was **$0.0669**, excluding EBS,
S3 and cleanup tail. Inherited V257 preparation and graph-build
costs remain separate and were not repeated.

## Post-terminal complementarity diagnostic

This descriptive analysis used **only closed** V257/V258 sealed final
k100 ID lists and their shared exact truth. Their final-list union
contains 99,859/100,000 exact GT100 IDs: development25,551/25,600
and validation74,308/74,400, p05 99 in each split. V258 beat V257
on 130 queries, tied on 687 and lost on 183. The 99,859 number is an
upper bound for an exact reranker restricted to that 200-ID union; no
combined final top100, latency, throughput or cost was measured. It
shows complementary candidate misses and motivates one material
dual-navigation candidate-generation gate, but cannot establish that
such a method passes product or vendor requirements.

**Next:** first falsify the fixed dual-navigation union on authenticated
CoHere first100k with exact final rerank and measured work/latency;
only a qualifying result may promote to a new frozen 1M run. Do not
sweep ef, probes or query-specific thresholds. Keep HTTP, 10M and
competitor claims closed until exact quality and resources pass.

# V260 CoHere 100k: dual graph passes frozen gate

**Decision: promote this fixed two-scorer graph method to one first1M
quality/resource falsifier.** It passed all preregistered exact quality,
loaded latency, throughput, RSS and zero-query-GET gates. It is an
in-process result, not client HTTP or a matched vendor win.

The sole `causality` c7i.4xlarge Spot attempt `a0001` ran on
`i-08774686a019ee407` in `eu-central-1c`, now terminated. Source
commit `ed8f233404da8df4cc6d6b082540c58e1faa7308`, archive
SHA-256
`93e07a9a3d96c6c9e85ea69bb5148a30067181fd960490a8025b062f8e1f3809`.
Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v260-cohere-dual-graph-100k/ed8f233404da8df4cc6d6b082540c58e1faa7308/runs/a0001/`.
Terminal SHA-256
`55c719ef8cde037451b3001318b9c70b5ab64315843ba9651b3eb949b84f5aa6`,
status `complete`, exit0. The original launcher replayed every
artifact size/SHA-256 and confirmed EC2 termination; the narrow remote
graph-workspace test passed before measurement. Source, plane,
books/codes, map, graph, requests and exact truth match V250/V256
byte for byte. No coarse index was built or queried.

Dataset and split: CoHere-large-10M canonical first100,000 source
rows, D768 cosine, k100, authenticated prior-used test ordinals0–999;
development0–255 and validation256–999. One fixed arm runs PQ-cosine
graph ef4096 with FP16 shortlist4096→k100, exact FP16 graph ef2048→
k100, then FP16-reranks their distinct final-list union to k100.

| Same 100k panel | Development GT100 hits / 25,600; p05 | Validation GT100 hits / 74,400; p05 | Combined / 100,000 | Loaded p50/p90/p95/p99, ms | QPS |
|---|---:|---:|---:|---:|---:|
| V256 hybrid baseline | 25,558; 99 | 74,320; 99 | 99,878 | 30.026 / 30.621 / 30.777 / 31.077 | 266.6 |
| V259 coarse + dual graph, rejected | 25,586; 99 | 74,369; 100 | 99,955 | 76.319 / 81.363 / 82.909 / 84.958 | 104.21 |
| **V260 dual graph** | **25,583; 99** | **74,365; 100** | **99,948** | **47.756 / 52.109 / 53.510 / 55.730** | **166.32** |

V260 mean exact R@100 was0.99933594 development and0.99952957
validation. It passed frozen per-split mean≥0.995 and p05≥98,
combined≥99,800; loaded p95≤75 ms, p99≤90 ms and QPS≥110.
Peak serving RSS was214,740,992 B, under300 MiB; query vector-body
GETs were zero. Sequential p50/p90/p95/p99 was
47.475/51.998/53.174/55.636 ms; cold hydration330.634 ms.

Sealed raw IDs SHA-256
`ae3c2b8f1372de813b06f9e4378fd824b6fa714158aba99f66c44c76a26b982c`,
exact truth SHA-256
`06cd59b31962d4190367b54d7abf24dd4e018d3c4ac8da0b2b528d21a5a7cbb8`,
and sealed loaded timing SHA-256
`9f4a63452638a08566d76c56177563e53741412c962a3477812a8fe643f68e6e`
were independently replayed. All split counts and four same-sample
loaded percentiles reproduced. Per-query p50/p90/p95/p99 PQ graph
scores were28,284/32,430/33,637/35,288; exact FP16 graph scores
18,585/20,684/21,236/22,265; distinct final union sizes
100/101/102/103. Component percentiles are not additive.

Graph rebuild took171.00 s, peak522,812 KiB, zero swap. The instance
launched13:25:46 UTC and terminal landed13:37:22 UTC, elapsed696 s.
At the previously recorded $0.3663/hour Spot quote, compute through
terminal was about $0.0708, excluding EBS, S3 and cleanup tail.

The closed first1M V255 PQ graph and V258 exact graph final-list
union contains99,733/100,000 exact GT100 IDs, above the 99,500 gate;
this is a **candidate ceiling only**. No dual final rerank, latency
or product HTTP result has been measured at1M. The next gate freezes
the same ef4096/4096 PQ and ef2048 exact paths with their FP16 final
union, using the already authenticated first1M graph artifacts. Do
not infer 1M quality, scale work or a vendor win from this 100k pass.

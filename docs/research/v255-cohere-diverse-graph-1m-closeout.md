# V255 CoHere first 1M: graph scale quality failed

Source commit `68c35e24a648e2036c3d7f568a3b5d6a27fcaa18`; the sole
`causality` c7i.4xlarge Spot attempt `a0001` ran on
`i-0b4face2d657d499a` in `eu-central-1c`, now **terminated**.
Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v255-cohere-diverse-graph-1m/68c35e24a648e2036c3d7f568a3b5d6a27fcaa18/runs/a0001/`.
Source archive SHA-256
`bfa0b8f816d20077add9443167891d61882f9427c1e11f5ef4c2313bd75cebec`;
terminal SHA-256
`253f787ba8fe6efa4334c259d55ecaae557d1f8e302063291e0e108aefe1e99b`.
The terminal exited zero, the original launcher independently replayed
the size and SHA-256 of every artifact, and EC2 reports termination.
The narrow remote graph test passed before measurement. Source F32,
FP16 plane, PQ64 codes, graph and raw IDs have separate authenticated
hashes in the terminal. Sealed raw SHA-256
`0d86f93a02a7b724cd06f8ab900ec6366f7ea4bc08723a8233208eb80ee0f8ec`;
exact GT100 SHA-256
`62e14eba043fafb8d8ec7c833d7d320c5d823c549683d15e5eacdff365a87f39`.

CoHere-large-10M canonical first **1,000,000** source rows, D768
cosine, k100. The 1,000 test queries were already used; development
ordinals 0–255 and remaining validation 256–999 are descriptive
splits. One preregistered arm: source-only diverse graph M32/M0 64,
construction ef128, PQ-cosine navigation ef4096, authenticated FP16
rerank shortlist4096, eight build and eight serving workers. IDs were
sealed before exact F32 truth was computed.

| Result | Development / 25,600 | Validation / 74,400 | Combined / 100,000 |
|---|---:|---:|---:|
| Exact GT100 hits | 25,213 | 73,504 | 98,717 |
| Mean R@100 | 0.984883 | 0.987957 | 0.987170 |
| p05 hits/query | 93 | 94 | 93 |

Independent local replay of all 1,000 sealed ID sets against the
authenticated truth reproduced these counts. The frozen ≥0.995 mean
and ≥98 p05 gates **failed on both splits**. There were 163 queries
with fewer than 98 exact hits, including 72 with fewer than 95.

Loaded eight-worker **in-process** p50/p90/p95/p99 was
21.888/24.977/25.952/27.456 ms, 356.1 completed QPS. The p95 PQ
row-score count was 72,353 (7.24% of N); peak serving RSS was
2,014,601,216 B; zero vector-body GETs. These passed their separate
frozen gates. Sequential raw timing p50/p90/p95/p99 independently
replayed as 21.632/24.768/25.802/27.459 ms. Loaded percentiles are
authenticated summary values; this harness did not seal individual
loaded-pass samples, so those percentiles cannot be independently
recomputed from raw loaded samples. Cold hydration was 3.434 s.
Neither timing is end-to-end HTTP nor a vendor comparison.

The graph was fully reachable with minimum in-degree four, maximum
degree 256 and 65,231,960 directed base edges. Graph build took
**1,951.495 s**, exceeding the frozen 1,800 s cap; peak builder RSS
was 5,321,728,000 B. Source/PQ preparation took 96.13 s and peaked
at 7,754,060 KiB; exact F32 truth took 21.06 s and peaked at
14,879,260 KiB, with zero swaps in each recorded process. This
separates build, serving and evaluation resource costs.

The instance launched 10:12:48 UTC; the terminal landed 10:55:55 UTC
(43m07s). At the recorded `eu-central-1c` Spot quote of $0.3663/hour,
compute through terminal is approximately **$0.2632**, excluding EBS,
S3, cleanup tail and billing adjustments.

**Decision:** do not promote this CoHere graph to persisted HTTP or
10M. Its query work and loaded latency scale plausibly from V250's
100k result, but exact quality drops below the frozen gate and the
builder exceeds its time cap. The 1M miss cannot yet be assigned
uniquely to PQ representation or graph coverage. On the closed CoHere
100k panel, the union of V250 graph ef4096 and V254 coarse-PQ returned
top100 lists contains 99,902/100,000 exact GT100 IDs (development
25,565, validation 74,337, p05 99 on each). This is a **read-only
candidate-coverage diagnostic**, not a measured hybrid serving arm;
its up-to-200 IDs have not been reranked together. It suggests the two
routes make complementary errors. Freeze one materially different
graph-plus-coarse-PQ candidate-union method and falsify it at 100k
before another 1M run. Keep build time, quality, latency, throughput,
memory and cost as separate gates. Do not sweep ef on the used 1M
panel or infer a matched S3 Vectors/Turbopuffer win.

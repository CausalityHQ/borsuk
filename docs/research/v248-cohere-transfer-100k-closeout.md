# V248 CoHere 100k transfer: quality gate failed

Source commit `7b66f9f7d306a0f1febc2091783de68a6ece72c7`; one `causality`
c7i.4xlarge Spot attempt `a0001` in eu-central-1c, instance
`i-01a8be9e277ed3487`, terminated after its complete terminal. Immutable
prefix:
`s3://borsuk-bench-453182569524-euc1/research/v248-cohere-transfer-100k/7b66f9f7d306a0f1febc2091783de68a6ece72c7/runs/a0001/`.
Source archive SHA-256
`221f65fcbb4623a712fd2762dceb3fb6f6ba84dbce7c822f2aa4b137b178eeae`;
terminal SHA-256
`9e45f55216518fec13bba3c40002aedb8d356ce8fe1fa943d6ac4caf9ac817f6`.
The launcher authenticated every complete terminal artifact by length and
SHA-256 on readback. Raw returned IDs SHA-256
`5e0fb930d30f90893259981fd9fd9707e623cee4ca0a2ecc374a82a7eaeaad87`;
independently computed exact 100k GT100 IDs SHA-256
`06cd59b31962d4190367b54d7abf24dd4e018d3c4ac8da0b2b528d21a5a7cbb8`.
The 1,000 sealed query rows and 100,000 corpus rows use D768 cosine,
query k=100, with configuration development ordinals 0–255 and validation
256–999. This published dataset was used in earlier repository work; the
validation panel is **not** a pristine external holdout. Truth was recomputed
over the first 100k rows and only after returned IDs were sealed to S3;
the published 10M neighbors were not used.

| Arm ef/shortlist | Development GT100 hits / 25,600; p05 | Validation GT100 hits / 74,400; p05 | Combined GT100 hits / 100,000 | Loaded p50/p90/p95/p99 ms | Loaded QPS |
|---|---:|---:|---:|---:|---:|
| 2048/2048 | 24,927; 91 | 72,669; 93 | 97,596 | 7.896 / 8.350 / 8.474 / 8.707 | 1005.5 |
| 4096/4096 | 25,303; 95 | 73,598; 96 | 98,901 | 14.559 / 15.098 / 15.221 / 15.516 | 545.2 |
| 8192/8192 | 25,401; 96 | 73,847; 97 | 99,248 | 27.647 / 28.382 / 28.570 / 28.888 | 289.4 |

The frozen quality gate required development and validation mean R@100
≥0.995 and p05 hits≥98. No arm passed. Independent local replay of the
sealed IDs and saved exact truth reproduced every split hit count and each
sequential per-query percentile. The loaded latency and QPS figures are
in-process, eight-worker measurements from the serving process, not HTTP
or product service latency. Serving peak RSS was 217,862,144 B, with zero
vector-body GETs. No S3 Vectors or Turbopuffer measurement was made on
this CoHere 100k workload; the V247 ReLAION-1M comparator is a different
dataset and must not be represented as paired here.

The graph reached all 100,000 nodes, minimum in-degree four, maximum
degree 256. Builder time was 23.912 s and peak RSS 533,286,912 B, passing
the frozen ≤120 s/≤1.5 GB builder gate. Separate source/PQ preparation
took 52.16 s wall time and 1,837,158,400 B peak RSS. Exact truth scoring
took 2.70 s wall time and 1,664,929,792 B peak RSS. The strongest prior
V244 ReLAION-100k resource reference built in 20.725 s at 537,739,264 B;
its quality number is not a paired CoHere control.

After this failure, a deliberately small diagnostic sampled query ordinals
0,10,…,990. Exact global FP16 top100 retained 9,999/10,000 exact F32 GT100
IDs (p05 100). Exhaustive cosine ranking of the existing PQ reconstruction
placed 9,996/10,000 GT100 IDs in its global top8192 (p05 100), while the
graph arm returned 9,933/10,000 (p05 97). Thus FP16 precision and PQ
candidate capacity are not the limiting layers on this sample; the gap is
in graph navigation under PQ scores. The current graph was constructed
from exact source directions but navigated by PQ reconstructed directions.
Whether this metric mismatch causes the gap is a **hypothesis**, not a
measured causal effect.

**Decision:** keep the 10M promotion closed. The next single 100k falsifier
builds the same owner-partitioned graph from PQ reconstructed directions,
then reuses identical source plane, PQ books/codes, requests, exact truth,
search arms, and hardware class. It must close the quality gate without a
material regression in build/serving resource use before any 10M run.

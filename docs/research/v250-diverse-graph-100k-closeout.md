# V250 diverse links: CoHere quality passed; query-work gate failed

Source commit `34a38d6427405ade5931e0cfc8e2f19e5436a6d8`;
one `causality` c7i.4xlarge Spot attempt `a0001`, instance
`i-07a4ea468e532c458`, terminated after complete terminal. Immutable
prefix:
`s3://borsuk-bench-453182569524-euc1/research/v250-cohere-diverse-100k/34a38d6427405ade5931e0cfc8e2f19e5436a6d8/runs/a0001/`.
Source archive SHA-256
`ab1bbd66ee1429509664f6f68f2281e13dc9c3465c7263e524722172924f0de1`;
terminal SHA-256
`811dfda87424380926c359d8d0c02c3db153986d7f48a31b249c2ea0316fa689`.
The launcher authenticated every terminal artifact by length and SHA-256
on readback. Graph SHA-256
`688941c7c61c89a39739909af14cb2b4a935a7a4967a168f9503ac34e170d0e7`;
sealed raw IDs SHA-256
`d89afd57561e83b7ceba81b53e6225a831267737edc73852bb8be6ec205e7c6f`.
Source F32, FP16 plane, PQ books/codes, physical map, 1,000 test queries
and exact 100k GT100 truth match V248 and V249 byte for byte. This is
CoHere-large-10M's first100k D768 cosine source rows, query k100,
development ordinals 0–255 and validation 256–999; both splits were
used in earlier repository work.

| ef/shortlist | Development hits / 25,600; p05 | Validation hits / 74,400; p05 | Combined hits / 100,000 | Loaded p50/p90/p95/p99 ms | Loaded QPS | Base scores p95 |
|---|---:|---:|---:|---:|---:|---:|
| 2048/2048 | 25,208; 94 | 73,434; 94 | 98,642 | 6.945 / 7.400 / 7.534 / 7.766 | 1142.0 | 23,486 |
| 4096/4096 | 25,508; 98 | 74,181; 98 | **99,689** | 12.709 / 13.274 / 13.421 / 13.717 | 627.1 | **33,637** |
| 8192/8192 | 25,578; 99 | 74,352; 99 | 99,930 | 24.081 / 24.823 / 25.045 / 25.331 | 329.7 | 45,804 |

The smallest quality-passing arm is 4096/4096: development mean
R@100=0.996406, validation=0.997056, and both p05=98. Independent
replay of V248, V249 and V250 sealed IDs against the same saved exact
truth reproduced every hit count. V250 gained 788 combined hits over
V248 at that arm (375 query wins, 613 ties, 12 losses). It also passed
loaded p95≤35 ms, serving peak RSS≤256 MiB and zero vector-body GETs.
The in-process timings are **not** network service latency or a matched
S3 Vectors/Turbopuffer claim.

**Scale promotion failed:** selected-arm p95 base-row scores=33,637,
above the preregistered 10,000 bound. `ef` limits retained candidates;
it does not bound base scores. The graph was fully reachable, minimum
in-degree four, maximum degree 118, with build time 170.238 s and peak
RSS 535,154,688 B, passing the≤300 s/≤1.5 GB build limit. Separate
source/PQ preparation took 47.77 s and 1,893,564,416 B peak RSS.
Serving peak RSS was 218,046,464 B.

In a read-only graph audit, all-node in-degree≤8 fraction rose from
V249's 8.04% to V250's 19.59%. Yet among GT IDs missed at ef8192,
the low-in-degree fraction fell from 91.82% to 2.86%, and combined
misses fell from 746 to 70. Link diversity clearly improved query
coverage, but the changed degree distribution does not prove a general
anti-hub theorem. More precise navigation may reduce the still-large
visited-row count.

**Decision:** keep 10M closed. The next 100k gate reuses this exact
authenticated diverse graph and FP16 plane, switches only navigation
scores from PQ reconstruction to exact FP16 cosine, and measures recall,
visited rows, p50/p90/p95/p99, QPS, RSS and zero-GET behavior at frozen
smaller beams. Passing the quality gate alone is insufficient; the
visited-row and latency limits must pass before a 1M/10M scale test.

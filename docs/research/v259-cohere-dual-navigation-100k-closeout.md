# V259 CoHere 100k: quality passes, latency fails

**Decision: reject this frozen dual navigation arm.** It passed exact
quality, throughput and memory, but its loaded p95 was **82.909 ms**
against the preregistered ≤80 ms gate. There is no 1M promotion from
V259 and no client HTTP or vendor latency claim.

The sole `causality` c7i.4xlarge Spot attempt `a0001` ran on
`i-0e6a3a65a988e24e4` in `eu-central-1c`, now terminated. Source
commit `6bcd55bd223fdd9fb679d29f4bb5aca3b03be39c`, archive SHA-256
`d70fbc2fbffabd0f7ede59ac245d165c920b1f9f85f504afbbfead686a237126`.
Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v259-cohere-dual-navigation-100k/6bcd55bd223fdd9fb679d29f4bb5aca3b03be39c/runs/a0001/`.
Terminal SHA-256
`f47f2c64d79186ca62cbd6d4f9af0c2cb595b074df312aaf83485328d82405ba`,
status `complete`, exit 0. The original launcher replayed every
terminal artifact's size/SHA-256 and confirmed EC2 termination. The
narrow remote PQ scorer test passed before measurement.

Dataset and split: CoHere-large-10M canonical first100,000 source
rows, D768 cosine, k100, authenticated prior-used test ordinals0–999;
development0–255 and validation256–999. Source, FP16 plane, PQ
books/codes, physical map, graph, coarse index, request panel and
exact truth matched the closed V250/V254/V256 baseline artifacts.
The one fixed arm took V256's graph-plus-coarse final k100, V251's
exact FP16 graph ef2048 final k100, and FP16-reranked their distinct
union to k100.

| Same 100k panel | Development GT100 hits / 25,600; p05 | Validation GT100 hits / 74,400; p05 | Combined / 100,000 | Loaded p50/p90/p95/p99, ms | QPS |
|---|---:|---:|---:|---:|---:|
| V256 hybrid baseline | 25,558; 99 | 74,320; 99 | 99,878 | 30.026 / 30.621 / 30.777 / 31.077 | 266.6 |
| **V259 dual** | **25,586; 99** | **74,369; 100** | **99,955** | **76.319 / 81.363 / 82.909 / 84.958** | **104.21** |

V259 development mean R@100=0.999453125 and validation=0.999583333;
combined=0.99955. Its quality cleared ≥0.995 mean and p05≥98 in
each split and ≥99,800 combined. Peak serving RSS was227,090,432 B,
zero query vector-body GETs; loaded p99≤95 ms and QPS≥100 passed.
Loaded p95 missed by2.909 ms. Sequential p50/p90/p95/p99 was
77.471/82.881/84.104/86.193 ms. Cold hydration took383.846 ms.
The sealed IDs SHA-256
`48b07ae03cd947a89e2fef378ea4b2cfafd0be161d4d23c4c8f218fab38a41d1`,
truth SHA-256
`06cd59b31962d4190367b54d7abf24dd4e018d3c4ac8da0b2b528d21a5a7cbb8`
and loaded raw SHA-256
`346f220944f892f02fde39e99342e40ae3cac14b2eb61def9290404215bed3c9`
were independently replayed. All split counts and same-sample loaded
percentiles were reproduced.

At p50/p90/p95/p99, PQ graph scores were
28,284/32,430/33,637/35,288; coarse PQ scores
14,696/16,788/17,254/18,158; exact FP16 graph scores
18,585/20,684/21,236/22,265. The final distinct union had
100/101/101/102 IDs at those percentiles. These are per-component
percentiles, not additive per-query p95 values. Graph build took
202.99 s, peak523,380 KiB; coarse preparation6.89 s,
peak933,548 KiB; both zero swap. The instance launched13:04:33 UTC
and terminal landed13:18:40 UTC, elapsed847 s. At the previously
recorded $0.3663/hour Spot quote, compute through terminal was about
$0.0862, excluding EBS, S3 and cleanup tail.

## Post-terminal simplification diagnostic

This is a read-only diagnostic on **closed** 100k V250/V251 artifacts,
not a frozen serving result. Unioning their two graph final k100 lists
without any coarse candidate route, then scoring the union with the
authenticated FP16 plane, returned99,948/100,000 GT100 hits:
development25,583/25,600 p05 99, validation74,365/74,400 p05 100.
The candidate ceiling was99,960. On the closed 1M V255/V258 final
lists, the analogous **candidate ceiling only** was99,733/100,000;
no final rerank or latency was measured there. This removes V259's
entire coarse route and its p95 17,254 PQ scores while retaining a
candidate ceiling above the 1M 99,500 gate. The decision is to
falsify **one** simpler dual-graph arm at100k with measured latency,
quality and resource use before another1M job. Do not retrospectively
count this diagnostic as a gate pass or tune V259's thresholds.

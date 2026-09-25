# V245 owner-partitioned source graph, ReLAION-1M closeout

**Decision: pass the frozen 1M construction, quality and loaded-serving
gates.** Select the V244 owner-partitioned batch builder with the
source-only reachable graph, PQ64 cosine navigation and authenticated
FP16 rerank at ef/shortlist 4,096/4,096. This qualifies one
same-revision persisted end-to-end HTTP gate; these in-process numbers
are not product network latency or a matched vendor win.

The single valid `causality` c7i.4xlarge Spot attempt was `a0001` on
`i-08421ba511fd2cdda` in `eu-central-1c`, now **terminated**. Source
commit `a45c8f9635b7663334367960d2e423ddf29769f4`, source archive
SHA-256 `28f572de89e6e0d3e570ead5aa547489e8fc211b5963f336ee8f031b5118f3b5`,
terminal SHA-256 `632bb364eb026220116f45e7e5ff5f980d197a4d0668c491773344cfd03ff689`.
The terminal reports exit zero; the original launcher checked sizes and
SHA-256 for all 13 artifacts and confirmed EC2 termination. Sealed raw
SHA-256 `dffb4e97d7ab0d00745673d299df6d2c9c8b62f881d7d85eaf61f0fdd585216a`
was independently replayed against the exact GT100 witness. Evidence:
`s3://borsuk-bench-453182569524-euc1/research/v245-owner-graph-1m/a45c8f9635b7663334367960d2e423ddf29769f4/runs/a0001/`.
The narrow release graph thread-count test passed 1/1.

Frozen dataset and split: ReLAION-1M, D768, cosine, k=100, validation
queries 0–999 **already used**, development 0–255 and remaining
256–999. The source, physical order, FP16 plane, PQ books/codes,
requests, eight serving workers and four ef arms match V219. Graph
parameters are M=32, M0=64, ef_construction=128; build workers=8.
The 266,910,242-byte graph SHA-256 is
`92df3782b2836e608d206401c4efdd3d71bd8e81fe18887b0e93af4d23d8ade3`.
All 1,000,000 rows are reachable, minimum in-degree four, zero below
four, maximum degree 256 and 64,927,486 directed base edges.

| Ef / shortlist | Exact GT100 hits / 100,000 | Dev / 25,600 | Remaining / 74,400 | p05 hits/query | Loaded p50/p90/p95/p99, ms | QPS |
| --- | ---: | ---: | ---: | ---: | --- | ---: |
| 2,048 / 2,048 | 99,504 | 25,461 | 74,043 | 98 | 7.741 / 10.300 / 10.998 / 12.223 | 946.3 |
| **4,096 / 4,096** | **99,662** | **25,506** | **74,156** | **98** | **15.107 / 19.525 / 20.617 / 23.047** | **498.9** |
| 8,192 / 8,192 | 99,768 | 25,537 | 74,231 | 99 | 30.061 / 37.848 / 39.948 / 43.567 | 252.4 |
| 16,384 / 16,384 | 99,821 | 25,546 | 74,275 | 99 | 60.043 / 72.835 / 76.943 / 82.040 | 129.2 |

The selected arm passes frozen quality thresholds (combined 99,614,
dev 25,498, remaining 74,116 and p05 98) and loaded p95 ≤25 ms,
p99 ≤30 ms, ≥400 QPS, ≤3 GiB serving RSS and zero vector-body GETs.
Its p95 base visits were 57,699. Peak serving RSS was 2,021,433,344 B;
cold S3 hydration took 3.736 s. These timings cover in-process query
execution after hydration, excluding HTTP, network admission and
external object-store service.

The V219 same-panel serial-source-graph baseline, from a different
revision and Spot instance, returned 99,664 hits (dev 25,508,
remaining 74,156), p05 98, loaded p50/p90/p95/p99
13.365/17.697/18.911/20.737 ms, 555.6 QPS and 2,023,636,992 B
peak serving RSS. V245 changes four ordered returned-ID lists at
the selected arm (one query gains, three lose); combined quality is
two hits lower. Loaded p95 is 1.706 ms slower and throughput is
10.2% lower across those separate hosts/revisions. The builder
change has no demonstrated serving-speed benefit.

The V245 graph build took **358.766 s** by Rust timer, peaked at
**5,281,869,824 B builder RSS**, and passed the frozen 900 s / 6 GB
gates. `/usr/bin/time` recorded 6m02.59s wall, 687% CPU and zero swaps.
V219 took 2,665.254 s and 8,394,833,920 B: V245 reduced measured
build time 86.5% and builder RSS 37.1% across instances. Separate
Python source preparation took 19.16 s and peaked at **7,809,417,216 B**;
this is the full campaign preparation peak and remains a distinct
memory scaling concern. The instance launched at 22:04:50 UTC and the
terminal landed at 22:21:21 UTC (991 s). At the eu-central-1c Spot
quote of $0.3676/hour, compute was approximately **$0.1012**,
excluding EBS, S3, termination tail and billing rounding.

**Next gate:** persist and serve this exact graph architecture through
the production HTTP path on the frozen ReLAION-1M panel, then run a
direct S3 Vectors comparison at disclosed equivalent conditions.
Record per-query recall, p50/p90/p95/p99 from each arm's same raw
samples, throughput/concurrency, bytes/GETs, RSS, cache state,
hardware/region and cost. Compare BORSUK's first pass after S3
hydration and direct S3 Vectors first pass carefully because server
cache semantics differ; Turbopuffer's published cold p90 remains
unmatched context without authenticated tenant access. The later
10M/100M scale gates require separate measured resource and quality
evidence. Lean can prove conditional algorithm and allocation bounds
under explicit data assumptions, not empirical recall or latency.

# V134 native serial route: exact planner latency rejection

One immutable Causality `c7i.8xlarge` Spot cell completed in `eu-central-1c`
on instance `i-0745fe3b7a19b5bd1`, which was independently observed
**terminated**. Source commit:
`5cb3936dbbddacbc552f558c3b9078ffeb46402c`; source archive SHA-256:
`54edbf901d3a9bbaaf003756ef36a578b093cd4dd441a119c1312e50c91e68ef`
(11,661,871 bytes). The complete exit-0 terminal is at
`s3://borsuk-bench-453182569524-euc1/research/v134-native-route/5cb3936dbbddacbc552f558c3b9078ffeb46402c/runs/v134-20260924T072127Z/a0001/terminal.json`,
SHA-256 `fec8de3f18f7ce5f4bbad736aae3cce2fdc7dac0864a18fd46a90d3abb635180`.
All 12 listed artifacts (2,030,577 bytes) were independently read back and
authenticated by length and SHA-256. The
[preregistration](v134-native-route-prereg.md) froze this gate.

Dataset: **deep-image-96-angular random 100k train subset**, 1,000
already-used **publication-test ordinals 9000–9999**, GT100 within that
subset. Candidate requests executed the production Rust PQ64 router, exact
local nominee scorer, weighted physical interval planner, authenticated
local SQ8 reads, returned SQ8 scorer, source map and exact F32 source scorer.
The paired control replayed the sealed capped ranges without control routing.
This is a serial development gate, not concurrent serving or untouched recall.

| Measured metric, 1,000 queries | Dynamic candidate | Fixed-route capped control |
| --- | ---: | ---: |
| Exact-source Recall@100, hits / 100,000 | **99.942%, 99,942** | 98.827%, 98,827 |
| Complete p50 / p95 / p99, ms/query | 90.793 / **117.916** / 123.241 | 19.982 / **24.621** / 26.104 |
| PQ64 router p50 / p95 / p99, ms/query | 1.734 / **1.799** / 1.827 | excluded |
| Exact nominee score p50 / p95 / p99, ms/query | 0.505 / 0.734 / 0.811 | included in control total |
| Exact weighted planner p50 / p95 / p99, ms/query | 42.274 / **67.602** / 72.797 | excluded |
| Local SQ8 read p50 / p95 / p99, ms/query | 13.712 / 15.212 / 15.596 | 1.906 / 3.776 / 4.346 |
| Local SQ8 bytes / range reads, all queries | 9,428,126,976 / 1,000 | 1,535,701,248 / 24,674 |

The candidate won 335 paired source-hit queries, tied 665, and lost none.
Every dynamic nominee and primary roster had the same **order**, not only
set, as sealed V122 evidence; every candidate range was byte-identical to
V122. Every per-query source ID order, hit count, candidate count, SQ8
byte/range count and authenticated source read count matched V133. The
independent recount verified all 1,000 ordinals, counters, paired outcomes
and nearest-rank timings against `summary.json` (SHA-256
`d4afe096cf69059bc6c743f434c9afbfb92a4218bf8ddf151586cbdeb1176e82`) and
`replay.jsonl` (SHA-256
`62f09819fbfad8affcf12cd5f41c2fbf4b8b173b28f3276f3f0931cbcf616dfd`).
All 2,000 arms stayed within 32 physical reads and 16,777,216 bytes/query.
The dedicated serving cgroup peaked at **138,227,712 charged bytes** (131.8
MiB), including file cache. Staging downloaded 69,718,777 authenticated
bytes in 8.206 seconds; evaluation startup authentication took 0.393 seconds,
both outside per-query timing.

**Decision: reject this exact DP planner execution path.** Candidate complete
p95 117.916 ms exceeded the preregistered 75.624654-ms screen by 42.292 ms.
The fixed-route same-quality candidate in V133 measured 47.285 ms p95 in a
separate run. The V134 router p95 was only 1.799 ms; the 67.602-ms p95
planner is the measured cause. It allocated and traversed a dynamic program
even when all positive-weight pages lay inside one affordable physical
interval. In that case the convex hull of weighted pages covers the maximum
possible vote score; one range is the fewest nonzero requests and its hull is
the smallest one-range byte span. An algebraic exact-optimum shortcut can
return it without DP while preserving weights, tie policy, caps and route.
This is a generic optimization triggered by a proven cost condition, not a
vector-count or dataset-name threshold. Prove the conditional property in
Lean and test parity against the existing DP, then preregister one new frozen
100k serial Spot cell. Do not proceed to concurrent serving or matched 1M
until that revised serial route passes. The broad 10M/100M routing problem
remains separate: a sparse nominee hull may exceed the byte budget, and
V121 already measured 184.76 ms/query flat nomination at 9.99M rows.

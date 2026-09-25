# V205 boundary-price certificate negative closeout

Source `44c573a88ac931febe0321ec81629e6a51785d84`, Causality Spot
attempt `a0001`, instance `i-0679e80ed555b71d7`, terminal SHA-256
`c3e9598689d3c6a781551d42f606e74a5e38b1e11aa614a416a38cf9d493ccdc`.
The launcher read back all six terminal artifacts and terminated the Spot
instance. `scripts/check_v205_dual_certificate.py` independently passed
closed identity, targeted tests, 1,000/1,000 V198 physical witness, price
boundary, probe-count and percentile replay.

The **already-used** ReLAION-1M D768 validation-1000 real-query hierarchy
admitted 861 linear, 85 unit-cap, 3 GET-cap, **2 boundary-certified** and
49 dense hard-DP queries. The two valid certificates were query ordinals
392 and 654, at extra unit prices 608 and 256 respectively; each used the
672-unit and 32-GET boundary. Across the 51 searched queries, the tier
spent 953 extra GET-cap solver probes. All other searches fell back to the
exact V203 two-cap solver.

| Planner-only metric | V203 baseline | V205 measured |
| --- | ---: | ---: |
| Complete hierarchy p95 | 55.588 ms | 10.231 ms |
| Complete hierarchy p99 | 58.318 ms | 66.392 ms |
| Hard-DP phase p95 | 52.384 ms | 51.476 ms |
| Certificate search p95 | — | 10.204 ms |
| Peak Rust process RSS | 43,606,016 B | 43,585,536 B |

The p95 drop is a quantile boundary effect: moving only two of the
previous 51 hard cases below the slow tier leaves 49/1,000 there, just
under a 5% tail. It does not reflect a broadly faster method. Failed
searches raised p99 by about 8.1 ms and the typical hard-case total to
about 64.8 ms. The fixed 32-probe/65,536-price policy did not run out of
probes on the 49 failed cases; the one-dimensional priced solutions
usually jump over the exact unit boundary. This is an integrality-gap
limitation of this certificate path on the cohort, not a reason to tune
prices to dataset ordinals.

Decision: reject the runtime search from the production hierarchy and
retain the exact, faster V203 implementation as the serving candidate.
The V205 helper and binary remain research code for source reproducibility;
no serving path calls them. Keep the compiled Lean theorem as a valid
conditional statement; it does not assert that a boundary winner exists.
Future work must reduce dense state work or integrate the planner with
serving and measure full query latency. V205 does not provide new recall,
S3, scale or concurrency evidence.

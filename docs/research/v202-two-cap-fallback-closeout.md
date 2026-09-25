# V202 complete two-cap hierarchy closeout

Source `576479788947be9c3d9df26a4b65b794a1079fbd`, Causality Spot
attempt `a0001`, instance `i-0516b444461a3a38e`, terminal SHA-256
`35e57bb75b854b7750c4c7e23794b3f3015f7007729135356c4d87139c2bf777`.
The terminal and six artifacts were read back, the worker was terminated,
and `scripts/check_v202_two_cap_cover.py` independently replayed all closed
identity, Rust test, tier, reference-witness and percentile checks.

The fixed cohort is **already-used** ReLAION-1M D768 validation-1000 real
queries with V198 GT-blind weights and caps. No ground truth was opened by
V202 and no new recall or end-to-end serving result was measured.

| Exact tier | Queries | Phase p50 | Phase p95 | Phase p99 |
| --- | ---: | ---: | ---: | ---: |
| Linear | 861 | 47.129 µs | 58.168 µs | 63.851 µs |
| Unit cap | 85 | 6.146 ms | 6.560 ms | 6.614 ms |
| GET cap | 3 | 0.435 ms | 0.484 ms | 0.491 ms |
| Both caps | 51 | 149.231 ms | 161.413 ms | 164.785 ms |

The unit phase ran for 139 fallback queries, and the GET phase for 54; tier
counts name where each query finished. All 1,000 final physical interval
lists, modeled masses, units and GETs matched the V198 reference exactly.
Complete hierarchy planner-core p50/p95/p99 was
**47.285 µs / 139.938 ms / 163.787 ms**. Peak Rust process RSS was
43,614,208 bytes. These times exclude feature and weight construction,
S3, reranking and concurrent throughput.

For the 51 hard queries, frozen inputs contain 590–622 sites each and all
have a 672-unit cap. The dense recurrence visits about 682,726,869
`site × GET × unit` states in total across those queries. This accounts for
the tail latency; it is algorithmic work, not local swap or S3 transport.

## Decision

Exact completeness and parity pass, but the dense 2D fallback is too slow
for the intended low-latency serving path. Keep this result as the strict
baseline. Next test a generic exact optimization that reduces reachable
states or reuses one-cap bounds without changing the objective or caps;
require the same 1,000/1,000 physical witness parity and measure the tail.
Do not combine V202 planner percentiles with V199 live S3 percentiles from
a separate run. A frozen held-out quality and end-to-end concurrent run
remain necessary after planner optimization and feature/weight integration.

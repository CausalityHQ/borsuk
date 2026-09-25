# V203 hard-DP loop closeout

Source `2cc92e1afc1d084f5979d6123eb5c092cb02bc19`, Causality Spot
attempt `a0001`, instance `i-08e96436f3b39e304`, terminal SHA-256
`8c1eb0a49c5c430c056866e06742d68fbab5c3744f4f10c416d8703f907aa4d1`.
The instance was terminated after terminal, all artifacts were read back,
and `scripts/check_v203_hard_dp_loop.py` independently passed closed input,
test, exact witness and percentile replay.

On the **already-used** ReLAION-1M D768 validation-1000 real-query split,
all 1,000 physical witnesses still exactly matched V198; tiers remained
861 linear, 85 unit-cap, 3 GET-cap and 51 hard two-cap. The only source
change hoisted mandatory-site membership out of the dense state loops.

| Planner-only metric | V202 baseline | V203 measured |
| --- | ---: | ---: |
| Hard two-cap p50 | 149.231 ms | 51.095 ms |
| Hard two-cap p95 | 161.413 ms | 52.384 ms |
| Hard two-cap p99 | 164.785 ms | 52.796 ms |
| Complete hierarchy p95 | 139.938 ms | 55.588 ms |
| Complete hierarchy p99 | 163.787 ms | 58.318 ms |
| Peak Rust process RSS | 43,614,208 B | 43,606,016 B |

The preregistered hard p95 <30 ms screen was **not met**. V203 is a
verified exact and faster baseline, not a serving-latency pass. The dense
recurrence still visits about 682.7 million states across the 51 hard
queries; its per-state trace operations are the next generic bottleneck
candidate. Test a byte trace in a separate immutable source cell while
preserving the 512 MiB trace cap and the same full reference-witness gate.
These figures exclude feature/weight construction and live S3 and do not
measure new recall, scale or end-to-end serving latency.

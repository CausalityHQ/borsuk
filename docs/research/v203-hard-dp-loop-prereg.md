# V203 hard-DP mandatory lookup hoist preregistration

V202's exact two-cap fallback matched all 51 reference witnesses but spent
161.413 ms p95 in that phase on Causality Spot c7i.12xlarge, raising the
1,000-query planner-only p95 to 139.938 ms. Its dense recurrence visits
about 682,726,869 states across the hard tier. The V202 code asks a
`BTreeSet` whether the **same site** is mandatory inside every state visit.

V203 changes only that loop-invariant test: evaluate mandatory membership
once per site, then reuse the Boolean for skip and trace behavior. All
prices, caps, tie rules, weights, frozen source files, test filter,
hierarchy order and output fields remain the same as V202. This is a
generic implementation optimization independent of dataset labels or
vector count. Its source revision and artifacts are separate from V202.

On the **already-used** ReLAION-1M D768 validation-1000 real-query cohort,
require 1,000/1,000 exact V198 interval/mass/unit/GET parity, the same
861/85/3/51 tier counts, and targeted hard-cap tests. Record complete and
per-tier p50/p95/p99 planner-core time plus RSS on one Causality Spot
instance. A hard-phase p95 below 30 ms would clear this implementation
screen; otherwise retain V202 as the exact baseline and diagnose the next
state-loop bottleneck. Neither result estimates new recall or end-to-end
latency. Terminate after terminal, read back all artifacts, and restart an
interrupted cell only at a new attempt without inspecting partial CSVs.

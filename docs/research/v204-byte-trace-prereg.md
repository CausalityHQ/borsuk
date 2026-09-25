# V204 exact hard-DP byte-trace preregistration

V203's exact hard two-cap phase measured 52.384 ms p95 on 51 of the
**already-used** ReLAION-1M D768 validation-1000 real queries. It writes
two packed Boolean trace vectors at each of roughly 682.7 million total
state visits. V204 changes only trace storage from packed Boolean to one
byte per entry; the recurrence, tie order, caps, 512 MiB trace limit,
frozen inputs, benchmark binary and remote hardware remain unchanged.

The byte trace uses exactly `2 × sites × (GET_cap+1) × (unit_cap+1)` bytes
for retained choices, bounded by the checked caller budget. The change is
generic to site/cap geometry and does not select parameters by dataset or
vector-count knee. It trades more RAM for simpler state-loop writes.

Run one Causality Spot c7i.12xlarge cell from an immutable pushed source.
Require the same 1,000/1,000 exact V198 physical witnesses, tier counts
861/85/3/51, targeted hard-cap tests, closed artifact readback and Spot
termination. Report hard-phase and full-hierarchy p50/p95/p99 plus peak
process RSS. The improvement screen is hard-phase p95 <30 ms with process
RSS <128 MiB; otherwise keep V203 as the exact baseline and diagnose the
next generic bottleneck. No new recall, end-to-end S3 latency or 100M
behavior can be inferred from this paired planner-only cell.

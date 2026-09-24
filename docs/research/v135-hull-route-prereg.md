# V135 exact hull shortcut serial-route preregistration

V134's complete production-Rust route matched every sealed nominee,
primary, physical range and source result on the deep-image 100k development
cohort, but failed the 75.624654-ms candidate p95 screen: 117.916329 ms.
Its exact weighted-interval DP alone took 67.601912 ms p95. V135 changes
only that planner's execution algorithm. If the smallest physical interval
containing every positive-vote page fits the same charged byte and range
budget, return its one-range, all-vote optimum before allocating DP state.
The positive weights, layout, route, source ranker, 32-range/16,777,216-byte
budget, and generation remain unchanged. An exact one-range all-vote plan
dominates any lower-score plan; no equal-score plan can use fewer than one
range or a shorter one-range span. The conditional argument is checked in
`formal/PhysicalIntervalBudget.lean`; code/refinement and cohort parity
remain tests, not consequences of the theorem alone.

Dataset: **deep-image-96-angular random 100k train subset**, 1,000
already-used **publication-test ordinals 9000–9999**, GT100 within that
subset. This is a development rerun, not fresh recall evidence. Reuse the
V134 harness and generation-131 authenticated local placement; candidate
timer includes router, exact nominee, planner, local SQ8 read, returned
SQ8 score, ID resolution and exact source rank. The paired capped control
replays the same fixed ranges and excludes online control routing. Execute
arms in alternating order. Require every nominee and primary order,
candidate byte range, top-100 set/order, hit count and physical cap to match
V134/V133. A mismatch fails the cell before publication as a performance
result.

The serial route passes only if all 1,000 queries preserve exact parity and
candidate complete p95 is at most **75.624654 ms/query**, the unchanged
V134 gate, with candidate planner p95 at most **5 ms/query** as a direct
screen of the intended fast path. Report p50/p95/p99 total and phase times,
raw per-query counters, startup download/authentication, charged serving
cgroup peak including file cache, source archive/generation hashes, terminal
and artifact digests. One frozen Causality Spot attempt; discard interrupted
measurement cells and terminate compute immediately after its terminal.

If V135 passes, next run a separate C1/C8/C32 concurrent end-to-end serving
gate with the same immutable generation and an explicit memory budget, then
fresh matched deep-image and ReLAION-1M builds under one source-only layout
fitter. This shortcut does not address 1M/10M/100M cases where the weighted
hull exceeds the byte budget; a different scalable route/planner remains
required before any large-scale or commercial performance claim.

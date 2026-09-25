# V192 closed-fit hard-plan screen: preregistration

## Decision

Test whether separating mandatory-page truth from optional utility
changes actual source GT100 containment under *hard* 672-unit and
32-GET per-query caps. This is a development screen on already closed
V189 fit data, not an untouched validation or production claim. It
can reject the utility before paying for a fresh candidate-generation
campaign. Its comparator is a full-rank curve fitted on the same model
split and a constant optional-risk ablation; no query ID or dataset
threshold enters the prediction rule.

## Frozen inputs and split

Use only the V189 complete attempt `a0002` sealed features
SHA-256 `7eb4833c76675534cde41330c5939599ab70d7871ac27cb365d62cdb840c3fbe`
and sealed fit labels SHA-256
`4023ade93d32e4aa4377a3f56e7e9b5d469468396e96459715caa5f55394ebf4`.
The 128 ReLAION-1M D768 source pseudoqueries are V189 fit ordinals
2432–2559. Fit both utility models on 2432–2495 (64 queries), select
each model's shared price separately on 2496–2527 (32 queries), and
inspect the frozen selections on 2528–2559 (32 queries). All three portions
are reused developmental fit data; no V189/V190 holdout truth enters.

Use V189's fixed 35-pair price grid: unit prices
`1000, 2000, 3000, 4000, 6000, 10000, 20000` times GET prices
`0, 50000, 100000, 200000, 400000`, all in millionth-hit units.
At each price, use the exact hard-capped reference with mandatory
coverage, `page_count=31250`, `max_units=672`, `max_gets=32`,
and explicit trace data allowance 512 MiB. Reject a price if any
query is infeasible or if its 32-query total exceeds 14,274 units or
708 GETs (half of V189's 64-query scaled caps). Among feasible prices,
maximize each model's covered modeled mass; tie break by fewer units,
fewer GETs, then lower prices. Do not use price-fit labels in this
selection. The full-rank control gets its own refitted price because
a global scale difference can otherwise be absorbed by price. Run
the constant-risk ablation at the new utility's selected price.

## Diagnostic outputs and decision

The pre-screen on all 128 closed fit queries has already established a
candidate-restricted truth-oracle witness at 12,779/12,800 hits,
p05 99, 150,259,200 planned bytes and 2,359 GETs. The paired
mandatory-only floor was 12,579 hits, p05 93. The oracle uses GT100
at planning time, so it is an attainability reference only.

For the price-fit and diagnostic portions, independently recount
mandatory coverage, interval units including bridges, GETs, planned
bytes at 24,960 bytes/unit, candidate omissions, full GT100 hits,
aggregate hits and nearest-rank empirical p05. Include the fitted
full-rank curve at its own fit-selected price under the same hard
caps, plus the constant-risk ablation at the new price. Report every infeasible query
as failure, never as zero recall. Measure offline wall time, CPU time
and peak process RSS separately from any serving claim.
Also run the V189-style ranked greedy control with a 446-unit base
allowance elastic to each query's mandatory floor, bounded by 672
units and 32 GETs. It may exceed the aggregate GET envelope; report
that excess rather than treating its extra quality as a matched win.

If either model cannot select a feasible price, record that failure
without assigning zero recall. If the new policy does not improve
the diagnostic physical hit/resource frontier and p05 over both
controls, revise the utility or candidate generator before a fresh
campaign. The next utility candidate should test row-level SQ8 score
gaps to the primary boundary, since optional rank order alone leaves
the same ordering as the prior model. Even a diagnostic win only
licenses a preregistered fresh 100k transfer and a 1M source holdout
of at least 512 queries with an uncertainty interval for tail failures,
then a distinct dataset/distribution transfer and live S3 returned
quality, latency, charged RAM and cost. Old closed labels cannot
promote this architecture.

Run on one Causality Spot instance, upload source/inputs/terminal-listed
artifacts to an immutable S3 attempt, discard an interrupted cell,
replay terminal evidence, and terminate the instance immediately after
its terminal marker. Keep the swap-pressured devbox to narrow tests.

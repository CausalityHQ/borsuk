# ReLAION-1M OPQ8 final-page feasibility gate

## Question

The terminal-closed OPQ8 1M route selected 98,985/100,000 GT100 owner
groups under a projected 96-byte code wave. A final data wave may admit
only 32 physical pages and 16,777,216 encoded bytes. Before training or
reading any 96-byte row code, first determine whether these sealed group
plans can satisfy the final-page quality gate even with truth-aware page
selection. If they can, run a separate source-vector nomination stage.
These are source-only decisions about the page cap, nomination rule and
layout. They are not production search or latency runs.

## Frozen authority and phase order

Use the exact five source and two development identities in
`scripts/native_one_million_selector_cell.py`, and the completed OPQ8 1M
attempt under source commit `cc0dd60de8b63a87656475591be12127ad4f3769`.
Authenticate its terminal and every reused artifact. In particular,
`plans.json` has SHA-256
`cb39314a53f0030f57386887f3fa7213a3e5e869610dcdc107f28b1c7a4fbe08`
and `plan-seal.json` has SHA-256
`ee4c9c4c91b2197c16d07a91c726c8d5401fb22f14950470d32f3bfb8250ae4b`.
Replay candidate and page-centroid control selected groups, merged
intervals, projected GETs and bytes exactly. Do not rank or replan.

For Stage A, authenticate the frozen base/delta Arrow runs and generation
before building stable-ID-to-page, page-size and physical-row maps. Read
truth only in the oracle evaluator. Keep 1,000 ordered queries and their
100 ordered, distinct truth IDs, with a separate first-ten mask. Stage A
authenticates the query file to bind the cohort but does not score
queries; it may close immediately if the necessary bound
fails. If Stage A passes, a separately sealed Stage B authenticates source
parquet, computes query-only source scores and page nominations, and
seals those outputs before truth is downloaded or visible to its
evaluator. Stage B replays the same source, plan and cohort identities.

## Cheap necessary upper bound

In Stage A, for each query and arm count how many GT positions belong to each
physical page inside selected groups. The sum of the 32 largest page
counts is an optimistic upper bound on any 32-page final wave; it ignores
the byte cap and does not predict a real scorer. This calculation needs
truth and belongs only to Stage A's oracle evaluation. If the candidate's
upper bound misses any fixed absolute gate below, stop before Stage B and
PQ96: no code representation or nomination rule with that
32-page cap can pass. Record group containment and the bound as separate
metrics. Also report the corresponding control bound.
The GT100 and GT10 optimistic bounds may use different page selections;
each is a separate necessary condition, not a jointly attainable plan.
Because ten GT positions occupy at most ten pages, the GT10 bound equals
selected-group GT10 containment under this 32-page cap. Report it for
completeness, not as a new selectivity test.

## Source-vector page nomination

Only if Stage A passes, before truth is visible in Stage B, stream authenticated source rows in bounded
batches. Compute squared L2 in float64 by explicit subtraction and
square summation for only each arm's sealed selected rows, with a fixed
stable-ID tie rule. Apply the unchanged `nominate_pages` rule: count the
top 100 selected rows per query by `(distance, source ordinal)`, rank
their owner pages by count, nearest score and page ordinal, then fill
remaining pages by nearest score. Skip a page that exceeds the remaining
data-byte budget and continue until at most 32 pages are selected.
Use authenticated encoded page byte lengths from the generation manifest.
Seal per-query page selections, data bytes, score arithmetic version,
query and source identities before truth evaluation. Score only the
candidate and contemporaneous page-centroid control; the prior
source-distance group-ranking diagnostic is not a control here.

The candidate advances to a real 96-byte code gate only if its nominated
pages contain at least 98,151 GT100 positions, p05 at least 90 GT100
positions per query, at least 9,928 GT10 positions, and at most 49
queries below 90 GT100 positions. The selected-group upper bound must
also meet those absolute values. Both arms must obey 32 pages and
16,777,216 data bytes for every query. Record candidate versus control
paired better/worse/tied query counts and both aggregate outcomes. A
valid miss means the fixed OPQ8/group-layout/32-page combination is
insufficient; revise page locality, nomination or final-wave budget
before training PQ96. Do not tune it on this development cohort.

## Run and closeout

Run one create-only/readback-verified Causality Spot attempt from a
pushed source archive with a 3-GiB process-tree cap, 64-MiB margin and
zero swap. On Spot interruption, publish a failed terminal, discard
partial measurement cells and restart the whole cell at a new attempt
ordinal from unchanged source. Stop compute immediately after terminal.
Monitor incomplete work only by terminal and infrastructure health.
After closure, authenticate all terminal-listed artifacts and
independently recompute the page-owner hit masks, optimistic 32-page
bound, per-query page/data caps, p05 and aggregate decision. The
source-score stage is a diagnostic finite-precision calculation, not a
formal proof of real-arithmetic nearest-neighbor order.

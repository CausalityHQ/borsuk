# V184 full-source aggregate resource oracle preregistration

## Decision

V183's GT-blind source plan passed quality only at 3.397 GB across 128
queries, versus 1.425 GB of V155-scaled bytes. This truth-aware **kill
gate** asks whether the V164 whole-32-row-unit layout and V182 mandatory
primary policy can meet the same source quality threshold under V155-scaled
aggregate resources at all. It reuses the already closed ReLAION-1M D768
source pseudoquery SHA ranks 1281–1408 (128 V182 holdout queries), so it is
a capacity certificate on a development panel, not fresh validation or
returned Recall@100.

Recompute exact float64 cosine GT100 from the frozen source parquet, exclude
each query row, and map **every** truth ID through the authenticated V164
physical permutation. Rehash V182's sealed GT-blind rosters and independently
check its candidate-only labels against the full truth units. This removes
V183's 20-position outside-candidate band.

The permitted plan family is whole 32-row SQ8 units in disjoint contiguous
intervals, at most 32 GETs per query, containing every V182 mandatory
primary unit. For each query compute the exact minimum-unit frontier indexed
by GET allowance and exact truth hits. Trim endpoints to mandatory or
truth-bearing units. Across 128 queries require at least 12,745/12,800
source hits and p05 at least 98, equivalent to at most 55 misses and at most
six queries below 98 hits. V155's used-validation mean scaled to this panel
allows at most **57,097 complete units** (1,425,141,120 bytes) and **2,832
GETs** after flooring both aggregate caps. The published V155 exact totals
are 11,134,007,040 planned bytes and 22,126 GETs for 1,000 used queries;
this source oracle is unpaired and cannot establish a product gain.

Use a nonnegative GET price to relax the aggregate GET cap, while the global
DP enforces the miss and p05-tail constraints exactly. For each evaluated
price, `minimum(units + price × GETs) − price × 2,832` is a valid lower bound
on units for every feasible plan. Search prices by doubling then integer
binary search around the point where the priced optimum uses at most 2,832
GETs; retain every evaluated price and the strongest lower bound. If that
bound exceeds 57,097 units, certify the current plan family infeasible. If
a priced optimum itself meets both aggregate caps, reconstruct every
interval and independently revalidate full truth, mandatory coverage,
bytes, GETs and p05 before declaring a truth-aware **feasibility witness**.
Otherwise report an unresolved bound/witness interval and proceed to exact
global GET-constrained DP; do not infer feasibility or impossibility.

Even a feasible oracle only shows that a truth-aware allocator can fit this
layout. A GT-blind method still needs a difficulty predictor, a fresh sealed
source panel, then paired used ReLAION-1M validation-1000 returned recall,
latency and charged serving RAM. An infeasible oracle identifies the
current layout/mandatory policy as the responsible layer within this plan
family; it does not rule out a new layout, partial-unit reads or a changed
primary policy. No dataset-specific query exception or fixed 100M memory
knee follows from this gate.

Run one immutable Causality Spot cell with exact source/input digests,
closed V182 terminal identity, raw truth-unit and frontier artifacts, a
terminal hash and immediate instance termination. If interrupted, discard
the cell and restart a new attempt.

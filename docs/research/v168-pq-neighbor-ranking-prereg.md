# V168 direct PQ neighbor ranking: preregistration

## One decision

Test whether direct PQ64 row scores in **adjacent-only** 32-row units
rank useful non-nominee SQ8 rows better than a query's nearest
neighboring nominee PQ rank. If the direct score has no transferable
advantage, stop this exact min-ADC representation before implementing a
new interval scheduler. A pass licenses only a separately frozen
GET/byte-aware planner test, not Recall@100, latency, a production
default, or 10M/100M qualification.

Use the ReLAION-1M D768 source, V70 SQ8, V63 old order, V115 PQ64 router
and V164 source-only order with the exact S3 key, length and SHA-256
identities pinned by the launcher. The source archive comes from one
clean pushed `origin/main` commit. The score field must use
`scripts/v168_scored_neighbor_field.py` with V114's float32 PQ64 ADC
arithmetic; it reads resident router codes and **no remote SQ8 range**
for candidate scoring.

## Fresh panel and features

Sort all source stable IDs by the existing fixed V166 pseudoquery SHA-256
domain and numeric tie break. V166 used human ranks 1–256 and V167 used
257–512. This cell uses exactly ranks **513–640**, with zero-based
`query_ordinal` 512–639. These are source rows already present in
router/layout training, so this panel is an internal source proxy.
No external validation truth is opened.

For each query, reproduce the frozen 512 PQ64 nominees, excluding the
query's own old physical row before selecting the SQ8 top-100 primary
roster and its 100th SQ8 threshold. The declared candidate universe is
the nominees' 32-row physical units and immediate neighbor units.
Labels count non-nominee SQ8 rows at or below that threshold, excluding
the query row. Candidate scoring never reads those labels.

An adjacent-only unit has no nominee row. For each such unit, freeze
two ascending score features:

1. **Direct field:** minimum PQ64 ADC score among its rows, excluding the
   query row if present. A unit with no remaining row receives `+∞`.
2. **Neighbor nominee:** minimum 1-based PQ rank of a nominee in either
   immediately neighboring unit. Because the unit is adjacent-only,
   at least one such rank exists. This is a strong current-information
   control, not the V165 physical plan.

Tie break by the physical unit ordinal. Select the first `min(32, U)`
adjacent-only units per query for each feature, where `U` is that
query's adjacent-only unit count. Also report `min(16, U)` and
`min(64, U)` as predeclared sensitivity points. The actual captured
count is the sum of sealed positive-row counts in selected units. The
same candidate set and unit budget are used for both features. The
preparation phase writes and hashes features separately from labels;
the plan phase ranks units from features only and seals all selected
unit IDs before the evaluation phase opens labels.

## Frozen decision

For each of the 128 queries, compute `direct capture − neighbor-rank
capture` at 32 units. Run 10,000 paired bootstrap resamples of 128
queries with NumPy `PCG64` seed 168; use the nearest-rank 2.5th and
97.5th percentiles of the bootstrap **total** difference. Advance the
representation only if:

- the 32-unit aggregate direct gain is at least **8 useful rows**;
- the paired 95% bootstrap lower endpoint is strictly above zero;
- direct aggregate capture at the 16- and 64-unit points is no lower
  than the neighbor-rank control; and
- all 128 candidate sets, feature plans and proxy labels pass the
  independent identity and geometry checker.

If there are no positive adjacent-only rows, classify the test as
uninformative and stop this cell without promoting the representation.
If any gate fails, kill the min-ADC ranking feature; the result does not
prove that other PQ features or layouts cannot work. Report total
positive adjacent-only rows, positive units, candidate units, capture
at every budget, wins/ties/losses, the bootstrap interval, source/score
resource use and the exact stop reason. Do not tune the feature or
unit budget on these 128 labels.

## Resource and method limits

The field scans at most 49,152 row codes or 3,145,728 PQ table lookups
after a 512-row roster. `formal/ScoredNeighborField.lean` proves this
arithmetic from authenticated candidate-unit and row bounds. It does
not bound V115's flat summary router, which measured 184.76 ms/query
offline on the D96 9.99M screen, nor prove PQ discrimination or actual
latency. A complete resident PQ64 plane is 64N bytes per generation;
memory may scale with N and requested recall. Source-parquet prepare
RSS is offline build memory, not serving RAM.

Launch one Causality `c7i.12xlarge` Spot cell with a unique attempt and
four-hour hard shutdown. On interruption, discard the whole cell and
restart under a new attempt. Upload all artifacts and a terminal marker
with instance, commit, archive and artifact hashes; stream and rehash
all terminal-listed S3 objects, then terminate the instance immediately.
Monitor incomplete work only through the terminal and infrastructure
health. Do not inspect incomplete measurement files or overlap a second
V168 run.

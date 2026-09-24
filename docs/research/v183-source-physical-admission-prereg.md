# V183 source physical admission preregistration

V182 passed a width-32 PQ unit-rank screen, but its top-unit counts ignored
contiguous GETs and mandatory SQ8 primary pages. V183 asks whether a generic
ranked greedy interval cover can preserve that source signal with real unit
and GET accounting. It is an exploratory physical gate on **closed V182
ReLAION-1M D768 source pseudoquery SHA ranks 1153–1408**; the fit split is
1153–1280 and holdout 1281–1408, 128 queries each. V182 labels are already
closed, so this is a development test, not a fresh independent holdout.

Use V182's immutable GT-blind width-32 PQ rankings and mandatory primary
units. Before reading either label file, seal all V183 plans to S3. For each
query, compute the exact minimum mandatory cover for the arm's GET cap.
Raise the arm's unit allowance to that floor when necessary, then greedily
admit ranked optional units only when their new exact cover floor fits.
Construct the minimum cover by joining the shortest gaps, breaking equal
gaps by physical position. The rule depends only on rank, geometry and
caller resource inputs; it does not inspect truth or recognize query IDs.
It is a deterministic heuristic, not a claim of globally optimal utility.

Predeclare three exploratory arms:

| Arm | GET cap | Base complete 32-row SQ8 units |
|---|---:|---:|
| `v155_mean` | 22 | 446 |
| `elastic_16m` | 32 | 672 |
| `elastic_32m` | 32 | 1,344 |

The first arm rounds down V155's 22,126 planned GETs and 11,134,007,040
planned bytes per 1,000 used validation queries into per-query mean caps.
All arms can exceed their base unit allowance only when the authenticated
mandatory cover floor requires it. These are research comparator policies,
not production defaults or a vector-count knee.

After the plan seal, score exact-source truth positions **known inside V182's
width-32 candidate universe** if their physical unit falls in an interval.
That is a lower bound on true source containment because a bridged interval
could also fetch truth outside the candidate universe. Report an upper bound
by adding each query's missed candidate truth positions. Report per-query
p05 lower and upper bounds, exact bytes, GETs, mandatory floors and aggregate
resources. A lower bound of at least 12,745/12,800 with p05 at least 98,
and aggregate resources no worse than V155's scaled 128-query totals, would
advance to an independent used-query paired gate. If only a looser arm meets
quality, revise global resource allocation or layout. If no arm's upper
bound meets quality, revise candidate/utility/physical layout. An interval
between lower and upper thresholds is inconclusive and requires full source
truth mapping on a fresh panel. No result is returned Recall@100 or S3
latency/RAM.

Use one immutable Causality Spot cell. Rehash closed V182 terminal inputs,
upload the V183 GT-blind plan seal before downloading labels, rehash all
terminal artifacts and terminate compute immediately.

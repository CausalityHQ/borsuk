# V177 closed source-only candidate ceiling

## Authority and decision

Commit `3d85095e5094742fc1272824dda24263f5ea3fc5`; Causality Spot
`c7i.12xlarge` instance `i-02143bc7b3c209913`, terminated after a complete
terminal. Attempt prefix:
`s3://borsuk-bench-453182569524-euc1/research/v177-source-candidate-ceiling/3d85095e5094742fc1272824dda24263f5ea3fc5/runs/a0001/`.
Terminal SHA256 `41acf150e2c76eae346d56fd9db049321348932fd934d8fa58a8ab9fdbc559e7`.
The launcher streamed and rehashed every closed terminal artifact. The
independent readback of the pre-label S3 seal and roster matched their terminal
digests `b39e8744...` and `6be71fbd...`.

The preregistered width-8 source-only ceiling **passes**: fit 6,388/6,400
and holdout 6,387/6,400 exact source neighbors; the holdout threshold was
6,373/6,400. This advances to a source-calibrated, physical-budget planner
gate. It does **not** establish returned recall, a feasible 16-MiB/32-GET
plan, latency or serving RAM.

## Measured source-only evidence

Dataset is **ReLAION-1M D768 source pseudoquery SHA ranks 641–768**. Fit is
ranks 641–704 (64 queries), holdout is ranks 705–768 (64 queries). Each truth
set contains the 100 highest exact float64 cosine source neighbors after
excluding the query row. Counts are exact truth rows present in the candidate
union, not returned rows. `p05` is the fourth smallest of 64 per-query counts.

| Neighbor width, 32-row units | Fit hits / 6,400 | Fit p05 / 100 | Holdout hits / 6,400 | Holdout p05 / 100 |
|---:|---:|---:|---:|---:|
| 0 | 6,299 | 92 | 6,349 | 95 |
| 1 | 6,332 | 95 | 6,374 | 97 |
| 2 | 6,348 | 96 | 6,381 | 98 |
| 4 | 6,366 | 97 | 6,387 | 99 |
| 8 | 6,388 | 98 | 6,387 | 99 |

The strongest existing paired product comparator remains **V155 used
ReLAION-1M D768 validation-1000** exact-source Recall@100 99,567/100,000,
p05 98, at 11,134,007,040 planned bytes and 22,126 planned GETs. V177 has
different source-derived queries and no plan; its numbers cannot be subtracted
from V155 as a product gain. No S3 Vectors or Turbopuffer paired cell exists
for this revision.

Raw width-8 union size across all 128 queries: minimum 107, median 650.5,
p95 1,664, maximum 3,083 physical 32-row units. A unit is 24,960 SQ8 bytes,
so the 16-MiB physical cap accommodates 672 complete units. **62/128** full
width-8 unions exceed that cap. This is a resource warning about fetching the
entire universe, not evidence that a selective plan is infeasible. Exact
primary units have median 38.5 per query (fit range 12–79; holdout 5–75).

The remote prepare process took 22.49 seconds wall time and peaked at
9,355,724 KiB RSS; exact-source evaluation took 22.19 seconds and peaked at
9,482,196 KiB RSS. These are **offline experiment-process** resources on the
Spot machine, not production query latency or serving memory. The current
devbox ran only narrow local geometry tests (2 passed) and Python syntax
checks; no local full suite ran under swap pressure.

## Next gate

Use the sealed source-fit panel to calibrate direct PQ optional-unit utility
with query exclusion; evaluate reliability on sealed source holdout. Generate
GT-blind query plans that always cover primary units, charge complete
32-row SQ8 units and contiguous GET intervals, and enforce 32 GETs/16 MiB
per query. Report coverage missed outside the candidate universe separately
from loss caused by the physical cap and from utility ranking error. Width
and resources are selected by caller `k`/recall target and measured cost, not
by a ReLAION identifier or a vector-count knee. A plan earning a source gate
then needs the paired **used ReLAION-1M validation-1000** external-query test
against V155 under disclosed equivalent conditions.

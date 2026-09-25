# V190 closed frozen rank-price replication

## Decision

Reject the V189 rank model with its frozen `(unit=6000, GET=50000)`
price as a production candidate. On an untouched 256-query ReLAION-1M
source panel it fetched **25,396/25,600** exact GT100 rows, p05 **97**,
with one per-query plan above the 672-unit cap. The preregistered gate
was ≥25,490, p05 ≥98, zero infeasible queries, ≤2,850,305,802 planned
bytes and ≤5,664 GETs. Actual planned charges were 1,499,596,800
bytes and 4,744 GETs, including the infeasible plan. Lower aggregate
resource use does not rescue the failed quality and physical cap.

The paired V187-style greedy control fetched 25,512/25,600, p05 98,
at 2,783,938,560 bytes and **7,465 GETs**, so it too fails the matched
GET envelope. Candidate truth ceiling was 25,562 and top-446 cosine
unit truth was 25,559. The candidate generator retains enough source
truth on this panel; the current optional-unit allocation/utility loses
the gate under the GET cap. This is source containment, not returned
Recall@100 or live S3 performance.

The rank-price optimizer enforces 32 GETs but only checks the 672-unit
cap after optimization. Query ordinal **2871** (source ID 69567604)
returned 935 units/32 GETs and was marked infeasible. The paired greedy
plan for this same query covers its mandatory units in 619 units/32
GETs and captures 99 truth rows. Thus the failure is a missing hard
unit constraint in the priced planner, not intrinsic mandatory-cover
infeasibility. The 935-unit plan would capture 99 if its cap were
ignored, but that is an invalid counterfactual. Replacing the failed
query with its greedy result in a **posthoc diagnostic** would yield
25,495 aggregate hits, above the aggregate threshold, while p05 would
still be 97. Thirteen other rank-plan queries fetched at most 97 rows,
from a mix of candidate omissions and optional-unit misses. Hard-cap
repair alone therefore cannot pass the frozen policy's tail gate.

The next design needs a hard unit/GET-constrained interval planner and
a calibrated tail/reliability decision that can spend resources where
the source score evidence supports it. Diagnose margin-price rejection
on V189's closed **fit** split, not V189/V190 holdout truth, and test
any revised generic method on a fresh preregistered panel. No query-ID
exception, holdout price retuning, fixed 100M RAM knee or production
default is licensed by this failure. Lean can certify conditional caps
and recall under explicit error premises; V190 falsifies any assumption
that the frozen rank curve plus price already supplies the needed
empirical per-query error bound.

## Closed authority and replay

Frozen source commit `b5ae9ce5d9fdeae47ee4047d5e6b3eff08d9ad3d`,
one Causality Spot `c7i.12xlarge` worker `i-09abc04fe628a51f7`,
terminated immediately after a complete terminal. Attempt:
`s3://borsuk-bench-453182569524-euc1/research/v190-frozen-rank-replication/b5ae9ce5d9fdeae47ee4047d5e6b3eff08d9ad3d/runs/a0001/`.
Terminal SHA-256 `1df05ffc448d324ba145943c9d21c0fb70b10dbb370fb76a85d3bc57d96b9d94`;
summary SHA-256 `35b8b4e97b57b6fe9a64811d9f6a8d7bbd1ebb01730db7616e491ed99b780d4d`;
feature SHA-256 `cfe9d0ed21147045355f233089a526cb67324667c33648edcbe56b45d56bf694`.
The controller rehashed all terminal artifacts and S3 pretruth seals,
checked seal publication before source labels, and confirmed termination.
Independent closed replay matched 256 SHA-ranked query IDs, exact GT100
masses, model/plan seals, every mandatory interval, bridge charge, GET,
byte count, per-query hit count and both summary arms. No incomplete
measurement file was read.

## Measured scope and resources

Dataset **ReLAION-1M D768**, source pseudoquery SHA ranks 2689–2944,
all 256 held out from V189's fit/holdout. Exact float64 cosine GT100
excludes each query row. The strongest BORSUK returned-quality comparator
remains **V155 used ReLAION-1M D768 validation-1000**, actual returned
exact-source Recall@100 99,567/100,000, p05 98, at 11,134,007,040
planned bytes and 22,126 GETs. It is unpaired with this source panel.
No paired current-revision S3 Vectors or Turbopuffer measurement exists.

Spot source preparation took 74.74 seconds and peaked at 9,261,128 KiB
RSS; 256 offline plans took 12.03 seconds and peaked at 97,752 KiB RSS;
truth evaluation and replay took 43.61 seconds and peaked at 9,493,568
KiB RSS. The isolated Python 256-plan section used 11.813 CPU seconds
and 11.814 wall seconds. These are offline-process measurements, not
serving latency or charged production RAM. The devbox ran narrow tests
and one Lean file, not a full suite, during swap pressure.

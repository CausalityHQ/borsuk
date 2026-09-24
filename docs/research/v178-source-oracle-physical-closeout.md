# V178 closed source-oracle physical feasibility

## Decision

**Stop promoting the V164 relaid layout with V177's mandatory primary roster
under a fixed 32-GET/16-MiB per-query cap.** The source-only omniscient unit
transport oracle passes, but one query's mandatory primary cover is
mathematically infeasible at that cap. This is a physical layout/scheduling
failure, not evidence that the PQ representation cannot capture the truth.
Do not label the result as measured returned recall or as a generic failure
at 1M or 100M rows.

The revised method derives each query's minimum primary cover from its
physical geometry, reports infeasibility when the caller's hard cap is below
that minimum, and allows a larger per-query budget when the caller permits
it. A global allocator must then price optional units against measured GET
costs and an aggregate resource target. This rule applies to any corpus and
recall target; it has no ReLAION/query-ID special case or vector-count knee.
The next real-query gate still has to establish paired quality, planned
bytes/GETs, latency and charged serving memory. A 32-MiB exploratory
per-query allowance would cover the observed mandatory minimum, but has not
been tested or selected as a production default.

## Closed authority

Frozen source commit `910229f936368bf409ea08215790c234ecef9eb9`, one
Causality Spot `c7i.12xlarge` instance `i-0a81a6bce358fc4ae`, attempt
`s3://borsuk-bench-453182569524-euc1/research/v178-source-oracle-physical/910229f936368bf409ea08215790c234ecef9eb9/runs/a0001/`.
Terminal SHA256 `bf415a5a80956225dd2e01a5324b1cc61d611031fc072e952fda4878a6b70ee7`;
summary SHA256 `e88756971165a29e0a9cff3051366e6e051e1b0d74991ceee9d19087c3926ee6`;
plans SHA256 `dd93bf3ad79856970987be514435f2ebffeac93f4cd0de81d1e5d552d54f8cc6`.
The launcher streamed and rehashed every terminal artifact and terminated the
instance. The independent checker rehashed the closed V177 rosters and V178
terminal/summary/plans; its 128-query result is in
`v178-primary-gap-certificate.json`. No incomplete measurement file was read.

## Source-only measured results

Dataset: **ReLAION-1M D768 source pseudoquery SHA ranks 641–768**, fit
641–704 and holdout 705–768, 64 queries each. Metric: exact source truth
neighbors contained in truth-aware whole-unit interval plans, not returned
Recall@100. Every arm uses at most 32 GETs and 672 complete 32-row SQ8 units
(16,773,120 bytes). The qualification threshold was 12,745/12,800 truth
positions plus p05 at least 98; it transfers the rate of the **unpaired**
V155 used ReLAION-1M D768 validation-1000 baseline, whose actual returned
exact-source Recall@100 was 99,567/100,000 with p05 98, 11,134,007,040
planned bytes and 22,126 planned GETs.

| Truth-aware arm | Fit hits / 6,400 | Holdout hits / 6,400 | Total / 12,800 | p05 / 100 | Decision |
|---|---:|---:|---:|---:|---|
| No mandatory units; unit-transport upper bound | 6,399 | 6,400 | 12,799 | 100 | Pass |
| All sealed V177 primary units mandatory | 6,299 | 6,400 | 12,699 | 100 | Fail: one infeasible query |
| Mandatory units; width-8 truth-weighted witness | 6,291 | 6,388 | 12,679 | 99 | Fail |

The all-truth unit-transport oracle misses one truth position, on query 653.
The primary-constrained arm instead marks query 653 infeasible and assigns it
zero hits; query 690 contributes the other one-position miss. That zero is a
decision accounting convention for an invalid plan, not a measurement of
what an unconstrained search would return. The width-8 arm is a feasible
truth-aware witness for other queries, not an upper bound on every possible
candidate policy. These cohorts and oracle plans cannot be compared as a
paired product improvement over V155 or either commercial service.

For query 653 (stable ID 67,595,616), the sealed primary roster has 71
distinct units in 57 disconnected runs. A 32-GET cover must join at least
25 run gaps. The sum of the 25 shortest gaps is **752 units**; therefore any
such cover fetches at least **823 units = 20,542,080 bytes**, exceeding the
16,777,216-byte cap by **3,764,864 bytes**. The independent certificate finds
this as the sole mandatory-cover failure among all 128 queries and agrees
with the remote DP. `formal/PrimaryRunBudget.lean` checks that a certified
752-unit gap floor implies the byte conflict. The gap certificate is derived
from the closed roster; Lean does not prove the roster or measured recall.

The offline oracle process took 2.12 seconds wall time and peaked at
971,648 KiB RSS on the Spot worker. These are experiment-process resources,
not serving query latency or charged serving RAM. No S3 Vectors or
Turbopuffer paired measurement exists for this revision.

## Next gate

Build a generic planner that first computes the exact minimum mandatory
GET/byte frontier, admits a query only under the caller's cap, then selects
optional units using a source-fit utility model and measured object-store
costs. Freeze its source-fit calibration and holdout reliability before any
external-query truth read. On the **used ReLAION-1M D768 validation-1000**
roster, seal GT-blind plans, verify every query's physical cover and the
aggregate bytes/GETs against V155, then score returned exact-source results.
A passing used-cohort result must be repeated on a frozen holdout before
production defaults or larger-scale claims are made.

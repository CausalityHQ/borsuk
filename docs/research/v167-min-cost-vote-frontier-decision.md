# V167 minimum-cost vote frontier: method decision

## Why change the objective

V165's 32-row unit plan maximizes every positive 513/1 primary/secondary
vote up to 32 GETs and 16,777,216 encoded bytes. It spent 13.545 GB per
1,000 used ReLAION-1M validation queries, 21.65% more than V155's
11.134 GB. V166's isotropic moment surrogate failed a separate GT-blind
source-pseudoquery ranking gate, 380 versus 434 actual above-threshold
non-nominee rows captured at per-query matched caps. The source/SQ8 score
boundary mismatch was small; per-unit Gaussian tail predictions assigned
zero planner mass to most positive units. It is killed, with no tuning on
that holdout.

A postterminal replay of unchanged V165 voting on V166's **used** source
pseudoqueries gives a causal lead, not a quality promotion. At a uniform
11,134,007-B/query cap it captured 420 of the 434 rows its 16-MiB plan
captured, using 1.199 versus 1.700 GB across 128 queries; GETs rose from
2,088 to 2,263. The 16-MiB arm exactly reproduced its sealed plans. This
suggests that some V165 votes can be forgone at materially lower bytes
on this internal panel; it does not measure the V167 frontier.
The source-pseudoquery cohort, candidate-universe proxy and increased
GETs prevent inference about Recall@100 or live latency.

## Selected method

Keep the source-only V163/V164 physical-order rule, PQ64 nominees,
exact-SQ8 primary 100, and 32-row fetch units. Change only the physical
admission objective to a **minimum encoded-byte frontier**:

1. Compute the maximum 513/1 vote score attainable under the explicit
   32-GET/16-MiB operator cap. Primary votes dominate all secondary votes,
   so a feasible plan must retain every exact-primary unit.
2. Separate that score into all 100 primary votes weighted at 513 each,
   plus the achievable secondary-vote count `Smax`. A profile supplies a retained fraction
   `f ∈ (0,1]`; its target is all primary votes plus `ceil(f·Smax)`
   secondary votes. This target is defined from each query's authenticated
   roster, with no corpus-name or N threshold.
3. Find the smallest integer number of 32-row units for which the unchanged
   weighted-interval DP can meet the target within 32 GETs. Reconstruct
   its plan and charge the full contiguous ranges, including gap units.
   On ties, minimize GETs and then use the frozen physical-order tie rule.
   If the initial 16-MiB plan cannot retain all primary units, raise an
   explicit primary-infeasibility error and fail this experiment cell.
   Do not claim this profile supports that request.

The primary-preservation rule is an invariant, not a recall certificate.
Increasing `f` cannot reduce the minimum feasible charged byte cost for a
fixed query/model. `formal/SmoothPageBudget.lean` proves this conditional
minimum-cost monotonicity over plans that satisfy ordered targets; a
small-geometry exhaustive test must check the implementation's optimum
and tie behavior. The resident index layout is unchanged in this slice.
Higher quality may cost more fetched bytes/GETs; a later measured profile
may also elect more resident routing or source memory, admitted with the
existing checked generation-resource formula. There is no vector-count
memory knee or silent 100M cap.

## Calibration and gates

Do not fit `f` on V166 source pseudoqueries 1–256, their closed results,
or used ReLAION validation truth. Before opening any new panel, freeze
the exact rational candidate grid `{4/5, 9/10, 19/20, 39/40, 99/100,
1/1}` and the selection rule. Hash-selected source pseudoqueries
257–384 may fit the smallest `f` that captures at least 99% of the
full-cap actual above-threshold non-nominee rows in aggregate within a
declared candidate universe. At least 95% of these 128 queries must
lose at most one such row relative to their full-cap plans (equivalently,
the empirical nearest-rank p95 loss is at most one). Pseudoqueries
385–512 are disjoint holdout IDs and must pass **both** quality checks
at the selected `f`, while using at most 90% of full-cap aggregate
planned bytes and at most 120% of its aggregate GETs. Every query
still obeys 32 GETs and 16 MiB. Kill the stopping method if no grid
value passes fit, the selected `f` is 1/1, or any holdout condition
fails. These rows were still present in router and layout training, so
this is internal calibration only. A quality profile may not be called
a Recall@100 guarantee from this proxy.

The cheapest external decision is one frozen ReLAION-100k D768
development-1000 paired gate under the V163 source-only L2 order and
V114 nominee/primary rosters. Compare returned Recall@100, p05,
Recall@10, planned bytes and GETs against V163's 99.322%, p05 98,
99.49% and 15.657 GB/15,275 GETs per 1,000, and V160's stronger
resource point of 15.563 GB/9,894 GETs. Advance only as a candidate if
returned quality does not fall below V163's measured total or p05,
planned bytes are at most 90% of V160's frozen exact byte total,
aggregate GETs are no greater than V163's 15,275, and every query
obeys the 32-GET/16-MiB cap. This
gate cannot freeze a default without paired live latency and cost. A
source-pseudoquery calibration pass alone does not advance.

Only a 100k advance permits a separately frozen paired ReLAION-1M
D768 returned-quality gate against V155 validation-1000: 99.567%
exact-source Recall@100, p05 98, 11.134 GB planned SQ8 bytes and
22,126 GETs per 1,000. The 1M gate must seal plans and returned IDs
before GT, then report SQ8-only and exact-source tails, actual physical
coverage, bytes, GETs and paired uncertainty. If either resource or
quality loses, diagnose that layer and stop; do not tune `f` on the
used validation cohort. A matched live-S3 latency and charged-RAM gate
follows only from a frozen architecture.

This objective does not solve the flat PQ64 router's measured
184.76-ms/query offline cost at deep-image-96-angular 9.99M or prove
100M scalability. V98's budgeted hierarchy failed its exact ceiling,
and V138's flat center/radius bound admitted almost every D96 unit.
Neither is a qualified replacement. A bounded-work source router needs
its own 10M transfer and 100M build/query/memory gate after the 1M
admission decision. Fable's suggested exact top-R far-unit lists could
reach unseen units, but their exact all-row construction cost at 100M is
unproved and no memory-only payload estimate makes that build feasible.

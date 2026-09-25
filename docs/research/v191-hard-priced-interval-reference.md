# V191 hard-capped priced interval reference

V190 exposed a concrete optimizer boundary: the sparse price DP
maximizes modeled mass minus unit and GET prices with a GET cap, then
rejects any optimum above the unit cap. Its one 935-unit plan had a
feasible 619-unit greedy control. A price alone does not enforce a hard
physical cap, and rejecting the unconstrained optimum does not find the
best feasible plan.

`scripts/hard_priced_interval.py` is an **offline exact reference** for
this physical subproblem. For each sparse scored/mandatory site it keeps
the maximum modeled mass at every exact `(GETs, charged units,
open/closed interval)` state. Mandatory sites cannot be skipped. The
final selection maximizes `modeled mass - unit price × units - GET price
× GETs` only among states within both caller caps; reconstruction
independently recounts interval geometry, mandatory coverage, modeled
mass and objective. It accepts caller budgets instead of a vector-count
or fixed 100M branch. It proves neither the correctness of the fitted
utility nor a low serving CPU cost. Its state/trace work grows as
`O(scored sites × GET cap × unit cap)`, so production needs a bounded
Rust refinement or a certified fast path with an exact fallback.

Narrow exhaustive tests compare 90 random physical layouts against all
small page subsets and cover mandatory and infeasible cases. Under a
**postterminal diagnostic only**, the reference planned V190's failed
ordinal 2871 with its frozen V189 rank model and price at **672 units,
32 GETs**, capturing 99/100 closed source truth rows; the uncapped
V190 plan used 935 units and was invalid. The local Python call took
0.077 seconds on this one query. This is not a preregistered V190 arm,
a serving-latency measurement, or a rescued V190 result. V190's p05
would still be 97 after that outlier is repaired, so a hard cap alone
does not solve the tail-quality failure.

The next architecture decision must pair hard physical feasibility with
a score-conditioned reliability/utility rule and validate it on a fresh
sealed panel. Closed V189 fit-only diagnostics showed the margin arm's
strongest tested grid penalty `(20000, 400000)` still selected 951 and
1,292 units for two queries whose mandatory floors were 125 and 320;
other tested prices also breached the hard cap or aggregate GET limit.
Those numbers diagnose the old solver's cap handling. No V189/V190
holdout truth was used to select a new price or model.

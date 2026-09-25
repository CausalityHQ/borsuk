# V201 one-cap priced interval fallback preregistration

## Decision and frozen inputs

How many of V200's 139 cap-rejected real-query plans can be solved exactly
without the full two-dimensional `(GET, unit)` DP? Keep V198's GT-blind
ReLAION-1M D768 validation-1000 features, V192 optional model, unit price
1000, GET price 50000, mandatory set, 672-unit cap, 32-GET cap and physical
page geometry unchanged. Use V200's closed GT-blind weight file SHA-256
`797a83a7d830afd5cec491e70de3f49023a52c2f972af2045704b68df6f76313`
and V198's sealed plan file SHA-256
`0a61974457030d2e2ce828e7bbbbaf3d8acd9849e70a7aaafd0cb44b0c5dda00`.
No GT is read by the worker.

For each query, first accept the exact V200 linear optimum if within both
caps. Otherwise optimize exactly with the **unit cap alone** and accept only
if that winner also satisfies 32 GETs. If it does not, optimize exactly with
the **GET cap alone** and accept only if that winner also satisfies 672
units. Otherwise mark `needs_2d`; do not emit a serving plan from the
relaxations. `formal/PredictedIntervalGuarantees.lean` proves the conditional
admission implication for one-cap optima. It does not prove the Rust solver
refines the mathematical optimum.

## Pass and measurement

The strict parity gate requires every admitted Rust solution, from any of
the three tiers, to reproduce V198's *exact* physical interval list,
modeled mass, units and GETs. Record per-query tier, witness and solver
phase nanoseconds, then aggregate tier counts and p50/p95/p99. A failed
exact witness is a negative result even if objective and charges tie;
inspect the closed mismatch and decide whether the physical choice can
change quality. Report the number still needing a full two-cap solver.
Do not call this a full production planner until all 1,000 queries are
solved and feature/weight construction is in the serving path.

Run one Causality Spot cell from an immutable source commit. Read back all
terminal artifacts and terminate immediately. An interrupted cell is
discarded and restarted at a new attempt. Preserve failures and make a
root-cause decision before another run. A passing partial hierarchy
licenses a full two-cap fallback only for its remaining queries, followed
by an end-to-end concurrent live S3 gate and a distinct real-query dataset.

## Attempt record

`a0001` at source `6775a44b` was canceled before measurement when code
inspection found that a later DP candidate could replace an interval without
updating its backtrace flag. Its Spot instance `i-0ac2c1596607d7ab6` was
verified terminated. This attempt has no scientific measurement. The repaired
source adds a targeted traceback regression test; the next attempt must run
that remote test before the frozen 1,000-query gate.

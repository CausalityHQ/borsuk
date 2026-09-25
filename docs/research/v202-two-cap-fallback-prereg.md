# V202 exact two-cap fallback preregistration

## Decision

Can a Rust two-dimensional priced interval DP close V201's 51 unresolved
ReLAION-1M D768 validation-1000 plans while keeping the exact V198 physical
witness and a practical planner-core latency/resource envelope? This is a
generic recurrence parameterized by weights, mandatory units, prices, geometry
and both caps. The cohort is already used and supplies **parity and speed
evidence only**, not a new quality estimate.

The inputs remain the frozen V200 GT-blind weight JSONL SHA-256
`797a83a7d830afd5cec491e70de3f49023a52c2f972af2045704b68df6f76313`
and V198 plan JSONL SHA-256
`0a61974457030d2e2ce828e7bbbbaf3d8acd9849e70a7aaafd0cb44b0c5dda00`.
Use page count 31,250, unit price 1,000, GET price 50,000, each query's
recorded unit cap (at least 672), GET cap 32, and 512 MiB maximum trace.
No ground truth is opened by the worker.

## Algorithm and gate

Run V201's exact linear → unit-cap → GET-cap admissions first. For only the
remaining 51, retain the greatest predicted mass at each exact `(GETs,units)`
closed/open state over sorted weighted or mandatory sites. At each site,
either skip it if optional, start a new interval, or extend an open interval
across the physical gap. A strict improvement updates the winning trace.
Select by `(mass - unit_price × units - get_price × GETs, -units, -GETs,
mass)` and reconstruct whole physical intervals. Independently recount
mass, units, GETs and mandatory coverage from the witness.

The recurrence is `O(sites × GET_cap × unit_cap)` time and
`O(GET_cap × unit_cap + sites × GET_cap × unit_cap)` state/trace bytes, within
the explicit trace budget. These are structural bounds, not a proven wall
latency or actual recall guarantee. Caps are workload/quality policies, not
fixed vector-count thresholds.

Pass requires targeted small-geometry tests, a complete 1,000-query hierarchy,
and **exact interval/mass/units/GET parity** with V198 for every query. Record
per-tier p50/p95/p99 planner-core nanoseconds, total hierarchy latency,
peak process RSS, instance identity and all closed raw rows. Fail on the first
parity error; classify a tie or trace issue before revising the source. Run
one Causality Spot cell from a pushed source commit; terminate after terminal,
read back all artifacts, and discard/restart any interrupted cell under a new
attempt. Do not inspect incomplete measurement files.

Passing this gate licenses serving integration of feature and weight
construction, then concurrent live S3 and distinct held-out dataset gates.

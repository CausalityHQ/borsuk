# V169 resource-aware PQ field planner: design boundary

## Intended product behavior

Choose physical SQ8 ranges for a requested `k`, recall target and
resource profile using a single query-time rule. The rule must use no
dataset name, vector-count knee or fixed 100M RAM limit. Its charged
memory may grow with corpus size, requested recall, generations and
concurrency; it must be measured and reported. Every exact-primary
physical unit remains mandatory. A plan must pass independent physical
byte and GET ceilings before remote execution. The first research
implementation remains experimental until paired returned-quality,
serving-latency, charged-RAM and failure-recovery gates pass.

## Evidence that constrains the design

V168 measured a direct-PQ64 ranking advantage only for adjacent-only
units on 128 ReLAION-1M D768 source pseudoqueries: 89 versus 32
threshold-crossing SQ8 rows at 32 units/query, paired gain 57, bootstrap
95% total interval 17–121. There were 107 ties; 21 queries carry all
non-tie information. The proxy threshold was the 100th SQ8 score among
the 100 exact-primary nominees. After the other nominees are reranked,
the final threshold can tighten. Source pseudoqueries are present in
the index and source-trained physical layout. None of this establishes
real-query Recall@100 or the value of secondary nominee units.

V167's byte-first frontier cut bytes while increasing GETs by 63.95%
and failed its quality gate. Its lost useful rows were in adjacent-only
units, but most useful V165 rows were in nominee-voted units. The new
planner therefore assigns utility to both nominated and adjacent
units and prices both physical resources. An adjacent unit is a
zero-extra-GET extension **only when its neighboring interval is
actually fetched**; otherwise it may need a GET or a bridge.

## Candidate utility and threshold

The V168 minimum PQ64 ADC score is the verified ranking feature.
Retain it as one frozen arm. A second arm may sum row-level estimated
SQ8 exceedance probabilities within each unit, excluding the query row
and already exact-scored primary rows. This sum is an **expected count**
by linearity; it does not require independent row events. The model's
probabilities and their transfer to real queries must be calibrated and
measured. A residual-energy feature is optional and earns its byte per
row only after a paired reliability/quality win; V166's failed
unit-level Gaussian model is a reason to test tails rather than assume
isotropy. Calibration is a versioned per-generation build artifact,
trained from source rows without dataset-specific coefficients.

The threshold is a serving decision. An exact threshold from remotely
scored nominees requires an additional wave unless a charged local SQ8
mirror exists. A one-wave PQ-derived threshold is cheaper but may be
biased. The next offline screen must seal and compare both routes,
including the extra GET round trip or mirrored memory. A final-nominee
SQ8 threshold is a tighter research label than V168's primary-only
threshold but remains a proxy for exact-source top-100 truth.

## Physical planner

Quantize nonnegative unit utility into bounded integers. Use an exact
interval dynamic program over physical 32-row units with state for GET
count, charged units and open/closed interval. Forbid skipping every
mandatory primary unit directly; do not rely on a large artificial
weight. The resulting states form a quality/resource frontier. Admit
only witnesses with at most 32 GETs and 672 full 32-row units, or
16,777,216 actual SQ8 bytes. The first equal-unit solver conservatively
charges a partial final unit as full; the serving checker recounts its
authenticated actual length. At D768 a full unit is 24,960 bytes. The
objective is the least measured resource cost among plans meeting a
caller quality target, with deterministic ties. Report GETs, bytes,
largest interval, estimated captured mass, exact-primary coverage and
the full feasible frontier. No scalar S3 latency or dollar formula is
assumed without a matched storage measurement. Parallel wave makespan
and largest-interval tail need separate observation because total bytes
and GETs alone may not predict p95 latency.

The V114 dynamic program already holds a best-weight table indexed by
GET and charged-unit counts. Reuse its recurrence concept, but do not
change frozen historical code. A new implementation must expose and
check the frontier in one pass, force primary coverage in the state
transitions, and compare small instances against exhaustive search.
Planner CPU is on the serving path; a NumPy offline result is not a
Rust latency measurement. The fixed V115 flat summary route remains a
separate 10M/100M scale blocker.

The first V169 Python solver is an **experimental exact reference** for
equal-unit cost `get_cost × GETs + unit_cost × units`, with both costs
positive integers supplied by its caller. It returns one least-cost
witness for a supplied integer utility target; it does not yet expose
the entire frontier, accept a measured latency profile, or price a
partial tail exactly. Among equally priced resource cells it chooses
fewer GETs, then fewer units. Within a cell it keeps the maximum
utility; equal-utility predecessor ties prefer a skip over keeping an
interval open and a new GET over extending an open interval. A Rust
refinement must reproduce these ties or record a new format/algorithm
version. The impossible-state sentinel is `−2³⁰`, and total integer
utility must stay below `2³⁰`; otherwise the proof of witness
feasibility fails. An independent read-only review reported agreement
on 3,000 more small brute-force cases and 0.37–0.54 seconds per query
in two local NumPy 1,536-event diagnostics. Those advisory runs are
neither a controlled serving Rust latency measurement nor an acceptable
production target.

The current `Pq64Router` code plane is indexed by its original physical
order, whereas V164/V163 SQ8 records were repacked into a new physical
order. The offline V168 runner carries authenticated old/new source-row
permutations; the serving generation does not yet persist or bind that
mapping. A production format change must store a generation-bound
permutation (or rebuild the complete router summaries and PQ code plane
in SQ8 order), authenticate
its digest together with the router, SQ8 object and source identity,
and reject any mismatch before scoring candidate units. No serving
route may infer the mapping from row position alone. Because BORSUK is
unreleased, replace the experimental format marker rather than adding
a legacy reader.

The narrow production primitive `Pq64Router::score_rows` was pushed at
`e92fec66f2d026429d0ab0a083371953b1e03415`. It shares the ADC
table arithmetic with nomination, returns scores in caller-requested
old-row order, and rejects duplicate, empty and out-of-range rosters.
One Causality Spot `c7i.12xlarge` worker
`i-04a4163da931705b5` ran `cargo test --locked -p borsuk --lib
pq64_nominee::tests`: **8 passed**, 1,650 filtered out. The narrow
compile/test took 99.89 wall seconds and peaked at 5,078,852 KiB RSS
on the remote build worker; those are not serving measurements. The
terminal SHA-256 is
`b249e6f537d7568e860e6a55730b5ae4598dbdf9b4d567b004e44491c7f3c3ec`
at `s3://borsuk-bench-453182569524-euc1/research/v169-pq64-compile/e92fec66f2d026429d0ab0a083371953b1e03415/runs/a0001/`.
The controller streamed and rehashed its three terminal-listed
artifacts and confirmed the instance terminated. This validates the
primitive's crate integration, not a relaid generation or full suite.

## Qualification and stop conditions

First freeze features, calibration method, resource profile, baseline
reproductions, query cohorts and decision thresholds **before** opening
new labels. Use source-hash ranks beyond V168's 640 for any 1M
calibration and a disjoint source reliability panel. The original V168
labels may be cited as history, never used to tune the model. A
GT-blind plan seal precedes all new proxy and external quality reads.
On fresh data, compare the minimum-score and calibrated-sum arms with
the exact same nominee-plus-neighbor candidate universe, and compare
their interval plans against V165 vote planning at matched GET and
byte points. Report the physical cost of mandatory primaries and
quality conditional on remaining GET slack. Kill a calibration arm if
its reliability fails or it loses to the already verified minimum
feature at matched resources; retain the raw artifacts.

The first **product** quality gate remains a fresh paired
ReLAION-100k D768 development-1000 comparison against the strongest
V163 returned-quality point (99.322% Recall@100, p05 98,
15,657,408,000 planned bytes, 15,275 GETs) and V160's resource point
(15,562,734,720 bytes, 9,894 GETs). The large fraction of a 100k
corpus covered by 16 MiB may make this gate insensitive to resource
improvement; still report both dimensions and do not declare a general
winner from saturation. Only a decisive 100k gate promotes the frozen
implementation to a paired ReLAION-1M D768 validation comparison with
V155 (99.567% returned Recall@100, p05 98, 11,134,007,040 planned
bytes, 22,126 GETs), then to live S3 latency/cost and 10M/100M
build/query/memory gates. Historical revisions require disclosed paired
reproductions before product claims.

## V170/V171 evidence and budget allocation correction

V170's closed paired ReLAION-100k D768 **used** development-1000 cell
passed its preregistered direct-field gate: 99,357/100,000 SQ8-only
GT100 hits versus 99,338 for the same-unit neighbor-rank arm and
99,322 for V163. Direct versus neighbor gained 19 hits, with a paired
bootstrap total 95% interval of 8–32. Direct planned 14,785,704,960
SQ8 bytes and 13,888 GETs versus V163's 15,657,408,000 bytes and
15,275 GETs. It used 6,614,400 more bytes than neighbor rank at the
same aggregate and per-query GET counts, so the whole-cohort feature
gain is under matched ceilings, not exact equal bytes. Its terminal,
checker, raw and GT-blind seals are recorded in
`v170-pq-field-100k-transfer-closeout.md`. This promotes the score
field to a 1M test, not a production default.

The V171 first-stage ReLAION-1M D768 **used** validation-1000 screen
rejected strict inheritance of V155's *per-query* GET and byte caps.
Query 898 has 26 primary units in nine physical runs; its V155 cap is
3 GETs / 3,394,560 bytes, while any three-GET primary cover needs
97,843,200 bytes. The other 999 queries fit their paired caps. The
failure is a mismatch between the smooth layout's primary geometry and
V155's per-query GET allocation. It does not refute the direct PQ score
or aggregate resource competitiveness. A generic planner must derive a
minimum primary GET requirement from authenticated physical geometry,
then price optional units under caller GET/byte and quality targets.
It may allocate more GETs to a dispersed query and fewer elsewhere,
while each query still obeys the physical 32-GET/16-MiB caps. The
resource comparison must report both per-query and aggregate charges;
it may not hide a failed query in an average. See
`v171-primary-feasibility-closeout.md`.

The V171 closed counters also imply an aggregate **mandatory-only**
feasibility witness, not a scored plan: its 999 feasible per-query
minimum-unit witnesses plus nine separate runs for query 898 total
17,684 GETs and 1,098,814,080 bytes. Both totals are below V155's
22,126 GETs and 11,134,007,040 bytes, and every query can obey the
physical cap. Optional recall-improving units still have to fit the
remaining resource envelope; this arithmetic does not predict recall.

## Formal and empirical boundary

`formal/ScoredNeighborField.lean` proves conditional candidate-work and
64N code-payload arithmetic. `formal/ConditionalRecallWindow.lean`
proves a true top-100 capture floor if a uniform score-error bound,
nominee-threshold relation, complete source roster and omitted-near
count are supplied. `formal/PrimaryRunBudget.lean` proves conditional
primary-run GET/byte lower bounds and checks the V171 query-898
arithmetic under an externally authenticated gap certificate. It does
not prove the Python gap-sort refinement. A further planner proof should
certify that an accepted witness covers all mandatory units and stays
within GET and byte caps, and should bound integer-utility rounding
error. Exact DP
optimality needs a refinement check against the implementation.
Calibration, query transfer, threshold bias, actual returned recall,
serving latency, charged RAM and 100M feasibility remain measurements.
For probabilistic unit utility, independence is unnecessary for
expected count, but a per-query tail or p05 guarantee needs additional
validated dependence assumptions or direct held-out evidence.

# V189 preregistration: generic predicted utility and GET-aware intervals

## Decision and evidence boundary

Test whether a fit-trained, query-relative PQ cosine utility and one shared
GET/unit price pair can choose physical SQ8 intervals that retain the V155
source-quality target within its scaled aggregate byte and GET envelope.
This is a source pseudoquery **screen**, not returned Recall@100, serving
latency, S3 performance, or a production default. V187's greedy physical
plan missed the quality gate and used too many GETs; V188 showed that a
leave-one-out 512-nominee roster preserves the source rank signal on its
closed cohort. Neither cohort may select V189 prices or thresholds.

Dataset is ReLAION-1M D768. Select source pseudoqueries by the existing
stable-ID SHA ordering at ranks **2433–2688**. Ranks 2433–2560 are fit
(128 queries), ranks 2561–2688 are untouched holdout (128 queries).
The exact float64 cosine GT100 excludes the query row. Source-index
artifacts, layouts, SQ8, router, row maps and hashes are the same frozen
inputs used in V188. The source commit and one Spot attempt will be frozen
before launch. No dataset-name branch, query-ID exception, fixed 100M RAM
knee or holdout-specific parameter choice is allowed.

## Fixed feature and arms

Nominate PQ top 513, verify its top-512 prefix against the direct 512
call, remove the source row and take the next 512. Select exact-primary
100 from those nominees using the existing SQ8 squared-L2 order. Form
the width-32 physical candidate union around the 512 nominees, score
candidate rows with **PQ reconstructed cosine**, and exclude the source
row from unit minima. Primary physical units are mandatory. The source
row is also excluded from the exact GT.

The utility feature never subtracts SQ8 squared-L2 from PQ cosine.
Within the 512 eligible nominees, sort their PQ cosine scores; let `t`
be the 100th score and `d = max(score90 - score10, 1e-6)`. Each candidate
unit's feature is `(minimum_PQ_cosine_score - t) / d`. This dimensionless
margin is query relative; lower values predict more truth mass. Fit 64
equal-count margin bins from fit truth counts per physical unit, keeping
identical margin values in one bin. Pool adjacent bins until expected
truth mass is nonincreasing with margin. Quantize each expected count to
the nearest millionth. The prediction path reads only the frozen model
and PQ scores. A query's expected count is a linear sum of unit means;
it makes no independent-row assumption. Calibration and tail reliability
remain empirical obligations.

Arm A uses margin utility and the exact sparse priced interval DP. Arm B
uses the existing fit-trained monotone PQ **rank** utility with the same
DP and price grid. Arm C is V187's greedy rank admission under 32 GETs
and a 446-unit base with mandatory floor elasticity to 672 units. Arm C
is a paired physical control on this new cohort. All arms use the same
leave-one-out nominees, width-32 candidates and mandatory units.

For A and B, predefine the price grid as unit prices
`[1000, 2000, 3000, 4000, 6000, 10000, 20000]` and GET prices
`[0, 50000, 100000, 200000, 400000]`, in millionth-hit utility units.
On the fit **features and fitted model only**, choose the price pair with
maximum predicted captured mass subject to every query ≤32 GETs and
≤672 full units, aggregate ≤57,097 units and ≤2,832 GETs. Ties choose
fewer units, fewer GETs, then lower prices. Any pair whose unconstrained
priced optimum breaches a cap is rejected; the solver must never clip a
plan and call it optimal. If no pair is feasible, record that arm as
infeasible and do not relax caps after seeing labels. Freeze the chosen
fit price and all holdout intervals before opening holdout truth.

## Outcome and stop rule

For each arm and split report candidate truth ceiling, top-446 unit
truth, fetched truth, p05 per-query fetched truth, byte/GET totals,
maximum per-query units/GETs, mandatory floor, planner CPU and process
RSS. Independently replay interval geometry, mandatory coverage, charged
bytes and truth hits from the sealed artifacts. The primary advance gate
is **Arm A holdout ≥12,745/12,800 fetched GT100 rows, p05 ≥98, zero
infeasible queries, ≤1,425,152,901 bytes and ≤2,832 GETs**, with each
query ≤672 units and ≤32 GETs. The aggregate ceilings are the integer
floor of V155's 1000-query resource totals scaled to 128 source queries.
The byte criterion is checked in bytes; the equivalent full-unit maximum
is 57,097 at 24,960 bytes/unit. Report A versus B to assess the utility
feature under the same envelope, and B versus C to assess priced versus
greedy allocation with rank utility. These comparisons do not hold exact
bytes and GETs equal, so attribution remains conditional. A tie or failure
does not authorize price retuning on holdout; choose a materially revised
method or layout for a new panel.

V155 used ReLAION-1M D768 validation-1000 actual returned exact-source
Recall@100 was 99,567/100,000, p05 98, at 11,134,007,040 planned bytes
and 22,126 GETs. It is an **unpaired returned-quality comparator** for
this screen. No paired S3 Vectors or Turbopuffer result exists. Even a
passing V189 source gate advances only to independent replication and a
fresh paired used-validation returned-quality/S3/resource gate, then
10M/100M scale. The production RAM budget may scale with corpus size,
recall target, generations and concurrency; this 1M physical envelope is
a qualification point, not a universal memory policy.

## Seal, compute and formal obligations

Run one immutable Causality Spot cell. Publish and rehash the GT-blind
feature seal before computing fit truth. Publish and rehash fit labels,
fitted model, chosen price and **all** holdout plans before computing
holdout truth. Record instance identity, source archive hash, complete
terminal and every artifact hash. An interruption discards the whole
measurement cell and restarts under a new attempt ID. Terminate the
instance immediately after its terminal marker. Do not inspect an
incomplete measurement CSV or tune on a closed holdout.

Lean can prove conditional interval/byte/GET accounting, shared-price
weak duality and the ≤`n/(2×10^6)` total quantization error for `n`
modeled units. Such a proof assumes authenticated geometry, correct
model values and an implementation refinement relation. It cannot
derive empirical recall, p05, calibration transfer, S3 latency or charged
RAM without validated data/hardware assumptions. Those remain separate
gates.

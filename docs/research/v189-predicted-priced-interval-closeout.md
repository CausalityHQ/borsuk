# V189 closed predicted priced interval source screen

## Decision

The preregistered primary **PQ cosine margin** arm did not yield a
feasible fit price under the fixed 35-pair grid and the physical/resource
caps. It produced no holdout plan; its recorded zero fetched hits are an
**infeasible-arm marker**, not a measured zero-recall algorithm. V189's
primary advance gate therefore failed. The exact reason each price pair
was rejected was not recorded; it may be a per-query unit cap or an
aggregate byte/GET cap. Diagnose this on the **closed fit split** only
before revising the margin model or grid. Do not use V189 holdout labels
to choose a price.

The secondary **rank utility plus priced intervals** arm met the numeric
source holdout gate, but narrowly: 12,746/12,800 fetched GT100 rows,
p05 98, 751,046,400 planned bytes and 2,329 GETs, zero infeasible
queries. It missed the same quality threshold on fit (12,737/12,800,
p05 97). This is a predeclared secondary diagnostic, not a promoted
product/default result. Freeze its fit-trained model and price
`(unit=6000, GET=50000)` for an untouched source replication panel;
only a replicated source gate can advance it to paired used-validation
returned quality and S3/resource testing.

The V187-style greedy paired control fetched more holdout truth
(12,758/12,800, p05 98) but required 1,402,627,200 bytes and 3,692
GETs, exceeding the V155-scaled 2,832-GET envelope. This supports the
GET-aware stopping decision, while the rank arm's 12-hit loss shows the
quality/resource trade-off. The comparisons are under a common envelope,
not identical realized resources. No dataset/query exception or fixed
100M RAM knee is justified.

## Closed authority and verification

Frozen source commit `ae0b160e0b48c419a6bac1b6d9e5964f7fd4b63b`,
one Causality Spot `c7i.12xlarge` worker `i-0695b07e441150183`,
terminated after its complete terminal. Attempt:
`s3://borsuk-bench-453182569524-euc1/research/v189-predicted-interval-source/ae0b160e0b48c419a6bac1b6d9e5964f7fd4b63b/runs/a0002/`.
Terminal SHA-256 `ec2a845a4471afacf48ec8e78cb16fdafff75bb0852a7323e67d9ea9db2f8605`;
summary SHA-256 `2a3293bf23fb1088cd25b51561cd6481c42a135c3239792098da6668f740dc85`;
features SHA-256 `7eb4833c76675534cde41330c5939599ab70d7871ac27cb365d62cdb840c3fbe`.
The controller streamed and rehashed every terminal-listed artifact,
verified both S3 pretruth seal stages against their final artifacts and
publication times, and confirmed instance termination. An independent
closed replay checked all 256 query IDs, exact GT100 mass, fit-label
identity, model/plan seal hashes, mandatory coverage, whole-unit bridge
charges, per-query resource caps, per-arm truth hits and every summary
aggregate. All matched. The failed first attempt and its root cause are
recorded separately in `v189-attempt-a0001-failure.md`; it supplied no
holdout result.

## Measured source-only evidence

Dataset **ReLAION-1M D768**, source pseudoquery SHA ranks 2433–2688.
Fit is ranks 2433–2560, holdout 2561–2688, 128 queries each. Exact
float64 cosine GT100 excludes the source query row. Counts below are
truth rows in scored or planned SQ8 units, **not returned Recall@100**.

| Split | Arm | Fetched truth / 12,800 | p05 | Planned bytes | GETs | Infeasible queries |
|---|---|---:|---:|---:|---:|---:|
| Fit | Margin priced | no plan | — | 0 | 0 | 128 |
| Fit | Rank priced | 12,737 | 97 | 762,353,280 | 2,402 | 0 |
| Fit | Greedy control | 12,739 | 98 | 1,401,903,360 | 3,649 | 0 |
| Holdout | Margin priced | no plan | — | 0 | 0 | 128 |
| Holdout | Rank priced | 12,746 | 98 | 751,046,400 | 2,329 | 0 |
| Holdout | Greedy control | 12,758 | 98 | 1,402,627,200 | 3,692 | 0 |

Candidate truth ceiling was 12,779 fit and 12,788 holdout; top-446
cosine-ranked units contained 12,775 fit and 12,784 holdout. The fixed
rank price produced 30,543 units on fit and 30,090 on holdout, with
maximum single-query counts 666 and 329 units respectively, at most
32 GETs each. The greedy control used 56,166 fit and 56,195 holdout
units. Mandatory-only floor at ≤32 GETs totaled 5,195 fit and 4,819
holdout units; the maximum query floor was 320 fit and 114 holdout.

The scaled V155 resource gate for 128 queries was ≤1,425,152,901
planned bytes and ≤2,832 GETs, with ≤672 units and ≤32 GETs for each
query. The strongest BORSUK returned-quality comparator is **V155 used
ReLAION-1M D768 validation-1000** actual returned exact-source
Recall@100 99,567/100,000, p05 98, at 11,134,007,040 planned bytes
and 22,126 GETs. It is unpaired with this source panel. There is no
paired current-revision S3 Vectors or Turbopuffer comparison.

Spot preparation took 79.20 seconds and peaked at 9,272,636 KiB RSS;
fit/price/planning took 326.49 seconds and peaked at 9,572,560 KiB RSS;
evaluation/replay took 43.86 seconds and peaked at 9,538,148 KiB RSS.
The isolated model-fit plus 256-plan section took 303.53 CPU seconds and
302.91 wall seconds. These are **offline experiment-process** resources,
not serving query latency or charged production RAM. The devbox ran only
narrow unit/syntax checks, not a full suite, during swap pressure.

## Next gate

Replicate the frozen V189 rank model and `(6000, 50000)` price without
refitting or price search on untouched source pseudoquery ranks beyond
2688. Preregister a 256-query gate at ≥25,490/25,600 fetched truth,
p05 ≥98, zero infeasible queries, ≤2,850,305,802 planned bytes and
≤5,664 GETs, plus each query ≤672 units and ≤32 GETs. Pair it with the
same greedy control and report candidate/top-446 ceilings. If it fails,
revise the utility or layout rather than rescue it with posthoc prices.
If it passes, run a new paired **used validation-1000** returned-quality,
live S3 latency, and charged-RAM comparison before a production default;
then test 10M/100M scale. Any Lean resource/recall theorem remains
conditional on authenticated geometry, calibrated score assumptions and
an implementation refinement check; empirical transfer and latency
remain measured gates.

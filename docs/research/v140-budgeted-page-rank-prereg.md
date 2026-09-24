# V140 budgeted centroid-page rank preregistration

**Question:** does a single query-dependent page order preserve the current
100-row nomination while adding useful unvoted pages within a fixed S3
transfer budget on both used D96 100k and D768 1M layouts? V139 rejected
uncapped threshold admission. V140 changes the policy to ordered selection
with a hard 32-GET/16,777,216-byte limit. It does not claim a sound bound,
returned recall, live S3 latency, or a production router.

## Inputs, score, and one generic policy

Use V139's authenticated GT-free input identities and derived primary-only
files. The first cohort is the **deep-image-96-angular random100k train
subset**, already-used **publication-test ordinals 9000–9999**; the second is
**ReLAION-1M**, already-used **validation-1000**. Both run in one remote
attempt because source-only geometry cannot select a publication winner. The
different historical layouts are diagnostic, not a matched-method claim.

Reconstruct SQ8 rows, form f16 means of consecutive 32-row physical units,
and score each unit by Euclidean distance from the query. A 256-row physical
page has the minimum score of its eight units. The 100 frozen primary row
ordinals are input from the existing source-only router and scorer, not GT.
Order unique primary pages by the earliest primary rank they contain; they
have first admission priority. Order all other pages by `(page score, page
ordinal)`. Walk this order once. Admit a page only if the **minimum-byte cover
of all selected pages in at most 32 contiguous S3 ranges** remains within
16,777,216 bytes. The cover bridges the shortest page gaps and charges the
short final page exactly. If a page is unaffordable, skip it and continue:
a later nearby page may fit. Stop when the selected page count reaches the
largest requested target or the order is exhausted.

The recall-resource ladder is one global multiplier **β = 1, 2, 4, 8**.
For each β, the target is `ceil(β × unique primary pages)` per query, capped
by the corpus page count. Snapshot the selected set at that target. If the
byte cap prevents reaching a target, snapshot the final affordable set and
record the shortfall. The page score, priority rule, page count formula,
unit/page geometry, and cap are identical across both corpora. β is a
resource/recall request, not a dataset branch; no β is selected from this
used split as a production default. Resident f16 centroid payload is
`ceil(N/32) × 2D` bytes, plus the existing router and runtime allocations.
This screen does not prove a final memory policy: the summary resolution and
its cost must be qualified as a smooth function of requested recall and N
before 10M/100M.

Record all 1,000 raw query rows per cohort with per-β selected pages,
selected and target counts, preserved primary pages, planned ranges, GETs,
bytes, and exact cap result. Report per-β p50/p95/p99 and mean bytes and
GETs, p05 primary-page retention, fraction preserving all primary pages,
target shortfalls, and charged worker memory. The evaluator reads no GT or
returned IDs. A post-terminal, separate read-only assessor may count
**physical GT coverage ceilings** on the already-used cohorts, clearly
labelled as post-hoc; it must never feed GT back into page ordering. Neither
coverage nor resource counts are returned Recall@100.

## Frozen comparison and promotion rule

Compare D96's planned bytes and physical coverage ceiling with the sealed
V122 capped control (98,827/100,000 GT positions, 1,535,701.248 B/query)
and V135 broad candidate (99,942/100,000 returned exact-source hits; its
V132 S3 run received 9,428,126.976 B/query). Compare D768's primary-page
retention and planned GET/bytes with the historical V116/V126 capped routes,
with their different source revisions disclosed. Do not compare local planned
bytes to S3 latency as if paired.

A β is eligible for a new returned-quality implementation only if its D96
post-hoc physical coverage is at least **99,500/100,000**, its D96 mean
planned transfer is at most **4,714,063 B/query**, both corpora have **zero
cap violations**, and D768 retains at least **95% of all primary pages in
aggregate**. If no β meets all four, reject this centroid page-rank layer
and revise routing or physical layout rather than tuning the ladder on these
used splits. Passing this screen permits a fresh matched-layout 1M
returned-quality gate with exact-source recall, per-query tails, and live S3
transport. It does not establish that gate.

Run one frozen Causality Spot attempt from a committed archive, one instance
identity, and one immutable attempt prefix. Authenticate every input, terminal
and artifact, preserve all 1,000 raw rows per corpus, monitor only terminal
and infrastructure before completion, terminate at terminal, and discard an
interrupted cell. Use the worker's whole-instance 5,400-second shutdown
timer; do not run this full screen on the devbox or overlap copies.

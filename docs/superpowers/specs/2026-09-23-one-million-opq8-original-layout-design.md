# ReLAION-1M OPQ8 original-layout containment gate

## Decision

The terminal-closed 100k paired actual-read gate at source
`b0c0c25fb6816af68935690f3c4b6603e37b2d39` passed. This cheaper
1M source-only gate asks whether the same 100k-trained eight-byte row
router can select enough relevant rows on the frozen original 1M physical
layout. It does not claim final-page quality, actual 96-byte code reads,
serving latency, or 100M throughput.

## Frozen inputs and route

Use the exact five source and two development identities in
`scripts/native_one_million_selector_cell.py`: source parquet, generation,
base and delta page runs, router, queries and truth. Preserve the original
7,278 pages, 910 adjacent eight-page groups, physical row order and
1,000-query development cohort. Authenticate the completed 100k OPQ8
model bytes by terminal identity before reading the 1M source. Do not
retrain the model, change codebook, rotation, score, or training seed.

Stream the 1M source parquet in bounded batches. Map each source stable ID
to the authenticated original physical row ordinal, encode its fixed
eight-byte OPQ code, and scatter into one 8,000,000-byte plane. Verify
exactly one source row and one physical position per ID. Avoid a full
1M-by-768 float array. Seal model identity, source identities, physical
order, row counts, code digest and incompatible format marker before
admitting queries or truth. The measured process tree must remain at least
64 MiB below 3 GiB, with zero swap.

For each frozen query, evaluate the unchanged eight-table ADC expression
over the physical OPQ codes. Rank each eight-page group by the mean of its
four smallest row scores, then by its smallest score and fixed physical
group ordinal; only finite scores are valid. Reuse the existing 1M
adjacent-range planner on that ranking, with a projected 96-byte row
payload, 32 contiguous GET ranges and 16,777,216 projected bytes.
It may skip an over-budget group and continue. Store all 1,000 ranked and
selected plans before reading truth. Record the number of selected groups
separately from the number of merged GET ranges. In the same query-only
phase, stream the source once more in bounded batches and calculate source
squared-L2 top-four group scores for all 1,000 queries. Run those scores
through the same range planner as a sealed diagnostic arm, with no role in
the primary pass/fail gate. Count ordered GT10 and GT100 owner groups only
after both plans are sealed. This is a group-containment upper bound.

## Paired control and fixed gate

Authenticate the terminal-closed original PQ96 locality projection at
source `1a1d2e7380c4d6635a2b370784ea009aa3af0851`. Replay its
per-query selected groups, projected bytes, GET counts and GT owner hits
against its sealed evidence, not merely aggregate totals. The challenger
uses the same physical groups, projected lengths and cohort; only the
ranking changes. The historical control is 98,151/100,000 GT100 hits,
89 GT100 hits at p05, 9,928/10,000 GT10 hits and 51 sub-90 queries.

Advance only if the OPQ8 route has at least 98,151 GT100 hits, at least
90 hits at p05, at least 9,928 GT10 hits, at most 49 sub-90 queries,
and every plan meets both 32 GET and 16-MiB caps. Require exact control
replay, source and phase authentication, independent code/plan validation,
and the memory/swap cap. A valid quality miss kills this fixed OPQ8
model/top-four/original-layout combination; do not tune it on the cohort.
An authority, replay, or resource failure invalidates the attempt and
requires a repaired source revision and fresh attempt.

Report projected bytes and selected-group counts for both primary and
control. "Materially more" means the candidate's 1,000-query total
projected bytes or total selected groups exceeds the control total by
more than 1%. Record that boolean in the sealed result. If primary
containment improves with material plan expansion, report the gain as
budget use within the fixed caps, not as a routing-quality gain.
If OPQ8 fails but the source-distance diagnostic passes, assign the loss
to representation error. If both fail, assign the loss to the frozen
original-layout/top-four-score combination, rather than tuning OPQ8 on
this cohort. The diagnostic is a source-only calculation, not an S3 read
or latency measure.

## Execution and next gate

Run one create-only/readback-verified Causality Spot attempt from a pushed
source revision. Preregister terminal behavior, stop compute immediately
after terminal publication, and monitor incomplete work only by terminal
and infrastructure health. Preserve raw sealed evidence and independently
recompute every result after terminal closure.

An EC2 Spot interruption invalidates that attempt's measurement cell.
The controller records its instance identity and failed terminal, and
every published artifact remains immutable. Discard all partial
measurements without reading them; restart the entire cell at `a{n+1}`
from the same revision when the code did not fail. If the worker reached
a successful terminal, do not restart. The controller's active deadline
is 14,400 seconds from launch plus 900 seconds for boot and closure;
this whole-run deadline takes precedence over each phase's own timeout.

If this gate passes, build or use a real 1M row-code object for a separate
actual-read/final-page gate. If it fails, diagnose row representation
versus original-layout locality and change the responsible layer before
another measured run. In either case, the Lean planner and conditional
recall theorems remain analytical claims whose premises must be checked
against the frozen data; object-store latency still needs measurements.

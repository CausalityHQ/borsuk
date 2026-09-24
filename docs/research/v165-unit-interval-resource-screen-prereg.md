# V165 32-row-unit interval resource screen

## Decision

Test the cheapest part of the V164 closeout's physical schedule redesign:
whether changing only the atomic fetch unit from 512 SQ8 rows to 32 rows
can meet the paired V155 resource totals under the same 32-GET/16,777,216-byte
per-query caps. This is a **GT-blind plan-only resource screen** on used
ReLAION-1M D768 validation-1000. It does not measure returned recall and
cannot promote a production default. A resource failure kills this simple
unit-only policy without downloading truth or running exact-source scoring;
a resource pass permits a separately frozen quality gate.

## Frozen inputs and rule

Authenticate the complete V164 terminal SHA-256
`daa4025093ddef883358a200751972b9d953cd53be80681b9055677c3c7793c7`,
its 8,000,128-byte source-only `order.npy` SHA-256
`5b5ef48d86570e5ca68fdaaac9aef231ec7368dd526baef00474cd0a2f59a06f`,
the V63 old physical order and V116 1,000 requests/exact-primary rosters
by the pinned V164 input identities. Verify the order is a permutation
and the terminal binds it and the frozen source commit. No SQ8 body,
truth, V155 replay or returned result is opened in this screen.

Map each frozen V116 old physical nominee through V63 to the V164 source
ordinal, then through the inverse V164 order to a 32-row physical unit.
Give exact-primary rows 513 votes and the other nominees one vote, exactly
as V164. Apply the unchanged V114 weighted-interval optimizer with unit32
as its page: 31,250 units, one charged unit per page, at most 32 intervals,
at most 672 units (16,773,120 encoded bytes) per query. Every interval
has contiguous 32-row-unit boundaries. The 32-row fetch unit is independent
of corpus name and N; the current cohort happens to divide evenly.
Seal all plans before aggregating resource counts. A second pass replays
all 1,000 rosters, plans, byte/GET counts, and the decision. Record per-query
plans and distributions plus total bytes and GETs. This screen does not
claim an independently proved planner or measured live S3 I/O.

## Gate

`resource-pass` requires at most V155's **11,134,007,040 planned bytes**
and **22,126 planned GETs** over 1,000 queries, with every query within
32 GETs and 16,777,216 bytes and a passing replay. If either aggregate
resource threshold fails, kill the pure unit-only schedule and redesign
the admission objective rather than tuning unit width on this used cohort.
Because the frozen optimizer maximizes every positive nominee vote before
breaking ties on bytes or GETs, cap saturation is a known risk; this screen
measures whether the smaller unit alone actually relieves it.
Even a pass says nothing about Recall@100, p05, latency or serving RAM;
those require an exact-source quality and live-service gate.

Run one immutable Causality Spot attempt from clean pushed source with a
3,600-second hard wall cap, an interruption requiring a new whole-cell
attempt, terminal-listed artifacts, immediate termination and controller
S3 byte/SHA-256 readback. Keep local full suites paused during devbox swap
pressure. `formal/SmoothPageBudget.lean` proves the conditional byte and
GET caps for a V165 plan given authenticated 32-row-unit counts; it does
not prove the Python planner refines that model. Empirical quality remains
open.

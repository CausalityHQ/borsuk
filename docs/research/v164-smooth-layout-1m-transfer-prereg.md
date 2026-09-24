# V164 smooth source-only layout 1M transfer preregistration

## Question and cohort

Test whether V163's source-only smooth k-means/centroid-chain order transfers
from used ReLAION-100k D768 development-1000 to **used ReLAION-1M D768
validation-1000** under the same 512-row SQ8 pages and 32-GET/16,777,216-byte
per-query envelope. This is an architecture-transfer screen, not fresh
qualification. V163's 100k quality gain came with 54.38% more planned GETs
than V160, so the 1M result must report both quality and resources.

The strongest paired control is V155 cached sparse exact-source, which
returned 99,567/100,000 GT100 hits (99.567%), p05 98, SQ8-only 99,222,
planned 11,134,007,040 bytes and 22,126 GETs over the same 1,000 queries.
V161's balanced-two-means 1M arm is diagnostic context, not the strongest
control. Its exact-source returned 98,859/100,000, p05 94, and planned
16,493,667,840 bytes / 19,012 GETs.

## Frozen method and isolation

Authenticate the exact V36 1M source, V63 old physical permutation, V70
780,000,000-byte SQ8 object, V114 quantizer manifest, V116 frozen
validation requests and exact-primary rosters, V36 GT, and complete V155
terminal/replay/evidence by pinned byte length and SHA-256 from V161's
preregistration and launcher. Map source IDs to old physical rows exactly.
No query, truth or baseline result enters construction.

Require every source vector to be finite and nonzero; reject the cell if
that precondition fails. Normalize each source vector to unit length for cosine,
then run the unchanged V120 `_fit_order` implementation: sample seed 8201,
12 Lloyd iterations with seed 8202, nearest-centroid assignment and
centroid-chain order. Use the already frozen smooth
`K(N)=min(N,ceil(8192*(N/1,000,000)^(1/3)))`, hence K=8192 at 1M.
Seal the 1M source-ordinal permutation and relaid SQ8 body before opening
GT; preserve every V70 encoded row byte. Retain V161's transport-derived
512-row page width, 513/1 primary/nominee votes, optimal weighted interval
planner and per-query caps. The only causal change from V161 is the physical
order and its source-only builder.

Map the same V116 old physical 512 nominees and exact-primary 100 through
V63 to the new permutation. Seal all 1,000 plans without GT. Score only
fetched SQ8 rows to top 512, then exact-source rerank the union with the
same 512 nominee IDs using V155's float64 cosine/tie semantics. Seal all
returned IDs before GT or V155 replay is read. Recount returned exact-source
and SQ8 GT100 hits, physical GT100 coverage, p05, distinct primary pages,
union width, planned bytes/GETs and paired wins/ties/losses. An independent
checker rebuilds the source-only order and verifies every encoded row,
plan/cap, returned ID, GT hit, aggregate and decision. It reuses the frozen
V120 fitter and V161 planner/scorer implementations, so this is a
deterministic replay and aggregate audit, not an independent proof of those
algorithms.

## Decision

Apply V161's predeclared `transfer-pass` floor: at least 99,400 exact-source
GT100 hits and p05 at least 97, plus complete independent check and all
caps. `baseline-competitive` additionally requires at least V155's
99,567 hits and p05 98, with total planned bytes no greater than
11,134,007,040 and GETs no greater than 22,126. A transfer pass without
baseline competitiveness leaves V155 the strongest 1M point and requires
a physical schedule/resource redesign before fresh or scale promotion.
Failure triggers a closed decomposition of nominee, primary-page, admitted
page, SQ8, and exact-source union loss; do not tune K/page width post hoc.
No 10M/100M, live-S3 latency, charged serving RAM or generalized recall
claim follows this used cohort.

Run one immutable Causality Spot `c7i.12xlarge` attempt from clean pushed
source with eight BLAS threads and a 10,800-second hard science wall cap.
An interruption discards and restarts that cell under a new attempt.
Upload terminal-listed artifacts, terminate the instance immediately after
the terminal, then independently stream and check every S3 artifact's byte
length and SHA-256 before reporting the result.
Do not run a local full suite during devbox swap pressure. Lean can prove
conditional budget and memory bounds; observed quality and latency require
experiments. Requested recall may select a larger charged local tier, with
no dataset-specific switch or vector-count knee.

# V177 source-only candidate ceiling preregistration

## Decision

Test whether the authenticated V115 router and V164 relaid physical order
place enough exact source neighbors near nominated 32-row SQ8 units to justify
calibrating a generic optional-unit utility model. This is a candidate-universe
screen, not returned Recall@100, a latency measurement or a production default.
The comparator is V155's **used ReLAION-1M D768 validation-1000** exact-source
Recall@100 of 99,567/100,000, p05 98, 11,134,007,040 planned bytes and
22,126 planned GETs. V177 uses different **source pseudoqueries** and therefore
does not claim a paired improvement over V155.

## Frozen source panel and split

Use the pinned ReLAION-1M source, V63 old layout, V70 SQ8, V115 router and
V164 new order in the launcher. Select stable IDs by the existing V166 SHA256
domain order, zero-based ranks 640–767 (human ranks 641–768). The first 64
are fit and the next 64 are holdout, disjoint from V166–V168. A source vector
is the query; exclude its own row from nomination scoring and exact cosine
truth. Exact truth is the 100 highest float64 cosine scores over all other
source vectors, with stable ID as the tie rule. Source normalization uses
float64. These queries are easier than external queries; a positive result
licenses further testing only.

Prepare writes 512 router nominees, their unique V164 physical 32-row units,
100 eligible SQ8 primary nominees and their mandatory units. The saved roster
contains only unit sets and source identity; it is sealed and uploaded to S3
before exact source-neighbor labels are calculated. `prepare-seal.json` binds
all input identities and roster SHA256. Evaluate requires that seal's SHA256
as a separate argument. An interrupted cell is discarded and restarted under
a new immutable attempt prefix. No incomplete measurement artifact is opened.

## Candidate arms and gate

The same rule applies at every corpus size: for every nominated physical
unit, include all existing physical units at integer distance at most
`w ∈ {0,1,2,4,8}`. No dataset name, corpus-size knee or GT label affects
the unit selection. Count the exact truth rows contained in each candidate
union, per query and in fit/holdout totals. Units are a ceiling on possible
quality, not a fetched plan: this gate makes no GET/byte or latency claim.

Advance width 8 to calibration only if its holdout coverage meets the V155
qualification target, which means at least 6,373 of 6,400 source truth slots.
Otherwise kill width 8 for this layout and inspect whether router nomination
or layout dispersion is responsible. Report all five widths and fit/holdout
separately regardless of the decision. A source pass does not imply that a
32-GET/16-MiB plan or an external-query recall target is feasible; those are
subsequent gates. If narrower widths also pass, prefer the least expensive
width only after an actual resource/quality comparison, not from this ceiling.

The selected production method remains parameterized by caller `k`, target
recall and physical resource caps. Its resident memory may grow with corpus
size and requested recall. No fixed 100M memory cap is assumed.

# V161 geometric relayout 1M transfer preregistration

## Question and frozen cohort

Test whether V160's query-blind balanced-two-means physical ordering plus
transport-derived 512-row SQ8 pages transfers to **used** ReLAION-1M D768
validation-1000, where V155 cached sparse exact-source returned Recall@100
was 99.567% (p05 98) and SQ8-only Recall@100 was 99.222% (p05 97).
This is an architecture-transfer screen, not fresh qualification. Its
strongest direct control is V155's terminal-closed cached sparse arm on
the same source, queries, truth and exact-source scorer; V160's 100k
numbers are contextual and not a cross-scale paired control.

## Immutable source and GT-blind construction

Authenticate the V36 ReLAION-1M source Parquet SHA-256
`2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86`,
V63 physical order SHA-256
`32cba9690cd9d0ed3809763e5a0fa3574b09a207da26e93404651acaa1a66a0b`,
V70 780,000,000-byte SQ8 object SHA-256
`2284f24745f964ff2b125eedb883d5cd8ff6afab0738f49f9e16e593167a318b`,
V114 manifest, V116 validation requests and Rust replay, and V155 complete
terminal, replay, and result evidence by pinned byte length and digest.
The source and V70 order must map every stable ID exactly. Frozen V116
512-nominee and exact-SQ8-primary-100 rosters are old physical ordinals;
map them to source ordinals through V63, then to the new permutation.

Run the unchanged `construct_layout` balanced-two-means-480k rule from
`native_geometric_layout_screen.py` with seed `20260921`, metric `cosine`,
the authenticated source vectors and decimal stable IDs. The 100k gate
used the same builder family with L2 as required by its source metric.
At 1M do not use queries, truth, old results or dataset name in
construction. Sort the sealed membership by page/in-page ordinal, then
re-cut its permutation into fixed pages. The generic page-width rule
`2^floor(log2(floor(B/(G*(D+12)))))` gives 512 rows at B=16,777,216,
G=32, D=768, regardless of N. Preserve every V70 SQ8 row's 780 encoded
bytes exactly; record a digest-bound source, membership, permutation,
SQ8 and quantizer seal.

## Frozen route and returned result

For each validation query, use the V116 exact-primary-100 and remaining
512 nominees with V114's 513/1 page votes. Run the same weighted contiguous
interval planner under 32 GETs and 16,777,216 SQ8 bytes with 512-row pages
and 32-row units. Seal all 1,000 plans before downloading GT or the V155
replay/evidence. Score only rows within these ranges with the V114 SQ8
float32 arithmetic to return top 512. Exact-source rerank the union of
those IDs with the same source-only 512 nominee IDs using V155's float64
unit-normalized cosine arithmetic and stable-ID tie rule. Seal returned
IDs before GT is opened. The V155 sparse control is replayed from its
authenticated per-query exact and SQ8 IDs and independently recounted
against the same truth.

Record each query's exact-source and SQ8-only GT100 hits, p05, physical
GT coverage, distinct exact-primary pages, source-union width, planned
GETs/bytes, paired wins/ties/losses, plus phase wall/RSS on the worker.
Independently verify every permutation, plan/cap, returned-ID membership,
hit and aggregate from terminal-listed artifacts. Do not interpret offline
CPU or local-file scoring as live S3 latency, throughput or serving RAM.

## Decisions

`transfer-pass` requires at least 99,400 exact-source GT100 hits, p05 at
least 97, and no cap or independent-check failure. This only permits
subsequent fresh and live-service gates. `baseline-competitive` additionally
requires at least V155 sparse's 99,567 hits and p05 98, with total bytes
and GETs no greater than V155 sparse's 11,134,007,040 bytes and 22,126
GETs. A transfer pass without baseline competitiveness retains V155 as
the stronger 1M point and sends the proposed format back for a physical
schedule/resource redesign. Any transfer failure sends locality, exact
primary, SQ8 score and exact-source union losses to a causal decomposition.
No production default or 10M/100M claim follows this used cohort.

Run one immutable Causality Spot attempt from pushed source, with a hard
wall cap, interruption as discard/restart under a new attempt, terminal
marker even on failure, controller artifact SHA/length readback, and
immediate instance termination after terminal. Local full suites remain
paused while swap pressure persists. Larger RAM for high requested recall
is allowed; no vector-count knee or dataset-specific method switch is
introduced. Lean can prove conditional budget and memory bounds, while
recall and latency remain measurements.

# Precision-first route after the G0 code-only failures

Status: architecture hypothesis and next development gate, 2026-09-23. No
production format or 100M performance claim is frozen here.

## Decision evidence

The three terminal-validated ReLAION-100k **development**, 1,000-query tests
used the same fetched rows. Exact float32 ranking on those rows returned
98.590% Recall@100. A 200-byte two-bit score returned 90.295%, the same code
with stored source norm 88.464%, and a corpus-trained 208-byte PQ192 score
88.433%. These results reject these codes as sole final scorers on this
roster. The closed cells, artifact identities and paired losses are in the
G0, G0b and G0c closeouts. They do not reject those codes as shortlist
estimators followed by precise scoring.

The strongest relevant precise reader is V75 on ReLAION-1M **validation**,
1,000 queries: 99.272% returned Recall@100, 21 S3 GETs, 11.0 MiB, p50
49.0 ms and p95 137.9 ms. Its resident PQ64 row router is 64 bytes per row:
6.4 GB for one 100M-row generation before other data. The result is a
historical reader measurement, not a 100M production qualification. The V85
ReLAION-1M **development** 1,000-query hard-capped 99.285% page-SQ8 result
used a 900k base plus 100k delta and its own planner. It is not the
all-pages control for V63's single centroid-chain layout. V66's two PQ192
summaries per page achieved 99.61% *page containment* at 256 pages on the
ReLAION-1M development split; those pages are scattered and a full SQ8 data
read would exceed the 16-MiB limit. No current result establishes their
combination with a locally stored PQ64 tier and the precise capped reader.

The executable V85 100M worksheet (`scripts/v85_memory_worksheet.py`)
charges 1,098,772,248 bytes apart from its one resident row-code generation.
An optimistic reuse of that entire reserve for two independent full
100M-row generations under 3 GiB leaves 2,122,453,224 bytes for both row
planes, or at most ten **integer** bytes per row in each. The Lean theorem
`two_generation_width_with_v85_reserve_at_most_ten` checks that implication.
The reserve is an assumption from a different architecture, not a measured
two-generation peak. The weaker 64-MiB-only bound is 15 bytes per row. A
resident PQ64 tier cannot fit either model; compact page summaries can fit
arithmetically if scored while compressed. Page-cache memory, allocator
overheads, query buffers and overlapping generations must be charged in an
actual implementation and measured as node/cgroup memory, not only process
RSS.

## Candidate, with explicit uncertainty

Store authoritative immutable page bodies, codebooks and generation manifest
in S3. Keep a compact page-summary hierarchy resident. Cache each immutable
generation's page-ordered PQ64 row-code plane on locally attached storage;
fetch only selected code pages into bounded buffers, choose a shortlist and
then fetch authenticated SQ8 data pages in one S3 wave for final scoring.
The local plane is 6.4 GB per 100M full generation, or 12.8 GB during a
two-generation rollover, **on storage**. This adds cache warm-up, local read
tail, persistence/recovery and node-state obligations. `pread` alone does not
bypass the kernel page cache; any memory claim must account for cache charging
or use and validate direct I/O. No choice of instance type or local IOPS is
qualified yet. A second S3 code wave remains an unqualified alternative;
historical two-wave 64-GET/33-MB results do not prove mathematical
impossibility under a different planner, but they miss the present total cap.

This candidate may fail even at 1M because page summaries may miss rows
needed by PQ64 nomination, or the local tier may require too many scattered
pages. Even a 1M quality pass says nothing about 10M/100M selected-page
growth, locality, p95 latency, throughput or the charged two-generation peak.

## Next gate: one physical identity, one control

Run a read-free, terminal-validated ReLAION-1M **development** 1,000-query
replay on the V63 corpus-only k-means-8192 centroid-chain order. Authenticate
the source, query, GT100 and permutation before use. Rebuild or verify the
V73 PQ64 books/codes and V66 two-per-page PQ192 summaries against exactly
that order; if an artifact differs, do not combine it. Use the V73/V75
single-layout SQ8 row geometry and implement one exact interval calculator
for every data arm. First reproduce the V73 200-query development
shortlist-512/gap-2 result as a harness identity check: 99.155% returned
Recall@100, 18 GETs and 10.3 MiB are the historical means. This check is
necessary, not a new product baseline. Then run the full 1,000-query
unrestricted PQ64 nomination as the **same-layout** control under the
registered 32-GET/16-MiB cap; record any cap violations rather than silently
discarding queries.

Only after the control matches its historical prefix and meets the caps,
select K in {256, 512, 1024, 2048} pages with the fixed PQ192 summaries,
read those PQ64 code pages from the emulated local tier, nominate rows and
fetch/scored SQ8 pages under the same interval planner. Freeze the summary
score, tie breaks, shortlist width (512 primary; 1024 diagnostic), range
coalescing and stop criteria before opening truth. Include an f32-summary
arm at K=1024 to distinguish summary compression from insufficient page
count. Save per-query selected page IDs, nominated row IDs, planned GETs,
bytes, truth hits and returned hits, plus authenticated code-plane bytes.
Report aggregate mean, p05, sub-90, local bytes/read count and CPU work.
The independent reducer must validate roster/layout/code identities,
recompute interval bounds and rerank returned IDs from the same selected
rows. No new query-path S3 GET or latency/QPS claim is made by this replay.

Promotion requires returned Recall@100 at least 99.0%, p05 at least 90,
every query at most 32 GETs and 16,777,216 data bytes, and a coherent local
read budget at K no more than 1024. These are development filters, not
validation evidence. If the unrestricted same-layout control fails, repair
the physical plan or abandon this combination; V85's different layout cannot
rescue it. If f32 succeeds but PQ192 fails, test a preregistered four-summary
arm. If only K=2048 succeeds, reject the proposed local-read budget and
reconsider sharding or the memory target. A passing 1M gate authorizes one
matched-data 10M page-growth replay, then a new untouched serving cohort.
Only the latter can measure p95 latency and sustained QPS. Historical V82
10M deep-image-96 results are not a ReLAION-768 growth measurement.

## Formal boundary

Lean can verify row-byte inequalities, exact range accounting, finite-query
truth-hit lower bounds, score-gap stability when a certified approximation
error is supplied, and a latency or throughput implication when service
time and bandwidth premises are supplied. It cannot establish unseen-dataset
recall or AWS p95 latency/QPS from algorithm source alone. Each empirical
premise must be bound to a complete authenticated artifact and split; the
checked theorem then states exactly what follows from it.

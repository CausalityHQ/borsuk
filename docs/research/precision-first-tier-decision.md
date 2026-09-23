# Precision-first route after the G0 code-only failures

Status: architecture hypothesis and next development gate, 2026-09-23. The
operator clarified that resident memory may increase at 100M if the measured
quality, latency and node cost justify it. The 3-GiB figure is therefore a
research target, not a release gate. The memory envelope should scale with
vector count and the selected recall target rather than switching at an
arbitrary collection-size threshold. No production format or 100M performance
claim is frozen here.

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
combination with a resident PQ64 tier and the precise capped reader.

The executable V85 100M worksheet (`scripts/v85_memory_worksheet.py`)
charges 1,098,772,248 bytes apart from its one resident row-code generation.
An optimistic reuse of that entire reserve for two independent full
100M-row generations under 3 GiB leaves 2,122,453,224 bytes for both row
planes, or at most ten **integer** bytes per row in each. The Lean theorem
`two_generation_width_with_v85_reserve_at_most_ten` checks that implication.
The reserve is an assumption from a different architecture, not a measured
two-generation peak. The weaker 64-MiB-only bound is 15 bytes per row. A
resident PQ64 tier cannot fit either **3-GiB** model; it is still eligible
under the clarified product requirement. Compact page summaries can fit
arithmetically if scored while compressed. Page-cache memory, allocator
overheads, query buffers and overlapping generations must be charged in an
actual implementation and measured as node/cgroup memory, not only process
RSS. Do not call a larger design a failure merely because it misses 3 GiB.

The production decision is a measured frontier, not a single byte ceiling:
for each collection size `N`, returned-recall target `R`, query concurrency
`C`, and maximum pinned generations `G`, report both resident bytes and
node-charged bytes as `M(N,R,C,G)`, together with p50/p95 latency, sustained
QPS and node cost. Record the fixed intercept and bytes/vector for each
candidate and locate any observed changes in slope at 100k, 1M, 10M and
100M. Do not extrapolate a new knee from a different dataset or count a
projected memory number as a measured peak. Select the least-cost qualified
point at each declared quality target, with a stated rollover and growth
reserve; a higher target may require a wider resident representation.

## Candidate, with explicit uncertainty

Store authoritative immutable page bodies, codebooks and generation manifest
in S3. Keep a page-summary hierarchy and the page-ordered PQ64 row-code
plane resident for the first serving candidate. Choose a shortlist and fetch
authenticated SQ8 data pages in one S3 wave for final scoring. The PQ64 plane
costs 6.4 GB per 100M full generation, or 12.8 GB during a two-generation
rollover, before summaries and service reserves. This requires a measured
memory, CPU, latency, throughput and node-cost trade against competing
budgets; V75's 1M quality does not establish it at 100M.

An on-node storage tier is a later cost-reduction arm using the *same* row
codes and data plan if resident memory is too expensive. It adds cache warm-up,
local read tail, persistence/recovery and node-state obligations. `pread`
alone does not bypass the kernel page cache; any memory claim must account for
cache charging or use and validate direct I/O. No local IOPS result is
qualified yet. A second S3 code wave remains an unqualified alternative;
historical two-wave 64-GET/33-MB results do not prove mathematical
impossibility under a different planner, but they miss the present total cap.

This candidate may fail even at 1M because page summaries may miss rows
needed by PQ64 nomination. Even a 1M quality pass says nothing about
10M/100M selected-page growth, locality, p95 latency, throughput or the
charged two-generation peak.

## Canceled live gate and corrected next gate

An attempted complete ReLAION-1M **development** 1,000-query replay of the
existing V77 1,024-region, 512-row, gap-2 native reader was reserved at
`research/v109-hierarchical-1m/5753cefa/runs/dev1000-r1024/a0001/` and
launched on Spot instance `i-0f32dbb7a9d44715d` from pushed source
`5753cefa760cddaa6558f392e0aa888ae0b81959`. It was canceled before a
new result because the *terminal-complete prior V77 200-query result* at
exactly that operating point was independently fetched and found to report
**61 GETs at p95**, versus the current hard cap of 32 per query. Its other
historical numbers are 99.155% returned Recall@100 on ReLAION-1M development
200, 18 GETs at p50, 10,782,720 bytes at p50, and 41.842 ms total p50.
The old result SHA-256 is
`b66b3d660c4e282034efce91ce08c61fbc363d9b0ea4a3640c7b0e20ff4818e1`;
its terminal exit code is zero. `cancelled.json` records the new instance,
source and reason. The new attempt has **no measurement result** and must
never be described as a failed quality measurement. No additional live read
cell is justified on the unchanged planner.

The next gate is read-free physical planning on the same V63 layout and
ReLAION-1M development roster. Authenticate the inputs, reproduce the V77
prefix row nomination, and replace gap-2 coalescing with a truth-free exact
range admission algorithm. It must score the bytes of every bridged page and
reject or trim any plan over 32 GETs or 16,777,216 bytes **before** a query
read. Pair against V77's old page set to show which rows/pages were lost and
why. If no such planner preserves at least 99.0% returned Recall@100 and p05
90 with precise scoring, change page layout or the routing representation;
do not tune the same unconstrained coalescer. A passing development replay
permits a new untouched live serving cohort, not reuse of the canceled cell.

The executable V109 candidate uses V77's 1,024-region PQ192-reconstructed
page summaries and PQ64 row codes, selecting the top 512 encoded rows. It
ranks their distinct pages by best row-code score. The capped planner admits
pages in that order, merges the cheapest physical gaps when needed to stay
within 32 GETs, and skips a page if its full bridged interval would exceed
16,777,216 SQ8 bytes. It scores **all** rows in the admitted intervals from a
local authenticated copy of V70's SQ8 object. Its paired control scores all
rows in V77's original gap-2 intervals on the same queries. The existing
200-query V77 result is the prefix control; a difference greater than ten
GT100 hits, three p95 GETs, or one 199,680-byte page at p50 is a harness
mismatch, not a new architecture result. On the first 200 development queries,
stop and publish a terminal negative result if capped returned Recall@100 is
below 99.0%, p05 below 90, or a cap is violated. Only a passing prefix may
run the complete 1,000 development queries with the same frozen algorithm.
The complete cohort uses the same 99.0%/p05-90/cap promotion filters. The
separate reducer recomputes plans, membership and truth hits. This offline
replay reports planned network bytes and GETs, not live S3 latency or QPS.

The first V109 bootstrap attempt under source
`7a3c74a46dea0309348c31d59334911f51fc4b3c` launched Spot instance
`i-02c32cf9ec2a5f5e0` and emitted a terminal `failed` marker after 17
seconds in `dependencies`, before input authentication or science. The
instance is verified terminated and the attempt has no result. Its worker
had not captured early standard output, so the particular dependency
command failure is not established. The next source records worker output
and finer setup phases, initializes `HOME` before installing `uv`, and uses
a new attempt ordinal. The original terminal is immutable.

V109 attempt `a0002` completed its 200-query prefix and stopped by the
registered rule: capped returned Recall@100 was **98.695%**, p05 95, zero
GET/byte cap violations, versus the paired V77 plan's **99.160%** but 63/200
cap violations. The matched prefix control passed. The instance was
terminated, and the 1,000-query cohort was deliberately not run. See
`docs/research/v109-capped-reader-closeout.md` for authenticated evidence.
The next decision is the exact truth-aware interval cover under the same
32-GET/16-MiB cap **for full 256-row SQ8 pages**; no further local
coalescing tweaks are justified until it separates current page-geometry
infeasibility from query-only planner error. A failure would leave sub-page
ranges and a new layout as distinct architectural options.

The longer summary-width and row-code access gate below remains conditional
on a cap-safe physical planner:

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
score their resident PQ64 row codes, nominate rows and
fetch/scored SQ8 pages under the same interval planner. Freeze the summary
score, tie breaks, shortlist width (512 primary; 1024 diagnostic), range
coalescing and stop criteria before opening truth. Include an f32-summary
arm at K=1024 to distinguish summary compression from insufficient page
count. Save per-query selected page IDs, nominated row IDs, planned GETs,
bytes, truth hits and returned hits, plus authenticated code-plane bytes.
Report aggregate mean, p05, sub-90, resident code bytes, scored row count,
and counterfactual local-tier bytes/read count.
The independent reducer must validate roster/layout/code identities,
recompute interval bounds and rerank returned IDs from the same selected
rows. Local-tier bytes and read count are *counterfactual* cost diagnostics,
not measured I/O in this resident-code replay. No new query-path S3 GET or
latency/QPS claim is made by this replay.

Promotion requires returned Recall@100 at least 99.0%, p05 at least 90,
every query at most 32 GETs and 16,777,216 data bytes, and a measured or
bounded 100M resident and scan-cost trajectory at K no more than 1024. These are development filters, not
validation evidence. If the unrestricted same-layout control fails, repair
the physical plan or abandon this combination; V85's different layout cannot
rescue it. If f32 succeeds but PQ192 fails, test a preregistered four-summary
arm. If only K=2048 succeeds, revisit the coarse index and scan cost. A passing 1M gate authorizes one
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

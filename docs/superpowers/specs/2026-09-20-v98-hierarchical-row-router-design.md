# V98 Hierarchical Row-Score Router Design

## Purpose

V97 established on all 1,000 ReLAION-1M development queries that the common
two-summary, 128-page fence is the dominant quality loss. Even exact float32
row scoring behind that fence reached only 82.193% average Recall@100 and 51%
p05 Recall@100. Choosing a row-code width behind the same fence would therefore
select among representations using a router that has already failed.

V98 replaces only that causal bottleneck. It must first prove that a fixed,
query-independent hierarchy can expose enough candidate rows while keeping
100M routing work bounded. Only after the exact-float32 ceiling passes does it
compare PQ16, PQ24, PQ32, PQ32x4, and summary-only routing under the unchanged
32-GET and 16-MiB serving budget.

This is a development-screen architecture, not a serving fallback. It neither
adds benchmark manifests to production reads nor preserves an old persistent
format. A later production slice may implement the winning representation in
the native snapshot format using typed Rust Serde structures.

## Evidence and Success Criteria

The frozen dataset is ReLAION-1M: 1,000,000 source rows, 768 float32 dimensions,
the existing 1,000-query development split, and exact top-100 truth. V98 binds
the same source, queries, truth, native generation, base run, delta run, and
page map as V97. Every result retains one sample per query and is independently
recomputed from immutable artifacts.

The absolute gates are unchanged:

- average Recall@10 at least 96.0000%;
- average Recall@100 at least 97.5000%;
- p05 Recall@100 at least 90%;
- at most 32 physical GETs and 16 MiB of encoded page data per query;
- a complete 100M resident-memory projection below 3 GiB;
- 10,000 paired bootstrap resamples using one registered seed and the same
  query-index matrix for every arm comparison.

V98 is successful only when its exact-float32 ceiling and at least one row-code
arm pass every applicable gate. A width winner is the smallest resident-eligible
row representation whose paired average-Recall@100 interval is not inferior to
PQ16, with p05 Recall@100 and Recall@10 as deterministic tie breakers. If no arm
passes, the winner is null and G1 remains open.

## Considered Approaches

### Chosen: bounded hierarchy followed by row scoring

Group contiguous physical pages into fixed eight-page root groups. Score the
resident root summaries, expand only enough root groups to expose at most 4,096
page-summary records, retain at most 1,024 pages, scan row codes for at most
262,144 rows, shortlist 2,048 rows, then use the existing deterministic
budgeted page selector. At 100M rows this scores fewer than 65,536 root groups
and keeps child and row work independent of total corpus size.

This preserves the useful property demonstrated by V77: the hierarchy filters
where row scoring occurs but does not replace row scoring with a page-centroid
decision. It also keeps all intermediate work resident; S3 is contacted only
for the final selected data pages.

### Rejected: widen the flat page-summary fence

Widening V97's fence can recover quality at 1M, but it must score every page at
100M and offers no bounded transition from roughly 3,907 pages to roughly
390,625 pages. It treats the failed representation as a capacity problem and
does not establish a scalable serving path.

### Rejected: protected rescue after the small fence

Historical protected-rescue evidence reached high recall by issuing roughly
88 GETs and reading roughly 38 MiB. It violates both fixed serving budgets and
would make row width appear successful only by spending more object-store work.

## Fixed Hierarchy

### Layout

Pages remain immutable 256-row units in the authenticated native generation.
The hierarchy is derived from page order without using query or truth data:

1. Each page has exactly two 16-byte PQ16 summary codes, as in V97. They encode
   the mean of each nonempty contiguous half of that page's visible rows; a
   one-row page duplicates its only mean.
2. Eight consecutive pages of the same object role form one root group. Base
   and delta pages are never mixed in one group. A final partial group is valid.
3. Each root group has exactly two 16-byte PQ16 summary codes. Its ordered child
   pages are split into two nonempty contiguous halves and each code represents
   the mean of the visible source vectors in one half; a singleton group
   duplicates its only mean. The shared summary codebook is fitted only from
   base-tier page-summary training vectors with the registered seed, then used
   to encode page and root means for both roles.
4. Root groups, page summaries, page ordinals, row-code offsets, and final
   partial counts have explicit Arrow IPC schemas, little-endian numeric
   buffers, exact lengths, and SHA-256 identities.
5. Base and delta page keys retain total order `(object_role, ordinal)`.
   Generation visibility and latest-write/tombstone semantics are resolved
   before the hierarchy is built; obsolete rows never enter summaries or codes.

At 100M rows and 256 rows/page there are 390,625 pages and 48,829 eight-page
root groups. The ceiling of 65,536 root groups therefore covers the full target
without a dataset-specific exception.

### Query flow

For each query:

1. Compute PQ16 ADC scores for every root summary and score each root by the
   minimum of its two summary distances. Break ties by root ordinal.
2. Retain the best root groups until their children contain at most 4,096 page
   records. A partial final root is not split; because every root has at most
   eight pages, the exposed count cannot exceed 4,096.
3. Score the exposed page summaries and retain the best 1,024 pages, breaking
   ties by `(object_role, ordinal)`.
4. Scan only the row codes belonging to those pages, at most
   `1,024 * 256 = 262,144` rows. Score in deterministic blocks and retain the
   best 2,048 `(distance, row_id)` pairs without allocating or sorting all
   pairs. Ties are broken by row ID.
5. Feed shortlisted rows, in score order, to the existing page-budget selector.
   It stops before exceeding 32 GETs or 16 MiB. Final selected pages are sorted
   only for physical reads; their selection priority remains evidence.

The summary-only control skips step 4 and feeds the page-summary ranking to the
same budget selector. It is evidence, not a candidate production row format.

## Fail-Fast Scientific Sequence

One immutable attempt executes the following stages in order and records a
terminal classification at the first failure:

1. **Authority:** authenticate all six V97 objects, generation bindings, page
   map, source commit, registered critique hash, hierarchy arrays, and exact
   configuration.
2. **Hierarchy containment:** for all 1,000 queries, recompute which exact truth
   rows lie in the retained 1,024 pages. If average top-10 containment,
   average top-100 containment, or p05 top-100 containment misses the absolute
   quality gates, emit `hierarchy-containment-rejected` and stop before fitting
   any row-width arm.
3. **Exact ceiling:** score exact float32 rows inside the same retained pages,
   shortlist 2,048, and apply the same page planner. If it misses any quality or
   resource gate, emit `hierarchy-exact-ceiling-rejected` and stop.
4. **Width arms:** fit and evaluate PQ16, PQ24, PQ32, and PQ32x4 from the same
   query-blind base-tier training cohort. Evaluate summary-only as the control.
5. **Independent reduction:** authenticate raw result bytes and independently
   recompute every per-query hit set, recall, aggregate, percentile, physical
   budget, paired interval, memory projection, eligibility decision, and winner.

This sequence makes a router failure cheap: no representation training or
large result production occurs when the hierarchy cannot contain truth.

## Memory and Work Accounting

The existing V97 100M worksheet remains authoritative for row codes,
codebooks, SQ8 parameters, mutation directory, resident delta rows, page
directory reserve, response buffers, planner workspace, and runtime reserve.
V98 adds explicit, non-overlapping terms for:

- two 16-byte page summaries per page;
- two 16-byte root summaries per root group;
- page-to-root and root-child offset/count arrays;
- row-code offsets for base and delta pages;
- bounded query workspace for root scores, 4,096 child scores, 262,144 scanned
  row scores, and a 2,048-entry top-k heap.

For 100M rows, the new encoded summary payload is 12,500,000 bytes for pages
and 1,562,528 bytes for roots before array headers and offsets. The result must
derive every term from registered counts and integer widths; no projected term
may be omitted, reused under two labels, or inferred from process RSS. PQ32 is
expected to remain ineligible because row codes alone are 3.2 GB, but the
validator, not this expectation, decides eligibility.

Per-query work is bounded by fewer than 131,072 root-summary ADC evaluations,
8,192 page-summary ADC evaluations, 262,144 row-code evaluations, and a
2,048-entry retained set. Producer evidence records actual counts and rejects
any sample exceeding a bound.

## Typed Evidence Contract

Python research code uses frozen dataclasses for inputs, configuration,
identities, samples, projections, decisions, and terminal receipts. JSON is
only the canonical wire encoding of those typed values: sorted keys, compact
separators, one trailing LF, finite numbers, and no untyped `json!`-style
construction. Large numeric/vector artifacts use Arrow or Parquet with exact
physical schemas and non-null fields.

The raw result schema is `borsuk-v98-hierarchical-row-router-v1`. It contains:

- all immutable object URI/SHA-256/length identities and source commit;
- hierarchy construction parameters and every hierarchy-array identity;
- the exact root, page, row-scan, shortlist, GET, and byte limits;
- one containment sample and one exact-ceiling sample for each query;
- one sample per query per evaluated arm, including ordered selected pages,
  hit IDs, GETs, bytes, and recall values;
- artifact identities for each trained codebook and code array;
- complete 100M memory projections;
- the paired-bootstrap seed, resample count, intervals, stop classification,
  and winner.

The validator accepts only this schema. There is no V97 compatibility reader.
It rejects missing/extra fields, bool-as-int values, non-finite values, order or
cardinality drift, duplicate IDs/pages, identity drift, and any producer claim
that differs from recomputation.

## Testing

Development follows strict RED/GREEN slices:

1. typed authority and hierarchy layout validation;
2. deterministic root/page expansion with tie, partial-root, and cap cases;
3. bounded blockwise row scoring differential against a scalar full sort for
   every row format;
4. containment and exact-ceiling stop classifications;
5. full per-query result production, memory worksheet, and paired decision;
6. independent reducer mutations covering every identity, limit, sample,
   aggregate, interval, projection, classification, and winner branch;
7. Spot launcher single-attempt, interruption, terminal, and cleanup behavior.

Local verification remains narrow and must stop or move to Spot if resident
memory exceeds 3 GiB or memory PSI full avg10 exceeds 0.5. The full 1M screen
runs once on Causality AWS Spot from a clean commit. The launcher syncs terminal
evidence to S3 and terminates the instance immediately. An interrupted cell is
discarded and may be restarted as a new registered attempt; incomplete result
files are never inspected.

## Promotion and Next Boundary

V98 promotes exactly one row width or leaves G1 unresolved. Promotion requires
the independent reducer to reproduce the producer result byte-for-byte in all
derived claims and the winning arm to satisfy quality, resource, paired-CI, and
100M memory gates. No result from V77, V85, V94, or V97 can substitute for the
new immutable attempt because the hierarchy and evidence schema changed.

After promotion, and not before, the winner is implemented in the native
snapshot format with generation/delta/mutation/compaction semantics. G2 then
proves 100k quality and 1/10/100-run equivalence. A 1M latency/cost cell, 9.99M
reproduction, write qualification, and 100M build remain fenced behind their
respective goal gates.

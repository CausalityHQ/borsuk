# V99 Ranked-Gap Range Router Design

## Purpose

V98 proved on all 1,000 ReLAION-1M development queries that its hierarchy
contains enough truth (98.5180% average Recall@100 and 92% p05), but treating
each 256-row page as one GET makes the final gate impossible. Exact scoring
reached 83.3020% average Recall@100; a truth-aware 32-page oracle reaches only
85.9710% because the top 100 occupy 43.337 pages on average.

The immutable base and delta objects already store consecutive pages as
contiguous byte ranges. A truth-aware variable-range oracle passes every gate:
98.7500% average Recall@10, 98.3720% average Recall@100, and 91% p05
Recall@100 at no more than 32 range GETs and 16,740,984 bytes. V99 tests the
smallest query-blind analogue: preserve V98's hierarchy and row scoring, but
coalesce ranked candidate pages into deterministic same-object byte ranges.

This is a G1 research screen, not a production fallback. Production Rust and
the native snapshot format remain frozen until a row width wins.

## Fixed Evidence and Gates

V99 binds V98's six immutable ReLAION-1M development objects, source commit,
page map, generation, exact top-100 truth, and completed G1 critique hash. It
uses all 1,000 development queries and never reads validation or sealed
holdout queries.

The gates remain:

- average Recall@10 at least 96.0000%;
- average Recall@100 at least 97.5000%;
- p05 Recall@100 at least 90%;
- at most 32 physical same-object range GETs;
- at most 16,777,216 encoded bytes;
- a complete 100M resident-memory projection below 3 GiB;
- 10,000 paired bootstrap resamples with seed 7216.

## Ranked-Gap Planner

V99 retains V98's eight-page roots, 4,096-page exposure cap, 1,024 retained
pages, and 262,144-row scan cap. Exact-f32 and compressed arms retain the best
8,192 `(distance, row_id)` pairs in deterministic total order.

The planner consumes ranked row pages in that order:

1. Ignore a page already covered by a selected interval.
2. Insert the page as a singleton interval in its base or delta object.
3. If more than 32 intervals exist, merge the adjacent same-object pair with
   the smallest additional gap bytes. Ties use object role, left ordinal, then
   right ordinal. A merge includes every intervening physical page.
4. Accept the proposal only when interval count is at most 32 and the sum of
   exact byte spans is at most 16,777,216. Otherwise revert that candidate and
   continue, because a later ranked page may already lie inside an interval or
   require a cheaper gap.

Intervals are sorted, nonoverlapping, never cross object roles, and derive
their bytes from authenticated page offsets and lengths. Final selected pages
are exactly the union of interval pages. GET evidence counts intervals, not
pages; byte evidence sums interval spans, including gap pages. This models S3
Range GETs directly and never downloads the whole object.

The summary-only control feeds its deterministic page-summary ranking through
the identical planner. It remains evidence only.

## Fail-Fast Sequence

One immutable Spot attempt performs:

1. authenticate the six inputs, generation page ranges, source archive,
   configuration, and critique identity;
2. reproduce V98 hierarchy containment;
3. evaluate the exact-f32 ranked-gap ceiling on all 1,000 queries and stop as
   `range-exact-ceiling-rejected` if any quality or resource gate fails;
4. only after an exact pass, fit PQ16, PQ24, PQ32, and PQ32x4 and evaluate the
   summary-only control through the same range planner;
5. independently authenticate and recompute every sample, range, aggregate,
   interval, projection, eligibility decision, and winner.

The winner rule is unchanged: the smallest resident-eligible width whose
paired average-Recall@100 interval is not inferior to PQ16, with p05
Recall@100 and Recall@10 as deterministic tie breakers.

## Typed Evidence

The result schema is `borsuk-v99-ranked-gap-range-router-v1`. Frozen
dataclasses represent configuration, page intervals, samples, aggregates,
projections, decisions, and terminal receipts. JSON is only their canonical
wire encoding: sorted keys, compact separators, finite values, and one trailing
LF. Large vector artifacts remain Arrow or Parquet.

Each sample records ordered interval `(object_role, first_page, last_page,
offset, bytes)` values, the exact union of selected pages, truth and hit IDs,
recall, roots/pages/rows evaluated, physical GETs, and physical bytes. The
validator rejects overlap, gaps omitted from selected pages, cross-role spans,
offset or length drift, order drift, duplicate pages, cap violations, and any
producer aggregate or decision that differs from recomputation.

## Memory and Performance Accounting

The row representations and hierarchy are unchanged. V99 replaces V98's
2,048-entry shortlist with 8,192 entries, adding 73,728 bytes at 100M. It also
accounts for at most 1,024 temporary singleton intervals and 32 final
intervals using explicit integer widths. The worksheet includes every V97/V98
resident term once and must remain below 3 GiB.

Per query remains bounded by fewer than 131,072 root-summary ADC evaluations,
8,192 page-summary ADC evaluations, 262,144 row-code evaluations, and one
8,192-entry heap. Gap merging operates on at most 1,024 retained pages and
does not allocate corpus-sized state. S3 work remains at most 32 bounded Range
GETs and 16 MiB; no full-object or manifest-driven serving fallback exists.

## Rejection Rules

- The fixed aligned two-page design is rejected: its truth-aware oracle is
  only 95.0600% average Recall@10, 93.2500% average Recall@100, and 75% p05.
- If exact ranked-gap scoring fails once on the registered 1,000-query cell,
  this hypothesis is killed; no parameter tuning or rerun is allowed.
- No 10M, 100M, G2, or competitive parity work proceeds without an exact pass
  and a width winner.

## Verification

TDD covers interval insertion, cheapest-gap ties, same-role isolation,
partial final pages, exact byte spans, revert-and-continue behavior, cap
boundaries, selected-page union, canonical schemas, hostile reducer mutations,
and equality with a literal scalar reference. The Spot launcher remains
single-attempt, create-only, pressure-monitored, terminal-last, and terminating
on every path. The final cell records one sample per query, paired intervals,
resource telemetry, cost, artifact identities, and independent byte-identical
recomputation in the evidence ledger.

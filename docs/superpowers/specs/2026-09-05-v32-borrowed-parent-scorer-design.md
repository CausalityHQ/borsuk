# V32 borrowed parent scoring

## Decision and scope

Use one production-private parent scorer for the existing resident router
and a test-only encoded-object adapter. Preserve routing and candidate
semantics before adding network scheduling. Duplicating the scoring kernel
would make equivalence fragile; changing routing at the same time would hide
which change caused a recall regression. This slice changes neither.

The bounded Arrow codec at8fc2c692 supplies authenticated parent records.
Add a sequential borrowed cursor so scoring does not repeatedly count bitmap
prefixes or search ranges. Its constructor validates the parent once; the
immutable borrow prevents later mutation. A cursor stores parent reference,
range index/position, local row, and base/high byte positions only. It returns
`(u64,V30PqWidth,&[u8])` until exhaustion, then always returns None. Code slices
point into the parent's existing buffers; no per-row allocations or copies.

## Shared scoring boundary

Extract the current parent-loop body in `v30_s3_search.rs`. A private
`score_parent_codes` helper consumes normalized query, original f16[96]
centroid, fallible iterator of borrowed logical/width/code tuples, mutable
base/high `V30LazyQueryTable` references and `BoundedCandidates`.
It computes the same f32 residual subtraction and calls both `begin_parent`
methods, preserving eager validation even if a width has no rows.

Use blocks of at most32 input rows, split borrowed code references by width,
score with existing `score_block_into`, and restore original slots before
candidate insertion. Keep score `total_cmp` then ascending logical ID, the
existing prune window and candidate limit. No new SIMD, quantizer, distance
normalization or approximate reducer. The root64 experiment's524288 scan
budget and12288 candidate limit do not change.

The resident adapter traverses its existing selected leaf ranges in their
existing order and fires the leaf observer at range entry. Range boundaries
may delimit blocks as before; object cursors may cross them. Since each code
has independent fixed-order ADC arithmetic, output scores and candidates
must be bitwise identical. Implementation work counters need not be identical
when delivery/block order changes and are not scientific equivalence evidence.

Codec registration stays `cfg(test)` until a real production object consumer
lands. Merely sharing a generic scorer with the resident route does not justify
fake public codec exports or dead-code suppression. Resident full code planes
remain during this differential slice and are explicitly not the final serving
architecture.

## Evidence and resource bounds

Cursor tests use literal mixed-width codes and gapped ranges, byte-boundary
transitions, all-base/all-high populations, exact last row and repeated EOF;
invalid states fail construction. Pointer identity verifies borrowing.

Scorer tests compare independently enumerated scalar per-row ADC with shared
scoring, then compare resident and encoded/decoded object populations. Cover
distinct original parent centroids, mixed widths, gaps, ties, more than32 rows
per block and at least64 physical pages. Compare every retained logical ID and
score bit, then physical16/64-page prefixes; reorder whole-parent delivery.
Retain existing root64 and parent-residual regression tests.

Only one lazy table pair, fixed32-row buffers and bounded candidate state are
resident in the helper. The cursor adds constant state; it neither reads page
bodies nor allocates a logical-row map. This slice does not prove bounded S3
fetch lifetimes, selected-object authority, cold latency or100M recall. Those
remain subsequent directory/provider qualification work.

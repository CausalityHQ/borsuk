# V30 Geometric Page Packing Design

## Decision

V30 will replace arbitrary PQ-label page ordering with deterministic balanced
geometric packing inside each already-selected hierarchy leaf. The hierarchy,
24/48-byte PQ codes, fidelity selection, candidate scan, page cap, Arrow page
format, and exact reranker remain unchanged. Construction alone may see exact
vectors; serving still reads only selected immutable S3 pages.

This is a causal, claim-ineligible falsifier before any 9.99-million-row or
100-million-row gate. It asks whether page composition—not hierarchy recall or
PQ scoring—is preventing the same high-quality candidates from concentrating
in fewer S3 objects.

## Why the current order is weak

The current merge key is `(leaf_ordinal, base_code, source_ordinal)`. A PQ code
byte is the ordinal of a trained centroid, but centroid ordinal is a nominal
label rather than a geometric coordinate. Lexicographic adjacency therefore
does not imply residual-vector adjacency. Candidate scoring can be exact enough
to find the right rows while the first candidates occupy too many unrelated
pages.

The authenticated 100,000-row Deep Image run reached 320/320 Recall@10 while
reading at most 16 pages and 2,928,808 bytes. Routing CPU median was 7.864633 ms,
but page-read elapsed median was 58.650433 ms. This preserves quality but misses
the latency target; page concentration and request count are now the causal
variables.

## Alternatives considered

1. **Balanced geometric micro-clusters (selected).** Buffer one leaf under an
   explicit row cap, recursively split its exact normalized vectors into
   balanced page-sized groups, and emit each group contiguously. This directly
   optimizes the physical unit fetched from S3 without adding serving memory.
2. **Residual SimHash sort.** Add a deterministic query-independent projection
   signature and sort by it. It is cheaper to build and remains a useful
   control, but a scalar order has discontinuities and does not directly
   enforce balanced geometric pages.
3. **Resident page centroids or multi-prototypes.** Score metadata before GETs.
   Prior authenticated fixed-prototype experiments failed quality, and this
   adds resident bytes without fixing poor page membership. It is rejected for
   this slice.

## Packing algorithm

The external merge continues to group records by leaf. A new leaf packer holds
at most 65,536 records for one leaf; exceeding that bound stops construction.
For a leaf containing `n` rows, it creates `ceil(n / page_rows)` groups with
sizes differing by at most one.

Partitioning is recursive and deterministic:

1. Sort the current group by source ordinal.
2. Use the first row as the left seed. Choose the row with minimum cosine
   similarity to it as the right seed, breaking ties by source ordinal.
3. For four iterations, order rows by
   `(dot(row,right_centroid) - dot(row,left_centroid), source_ordinal)`, split
   at the exact required left cardinality, and recompute normalized f32
   centroids for both sides.
4. Reapply the same ordering after the fourth update, split, and recurse until
   each output group is one page.
5. Order final groups by their recursive left-to-right order and rows within a
   group by the final split order. This order drives PQ logical positions,
   fidelity ranks, page offsets, and Arrow page bodies together.

No query, truth, result, page-read credential, or prior layout is available to
the packer. Exact vectors remain transient and are released when the leaf is
flushed. The 65,536-row cap bounds the largest leaf buffer to under 32 MiB at
the current record width, separate from external-sort scratch.

## Authority and formats

The persistent formats do not change: metadata remains Parquet, typed serving
artifacts remain Arrow IPC, page bodies retain the strict Arrow `id` plus
non-null f32[96] schema, and receipts remain sorted compact JSON plus LF. A
construction manifest records the exact packing algorithm name
`balanced-cosine-v1`, `page_rows`, maximum observed leaf rows, four iterations,
and the source/codebook/hierarchy identities. There is no compatibility reader
or alternate production dispatch; the old lexicographic layout is only a
frozen experimental control.

## Fast falsifier

The local RED/GREEN gate uses synthetic leaves whose PQ centroid labels are
deliberately permuted. It requires complete one-owner coverage, deterministic
bytes across worker/scratch configurations, page sizes differing by at most
one, strictly smaller within-page cosine dispersion than lexicographic order,
and identical PQ scores and exact-rerank results.

The first remote gate rebuilds only the frozen 100,000-row Deep Image corpus.
It evaluates two independently constructed layouts with identical hierarchy,
codebooks, fidelity rows, queries, truth, candidate depth, and page limit:

- frozen lexicographic control;
- `balanced-cosine-v1` treatment.

Both use `page_rows=128` so the fixture actually contains multiple pages per
leaf. Page-count arms 4, 8, and 16 are preregistered before query execution.
The primary comparison is at equal page count and observed encoded bytes. The
treatment must not reduce aggregate or minimum Recall@10 and must reduce both
unique pages containing the exact top candidates and S3 bytes fetched. A useful
advance is at least 995,000-ppm aggregate recall, 800,000-ppm minimum recall,
and at least 31/32 perfect queries at eight pages. A 512-query disjoint holdout
is then generated and frozen against the same 100K corpus before any larger
gate; the burned 32 queries remain regression evidence only.

Request latency is first simulated from the measured GET count and recorded
same-region S3 latency distribution without sleeping. Only a passing packing
arm receives one same-region Causality Spot cold-GET run. No complete corpus is
downloaded to the devbox; the disposable builder streams registered Parquet
inputs and writes Arrow pages directly to S3.

## Failure and progression

If balanced packing does not improve page concentration at equal quality and
bytes, reject it without a 9.99M build. If it succeeds, the next independent
optimization is physical object coalescing: place adjacent logical pages into
immutable superpage objects and fetch coalesced byte ranges in one concurrent
wave. That step must preserve exact per-page authentication and is not part of
this design.

Strict Clippy and the full workspace suite run once only after the focused
layout, search, controller, formatting, and 100K causal gates are stable. D3,
100M construction, compatibility code, caching, and competitor claims remain
fenced.

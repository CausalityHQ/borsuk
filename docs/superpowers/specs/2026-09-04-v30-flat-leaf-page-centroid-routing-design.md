# V30 Flat-Leaf Page-Centroid Routing Design

## Decision

V30 will route directly from a flat scan of all resident leaf centroids to a
bounded scan of the geometric page centroids owned by the best leaves. It will
stop using the root frontier and row-level PQ candidate heap to choose S3
pages. Exact vectors remain only in immutable Arrow page objects, and serving
still downloads at most 16 logical pages.

This is a query-blind, claim-ineligible falsifier. It changes only routing
metadata and page selection; the balanced-cosine page membership, exact page
format, exact reranker, query/truth authorities, and S3 byte accounting remain
fixed.

## Evidence and failure mechanism

The authenticated 100,000-row geometric treatment at eight pages transferred
at most 412,112 bytes but reached only 915,625-ppm aggregate Recall@10. At 16
pages it transferred at most 791,088 bytes and reached 987,500 ppm, with
800,000-ppm minimum recall and 29/32 perfect queries. It missed four of 320
truth neighbors.

Page-free diagnostics located all four initial losses at the root/leaf
frontier. With `root_beam=12` and `leaf_beam=192`, every relevant leaf entered
the frontier, but one truth page was only the 190th distinct page induced by
row PQ order and three truth rows fell outside the global 12,288-candidate
heap. Reciprocal-rank page aggregation did not select them. Therefore neither
more S3 reads nor a different reduction over the same retained PQ rows fixes
the causal boundary.

## Alternatives

1. **Flat leaf scan plus page-centroid scan (selected).** At 100 million rows,
   32,768 f16 leaf centroids require about 6 MiB and only 3,145,728
   96-dimensional score components per query. Selecting 512 leaves exposes an
   expected 12,288 128-row pages and a hard maximum of 32,768 pages, another
   at most 3,145,728 score components. This is
   bounded metadata work and removes both observed frontier and candidate-heap
   loss modes.
2. **LSH or SimHash first level.** Random-projection buckets can be compact,
   but high recall requires multi-probe tables whose scattered postings create
   more S3 fan-out and a new tuning surface. It is retained only as a future
   control, not the primary router.
3. **Root/leaf spill replication.** Assigning boundary rows or leaves to
   multiple parents can repair frontier misses, but duplicates construction
   and page storage and still leaves the row-PQ-to-page reduction problem.

## Persistent authority and formats

`page-offsets.parquet` gains one required non-null fixed-size-list
`centroid: f16[96]` column. Each value is the normalized f32 mean of the exact
vectors in that page, rounded once to f16 for persistence. The builder rejects
non-finite or zero-norm means. The manifest records
`routing_algorithm=flat-leaf-page-centroid-v1`, the geometry-specific leaf
beam (`192` for the 256-leaf 100K screen and `512` for 32,768 leaves),
`page_count=16`, `maximum_pages_per_leaf=64`, the page-centroid physical
schema, and the exact Parquet identity. Metadata remains Parquet,
hierarchy/code artifacts remain Arrow IPC,
page bodies remain Arrow IPC, and receipts remain sorted compact JSON plus LF.

The reader authenticates the complete Parquet bytes before exposing any
centroid. It requires consecutive page ordinals, exact leaf ownership and row
ranges, f16[96] concrete types, finite nonzero normalized centroids, and exact
agreement with page counts. There is no legacy reader, alias, optional field,
or identity-based dispatch because the project is prerelease.

## Serving algorithm

1. Normalize the query once.
2. Score every leaf centroid using the existing deterministic f32 distance
   kernel and retain the configured leaf beam by `(distance, leaf_ordinal)`.
3. Enumerate only pages owned by those leaves, score each authenticated page
   centroid, and retain the best 16 by `(distance, page_ordinal)` with a bounded
   heap. A corpus-sized `(score,page)` allocation or sort is forbidden.
4. Fetch the 16 immutable Arrow pages concurrently in one wave, authenticate
   each body, deduplicate source ordinals, and exact-rerank f32 vectors.

The root hierarchy and row PQ scores remain diagnostic evidence but are not
allowed to influence page choice. Work receipts report all leaf centroids
scored, page centroids scored, selected pages, GETs, encoded bytes, decoded
rows, and phase CPU/elapsed time.

## Bounds

For the 100-million-row projection with 32,768 leaves and 128 primary rows per
page:

- leaf centroids: `32,768 * 96 * 2 = 6,291,456` bytes;
- page centroids: `ceil(100,000,000 / 128) * 96 * 2 = 150,000,000`
  bytes before Parquet metadata;
- per-query centroid work: at most `(32,768 + 32,768) * 96 = 6,291,456`
  score components, with 4,325,376 expected at balanced occupancy;
- bounded heaps: 512 leaves and 16 pages;
- page bodies: at most 16 GETs and the existing 4,587,520-byte hard stop;
- total resident process RSS remains below 3 GiB.

The fast gate measures rather than assumes SIMD time. The routing CPU gate is
5 ms on the 100K screen and 8 ms on the larger confirmation. Cold S3 latency
is reported separately from compute.

## TDD and causal evaluation

Synthetic tests construct deliberately misleading row PQ codes while page
centroids identify the correct pages. They require deterministic ties,
bounded heaps, exact page cardinality, full schema rejection, and unchanged
exact reranking. A differential scalar implementation must select identical
pages for random, tied, subnormal, and reversed-block fixtures.

The first scientific gate rebuilds only the frozen 100K Deep Image layout and
evaluates the same sealed 32 queries at exactly 16 pages. It must reach at
least 995,000-ppm aggregate recall, 800,000-ppm minimum recall, 31/32 perfect
queries, no more than 1 MiB maximum fetched bytes, and no more than 5 ms
routing CPU. Failure rejects the router before any 9.99M or 100M build.

On success, a query-independent 9.99M construction and a sealed disjoint
holdout establish scalable memory and CPU. Only after quality is sealed may a
separate physical coalescing design bundle four adjacent logical pages per S3
object or authenticated range, targeting four GETs without changing the
logical 16-page selection.

## Fences

No full corpus is downloaded to the devbox. Construction streams registered
Parquet shards on disposable Causality Spot and writes Arrow pages and Parquet
metadata to S3. Query/truth capabilities never reach construction. D3, 100M,
page coalescing, caching, compatibility, and competitor claims remain fenced
until their preceding causal gates pass.

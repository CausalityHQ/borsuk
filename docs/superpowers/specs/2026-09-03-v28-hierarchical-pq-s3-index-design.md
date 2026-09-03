# V28 Hierarchical PQ S3 Index Design

## Decision

Replace V27 page-prototype ranking with a hierarchical IVF-PQ page router.
Exact vectors and IDs remain exclusively in immutable S3 Arrow pages. Serving
keeps only a two-level centroid hierarchy, packed product-quantized row codes,
leaf/page offsets, page identities, and bounded scratch locally. It explores a
small leaf frontier, ranks pages from row-level approximate distances, performs
one concurrent S3 read wave, and exact-reranks the decoded vectors.

This is a pre-release format replacement. V28 has no V27 compatibility reader,
alias, migration path, or duplicate writer.

## Evidence and diagnosed failure

The corrected V27 100K build contains 100,000 primary rows, 15,000 replicas,
346 pages, and about 46.76 MB of page bodies. Its root-8/leaf-32/page-10 screen
recovered 92.1875% aggregate Recall@10 with a 40% minimum. Scanning every root
and every leaf changed that only to 91.5625%/40%, proving hierarchy pruning was
not the limiting layer.

Authenticated truth-page attribution reproduced 91.5625%/40% and located the
misses in page ranking: truth-page best rank had p50 2, p90 9, and maximum 68.
Increasing page modes from 4 to 32 reached only 92.5% aggregate and 70% minimum
recall while projecting 1,380,003,840 mode bytes at 100M. Raising replication
from 15% to 100% reached only 91.875%/40%. Regrouping the same assignments into
balanced local microclusters improved the result to 93.125%/60%, still far
below the perfect development gate. Fixed page summaries therefore discard
the extreme row evidence needed in high-dimensional neighborhoods.

V26 supplies the positive control. Its 32-subquantizer, 16-centroid, 16-byte
PQ4 scan achieved 997,708 ppm aggregate Recall@10, 1,000,000 ppm compliance
with the 800,000-ppm per-query floor, and 11.466 ms p99 on 9.99M rows. Its old
snapshot also stored exact vectors locally, which is not the V28 serving
architecture. V28 reuses the proven packed-code principle while moving exact
vectors to S3 pages and adding hierarchy so a query never scans the full code
plane.

## High-dimensional strategy

The hierarchy and quantizer solve different parts of the problem:

1. A query-independent 1,024-root/65,536-leaf IVF hierarchy removes broad
   geometric variation and limits search to nearby regions.
2. Product quantization preserves row-level residual evidence inside those
   regions. The initial ladder is exactly 32 three-dimensional PQ4
   subquantizers (16 bytes/row) and 48 two-dimensional PQ4 subquantizers
   (24 bytes/row). Both use 16 centroids/subquantizer and deterministic ties.
3. Codes are physically ordered by `(leaf, pq_code, source_ordinal)` and page
   ownership is implicit in leaf/page offsets; no row-to-page array is stored.
4. Exact f32 vectors in the selected S3 pages remove the remaining
   quantization error before returning results.

This is not exhaustive search disguised as hierarchy. The production query
must scan no more than 1,000,000 packed codes, at most 1% of 100M rows, and may
not allocate a corpus-sized score buffer.

## Query-independent construction

Construction has train-corpus read capability and output capability only. It
cannot access queries, truth, evaluation ordinals, prior results, or page-read
credentials. A deterministic hash sample trains the hierarchy and both PQ
widths. One bounded corpus stream normalizes each row, assigns its primary
leaf, encodes its global PQ code, and emits a fixed record to bounded external
leaf, subtracts that leaf centroid, encodes the leaf-residual PQ code, and emits
a fixed record to bounded external sort. Queries subtract the same selected-leaf
centroid before ADC scoring. Exact-zero residuals are valid. Boundary recovery
is performed by query multi-probe, not row replication.

The merge order is `(leaf_ordinal,pq_code,source_ordinal)`. Consecutive records
within a leaf are packed into pages of at most 1,024 rows. Every source ordinal
has exactly one owner and the primary union equals the corpus authority. The
same row order produces the packed code blocks, leaf offsets, page offsets,
and immutable Arrow page bodies, so a code position maps to a page without a
resident row ordinal or page-ordinal column.

## Persistent cross-language format

- `roots.arrow`: nonnullable `centroid: fixed-list<element:f16>[96]`;
- `leaves.arrow`: nonnullable leaf centroid plus `root_ordinal:u16`;
- `pq-codebook.arrow`: exact width-tagged nonnullable f32 centroids;
- `pq-codes.arrow`: leaf-ordered `fixed-binary[512]` transposed blocks plus
  explicit leaf/block offsets; the 24-byte arm uses 768-byte blocks;
- `page-offsets.parquet`: leaf/page ordinal, first code position, row count,
  encoded length, SHA-256, and object key;
- `pages/<sha256>.arrow`: nonnullable `source_ordinal:u64`,
  `id:fixed-binary[8]`, and `vector:fixed-list<element:f32>[96]`;
- Parquet query, truth, sample, latency, and resource evidence;
- sorted compact newline JSON manifests and terminal receipts.

All roles have exact schemas and SHA-256/length authority. Arrow and Parquet
make every persistent scientific artifact readable without Rust. Packed block
layout is fully described in the manifest rather than relying on a Rust repr.

## Resident memory at 100M

The 24-byte arm is the conservative bound:

- 100,000,000 row codes: 2,400,000,000 bytes;
- worst-case per-leaf block padding: 65,536 * 31 * 24 = 48,758,784 bytes;
- roots and leaves in f16: 12,779,520 bytes;
- codebook: 48 * 16 * 2 * 4 = 6,144 bytes;
- leaf offsets, page offsets, identities, and allocator overhead: 128 MiB;
- concurrent query scratch and page decode: 128 MiB;
- optional content-addressed page cache: 128 MiB hard cap.

The total conservative projection is 2,864,197,632 bytes, below the
3,221,225,472-byte (3 GiB) ceiling. The 16-byte arm is about 800 MB smaller.
Exact corpus vectors and IDs are not resident and are never fully downloaded.

## Query algorithm

For one normalized f32 query:

1. score 1,024 roots and retain a fixed root beam;
2. score only their leaf children and retain a fixed leaf beam;
3. build PQ lookup tables once;
4. scan only the selected leaves' packed blocks with the proven SIMD table
   kernel, rejecting an arm if it exceeds 1,000,000 codes;
5. maintain bounded best evidence per encountered page and select at most ten
   pages by `(best_adc_distance,page_ordinal)`;
6. issue exactly one concurrent S3 read wave;
7. authenticate every complete Arrow page and exact-rerank all returned rows by
   `(squared_l2,source_ordinal)`.

The development ladder is lexicographic: widths 16 then 24 bytes; root beams
8/16/32; leaf beams 64/128/256/512; candidate evidence depths
3,072/6,144/12,288. The smallest arm that recovers all 320 truth neighbors on
the fixed 32-query 100K development cohort and satisfies work/memory bounds is
frozen. A sealed cohort is evaluated once.

Only one width is opened by a serving process. The reduced development builder
may emit both width artifacts to avoid rereading the 100K fixture, but they are
evaluated in separate processes and their resident projections are never added
together. The sealed and 100M builders emit only the frozen width.

## S3 behavior and latency

The cold path performs at most ten GETs in one wave and reads at most
4,587,520 encoded bytes. No dependent second wave, full-corpus download,
runtime loader manipulation, or hidden local vector snapshot is permitted.
Standard S3 in the instance region is the primary store. S3 Express may be
qualified only as a separately named deployment if Standard S3 misses the
cold gate; results are never combined.

The fail-fast gate records actual request count and bytes and also computes
`cpu + request-wave p99 + bytes/aggregate-throughput`. Injected-latency tests
must use the same counters. The release target is router/exact CPU p99 at most
15 ms, cold end-to-end p99 at most 100 ms, and hard cold ceiling 150 ms.

## Quality and scale gates

The reduced 100K gate requires exactly 1,000,000 ppm aggregate and minimum
Recall@10 across the 32 fixed development queries. It runs after each
scientific repair and must finish in seconds once its immutable fixture exists.

The sealed 9.99M gate requires at least 995,000 ppm aggregate Recall@10,
997,500 ppm compliance with an 800,000-ppm per-query floor, and at least
800,000 ppm minimum recall. Perfect recall remains visible and is the design
target; a competitor claim requires a paired equivalent benchmark rather than
relaxing methodology after observing results.

The 100M gate additionally requires exactly 100,000,000 unique primary rows,
the same sub-1% code-scan cap, at most ten pages, the 3 GiB projection, and the
same latency bounds. Synthetic scale evidence cannot establish Deep-Image
quality.

## Fail-fast execution order

1. Unit tests lock encoding, hierarchy, implicit page mapping, bounded scan,
   exact rerank, and truthful work counters.
2. A deterministic in-memory reduced fixture catches logic errors in seconds.
3. One immutable 100K Deep fixture evaluates the complete arm ladder without
   S3 and rejects any arm below perfect Recall@10.
4. Only a passing arm runs a bounded same-region S3 latency screen over its
   selected pages.
5. Only then build and evaluate 9.99M on `causality` Spot.
6. Only a sealed pass advances to 100M scale and competitor work.

Focused selectors run per repair. Clippy and the full workspace suite run once
at a stable release milestone, never after every scientific change.

## Monitoring and disposition

Spot controllers poll every 30 seconds, preserve one original process, and
report rows, codes, pages, GETs, bytes, elapsed time, RSS, PSI, swap, instance
checks, and spend. They stop on impaired checks, three PSI-full breaches over
1%, swap growth, five minutes without build progress, one minute without query
progress, or the registered wall limit. Every instance publishes a terminal
receipt and terminates immediately.

V28 supersedes V27 only after its reduced and sealed gates pass. Until then,
the V26 PQ4 result is positive evidence for the code representation and V27 is
negative evidence for page-summary routing; neither is described as the final
S3 product.

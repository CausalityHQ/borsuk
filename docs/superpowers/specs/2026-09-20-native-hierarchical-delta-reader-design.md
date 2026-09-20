# Native Hierarchical Delta Reader Design

## Purpose

BORSUK's selected native reader already measures 99.272% Recall@100 at
1M x 768 and 98.430% at 9.99M x 96 with uncached S3 p50 near 40--42 ms.
Those results use a benchmark-only binary manifest that embeds queries and
truth, keeps 48--64 routing bytes per row, and cannot see the immutable delta
generations built by V85. V85 has correct snapshot, mutation, and compaction
semantics, but its centroid router failed the quality gate. This design replaces
both experimental boundaries with one generic pre-release production format.

There is no compatibility reader. Historical artifacts remain immutable
evidence and are not production inputs.

## Release-quality contract

The reader must meet these gates on every qualified dataset and split:

- average Recall@10 at least 960,000 ppm;
- average Recall@100 at least 975,000 ppm;
- p05 Recall@100 at least 900,000 ppm;
- zero failed queries and exact `(distance, id)` tie ordering;
- uncached in-region p50 at most 100 ms and p95 at most 250 ms;
- no more than 16 MiB of page bodies per query;
- query-visible write throughput at least 100,000 vectors/s in batches and
  visibility p95 at most 250 ms on the qualification host;
- projected complete 100M resident state below 3 GiB, including sixteen
  in-flight 16-MiB responses and a 512-MiB runtime reserve.

The 99% recall line is a stretch tier, not a universal release veto. Vendor
figures remain contextual because their datasets and cutoffs are not paired.

## Cross-language format

One canonical sorted compact JSON generation root with one trailing LF binds
every object by role, URI, SHA-256, byte length, schema, generation, previous
generation digest, source identity, dimensions, metric, page size, and
quantizer identity. It names:

1. `router.json`, another canonical JSON root containing scalar router shape
   and the identities of the three Parquet router objects;
2. `router-codebooks.parquet`, rows `(kind, subspace, codeword, centroid)` with
   non-null UTF-8 `kind`, u16 indices, and non-null fixed-size-list f32
   centroids. For dimension `d`, subspace `s` owns
   `[floor(s*d/16), floor((s+1)*d/16))`; the physical centroid width is
   `ceil(d/16)` and every unused suffix value is canonical positive zero. This
   keeps widths such as 97 and 768 equally representable without a
   dataset-specific dimension assumption;
3. `router-row-codes.parquet`, exactly one non-null fixed-size-list u8 code per
   physical base row, in page-major order;
4. `router-summary-codes.parquet`, rows `(page, block, code)` ordered by
   `(page, block)`, with exactly two blocks per 256-row page;
5. the canonical V85 mutation-directory Arrow IPC file;
6. ordered immutable base and delta runs whose independently decodable page
   slices are Arrow IPC streams with `(id:i64, sequence:u64, state:u8,
   code:fixed-size-list<u8>)`;
7. the global SQ8 low/step authority required to decode every run.

Queries and ground truth are never index artifacts. NumPy `.npy`, pickle,
native Rust layouts, benchmark magic headers, and identity inferred from paths
are forbidden.

## Resident routing

Both routing levels use independent query-blind PQ16 codebooks trained only on
base rows. A query builds two 16-by-256 ADC tables:

1. score the two PQ16 summary codes per page and retain the best fixed number
   of pages by `(distance, page)`;
2. score PQ16 row codes only inside those pages and retain the best shortlist by
   `(distance, physical_row)`;
3. map shortlisted physical rows to pages implicitly with
   `physical_row / page_rows`, then coalesce only adjacent registered page
   slices without crossing run or object boundaries.

The implementation uses fixed blocks and bounded top-k heaps. It must not
allocate or sort all `(distance, row)` pairs. SIMD ADC kernels must match a
scalar control for random values, ties, subnormals, reversed block order, and
tail widths. Query execution uses bounded range-read concurrency and no nested
query-level work-stealing pool.

At 100M rows the resident worksheet is bounded as follows:

| item | maximum bytes |
|---|---:|
| base PQ16 row codes | 1,600,000,000 |
| two PQ16 summaries per 256-row page | 12,500,000 |
| row + summary codebooks and SQ8 scalars | 4,000,000 |
| 1M-entry mutation directory | 96,000,000 |
| resident 100k-row SQ8 delta with IDs/sequences | 84,000,000 |
| sixteen 16-MiB response bodies | 268,435,456 |
| routing, candidate, decode, and result workspaces | 128,000,000 |
| runtime reserve | 536,870,912 |
| **total** | **2,729,806,368** |

Every line is validated from artifact counts and measured allocations before a
100M promotion. Parquet decoder buffers may not be hidden outside the runtime
reserve.

## Snapshot reads and writes

A reader authenticates and pins exactly one generation before routing. It
loads the compact router, global SQ8 scalars, and mutation directory once. A
bounded resident delta is decoded once at generation open; base page bodies
remain remote. Base candidates shadowed by a newer mutation are suppressed
before final admission. Live delta candidates are scored in the same metric;
tombstones are never returned. Results are deduplicated by stable ID and sorted
by `(distance, id)`.

Writers encode query-independent SQ8/PQ16 artifacts, upload immutable objects
create-only, publish generation JSON, and conditionally update `HEAD.json`.
Batch acknowledgement requires the successful conditional head update, not
merely an object PUT. Compaction merges delta runs without rewriting the base,
is byte-independent of input enumeration order, preserves latest sequence,
and may publish only through the same conditional protocol.

## Fail-fast evaluation ladder

1. Tiny Rust fixtures validate schemas, canonical authority, SIMD/scalar
   equality, routing, latest-wins/tombstones, range bounds, and deterministic
   results.
2. A 100k real-data local/Spot screen requires quality, one/ten/one-hundred run
   equivalence, mutation visibility, and compaction equivalence.
3. A 1M ReLAION Spot cell measures all release gates and cold versus reused
   client latency separately.
4. Only after 1M passes, the unchanged revision runs Deep Image 9.99M.
5. Only after the complete memory worksheet passes may a 100M build start.

Each paid cell has one immutable source commit, one attempt, a forced wall cap,
Spot interruption restart semantics, canonical per-query evidence, independent
validation, immediate termination, and explicit scratch cleanup.

## Fail-closed behavior

Reject schema additions or omissions, wrong physical types or nullability,
non-finite vectors/codebooks, dimension or metric drift, unordered or duplicate
codes/pages/runs, out-of-range page or mutation locations, digest/URI/length
drift, sequence ties, missing mutation entries, page bodies outside the pinned
generation, response work above the registered budget, and any aggregate that
does not recompute from raw per-query samples.

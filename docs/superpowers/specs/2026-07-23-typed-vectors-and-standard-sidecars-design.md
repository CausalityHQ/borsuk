# Typed Vectors And Standard Exact-Vector Storage

## Objective

BORSUK must support schema-declared dense, sparse, binary, and late-interaction
vectors without introducing a private durable vector container or duplicating
mutation semantics. Every type must participate in the same immutable-base,
WAL/delta, snapshot-isolation, compaction, cache, and garbage-collection model.

The public vector types are:

- `float32[N]`
- `float16[N]`
- `bfloat16[N]`
- `int8[N]`
- `binary[N bits]`
- sparse `{u32 -> float32}`
- sparse `{u32 -> float16}`
- late interaction `[][N]float32`
- late interaction `[][N]float16`

The element type belongs to the vector-field schema. It is not repeated per
record. Existing f32 records therefore remain the default source shape, while
typed constructors and bindings accept native typed buffers.

## Logical and compute model

Dense f32/f16/bfloat16/i8 fields use the existing ANN engines. Ingest converts
each source vector once to a canonical f32 compute row after validating and
rounding it to the declared field type. Routing, centroids, rotations, PQ
training, shortlist scoring, and exact distance semantics therefore all observe
the same value that is persisted.

Binary fields use packed bits and binary metrics. Hamming/Jaccard kernels score
packed machine words with XOR/AND/OR plus population count; they must not expand
one bit into one f32.

Sparse fields retain sorted `u32` indices and typed non-zero values. Their
inverted index remains the candidate source. Float16 postings are converted in
bounded SIMD-friendly blocks for accumulation; they are never densified.

Late-interaction fields store a variable number of fixed-width token vectors
per entity. The exact score is MaxSim: for every query token, take the maximum
similarity over document tokens, then sum those maxima. The first production
candidate path flattens token rows into a child ANN index, aggregates candidate
entities, and exact-reranks the selected entities with their token matrices.
An exhaustive TokenANN control remains available for quality validation.

## Standard durable representation

The current `BSKVEC01` object is a private row container despite historical
comments calling it Arrow IPC. It is not an acceptable publication or
production format and is replaced without a compatibility reader.

The initial standard exact-vector candidate is an Apache Arrow IPC file:

- one record batch per bounded row block;
- `record_id: Binary`;
- `generation: UInt64`;
- dense vectors as `FixedSizeList<primitive, N>`;
- binary vectors as `FixedSizeBinary<ceil(N/8)>`;
- sparse vectors as `List<Struct<index: UInt32, value: primitive>>`;
- late-interaction vectors as `List<FixedSizeList<primitive, N>>`;
- f16 uses Arrow `Float16`;
- bfloat16 uses `UInt16` physical values with Arrow extension metadata;
- IPC ZSTD buffer compression and uncompressed IPC are both selectable.

Arrow IPC file footers locate record batches, so an object-store reader fetches
the footer once, deduplicates candidate rows by batch, fetches each selected
batch range once, and shares the decoded immutable batch across concurrent
callers. This is standard Arrow IPC, not a BORSUK footer around Arrow buffers.

Two additional physical candidates must be evaluated before promotion:

1. Parquet with bounded row groups, offset/page indexes, projection, and row
   selection.
2. Vortex with bounded layouts, positional object-store reads, and its native
   segment single-flight/cache path.

Vortex vendor/project performance claims are context, not BORSUK evidence.
The three candidates must be built from identical typed rows and compared on
the same local NVMe and S3 workloads.

## WAL and distributed readers

The WAL uses the field's declared physical type in a standard Arrow/Parquet
shape and contains record id, generation, metadata, text/sparse/multivector
payloads, and operation type. It remains immutable and manifest-selected.

Every reader node:

1. opens a committed manifest snapshot;
2. loads the immutable indexed base;
3. overlays the manifest-selected WAL tail exactly;
4. overlays materialized delta segments not covered by the base artifact;
5. suppresses stale generations and tombstoned ids;
6. sees later publishes only after explicit refresh.

No process-local overlay is a correctness source. Process memory and NVMe only
cache checksum-addressed immutable objects. Concurrent requests for the same
IPC batch, Parquet page/range, or Vortex segment are single-flighted within a
node; nodes do not pretend to share RAM.

Bounded compaction rewrites only uncovered delta segments. Explicit full
rebuild replaces the base and retrains derived ANN artifacts. Garbage
collection retains every object referenced by active or retention-protected
manifests.

## Benchmark and promotion gates

Every vector type and physical format reports:

- exactness against a brute-force typed reference;
- recall@10/50 or NDCG@10/MRR as appropriate;
- p50/p95/p99/max and sample standard deviation;
- QPS at fixed caller counts and fixed cost;
- backing requests/bytes and disk-cache reads/bytes;
- process CPU, RSS/VMS, physical disk reads/writes, and cache footprint;
- ingest throughput, time-to-searchable, time-to-fully-indexed, and write
  amplification;
- update/delete freshness and post-compaction performance;
- single-node and multi-reader snapshot/isolation behavior.

No new physical format becomes the default from a single dataset. Promotion
requires repeated fresh-prefix runs on local NVMe and S3, no correctness
regression, bounded memory under concurrency, and a non-dominated
recall/latency/cost point across the publication dataset matrix.

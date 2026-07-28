# Explicit-ID ingest coordination diagnostic

Date: 27 July 2026

Status: internal diagnostic evidence. This is not a publication benchmark and
does not support a comparison with S3 Vectors, Turbopuffer, or another
commercial service.

## Question

The v14 cell-WAL already had eight writer lanes, but a 5,000-row explicit-ID
load remained slow. The diagnostic tested whether the remaining cost came from
the WAL data path or from insert-only ID coordination.

The v14 trace resolved the cause exactly:

- 5,000 caller IDs caused 5,000 conditional per-ID claim PUTs;
- ten 500-row batches added another 80 WAL protocol PUTs;
- the observed total was therefore 5,080 PUTs.

WAL lane sharding prevented collection-wide `CURRENT` contention. It could not
remove a separate one-object-per-ID uniqueness protocol.

## Change under test

Format v15 replaces per-ID insert claims with:

- 16 routing-independent claim shards;
- a fenced transaction state (`Prepared`, `Committing`, `Committed`, or
  `Aborted`);
- a short acquisition gate that permits parallel shard I/O without partial
  multi-lock deadlocks;
- parallel release and independent immutable-run preparation through the
  bounded I/O pool;
- per-handle claim-shard version checkpoints. An unchanged checkpoint avoids a
  full WAL refresh between consecutive batches; any external writer changes a
  shard version and forces the complete duplicate check.

The commit marker remains the reader-visible atomic publication point.

## Fixed environment

- backend: native Amazon S3 in `eu-central-1`;
- compute: `c7g.8xlarge`, Linux ARM64;
- workload: 5,000 generated float32 vectors, 96 dimensions, Euclidean metric;
- explicit caller IDs;
- ten batches of 500 rows;
- WAL record format: Parquet;
- WAL auto-flush disabled during ingest;
- ten post-ingest queries (query count is outside the ingest timing);
- fresh S3 index prefix for every repetition;
- release build; compilation excluded from measured time.

The exact final source archive is:

`s3://borsuk-bench-453182569524-euc1/format-qualification/source/f7f99d66ef06881a1578961fc72772b896b7c58e5258c310f597544f7ce27995.tar.gz`

Its SHA-256 is
`f7f99d66ef06881a1578961fc72772b896b7c58e5258c310f597544f7ce27995`.

Raw final rows are stored under:

`s3://borsuk-bench-453182569524-euc1/diagnostic/batch-id-v15/hardened-f7f99d66/`

The v14 comparison row is:

`s3://borsuk-bench-453182569524-euc1/layout-qualification/wal-results/wal-layout-qualification-20260727-v4/r01/boundary-f32/s3/fixed-parquet/result.csv`

## Results

| Arm | Repetition | Ingest ms | Batch p95 ms | GET | PUT | HEAD |
|---|---:|---:|---:|---:|---:|---:|
| v14 per-ID claims | r01 | 110,953.826 | 11,419.289 | 95 | 5,080 | 65 |
| v15 batch claims | 1 | 7,947.631 | 910.420 | 293 | 450 | 68 |
| v15 batch claims | 2 | 7,756.026 | 903.391 | 293 | 450 | 68 |
| v15 batch claims | 3 | 8,346.546 | 930.596 | 293 | 450 | 68 |

The v15 median is 7,947.631 ms, or approximately 629 vectors/s. Relative to the
single v14 diagnostic row, that is:

- 13.96x lower ingest time;
- 11.29x fewer PUTs;
- 92.84% lower elapsed ingest time.

The three v15 ingest results span 7.43% of their median. The baseline has only
one repetition, so the relative figures are diagnostic effect sizes, not a
confirmatory publication comparison.

One pre-run was excluded before index creation because the restarted worker
defaulted the S3 client to `us-east-1` and received a region redirect. It
produced no CSV result. Subsequent runs pinned both `AWS_REGION` and
`AWS_DEFAULT_REGION` to `eu-central-1`.

## What this establishes

The sharded WAL was not the original bottleneck. Per-ID uniqueness claims were.
Batch-bounded coordination, incremental validation, and parallel independent
I/O remove most of that cost while preserving strict insert-only semantics:

- duplicate IDs inside one batch are rejected;
- two concurrent writers inserting the same ID produce exactly one commit;
- a stale handle observes a changed shard version and refreshes before
  validation;
- expired prepared owners are fenced `Aborted`;
- a writer fenced `Aborted` cannot later commit;
- a `Committing` owner is recovered by completing the existing commit marker.

These properties are covered by the cell-WAL concurrency, corruption, crash,
flush-overlap, and compaction-overlap tests.

## Remaining limitations

This is materially faster, but it is not yet a competitive bulk-ingest result:

- the median batch p95 is still about one second;
- every ten-batch run still performs 450 PUTs and 293 GETs;
- the result covers one writer and a small synthetic corpus;
- it does not measure generated-ID ingest, upsert, delete, mixed modalities, or
  concurrent bulk writers;
- this v15 diagnostic predates the v16 fixed-shard MVCC generation allocator;
  its upsert/delete limitation has since been removed locally, but those paths
  require fresh exact-source S3 measurements before any throughput claim;
- it is not normalized against a commercial API's durability, indexing,
  availability, or visibility contract.

The v16 implementation also publishes every run prepared for the same cell lane
through one conditional lane-head update. The next ingest work is therefore to
remeasure add, generated-ID add, upsert, delete, and concurrent writers from the
exact v16 source. Large-dataset and commercial comparisons remain blocked until
those paths and the production defaults are qualified.

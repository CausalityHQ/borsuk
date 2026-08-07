# Mutation extent standard-format qualification

**Status:** Terminal local codec evidence; foreground format selected.

**Source revision:** `cc518dd4cc54` (the commit containing the benchmark
harness). The exact terminal run completed successfully on 2026-08-07.

## Question and decision

For immutable foreground mutation extents, use an uncompressed Arrow IPC
stream. Do not use Snappy Parquet on the acknowledgement path. Parquet remains
the standard container for materialized scan-oriented tables where its encoding
cost is amortized.

This choice preserves interoperability and materially reduces local codec work.
It does not prove S3 write/read p95, multi-writer scalability, or recall; those
remain separate local structural and frozen AWS gates.

## Method

Command:

```bash
RUSTC_WRAPPER= cargo run --release -p borsuk \
  --example mutation_extent_format_bench
```

The harness builds one identical typed record batch for each paired container:
binary ID, UTF-8 operation, `UInt64` HLC, `FixedSizeBinary(16)` writer,
`FixedSizeBinary(32)` digest, and `FixedSizeList<Float32>` vector. It uses
deterministic high-entropy finite vector values, 768 and 1,536 dimensions, and
1/32/128/512 rows. Three untimed warmups precede 15 timed samples. Both decoders
receive a cheap clone of the same immutable `Bytes`; batch construction and S3
I/O are outside the timed region.

Host: four-core AWS Neoverse-V2 aarch64, Linux 7.0.0-1009-aws, rustc 1.97.1,
release `opt-level=3`. The command was run at the session's normalized low
priority. Raw terminal JSON is preserved in
`mutation-extent-format-cc518dd.json`.

## Terminal results

Times are local codec microseconds. `A` is Arrow IPC stream; `P` is Snappy
Parquet.

| Rows | Dims | A bytes | P bytes | A encode p50/p95 | P encode p50/p95 | A decode p50/p95 | P decode p50/p95 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 768 | 5,192 | 6,427 | 4 / 8 | 50 / 75 | 6 / 7 | 32 / 42 |
| 32 | 768 | 105,736 | 147,429 | 8 / 28 | 938 / 1,005 | 9 / 9 | 143 / 244 |
| 128 | 768 | 417,672 | 606,215 | 21 / 224 | 4,001 / 4,392 | 16 / 31 | 513 / 624 |
| 512 | 768 | 1,665,672 | 2,209,629 | 103 / 727 | 15,068 / 16,695 | 64 / 86 | 1,946 / 2,037 |
| 1 | 1,536 | 8,328 | 10,673 | 5 / 6 | 64 / 71 | 6 / 6 | 35 / 41 |
| 32 | 1,536 | 207,112 | 297,752 | 12 / 23 | 1,563 / 1,738 | 11 / 13 | 249 / 321 |
| 128 | 1,536 | 823,176 | 1,229,489 | 44 / 64 | 6,708 / 7,128 | 32 / 49 | 990 / 1,063 |
| 512 | 1,536 | 3,287,688 | 3,782,512 | 219 / 1,486 | 18,820 / 20,091 | 139 / 169 | 5,690 / 6,161 |

At the largest 768D case, Arrow encoded about 146 times faster at p50,
decoded about 30 times faster, and emitted 24.6% fewer bytes. At the largest
1,536D case, it encoded about 86 times faster, decoded about 41 times faster,
and emitted 13.1% fewer bytes. Even the one-row cases favored Arrow on both
CPU and bytes.

## Boundaries

- High-entropy f32 values deliberately prevent a synthetic compression win;
  real customer distributions and other scalar types still require end-to-end
  qualification.
- The schema represents the dominant dense payload plus mutation identity. It
  does not time multimodal array construction or canonical hashing.
- The result selects a standard container. It does not justify increasing tail
  quotas, relying on cache, or weakening the sub-200 ms p95/recall gates.

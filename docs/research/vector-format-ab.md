# ANN Vector-Buffer Format A/B

Status: **Vortex latency results were invalidated on 24 July 2026; the
unreleased backend and its current-tree harnesses were later removed. Commands
below describe the frozen historical revision only.** The original reader timed `read_all()` followed by
`len(result)`, leaving compressed Vortex arrays unmaterialized, while the Arrow
path returned decoded values. File size, writer-resource, Arrow range-policy,
and physical-type compatibility evidence remains useful. Candidate-take
latency comparisons and the resulting format decision are provisional until
the corrected materialized-Arrow and real compressed-native compute runs
complete.

## Decision

The production ANN bundle remains a standard, uncompressed Arrow IPC File. Its
record batches keep `scan_payload` and typed `exact_vector` values in separate
fixed-width buffers, allowing BORSUK to issue direct byte-range reads without
decoding the other column. The ANN descriptor is standard Parquet.

Vortex 0.79 was an experimental physical backend. The previous candidate-`take`
latency result is invalid and did not justify a production decision by itself.
This is an ANN candidate-`take` decision, not a claim that Arrow is generally
better than Vortex or that Vortex loses to Parquet for durable tables. The
separate [Parquet/Vortex table A/B](table-format-ab.md) found Vortex materially
faster on common table types.

## Method

The executable harness is
[`scripts/benchmark_vector_formats.py`](../../scripts/benchmark_vector_formats.py).
It generated the same seeded Arrow table for Arrow IPC, Parquet with page
indexes, Vortex default, and Vortex compact. Readers were opened once before
timing. Each cell used 3 warmups and 30 measured repetitions for clustered and
scattered candidate sets of 10, 100, and 1,000 rows. Distribution files contain
mean, standard deviation, p50, p95, p99, minimum, and maximum.

The original local pass used PyArrow's generic `get_batch(...).take(...)`
reader. That is useful as an ecosystem baseline but does not represent BORSUK:
it materializes a complete Arrow record batch. The corrected harness resolves
the uncompressed exact-vector buffer ranges at build time and loads that
descriptor before timing. The initial 64 KiB gap policy was then swept on
native S3. The selected production policy merges gaps up to 1 MiB, hard-caps
each physical range at 4 MiB, and performs at most ten physical range reads
concurrently. Those constants now match BORSUK's `Storage::read_ranges` path.
Every CSV records all three parameters in `access_method`; Vortex uses its
native indices scan and records whether its segment cache was enabled.

The resource wrapper sampled the complete process tree every 100 ms. The local
host was an Apple M3 Max with 128 GiB RAM, Python 3.13.13, PyArrow 24.0.0, and
Vortex 0.79.0. macOS exposes CPU and RSS through the portable sampler but not
Linux `/proc` physical-I/O counters; the AWS repetition must supply disk-read
and disk-write graphs.

Two page-locality profiles were tested:

| Profile | Rows × dimensions | Batch rows | Raw vector bytes | Why |
|---|---:|---:|---:|---|
| 128-d | 250,000 × 128 f32 | 32,768 | 122.1 MiB | 16 MiB typed vector batches, representative of SIFT-like dimensions |
| 960-d | 25,000 × 960 f32 | 4,369 | 91.6 MiB | 16 MiB typed vector batches, representative of GIST-like dimensions |

The default Vortex layout is designed for analytical column pruning, zoned
filter pruning, and buffered compressed chunks. BORSUK's operation is
different: ANN has already produced row ordinals, so rerank is a direct
candidate `take`. Vortex layouts are configurable and may justify a future
ANN-specific strategy, but the default and compact strategies must not be
promoted on vendor/project benchmarks alone. See the
[Vortex layout documentation](https://docs.vortex.dev/concepts/layouts) and
[file-format specification](https://docs.vortex.dev/specs/file-format).

## Corrected ranged-reader results

Build and footprint:

| Profile | Format | Build | Bytes/vector | Peak RSS |
|---|---|---:|---:|---:|
| 128-d | Arrow IPC | 152.6 ms | 520.01 | 198.9 MiB |
| 128-d | Vortex default | 4,644.8 ms | 451.40 | 416.9 MiB |
| 128-d | Vortex compact | 2,661.5 ms | 425.32 | 440.1 MiB |
| 960-d | Arrow IPC | 106.2 ms | 3,848.08 | 174.1 MiB |
| 960-d | Vortex default | 3,441.1 ms | 3,387.07 | 335.0 MiB |
| 960-d | Vortex compact | 2,049.4 ms | 3,190.32 | 352.1 MiB |

Representative disk-cached candidate latency:

| Profile | Format | clustered 10 p95 | scattered 100 p95 | scattered 1,000 p95 |
|---|---|---:|---:|---:|
| 128-d | Arrow IPC | 0.180 ms | 4.013 ms | 19.944 ms |
| 128-d | Vortex default | 0.877 ms | 4.128 ms | 8.326 ms |
| 128-d | Vortex compact | 1.174 ms | 4.507 ms | 5.540 ms |
| 960-d | Arrow IPC | 0.221 ms | 3.389 ms | 18.263 ms |
| 960-d | Vortex default | 1.259 ms | 2.884 ms | 5.815 ms |
| 960-d | Vortex compact | 1.514 ms | 3.308 ms | 4.704 ms |

## AWS S3 result and range-policy ablation

The valid baseline AWS run is `20260724T080055Z`; the gap sweep is
`20260724T082747Z`; the bounded-range sweep is `20260724T084146Z`. They ran on
`c7g.8xlarge` in `eu-central-1`. Each S3 cell used 30 measured repetitions,
opened descriptors before timing, used application-uncached data prefixes, and
recorded p50/p95/p99, standard deviation, GETs, bytes, CPU, RAM, disk, and
network counters.

The original 64 KiB Arrow policy generated too many S3 GETs. Increasing only
the gap to 1 or 4 MiB could instead merge almost the whole file and destroy
parallelism. The physical-range cap is necessary; it is not a cosmetic tuning
knob.

Representative uncached-S3 p95 in milliseconds:

| Profile | Format/policy | clustered 10 | scattered 100 | scattered 1,000 |
|---|---|---:|---:|---:|
| 128-d | Arrow, 64 KiB gap, no cap | 45.8 | 272.8 | 1,525.4 |
| 128-d | Arrow, 1 MiB gap, 4 MiB cap | **38.2** | **152.6** | **178.7** |
| 128-d | Vortex default | 41.8 | 171.7 | 195.5 |
| 128-d | Vortex compact | 42.1 | 173.1 | 202.2 |
| 960-d | Arrow, 64 KiB gap, no cap | **28.8** | 256.2 | 1,266.3 |
| 960-d | Arrow, 1 MiB gap, 4 MiB cap | 31.2 | **128.4** | **140.3** |
| 960-d | Vortex default | 33.7 | 177.6 | 206.5 |
| 960-d | Vortex compact | 35.1 | 213.0 | 190.7 |

The 1 MiB/4 MiB policy beats both Vortex layouts on the scattered 100 and
1,000 candidate cells in both dimensional profiles. A 4 MiB gap with an 8 MiB
cap produced unstable tails (128-d scattered-1,000 p95 351.0 ms and maximum
1,197.0 ms), so it was rejected even where one median looked attractive.

Baseline whole-process resource peaks also favored Arrow. For the 128-d S3
case, peak RSS was 384.9 MiB for Arrow, 563.9 MiB for Vortex default, and
576.5 MiB for Vortex compact. For 960-d it was 321.0 MiB, 481.9 MiB, and
466.8 MiB respectively. These peaks include generation and build and therefore
must not be presented as steady-state query RSS.

## Interpretation and final decision

- Arrow dominated the small clustered rerank path and used roughly half the
  peak RAM of Vortex.
- The local 64 KiB baseline made Vortex appear better on the intentionally
  wide scattered point. The S3 ablation identified that as an Arrow request
  planner problem and fixed it without changing the durable format.
- Vortex's smaller files are useful evidence for storage cost, but the saving
  came with 17–32× slower builds and about 2× peak RAM in these profiles.
- Vortex compact traded additional CPU/RAM and variance for more compression,
  consistent with its documented size-over-read-speed objective.
- Vortex performed far fewer GETs but transferred much more data in the
  baseline S3 run. The selected Arrow policy controls both request count and
  overfetch instead of accepting either extreme.

The typed-vector compatibility probe
[`scripts/probe_vector_format_compatibility.py`](../../scripts/probe_vector_format_compatibility.py)
also round-trips the five public physical vector shapes without substituting a
different Arrow type. Arrow IPC completed f32, f16, physical bf16 (`uint16`),
i8, and fixed-size binary. Vortex 0.79 default and compact completed the first
four but both were blocked while writing fixed-size binary. This is a release
blocker for using Vortex as BORSUK's single ANN container, independent of any
f32 latency result.

Arrow IPC therefore remains the single production ANN container. It supports
all five declared physical types, builds much faster, uses less peak RAM, and
wins the selected native-S3 candidate trace after bounded request coalescing.
Vortex can be reconsidered only after the same physical representation supports
all declared vector types and wins a fresh end-to-end BORSUK matrix. All such
comparisons must use identical row traces and recreate files after any writer,
layout, library, or harness change.

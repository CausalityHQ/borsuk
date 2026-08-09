# Parquet Versus Vortex Table-Workload A/B

Status: **the earlier latency results were invalidated; the corrected replay
completed on 24 July 2026, and the final schedule-locked v13 end-to-end product
A/B completed on 27 July 2026 with no Vortex normal-segment promotion. The
unreleased backend and its current-tree harnesses were later removed; commands
below describe the frozen historical revision only.** The
original Vortex reader timed `read_all()` followed by
`len(result)`, which did not materialize the compressed Vortex arrays to the
Arrow values consumed by BORSUK. Parquet did materialize an Arrow table. File
size, writer-resource, and physical-type compatibility results remain useful,
but none of the Vortex latency rows below may be used in a publication or
format decision. The corrected harness times `to_arrow_table()` and separately
labels any future compressed-native execution that performs the real
downstream operation without Arrow materialization.

## Scope

This is the correct Vortex-versus-Parquet comparison for BORSUK's durable
routing, lifecycle, sparse/BM25, and segment tables. It is separate from the
[ANN vector-buffer A/B](vector-format-ab.md), where Arrow IPC is the direct
competitor because ANN has already produced candidate row ordinals.

The executable harness is
[`scripts/benchmark_table_formats.py`](../../scripts/benchmark_table_formats.py).
It writes the same 1,000,000-row, seven-column table to Parquet, Vortex default,
and Vortex compact. The table contains sorted row ids, tenant ids, generations,
term hashes, scores, booleans, and 64-byte binary codes. Each workload has 3
warmups and 30 measured repetitions:

- narrow two-column projection;
- 1%-selective tenant filter;
- 1%-selective sorted row range;
- one-row point lookup;
- full seven-column scan.

The local host was an Apple M3 Max with 128 GiB RAM, Python 3.13.13, PyArrow
24.0.0, and Vortex 0.79.0. The resource sampler covered the complete process
tree at 100 ms intervals.

PyArrow 25.0.0 is intentionally excluded from this campaign: its Linux/aarch64
wheel corrupted a dictionary-encoded `uint32` Parquet column in the AWS
preflight and could segfault in the native writer. The same minimal round trip
passed with 24.0.0 and 23.0.1. The pinned 24.0.0 environment now performs an
exact Parquet/Vortex value round trip before installing S3 metrics or recording
any sample, so a dependency-level format failure cannot become a benchmark
result.

## Local disk-cached result

> Invalid latency evidence: retained only as an audit trail until the corrected
> real-artifact and synthetic reruns replace it.

| Format | Build | Bytes/row | Peak RSS | Projection p95 | Tenant 1% p95 | Range 1% p95 | Point p95 | Full scan p95 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Parquet ZSTD + page index | 436.6 ms | 80.73 | 530.1 MiB | 16.655 ms | 24.712 ms | 11.900 ms | 6.278 ms | 33.751 ms |
| Vortex default | 615.7 ms | 92.59 | 471.8 MiB | 1.120 ms | 2.283 ms | 0.983 ms | 1.873 ms | 14.189 ms |
| Vortex compact | 1,151.5 ms | 69.03 | 462.9 MiB | 0.926 ms | 8.133 ms | 1.006 ms | 1.887 ms | 8.097 ms |

Vortex won all measured table latency p95 cells. Default prioritized filter
latency; compact prioritized footprint and full-scan latency. Each format ran
in a fresh process, so peak RSS is attributable to that format rather than the
sum of sequential writers. Parquet used the most peak RAM but its parallel
writer reached substantially more than one CPU, whereas the measured Vortex
Python writer remained close to one CPU. CPU-time and wall-time therefore both
remain mandatory in AWS comparisons.

## Compatibility gate

The performance matrix used Arrow `Binary` for the same 64 bytes in every
format. A separate exact-schema round-trip gate now covers the 15 physical
table shapes used by BORSUK: nullable integer and floating-point primitives,
boolean, UTF-8, variable binary, fixed-size binary, fixed-size f32/u8 lists,
and variable u32/f32 lists. The executable probe is
[`scripts/probe_table_format_compatibility.py`](../../scripts/probe_table_format_compatibility.py).

The 24 July local result was:

| Format | Exact round trips | Type changes | Blocked |
|---|---:|---:|---:|
| Parquet | 15/15 | 0 | 0 |
| Vortex default 0.79 | 12/15 | UTF-8 → `string_view`; Binary → `binary_view` | `FixedSizeBinary[64]` |
| Vortex compact 0.79 | 12/15 | UTF-8 → `string_view`; Binary → `binary_view` | `FixedSizeBinary[64]` |

The probe does not silently substitute variable binary, f32, or another
physical type. Arrow view types may be a deliberate zero-copy API choice, but
they are recorded as a type change until BORSUK explicitly chooses and tests
that contract. The fixed-size-binary blocker matters for product codes, packed
binary vectors, signatures, and fixed-width bloom data. These findings do not
invalidate the common-type performance result, but they prevent a blanket
“replace every Parquet file with Vortex” decision.

## AWS qualification

The valid AWS run is `20260724T080055Z` on `c7g.8xlarge` with a 500 GiB gp3
volume configured at 3,000 IOPS and 125 MiB/s. The follow-up row-group sweep is
`20260724T082747Z`. Both used native S3 in `eu-central-1`, opened metadata before
timing, disabled the application data cache for the uncached profile, sampled
the complete process tree, and recorded 30 repetitions per workload.

S3 uncached p95 in milliseconds:

| Format/layout | Projection | Tenant 1% | Range 1% | Point | Full scan |
|---|---:|---:|---:|---:|---:|
| Parquet, 8,192 rows/group | 798.8 | 782.3 | 52.6 | 52.3 | 388.2 |
| Parquet, 32,768 rows/group | 226.5 | 239.6 | **45.7** | 68.6 | 372.1 |
| Parquet, 131,072 rows/group | 80.6 | 92.4 | 54.1 | 106.3 | 355.1 |
| Parquet, 524,288 rows/group | 57.4 | 55.8 | 83.7 | 383.3 | 387.5 |
| Vortex default | 50.8 | 80.4 | 76.2 | 75.1 | 274.7 |
| Vortex compact | **46.8** | **49.3** | 64.6 | 58.6 | **179.1** |

The result has two important interpretations:

- Vortex compact is the best balanced implementation for this synthetic,
  analytical scan/filter table and reduces the number of S3 GETs sharply.
- Parquet wins the range and point cells when its row-group size matches the
  access pattern. No single Parquet row-group size wins every synthetic cell,
  and the 8,192-row baseline is not a fair universal Parquet default.

That is useful format research, but the benchmark table is not BORSUK's runtime
layout. BORSUK routes first and then reads bounded immutable segment, posting,
row-metadata, or routing pages. It does not normally issue a full scan of one
million mixed rows. Product row-group sizes are selected per object family and
bounded by the decoded-byte and object-store request budgets.

## Corrected real-segment replay

Run `20260724T112251Z` replayed all 17 normal-segment objects from a real
Fashion-MNIST-784 BORSUK index on a `c7g.8xlarge` in `eu-central-1`. The exact
source archive SHA-256 was
`9eacc662306671fce40bb562eeb028a3ca1f5de355ec1173b5080e1f8b5a3d80`.
Every cell used 3 warmups and 30 measured repetitions and converted the Vortex
result to an Arrow table before stopping the timer. There are 7,650 complete
raw samples: 17 objects × 3 formats × 5 workloads × 30.

The rows below pool the actual raw repetitions with uniform object weighting;
they do not average per-object percentiles. Mean ± standard deviation and tail
latencies are in milliseconds:

| Workload | Format | Mean ± SD | p50 | p95 | p99 |
|---|---|---:|---:|---:|---:|
| projection | Parquet | 699.438 ± 468.518 | 1029.752 | 1068.021 | 1084.444 |
| projection | Vortex default | 19.095 ± 12.980 | 24.941 | 31.733 | 35.678 |
| projection | Vortex compact | 19.420 ± 13.175 | 24.858 | 32.487 | 40.034 |
| point lookup | Parquet | 678.707 ± 456.151 | 1014.030 | 1062.043 | 1086.170 |
| point lookup | Vortex default | 33.766 ± 22.858 | 45.029 | 57.111 | 66.244 |
| point lookup | Vortex compact | 33.647 ± 22.880 | 44.541 | 58.128 | 71.499 |
| range lookup | Parquet | 697.938 ± 466.631 | 1028.142 | 1061.285 | 1082.332 |
| range lookup | Vortex default | 33.120 ± 22.474 | 43.849 | 57.063 | 63.346 |
| range lookup | Vortex compact | 32.725 ± 22.181 | 43.416 | 56.744 | 62.075 |
| filtered scan | Parquet | 697.275 ± 466.053 | 1025.241 | 1063.411 | 1078.932 |
| filtered scan | Vortex default | 32.814 ± 22.089 | 43.425 | 55.484 | 62.104 |
| filtered scan | Vortex compact | 33.291 ± 22.337 | 44.355 | 56.425 | 61.619 |
| full scan | Parquet | 89.316 ± 39.840 | 110.099 | 129.846 | 133.535 |
| full scan | Vortex default | 154.732 ± 107.706 | 215.764 | 253.470 | 284.314 |
| full scan | Vortex compact | 327.343 ± 242.935 | 387.951 | 637.798 | 643.670 |

The immutable segment footprint was 96.841 MiB for Parquet, 253.780 MiB for
Vortex default, and 319.560 MiB for Vortex compact. Vortex default was therefore
2.621× Parquet and compact was 3.300× Parquet on this real schema. The complete
campaign peaked at 1.287 GiB RSS and 1,186.6% of one CPU, wrote 574.7 MiB to
disk, received 22.80 GiB on the host interface, and ran for 2,084.2 seconds.
Those resource values cover the combined conversion/read campaign and are not
misrepresented as per-format query RSS.

The apparently contradictory latency result is an access-path result:
PyArrow's native-S3 Parquet scan pays for many 32-row-group range requests,
while Vortex coalesces selective scans. Parquet wins the full materialization
despite being much smaller. Current BORSUK serving does not use either native
reader that way: it fetches each selected checksummed normal-segment object in
one known-size read before decoding. Therefore the selective replay win is
useful evidence for a future range-native reader, but it cannot select the
current product default. The checked-in aggregate and resource evidence is in
[`aws-vortex-segment-replay-2026-07-24.csv`](../web/assets/benchmarks/aws-vortex-segment-replay-2026-07-24.csv)
and
[`aws-vortex-segment-replay-resources-2026-07-24.csv`](../web/assets/benchmarks/aws-vortex-segment-replay-resources-2026-07-24.csv).
The corresponding
[format/latency distribution chart](../web/assets/charts/aws-vortex-segment-replay-2026-07-24.svg)
and
[campaign CPU/RAM/disk/network chart](../web/assets/charts/aws-vortex-segment-replay-2026-07-24-experiment.svg)
are generated directly from those completed artifacts.

## Current product decision and next gate

The earlier “retain Parquet” decision is withdrawn because its Vortex latency
rows did not materialize the compressed arrays to the Arrow values consumed by
BORSUK, while the Parquet rows did. Those rows cannot select a product default.

The workspace contains a Vortex 0.81 experimental backend for normal segments
and compact WAL tables. Parquet remains the normal-segment default after the
fresh v13 end-to-end gate. That gate required:

1. identical logical schemas and downstream Arrow materialization;
2. fresh Parquet and Vortex indexes rather than read-time format conversion;
3. p50/p95/p99, mean and standard deviation plus CPU, peak RSS, disk, network,
   object requests, and bytes;
4. selective and full segment reads from local NVMe and S3;
5. no silent physical-type conversion. Vortex remains blocked for
   `FixedSizeBinary` until the same representation is supported.

The final campaign
`storage-layout-schedule-locked-20260727-v13` ran 100 cases and 10,000 paired
query samples across Fashion-MNIST, GloVe, local disk, and S3. All four global
candidate rows were `no-promotion`: worst p95 ratios were 1.283–1.292 and the
family-wise confidence upper bounds were 1.300–1.319. Parquet therefore remains
the normal-segment product default. A favorable isolated subcase, analytical
microbenchmark, or native-reader result cannot override the frozen
cross-dataset/cross-backend rule. Compact Vortex for WAL runs is evaluated
separately because it has a different schema and access pattern.

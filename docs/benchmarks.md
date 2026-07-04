# Benchmarks

BORSUK has two benchmark layers:

- Criterion functions in `crates/borsuk/benches/local_search.rs` for local
  repeatable timing.
- `crates/borsuk/examples/benchmark_report.rs` for developer-facing tables,
  CSV artifacts, write/compaction lifecycle timing, parallel-query pressure,
  RSS sampling, dataset-size scale sweeps, routing-overfetch sweeps, and web
  charts.

The hosted docs page renders the CSV outputs interactively.

`benchmark_report` measures the read-optimized query path. Each dataset is bulk
inserted through the append-only L0 path, explicitly compacted into
vector-local L1 leaves, and then queried. Compaction time is intentionally not
included in query latency; the report writes `lifecycle.csv` so ingest and
compaction throughput stay visible as their own gate.

## Run

```bash
cargo bench --locked -p borsuk
cargo test --locked -p borsuk --test performance_smoke
cargo run --locked --release -p borsuk --example benchmark_report -- \
  --queries 10 \
  --parallelism 1,2,4,8 \
  --artifacts-dir /tmp/borsuk-bench
```

The smoke test and Criterion benchmark assertions use tie-aware recall so
duplicate/equal-distance vectors do not fail only because their ids differ.
They enforce `0.95` tie-aware recall@10 for the high-recall modes `pq-scan`,
`vamana-pq`, and `hybrid`; strict id recall stays a diagnostic for duplicate
or tied vectors.

To generate dataset-size scaling artifacts for the web charts, pass a
comma-separated synthetic record-count sweep. Dataset names are suffixed with
`-n<count>` so the interactive selector can distinguish each size:

```bash
cargo run --locked --release -p borsuk --example benchmark_report -- \
  --synthetic-records-list 10000,100000 \
  --queries 10 \
  --parallelism 1,2,4,8 \
  --artifacts-dir /tmp/borsuk-bench-scale
```

That command writes `scale.csv` and `routing-overfetch.csv` in addition to
`sequential.csv`, `parallel.csv`, and `lifecycle.csv`. The scale artifact
normalizes names such as `synthetic-uniform-n100000` into a
`family=synthetic-uniform` column while preserving `records` as a numeric x-axis
for web charts.

Large-scale runs are intentionally outside default CI. Run the ignored release
gate explicitly when validating million-vector behavior:

```bash
BORSUK_LARGE_SCALE_OUTPUT=/tmp/borsuk-bench/large-scale.csv \
cargo test --locked --release -p borsuk --test large_scale \
  million_vector_local_search_scale_gate -- --ignored --nocapture
```

The large-scale gate defaults to 1,000,000 vectors, 16 dimensions,
`segment_max_vectors=128`, `routing_page_overfetch=8`, and batched ingest. Override with
`BORSUK_LARGE_SCALE_RECORDS`, `BORSUK_LARGE_SCALE_DIMENSIONS`,
`BORSUK_LARGE_SCALE_SEGMENT_MAX_VECTORS`, and
`BORSUK_LARGE_SCALE_BATCH_RECORDS`. Query routing lookahead can be overridden
with `BORSUK_LARGE_SCALE_ROUTING_PAGE_OVERFETCH`. When
`BORSUK_LARGE_SCALE_OUTPUT` is set,
the gate writes one CSV row per high-recall mode so the release artifact can be
copied to `docs/web/assets/benchmarks/large-scale.csv`. The artifact includes
both tie-aware recall@10 and strict id recall@10, matching the smaller
benchmark CSVs.

To include the real-data smoke dataset used by the web docs:

```bash
curl -L https://raw.githubusercontent.com/scikit-learn/scikit-learn/main/sklearn/datasets/data/digits.csv.gz \
  -o /tmp/digits.csv.gz
gzip -dc /tmp/digits.csv.gz > /tmp/digits.csv
cargo run --locked --release -p borsuk --example benchmark_report -- \
  --queries 10 \
  --parallelism 1,2,4,8 \
  --csv /tmp/digits.csv \
  --csv-name sklearn-digits \
  --csv-dimensions 64 \
  --artifacts-dir /tmp/borsuk-bench
```

The scikit-learn digits dataset is documented by scikit-learn as 1,797 rows
with 64 features, each row representing one 8x8 handwritten digit image:
https://scikit-learn.org/stable/modules/generated/sklearn.datasets.load_digits.html

## What Is Measured

Sequential rows:

- tie-aware recall@10 against exact mode, where any hit at or inside the exact
  kth distance counts even when duplicate vectors have different ids;
- strict id recall@10 as a diagnostic for duplicate-vector and tie behavior;
- termination-reason counts from `SearchReport`, so complete, exact-pruned,
  and budget-limited searches are visible in the artifact;
- dataset records, dimensions, segment size, routing overfetch, and
  approximate query budgets;
- p50/p95 latency;
- average segment bytes read;
- average graph bytes read;
- average resident metadata bytes reported by `SearchReport`;
- average segments searched, rows considered, rows exact-scored;
- object-cache hits and misses.

Scale rows:

- dataset family, concrete dataset name, mode, record count, dimensions,
  routing overfetch, and approximate budgets;
- tie-aware recall@10 and strict id recall@10 for each size;
- termination-reason counts for each dataset/mode/size row;
- p50/p95 latency, query bytes, graph bytes, resident metadata, segments
  searched, rows considered, exact-scored rows, and object-cache hits/misses
  as record count changes.

Routing-overfetch rows:

- high-recall modes only: `pq-scan`, `vamana-pq`, and `hybrid`;
- `routing_page_overfetch` values `1, 2, 4, 8, 16, 32`;
- tie-aware recall@10, strict id recall@10, p95 latency, query bytes, graph
  bytes, routing page/index reads, resident metadata, exact-scored rows, and
  cache misses for each value.

`routing_page_overfetch` is a lookahead ceiling for ambiguous routing pages,
not a forced multiplier. If persisted vector bounds are decisive, larger
values can leave routing page reads unchanged. If bounds are tied or close,
larger values allow the query to decode more cheap routing metadata before
spending the expensive segment and graph payload budgets.

Lifecycle rows:

- ingest wall time and vectors/sec for the append-only L0 write path;
- compaction wall time and rewritten vectors/sec;
- pre/post segment counts, source segments read, output segments written, and
  records rewritten;
- compaction bytes read/written and byte throughput;
- routing page/index read/write counts and old graph payload reads during
  compaction.

Parallel rows:

- the same per-query recall, dataset size, latency, bytes, resident metadata,
  cache hits/misses, and termination-reason counts;
- worker count 1, 2, 4, and 8;
- total query throughput;
- process RSS before, sampled peak, after, and peak delta.

RSS is sampled from the benchmark process. `resident_bytes_estimate` is the
BORSUK metadata estimate. They answer different questions: RSS shows observed
process pressure during a parallel batch, while resident bytes shows the index
metadata that BORSUK budgets.

Large-scale rows:

- record count, dimensions, segment size, `max_segments`,
  `routing_page_overfetch`, and `max_candidates_per_segment`;
- pre/post segment counts, ingest time, compaction time, exact reference time,
  and compaction bytes read/written;
- mode, tie-aware recall@10, termination reason, approximate query time,
  segment payload count, bytes read, graph bytes read, resident bytes, rows
  considered, rows scored, and graph candidates.

## Current Local Results

Measured on Apple M3 Max, 16 cores, 128 GB RAM, Darwin 25.2.0, Rust 1.95.0.
Synthetic datasets use 10,000 vectors, 64 dimensions, `segment_max_vectors=256`,
`max_segments=8`, `routing_page_overfetch=8`, and
`max_candidates_per_segment=64`. They are compacted into vector-local leaves
before query timing.

Lifecycle timing is reported separately from query latency:

| Dataset | Records | Ingest vectors/sec | Compaction vectors/sec | Ingest ms | Compaction ms | Segments read/written | Compaction bytes read/written |
|---|---:|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | 10,000 | 13,855 | 4,497 | 721.7 | 2223.7 | 40/40 | 1.19 MB / 564.7 KB |
| synthetic-clustered | 10,000 | 14,244 | 3,128 | 702.1 | 3197.0 | 40/40 | 682.8 KB / 429.6 KB |
| synthetic-adversarial | 10,000 | 15,427 | 4,536 | 648.2 | 2204.7 | 40/40 | 351.1 KB / 296.2 KB |

| Dataset | Records | Mode | Tie Recall@10 | Id Recall@10 | p95 ms | Bytes/query | Graph bytes/query | Resident bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | 10,000 | exact | 1.00 | 1.00 | 2.6 | 77.8 KB | 0 B | 267 B |
| synthetic-uniform | 10,000 | flat-scan | 0.92 | 0.92 | 8.3 | 173.4 KB | 0 B | 267 B |
| synthetic-uniform | 10,000 | sq-scan | 0.92 | 0.92 | 8.3 | 173.4 KB | 0 B | 267 B |
| synthetic-uniform | 10,000 | pq-scan | 1.00 | 1.00 | 9.8 | 173.4 KB | 0 B | 267 B |
| synthetic-uniform | 10,000 | graph | 0.88 | 0.88 | 20.1 | 173.4 KB | 48.4 KB | 267 B |
| synthetic-uniform | 10,000 | vamana-pq | 1.00 | 1.00 | 20.1 | 173.4 KB | 48.4 KB | 267 B |
| synthetic-uniform | 10,000 | hybrid | 1.00 | 1.00 | 18.5 | 173.4 KB | 48.4 KB | 267 B |
| synthetic-clustered | 10,000 | exact | 1.00 | 1.00 | 14.1 | 117.1 KB | 0 B | 267 B |
| synthetic-clustered | 10,000 | flat-scan | 0.94 | 0.92 | 8.6 | 148.3 KB | 0 B | 267 B |
| synthetic-clustered | 10,000 | sq-scan | 0.94 | 0.92 | 8.2 | 148.3 KB | 0 B | 267 B |
| synthetic-clustered | 10,000 | pq-scan | 1.00 | 1.00 | 10.0 | 148.3 KB | 0 B | 267 B |
| synthetic-clustered | 10,000 | graph | 1.00 | 0.97 | 25.0 | 148.3 KB | 51.8 KB | 267 B |
| synthetic-clustered | 10,000 | vamana-pq | 1.00 | 1.00 | 20.8 | 148.3 KB | 51.8 KB | 267 B |
| synthetic-clustered | 10,000 | hybrid | 1.00 | 1.00 | 21.3 | 148.3 KB | 51.8 KB | 267 B |
| synthetic-adversarial | 10,000 | exact | 1.00 | 1.00 | 3.6 | 57.9 KB | 0 B | 267 B |
| synthetic-adversarial | 10,000 | flat-scan | 1.00 | 1.00 | 7.6 | 101.9 KB | 0 B | 267 B |
| synthetic-adversarial | 10,000 | sq-scan | 1.00 | 1.00 | 8.0 | 101.9 KB | 0 B | 267 B |
| synthetic-adversarial | 10,000 | pq-scan | 1.00 | 1.00 | 9.1 | 101.9 KB | 0 B | 267 B |
| synthetic-adversarial | 10,000 | graph | 1.00 | 1.00 | 16.8 | 101.9 KB | 33.1 KB | 267 B |
| synthetic-adversarial | 10,000 | vamana-pq | 1.00 | 1.00 | 18.8 | 101.9 KB | 33.1 KB | 267 B |
| synthetic-adversarial | 10,000 | hybrid | 1.00 | 1.00 | 20.6 | 101.9 KB | 33.1 KB | 267 B |
| sklearn-digits | 1,797 | exact | 1.00 | 1.00 | 9.5 | 223.5 KB | 0 B | 267 B |
| sklearn-digits | 1,797 | flat-scan | 0.45 | 0.45 | 8.8 | 227.9 KB | 0 B | 267 B |
| sklearn-digits | 1,797 | sq-scan | 0.45 | 0.45 | 8.8 | 227.9 KB | 0 B | 267 B |
| sklearn-digits | 1,797 | pq-scan | 1.00 | 1.00 | 11.7 | 227.9 KB | 0 B | 267 B |
| sklearn-digits | 1,797 | graph | 0.97 | 0.97 | 26.9 | 227.9 KB | 88.1 KB | 267 B |
| sklearn-digits | 1,797 | vamana-pq | 1.00 | 1.00 | 28.3 | 227.9 KB | 88.1 KB | 267 B |
| sklearn-digits | 1,797 | hybrid | 1.00 | 1.00 | 27.0 | 227.9 KB | 88.1 KB | 267 B |

The synthetic-uniform and synthetic-clustered generators intentionally include
duplicate/tied nearest vectors. Tie-aware recall avoids treating a different id
with the same exact kth-distance as a miss. Id recall remains in the artifacts
so duplicate-id effects stay visible.

These checked-in numbers must be regenerated whenever routing, compaction,
leaf-mode, storage, or cache behavior changes. Low recall on synthetic-uniform
after compaction is a regression because query vectors are present in the
dataset and should route to their vector-local leaf blobs. The benchmark report
fails if `pq-scan`, `vamana-pq`, or `hybrid` fall below `0.95` tie-aware
recall@10; `flat-scan`, `sq-scan`, and plain `graph` stay visible as
diagnostic/baseline modes but are not the high-recall release gate.
Checked-in scale-sweep artifacts cover 10k and 100k synthetic vectors for the
three synthetic families. Use the ignored large-scale gate as the separate
correctness and I/O check for the million-vector case.

The checked-in `scale.csv` now includes 10k and 100k synthetic sweeps generated
with `--synthetic-records-list 10000,100000`. At 100k vectors, all high-recall
modes reached `1.000` tie-aware recall@10 and strict id recall@10 on the
synthetic-uniform, synthetic-clustered, and synthetic-adversarial datasets:

| Dataset | Records | Mode | Tie Recall@10 | p95 ms | Bytes/query | Graph bytes/query | Resident bytes |
|---|---:|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | 100,000 | pq-scan | 1.00 | 9.9 | 219.7 KB | 0 B | 267 B |
| synthetic-uniform | 100,000 | vamana-pq | 1.00 | 18.2 | 219.7 KB | 33.9 KB | 267 B |
| synthetic-uniform | 100,000 | hybrid | 1.00 | 18.6 | 219.7 KB | 33.9 KB | 267 B |
| synthetic-clustered | 100,000 | pq-scan | 1.00 | 12.3 | 255.5 KB | 0 B | 267 B |
| synthetic-clustered | 100,000 | vamana-pq | 1.00 | 19.9 | 255.5 KB | 34.9 KB | 267 B |
| synthetic-clustered | 100,000 | hybrid | 1.00 | 19.6 | 255.5 KB | 34.9 KB | 267 B |
| synthetic-adversarial | 100,000 | pq-scan | 1.00 | 10.1 | 154.5 KB | 0 B | 267 B |
| synthetic-adversarial | 100,000 | vamana-pq | 1.00 | 17.9 | 154.5 KB | 32.1 KB | 267 B |
| synthetic-adversarial | 100,000 | hybrid | 1.00 | 18.4 | 154.5 KB | 32.1 KB | 267 B |

The checked-in `routing-overfetch.csv` uses 100k synthetic rows and sweeps
`routing_page_overfetch=1,2,4,8,16,32` for the high-recall modes. On this run,
all required rows stayed at `1.000000` tie-aware recall@10. Most datasets kept
average routing page reads at `2.0` because the page bounds were decisive; the
adversarial 32x rows rose to about `2.2` routing pages/query, showing that the
option allows extra metadata reads only when routing bounds are close enough to
matter.

The latest million-vector gate was run with 1,000,000 synthetic vectors,
16 dimensions, `segment_max_vectors=128`, `max_segments=512`,
`routing_page_overfetch=8`, and `max_candidates_per_segment=128`. After
compaction into 7,813 vector-local segments, `pq-scan`, `vamana-pq`, and
`hybrid` all reached `1.000`
tie-aware recall@10 and strict id recall@10 while reading at most 512 segment
payloads. `pq-scan` read 14.46 MB/query and no graph bytes; graph-backed modes
read the same segment bytes plus 4.42 MB/query of graph bytes. The checked-in
`large-scale.csv` run ingested in 32.2s, compacted in 54.4s, and ran the exact
recall reference in 1.01s on the same machine. Compaction read 161.77 MB and
wrote 157.21 MB. The fix that made this pass is metadata overfetch: search
reads extra compact routing pages ranked by persisted vector bounds, then keeps
the expensive segment/graph payload budget strict.

## Parallel Graph Pressure

The table below shows the graph-heavy modes at 8 workers. The web page exposes
all modes and every parallelism point.

| Dataset | Records | Mode | Workers | QPS | p95 ms | RSS peak delta | Graph bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | 10,000 | graph | 8 | 417.9 | 21.7 | 848.0 KB | 48.4 KB |
| synthetic-uniform | 10,000 | vamana-pq | 8 | 414.7 | 22.9 | 400.0 KB | 48.4 KB |
| synthetic-uniform | 10,000 | hybrid | 8 | 377.0 | 22.6 | 368.0 KB | 48.4 KB |
| synthetic-clustered | 10,000 | graph | 8 | 394.5 | 24.9 | 448.0 KB | 51.8 KB |
| synthetic-clustered | 10,000 | vamana-pq | 8 | 374.3 | 24.3 | 368.0 KB | 51.8 KB |
| synthetic-clustered | 10,000 | hybrid | 8 | 349.1 | 25.0 | 352.0 KB | 51.8 KB |
| synthetic-adversarial | 10,000 | graph | 8 | 481.5 | 19.6 | 352.0 KB | 33.1 KB |
| synthetic-adversarial | 10,000 | vamana-pq | 8 | 463.3 | 20.5 | 272.0 KB | 33.1 KB |
| synthetic-adversarial | 10,000 | hybrid | 8 | 457.2 | 20.6 | 288.0 KB | 33.1 KB |
| sklearn-digits | 1,797 | graph | 8 | 261.2 | 31.9 | 400.0 KB | 88.1 KB |
| sklearn-digits | 1,797 | vamana-pq | 8 | 266.2 | 32.3 | 272.0 KB | 88.1 KB |
| sklearn-digits | 1,797 | hybrid | 8 | 264.4 | 32.4 | 272.0 KB | 88.1 KB |

## Criterion Entries

- `local_exact_search_10k_x_64`
- `local_approx_report_10k_x_64`
- `local_flat_scan_approx_report_10k_x_64`
- `local_sq_scan_approx_report_10k_x_64`
- `local_pq_scan_approx_report_10k_x_64`
- `local_vamana_pq_approx_report_10k_x_64`
- `local_hybrid_approx_report_10k_x_64`
- `local_warm_cache_approx_report_10k_x_64`
- `local_clustered_approx_report_10k_x_64`
- `local_adversarial_approx_report_10k_x_64`

The smoke test keeps exact search plus every implemented approximate leaf mode
under the local one-second target and validates recall, byte counters, segment
budgets, graph reads, and resident metadata counters.

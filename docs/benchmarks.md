# Benchmarks

BORSUK has two benchmark layers:

- Criterion functions in `crates/borsuk/benches/local_search.rs` for local
  repeatable timing.
- `crates/borsuk/examples/benchmark_report.rs` for developer-facing tables,
  CSV artifacts, write/compaction lifecycle timing, parallel-query pressure,
  RSS sampling, dataset-size scale sweeps, and web charts.

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

The smoke test uses tie-aware recall so duplicate/equal-distance vectors do
not fail only because their ids differ. It also enforces `0.95` tie-aware
recall@10 for the high-recall modes `pq-scan`, `vamana-pq`, and `hybrid`.

To generate dataset-size scaling artifacts for the web charts, pass a
comma-separated synthetic record-count sweep. Dataset names are suffixed with
`-n<count>` so the interactive selector can distinguish each size:

```bash
cargo run --locked --release -p borsuk --example benchmark_report -- \
  --synthetic-records-list 10000,100000,1000000 \
  --queries 10 \
  --parallelism 1,2,4,8 \
  --artifacts-dir /tmp/borsuk-bench-scale
```

That command writes `scale.csv` in addition to `sequential.csv`,
`parallel.csv`, and `lifecycle.csv`. The scale artifact normalizes names such
as `synthetic-uniform-n100000` into a `family=synthetic-uniform` column while
preserving `records` as a numeric x-axis for web charts.

Large-scale runs are intentionally outside default CI. Run the ignored release
gate explicitly when validating million-vector behavior:

```bash
cargo test --locked --release -p borsuk --test large_scale \
  million_vector_local_search_scale_gate -- --ignored --nocapture
```

The large-scale gate defaults to 1,000,000 vectors, 16 dimensions,
`segment_max_vectors=128`, and batched ingest. Override with
`BORSUK_LARGE_SCALE_RECORDS`, `BORSUK_LARGE_SCALE_DIMENSIONS`,
`BORSUK_LARGE_SCALE_SEGMENT_MAX_VECTORS`, and
`BORSUK_LARGE_SCALE_BATCH_RECORDS`.

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
- dataset records, dimensions, segment size, and approximate query budgets;
- p50/p95 latency;
- average segment bytes read;
- average graph bytes read;
- average resident metadata bytes reported by `SearchReport`;
- average segments searched, rows considered, rows exact-scored;
- object-cache hits and misses.

Scale rows:

- dataset family, concrete dataset name, mode, record count, dimensions, and
  approximate budgets;
- tie-aware recall@10 and strict id recall@10 for each size;
- p50/p95 latency, query bytes, graph bytes, resident metadata, segments
  searched, rows considered, and exact-scored rows as record count changes.

Lifecycle rows:

- ingest wall time and vectors/sec for the append-only L0 write path;
- compaction wall time and rewritten vectors/sec;
- pre/post segment counts, source segments read, output segments written, and
  records rewritten;
- compaction bytes read/written and byte throughput;
- routing page/index read/write counts and old graph payload reads during
  compaction.

Parallel rows:

- the same per-query recall, dataset size, latency, bytes, and resident
  metadata;
- worker count 1, 2, 4, and 8;
- total query throughput;
- process RSS before, sampled peak, after, and peak delta.

RSS is sampled from the benchmark process. `resident_bytes_estimate` is the
BORSUK metadata estimate. They answer different questions: RSS shows observed
process pressure during a parallel batch, while resident bytes shows the index
metadata that BORSUK budgets.

## Current Local Results

Measured on Apple M3 Max, 16 cores, 128 GB RAM, Darwin 25.2.0, Rust 1.95.0.
Synthetic datasets use 10,000 vectors, 64 dimensions, `segment_max_vectors=256`,
`max_segments=8`, and `max_candidates_per_segment=64`. They are compacted into
vector-local leaves before query timing.

Lifecycle timing is reported separately from query latency:

| Dataset | Records | Ingest vectors/sec | Compaction vectors/sec | Ingest ms | Compaction ms | Segments read/written | Compaction bytes read/written |
|---|---:|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | 10,000 | 27,122 | 9,482 | 368.7 | 1054.6 | 40/40 | 1.24 MB / 578.2 KB |
| synthetic-clustered | 10,000 | 26,915 | 6,916 | 371.5 | 1445.9 | 40/40 | 695.9 KB / 439.9 KB |
| synthetic-adversarial | 10,000 | 29,232 | 9,646 | 342.1 | 1036.7 | 40/40 | 357.0 KB / 303.3 KB |

| Dataset | Records | Mode | Tie Recall@10 | Id Recall@10 | p95 ms | Bytes/query | Graph bytes/query | Resident bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | 10,000 | exact | 1.00 | 1.00 | 16.0 | 65.5 KB | 0 B | 61.3 KB |
| synthetic-uniform | 10,000 | flat-scan | 0.92 | 0.92 | 17.4 | 115.5 KB | 0 B | 61.3 KB |
| synthetic-uniform | 10,000 | sq-scan | 0.92 | 0.92 | 17.9 | 115.5 KB | 0 B | 61.3 KB |
| synthetic-uniform | 10,000 | pq-scan | 1.00 | 1.00 | 26.7 | 115.5 KB | 0 B | 61.3 KB |
| synthetic-uniform | 10,000 | graph | 0.88 | 0.88 | 35.9 | 115.5 KB | 49.6 KB | 61.3 KB |
| synthetic-uniform | 10,000 | vamana-pq | 1.00 | 1.00 | 26.3 | 115.5 KB | 49.6 KB | 61.3 KB |
| synthetic-uniform | 10,000 | hybrid | 1.00 | 1.00 | 26.4 | 115.5 KB | 49.6 KB | 61.3 KB |
| synthetic-clustered | 10,000 | exact | 1.00 | 1.00 | 15.5 | 299.7 KB | 0 B | 61.3 KB |
| synthetic-clustered | 10,000 | flat-scan | 0.86 | 0.85 | 4.9 | 88.6 KB | 0 B | 61.3 KB |
| synthetic-clustered | 10,000 | sq-scan | 0.86 | 0.85 | 4.2 | 88.6 KB | 0 B | 61.3 KB |
| synthetic-clustered | 10,000 | pq-scan | 0.97 | 0.92 | 6.0 | 88.6 KB | 0 B | 61.3 KB |
| synthetic-clustered | 10,000 | graph | 0.97 | 0.92 | 13.3 | 88.6 KB | 53.0 KB | 61.3 KB |
| synthetic-clustered | 10,000 | vamana-pq | 0.97 | 0.92 | 11.5 | 88.6 KB | 53.0 KB | 61.3 KB |
| synthetic-clustered | 10,000 | hybrid | 0.97 | 0.92 | 11.1 | 88.6 KB | 53.0 KB | 61.3 KB |
| synthetic-adversarial | 10,000 | exact | 1.00 | 1.00 | 2.5 | 36.8 KB | 0 B | 61.3 KB |
| synthetic-adversarial | 10,000 | flat-scan | 1.00 | 1.00 | 3.5 | 62.6 KB | 0 B | 61.3 KB |
| synthetic-adversarial | 10,000 | sq-scan | 1.00 | 1.00 | 3.9 | 62.6 KB | 0 B | 61.3 KB |
| synthetic-adversarial | 10,000 | pq-scan | 1.00 | 1.00 | 4.3 | 62.6 KB | 0 B | 61.3 KB |
| synthetic-adversarial | 10,000 | graph | 1.00 | 1.00 | 8.2 | 62.6 KB | 34.0 KB | 61.3 KB |
| synthetic-adversarial | 10,000 | vamana-pq | 1.00 | 1.00 | 8.6 | 62.6 KB | 34.0 KB | 61.3 KB |
| synthetic-adversarial | 10,000 | hybrid | 1.00 | 1.00 | 9.0 | 62.6 KB | 34.0 KB | 61.3 KB |
| sklearn-digits | 1,797 | exact | 1.00 | 1.00 | 4.2 | 207.7 KB | 0 B | 12.5 KB |
| sklearn-digits | 1,797 | flat-scan | 0.45 | 0.45 | 4.1 | 208.7 KB | 0 B | 12.5 KB |
| sklearn-digits | 1,797 | sq-scan | 0.45 | 0.45 | 3.9 | 208.7 KB | 0 B | 12.5 KB |
| sklearn-digits | 1,797 | pq-scan | 1.00 | 1.00 | 4.8 | 208.7 KB | 0 B | 12.5 KB |
| sklearn-digits | 1,797 | graph | 0.97 | 0.97 | 13.8 | 208.7 KB | 90.2 KB | 12.5 KB |
| sklearn-digits | 1,797 | vamana-pq | 1.00 | 1.00 | 13.7 | 208.7 KB | 90.2 KB | 12.5 KB |
| sklearn-digits | 1,797 | hybrid | 1.00 | 1.00 | 14.9 | 208.7 KB | 90.2 KB | 12.5 KB |

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
Scale-sweep artifacts should include at least 10k, 100k, and 1M synthetic
vectors before a performance-sensitive release; use the ignored large-scale
gate as the separate correctness check for the million-vector case.

The latest million-vector gate was run with 1,000,000 synthetic vectors,
16 dimensions, `segment_max_vectors=128`, `max_segments=512`, and
`max_candidates_per_segment=128`. After compaction into 7,813 vector-local
segments, `pq-scan`, `vamana-pq`, and `hybrid` all reached `1.000`
tie-aware recall@10 while reading at most 512 segment payloads. `pq-scan`
read 14.46 MB/query and no graph bytes; graph-backed modes read the same
segment bytes plus 4.42 MB/query of graph bytes. Ingest took 142.0s,
compaction took 93.2s, and exact recall reference search took 6.89s on the
same machine. The fix that made this pass is metadata overfetch: search reads
extra compact routing pages ranked by persisted vector bounds, then keeps the
expensive segment/graph payload budget strict.

## Parallel Graph Pressure

The table below shows the graph-heavy modes at 8 workers. The web page exposes
all modes and every parallelism point.

| Dataset | Records | Mode | Workers | QPS | p95 ms | RSS peak delta | Graph bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | 10,000 | graph | 8 | 255.4 | 39.2 | 1.02 MB | 49.6 KB |
| synthetic-uniform | 10,000 | vamana-pq | 8 | 306.7 | 28.6 | 475.1 KB | 49.6 KB |
| synthetic-uniform | 10,000 | hybrid | 8 | 377.6 | 23.9 | 294.9 KB | 49.6 KB |
| synthetic-clustered | 10,000 | graph | 8 | 243.9 | 35.4 | 442.4 KB | 53.0 KB |
| synthetic-clustered | 10,000 | vamana-pq | 8 | 275.9 | 36.7 | 491.5 KB | 53.0 KB |
| synthetic-clustered | 10,000 | hybrid | 8 | 325.1 | 29.3 | 294.9 KB | 53.0 KB |
| synthetic-adversarial | 10,000 | graph | 8 | 383.3 | 23.1 | 294.9 KB | 34.0 KB |
| synthetic-adversarial | 10,000 | vamana-pq | 8 | 455.6 | 20.6 | 278.5 KB | 34.0 KB |
| synthetic-adversarial | 10,000 | hybrid | 8 | 463.5 | 21.0 | 311.3 KB | 34.0 KB |
| sklearn-digits | 1,797 | graph | 8 | 274.4 | 32.8 | 311.3 KB | 90.2 KB |
| sklearn-digits | 1,797 | vamana-pq | 8 | 260.1 | 39.2 | 327.7 KB | 90.2 KB |
| sklearn-digits | 1,797 | hybrid | 8 | 270.4 | 39.0 | 426.0 KB | 90.2 KB |

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

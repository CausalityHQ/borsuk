# Benchmarks

BORSUK has two benchmark layers:

- Criterion functions in `crates/borsuk/benches/local_search.rs` for local
  repeatable timing.
- `crates/borsuk/examples/benchmark_report.rs` for developer-facing tables,
  CSV artifacts, parallel-query pressure, RSS sampling, and web charts.

The hosted docs page renders the CSV outputs interactively.

`benchmark_report` measures the read-optimized query path. Each dataset is bulk
inserted through the append-only L0 path, explicitly compacted into
vector-local L1 leaves, and then queried. Compaction time is intentionally not
included in query latency; write/compaction throughput should be tracked as a
separate gate.

## Run

```bash
cargo bench --locked -p borsuk
cargo test --locked -p borsuk --test performance_smoke
cargo run --locked --release -p borsuk --example benchmark_report -- \
  --queries 10 \
  --parallelism 1,2,4,8 \
  --artifacts-dir /tmp/borsuk-bench
```

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

| Dataset | Records | Mode | Tie Recall@10 | Id Recall@10 | p95 ms | Bytes/query | Graph bytes/query | Resident bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | 10,000 | exact | 1.00 | 1.00 | 5.9 | 65.5 KB | 0 B | 61.3 KB |
| synthetic-uniform | 10,000 | flat-scan | 0.92 | 0.92 | 7.3 | 115.5 KB | 0 B | 61.3 KB |
| synthetic-uniform | 10,000 | sq-scan | 0.92 | 0.92 | 6.3 | 115.5 KB | 0 B | 61.3 KB |
| synthetic-uniform | 10,000 | pq-scan | 1.00 | 1.00 | 8.1 | 115.5 KB | 0 B | 61.3 KB |
| synthetic-uniform | 10,000 | graph | 0.88 | 0.88 | 19.3 | 115.5 KB | 55.1 KB | 61.3 KB |
| synthetic-uniform | 10,000 | vamana-pq | 1.00 | 1.00 | 21.9 | 115.5 KB | 55.1 KB | 61.3 KB |
| synthetic-uniform | 10,000 | hybrid | 1.00 | 1.00 | 22.2 | 115.5 KB | 55.1 KB | 61.3 KB |
| synthetic-clustered | 10,000 | exact | 1.00 | 1.00 | 15.7 | 299.7 KB | 0 B | 61.3 KB |
| synthetic-clustered | 10,000 | flat-scan | 0.86 | 0.85 | 3.8 | 88.6 KB | 0 B | 61.3 KB |
| synthetic-clustered | 10,000 | sq-scan | 0.86 | 0.85 | 4.0 | 88.6 KB | 0 B | 61.3 KB |
| synthetic-clustered | 10,000 | pq-scan | 0.97 | 0.92 | 5.0 | 88.6 KB | 0 B | 61.3 KB |
| synthetic-clustered | 10,000 | graph | 0.97 | 0.92 | 12.6 | 88.6 KB | 57.8 KB | 61.3 KB |
| synthetic-clustered | 10,000 | vamana-pq | 0.97 | 0.92 | 12.5 | 88.6 KB | 57.8 KB | 61.3 KB |
| synthetic-clustered | 10,000 | hybrid | 0.97 | 0.92 | 11.7 | 88.6 KB | 57.8 KB | 61.3 KB |
| synthetic-adversarial | 10,000 | exact | 1.00 | 1.00 | 3.0 | 36.8 KB | 0 B | 61.3 KB |
| synthetic-adversarial | 10,000 | flat-scan | 1.00 | 1.00 | 4.5 | 62.6 KB | 0 B | 61.3 KB |
| synthetic-adversarial | 10,000 | sq-scan | 1.00 | 1.00 | 4.4 | 62.6 KB | 0 B | 61.3 KB |
| synthetic-adversarial | 10,000 | pq-scan | 1.00 | 1.00 | 5.7 | 62.6 KB | 0 B | 61.3 KB |
| synthetic-adversarial | 10,000 | graph | 1.00 | 1.00 | 11.8 | 62.6 KB | 35.9 KB | 61.3 KB |
| synthetic-adversarial | 10,000 | vamana-pq | 1.00 | 1.00 | 12.6 | 62.6 KB | 35.9 KB | 61.3 KB |
| synthetic-adversarial | 10,000 | hybrid | 1.00 | 1.00 | 16.8 | 62.6 KB | 35.9 KB | 61.3 KB |
| sklearn-digits | 1,797 | exact | 1.00 | 1.00 | 5.9 | 207.7 KB | 0 B | 12.5 KB |
| sklearn-digits | 1,797 | flat-scan | 0.45 | 0.45 | 5.7 | 208.7 KB | 0 B | 12.5 KB |
| sklearn-digits | 1,797 | sq-scan | 0.45 | 0.45 | 6.2 | 208.7 KB | 0 B | 12.5 KB |
| sklearn-digits | 1,797 | pq-scan | 1.00 | 1.00 | 7.9 | 208.7 KB | 0 B | 12.5 KB |
| sklearn-digits | 1,797 | graph | 0.97 | 0.97 | 25.4 | 208.7 KB | 92.0 KB | 12.5 KB |
| sklearn-digits | 1,797 | vamana-pq | 1.00 | 1.00 | 27.9 | 208.7 KB | 92.0 KB | 12.5 KB |
| sklearn-digits | 1,797 | hybrid | 1.00 | 1.00 | 25.8 | 208.7 KB | 92.0 KB | 12.5 KB |

The synthetic-uniform and synthetic-clustered generators intentionally include
duplicate/tied nearest vectors. Tie-aware recall avoids treating a different id
with the same exact kth-distance as a miss. Id recall remains in the artifacts
so duplicate-id effects stay visible.

These checked-in numbers must be regenerated whenever routing, compaction,
leaf-mode, storage, or cache behavior changes. Low recall on synthetic-uniform
after compaction is a regression because query vectors are present in the
dataset and should route to their vector-local leaf blobs.

## Parallel Graph Pressure

The table below shows the graph-heavy modes at 8 workers. The web page exposes
all modes and every parallelism point.

| Dataset | Records | Mode | Workers | QPS | p95 ms | RSS peak delta | Graph bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | 10,000 | graph | 8 | 358.2 | 25.8 | 1.56 MB | 55.1 KB |
| synthetic-uniform | 10,000 | vamana-pq | 8 | 361.6 | 26.1 | 524.3 KB | 55.1 KB |
| synthetic-uniform | 10,000 | hybrid | 8 | 356.6 | 25.5 | 311.3 KB | 55.1 KB |
| synthetic-clustered | 10,000 | graph | 8 | 158.5 | 60.5 | 426.0 KB | 57.8 KB |
| synthetic-clustered | 10,000 | vamana-pq | 8 | 155.2 | 63.2 | 311.3 KB | 57.8 KB |
| synthetic-clustered | 10,000 | hybrid | 8 | 154.3 | 77.5 | 294.9 KB | 57.8 KB |
| synthetic-adversarial | 10,000 | graph | 8 | 399.6 | 23.4 | 393.2 KB | 35.9 KB |
| synthetic-adversarial | 10,000 | vamana-pq | 8 | 341.5 | 27.9 | 278.5 KB | 35.9 KB |
| synthetic-adversarial | 10,000 | hybrid | 8 | 346.5 | 28.5 | 311.3 KB | 35.9 KB |
| sklearn-digits | 1,797 | graph | 8 | 242.6 | 38.2 | 294.9 KB | 92.0 KB |
| sklearn-digits | 1,797 | vamana-pq | 8 | 248.5 | 35.7 | 327.7 KB | 92.0 KB |
| sklearn-digits | 1,797 | hybrid | 8 | 222.0 | 43.8 | 360.4 KB | 92.0 KB |

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

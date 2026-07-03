# Benchmarks

BORSUK has two benchmark layers:

- Criterion functions in `crates/borsuk/benches/local_search.rs` for local
  repeatable timing.
- `crates/borsuk/examples/benchmark_report.rs` for developer-facing tables,
  CSV artifacts, parallel-query pressure, RSS sampling, and web charts.

The hosted docs page renders the CSV outputs interactively.

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
`max_segments=8`, and `max_candidates_per_segment=64`.

| Dataset | Records | Mode | Tie Recall@10 | Id Recall@10 | p95 ms | Bytes/query | Graph bytes/query | Resident bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | 10,000 | exact | 1.00 | 1.00 | 19.4 | 1.18 MB | 0 B | 61.3 KB |
| synthetic-uniform | 10,000 | flat-scan | 0.76 | 0.74 | 4.7 | 242.2 KB | 0 B | 61.3 KB |
| synthetic-uniform | 10,000 | sq-scan | 0.76 | 0.74 | 4.2 | 242.2 KB | 0 B | 61.3 KB |
| synthetic-uniform | 10,000 | pq-scan | 0.76 | 0.74 | 5.3 | 242.2 KB | 0 B | 61.3 KB |
| synthetic-uniform | 10,000 | graph | 0.76 | 0.74 | 15.5 | 242.2 KB | 80.1 KB | 61.3 KB |
| synthetic-uniform | 10,000 | vamana-pq | 0.76 | 0.74 | 13.4 | 242.2 KB | 80.1 KB | 61.3 KB |
| synthetic-uniform | 10,000 | hybrid | 0.76 | 0.74 | 15.7 | 242.2 KB | 80.1 KB | 61.3 KB |
| synthetic-clustered | 10,000 | exact | 1.00 | 1.00 | 18.0 | 650.7 KB | 0 B | 61.3 KB |
| synthetic-clustered | 10,000 | flat-scan | 0.68 | 0.68 | 4.1 | 132.0 KB | 0 B | 61.3 KB |
| synthetic-clustered | 10,000 | sq-scan | 0.68 | 0.68 | 4.0 | 132.0 KB | 0 B | 61.3 KB |
| synthetic-clustered | 10,000 | pq-scan | 0.68 | 0.68 | 5.2 | 132.0 KB | 0 B | 61.3 KB |
| synthetic-clustered | 10,000 | graph | 0.66 | 0.66 | 15.6 | 132.0 KB | 78.0 KB | 61.3 KB |
| synthetic-clustered | 10,000 | vamana-pq | 0.68 | 0.68 | 12.0 | 132.0 KB | 78.0 KB | 61.3 KB |
| synthetic-clustered | 10,000 | hybrid | 0.66 | 0.66 | 15.9 | 132.0 KB | 78.0 KB | 61.3 KB |
| synthetic-adversarial | 10,000 | exact | 1.00 | 1.00 | 17.4 | 325.3 KB | 0 B | 61.3 KB |
| synthetic-adversarial | 10,000 | flat-scan | 1.00 | 1.00 | 3.8 | 65.6 KB | 0 B | 61.3 KB |
| synthetic-adversarial | 10,000 | sq-scan | 1.00 | 1.00 | 3.6 | 65.6 KB | 0 B | 61.3 KB |
| synthetic-adversarial | 10,000 | pq-scan | 1.00 | 0.60 | 5.3 | 65.6 KB | 0 B | 61.3 KB |
| synthetic-adversarial | 10,000 | graph | 1.00 | 1.00 | 12.7 | 65.6 KB | 54.4 KB | 61.3 KB |
| synthetic-adversarial | 10,000 | vamana-pq | 1.00 | 0.75 | 14.3 | 65.6 KB | 54.4 KB | 61.3 KB |
| synthetic-adversarial | 10,000 | hybrid | 1.00 | 1.00 | 12.1 | 65.6 KB | 54.4 KB | 61.3 KB |
| sklearn-digits | 1,797 | exact | 1.00 | 1.00 | 4.7 | 209.1 KB | 0 B | 12.5 KB |
| sklearn-digits | 1,797 | flat-scan | 0.45 | 0.45 | 4.4 | 209.1 KB | 0 B | 12.5 KB |
| sklearn-digits | 1,797 | sq-scan | 0.45 | 0.45 | 3.9 | 209.1 KB | 0 B | 12.5 KB |
| sklearn-digits | 1,797 | pq-scan | 1.00 | 1.00 | 5.3 | 209.1 KB | 0 B | 12.5 KB |
| sklearn-digits | 1,797 | graph | 0.94 | 0.94 | 15.5 | 209.1 KB | 90.4 KB | 12.5 KB |
| sklearn-digits | 1,797 | vamana-pq | 0.99 | 0.99 | 14.6 | 209.1 KB | 90.4 KB | 12.5 KB |
| sklearn-digits | 1,797 | hybrid | 0.94 | 0.94 | 14.9 | 209.1 KB | 90.4 KB | 12.5 KB |

The synthetic-uniform and synthetic-clustered generators intentionally include
duplicate/tied nearest vectors. Tie-aware recall avoids treating a different id
with the same exact kth-distance as a miss. Id recall remains in the artifacts
so duplicate-id effects stay visible.

## Parallel Graph Pressure

The table below shows the graph-heavy modes at 8 workers. The web page exposes
all modes and every parallelism point.

| Dataset | Records | Mode | Workers | QPS | p95 ms | RSS peak delta | Graph bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | 10,000 | graph | 8 | 271.4 | 35.0 | 1.52 MB | 80.1 KB |
| synthetic-uniform | 10,000 | vamana-pq | 8 | 294.8 | 31.8 | 376.8 KB | 80.1 KB |
| synthetic-uniform | 10,000 | hybrid | 8 | 278.0 | 33.9 | 311.3 KB | 80.1 KB |
| synthetic-clustered | 10,000 | graph | 8 | 254.7 | 35.7 | 917.5 KB | 78.0 KB |
| synthetic-clustered | 10,000 | vamana-pq | 8 | 352.6 | 26.7 | 393.2 KB | 78.0 KB |
| synthetic-clustered | 10,000 | hybrid | 8 | 256.0 | 38.1 | 344.1 KB | 78.0 KB |
| synthetic-adversarial | 10,000 | graph | 8 | 358.1 | 29.3 | 458.8 KB | 54.4 KB |
| synthetic-adversarial | 10,000 | vamana-pq | 8 | 275.3 | 33.7 | 294.9 KB | 54.4 KB |
| synthetic-adversarial | 10,000 | hybrid | 8 | 375.0 | 29.3 | 622.6 KB | 54.4 KB |
| sklearn-digits | 1,797 | graph | 8 | 284.7 | 34.5 | 360.4 KB | 90.4 KB |
| sklearn-digits | 1,797 | vamana-pq | 8 | 305.0 | 33.5 | 327.7 KB | 90.4 KB |
| sklearn-digits | 1,797 | hybrid | 8 | 280.6 | 35.0 | 327.7 KB | 90.4 KB |

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

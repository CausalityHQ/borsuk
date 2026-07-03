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

- recall@10 against exact mode;
- p50/p95 latency;
- average segment bytes read;
- average graph bytes read;
- average resident metadata bytes reported by `SearchReport`;
- average segments searched, rows considered, rows exact-scored;
- object-cache hits and misses.

Parallel rows:

- the same per-query recall, latency, bytes, and resident metadata;
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

| Dataset | Mode | Recall@10 | p95 ms | Bytes/query | Graph bytes/query | Resident bytes |
|---|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | exact | 1.00 | 45.9 | 1.18 MB | 0 B | 50.1 KB |
| synthetic-uniform | flat-scan | 0.28 | 8.3 | 243.7 KB | 0 B | 50.1 KB |
| synthetic-uniform | sq-scan | 0.28 | 8.5 | 243.7 KB | 0 B | 50.1 KB |
| synthetic-uniform | pq-scan | 0.28 | 11.1 | 243.7 KB | 0 B | 50.1 KB |
| synthetic-uniform | graph | 0.28 | 34.0 | 243.7 KB | 79.0 KB | 50.1 KB |
| synthetic-uniform | vamana-pq | 0.28 | 30.7 | 243.7 KB | 79.0 KB | 50.1 KB |
| synthetic-uniform | hybrid | 0.28 | 33.2 | 243.7 KB | 79.0 KB | 50.1 KB |
| synthetic-clustered | exact | 1.00 | 42.2 | 650.7 KB | 0 B | 50.1 KB |
| synthetic-clustered | flat-scan | 0.26 | 8.4 | 132.2 KB | 0 B | 50.1 KB |
| synthetic-clustered | sq-scan | 0.26 | 8.2 | 132.2 KB | 0 B | 50.1 KB |
| synthetic-clustered | pq-scan | 0.27 | 11.3 | 132.2 KB | 0 B | 50.1 KB |
| synthetic-clustered | graph | 0.26 | 36.0 | 132.2 KB | 77.9 KB | 50.1 KB |
| synthetic-clustered | vamana-pq | 0.27 | 27.3 | 132.2 KB | 77.9 KB | 50.1 KB |
| synthetic-clustered | hybrid | 0.26 | 36.2 | 132.2 KB | 77.9 KB | 50.1 KB |
| synthetic-adversarial | exact | 1.00 | 40.5 | 325.3 KB | 0 B | 50.1 KB |
| synthetic-adversarial | flat-scan | 1.00 | 8.3 | 65.6 KB | 0 B | 50.1 KB |
| synthetic-adversarial | sq-scan | 1.00 | 8.3 | 65.6 KB | 0 B | 50.1 KB |
| synthetic-adversarial | pq-scan | 0.60 | 10.7 | 65.6 KB | 0 B | 50.1 KB |
| synthetic-adversarial | graph | 1.00 | 29.4 | 65.6 KB | 54.4 KB | 50.1 KB |
| synthetic-adversarial | vamana-pq | 0.75 | 33.1 | 65.6 KB | 54.4 KB | 50.1 KB |
| synthetic-adversarial | hybrid | 1.00 | 29.8 | 65.6 KB | 54.4 KB | 50.1 KB |
| sklearn-digits | exact | 1.00 | 6.9 | 209.1 KB | 0 B | 10.2 KB |
| sklearn-digits | flat-scan | 0.45 | 6.7 | 209.1 KB | 0 B | 10.2 KB |
| sklearn-digits | sq-scan | 0.45 | 6.5 | 209.1 KB | 0 B | 10.2 KB |
| sklearn-digits | pq-scan | 1.00 | 9.2 | 209.1 KB | 0 B | 10.2 KB |
| sklearn-digits | graph | 0.94 | 34.0 | 209.1 KB | 90.4 KB | 10.2 KB |
| sklearn-digits | vamana-pq | 0.99 | 33.9 | 209.1 KB | 90.4 KB | 10.2 KB |
| sklearn-digits | hybrid | 0.94 | 33.5 | 209.1 KB | 90.4 KB | 10.2 KB |

## Parallel Graph Pressure

The table below shows the graph-heavy modes at 8 workers. The web page exposes
all modes and every parallelism point.

| Dataset | Mode | Workers | QPS | p95 ms | RSS peak delta | Graph bytes/query |
|---|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | graph | 8 | 217.5 | 40.0 | 1.88 MB | 79.0 KB |
| synthetic-uniform | vamana-pq | 8 | 241.9 | 35.0 | 540.7 KB | 79.0 KB |
| synthetic-uniform | hybrid | 8 | 229.5 | 37.4 | 376.8 KB | 79.0 KB |
| synthetic-clustered | graph | 8 | 222.4 | 39.8 | 393.2 KB | 77.9 KB |
| synthetic-clustered | vamana-pq | 8 | 225.5 | 40.9 | 360.4 KB | 77.9 KB |
| synthetic-clustered | hybrid | 8 | 219.3 | 42.9 | 311.3 KB | 77.9 KB |
| synthetic-adversarial | graph | 8 | 256.7 | 35.6 | 376.8 KB | 54.4 KB |
| synthetic-adversarial | vamana-pq | 8 | 219.6 | 39.8 | 409.6 KB | 54.4 KB |
| synthetic-adversarial | hybrid | 8 | 251.4 | 40.1 | 311.3 KB | 54.4 KB |
| sklearn-digits | graph | 8 | 214.4 | 40.4 | 376.8 KB | 90.4 KB |
| sklearn-digits | vamana-pq | 8 | 231.0 | 38.5 | 360.4 KB | 90.4 KB |
| sklearn-digits | hybrid | 8 | 236.6 | 37.3 | 327.7 KB | 90.4 KB |

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

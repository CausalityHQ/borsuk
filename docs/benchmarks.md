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
| synthetic-uniform | 10,000 | exact | 1.00 | 1.00 | 25.1 | 1.18 MB | 0 B | 50.1 KB |
| synthetic-uniform | 10,000 | flat-scan | 0.30 | 0.28 | 5.5 | 243.7 KB | 0 B | 50.1 KB |
| synthetic-uniform | 10,000 | sq-scan | 0.30 | 0.28 | 5.2 | 243.7 KB | 0 B | 50.1 KB |
| synthetic-uniform | 10,000 | pq-scan | 0.30 | 0.28 | 6.3 | 243.7 KB | 0 B | 50.1 KB |
| synthetic-uniform | 10,000 | graph | 0.30 | 0.28 | 18.4 | 243.7 KB | 79.0 KB | 50.1 KB |
| synthetic-uniform | 10,000 | vamana-pq | 0.30 | 0.28 | 17.2 | 243.7 KB | 79.0 KB | 50.1 KB |
| synthetic-uniform | 10,000 | hybrid | 0.30 | 0.28 | 17.4 | 243.7 KB | 79.0 KB | 50.1 KB |
| synthetic-clustered | 10,000 | exact | 1.00 | 1.00 | 20.5 | 650.7 KB | 0 B | 50.1 KB |
| synthetic-clustered | 10,000 | flat-scan | 0.32 | 0.26 | 4.3 | 132.2 KB | 0 B | 50.1 KB |
| synthetic-clustered | 10,000 | sq-scan | 0.32 | 0.26 | 4.7 | 132.2 KB | 0 B | 50.1 KB |
| synthetic-clustered | 10,000 | pq-scan | 0.33 | 0.27 | 5.5 | 132.2 KB | 0 B | 50.1 KB |
| synthetic-clustered | 10,000 | graph | 0.32 | 0.26 | 17.7 | 132.2 KB | 77.9 KB | 50.1 KB |
| synthetic-clustered | 10,000 | vamana-pq | 0.33 | 0.27 | 12.9 | 132.2 KB | 77.9 KB | 50.1 KB |
| synthetic-clustered | 10,000 | hybrid | 0.32 | 0.26 | 17.2 | 132.2 KB | 77.9 KB | 50.1 KB |
| synthetic-adversarial | 10,000 | exact | 1.00 | 1.00 | 19.6 | 325.3 KB | 0 B | 50.1 KB |
| synthetic-adversarial | 10,000 | flat-scan | 1.00 | 1.00 | 4.1 | 65.6 KB | 0 B | 50.1 KB |
| synthetic-adversarial | 10,000 | sq-scan | 1.00 | 1.00 | 4.7 | 65.6 KB | 0 B | 50.1 KB |
| synthetic-adversarial | 10,000 | pq-scan | 1.00 | 0.60 | 5.5 | 65.6 KB | 0 B | 50.1 KB |
| synthetic-adversarial | 10,000 | graph | 1.00 | 1.00 | 16.2 | 65.6 KB | 54.4 KB | 50.1 KB |
| synthetic-adversarial | 10,000 | vamana-pq | 1.00 | 0.75 | 16.1 | 65.6 KB | 54.4 KB | 50.1 KB |
| synthetic-adversarial | 10,000 | hybrid | 1.00 | 1.00 | 14.0 | 65.6 KB | 54.4 KB | 50.1 KB |
| sklearn-digits | 1,797 | exact | 1.00 | 1.00 | 6.7 | 209.1 KB | 0 B | 10.2 KB |
| sklearn-digits | 1,797 | flat-scan | 0.45 | 0.45 | 5.9 | 209.1 KB | 0 B | 10.2 KB |
| sklearn-digits | 1,797 | sq-scan | 0.45 | 0.45 | 5.8 | 209.1 KB | 0 B | 10.2 KB |
| sklearn-digits | 1,797 | pq-scan | 1.00 | 1.00 | 7.2 | 209.1 KB | 0 B | 10.2 KB |
| sklearn-digits | 1,797 | graph | 0.94 | 0.94 | 20.5 | 209.1 KB | 90.4 KB | 10.2 KB |
| sklearn-digits | 1,797 | vamana-pq | 0.99 | 0.99 | 18.9 | 209.1 KB | 90.4 KB | 10.2 KB |
| sklearn-digits | 1,797 | hybrid | 0.94 | 0.94 | 18.9 | 209.1 KB | 90.4 KB | 10.2 KB |

The synthetic-uniform and synthetic-clustered generators intentionally include
duplicate/tied nearest vectors. Tie-aware recall avoids treating a different id
with the same exact kth-distance as a miss. Id recall remains in the artifacts
so duplicate-id effects stay visible.

## Parallel Graph Pressure

The table below shows the graph-heavy modes at 8 workers. The web page exposes
all modes and every parallelism point.

| Dataset | Records | Mode | Workers | QPS | p95 ms | RSS peak delta | Graph bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | 10,000 | graph | 8 | 178.5 | 57.5 | 3.39 MB | 79.0 KB |
| synthetic-uniform | 10,000 | vamana-pq | 8 | 189.6 | 53.7 | 589.8 KB | 79.0 KB |
| synthetic-uniform | 10,000 | hybrid | 8 | 190.2 | 63.7 | 311.3 KB | 79.0 KB |
| synthetic-clustered | 10,000 | graph | 8 | 183.9 | 53.5 | 294.9 KB | 77.9 KB |
| synthetic-clustered | 10,000 | vamana-pq | 8 | 248.0 | 37.1 | 311.3 KB | 77.9 KB |
| synthetic-clustered | 10,000 | hybrid | 8 | 178.3 | 52.5 | 360.4 KB | 77.9 KB |
| synthetic-adversarial | 10,000 | graph | 8 | 277.2 | 34.1 | 327.7 KB | 54.4 KB |
| synthetic-adversarial | 10,000 | vamana-pq | 8 | 226.5 | 39.0 | 278.5 KB | 54.4 KB |
| synthetic-adversarial | 10,000 | hybrid | 8 | 223.4 | 54.9 | 311.3 KB | 54.4 KB |
| sklearn-digits | 1,797 | graph | 8 | 208.3 | 48.8 | 409.6 KB | 90.4 KB |
| sklearn-digits | 1,797 | vamana-pq | 8 | 229.3 | 44.7 | 311.3 KB | 90.4 KB |
| sklearn-digits | 1,797 | hybrid | 8 | 209.4 | 43.1 | 311.3 KB | 90.4 KB |

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

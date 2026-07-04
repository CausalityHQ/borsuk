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
  --queries 100 \
  --parallelism 1,2,4,8 \
  --artifacts-dir /tmp/borsuk-bench
```

The report exposes query and layout knobs so recall can be evaluated as a
budget curve instead of a single hard-coded point:

```bash
cargo run --locked --release -p borsuk --example benchmark_report -- \
  --synthetic-records 100000 \
  --queries 20 \
  --parallelism 1 \
  --segment-max-vectors 256 \
  --max-segments 32 \
  --routing-page-overfetch 8 \
  --max-candidates-per-segment 256
```

Use `--segment-max-vectors` to change ingest and compaction leaf size,
`--max-segments` to change the expensive segment-payload budget,
`--routing-page-overfetch` to change cheap routing metadata lookahead, and
`--max-candidates-per-segment` to change local exact-rerank work inside each
fetched leaf.

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
  --queries 100 \
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
  --queries 100 \
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

`routing_page_overfetch` is cheap metadata lookahead, not a segment-payload
multiplier. If persisted vector bounds are decisive, larger values can leave
routing page reads unchanged. If bounds are tied or close, larger values allow
the query to decode more cheap routing metadata before spending the expensive
segment and graph payload budgets. At each routing layer, the setting also acts
as a page-level floor for close pages, so one dense page cannot consume the
whole lookahead by leaf-segment count alone.

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
  segment payload count, bytes read, graph bytes read, RSS before/peak/after,
  RSS peak delta, resident bytes, rows considered, rows scored, and graph
  candidates.

## Current Local Results

Measured on Apple M3 Max, 16 cores, 128 GB RAM, Darwin 25.2.0, Rust 1.95.0.
The checked-in web artifacts were regenerated with `--queries 100`,
`--synthetic-records-list 10000,100000`, `--parallelism 1,2,4,8`, and the
scikit-learn digits CSV. Synthetic datasets use 64 dimensions,
`segment_max_vectors=256`, `max_segments=8`, `routing_page_overfetch=8`, and
`max_candidates_per_segment=64`. They are compacted into vector-local leaves
before query timing.

Lifecycle timing is reported separately from query latency:

| Dataset | Records | Ingest vectors/sec | Compaction vectors/sec | Ingest ms | Compaction ms | Segments read/written | Compaction bytes read/written |
|---|---:|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | 10,000 | 9,906 | 3,629 | 1009.5 | 2755.4 | 40/40 | 1.19 MB / 803.0 KB |
| synthetic-clustered | 10,000 | 12,702 | 3,166 | 787.3 | 3158.3 | 40/40 | 683.3 KB / 684.4 KB |
| synthetic-adversarial | 10,000 | 14,092 | 4,223 | 709.6 | 2368.0 | 40/40 | 351.7 KB / 458.3 KB |
| synthetic-uniform | 100,000 | 13,882 | 4,133 | 7203.4 | 24193.0 | 391/391 | 11.70 MB / 5.05 MB |
| synthetic-clustered | 100,000 | 13,040 | 3,117 | 7668.9 | 32084.6 | 391/391 | 6.54 MB / 4.97 MB |
| synthetic-adversarial | 100,000 | 13,842 | 3,018 | 7224.5 | 33134.9 | 391/391 | 3.35 MB / 4.32 MB |
| sklearn-digits | 1,797 | 12,455 | 2,819 | 144.3 | 637.6 | 8/8 | 229.0 KB / 291.9 KB |

| Dataset | Records | Mode | Tie Recall@10 | Id Recall@10 | p95 ms | Bytes/query | Graph bytes/query | Resident bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | 10,000 | exact | 1.00 | 1.00 | 3.2 | 76.1 KB | 0 B | 275 B |
| synthetic-uniform | 10,000 | flat-scan | 0.96 | 0.96 | 9.6 | 174.2 KB | 0 B | 275 B |
| synthetic-uniform | 10,000 | sq-scan | 0.96 | 0.96 | 8.6 | 174.2 KB | 0 B | 275 B |
| synthetic-uniform | 10,000 | pq-scan | 1.00 | 1.00 | 12.3 | 174.2 KB | 0 B | 275 B |
| synthetic-uniform | 10,000 | graph | 0.97 | 0.97 | 28.1 | 174.2 KB | 48.3 KB | 275 B |
| synthetic-uniform | 10,000 | vamana-pq | 1.00 | 1.00 | 26.7 | 174.2 KB | 48.3 KB | 275 B |
| synthetic-uniform | 10,000 | hybrid | 1.00 | 1.00 | 19.7 | 174.2 KB | 48.3 KB | 275 B |
| synthetic-clustered | 10,000 | exact | 1.00 | 1.00 | 11.7 | 118.0 KB | 0 B | 275 B |
| synthetic-clustered | 10,000 | flat-scan | 0.94 | 0.91 | 9.8 | 148.8 KB | 0 B | 275 B |
| synthetic-clustered | 10,000 | sq-scan | 0.94 | 0.91 | 10.0 | 148.8 KB | 0 B | 275 B |
| synthetic-clustered | 10,000 | pq-scan | 0.99 | 0.98 | 11.2 | 148.8 KB | 0 B | 275 B |
| synthetic-clustered | 10,000 | graph | 0.96 | 0.95 | 23.3 | 148.8 KB | 51.8 KB | 275 B |
| synthetic-clustered | 10,000 | vamana-pq | 0.99 | 0.98 | 23.4 | 148.8 KB | 51.8 KB | 275 B |
| synthetic-clustered | 10,000 | hybrid | 0.99 | 0.98 | 24.6 | 148.8 KB | 51.8 KB | 275 B |
| synthetic-adversarial | 10,000 | exact | 1.00 | 1.00 | 3.5 | 58.0 KB | 0 B | 275 B |
| synthetic-adversarial | 10,000 | flat-scan | 1.00 | 1.00 | 7.9 | 102.3 KB | 0 B | 275 B |
| synthetic-adversarial | 10,000 | sq-scan | 1.00 | 1.00 | 8.2 | 102.3 KB | 0 B | 275 B |
| synthetic-adversarial | 10,000 | pq-scan | 1.00 | 1.00 | 9.6 | 102.3 KB | 0 B | 275 B |
| synthetic-adversarial | 10,000 | graph | 1.00 | 1.00 | 16.8 | 102.3 KB | 33.0 KB | 275 B |
| synthetic-adversarial | 10,000 | vamana-pq | 1.00 | 1.00 | 18.3 | 102.3 KB | 33.0 KB | 275 B |
| synthetic-adversarial | 10,000 | hybrid | 1.00 | 1.00 | 17.9 | 102.3 KB | 33.0 KB | 275 B |
| sklearn-digits | 1,797 | exact | 1.00 | 1.00 | 10.5 | 224.6 KB | 0 B | 275 B |
| sklearn-digits | 1,797 | flat-scan | 0.46 | 0.46 | 10.5 | 228.5 KB | 0 B | 275 B |
| sklearn-digits | 1,797 | sq-scan | 0.46 | 0.46 | 11.4 | 228.5 KB | 0 B | 275 B |
| sklearn-digits | 1,797 | pq-scan | 1.00 | 1.00 | 14.8 | 228.5 KB | 0 B | 275 B |
| sklearn-digits | 1,797 | graph | 0.98 | 0.98 | 38.4 | 228.5 KB | 85.7 KB | 275 B |
| sklearn-digits | 1,797 | vamana-pq | 1.00 | 1.00 | 27.9 | 228.5 KB | 85.7 KB | 275 B |
| sklearn-digits | 1,797 | hybrid | 1.00 | 1.00 | 28.0 | 228.5 KB | 85.7 KB | 275 B |

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
with `--synthetic-records-list 10000,100000` and 100 queries per dataset. At
100k vectors, all high-recall modes reached `1.000` tie-aware recall@10 with
the strict `max_segments=8` payload budget. A previous diagnostic probe showed
that clustered misses came from routing metadata breadth, not per-segment
candidate scoring: raising `max_candidates_per_segment` increased exact-scored
rows without improving recall, while decoding one more close sibling L0 routing
page restored the missing neighbors. Routing overfetch now has a page-level
floor at every routing layer, so the default `routing_page_overfetch=8` can
keep sibling metadata pages eligible for final segment ranking without raising
vector payload reads.

| Dataset | Records | Mode | Tie Recall@10 | p95 ms | Bytes/query | Graph bytes/query | Resident bytes |
|---|---:|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | 100,000 | pq-scan | 1.00 | 10.8 | 220.4 KB | 0 B | 275 B |
| synthetic-uniform | 100,000 | vamana-pq | 1.00 | 27.3 | 220.4 KB | 33.9 KB | 275 B |
| synthetic-uniform | 100,000 | hybrid | 1.00 | 22.9 | 220.4 KB | 33.9 KB | 275 B |
| synthetic-clustered | 100,000 | pq-scan | 1.00 | 12.2 | 246.0 KB | 0 B | 275 B |
| synthetic-clustered | 100,000 | vamana-pq | 1.00 | 21.6 | 246.0 KB | 34.9 KB | 275 B |
| synthetic-clustered | 100,000 | hybrid | 1.00 | 20.9 | 246.0 KB | 34.9 KB | 275 B |
| synthetic-adversarial | 100,000 | pq-scan | 1.00 | 10.7 | 158.5 KB | 0 B | 275 B |
| synthetic-adversarial | 100,000 | vamana-pq | 1.00 | 19.9 | 158.5 KB | 32.1 KB | 275 B |
| synthetic-adversarial | 100,000 | hybrid | 1.00 | 19.3 | 158.5 KB | 32.1 KB | 275 B |

The checked-in `routing-overfetch.csv` sweeps
`routing_page_overfetch=1,2,4,8,16,32` for the high-recall modes across the same
datasets. On the 100k synthetic rows, synthetic-uniform and synthetic-adversarial
stayed at `1.000000` tie-aware recall@10 across the sweep. Synthetic-clustered
was `0.970000` at `routing_page_overfetch=1` and reached `1.000000` from
`routing_page_overfetch=2` upward. Average routing page reads stayed around
`2.0` for decisive bounds and rose to `2.42` on the clustered 100k sweep,
showing that overfetch spends extra metadata reads only when routing bounds are
close enough to matter.

The latest million-vector gate was run with 1,000,000 synthetic vectors,
16 dimensions, `segment_max_vectors=128`, `max_segments=512`,
`routing_page_overfetch=8`, and `max_candidates_per_segment=128`. After
compaction into 7,813 vector-local segments, `pq-scan`, `vamana-pq`, and
`hybrid` all reached `1.000`
tie-aware recall@10 and strict id recall@10 while reading at most 512 segment
payloads. `pq-scan` read 14.46 MB/query and no graph bytes; graph-backed modes
read the same segment bytes plus 4.42 MB/query of graph bytes. The checked-in
`large-scale.csv` run ingested in 33.3s, compacted in 57.8s, and ran the exact
recall reference in 1.40s on the same machine. Compaction read 161.77 MB and
wrote 224.70 MB, counting both new segment and graph payload bytes. RSS peak
delta stayed below 64 KB for each measured
single-query mode. The fix that made this pass is metadata overfetch: search
reads extra compact routing pages ranked by persisted vector bounds, then keeps
the expensive segment/graph payload budget strict.

| Records | Mode | Tie Recall@10 | Id Recall@10 | Query ms | Segments searched | Bytes/query | Graph bytes/query | Routing pages | RSS peak delta | Resident bytes |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000,000 | pq-scan | 1.00 | 1.00 | 254.0 | 512 | 13.79 MB | 0 B | 33 | 64.0 KB | 275 B |
| 1,000,000 | vamana-pq | 1.00 | 1.00 | 1565.0 | 512 | 13.79 MB | 4.22 MB | 33 | 48.0 KB | 275 B |
| 1,000,000 | hybrid | 1.00 | 1.00 | 1419.0 | 512 | 13.79 MB | 4.22 MB | 33 | 16.0 KB | 275 B |

## Parallel Graph Pressure

The table below shows the graph-heavy modes at 8 workers. The web page exposes
all modes and every parallelism point.

| Dataset | Records | Mode | Workers | QPS | p95 ms | RSS peak delta | Graph bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | 10,000 | graph | 8 | 359.9 | 24.9 | 1.00 MB | 48.3 KB |
| synthetic-uniform | 10,000 | vamana-pq | 8 | 355.1 | 24.3 | 496.0 KB | 48.3 KB |
| synthetic-uniform | 10,000 | hybrid | 8 | 347.7 | 25.7 | 336.0 KB | 48.3 KB |
| synthetic-clustered | 10,000 | graph | 8 | 332.2 | 26.9 | 304.0 KB | 51.8 KB |
| synthetic-clustered | 10,000 | vamana-pq | 8 | 335.3 | 26.6 | 368.0 KB | 51.8 KB |
| synthetic-clustered | 10,000 | hybrid | 8 | 338.5 | 27.0 | 304.0 KB | 51.8 KB |
| synthetic-adversarial | 10,000 | graph | 8 | 404.4 | 21.8 | 496.0 KB | 33.0 KB |
| synthetic-adversarial | 10,000 | vamana-pq | 8 | 384.9 | 22.6 | 288.0 KB | 33.0 KB |
| synthetic-adversarial | 10,000 | hybrid | 8 | 384.7 | 22.7 | 272.0 KB | 33.0 KB |
| synthetic-uniform | 100,000 | graph | 8 | 340.0 | 26.4 | 512.0 KB | 33.9 KB |
| synthetic-uniform | 100,000 | vamana-pq | 8 | 364.2 | 24.2 | 480.0 KB | 33.9 KB |
| synthetic-uniform | 100,000 | hybrid | 8 | 361.2 | 24.6 | 480.0 KB | 33.9 KB |
| synthetic-clustered | 100,000 | graph | 8 | 358.6 | 24.4 | 384.0 KB | 34.9 KB |
| synthetic-clustered | 100,000 | vamana-pq | 8 | 336.7 | 26.3 | 304.0 KB | 34.9 KB |
| synthetic-clustered | 100,000 | hybrid | 8 | 337.6 | 26.7 | 352.0 KB | 34.9 KB |
| synthetic-adversarial | 100,000 | graph | 8 | 379.6 | 23.2 | 304.0 KB | 32.1 KB |
| synthetic-adversarial | 100,000 | vamana-pq | 8 | 364.1 | 24.4 | 288.0 KB | 32.1 KB |
| synthetic-adversarial | 100,000 | hybrid | 8 | 359.3 | 24.5 | 272.0 KB | 32.1 KB |
| sklearn-digits | 1,797 | graph | 8 | 268.0 | 33.3 | 272.0 KB | 85.7 KB |
| sklearn-digits | 1,797 | vamana-pq | 8 | 272.5 | 33.6 | 272.0 KB | 85.7 KB |
| sklearn-digits | 1,797 | hybrid | 8 | 272.3 | 33.4 | 272.0 KB | 85.7 KB |

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

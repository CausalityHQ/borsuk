# Benchmark Plan

The benchmark harness starts with local deterministic synthetic vectors. It is
intended to grow into the benchmark plan from `design.md`, including Parquet
row-group/column-read measurements and S3-compatible object-store cold/warm
read tests.

Run:

```bash
cargo bench --locked -p borsuk
```

Current Criterion entries:

- `local_exact_search_10k_x_64` times exact top-k search over 10,000
  64-dimensional vectors.
- `local_approx_report_10k_x_64` times approximate search report generation
  with segment-local graph expansion and per-segment exact-scoring limits.
- `local_flat_scan_approx_report_10k_x_64` times the same budgeted approximate
  report path with the flat-scan leaf engine and no graph object reads.
- `local_sq_scan_approx_report_10k_x_64` times the budgeted scalar-code scan
  leaf engine with exact reranking and no graph object reads.
- `local_pq_scan_approx_report_10k_x_64` times the budgeted product-code scan
  leaf surface over persisted per-dimension UInt8 `pq_code` sketches with
  exact reranking and no graph object reads.
- `local_vamana_pq_approx_report_10k_x_64` times the initial VamanaPQ-style
  leaf engine backed by segment-local graph traversal and exact rerank.
- `local_hybrid_approx_report_10k_x_64` times the stored-segment leaf-mode
  selector, which uses graph traversal for current graph-backed L0 segments.
- `local_warm_cache_approx_report_10k_x_64` opens the same local index through
  the read-through cache, warms segment and graph objects, and times the
  cached approximate report path.
- `local_clustered_approx_report_10k_x_64` times the same approximate report
  path on deterministic clustered vectors so pruning and graph traversal are
  measured against dense local neighborhoods.
- `local_adversarial_approx_report_10k_x_64` times the approximate report path
  on deterministic high-dimensional alternating vectors where routing codes
  are deliberately less selective.

The approximate benchmark setup sanity-checks that approximate results retain
non-zero exact top-k recall, score fewer records than they consider, and read
segment-local graph bytes only for graph-backed leaf modes. The warm-cache setup
additionally verifies that the timed path is served from cache without
object-store misses.

CI also runs a deterministic performance smoke test:

```bash
cargo test --locked -p borsuk --test performance_smoke
```

The smoke test builds a 10,000-vector local Parquet index with 64-dimensional
vectors, searches for an exact existing vector, then exercises every
implemented leaf path through budgeted approximate search: graph, vamana-pq,
hybrid, flat-scan, sq-scan, and pq-scan. It checks
the nearest id for exact search, verifies non-zero approximate recall, confirms
graph-backed modes read graph bytes while scan modes do not, checks reduced exact
scoring under the per-segment candidate budget, validates segment-budget and
resident-routing memory counters, and enforces a one-second local search
ceiling for exact search plus all implemented budgeted approximate paths. It is
not a full benchmark, but it keeps the sub-second local latency target from
becoming purely aspirational.

The Python and TypeScript package test suites also include a lighter local
package smoke test named `test_local_package_search_reports_stay_subsecond`
and `local package search reports stay subsecond`. These build a deterministic
1,024-vector local Parquet index through the native package API and assert that
exact and hybrid approximate `SearchReport` timings stay below one second.

Tracked measurements:

- p50/p95/p99 latency;
- exact top-k agreement with brute force, measured with `recall_at_k` /
  `recallAtK`;
- segments touched per query;
- bytes read per query;
- graph bytes read and graph candidates added per query;
- object cache hits and misses per query;
- estimated memory resident in manifest, segment routing, and pivot summaries;
- insert throughput and segment write amplification.

Datasets to add:

- SIFT-128;
- GloVe angular;
- BEIR and MSMARCO embeddings;
- histogram/distribution vector datasets;
- binary/set-like vector datasets.

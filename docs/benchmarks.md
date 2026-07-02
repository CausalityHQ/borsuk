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
- `local_warm_cache_approx_report_10k_x_64` opens the same local index through
  the read-through cache, warms segment and graph objects, and times the
  cached approximate report path.

The approximate benchmark setup sanity-checks that approximate results retain
non-zero exact top-k recall, score fewer records than they consider, and read
segment-local graph bytes. The warm-cache setup additionally verifies that the
timed path is served from cache without object-store misses.

CI also runs a deterministic performance smoke test:

```bash
cargo test --locked -p borsuk --test performance_smoke
```

The smoke test builds a 10,000-vector local Parquet index with 64-dimensional
vectors, searches for an exact existing vector, checks the nearest id, verifies
that search reports segment, segment-byte, graph-byte, object-cache, and
resident-routing memory counters, and enforces a one-second local search
ceiling. It is not a full benchmark, but it keeps the sub-second local latency
target from becoming purely aspirational.

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
- synthetic clustered and adversarial vectors;
- string datasets for edit-distance metrics.

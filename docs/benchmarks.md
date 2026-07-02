# Benchmark Plan

The benchmark harness starts with local deterministic synthetic vectors. It is
intended to grow into the benchmark plan from `design.md`, including Parquet
row-group/column-read measurements and S3-compatible object-store cold/warm
read tests.

Run:

```bash
cargo bench --locked -p borsuk
```

CI also runs a deterministic performance smoke test:

```bash
cargo test --locked -p borsuk --test performance_smoke
```

The smoke test builds a 10,000-vector local Parquet index with 64-dimensional
vectors, searches for an exact existing vector, checks the nearest id, verifies
that search reports segment, segment-byte, and graph-byte counters, and
enforces a one-second local search ceiling. It is not a full benchmark, but it
keeps the sub-second local latency target from becoming purely aspirational.

Tracked measurements:

- p50/p95/p99 latency;
- exact top-k agreement with brute force;
- segments touched per query;
- bytes read per query;
- graph bytes read and graph candidates added per query;
- memory resident in manifest and routing summaries;
- insert throughput and segment write amplification.

Datasets to add:

- SIFT-128;
- GloVe angular;
- BEIR and MSMARCO embeddings;
- synthetic clustered and adversarial vectors;
- string datasets for edit-distance metrics.

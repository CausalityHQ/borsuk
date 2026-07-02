# BORSUK

**Blob-Oriented Retrieval with Segmental Unified KNN**

BORSUK is a Rust-first similarity-search library for indexes that live mostly
outside RAM. It stores vectors in immutable segment files and keeps only the
manifest and segment summaries resident while searching.

This repository is currently implementing the design in [`design.md`](design.md).
The first working slice is being migrated toward:

- Rust core crate: `borsuk`
- native Python API package in `python/`, backed by PyO3/maturin
- native TypeScript/Node API package in `packages/borsuk/`, backed by N-API
- Parquet/Arrow local-file and object-store storage
- append-only immutable segments, segment-local graph blocks, and binary
  manifest/routing/pivot tables
- out-of-place L0 to L1/L2 compaction and explicit obsolete-segment GC
- exact search with segment lower-bound pruning where the metric supports it
- budgeted approximate search with segment, byte, latency, and per-segment
  candidate limits plus bounded segment-local graph traversal
- optional local read-through cache for segment, graph, manifest, and routing
  objects
- search reports for Rust, Python, and TypeScript with segment, byte,
  exact-scoring, and resident-routing-memory counters
- broad dense-vector metrics, including Euclidean, cosine, inner product,
  angular, L1/L-infinity, Minkowski, histogram/distribution distances, set-like
  distances, plus string edit/similarity metrics exposed through Rust, Python,
  and TypeScript
- CI, publish workflow, pre-commit hooks, example, benchmark target, and docs

## Rust Quick Start

```rust
use borsuk::{BorsukIndex, IndexConfig, SearchOptions, VectorMetric, VectorRecord};

fn main() -> borsuk::Result<()> {
    let mut index = BorsukIndex::create(IndexConfig {
        uri: "file:///tmp/docs.borsuk".to_string(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1024,
        ram_budget_bytes: None,
    })?;

    index.add(vec![
        VectorRecord::new("a", vec![0.0, 0.0]),
        VectorRecord::new("b", vec![1.0, 0.0]),
    ])?;

    let hits = index.search(&[0.1, 0.0], SearchOptions::exact(1))?;
    println!("{hits:?}");
    Ok(())
}
```

## Examples

- Rust: [`crates/borsuk/examples/local_index.rs`](crates/borsuk/examples/local_index.rs)
- Python: [`python/examples/local_index.py`](python/examples/local_index.py)
- TypeScript: [`packages/borsuk/examples/local-index.ts`](packages/borsuk/examples/local-index.ts)
- SeaweedFS S3-compatible: [`examples/seaweedfs`](examples/seaweedfs/README.md)

## Current Status

BORSUK is not yet a production ANN system. The current code is a Phase 0/1
baseline being moved from a custom segment prototype to the design target:
Arrow schemas, Parquet durable storage, PyO3 Python bindings, and N-API
TypeScript bindings. Local files and S3-compatible object storage use the same
binary Parquet table layout through the Rust `object_store` backend; Avro and
Protobuf are reserved only for future non-index append logs or control-plane
messages. Basic
query-guided segment-local graph traversal, optional local read-through cache,
resident-memory budget enforcement, and multi-platform Python/TypeScript native
publish workflows are implemented; richer vector sketches and production tuning
are still active work.

## Object Storage

Use `s3://bucket/prefix` for AWS S3, MinIO, SeaweedFS, and other
S3-compatible stores. Endpoint and credentials are read from standard
object-store/AWS environment variables, for example:

```bash
export AWS_ENDPOINT=http://localhost:8333
export AWS_ALLOW_HTTP=true
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1
export AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false
export BORSUK_S3_TEST_URI=s3://borsuk-test/indexes

cargo test --locked -p borsuk s3_compatible_index_round_trip_when_configured \
  --test s3_compatible
```

Set `BORSUK_S3_TEST_URI=s3://bucket/prefix` to the bucket/prefix you want the
smoke test to write into.

For a local S3-compatible stack, see
[`examples/seaweedfs`](examples/seaweedfs/README.md). It starts SeaweedFS with
the S3 API enabled and runs the same integration test against
`http://127.0.0.1:8333`.

For blob-backed indexes, pass a local cache directory from Rust, Python, or
TypeScript to keep fetched immutable objects on local NVMe:

```python
idx = borsuk.open(
    "s3://my-bucket/indexes/docs.borsuk",
    cache_dir="/mnt/nvme/borsuk-cache",
)
```

Metric helpers are available without building an index:

```python
borsuk.vector_distance("cosine", [1.0, 0.0], [1.0, 0.0])
borsuk.string_distance("jaro-winkler", "segment", "segments")
```

## Development

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets
cargo bench --locked --workspace --no-run
maturin develop --manifest-path crates/borsuk-python/Cargo.toml
PYTHONPATH=python/src python -m unittest discover python/tests
(cd packages/borsuk && npm ci && npm run build:native && npm test)
```

Install hooks:

```bash
pre-commit install
```

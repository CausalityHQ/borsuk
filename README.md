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
- Arrow schema and FFI model with Parquet local-file and object-store storage
- append-only immutable segments, segment-local graph blocks, and binary
  manifest/routing/pivot tables
- out-of-place L0 to L1/L2 compaction and explicit obsolete-segment GC
- exact search with segment lower-bound pruning where the metric supports it
- budgeted approximate search with segment, byte, latency, and per-segment
  candidate limits, compressed `pq-scan`/`sq-scan`, and bounded segment-local graph
  traversal
- optional local read-through cache for segment, graph, manifest, and routing
  objects
- search reports for Rust, Python, and TypeScript with segment, byte,
  cache-hit/miss, exact-scoring, and resident-routing-memory counters
- manifest-derived index stats for Rust, Python, and TypeScript covering active
  records, segments, segment/graph bytes, resident metadata, and RAM budget
- broad dense-vector metrics, including Euclidean, cosine, inner product,
  angular, L1/L-infinity, Minkowski, histogram/distribution distances, set-like
  and binary coefficient distances exposed through Rust, Python, and TypeScript
- CI, publish workflow, pre-commit hooks, example, benchmark target, and docs

## Rust Quick Start

```rust
use borsuk::{BorsukIndex, IndexConfig, LeafMode, SearchOptions, VectorMetric};

fn main() -> borsuk::Result<()> {
    let mut index = BorsukIndex::create(IndexConfig {
        uri: "file:///tmp/docs-index".to_string(),
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors: 1024,
        ram_budget_bytes: None,
    })?;

    index.add_vectors_with_ids(
        vec![vec![0.0, 0.0], vec![1.0, 0.0]],
        vec!["a".to_string(), "b".to_string()],
    )?;

    let ids = index.search_ids(&[0.1, 0.0], SearchOptions::exact(1))?;
    let vectors = index.search_vectors(&[0.1, 0.0], SearchOptions::exact(1))?;
    let vector = index.get_vector("a")?;
    let approx = index.search_with_report(
        &[0.1, 0.0],
        SearchOptions::approx(1, LeafMode::VamanaPq)
            .with_max_candidates_per_segment(64),
    )?;
    println!("{ids:?} {vectors:?} {vector:?} {:?}", approx.hits);
    Ok(())
}
```

Record ids must be unique. Python and TypeScript `add` calls can omit ids; in
that case BORSUK returns generated ids that skip existing caller-supplied
numeric ids.

## Examples

- Rust: [`crates/borsuk/examples/local_index.rs`](crates/borsuk/examples/local_index.rs)
- Rust S3-compatible: [`crates/borsuk/examples/s3_index.rs`](crates/borsuk/examples/s3_index.rs)
- Python: [`python/examples/local_index.py`](python/examples/local_index.py)
- Python S3-compatible: [`python/examples/s3_index.py`](python/examples/s3_index.py)
- TypeScript: [`packages/borsuk/examples/local-index.ts`](packages/borsuk/examples/local-index.ts)
- TypeScript S3-compatible: [`packages/borsuk/examples/s3-index.ts`](packages/borsuk/examples/s3-index.ts)
- SeaweedFS S3-compatible: [`examples/seaweedfs`](examples/seaweedfs/README.md)

## Package Support Matrix

CI builds and tests the Python package on Python 3.12, 3.13, and 3.14 across
Linux, Windows, macOS arm64, and macOS Intel runners. The Python package
metadata requires Python 3.12 or newer.

CI builds and tests the TypeScript/Node package on Node 22, 24, and 26 across
Linux, Windows, macOS arm64, and macOS Intel runners. The npm package declares
`node >=22 <27` because these are the maintained Node lines targeted by the
native N-API package.

## Current Status

BORSUK is not yet a production ANN system. The current code is a Phase 0/1
baseline being moved from a custom segment prototype to the design target:
Arrow schemas, Parquet durable storage, PyO3 Python bindings, and N-API
TypeScript bindings. Local files and S3-compatible object storage use the same
binary Parquet table layout through the Rust `object_store` backend. All
durable index tables except the fixed binary `CURRENT` pointer are Parquet,
including manifests, segment summaries, pivot/routing tables, segment payloads,
and graph blocks. Avro and Protobuf are reserved only for future non-index
append logs or control-plane messages, not vector/index persistence or
Python/TypeScript FFI payloads. Basic
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

With `BORSUK_S3_TEST_URI` and the AWS/object-store environment variables set,
run the Python and TypeScript S3 examples directly:

```bash
cargo run --locked -p borsuk --example s3_index
(cd python && python examples/s3_index.py)
(cd packages/borsuk && npm run example:s3)
```

For a local S3-compatible stack, see
[`examples/seaweedfs`](examples/seaweedfs/README.md). It starts SeaweedFS with
the S3 API enabled and runs the same integration test against
`http://127.0.0.1:8333`.

For blob-backed indexes, pass a local cache directory from Rust, Python, or
TypeScript to keep fetched immutable objects on local NVMe:

```python
idx = borsuk.open(
    "s3://my-bucket/indexes/docs-index",
    cache_dir="/mnt/nvme/borsuk-cache",
    ram_budget="2GB",
)
```

The CLI is only for administration/debugging, but it can inspect an index
without becoming a runtime bridge:

```bash
borsuk stats --uri file:///tmp/docs-index
borsuk search --uri file:///tmp/docs-index --query '[0.1,0.0]' --report
borsuk search --uri s3://my-bucket/indexes/docs-index --query '[0.1,0.0]' --cache-dir /mnt/nvme/borsuk-cache --report
```

Metric helpers are available without building an index:

```python
borsuk.vector_metric_names()
borsuk.leaf_mode_names()  # ["flat-scan", "sq-scan", "pq-scan", "graph", "vamana-pq", "hybrid"]
borsuk.minkowski_metric(3)
borsuk.vector_distance(borsuk.VectorMetricName.COSINE, [1.0, 0.0], [1.0, 0.0])
borsuk.recall_at_k(["doc-a", "doc-b"], ["doc-b", "doc-x"], 2)
```

## Development

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets
cargo package --locked -p borsuk --allow-dirty
cargo bench --locked --workspace --no-run
(cd python && uvx maturin build --locked --out dist)
wheel="$(ls -t python/dist/borsuk-*.whl | head -1)"
BORSUK_WHEEL_PATH="$wheel" uv run --with "./$wheel" python -m unittest discover python/tests
(cd packages/borsuk && npm ci && npm run build:native && npm test)
```

Install hooks:

```bash
pre-commit install
```

## License

BORSUK is licensed under the Business Source License 1.1 with a revenue-limited
Additional Use Grant: free production use unless your company, organization,
and affiliates make over US $100,000/year. See [LICENSE](LICENSE).

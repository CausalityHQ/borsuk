# BORSUK Python

Native Python API for BORSUK, backed by the Rust core through PyO3 and
maturin. The package imports a compiled extension module and does not shell out
to the `borsuk` CLI for runtime search, indexing, compaction, or storage I/O.

## Install From Source

```bash
uvx maturin build --locked --out dist
wheel="$(ls -t dist/borsuk-*.whl | head -1)"
uv run --with "./$wheel" python examples/local_index.py
```

For development:

```bash
uvx maturin develop --manifest-path ../crates/borsuk-python/Cargo.toml
python -m unittest discover tests
```

## Local Files

```python
import borsuk
from array import array

index = borsuk.create(
    uri="file:///tmp/docs.borsuk",
    metric="euclidean",
    dimensions=2,
    segment_size=1024,
    ram_budget="1GB",
)
index.add(["a", "b"], [[0.0, 0.0], [1.0, 0.0]])
index.add_buffer(["c", "d"], array("f", [2.0, 0.0, 3.0, 0.0]))
hits = index.search([0.1, 0.0], k=1)
batch_hits = index.search_batch_buffer(array("f", [0.1, 0.0, 2.9, 0.0]), k=1)
print(hits[0].id, hits[0].distance)
```

## S3-Compatible Storage

Use `s3://bucket/prefix` for AWS S3, MinIO, SeaweedFS, or another
S3-compatible object store. Configure credentials and endpoints with the usual
`AWS_*` environment variables.

```bash
export AWS_ENDPOINT=http://127.0.0.1:8333
export AWS_ALLOW_HTTP=true
export AWS_ACCESS_KEY_ID=borsuk
export AWS_SECRET_ACCESS_KEY=borsuk-secret
export AWS_REGION=us-east-1
export AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false
export BORSUK_S3_TEST_URI=s3://borsuk-test/indexes

python examples/s3_index.py
```

`cache_dir` keeps fetched immutable segment, graph, manifest, and routing
objects on local storage while the durable index remains in the object store.

## Formats And Budgets

BORSUK persists durable index data as Arrow-schema Parquet tables plus a small
fixed binary `CURRENT` pointer. JSON is only for human-facing tooling.
`Index.add_buffer` accepts contiguous float32 buffers such as `array("f")` for
bulk ingest without nested Python row lists. `Index.search_batch_buffer` accepts
the same flat row-major float32 layout for multiple queries. Future bulk APIs
should use Arrow-compatible batches; Avro and Protobuf are not Python runtime
payload formats for vector/index data.

Approximate-search budgets such as `max_segments`, `max_bytes`,
`max_latency_ms`, and `max_candidates_per_segment` must be greater than zero
when set. `eps` must be finite and non-negative.

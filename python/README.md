# BORSUK Python

Native Python API for BORSUK, backed by the Rust core through PyO3 and
maturin. The package imports a compiled extension module and does not shell out
to the `borsuk` CLI for runtime search, indexing, compaction, or storage I/O.

Supported Python versions are 3.12, 3.13, and 3.14 on Linux x64, Linux arm64,
Windows x64, macOS arm64, and macOS Intel runners. The package metadata
requires Python 3.12 or newer.

## Install From Source

```bash
uvx maturin build --locked --out dist
wheel="$(ls -t dist/borsuk-*.whl | head -1)"
uv run --with "./$wheel" python examples/local_index.py
```

For development:

```bash
uvx maturin develop --locked
python -m unittest discover tests
```

## Local Files

```python
import borsuk
from array import array

index = borsuk.create(
    uri="file:///tmp/docs-index",
    metric=borsuk.VectorMetricName.EUCLIDEAN,
    dimensions=2,
    segment_size=1024,
    ram_budget="1GB",
)
index.add([[0.0, 0.0], [1.0, 0.0]], ids=["a", "b"])
index.add_buffer(array("f", [2.0, 0.0, 3.0, 0.0]), ids=["c", "d"])
ids = index.search_ids([0.1, 0.0], k=1)
vectors = index.search_vectors([0.1, 0.0], k=1)
vector = index.get_vector("a")
buffer_ids = index.search_ids_buffer(array("f", [0.1, 0.0]), k=1)
buffer_vectors = index.search_vectors_buffer(array("f", [0.1, 0.0]), k=1)
report = index.search_with_report_buffer(array("f", [0.1, 0.0]), k=1)
batch_ids = index.search_ids_batch_buffer(array("f", [0.1, 0.0, 2.9, 0.0]), k=1)
batch_vectors = index.search_vectors_batch_buffer(array("f", [0.1, 0.0, 2.9, 0.0]), k=1)
batch_reports = index.search_batch_with_report_buffer(array("f", [0.1, 0.0, 2.9, 0.0]), k=1)
vector_metrics = borsuk.vector_metric_names()
leaf_modes = borsuk.leaf_mode_names()
print(ids, vectors, vector, buffer_ids, buffer_vectors, batch_ids, batch_vectors, report.hits[0].distance)
```

Record ids must be unique. If `ids` is omitted, BORSUK returns generated string
ids that skip existing caller-supplied decimal-string ids. Explicit ids may be
`str`, `bytes`, or non-negative `int`; integer ids are encoded as compact
unsigned varint bytes, and `search_id_bytes` returns those canonical bytes.

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
Opens read `CURRENT` from backing storage and use its checksums to refetch
stale or corrupt cached active metadata tables automatically.
Cached segment, graph, and routing page payloads are also checksum-validated
and repaired from backing storage when only the local cache copy is corrupt.

## Formats And Budgets

BORSUK persists durable index data as Arrow-schema Parquet tables plus a small
fixed binary `CURRENT` pointer. JSON is only for human-facing tooling.
`Index.add_buffer` accepts contiguous float32 buffers such as `array("f")` for
bulk ingest without nested Python row lists. `Index.search_ids_buffer` and
`Index.search_vectors_buffer` accept one flat float32 query. `Index.search_ids_batch`,
`Index.search_vectors_batch`, `Index.search_ids_batch_buffer`, and
`Index.search_vectors_batch_buffer` search multiple queries without returning
hit objects. `Index.search_with_report_buffer` accepts one flat float32 query
and returns the same counters as `search_with_report`. Report hits expose
`id_bytes` for arbitrary binary or integer-encoded ids; non-UTF8 ids use a
`0x...` display string in `id`.
`Index.search_batch_with_report_buffer` returns one report per row-major query.
Future bulk APIs should use
Arrow-compatible batches; Avro and Protobuf are not Python runtime payload
formats for vector/index data.

The Python package ships `py.typed` and typed stubs. Use
`VectorMetricName`, `SearchMode`, and `LeafModeName` enums for typed config
values. Use `minkowski_metric(p)` for parameterized Minkowski configs.
`vector_metric_names()` and `leaf_mode_names()` expose the canonical runtime
catalogs. Implemented leaf modes are `flat-scan`, `sq-scan`, `pq-scan`,
`graph`, `vamana-pq`, and `hybrid`.

Approximate-search budgets such as `max_segments`, `max_bytes`,
`max_latency_ms`, and `max_candidates_per_segment` must be greater than zero
when set. `eps` must be finite and non-negative.

Open large object-store indexes with `resident_routing=False` to keep segment
summaries and pivots out of the resident manifest and resolve summaries from
routing pages:

```python
index = borsuk.open("s3://bucket/index", resident_routing=False, ram_budget="512MB")
```

`Index.compact()` uses a bounded source-segment batch by default. Pass
`max_segments` to tune incremental compaction, and keep
`min_segments <= max_segments` when both are set. It reads the selected source
leaf payloads plus needed routing metadata, rebuilds graph blocks from those
records, and leaves unrelated leaves and old graph payloads unread.

Use `Index.rebuild(source_level=0, target_level=1, delete_obsolete=True)` for
an explicit full matching-level rewrite followed by obsolete segment/graph
cleanup. Without `delete_obsolete=True`, rebuild reports garbage-collection
candidates but keeps old objects.

## License

The Python package is distributed under the Business Source License 1.1 with a
revenue-limited Additional Use Grant. See `LICENSE`.

# Storage Format Decision

BORSUK should use a two-layer format strategy:

- **Arrow** for in-memory arrays, record batches, FFI boundaries, and schemas.
- **Parquet** for durable local-file and blob/object-store tables.

This is the best fit for low-RAM ANN over local files and S3-compatible storage
because BORSUK needs column projection, row-group reads, compression, typed
vector columns, broad Python/Rust/TypeScript ecosystem support, and predictable
large-object access.

## Decision Matrix

| Format | Use in BORSUK | Reason |
|---|---|---|
| Arrow | In-memory model, schema contract, and FFI ABI | Language-independent columnar memory format with efficient cross-language data exchange |
| Parquet | Canonical durable tables | Column-oriented storage format designed for efficient storage/retrieval, compression, projection, and row-group/range access |
| Arrow IPC/Feather | Optional diagnostics/interchange | Useful for local inspection and tests, but not the durable object-store format |
| Avro | Not for index/vector storage | Compact binary serialization and container files; useful for optional streaming ingest logs if needed |
| Protobuf | Not for index/vector storage | Good for small RPC/control messages; not a table/columnar storage format |

Arrow IPC/Feather is not the canonical durable index format. It is useful for
local interchange and tests, but Parquet is the format that gives BORSUK
compressed column chunks, row groups, footers, statistics, projection, and
object-store-friendly range reads.

## Boundary Rules

The same Arrow schemas define data at every boundary, but the physical format
depends on where the data lives:

```text
in-process Rust/Python/Node batch data    Arrow-compatible arrays/buffers
published durable local/blob objects      Parquet tables
active manifest pointer                   fixed binary CURRENT record
future network control plane              optional Protobuf messages
future append-only ingest journal         optional Avro container files
```

Avro and Protobuf are intentionally excluded from canonical index persistence.
They can encode rows or messages compactly, but BORSUK queries need to project
columns, skip row groups, read object ranges, and preserve vector/routing/graph
tables in an analytics-compatible layout.

Python and TypeScript bindings should not use a Rust CLI subprocess or
JSON-over-stdin/stdout transport. They should call the Rust core through native
FFI and pass vectors as contiguous numeric buffers now, with Arrow-compatible
record batch APIs available as the batch interface grows.

## Durable Tables

All durable BORSUK tables should be binary and efficient:

```text
CURRENT                         fixed binary pointer record
manifests/manifest-*.parquet    manifest/config/version rows
routing/segments-*.parquet      segment summary rows
routing/pivots-*.parquet        pivot/router rows
segments/L*/xx/seg-*.parquet    immutable vector/sketch/payload rows
graphs/L*/xx/graph-*.parquet    segment-local graph edge rows
objects/shard-*.parquet         optional payload/object rows
```

JSON is acceptable only for developer fixtures, tests, examples, or human
debugging exports, not as the persisted index format.

Current segment rows include:

```text
record_id
vector
routing_code
```

`routing_code` is a compact scalar sketch used by approximate search to choose
entry rows inside a fetched segment before exact distance scoring. It is
intentionally small and durable; richer pivot sketches can be added as
additional Parquet columns/tables without changing the Arrow/Parquet format
decision.

Current graph rows include:

```text
segment_id
source_record_id
neighbor_record_id
neighbor_distance
```

Graph blocks are rebuilt out-of-place with their segments during compaction,
referenced from the active routing summary table, and used for bounded
query-guided candidate traversal in approximate search.

## Source Notes

- [Apache Arrow](https://arrow.apache.org/) describes Arrow as a
  language-independent columnar memory format for efficient analytic operations
  and zero-copy reads.
- [Apache Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
  defines a small ABI-stable interface for sharing Arrow data across runtimes
  without adding another marshalling layer.
- [Apache Parquet](https://parquet.apache.org/) describes Parquet as a
  column-oriented data file format for efficient storage and retrieval with
  high-performance compression/encoding.
- [Apache Avro](https://avro.apache.org/docs/) describes Avro as a compact
  binary data serialization system with a container file and strong schema
  evolution.
- [Protocol Buffers](https://protobuf.dev/overview/) describe Protobuf as a
  language-neutral structured-data serialization mechanism, suited to compact
  messages and generated bindings.

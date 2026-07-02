# Storage And FFI Format Decision

BORSUK uses one canonical storage/output strategy:

- **Arrow** for schemas, in-memory arrays, record batches, and FFI boundaries.
- **Parquet** for every durable local-file and blob/object-store table.
- **Fixed binary `CURRENT`** as the only non-Parquet persistent object.

For this use case, Arrow + Parquet is the canonical choice. Avro and Protobuf
are useful formats, but they are not acceptable substitutes for BORSUK's
persisted vector, graph, routing, manifest, or payload output.

This is the best fit for low-RAM ANN over local files and S3-compatible storage
because BORSUK needs column projection, row-group reads, compression, typed
vector columns, broad Python/Rust/TypeScript ecosystem support, and predictable
large-object access.

The short rule is:

```text
Use Arrow for the schema and in-process/bulk FFI shape.
Use Parquet for persisted output and every durable table.
Do not use Avro or Protobuf for vector/index output.
```

There is no small JSON manifest exception. Manifests, segment summaries,
pivots, routing rows, segment vectors, graph blocks, and optional payload
shards are binary Parquet tables. JSON may be emitted by tools for people, but
it is not an index format and not a runtime API contract.

## Decision Matrix

| Format | Use in BORSUK | Reason |
|---|---|---|
| Arrow | In-memory model, schema contract, and FFI ABI | Language-independent columnar memory format with efficient cross-language data exchange |
| Parquet | Canonical durable tables | Column-oriented storage format designed for efficient storage/retrieval, compression, projection, and row-group/range access |
| Arrow IPC/Feather | Optional diagnostics/interchange | Useful for local inspection and tests, but not the durable object-store format |
| Avro | Not for index/vector storage | Compact binary serialization and container files; useful for optional streaming ingest logs if needed, but not for segment scans |
| Protobuf | Not for index/vector storage | Good for small RPC/control messages; not a table/columnar storage format and a poor fit for large multidimensional numeric arrays |

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

Published index output uses Parquet. Query/API output may be native language
objects for scalar calls today and Arrow-compatible record batches for bulk
calls later. The CLI may print JSON for administrator convenience, but that JSON
is not a storage or runtime API contract.

The word "output" therefore has three separate meanings:

```text
durable index output       Parquet tables, plus fixed binary CURRENT
library/API query output   native objects now, Arrow-compatible batches for bulk APIs
CLI/admin output           JSON allowed only for human-readable tooling
```

Avro and Protobuf are intentionally excluded from canonical index persistence.
They can encode rows or messages compactly, but BORSUK queries need to project
columns, skip row groups, read object ranges, and preserve vector/routing/graph
tables in an analytics-compatible layout.

## Native FFI Rules

Python and TypeScript bindings should not use a Rust CLI subprocess or
JSON-over-stdin/stdout transport. The CLI is administration/debug tooling, not
an embedding ABI.

Python should import the Rust core as a PyO3/maturin native extension.
TypeScript/Node should load the Rust core as an N-API native addon. Both
bindings should keep operations coarse-grained: create/open/add/search/compact
and GC cross the boundary, while row-by-row vector, graph-node, and object-read
calls stay inside Rust.

Current bindings can pass vectors as contiguous numeric buffers or memory views.
Future bulk APIs should expose Arrow-compatible record batches, preferably via
the Arrow C Data Interface where a stable cross-runtime ABI is needed. They
should not introduce Avro, Protobuf, JSON, or subprocess streams as the data
plane between Python/TypeScript and Rust.

## Durable Tables

All durable BORSUK tables should be binary and efficient:

```text
CURRENT                         fixed binary pointer record with metadata checksum
manifests/manifest-*.parquet    manifest/config/version rows
routing/segments-*.parquet      segment summary rows
routing/pivots-*.parquet        centroid-derived pivot/router rows
segments/L*/xx/seg-*.parquet    immutable vector/sketch/payload rows
graphs/L*/xx/graph-*.parquet    segment-local graph edge rows
objects/shard-*.parquet         optional payload/object rows
```

JSON is acceptable only for developer fixtures, tests, examples, or human
debugging exports, not as the persisted index format.

`CURRENT` contains a magic header, pointer-format version, active manifest
version, and BLAKE3 checksum over the active manifest, segment-summary routing,
and pivot routing Parquet tables. It lets readers reject a swapped or stale
metadata table before returning an index handle.

Current segment rows include:

```text
record_id
payload_ref
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

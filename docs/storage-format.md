# Storage And FFI Format Decision

BORSUK uses one canonical storage/output strategy:

- **Arrow** for schemas, in-memory arrays, record batches, and FFI boundaries.
- **Parquet** for every durable local-file and blob/object-store table.
- **Fixed binary `CURRENT`** as the only non-Parquet persistent object.

For this use case, Arrow + Parquet is the canonical choice. Avro and Protobuf
are useful formats, but they are not acceptable substitutes for BORSUK's
persisted vector, graph, routing, manifest, or record output.

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

For BORSUK's output use case, the right answer is split by boundary rather
than choosing one universal serialization format:

```text
published index output     Parquet tables, governed by Arrow schemas
bulk FFI/API output        Arrow-compatible arrays or record batches
human CLI/admin output     JSON allowed for inspection only
```

That means Parquet is the durable binary output format. Arrow is the schema and
memory ABI that keeps Rust, Python, and TypeScript aligned. Avro and Protobuf
remain outside vector/index output because they optimize row or message
serialization, not projected scans, row-group skipping, vector columns, or
object-store range reads.

There is no small JSON manifest exception. Manifests, segment summaries,
pivots, routing rows, segment records, and graph blocks are binary Parquet
tables. JSON may be emitted by tools for people, but it is not an index format
and not a runtime API contract.

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
CURRENT                         fixed binary pointer record with metadata checksums
manifests/manifest-*.parquet    manifest/config/version rows
routing/segments-*.parquet      segment summary rows, including blooms and leaf_mode
routing/pivots-*.parquet        centroid-derived pivot/router rows
segments/L*/xx/seg-*.parquet    immutable record id, vector, and sketch rows
graphs/L*/xx/graph-*.parquet    segment-local graph edge rows
```

JSON is acceptable only for developer fixtures, tests, examples, or human
debugging exports, not as the persisted index format.

`CURRENT` contains a magic header, pointer-format version, active manifest
version, and BLAKE3 checksums for the active manifest, segment-summary routing,
and pivot routing Parquet tables. Pointer v2 stores the per-table checksums
directly, so paged-routing opens can validate only the manifest table without
fetching large `routing/segments-*` or `routing/pivots-*` objects. Resident
opens still validate every referenced metadata table before returning an index
handle. Pointer v1 is accepted for existing indexes and validates the legacy
combined metadata checksum by reading all three metadata tables.

The local read-through cache is not an authority for active metadata. Opens
always fetch `CURRENT` from backing storage. For pointer v2 indexes, cached
manifest, segment-summary routing, and pivot metadata tables are accepted only
when their BLAKE3 table checksums match `CURRENT`; otherwise the cache entry is
deleted, the object is refetched, and the replacement is validated before use.
Segment payloads, graph payloads, and routing page payloads are immutable and
validated against their persisted checksums on every read. If the local cache
copy fails that checksum, it is discarded and refetched; if backing storage
fails the checksum, the read fails.

Manifest rows also store `next_generated_id`, a monotonic counter used by add
paths that omit ids and return decimal-string convenience ids. Explicit
decimal-string ids advance the counter when the manifest is published, so
generated string ids remain collision-free without loading old segment payloads
into RAM. Explicit binary and integer ids are duplicate-checked by their
canonical stored bytes and do not share the decimal-string generated-id counter.

IDs should be compact. Production-scale callers should prefer explicit compact
integer ids, hashes, fixed-width keys, or application-native byte ids over long
object keys. User-supplied ids are arbitrary binary bytes, not UTF-8-only
strings, so these compact forms avoid inflating every routing and graph
structure.

Older manifest tables without `next_generated_id` are still readable. During
open, BORSUK derives the missing counter by scanning existing segment ids once
and then publishes future manifests with the counter, so generated-id adds keep
skipping caller-supplied decimal-string ids without repeatedly scanning segment
payloads.

Segment-summary rows store fixed-size `id_bloom` and
`vector_signature_bloom` binary columns plus a typed `leaf_mode` string column.
`id_bloom` is a negative filter for id lookups: when the bloom says an id is
definitely absent, explicit duplicate-id validation and `get_vector(id)` skip
that segment without reading the segment Parquet object.
`vector_signature_bloom` stores hashes of quantized vectors in the segment.
Budgeted approximate search uses it as a cheap priority signal before fetching
segment objects: segments that may contain a vector with the same signature as
the query are tried before lower-bound ties that definitely cannot. It is not a
correctness filter; exact search and epsilon-bound approximate search still use
the metric lower-bound order. `leaf_mode` declares the segment-local leaf engine
represented by the summary: current L0 insert segments use `graph`, while
compacted L1+ segments declare `vamana-pq`. Older routing tables without these
columns are still readable; missing `id_bloom` falls back to scanning candidate
segment payloads for id lookups and duplicate checks, missing
`vector_signature_bloom` falls back to lower-bound-only approximate routing, and
missing `leaf_mode` defaults to `graph`.

Current segment rows include:

```text
record_id
vector
routing_code
pq_code
```

New segment and vector-record Parquet files store `record_id` as binary bytes.
Readers still accept legacy UTF-8 `record_id` columns for compatibility, and
current Python/TypeScript convenience APIs expose ids as strings. The storage
target is a binary `record_id` plus dense internal row ids for graph and lookup
structures. Smaller ids reduce segment size, bloom work, lookup indexes, and
query result payloads.

`routing_code` is a compact scalar sketch used by approximate search to choose
entry rows inside a fetched segment before exact distance scoring. It is
intentionally small and durable; richer pivot sketches can be added as
additional Parquet columns/tables without changing the Arrow/Parquet format
decision.

`pq_code` is a fixed-size UInt8 list with one quantized coordinate per vector
dimension. `pq-scan` uses it for vector-shaped compressed candidate ranking
inside fetched segments before exact rerank, while `sq-scan` continues to use
the scalar `routing_code` path. Older segment tables without `pq_code` remain
readable; BORSUK derives equivalent codes from the exact vectors after loading
the segment.

Current graph rows include:

```text
segment_id
source_record_index
neighbor_record_index
neighbor_distance
```

Graph blocks are rebuilt out-of-place with their segments during compaction,
referenced from the active routing summary table, and used for bounded
query-guided candidate traversal in approximate search.

Compaction should treat graph blocks as derived data. A scoped compaction reads
the selected source leaf payloads, rebuilds graph blocks for the new leaves, and
leaves unrelated graph objects untouched until garbage collection. It should not
read old graph blocks just to rewrite a leaf. Omitted compaction batch settings
use the bounded default source-leaf count; whole-level/all-matching compaction is
an explicit offline choice.

Graph rows reference segment-local numeric row ids instead of external ids. That
prevents long external ids from being repeated once per edge and keeps leaf
graph blocks small enough for high-parallelism S3 queries. Older graph tables
with `source_record_id` and `neighbor_record_id` remain readable; the reader maps
those legacy ids to local row indices after loading the segment payload.

## Routing Layers

The current default manifest still publishes a full segment-summary routing
table for compatibility, but query routing can operate from binary routing
pages when that full table is empty. Each publish writes a versioned page-index
table under `routing/layers/<version>/L0/pages.parquet`. The index points at
immutable, content-addressed Parquet page objects under `routing/pages/L0/`.
Page-index rows include page centroid/radius metadata, persisted per-dimension
vector bounds, page-level id bloom, a `level_mask` for source-level pruning,
aggregate byte/record counters, and `leaf_segments`, the number of L0 segment
summaries covered below that row.
Publish rolls leaf page refs into parent routing page objects under
`routing/pages/L1/`, recursively writes higher parent indexes while each layer
has more than one page, and stores the highest layer in the manifest as
`routing_max_level`.

Paged approximate search starts from `routing_max_level`, ranks page refs by
vector-bound lower bound and `leaf_segments`, reads an overfetch of selected
routing metadata pages, and descends until it reaches selected L0 routing
pages. The overfetch applies to routing metadata only; the later search loop
still enforces the caller's segment-payload budget. It does not need the global
L0 page index when a parent layer exists. `get_vector` can filter page objects
by id bloom, decode only candidate routing pages, and then use
segment-level blooms before reading segment payloads.

When normal `add` runs with an empty resident segment-summary table, it appends
new L0 routing page objects and republishes the page index with existing page
refs reused. Generated-id appends do not decode old routing pages. Explicit-id
appends decode only page-bloom and segment-bloom candidates to reject duplicate
ids before writing new segment objects.

Garbage collection also treats routing page metadata as active-object metadata.
When the full `routing/segments-*.parquet` table is empty, GC reads the active
page index and leaf routing pages to collect referenced segment and graph paths
before it considers any object obsolete. It does not read segment payloads or
graph payloads for this protection step.

Scoped compaction uses the same routing page tree to choose source leaves
whenever the active version has routing pages, even if the index handle was
opened with resident summaries. It starts from `routing_max_level`, uses
page-level `level_mask` and `leaf_segments` to descend only into candidate
parent pages, decodes only enough L0 routing pages to satisfy the requested
batch, and only then reads selected segment payload objects. Replacement graph
blocks are derived from those records. Unselected segment payloads, graph
payloads, unrelated target-level leaves, and unrelated routing page payloads
stay unread. The default bounded source-leaf count is the online maintenance
path; unbounded compaction is an explicit offline rebuild-style choice because
it must touch every matching source leaf. Publishing the compaction leaves the
active manifest's segment-summary table empty so later
search, add, stats, GC, and compaction operations stay page-backed. If the
replacement summaries fit inside the dirty leaf routing pages, publishing
rewrites only the dirty leaf pages, the parent pages on those branches, and the
new top routing page index. If a compaction creates additional leaf routing
pages, the publish path chooses new leaf ordinals from decoded dirty-branch
metadata and treats uncached sibling subtrees as reserved ranges. It writes the
appended leaf pages and rewrites only the dirty and append parent branches plus
the top routing page index. It does not reconstruct every leaf ref, does not
read unrelated append/rightmost branches, and does not read the global L0 page
index.

Page indexes also store aggregate `page_records`, `page_segment_bytes`,
`page_graph_bytes`, and `leaf_segments` counters. `IndexStats` sums those
top-level page-index columns when the resident segment-summary table is empty,
so tuning counters stay accurate without loading segment payloads, graph
payloads, or routing page payloads.

```text
routing/layers/<version>/L0/pages.parquet   versioned page index with bounds/centroid/id_bloom/level_mask/leaf_segments/totals
routing/pages/L0/<hash>/page-*.parquet      immutable leaf-level summaries
routing/layers/<version>/L1/pages.parquet   parent page index
routing/pages/L1/<hash>/page-*.parquet      parent routing pages
```

The production layer count is derived from leaf count and routing fanout during
publish and persisted in the manifest. Queries and compaction candidate
selection walk routing pages from the top layer to leaves, then fetch only
selected segment and graph objects. Leaf size remains bounded; higher levels
are compact routing records, not larger vector payload blobs.

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

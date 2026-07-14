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

## Plain Routing Model

The production shape is not one map plus many vector boxes. That works only as
a small-index mental model. At large scale, BORSUK uses a map of maps over
bounded vector boxes:

```text
top routing page
  parent routing pages
    leaf routing pages
      bounded vector segment blobs
      bounded leaf graph blobs
```

The upper layers contain only compact routing metadata: bounds, centroids,
blooms, counters, and child page references. They do not contain vectors. A
query walks from the top routing layer to a small set of leaf routing pages,
then fetches only the selected vector and graph blobs. This is the mechanism
that keeps S3/object-store reads bounded and keeps process memory close to the
query buffers instead of the full index.

The layer count is controlled by `routing_page_fanout` and by how many leaf
pages exist. Publishing and compaction compute the required depth and persist it
in the manifest. Small indexes may have one routing level; very large
indexes should naturally grow more parent layers without changing the vector
blob size or requiring a full resident routing table.

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
routing/segments-*.parquet      segment summary rows, including blooms, leaf_mode, and metadata stats
routing/pivots-*.parquet        centroid-derived pivot/router rows
segments/L*/xx/seg-*.parquet    immutable record id, vector, sketch, and metadata rows
graphs/L*/xx/graph-*.parquet    segment-local graph edge rows
```

JSON is acceptable only for developer fixtures, tests, examples, or human
debugging exports, not as the persisted index format.

Per-record **metadata** is one additional binary column on the segment payload,
encoded with a compact typed codec (tag byte plus LEB128 varints, zigzag for
signed integers) rather than JSON — consistent with the no-JSON storage rule.
Each segment summary carries a derived **metadata statistics** blob: per dotted
path numeric min/max plus a presence bloom over string values and value kinds,
bounded to a fixed number of paths. Readers use it to prune whole segments
against a query filter before fetching any payload. Both are additive columns:
the metadata payload column round-trips through compaction, and same-major
readers that predate them simply ignore the columns and see empty metadata.

`CURRENT` contains a magic header, pointer-format version, active manifest
version, and BLAKE3 checksums for the active manifest, segment-summary routing,
and pivot routing Parquet tables. Pointer v2 stores the per-table checksums
directly, so paged-routing opens can validate only the manifest table without
fetching large `routing/segments-*` or `routing/pivots-*` objects. Resident
opens still validate every referenced metadata table before returning an index
handle. Pointer v1 is accepted for existing indexes and validates the legacy
combined metadata checksum by reading all three metadata tables.

Publishes are optimistic and single-winner per manifest version. Writers first
write immutable segment and graph payloads, then routing page content, then
versioned routing layer indexes, manifest, routing, and pivot tables with
conditional create semantics. `CURRENT` is written strictly last. If another
writer already occupied the candidate version namespace, the loser gets a typed
`concurrent_modification` error and refreshes `CURRENT`.

If `CURRENT` is unchanged after a conflict, BORSUK treats the occupied namespace
as an orphan left by an interrupted publish and retries at the next unused
version after a short `CURRENT` re-check. This version-skip recovery keeps the
index writable after crashes before `CURRENT`. Strict pointer arbitration after a
version skip requires a backend that supports conditional `CURRENT` updates by
ETag/version, such as S3, Azure, GCS, or the in-memory test store. Local
filesystem storage supports conditional creates for versioned objects but not
conditional `CURRENT` updates, so concurrent multi-process writers on local files
are not a production-supported mode; use one writer or external locking there.

## Versioning Policy

Pointer-format version changes apply only to the fixed binary `CURRENT` record.
BORSUK bumps the pointer-format version when the `CURRENT` byte layout or
checksum contract changes in a way older readers cannot validate.

Table-format version changes apply to Parquet metadata tables such as
`manifests/manifest-*`, `routing/segments-*`, `routing/pivots-*`, and
`routing/layers/<version>/L*/pages.parquet`. BORSUK bumps the table-format
version when a metadata schema change is incompatible with same-major readers.

Same-major readers must ignore unknown columns in metadata tables and read known
columns by name. Additive columns must be written so older same-major readers can ignore them
while preserving all existing required column meanings. Removing a required
column, renaming a required column, changing a required column type, or changing
the meaning of an existing value requires a table-format version bump.

The current table-format version is **5**. Version 5 moved sparse named vectors
from a single global object into per-segment sidecars (one small content-
addressed object per segment, carrying record id, MVCC generation, and non-zero
`indices`/`values`), so they shard, commit atomically via `CURRENT`, and apply
generation-aware visibility like the dense and lexical legs.
Version 4 added a per-row
`generation` column to the BM25 sidecar so the lexical leg applies the same
generation-aware MVCC visibility as the dense leg — a re-upserted document is
searchable in text/hybrid queries immediately, not only after compaction.
Version 3 moved the sparse named-vector store and BM25 sidecar onto Parquet.
Version 2 was bumped from 1 when
`cosine`/`angular` indexes began storing their segment and routing bubble
geometry (centroid, radius, per-dimension bounds) as Euclidean geometry over
unit-L2-normalized vectors — that changed the *meaning* of existing values, so
per the rule above the version bumped. A pre-existing version-1 index is rejected
with a clear `unsupported manifest table version` error rather than being read
with the new interpretation; rebuild it with the current version. (The library is
pre-release, so there is no cross-version migration.)

## S3 assumptions and caveats

S3-compatible storage must provide read-after-write visibility for newly written
objects and list results that converge quickly enough for garbage collection.
Search and open paths read objects referenced by `CURRENT`; if the backend does
not make those writes visible before `CURRENT` is visible, readers can fail fast
with a typed storage error instead of returning partial results. GC discovers old
and orphaned objects by listing prefixes, so a backend with delayed listings may
require a longer retention window.

Publish concurrency is optimistic. Versioned routing indexes and
manifest/routing/pivot tables use conditional create, and `CURRENT` is updated
last. Same-version races produce `concurrent_modification`. Version-skip recovery
after an orphaned namespace relies on conditional `CURRENT` updates for strict
cross-version arbitration; S3, Azure, and GCS provide this through object
ETag/version support, while local filesystem storage is best used with one writer
or an external lock.

BORSUK does not add a second retry policy around cloud clients. S3, Azure, and
GCS retries are delegated to `object_store`'s built-in defaults. After those
backend retries are exhausted, BORSUK maps transient or generic store failures to
`object_store_retryable`, missing objects to `object_store_not_found`, and
authentication or authorization failures to `object_store_permission_denied`.
Search either returns complete results or one of these errors; it does not return
silently partial results after a failed segment, graph, or routing-page read.

Unconditional object writes larger than 64 MiB use multipart upload with fixed
8 MiB parts. Conditional publish objects keep single-request conditional writes
so create/update preconditions remain the concurrency boundary. Configure S3
lifecycle cleanup for abandoned multipart uploads according to your backend's
normal operational policy.

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

Manifest rows also carry the optional cumulative **tombstone** summary in
nullable columns: `tombstone_path`, `tombstone_checksum`, `tombstone_count`,
`tombstone_id_bloom`, and `tombstone_created_at_ms`. All null means nothing is
deleted. When present, they point at a content-addressed tombstone object under
`tombstones/<prefix>/tomb-<checksum>.parquet` holding a single binary
`record_id` column — the ids currently deleted. Keeping the bloom in the always-
loaded manifest table lets `search` and `get_vector` reject undeleted ids with no
extra fetch and pull the id list only on a bloom hit. `delete` republishes this
summary; compaction and `purge` drop tombstoned rows and clear the summary.

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
pq_min
pq_max
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

`pq_min` and `pq_max` are fixed-size Float32 lists holding the per-dimension
quantization bounds used to build `pq_code`. Persisting them lets a query be
quantized without the segment's full vectors, so pq-scan and sq-scan can decode
a segment column-projected (skipping the `vector` column) to select candidates
and then read back only the chosen candidates' vectors for exact rerank. This
bounds per-query decode memory to the candidate budget rather than the segment
size on large segments. Both are additive columns: older readers ignore them,
and segments written without them fall back to deriving bounds from the exact
vectors after loading.

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
`routing_max_level`. The same manifest stores `routing_page_fanout`; older
manifests without that column read as fanout 128.

Paged approximate search starts from `routing_max_level`, ranks page refs by
vector-bound lower bound and `leaf_segments`, reads an overfetch of selected
routing metadata pages, and descends until it reaches selected L0 routing
pages. At L0, overfetch also keeps close sibling metadata pages eligible even
when the first dense page already contains enough segment summaries for the
payload budget. Parent layers apply the same page-level floor to close sibling
branches. The overfetch applies to routing metadata only; the later search loop
still enforces the caller's segment-payload budget. It does not need the global
L0 page index when a parent layer exists. `get_vector` can filter page objects
by id bloom, decode only candidate routing pages, and then use segment-level
blooms before reading segment payloads.

When normal `add` runs with an empty resident segment-summary table, it appends
new L0 routing page objects and republishes the page index with existing page
refs reused. Generated-id appends do not decode old routing pages; they read
the top routing page index, allocate new L0 leaf ordinals after the existing
top-level span, and write only the new append branch plus the new top page
index. Repeated small appends decode only the readable rightmost append branch
to fill it before adding another parent branch. If that branch cannot be
decoded, append falls back to a new sparse branch instead of reading unrelated
cold parents. Explicit-id appends decode only page-bloom and segment-bloom
candidates to reject duplicate ids before writing new segment objects.

Garbage collection derives liveness from the retained manifest versions: the
version `CURRENT` points to, plus every earlier published version whose
superseding manifest table is still younger than the `min_age` retention
interval. For each retained version it protects that version's
manifest/routing/pivot tables, its existing layer indexes, all routing page
objects reachable from its top layer index, and all segment/graph payloads
referenced by its routing summaries. It then scans segment payloads, graph
payloads, `routing/pages/`, `routing/layers/`, `manifests/`, and the top-level
`routing/segments-*` / `routing/pivots-*` tables. Any Parquet object outside
the union of retained reference sets is reclaimable regardless of whether its
version is older or newer than `CURRENT`, so GC also reclaims publish-crash
orphans and skipped version namespaces once they age out. Listings are streamed
by prefix; the report retains only candidate paths.

Retention is obsolescence-based. An unreferenced object becomes a deletion
candidate only when it is at least `min_age` old and no retained version
references it, so an object compacted out of the active manifest stays
protected for at least `min_age` after it became unreachable, not merely
`min_age` after it was created. The default is 24 hours, which protects pinned
readers holding a recently superseded manifest snapshot and legitimate
in-flight publishes, including reused content-addressed routing page objects
that a publish references without re-putting. Passing `min_age = 0` disables
both protections and is intended for tests or externally quiesced maintenance
windows with no concurrent readers or writers.
The report separates total deletes from `routing_objects_deleted` and
`tables_deleted`; segment and graph deletes remain part of `objects_deleted`.

Scoped compaction uses the same routing page tree to choose source leaves
whenever the active version has routing pages, even if the index handle was
opened with resident summaries. It starts from `routing_max_level`, uses
page-level `level_mask` and `leaf_segments` to descend only into candidate
parent pages, decodes only enough L0 routing pages to satisfy the requested
batch, and stops before sibling L0 routing pages once the requested source batch
is full. Only then does it read selected segment payload objects. Replacement
graph blocks are derived from those records. Unselected segment payloads, graph
payloads, unrelated target-level leaves, and unrelated routing page payloads stay
unread. The default bounded source-leaf count is the online maintenance path;
unbounded compaction is an explicit offline rebuild-style choice because it must
touch every matching source leaf. Publishing the compaction leaves the active
manifest's segment-summary table empty so later
search, add, stats, GC, and compaction operations stay page-backed. If the
replacement summaries fit inside the dirty leaf routing pages, publishing
rewrites only the dirty leaf pages, patches their page refs by persisted
`page_ordinal`, rewrites the parent pages on those branches, and writes the new
top routing page index. If a compaction creates additional leaf routing pages,
the publish path chooses new leaf ordinals from decoded dirty-branch metadata
and treats uncached sibling subtrees as reserved ranges instead of reading them
to find holes. It writes the appended leaf pages and rewrites only the dirty and
append parent branches plus the top routing page index. If the new top index
would exceed routing fanout, the publish path promotes top refs into higher
parent routing pages using only the already available page-ref metadata. It does
not reconstruct every leaf ref, does not assume dense leaf ordinals, does not
read unrelated append/rightmost branches, does not decode unrelated parent page
bodies, and does not read the global L0 page index.

Page indexes also store aggregate `page_records`, `page_segment_bytes`,
`page_graph_bytes`, `leaf_segments`, `leaf_pages`, and `routing_pages`
counters. `IndexStats` sums those top-level page-index columns for payload and
topology totals when the resident segment-summary table is empty, so sparse
trees report the actual active leaf and parent page objects without parent-page
reads. Older page indexes without `leaf_pages` and `routing_pages` fall back to
walking parent routing page metadata for topology only. Stats still do not load
segment or graph payloads.

```text
routing/layers/<version>/L0/pages.parquet   versioned page index with bounds/centroid/id_bloom/level_mask/leaf_segments/leaf_pages/totals
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

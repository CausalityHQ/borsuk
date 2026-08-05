# Storage And FFI Format Decision

BORSUK uses one canonical storage/output strategy:

- **Arrow** for schemas, in-memory arrays, record batches, and FFI boundaries.
- **Parquet** for durable local-file and blob/object-store control, routing,
  graph, and lexical tables. It is also the default immutable normal-segment
  and cell-WAL table format.
- **Vortex compact layout** is a selectable normal-segment or cell-WAL codec in
  the versioned role policy. The frozen normal-segment and v5 cell-WAL
  qualifications rejected both placements, so Vortex is an explicit research
  override rather than an automatic production placement. It does not change
  manifests, routing, graph, sparse/BM25, or Arrow IPC sidecars.
- **Arrow IPC File** for the dense-vector exact-rerank sidecar. It is a standard
  random-access record-batch file with a footer, typed fixed-size vector arrays,
  and optional IPC V5 ZSTD buffer compression.
- **Arrow IPC File** for each global PQ/SRHT/TurboQuant ANN bundle. Every cell
  chunk is one record batch with a fixed-size `scan_payload` column (code plus
  packed row location), a binary `record_identity` column, and a typed
  `exact_vector` column. The `identity-v2` Parquet ANN descriptor persists the
  Arrow-derived padding to the identity offset and value buffers rather than
  inferring a fixed IPC alignment. Checked range reconstruction lets S3 reads
  fetch identities together with selected exact rows without downloading the
  scan payload or whole file. Exact rows retain the declared physical
  `f32/f16/bf16/i8/binary` width.
- Checked compact binary records for `CURRENT`, cell-lane heads/frontier nodes,
  transaction descriptors and commit markers, ID-directory runs, mutation
  metadata, and coordination counters.

Storage format v23 partitions each live-WAL ID-directory run by the BLAKE3
ID-hash logical cell recorded on the run. Insert-only duplicate validation reads
only the requested IDs' partitions and never decodes accumulated vector runs.
Format v22 artifacts are intentionally rejected: their directory runs used the
record bundle cell and cannot support the bounded lookup invariant.

For this use case, Arrow + Parquet is the canonical choice. Avro and Protobuf
are useful formats, but they are not acceptable substitutes for BORSUK's
persisted vector, graph, routing, manifest, or record output.

Exact dense vectors live only in the rerank sidecar, not in the selectable
normal-segment table. This split is unchanged by the segment-table experiment:
Parquet or Vortex carries ids, coarse codes, metadata, and sketches, while the
Arrow IPC sidecar carries lossless vectors for random-access rerank. See
[Two storage formats](#two-storage-formats).

This is the best fit for low-RAM ANN over local files and S3-compatible storage
because BORSUK needs column projection, row-group reads, compression, typed
vector columns, broad Python/Rust/TypeScript ecosystem support, and predictable
large-object access.

The short rule is:

```text
Use Arrow for the schema and in-process/bulk FFI shape.
Use the versioned role policy for persisted output.
Policy v3 uses Parquet for normal segments and WAL runs by default.
Use Vortex only as an explicit experiment; both frozen AWS gates rejected it.
Do not use Avro or Protobuf for vector/index output.
```

For BORSUK's output use case, the right answer is split by boundary rather
than choosing one universal serialization format:

```text
published index output     role-specific automatic policy; currently Parquet table baseline
bulk FFI/API output        Arrow-compatible arrays or record batches
human CLI/admin output     JSON allowed for inspection only
```

Parquet therefore remains the frozen default durable table format. Users do
not provide a collection row-count estimate. At each immutable-object write,
the engine already knows that object's actual row count, vector dimensions,
and declared element type; it resolves and persists the format from those
facts. Different objects in one live index may therefore use different
formats without a user setting.

The rejected WAL experiment is deterministic: runs below 500 rows, below 64
dimensions, or using a non-f32 primary type remain Parquet; f32 runs with at
least 500 rows and 64 dimensions may use compact Vortex only when a caller
explicitly selects the experimental rule. Campaign
`wal-layout-qualification-20260728-v5` completed all 220 paired cases and
rejected promotion because the end-to-end latency, confidence, CPU, RSS, and
storage gates did not all pass. One-record streaming writes remain immediately
durable as small Parquet runs; BORSUK does not delay acknowledgement merely to
accumulate a Vortex-sized batch. Normal-segment placement was qualified and
rejected independently.

Arrow remains the schema and memory ABI that keeps Rust, Python, and TypeScript
aligned. Avro and Protobuf remain outside vector/index output because they
optimize row or message serialization, not projected scans, row-group
skipping, vector columns, or object-store range reads.

`BuildConfig::default()` uses
`PhysicalLayoutPolicy::production_default()` and requires no cardinality or
format hint. Explicit format selectors are experimental/qualification
overrides. Set `BuildConfig::physical_layout` in Rust. The temporary
`BuildConfig::segment_table_format` binding alias,
`segment_table_format="vortex"` in Python, `segmentTableFormat: "vortex"` in
TypeScript, `--segment-table-format vortex` in the CLI, or
`BORSUK_SEGMENT_TABLE_FORMAT=vortex` in `production_bench`. Parquet is the
default on every surface. The value is persisted at creation; comparison runs
must use a fresh URI and rebuild the index rather than reuse objects from the
other format.

The Rust Vortex reader exposes object size plus asynchronous range reads backed
by `Storage::read_range`; it does not preload a `Vec<u8>`. Lean serving performs
a one-row packed-header projection and a separate vector-less row projection.
Independently verified 1 MiB BLAKE3 chunks authenticate each transferred range,
and per-chunk singleflight prevents concurrent duplicate fetches while leaving
different chunks parallel.
One process-wide Vortex runtime bounds codec CPU and prevents each caller from
creating another runtime.

Vortex 0.81's Unix I/O dependency currently pulls `custom-labels`, whose build
script uses bindgen and therefore needs a C++ toolchain plus `libclang`. This is
another reason the experiment is not the default: a clean Parquet-only BORSUK
build should not silently acquire that system dependency or the associated
compile-time cost.

There is no small JSON manifest exception. Manifests, segment summaries,
pivots, routing rows, and graph blocks are binary Parquet tables. Normal
segment records are Parquet by default and may use the explicitly selected
Vortex experiment. JSON may be emitted by tools for people, but it is not an
index format and not a runtime API contract.

## Decision Matrix

| Format | Use in BORSUK | Reason |
|---|---|---|
| Arrow | In-memory model, schema contract, and FFI ABI | Language-independent columnar memory format with efficient cross-language data exchange |
| Parquet | Canonical durable tables and default normal-segment container | Column-oriented storage format designed for efficient storage/retrieval, compression, projection, and row-group/range access |
| Arrow IPC File | The exact-vector rerank store | Standard typed `FixedSizeList` record batches, footer-addressable bounded range reads, and optional IPC V5 ZSTD buffer compression |
| Arrow IPC File | PQ/TurboQuant ANN scan pages and colocated typed exact candidates | One record batch per bounded cell chunk; fixed-size scan and vector buffers remain independently range-addressable through standard Arrow metadata |
| Vortex | Explicit experimental role-specific backend | The schedule-locked normal-segment campaign rejected every cross-backend candidate, and the independently reproduced 220-case v5 WAL campaign rejected compact Vortex. Normal segments and WAL runs therefore remain Parquet automatically. |
| Avro | Not for index/vector storage | Compact binary serialization and container files; useful for optional streaming ingest logs if needed, but not for segment scans |
| Protobuf | Not for index/vector storage | Good for small RPC/control messages; not a table/columnar storage format and a poor fit for large multidimensional numeric arrays |

Parquet is the canonical durable *index-table* format: it gives BORSUK compressed
column chunks, row groups, footers, statistics, projection, and object-store-
friendly range reads. The deliberate exception is the dense-vector rerank
sidecar. It is a standard Arrow IPC File because rerank needs a small,
footer-addressable record batch containing each candidate, not a projected scan
of a large Parquet row group. Candidate rows in the same batch share one fetch
and one decode.

## Why Arrow IPC now, and where Vortex may fit

The current choice is evidence-led, not a claim that Arrow IPC is universally
faster than Parquet or Vortex:

- Arrow IPC File is a stable cross-language format whose footer records the
  offsets and sizes of every record batch, explicitly enabling random access.
  Its fixed-width arrays are SIMD-friendly and can be reconstructed without
  changing the logical schema. See the
  [Apache Arrow columnar and IPC specification](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format).
- Parquet remains the index-table format because its optional ColumnIndex and
  OffsetIndex support page skipping and navigation by row index, while row
  groups, projection, and statistics fit routing, metadata, postings, and
  lifecycle tables. See the
  [Apache Parquet page-index specification](https://parquet.apache.org/docs/file-format/pageindex/).
- Vortex is retained as a research control and experimental normal-segment
  container. Its stable file format exposes a footer-addressed layout tree and
  segments for local and cloud range access; its default layout uses 8K-row
  zones, 2 MiB uncompressed chunks, and buffered compressed chunks. See the
  [Vortex file-format specification](https://docs.vortex.dev/specs/file-format)
  and [default layout strategy](https://docs.vortex.dev/concepts/file-format).
  Its I/O layer also documents backend-specific coalescing, byte backpressure,
  a byte-sized segment cache, and single-flight reads—properties directly
  relevant to BORSUK's multi-caller object-store path. See the
  [Vortex I/O subsystem](https://docs.vortex.dev/developer-guide/internals/io).

Vortex's project website publishes large performance claims against Parquet.
Those are vendor/project claims, not BORSUK evidence. BORSUK therefore does
**not** default to Vortex merely from those claims. On 24 July 2026 we found
that the original benchmark timed compressed Vortex results without converting
them to the Arrow values used downstream, while the Parquet path did perform
that materialization. All Vortex latency comparisons and the earlier closed
decision are therefore invalid. Footprint, writer-resource, type-compatibility,
and Arrow range-policy measurements remain usable. Corrected comparisons report
both materialized-Arrow execution and, only when the real downstream operation
is implemented over Vortex arrays, compressed-native execution. See
[ANN vector-buffer format A/B](research/vector-format-ab.md) and
[Parquet/Vortex table-workload A/B](research/table-format-ab.md). No production
default will be selected until the corrected real-artifact and AWS runs finish.

The default Vortex layout is aimed at analytical scans. BORSUK's exact rerank
trace is different: it is a `take` of small clustered or scattered candidate
sets selected by ANN. Consequently the publication A/B reports the project's
claimed Parquet speedups only as external claims and publishes BORSUK's own
row-trace, NVMe, and S3 measurements beside them. Vortex is a Linux Foundation
project originally donated by Spiral; it is not a BORSUK or Zilliz-owned
format.

The executable compatibility gate is
[`scripts/probe_vector_format_compatibility.py`](../scripts/probe_vector_format_compatibility.py).
It does not translate a rejected type to f32: that format/type cell is recorded
as `blocked` in `compatibility.csv`. With Vortex Python 0.79, the current matrix
accepts fixed-size f32, f16, bf16-as-UInt16, and i8 vector arrays, but rejects
Arrow `FixedSizeBinary` input. That is compatibility evidence only, not a
performance result; packed-binary Vortex remains ineligible until it has a
same-physical-representation path. The selectable Rust backend uses Vortex
0.81, raises BORSUK's MSRV to Rust 1.91, and aligns the workspace on
Arrow/Parquet 58.4.

The required comparison records:

| Measurement | Why it decides the format |
|---|---|
| bytes/vector and object count | storage and request-cost floor |
| footer/open bytes and latency | first-query cost |
| random candidate rows at clustered and scattered locality | exact-rerank reality |
| sequential/vector-column scan throughput | rebuild and exact fallback |
| GET count, returned bytes, useful-byte ratio | S3 efficiency |
| decode CPU, peak/steady RSS, allocation bytes | 100M-scale resource envelope |
| p50/p95/p99 plus standard deviation over repetitions | tail and variance, not a single best run |

BORSUK does not call its composition a new universal “Arrow + Parquet file
format.” Canonical records, routing, sparse/BM25 postings, exact sidecars, and
global ANN bundles are independently readable standard Parquet or Arrow IPC
files. What is BORSUK-specific is the manifest/content-addressed index layout,
the quantizer semantics, and which standard column buffers a query range-reads.
The ANN bundle is derived and may be deleted and recreated from canonical
sidecars, but it is not a private durable container.

That distinction matters against Vortex. Vortex is a general physical
array/file format with configurable layout trees and compressed-array compute.
BORSUK is an index architecture that assigns physical representations by
access pattern. The publication question is therefore not “is Arrow always
better than Vortex?” but “for a known ANN candidate `take`, which physical
layout minimizes S3 GETs, transferred bytes, decode CPU, and tail latency at
the same recall and resource cap?”

For BORSUK's current workload, Arrow IPC has a concrete advantage over a generic
columnar scan for exact rerank: candidate row numbers already exist, so the
reader can jump to the bounded record batches containing those rows without
evaluating a filter. Parquet has a concrete advantage for routing, lifecycle,
metadata, sparse, and BM25 tables: mature projection/statistics/row-group tools
and broad cross-language inspection. Vortex may be reconsidered when its exact
physical types and dependency baseline align and it wins an end-to-end BORSUK
workload. “Better” remains access-pattern-specific, not a universal format
claim.

The Arrow reader also avoids a pathological `take`: after candidate rows are
deduplicated it compares their count with the sidecar's physical record-batch
count. If the candidate set can touch every batch, it fetches the complete
immutable sidecar before reading the footer; otherwise it fetches the footer
once and only the selected batch ranges. Thus a dense projected rerank cannot
pay “footer + effectively the whole file,” while sparse candidates retain the
range-read advantage.

Physical footprint accounting includes all active segment-table bytes,
standard Arrow IPC exact-vector sidecars, optional graph objects, and the
active global scan artifact. Exact sidecars are not silently omitted from
`bytes/vector`; routing pages carry their aggregate byte count so the same
number is available without listing or reading every data object.

The global descriptor persists its exact-row element type. Its chunk validator,
external build spool, optional cell-graph builder, range planner, and final
reranker use the corresponding physical row width: `4N` bytes for f32, `2N`
for f16/bf16, `N` for i8, and `ceil(N/8)` for binary. A descriptor whose type
does not match the manifest is rejected at open. This closes the previous
footprint bug where a nominal f16 index still duplicated every global exact
row as f32.

Until that matrix is complete, standard Arrow IPC is the conservative exact
sidecar default and Parquet remains the durable index-table default. This is a
replaceable storage boundary; ANN routing, WAL/base/delta publication, and
query semantics must not depend on the selected physical sidecar format.

## Boundary Rules

The same Arrow schemas define data at every boundary, but the physical format
depends on where the data lives:

```text
in-process Rust/Python/Node batch data    Arrow-compatible arrays/buffers
published durable local/blob objects      Parquet index tables + Arrow IPC vector sidecars
active manifest pointer                   fixed binary CURRENT record
append-only ingest journal                immutable cell/lane runs + atomic commit marker
future network control plane              optional Protobuf messages
```

(BORSUK's append-only ingest journal ships today as immutable typed runs below
`cells/<epoch>/<cell>/wal/<lane>/`, with transaction descriptors and commit
markers below `transactions/`; see [Write-ahead log](#write-ahead-log).)

Published index output uses Parquet. Query/API output may be native language
objects for scalar calls today and Arrow-compatible record batches for bulk
calls later. The CLI may print JSON for administrator convenience, but that JSON
is not a storage or runtime API contract.

The word "output" therefore has three separate meanings:

```text
durable index output       Parquet tables + Arrow IPC sidecars + fixed binary CURRENT
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
      lossless bounded-batch vector sidecars
      optional leaf graph blobs (graph-enabled indexes only)
```

The upper layers contain only compact routing metadata: bounds, centroids,
blooms, counters, and child page references. They do not contain vectors. A
query walks from the top routing layer to a small set of leaf routing pages,
then fetches only the selected scan cells and exact-rerank rows. An explicitly
graph-enabled query may additionally fetch graph blobs. This is the mechanism
that keeps S3/object-store reads bounded and keeps process memory close to the
query buffers instead of the full index.

The layer count is controlled by `routing_page_fanout` and by how many leaf
pages exist. Publishing and compaction compute the required depth and persist it
in the manifest. Small indexes may have one routing level; very large
indexes should naturally grow more parent layers without changing the vector
blob size or requiring a full resident routing table.

## Lexical Parquet hierarchy

BM25 and named sparse search use the same bounded object-store hierarchy. There
is no corpus-sized postings object and no private lexical codec:

```text
manifest lexical root reference
  small field root loaded during open
    bounded term-range page selected by query terms
      immutable posting row group selected by (term, run)
      matching immutable row-metadata group
```

The root contains corpus statistics and ordered references to term pages; it
does not contain postings. A term page is capped by both entry count and an
estimated 1 MiB decoded target. Each entry maps a term and immutable run to one
postings row group and one row-metadata group, with compressed object sizes,
decoded working-set bytes, exact score bounds, and canonical decoded checksums.
The posting and metadata files may contain many row groups. Queries read the
Parquet footer and only the selected projected column chunks; they never fetch
the complete file merely to locate a block.

Sparse posting values and sparse query weights are non-negative. Negative
weights are rejected before a record is published or a query is planned. Exact
sparse search returns the top strictly positive inner-product matches; records
with zero score are outside that result set.

The query path is:

1. Load field roots while opening the library handle, before measured traffic.
2. Binary-route query terms to the intersecting term pages and range-read only
   those Parquet pages.
3. Order immutable runs by a sign-safe sparse or BM25 upper bound.
4. Fetch posting and row-metadata groups in bounded parallel waves.
5. Stop at a wave boundary only when the next exact upper bound is strictly
   below the current kth score. Equality is retained so deterministic tie
   ordering remains exact.

Posting and metadata reads share a global weighted byte gate. Its permits are
based on ingest-measured decoded block bytes, so concurrent users and multiple
lexical legs cannot multiply the working set without bound. Overlapping callers
single-flight the same immutable decode and share it only while consumers are
active; the query layer does not retain a corpus-sized posting cache. The disk
range cache separately reuses fetched footer and column-chunk ranges.

Ingest creates bounded per-segment Parquet shards. Publication merges their
compact routing/stat rows into global term pages without copying postings.
Compaction creates new immutable shards and rebuilds roots from the active
segment set; old objects remain unreachable until garbage collection. Record id
and generation in the row-metadata groups give sparse and BM25 the same MVCC
visibility as dense search.

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
segments/L*/xx/seg-*.{parquet,vortex} immutable record id, coarse-code, sketch, and metadata rows (no dense-vector column)
graphs/L*/xx/graph-*.parquet    segment-local graph edge rows
vectors/xx/<checksum>.arrow     standard Arrow IPC exact-vector record batches (optional IPC ZSTD)
quantizer/xx/<checksum>.parquet persisted IVF coarse quantizer (centroid HNSW), single-row Parquet
lexical/roots/.../*.parquet       resident field root with bounded term-page references
lexical/terms/.../*.parquet       bounded term-range routing pages
lexical/postings/.../*.parquet    immutable posting blocks, one addressable row group per run
lexical/rows/.../*.parquet        immutable id/generation/document-length blocks
lexical/shards/.../*.parquet      per-segment build summaries used to rebuild roots
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

Catalog publishes are optimistic and single-winner per manifest version.
Maintenance writers first
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
conditional `CURRENT` or lane-head updates across processes, so concurrent
multi-process writers on local files are not a production-supported mode.
Foreground cell-WAL mutations are concurrent on cloud backends with native
conditional create/update support; catalog maintenance retains the separate
`CURRENT` CAS boundary.

## Versioning Policy

BORSUK has no released on-disk compatibility contract yet. The repository
supports exactly the current schema: incompatible experimental indexes are
recreated from source, with no migration reader or compatibility branch. The
small format marker exists only to reject stale benchmark artifacts clearly.
Until the first release, every incompatible storage change increments the
relevant pointer/table/artifact marker and requires a fresh build from canonical
Parquet/Arrow source. No benchmark result may cross such a format change.

The current table format is v18. It rejects v17 indexes because lane
preparation now requires an expiring reservation in the root-authorized,
bounded 64-way collection frontier. The final root CAS replaces that
reservation with the active collection commit, fencing crash cleanup against a
delayed publisher. Silently opening a pre-reservation index would make orphan
reclamation unsafe.

- **Pointer-format version** changes whenever the fixed binary `CURRENT`
  layout, checksum coverage, or publication semantics become incompatible.
- **Table-format version** changes whenever a required Parquet/Arrow field,
  physical type, invariant, or interpretation becomes incompatible.
- **Same-major readers must ignore unknown columns** in standard Parquet/Arrow
  tables.
- **Additive columns must be written so older same-major readers can ignore them**
  without changing the meaning of existing required columns.

## S3 assumptions and caveats

S3-compatible storage must provide read-after-write visibility for newly written
objects and list results that converge quickly enough for garbage collection.
Search and open paths read objects referenced by `CURRENT`; if the backend does
not make those writes visible before `CURRENT` is visible, readers can fail fast
with a typed storage error instead of returning partial results. GC discovers old
and orphaned objects by listing prefixes, so a backend with delayed listings may
require a longer retention window.

Catalog-publish concurrency is optimistic. Versioned routing indexes and
manifest/routing/pivot tables use conditional create, and `CURRENT` is updated
last. Same-version races produce `concurrent_modification`. Version-skip recovery
after an orphaned namespace relies on conditional `CURRENT` updates for strict
cross-version arbitration; S3, Azure, and GCS provide this through object
ETag/version support. Foreground cell-WAL transactions use create-only commit
markers and conditional lane heads, so different cells and lanes progress
independently. Local filesystem storage is best used from one process because
its conditional-update fallback is process-local.

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
Cell-WAL appends skip the collection-wide allocator entirely when that numeric
floor does not advance; a zero-width ensure operation also avoids a CAS when
another allocator already established the required floor.

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
that segment without reading its normal-segment table object.
`vector_signature_bloom` stores hashes of quantized vectors in the segment.
Budgeted approximate search uses it as a cheap priority signal before fetching
segment objects: segments that may contain a vector with the same signature as
the query are tried before lower-bound ties that definitely cannot. It is not a
correctness filter; exact search and epsilon-bound approximate search still use
the metric lower-bound order. `leaf_mode` declares the segment-local leaf engine
represented by the summary. New pq-scan-only production indexes store
`pq-scan` and empty graph references. Explicitly graph-enabled indexes use
`graph` for L0; their compacted L1+ leaves declare `vamana-pq`. Older routing
tables without these columns are still readable; missing `id_bloom` falls back to scanning candidate
segment payloads for id lookups and duplicate checks, missing
`vector_signature_bloom` falls back to lower-bound-only approximate routing, and
missing `leaf_mode` defaults to `graph`.

Current segment rows include:

```text
segment_header (Binary; valid only in row zero)
record_id
routing_code
pq_code
metadata
(sparse_indices/sparse_values only for mostly-zero rows)
(text_term_ids/text_term_freqs only when text is present)
(generation only when nonzero generations are present)
```

`segment_header` is a checked deterministic little-endian `BSH1` value
containing the format version, segment id, level, metric, dimensions, centroid,
radius, nanosecond creation time, and the two coarse-quantization bound arrays.
Explicit lengths and an internal BLAKE3 checksum reject malformed or corrupt
headers before materialization. Storing it once is a deliberate v12 breaking
change: the library is unreleased, stale experiment indexes are rebuilt from
source, and the row-varying scan no longer pays to encode or project wide
repeated constants.

Normal segment tables no longer carry a dense `vector` column: exact dense
vectors live only in the per-segment Arrow rerank sidecar
(`vectors/<checksum>.arrow`).
A row is stored sparse — `(sparse_indices, sparse_values)` — only when it is
mostly zero; otherwise the row is dense and its full vector is reconstructed from
the sidecar. (WAL objects are the one exception: they inline the dense vector so
an un-flushed tail is searchable before a sidecar exists — see
[Write-ahead log](#write-ahead-log).)

Segment tables and vector-record sidecars store `record_id` as binary bytes;
current Python/TypeScript convenience APIs expose ids as strings. The storage
target is a binary `record_id` plus dense internal row ids for graph and lookup
structures. Smaller ids reduce segment size, bloom work, lookup indexes, and
query result payloads.

`routing_code` is a compact scalar sketch used by approximate search to choose
entry rows inside a fetched segment before exact distance scoring. It is
intentionally small and durable; richer pivot sketches can be added as
additional Parquet columns/tables without changing the Arrow/Parquet format
decision.

`pq_code` is a fixed-size UInt8 list holding the segment's coarse quantization
codes. Its width tracks the active [quantizer](#coarse-quantization-turboquant):
`ScalarBounds` stores one code per raw dimension; the default `TurboQuant`
rotates each vector with a seeded SRHT (padding to the next power of two, so on
non-power-of-two dims the code list is *wider* than `dimensions`) and stores one
code per rotated coordinate. `pq-scan` uses `pq_code` for compressed candidate
ranking inside fetched segments before exact rerank, while `sq-scan` uses the
scalar `routing_code` path. Format-v12 normal segments require the stored code.

The packed header's `pq_min` and `pq_max` arrays hold the scalar quantization
bounds (per raw dimension for `ScalarBounds`, per rotated coordinate for
`TurboQuant`). Persisting them lets a query be quantized without the segment's
full vectors, so pq-scan and sq-scan decode the vector-less row projection to
select candidates, then range-read only the chosen candidates' rows from the
dense-vector sidecar for exact rerank. This bounds per-query decode memory and
rerank I/O to the candidate budget rather than the segment size.

Current graph rows include:

```text
segment_id
source_record_index
neighbor_record_index
neighbor_distance
```

On a graph-enabled index, graph blocks are rebuilt out-of-place with their
segments during compaction, referenced from the active routing summary table,
and used for bounded query-guided candidate traversal in approximate search.
A pq-scan-only index writes no graph blocks.

Compaction should treat graph blocks as optional derived data. A scoped
compaction reads the selected source leaf payloads, rebuilds graph blocks for
new graph-enabled leaves, and
leaves unrelated graph objects untouched until garbage collection. It should not
read old graph blocks just to rewrite a leaf. Omitted compaction batch settings
use the bounded default source-leaf count; whole-level/all-matching compaction is
an explicit offline choice.

Graph rows reference segment-local numeric row ids instead of external ids. That
prevents long external ids from being repeated once per edge and keeps leaf
graph blocks small enough for high-parallelism S3 queries. Older graph tables
with `source_record_id` and `neighbor_record_id` remain readable; the reader maps
those legacy ids to local row indices after loading the segment payload.

## Two storage formats

Each segment stores its vectors in **two** objects, split by access pattern:

```text
segments/L*/xx/seg-<checksum>.{parquet,vortex}
                                         packed header plus ids, coarse codes, sketches, metadata — a projected/scanned table
vectors/xx/<checksum>.arrow             the segment's exact dense vectors — a random-access row store
```

The role policy chooses Parquet or Vortex for the normal-segment table. Routing
and candidate selection scan that column-projectable object (decode coarse
codes and metadata without touching vectors), and it carries no dense-vector
column. The sidecar is what exact rerank reads: after a budgeted scan picks a
handful of candidate rows, BORSUK range-reads only those rows from the sidecar
and computes their true distances.

The sidecar is a standard Arrow IPC File rather than a private codec. Its schema
stores `record_id: Binary`, `generation: UInt64`, and one declared vector
column:

| API type | Arrow/Parquet physical type |
|---|---|
| `float32[N]` | Arrow `FixedSizeList<Float32, N>` |
| `float16[N]` | Arrow `FixedSizeList<Float16, N>` |
| `bfloat16[N]` | Arrow `FixedSizeList<UInt16, N>` plus `bfloat16` schema metadata |
| `int8[N]` | Arrow `FixedSizeList<Int8, N>` |
| `binary[N]` | Arrow `FixedSizeBinary(ceil(N/8))` |
| sparse `float16` | Parquet `term: UInt32`, `row: UInt32`, `value: Float16` posting tables |
| late interaction `[][N]` | Arrow `List<FixedSizeList<Float32/Float16, N>>`; flattened token child ANN selects entities, then exact SIMD MaxSim reranks their persisted matrices |

The footer supplies the exact byte
range of every record batch, and the writer chooses a dimension- and
physical-width-aware bounded batch size. Rerank coalesces candidate rows from the same batch into one range
read and decodes that batch once. Optional IPC V5 ZSTD compresses Arrow buffers;
the uncompressed mode is also standard IPC. Float32 is byte-lossless.
Float16/bfloat16 intentionally canonicalize once before WAL publication, so
WAL, compaction, reopen, and every reader node score the same declared-precision
value. Int8 and binary reject values that are not exactly representable rather
than silently rounding.
Late-interaction record batches have a fixed, configurable 64 MiB decoded-cache
default shared across callers. The cache is byte-bounded and
corpus-independent; simultaneous misses for the same immutable batch are
single-flight, so one S3 range and one Arrow decode can serve all overlapping
queries. Dense-only searches never load these nested sidecars. Exact
late-interaction search remains the deterministic recall reference, while the
bounded research API reports token frontier, unique entity amplification,
token-search/rerank time, bytes, and backing requests for recall/latency curves.
Both segment objects are content-addressed by BLAKE3 and referenced from the
same routing summary. GC lists and reclaims orphaned `vectors/` sidecars
alongside their segments.

## Coarse quantization (TurboQuant)

The row `pq_code` values and packed-header `pq_min`/`pq_max` bounds are produced
by the index's configured quantizer, fixed at create time on the manifest
`BuildConfig`. They
only decide *candidate ordering*; the exact rerank from the lossless sidecar
restores the true distances, so the quantizer choice never affects end-to-end
correctness — only how good the coarse shortlist is at a given candidate budget.
For cosine and angular indexes, current builds create and query both
`routing_code` and the TurboQuant/ScalarBounds codes from unit-L2-normalized
vectors. This keeps the shortlist invariant to positive vector scaling, matching
the index metric instead of allowing raw norm to dominate candidate selection.
Format-v12 indexes always persist the chosen geometry; older table versions are
not accepted.

- **`TurboQuant` (default).** A TurboQuant/RabitQ-style quantizer: apply a
  seeded structured randomized rotation (SRHT: `H D`, `O(d log d)`) so the
  rotated coordinates are near-independent, then scalar-quantize each rotated
  coordinate (4 bits by default) and score asymmetrically (rotate the query,
  dequantize-and-dot). The rotation seed is persisted so queries rotate
  identically. SRHT pads to the next power of two, so on non-power-of-two dims
  the code list is wider than the raw dimensionality (this is what bumped the
  table version to 6). An A/B (`tests/turboquant_ab.rs`) showed it gives
  strictly higher recall@10 at every tight coarse-candidate budget while storing
  about half the coarse bytes per vector (4 bits per rotated coordinate vs 8
  bits per raw dimension), which is why it is the default.
- **`ScalarBounds` (selectable).** The historical quantizer: per-raw-dimension
  min/max scalar quantization, scored symmetrically. Byte-identical to
  pre-existing indexes; still selectable per index.

Two `TurboQuant` knobs exist but default OFF because A/B measurement showed no
gain on tested data, and the docs do not recommend enabling them:

- **`qjl_bits` (default 0 = disabled).** An optional stage-2 1-bit Quantized-JL
  residual correction. It lowers the per-vector estimate's mean squared error but
  injects ranking noise that reorders near-neighbours, so measured recall@10 was
  *lower* at every tight budget. Kept selectable, not recommended.
- **`shards` (default 1 = whole-vector).** An optional Product-Quantization-style
  split into `S` independently rotated subspaces. A/B found no consistent
  recall gain over the whole-vector default (differences stay within run-to-run
  noise) while risking extra per-shard padding overhead. Kept selectable, `1`
  stays the default.

### Resident global product-PQ artifact

A finalized `pq-scan-only` index stores one small content-addressed standard
Parquet file at
`global-pq/descriptors/.../descriptor-<checksum>.parquet` plus immutable
standard Arrow IPC files under `global-pq/bundles/`. Every bundle has one
record batch per cell chunk. Its fixed-size `scan_payload` values contain an
interleaved product code and packed `(segment ordinal, row ordinal)`; its
`exact_vector` values use the declared typed Arrow vector representation. The
descriptor stores the byte offset, length, and checksum of each Arrow value
buffer, so queries range-read selected scan or exact slices without loading the
other column, the bundle, or the corpus into memory. Bundles are capped at
1 MiB of scan payload and 32 MiB total; a single oversize chunk is the
irreducible exception. The descriptor contains the structured-rotation
product codebook, a bounded hierarchical full-dimensional coarse router trained
from corpus vectors,
and content-addressed cell/chunk references. Each chunk holds
one byte per product subspace plus packed `(segment ordinal, row ordinal)`
locations. Every code chunk has a row-aligned typed Arrow exact-vector values
buffer capped at 16 MiB; its row range is arithmetic, so it needs no offset
table, dictionary, or decompression state. IDs and MVCC generations remain in the
physical exact-record sidecar and are materialized only for the final top-k.
Within each bounded chunk, learned centroid ordinals and rows use a deterministic
full bit-plane Morton order. Every bit from every product-code subspace
participates; 64/128-byte codes are not truncated to a 64-bit key. This does
not change ADC distances or recall; it only places similar PQ tuples near one
another so bounded range coalescing can serve several candidates with one
physical GET.
The manifest records descriptor
checksum, vector/subspace counts, default candidate and IVF-probe budgets,
resident-byte envelope, and the ordered segment-checksum set it indexes.

Open rejects a checksum, shape, or segment-set mismatch. Search routes first,
loads only selected immutable code slices, range-reads their aligned lossless
vectors, exact-scores the shortlist, and reads physical ID/generation rows only
for the final top-k (plus exact-distance boundary ties). Selected chunks are
scanned in fixed waves of at most 32 **and** 32 MiB of compressed code payload
per query; one oversize chunk is the irreducible exception. Only the wave's top
approximate candidates survive, and its byte payloads are released before the
next wave. With the four-query production admission default, code payload
retained between I/O and ADC is therefore capped at 128 MiB process-wide.
Adjacent selected code slices in one bundle become one physical range GET.
Lossless candidate ranges from the same bundle are issued together and retain
the sidecar coalescing policy. Merging the wave-local top-k sets is
equivalent to a whole-selection top-k, while transient code memory remains
independent of corpus size. Product codes are never corpus-sized resident
state. Full compaction creates the layout after the final segment set is known.
Direct bulk-ingest batches do not repeatedly rebuild it, and bounded maintenance
clears a stale reference rather than publishing stale row ordinals.

Physical segments are bounded ingest and exact-rerank units, not global semantic
cells. Normalized corpora use the measured flat 256-cell full-dimensional
router below five million rows. Larger normalized corpora and Euclidean shapes
use 64 full-dimensional parent cells and local full-dimensional k-means leaves;
the independently tested 2x64 product router is retained only as a rejected
research control. Hierarchical corpus assignment checks four
neighbouring parents and chooses the closest child across them; query routing
scores the persisted leaf centroids directly. Ordinary million-row hierarchical corpora receive up to
1,024 cells; corpora at five million rows and above receive up to 4,096, rising
to 16,384 at 50 million rows so corpus growth does not multiply rows per cell. The
leaf-centroid table has a hard 32 MiB resident cap, reducing fan-out for very
wide vectors rather than violating the memory envelope. The `u16` cell stores
the parent and local-child ordinals. Every vector is assigned independently, so selecting a cell
selects the matching rows from all ingest checkpoints instead of relying on one
centroid to represent a whole physical segment.

`finish_bulk_load()` publishes this artifact without rewriting bounded ingest
segments. Full compaction is a separately measured layout option that can
co-locate rerank candidates and reduce GETs, but its larger clustering working
set is not required for serving correctness or recall.

Artifact construction retains a coarse-training reservoir capped at 65,536
vectors and 64 MiB (the product codebook samples 4,096), one decoded segment,
one bounded 32 MiB spool chunk, and at most one 32 MiB pending bundle plus its
bounded assembly buffer.
Compact product-code/location/vector rows are partitioned through temporary disk
under `BORSUK_BUILD_SCRATCH_DIR` (default: `.borsuk-scratch` below the process
working directory), then deleted as immutable chunks are published. Scratch is
approximately `vectors × (product subspaces + packed-location bytes +
dimensions × 4)` plus at most one coarse bucket; it does not scale process RAM.
This removes the former
`vector_count × dimensions × 4` allocation (about 3.8 GiB for GIST-1M).

The default code width is dimension- and scale-aware. The 96–128D standard
corpora use two rotated coordinates per product subspace and a 64-byte code. A
measured one-coordinate/128-byte GloVe layout retained 1.000 empirical recall
with a smaller rerank, but doubled scan bytes and worsened latency, CPU, and
RSS, so it is rejected for that dimensionality. At 256–767 dimensions and at
least 100,000 rows, two coordinates per subspace produce a 128-byte code; the
NYTimes recreation raised the all-cell empirical ceiling from 0.991 to 0.993
without increasing peak process RSS. At 768+ dimensions and 100,000+ vectors,
the adaptive cap is 256 subspaces: the fresh GIST-960 control reduces the
qualified exact shortlist from 608 to 96 rows and raises empirical recall from
0.985 to 0.995 while adding only 1.5% to the complete index. This rule is gated
by dimension and scale because GloVe and NYTimes controls show that wider codes
can instead waste bytes and CPU. Exact mode continues to use the lossless
float32 pages and guarantees recall 1.0.
The default exact-rerank shortlist is `3 × subspaces - 8` (minimum 32). Large
low-dimensional angular corpora use at least `3 × subspaces + 8`. For
normalized cosine/angular geometry at 192+ dimensions, the 64-byte layout uses
at least `5 × subspaces`; this selects the measured 320-row NYTimes-256 point.
The 128-byte NYTimes frontier instead needs 288 rows; larger reranks were
measured and dominated, so its dedicated default is selected after the probe
sweep rather than extrapolating the generic multiplier.
Code64 corpora at 512+ dimensions use at least `5 × subspaces`, rising to
`6 × subspaces` at 100,000 rows; this selects 320 rows for Fashion-MNIST. The
measured GIST code256 exception uses 96 rows because its higher-fidelity ADC
ordering reaches 0.995 there; 128 rows adds only 0.001 recall and 384 rows is a
higher-I/O 0.997 research profile. An
explicit `max_candidates_per_segment` becomes the routed global rerank budget.
The default coarse-cell probe count grows as
`max(dataset base, 2 × sqrt(actual coarse cells))`, caps at 256, and is
overridden by `max_segments`. Low-dimensional normalized angular data currently
uses a 128-probe base: GloVe routes over flat 256 cells, Deep-Image-scale 96D
data uses the full-dimensional hierarchy, and Euclidean SIFT uses the
separately measured hierarchy. Thus even the conservative large-corpus fallback scans a bounded fraction of product
codes per query instead of all 100M; physical segment count does not become the
query fan-out.

NYTimes-256 is an explicit measured exception to the generic base: a boundary
sweep found that the code128 curve first reaches its 0.993 ceiling at 223 of
256 cells; 221–222 remain at 0.989, while 256 adds bytes and latency without
recall. The default therefore uses 223/288 for that dimensional profile. This high probe fraction is documented as a routing
limitation and motivates the next full-dimensional coarse-layout ablation; it
is not hidden behind a claim that one rule is optimal for every dataset.
An explicit 256-byte control did not improve the 0.993 ceiling even at 1,024
rerank candidates; it doubled code bytes and increased CPU/latency, so the
adaptive NYTimes width remains 128 bytes.

GIST-960 is the corresponding very-wide exception. Its 1,024-cell
full-dimensional hierarchy uses 24 probes, a 256-byte code, and a 96-row exact
shortlist. The code128 control plateaus at 0.986 even as routing widens; the
code256 probe curve reaches 0.999 at 64 probes, making the cost of the last
empirical recall points visible rather than hiding it behind a universal
high-recall preset.

Lossless sidecar rows use a qualified three-part policy: merge unselected gaps
up to 1 MiB, cap every physical range at 4 MiB, and issue at most ten physical
range reads concurrently. The cap is essential: it prevents the 1 MiB gap from
turning a scattered shortlist into an almost whole-sidecar transfer, while the
larger gap avoids the thousands of tiny GETs observed with the original 64 KiB
policy.

### Persisted coarse quantizer for paged indexes

A resident-routing index builds the IVF coarse quantizer — an HNSW over the cell
centroids — in memory from its resident routing summaries. A paged index has no
resident summary array, so historically it fell back to the paged
routing tree, which degrades on high-dimensional data (overlapping
centroid+radius bubbles prune poorly — the curse of dimensionality).

Compaction now persists that quantizer as one content-addressed object,
`quantizer/<checksum>.parquet`, so an opened paged reader can load it with a
single metadata read before measured query service and route to the `nprobe`
nearest cells without decoding every routing summary. The
object is a standard single-row Parquet file whose one `quantizer_json` column
holds the centroid HNSW plus the per-cell segment summaries, serialized with
`serde_json` (which round-trips `f32` losslessly, so the loaded graph routes
bit-identically to the resident one) — a plainly-visible JSON string column any
cross-language reader can open, not a bare blob. It carries centroids, adjacency,
and light per-cell records, never the full vectors, so it stays small. Older
manifests that reference no quantizer object, and queries run with the coarse
quantizer disabled, fall back to the routing tree; a corrupt object is treated as
"no quantizer" and also falls back.

## Write-ahead log

The WAL is **on by default** and transaction-bundled. Small
`add`/`upsert`/delete batches prepare one immutable content-addressed record
bundle, one optional tombstone bundle, one ID-directory bundle, and one checked
descriptor. Physical cell assignment is deferred until flush. Before staging,
the collection coordinator installs an expiring
reservation in `collection/wal-frontier/<shard>/HEAD`; after all modalities
finish, one CAS replaces it with the checked collection commit that makes the
whole cross-cell, cross-modality mutation visible without changing `CURRENT`.

Collection readers double-collect the 64 root heads and load only descriptors
embedded in their checked commits; each referenced modality descriptor must
validate, so prepared or torn transactions are
invisible. The committed tail is cached by frontier checksum and exact-scored
as a small overlay alongside the immutable global base or cell-routed corpus;
records are searchable immediately, before flush. The bounded live tail is
exact-scored in full so bundling cannot reduce recall. Writers on different root
shards progress independently; same-shard writers use bounded CAS rebasing. WAL
payloads remain immutable and content-addressed, and collection visibility comes
only from the reserved root-shard CAS rather than a global manifest publish.

The write/recovery control path uses deterministic checked binary objects rather
than JSON:

- `BWH1` lane heads and `BWN1` immutable frontier nodes encode their epoch,
  lane, linked-frontier reference, and run references in little-endian form.
- `BWD1` transaction descriptors pin every prepared run plus an opaque
  caller-metadata byte string, and `BWC1` commit markers pin the descriptor.
  `BWS1` transaction states fence prepared, committing, committed, and aborted
  owners. The prepared-to-committing CAS prevents a failed writer from
  publishing after recovery has aborted it; a reclaimer can finish a fenced
  committing descriptor's marker after a crash. These remain the standalone
  `CellWalStore` protocol; ordinary collection mutation in format v22 uses the
  root-authorized bundle path and does not publish them.
  BORSUK's mutation payload in that field is itself the checked packed `BMM1`
  codec, including any referenced BM25-statistics delta pages.
- `BCWH` collection-frontier heads contain bounded, canonical reservation and
  commit sets. Reservations expire after one hour. Actual garbage collection
  removes expired reservations, double-checks stable root authorization around
  a lane snapshot, and then CAS-detaches unrooted runs before immutable-object
  deletion. Immutable runs, frontier nodes, descriptors, and WAL-owned BM25
  correction pages encode their owning transaction in the path. Live root
  truth protects those paths during the reservation-to-lane-HEAD window, while
  abandoned objects remain reclaimable without a mandatory one-hour age floor.
  Materializing manifests retain consumed run identities; retained versions
  resolve those descriptors, payload paths, and metadata-owned external pages
  so a reader pinned before flush keeps them for `min_age` after obsolescence.
  GC aborts its delete pass when the manifest advances during its scan.
- `BID1` ID-directory delta runs encode binary record IDs, logical-cell
  ownership, generation, and deletion state. `BCN1` generation and generated-ID
  counters encode one little-endian `u64`. Tombstone run summaries use the
  checked packed `BTM1` codec with nanosecond timestamp preservation.
  ID-directory entries are strictly sorted; lookup hashes to one logical
  partition, reads only that partition's live runs, and binary-searches each
  checked run rather than scanning unrelated partitions.
- Insert-only coordination uses sixteen routing-independent `BCL1` claim-shard
  locks. Explicit-ID batches acquire the deduplicated shard paths in ascending
  order. Contention releases only the caller's version-fenced partial set
  before a jittered retry; the total order prevents circular wait, and
  disjoint shard sets share no coordination object. A per-handle version
  checkpoint skips the full collection/WAL refresh only when every touched
  shard still has the version paired with that handle's snapshot. Any external
  commit changes a shard version and forces the complete refresh and
  existing-ID validation. The `Available` body includes the releasing
  transaction ID as a revision, preventing a content-derived S3 ETag from
  returning to the same value after another writer's acquire/release cycle.
  The protocol performs at most one
  acquire and one release per touched shard rather than one PUT per record.
  Failure conditionally releases only exact versions it owns. A crashed
  prepared owner is reclaimed only after its `BWS1` state is conditionally
  fenced as aborted; a committing owner is completed, so recovery cannot create
  two successful inserts.

Every one of these envelopes has an explicit codec version, bounded
length-prefixed fields where needed, a BLAKE3 checksum, strict trailing-byte
rejection, and corruption tests. Content-addressed frontier nodes, transaction
descriptors, and ID-directory runs use a `.bin` suffix; conditional `HEAD`,
`STATE`, `COMMIT`, `LOCK`, and `NEXT` objects are also packed binary despite
having no suffix.
Run creation rejects caller-controlled extensions and fixes the role/codec
mapping to records=`parquet|vortex`, tombstones=`parquet`, and
ID-directory=`bin`, preventing path construction from becoming an escape hatch.

WAL record runs use a dedicated record-only Arrow schema shared by their
Parquet and Vortex containers. It stores `record_id`, metadata, optional
sparse/text/generation columns, the nullable exact primary vector, named
payload extras, and constant vector-type/dimension columns. It deliberately
does not store the normal segment header, routing score, or product/rotated
scalar code: the live tail is exact-scored and never consumes those fields.
This pre-release breaking change removed hundreds of kilobytes of dead payload
from wide 500-row runs and requires a fresh layout qualification; historical
campaigns remain tied to their frozen source.

Because a WAL record has no exact-vector sidecar yet, its table serializes the
full dense vector so the un-flushed tail is searchable and self-contained. The
column keeps the declared Arrow physical type—`Float32`, `Float16`, bfloat16
`UInt16` bits, FP8 `UInt8` bits, `Int8`, or bit-packed `UInt8`—and persists
explicit type and logical-dimension constants because Vortex does not preserve
Arrow schema metadata. WAL publication therefore does not silently expand a
typed vector to float32 storage.
`flush()` materializes the tail directly into real segments (with the normal
vector-less Parquet plus a dense-vector sidecar) — there is no intermediate
double-build. A finalized global artifact remains the immutable base: flush
retains its reference and the new segment checksums form a materialized delta.
Paged `compact()` flushes a bounded live tail before selecting cells; its default
bounded run rewrites only delta cells. Production defaults bound every dimension
of the overlay:
`DEFAULT_WAL_FLUSH_THRESHOLD_RUNS` = 64 immutable runs,
`DEFAULT_WAL_FLUSH_THRESHOLD_RECORDS` = 16,384 records, and
`DEFAULT_WAL_FLUSH_THRESHOLD_BYTES` = 32 MiB. The byte threshold makes wide
vectors flush at fewer rows automatically; the record threshold bounds
low-dimensional/text tail scoring; the run threshold bounds manifest size and
refresh request count for tiny-write workloads. A single-modality collection
can disable the WAL explicitly (`WalConfig::disabled()`) for classic
synchronous segment-per-`add` behavior. Dense and late-interaction child
modalities require the collection WAL for atomic multimodal visibility.

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
graph blocks are derived from those records only for graph-enabled indexes.
Unselected segment payloads, graph
payloads, unrelated target-level leaves, and unrelated routing page payloads stay
unread. The default bounded source-leaf count is the online maintenance path;
unbounded compaction is an explicit offline rebuild-style choice because it must
touch every matching source leaf. When a global artifact exists, the bounded
selector excludes its covered segment checksums and rewrites only materialized
delta leaves, preserving the base descriptor and its row ordinals. The
unbounded path may replace covered leaves and publishes a newly trained
artifact for the resulting complete layout. Publishing the compaction leaves the active
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
- [Vortex file-format specification](https://docs.vortex.dev/specs/file-format)
  defines the stable-since-0.36 footer/segment container and its compatibility
  boundary; [Vortex layouts](https://docs.vortex.dev/concepts/layouts) describe
  the lazy, object-store-backed layout tree evaluated by the planned A/B.
- [Apache Avro](https://avro.apache.org/docs/) describes Avro as a compact
  binary data serialization system with a container file and strong schema
  evolution.
- [Protocol Buffers](https://protobuf.dev/overview/) describe Protobuf as a
  language-neutral structured-data serialization mechanism, suited to compact
  messages and generated bindings.

# BORSUK API

BORSUK's public APIs currently index records shaped as:

```text
id: opaque bytes in storage; string, bytes, or unsigned integer in Python/TypeScript
vector: f32[dimensions]
```

You can add vectors only and let BORSUK return ids, or pass explicit ids. Search
APIs are split by return type: id searches return ids, vector searches return
stored vectors, and report searches return hits plus execution counters.

The storage design does not depend on strings as the primitive id type.
Production-scale indexes should use compact arbitrary binary ids plus dense
internal numeric row ids. Python accepts `str | bytes | int`; TypeScript accepts
`string | Uint8Array | number | bigint`. Explicit integer ids are encoded as
compact unsigned varint bytes, so smaller ids use fewer bytes. String ids remain
a convenience binding, but shorter ids are better because ids are bloomed,
indexed, and returned by query paths. `search_id_bytes` / `searchIdBytes`
returns the canonical stored bytes for arbitrary id forms. Report hits keep a
display `id` plus raw `id_bytes` / `idBytes`; non-UTF8 ids use a `0x...`
display string instead of failing report conversion.

## Create And Open

| Parameter | Rust | Python | TypeScript | Default | When it can change |
|---|---|---|---|---|---|
| Index URI | `IndexConfig::uri` | `uri` | `uri` | required | Fixed for the handle. Reopen another URI for another index. |
| Metric | `IndexConfig::metric` | `metric` | `metric` | required | Fixed for the physical index. Rebuild to change it. |
| Dimensions | `IndexConfig::dimensions` | `dimensions` or `dim` | `dimensions` or `dim` | required | Fixed for the physical index. Rebuild to change it. |
| Segment size | `segment_max_vectors` | `segment_max_vectors` or `segment_size` | `segmentMaxVectors` or `segmentSize` | 4096 in Python/TypeScript/CLI | New inserts use the persisted value. Compaction can write different output sizes with `target_segment_max_vectors`. |
| Resident RAM budget | `ram_budget_bytes` | `ram_budget` | `ramBudget` | none | Persisted create-time budget stays in the manifest. Open-time budget may be stricter. |
| Resident routing | `OpenOptions::resident_routing` | `resident_routing` | `residentRouting` | `true` | Runtime only. Set to `false` for large indexes that should resolve segment summaries from routing pages. |
| Read cache | `create_with_cache` / `open_with_cache` | `cache_dir` | `cacheDir` | none | Runtime only. Does not change the index format. |

The cache is read-through and local to the process host. `CURRENT` is fetched
from backing storage on every open. Cached active manifest, routing, and pivot
metadata tables are validated against the checksums in `CURRENT`; stale or corrupt metadata cache files are refetched automatically before an index handle
is returned. Cached segment, graph, and routing page objects are also validated
against their persisted checksums before decode; corrupt local copies are
discarded and fetched again.

`segment_max_vectors` is the maximum number of vectors in each immutable L0
segment written by normal ingest. It is a write-path setting. Smaller values
flush smaller objects and can improve early pruning, but create more objects and
more routing metadata. Larger values reduce object count and metadata, but each
fetched segment reads more rows. Start with 4096 for normal use, then tune with
`SearchReport.bytes_read`, `SearchReport.segments_searched`, and
`IndexStats.resident_bytes_estimate`.

Open with `OpenOptions { resident_routing: false, .. }`, Python
`borsuk.open(uri, resident_routing=False)`, TypeScript
`open(uri, { residentRouting: false })`, or CLI `--paged-routing` to keep
segment summaries and pivots out of the resident manifest. In that mode, open
loads only manifest/config metadata and validates the active routing page index;
it does not decode the full `routing/segments-*.parquet` or
`routing/pivots-*.parquet` tables into the handle. `IndexStats` derives segment
count, record count, segment bytes, and graph bytes from the routing page index aggregate columns.
It does not read segment payloads, graph payloads, or routing
page payloads for those counters. Rust exposes
`try_stats()` for metadata-error propagation; Python, TypeScript, and CLI stats
commands use that error-returning path.

Append writes stay fast in the same non-resident mode. Generated-id adds append
new L0 routing page objects and reuse the existing page-index refs without
decoding old routing pages. Explicit-id adds first use page-level and
segment-level id blooms to decode only candidate routing pages and segment
payloads for duplicate validation.

Compaction can write a different output leaf size with
`target_segment_max_vectors`. That is the read-path knob: after bulk ingest,
compact into vector-local leaves that are large enough to reduce S3 object
count but small enough that a query can read only a few bounded blobs.

RAM budget enforcement is strict. If resident manifest, routing, pivot, bloom,
and summary metadata exceeds the configured budget, create/open/add/compact
returns a `RAM budget exceeded` error with both resident and budget byte counts.
BORSUK does not silently skip segments to fit a memory budget.

```rust
use borsuk::{BorsukIndex, IndexConfig, VectorMetric};

let index = BorsukIndex::create(IndexConfig {
    uri: "file:///tmp/docs-index".to_string(),
    metric: VectorMetric::Cosine,
    dimensions: 768,
    segment_max_vectors: 4096,
    ram_budget_bytes: Some(1_000_000_000),
})?;
```

```python
import borsuk

index = borsuk.create(
    uri="file:///tmp/docs-index",
    metric=borsuk.VectorMetricName.COSINE,
    dimensions=768,
    segment_max_vectors=4096,
    ram_budget="1GB",
)
```

```ts
import { create, VectorMetricName } from "borsuk";

const index = await create({
  uri: "file:///tmp/docs-index",
  metric: VectorMetricName.Cosine,
  dimensions: 768,
  segmentMaxVectors: 4096,
  ramBudget: "1GB",
});
```

## Add And Read Records

| Operation | Rust | Python | TypeScript |
|---|---|---|---|
| Add vectors, generated ids | `BorsukIndex::add_vectors(vectors)` | `index.add(vectors)` | `await index.add(vectors)` |
| Add vectors, explicit ids | `BorsukIndex::add_vectors_with_ids(vectors, ids)` | `index.add(vectors, ids=ids)` | `const explicitIds = await index.add(vectors, ids)` |
| Add flat float32 buffer | Rust lower-level record API | `index.add_buffer(buffer, ids=ids)` | `const bufferIds = await index.addBuffer(new Float32Array(flatVectors), ids)` |
| Load one vector | `BorsukIndex::get_vector(id)` | `index.get_vector(id)` | `await index.getVector(id)` |

Record ids must be unique. Generated string ids skip existing caller-supplied
decimal-string ids without scanning old segment payloads on every add. Explicit
binary and integer ids are duplicate-checked by their canonical stored bytes.

## Search

| Return shape | Rust | Python | TypeScript |
|---|---|---|---|
| ids | `BorsukIndex::search_ids(query, options)` | `index.search_ids(query, k=10)` | `await index.searchIds(query, { k: 10 })` |
| vectors | `BorsukIndex::search_vectors(query, options)` | `index.search_vectors(query, k=10)` | `await index.searchVectors(query, { k: 10 })` |
| ids, batch | `BorsukIndex::search_ids_batch(queries, options)` | `index.search_ids_batch(queries, k=10)` | `await index.searchIdsBatch(queries, { k: 10 })` |
| vectors, batch | `BorsukIndex::search_vectors_batch(queries, options)` | `index.search_vectors_batch(queries, k=10)` | `await index.searchVectorsBatch(queries, { k: 10 })` |
| report | `BorsukIndex::search_with_report(query, options)` | `index.search_with_report(query, k=10)` | `await index.searchWithReport(query, { k: 10 })` |

Vector-return searches project the stored vectors from the segment payloads
already loaded and exact-reranked by the query. They do not perform a second
`get_vector`/`getVector` lookup per hit.

Rust uses `SearchOptions::exact(k)` for exact mode and
`SearchOptions::approx(k, leaf_mode)` for approximate mode. Approximate options
can set `with_max_segments`, `with_max_bytes`, `with_max_latency_ms`,
`with_eps`, and `with_max_candidates_per_segment`.

Python and TypeScript expose the same settings as keyword/object fields:

```python
ids = index.search_ids(
    query,
    k=20,
    mode=borsuk.SearchMode.APPROX,
    leaf_mode=borsuk.LeafModeName.HYBRID,
    max_segments=16,
    max_bytes="128MB",
    max_candidates_per_segment=64,
)
```

```ts
const ids = await index.searchIds(query, {
  k: 20,
  mode: SearchMode.Approx,
  leafMode: LeafModeName.Hybrid,
  maxSegments: 16,
  maxBytes: "128MB",
  maxCandidatesPerSegment: 64,
});
```

## Leaf Modes

Every approximate query first ranks segment summaries. When a query sets
`max_segments` and does not set `eps`, routing uses persisted vector bounds
when available, falls back to the centroid/radius lower bound, and breaks
lower-bound ties by preferring summaries whose resident
`vector_signature_bloom` may contain the quantized query signature. That
prevents tied segments from making recall depend on ingest order. Paged routing
may overfetch routing metadata pages for recall, but the payload loop still
caps `SearchReport.segments_searched` at `max_segments`. Inside each fetched
segment, the leaf mode chooses which rows are exact-scored.

| Mode | How candidates are selected | Reads graph Parquet | Good for |
|---|---|---:|---|
| `flat-scan` | Keeps the first budgeted rows from the fetched segment. | No | Baselines and graph-free tests. |
| `sq-scan` | Sorts rows by scalar `routing_code` distance to the query. | No | Cheap graph-free candidate reduction. |
| `pq-scan` | Sorts rows by per-dimension UInt8 `pq_code` distance. | No | Compressed vector-shaped candidate ranking. |
| `graph` | Uses scalar entry rows, then walks segment-local graph neighbors. | Yes | L0 insert segments and graph traversal checks. |
| `vamana-pq` | Uses PQ entry rows, then walks segment-local graph neighbors. | Yes | Compacted L1+ segments. |
| `hybrid` | Uses each segment's stored `leaf_mode`. | Depends | Mixed indexes with L0 and compacted segments. |

Current ingest writes L0 segments with stored `leaf_mode = graph`. Current
compaction rewrites L1+ segments with stored `leaf_mode = vamana-pq` and packs
records by vector locality before splitting output leaves. Hybrid therefore
reads graph blocks for graph-backed segments and uses the stored candidate
selector for each segment. The public catalog is available as
`leaf_mode_names()` / `leafModeNames()`.

## Reports And Tuning

`SearchReport` is the main tuning API.

| Field | Meaning | How to use it |
|---|---|---|
| `hits` | Ranked ids and distances; Python/TypeScript hits also expose raw id bytes. | Use `id_bytes` / `idBytes` when ids are binary or integer-encoded. |
| `segments_total` | Active segments ranked by resident routing. | Shows total routing fanout. |
| `segments_searched` | Segment payloads actually fetched. | Lower with tighter `max_segments`, `max_bytes`, or exact pruning. |
| `segments_skipped` | Segments not fetched because routing-page pruning, lower-bound pruning, or budgets stopped the query. | Useful for checking whether budgets are active before and after page decoding. |
| `bytes_read` | Routing page-index, routing-page, and segment Parquet payload bytes read. | Main object-store I/O counter before graph expansion. |
| `graph_bytes_read` | Graph Parquet bytes read. | Nonzero for graph-backed modes; add to `bytes_read` for total object bytes. |
| `records_considered` | Rows loaded from fetched segments. | Measures local work before candidate selection. |
| `records_scored` | Rows exact-scored with the index metric. | Controlled by `max_candidates_per_segment`. |
| `resident_bytes_estimate` | Manifest, routing, pivot, bloom, and summary bytes kept resident. | Compare with RAM budgets and stats. |
| `object_cache_hits` / `object_cache_misses` | Immutable object cache behavior. | Validate cache usefulness. |

Tuning loop:

1. Run exact mode on a sample query set and keep those ids.
2. Run approximate modes with `search_with_report`.
3. Compare ids with `recall_at_k` / `recallAtK`.
4. Adjust `max_segments`, `max_candidates_per_segment`, and `segment_max_vectors`.
5. Watch p95 latency, bytes read, graph bytes, records scored, and resident bytes.

## Maintenance

`BorsukIndex::compact(CompactionOptions)` rewrites selected immutable source
segments into new target-level Parquet segments and publishes a new manifest.
It does not mutate old segment objects. `target_segment_max_vectors` controls
the compacted output leaf size for that compaction run.

`BorsukIndex::rebuild(RebuildOptions)`, Python `Index.rebuild(...)`,
TypeScript `index.rebuild(...)`, and CLI `borsuk rebuild` are the explicit
whole-source-level maintenance path. Rebuild sets compaction to all matching
segments for the requested source level, then runs obsolete-object garbage
collection in dry-run mode unless `delete_obsolete` / `deleteObsolete` /
`--delete-obsolete` is set. Use rebuild after a bulk load or offline migration;
use normal compact for steady incremental maintenance.

Compaction is incremental by default. If `max_segments` is omitted, Rust uses
`DEFAULT_COMPACTION_MAX_SEGMENTS` and Python/TypeScript/CLI use the same bounded
batch. Set `max_segments` to tune the batch size. Use `None` in Rust or
`all_matching=True` / `allMatching: true` / `--all-matching` in the public
bridges only when you intentionally want to compact every matching source-level
leaf in one offline run.
`min_segments` is the trigger threshold for a compaction run. Keep it less than
or equal to `max_segments` whenever `max_segments` is set; impossible thresholds
are rejected before BORSUK reads routing pages or segment payloads.

Use compaction explicitly. The intended high-throughput flow is:

1. Add many vectors through the append-only L0 path.
2. Compact on a user-controlled schedule.
3. Query the compacted leaves with `hybrid` or `vamana-pq`.
4. Garbage-collect inactive objects after readers have moved to the new
   manifest.

For billion-scale data, publish computes multiple binary routing layers from
leaf count and routing fanout. The manifest stores `routing_max_level`, and
each routing page ref stores aggregate `leaf_segments`, byte counters, record
counters, blooms, centroid/radius metadata, and persisted per-dimension vector
bounds. Higher layers are routing pages above bounded leaf blobs; they should
not be modeled as ever larger vector payload blobs.

Compaction must stay scoped: it reads only the selected source leaf payloads
for vector data, and it reads only the routing metadata needed to pick that
batch. A normal run derives new graph blocks from those selected records,
writes only dirty leaf routing page objects, and reuses unchanged
content-addressed routing pages through the new version's page index. It must
not read unrelated target-level leaves, unselected source leaves, or old graph
blocks. Graphs are derived outputs of the new leaves; old graph objects are
only listed by garbage collection and deleted when explicitly requested.
`CompactionReport.bytes_read` and cache counters include the required routing
page-index object, routing page objects, and selected source leaf payloads. A
report also exposes `routing_page_indexes_read`, `routing_pages_read`,
`routing_page_indexes_written`, `routing_pages_written`, `graph_payloads_read`,
and `graph_bytes_read` so scoped compaction I/O is visible from Rust, Python,
and TypeScript. For a normal scoped compaction, `graph_payloads_read` and
`graph_bytes_read` should stay zero because replacement graph blocks are derived
from the selected vector records rather than copied from old graph objects.
A whole-index rebuild is a separate offline operation, not the default
maintenance path.

When routing pages exist, compaction resolves candidate source leaves from the
active routing tree first, even if the handle was opened with resident segment
summaries. Compaction selects source leaves from the active routing page Parquet metadata:
it starts at `routing_max_level`, uses page-index `level_mask` to skip
parent ranges that cannot contain the requested source level, uses
`leaf_segments` to stop once the batch budget is covered, and decodes only
candidate routing page objects on the path to L0. It still rewrites only the
selected source leaf payloads, writes dirty routing pages only, and does not
read unselected segment payloads or old graph payloads. The default bounded
`max_segments` value is the online maintenance path; `max_segments: None`
intentionally compacts every matching source-level segment and should be treated
as an explicit offline rebuild-style operation. Publishing the compaction leaves
the active resident segment-summary table empty, so later operations stay
page-backed. When replacement summaries fit in the dirty
routing pages, publishing rewrites only those leaf page objects, the affected
parent page objects, and the new top routing page index. If replacement
summaries overflow into additional leaf routing pages, the publish path assigns
new leaf ordinals from decoded dirty-branch metadata and treats uncached sibling
subtrees as reserved ranges. It rewrites only the dirty and append parent
branches plus the top routing page index. If that would leave more top refs
than the routing fanout, compaction promotes those refs into one or more higher
parent layers from existing page-ref metadata; it does not read unrelated parent
page bodies to do that. It does not reconstruct every leaf ref, read unrelated
append/rightmost branches, or need the global L0 page index when a parent layer
exists.

Approximate search uses the routing tree before reading leaf page objects. When
`max_segments` is set, top-level page refs are ranked by vector-bound lower bound with centroid/radius as the compatibility fallback.
Search deliberately
overfetches routing metadata pages before it reaches L0 so coarse parent pages
do not destroy recall. The expensive budget is still enforced at the segment
payload layer: `SearchReport.segments_searched` remains capped by
`max_segments`, and graph payloads are read only for graph-backed modes. Exact
search and unbounded approximate search still decode all active routing pages
needed to cover the request.

Approximate search can open from routing page indexes when the full `routing/segments-*.parquet` summary table is empty.
That path keeps the active manifest's resident segment-summary vector empty and
materializes only the selected page summaries during search.
`get_vector(id)` uses the same non-resident path: it filters routing page
objects with the page-level id bloom, decodes only candidate pages, then uses
segment-level blooms before reading segment payloads.

`BorsukIndex::gc_obsolete_segments(GarbageCollectionOptions)` reports inactive
segment and graph objects. Dry-run is the default; deletion is explicit. When
the full resident routing table is empty, GC derives the active segment and
graph paths from routing page Parquet metadata before deleting anything. It
does not need to read segment payloads or graph payloads to protect active
objects.

The CLI is an administration surface:

```bash
borsuk create --uri file:///tmp/docs-index --metric euclidean --dimensions 2 --ram-budget 1GB
borsuk add --uri file:///tmp/docs-index --input records.parquet
borsuk add --uri file:///tmp/docs-index --input records.json --input-format json
borsuk stats --uri file:///tmp/docs-index --paged-routing
borsuk search --uri file:///tmp/docs-index --query '[0.2,0.0]' --mode approx --report --paged-routing
borsuk compact --uri file:///tmp/docs-index --source-level 0 --target-level 1 --max-segments 32
borsuk compact --uri file:///tmp/docs-index --source-level 0 --target-level 1 --all-matching
borsuk rebuild --uri file:///tmp/docs-index --source-level 0 --target-level 1 --delete-obsolete
borsuk gc --uri file:///tmp/docs-index --delete
```

Python and TypeScript packages call the Rust core directly through native FFI.
They must not shell out to this CLI.

## Metrics And Helpers

One physical index has one fixed metric. Python and TypeScript expose typed
enums/literal aliases for metrics, search modes, and leaf modes. Direct helper
APIs include:

```python
borsuk.vector_metric_names()
borsuk.leaf_mode_names()
borsuk.minkowski_metric(3)
borsuk.vector_distance(borsuk.VectorMetricName.COSINE, [1.0, 0.0], [1.0, 0.0])
borsuk.recall_at_k(["doc-a", "doc-b"], ["doc-b", "doc-x"], 2)
```

```ts
vectorMetricNames();
leafModeNames();
minkowskiMetric(3);
vectorDistance(VectorMetricName.Cosine, [1, 0], [1, 0]);
recallAtK(["doc-a", "doc-b"], ["doc-b", "doc-x"], 2);
```

Rust byte helpers, CLI `--ram-budget` / `--max-bytes`, Python `ram_budget` /
`max_bytes`, and TypeScript `ramBudget` / `maxBytes` accept integer byte counts
with optional units: `B`, `KB`, `MB`, `GB`, `TB`, `KiB`, `MiB`, `GiB`, or
`TiB`.

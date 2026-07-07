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
| Routing page fanout | `create_with_routing_page_fanout(..., fanout)` | `routing_page_fanout` | `routingPageFanout` | 128 | Create-time hierarchy knob. Compaction computes the number of routing layers from active leaf count and this fanout. |
| Search routing overfetch | `SearchOptions::with_routing_page_overfetch(n)` | `routing_page_overfetch` | `routingPageOverfetch` | 8 | Per-query approximate-search knob. Reads more cheap routing metadata before spending the segment payload budget. |
| Resident RAM budget | `ram_budget_bytes` | `ram_budget` | `ramBudget` | none | Persisted create-time budget stays in the manifest. Open-time budget may be stricter. |
| Resident routing | `OpenOptions::resident_routing` | `resident_routing` | `residentRouting` | `true` | Runtime only. Set to `false` for large indexes that should resolve segment summaries from routing pages. |
| Read cache | `create_with_cache` / `open_with_cache` | `cache_dir` | `cacheDir` | none | Runtime only. Does not change the index format. |
| Read cache size bound | `OpenOptions::cache_max_bytes` | `cache_max_bytes` | `cacheMaxBytes` | none/unbounded | Runtime only. Enforces an LRU bound on local cached immutable objects. |

The cache is read-through and local to the process host. `CURRENT` is fetched
from backing storage on every open. Cached active manifest, routing, and pivot
metadata tables are validated against the checksums in `CURRENT`; stale or corrupt metadata cache files are refetched automatically before an index handle
is returned. Cached segment, graph, and routing page objects are also validated
against their persisted checksums before decode; corrupt local copies are
discarded and fetched again. The cache is unbounded by default. Set
`cache_max_bytes` / `cacheMaxBytes` / `OpenOptions::cache_max_bytes` to evict
least-recently-used cached objects after writes.

`segment_max_vectors` is the maximum number of vectors in each immutable L0
segment written by normal ingest. It is a write-path setting. Smaller values
flush smaller objects and can improve early pruning, but create more objects and
more routing metadata. Larger values reduce object count and metadata, but each
fetched segment reads more rows. Start with 4096 for normal use, then tune with
`SearchReport.bytes_read`, `SearchReport.segments_searched`, and
`IndexStats.resident_bytes_estimate`.

`routing_page_fanout` controls the routing tree shape, not the number of
vectors in a segment. A fanout of 128 means each parent routing page groups up
to 128 child page refs. Smaller fanout creates more routing layers and narrower
metadata pages; larger fanout creates a shallower tree with fewer metadata
objects. Keep the default unless routing stats or benchmarks show the top tree
is too coarse for the target object-store budget. This value is fixed when the
index is created; compaction uses the persisted fanout to compute
`routing_max_level`, `routing_leaf_pages`, and `routing_pages`.

Do not manually choose "one map" versus "many maps" per index. The publish path
groups leaf routing pages by fanout and repeats that grouping until the top
index fits. Single-level routing is only the small-index degenerate case of that
same algorithm, not a different storage mode.

Do not model production-scale search as one flat map plus vector boxes. The
user-facing intuition is a map, but the durable structure is a computed hierarchy:
root page index, parent routing pages, L0 leaf routing pages, then bounded
segment and graph blobs. Layer count comes from active leaf count and
`routing_page_fanout` during publish/compaction; advanced users tune fanout at
create time and per-query `routing_page_overfetch` at search time.

Open with `OpenOptions { resident_routing: false, .. }`, Python
`borsuk.open(uri, resident_routing=False)`, TypeScript
`open(uri, { residentRouting: false })`, or CLI `--paged-routing` to keep
segment summaries and pivots out of the resident manifest. In that mode, open
loads only manifest/config metadata and validates the active routing page index;
it does not decode the full `routing/segments-*.parquet` or
`routing/pivots-*.parquet` tables into the handle. The routing page index aggregate columns
provide segment count, record count, segment bytes, graph bytes, active L0 page
count, and total routing content-page count for `IndexStats`. Older page-index
files that lack page-count aggregates fall back to walking parent routing page
metadata for topology only. `routing_max_level = 0`
means the top index points directly at leaf routing pages; higher values mean
parent routing layers are present and paged search starts at that top layer. It
does not read segment or graph payloads for those counters. Rust exposes
`try_stats()` for metadata-error propagation; Python, TypeScript, and CLI stats
commands use that error-returning path.

Append writes stay fast in the same non-resident mode. Generated-id adds append
new L0 routing page objects and reuse the existing page-index refs without
decoding old routing pages. When a parent routing layer exists, generated-id
append reads the top routing page index, allocates new L0 leaf ordinals after
the existing top-level span, and writes only the new append branch plus the new
top page index. Repeated small appends decode only the readable rightmost
append branch so they can fill it before creating another parent branch; if
that branch cannot be decoded, append falls back to a new sparse branch instead
of reading unrelated cold parents. Explicit-id adds first use page-level and
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

## Updates and deletes

BORSUK's mutation model is append-only. `add` writes new immutable objects and
publishes a new active snapshot; it does not rewrite existing segment rows.
Opened handles search the active snapshot they loaded or published, and a fresh
open reads the latest `CURRENT` pointer from backing storage.

There is no in-place update or delete API yet; tombstones are not implemented.
An add with an existing id is rejected rather than replacing the old vector.
Until tombstones exist, logical updates and deletes are application-level
operations: materialize the desired live records from your source of truth,
write them into a replacement index root, switch readers to that URI, and retire
the old root outside BORSUK.

Use rebuild for replacement datasets after bulk loading the live records into
the replacement root, then run garbage collection to remove superseded internal
objects. `delete_obsolete` / `--delete-obsolete` is the Rust, Python, and CLI
cleanup flag family; TypeScript uses `deleteObsolete`. `borsuk gc --delete` is
the explicit garbage collection command for obsolete objects that are already
unreferenced by an index root's active manifest. Garbage collection does not
decide logical liveness and does not delete currently referenced records.

```bash
export NEW_URI=file:///tmp/docs-index-v2

borsuk create --uri "$NEW_URI" --metric euclidean --dimensions 2 --segment-max-vectors 1024
borsuk add --uri "$NEW_URI" --input live-records.parquet
borsuk rebuild --uri "$NEW_URI" --source-level 0 --target-level 1 --delete-obsolete
borsuk gc --uri "$NEW_URI" --delete
# Note: gc --delete honors the default 24 h retention window; run immediately
# after rebuild it reclaims nothing yet. Pass --min-age-seconds 0 only when
# the index is externally quiesced (no concurrent readers or writers).
```

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

## Observability

Rust tracing is disabled by default. Enable Cargo feature `tracing` to make the
core crate emit spans for open, add, publish, compact, garbage collection, and
search operations:

```toml
borsuk = { version = "0.1", features = ["tracing"] }
```

The spans use the caller's active Rust `tracing` subscriber. Operation reports
are mirrored into span fields, including add write counters, publish write
counters, compaction counters, GC counters, and search counters such as
`segments_searched`, `segments_skipped`, `bytes_read`, `records_scored`, and
`termination_reason`. Segment-skip events include a `reason` field such as
`max-segments`, `max-bytes`, or `exact-pruned`.

Python and TypeScript bindings do not install their own subscribers. When those
bindings are hosted in a Rust process, spans surface through the Rust subscriber
configured by that host; otherwise use the regular report APIs for counters.

Rust uses `SearchOptions::exact(k)` for exact mode and
`SearchOptions::approx(k, leaf_mode)` for approximate mode. Approximate options
can set `with_max_segments`, `with_max_bytes`, `with_max_latency_ms`,
`with_eps`, `with_routing_page_overfetch`, and
`with_max_candidates_per_segment`.

Python and TypeScript expose the same settings as keyword/object fields:

```python
ids = index.search_ids(
    query,
    k=20,
    mode=borsuk.SearchMode.APPROX,
    leaf_mode=borsuk.LeafModeName.HYBRID,
    max_segments=16,
    max_bytes="128MB",
    routing_page_overfetch=8,
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
  routingPageOverfetch: 8,
  maxCandidatesPerSegment: 64,
});
```

## Recall Guarantee Semantics

`SearchReport.recall_guarantee` / `recallGuarantee` describes whether a search
execution preserved the recall contract for the mode and budgets that were
actually used.

| Mode and options | Report value | Semantics |
|---|---|---|
| Exact mode: Rust `SearchOptions::exact(k)`, Python/TypeScript `mode="exact"` or omitted. | `exact` | Returns the true k nearest neighbors from the active index snapshot under the index metric. No unreturned active record has a smaller distance than a returned hit; equal-distance ties may be returned in any tied order. |
| Approximate mode with complete coverage: no routing preselection skips, no lower-bound/budget/epsilon stop, and no per-segment candidate truncation. | `budget-complete` | The approximate path completed without known recall-loss budgets. It is reported separately from `exact` because the caller selected approximate mode, but the report confirms no segment or local candidate budget reduced coverage. |
| Approximate mode with routing preselection pruning, `eps`, `max_segments`, `max_bytes`, `max_latency_ms`, or `max_candidates_per_segment` truncation. | `degraded` | The result is empirical. Use exact search or compare against exact-oracle queries with `recall_at_k` / `recallAtK` or tie-aware recall helpers. |
| Approximate mode with `guaranteed_recall=True` / `guaranteedRecall: true`. | `budget-complete` on success, typed error on violation. | BORSUK disables routing preselection pruning and per-segment candidate truncation, exact-reranks all admitted records, and returns `recall_guarantee_violated` instead of silently degrading if a hard budget would stop the query. |

This guarantee is about recall under the index's configured vector metric only.
Exact search does not approximate by leaf mode: it returns true k-NN for the
active snapshot according to that metric. Approximate search remains a tuning
surface unless the report says `budget-complete` or the caller requested
`guaranteed_recall` / `guaranteedRecall` and the query returned successfully.

## Leaf Modes

Every approximate query first ranks segment summaries. When a query sets
`max_segments` and does not set `eps`, routing uses persisted vector bounds
when available, falls back to the centroid/radius lower bound when that bound is
safe for the metric, and uses centroid metric distance as the routing rank for
metrics without a safe lower bound such as inner product. It breaks routing-rank
ties by preferring summaries whose resident
`vector_signature_bloom` may contain the quantized query signature. That
prevents tied segments from making recall depend on ingest order. Paged routing
may overfetch routing metadata pages for recall, but the payload loop still
caps `SearchReport.segments_searched` at `max_segments`. Inside each fetched
segment, the leaf mode chooses which rows are exact-scored.
Graph-backed modes read graph Parquet only when
`k < min(max_candidates_per_segment, segment_len) < segment_len`; otherwise the
entry rows already fill the per-segment candidate budget, or the candidate
budget covers the whole segment, so BORSUK skips graph I/O.
Centroid metric distance for unsupported-lower-bound metrics is not used for exact pruning or epsilon termination.
Graph construction is exact for small segments and bounded for larger segments:
large graph blocks use vector-locality and routing-code candidate windows rather
than all-pairs distance checks, so larger `segment_max_vectors` does not make
write cost quadratic.

| Mode | How candidates are selected | Reads graph Parquet | Good for |
|---|---|---:|---|
| `flat-scan` | Keeps the first budgeted rows from the fetched segment. | No | Baselines and graph-free tests. |
| `sq-scan` | Sorts rows by scalar `routing_code` distance to the query. | No | Cheap graph-free candidate reduction. |
| `pq-scan` | Sorts rows by per-dimension UInt8 `pq_code` distance. | No | Compressed vector-shaped candidate ranking. |
| `graph` | Uses scalar entry rows, then walks segment-local graph neighbors. | If budget can expand | L0 insert segments and graph traversal checks. |
| `vamana-pq` | Uses PQ entry rows, then walks segment-local graph neighbors. | If budget can expand | Compacted L1+ segments. |
| `hybrid` | Uses each segment's stored `leaf_mode`. | Per stored mode and budget | Mixed indexes with L0 and compacted segments. |

Current ingest writes L0 segments with stored `leaf_mode = graph`. Current
compaction rewrites L1+ segments with stored `leaf_mode = vamana-pq` and packs
records by vector locality before splitting output leaves. Hybrid therefore
reads graph blocks for graph-backed segments only when the candidate budget can
add graph neighbors, and uses the stored candidate selector for each segment.
Use `hybrid` when fresh L0 inserts and compacted L1+ leaves coexist, so callers
do not need to track the active segment mix.
The public catalog is available as
`leaf_mode_names()` / `leafModeNames()`.

## Reports And Tuning

`SearchReport` is the main tuning API.

| Field | Meaning | How to use it |
|---|---|---|
| `hits` | Ranked ids and distances; Python/TypeScript hits also expose raw id bytes. | Use `id_bytes` / `idBytes` when ids are binary or integer-encoded. |
| `termination_reason` / `terminationReason` | Why the query stopped reading segment payloads: `complete`, `exact-pruned`, `epsilon`, `max-segments`, `max-bytes`, or `max-latency`. | Treat `max-*` reasons as explicit budgeted partial searches, not full-index evidence. |
| `recall_guarantee` / `recallGuarantee` | Recall classification: `exact`, `budget-complete`, or `degraded`. | Use `exact` for true k-NN, `budget-complete` for complete approximate coverage, and `degraded` as empirical recall evidence only. |
| `segments_total` | Active segments ranked by resident routing. | Shows total routing fanout. |
| `segments_searched` | Segment payloads actually fetched. | Lower with tighter `max_segments`, `max_bytes`, or exact pruning. |
| `segments_skipped` | Segments not fetched because routing-page pruning, lower-bound pruning, or budgets stopped the query. | Useful for checking whether budgets are active before and after page decoding. |
| `routing_page_indexes_read` / `routingPageIndexesRead` | Routing page-index objects read before leaf selection. | Should stay small; usually one top index for a query. |
| `routing_pages_read` / `routingPagesRead` | Routing page content objects decoded while walking to selected leaves. | Use this to tune `routing_page_fanout` and `routing_page_overfetch` / `routingPageOverfetch` separately from segment payload reads. |
| `bytes_read` | Routing page-index, routing-page, and segment Parquet payload bytes read. | Main object-store I/O counter before graph expansion. |
| `graph_bytes_read` | Graph Parquet bytes read. | Nonzero only for graph-backed modes with expansion budget; add to `bytes_read` for total object bytes. |
| `prefetched_bytes_unused` / `prefetchedBytesUnused` | Reserved segment payload bytes fetched speculatively but not consumed because the query stopped early. | Keep separate from `bytes_read` when comparing serial and pipelined searches. |
| `records_considered` | Rows loaded from fetched segments. | Measures local work before candidate selection. |
| `records_scored` | Rows exact-scored with the index metric. | Controlled by `max_candidates_per_segment`. |
| `resident_bytes_estimate` | Manifest, routing, pivot, bloom, and summary bytes kept resident. | Compare with RAM budgets and stats. |
| `object_cache_hits` / `object_cache_misses` | Immutable object cache behavior. | Validate cache usefulness. |
| `cache_repairs` / `cacheRepairs` | Cached immutable objects that failed checksum validation and were repaired by refetching from backing storage. | Nonzero values indicate local cache corruption or stale local files. |

`IndexStats.routing_max_level`, `routing_page_fanout`, `routing_leaf_pages`,
and `routing_pages` are the stats-side hierarchy signals. `routing_max_level`
is `0` when the active manifest has only leaf routing page refs, `1` when a
parent routing layer sits above those leaves, and higher when the publish path
has computed additional parent layers from leaf count and routing fanout.
`routing_leaf_pages` is the number of L0 routing pages and `routing_pages` is
the total routing content-page count across all layers.

Tuning loop:

1. Run exact mode on a sample query set and keep those ids.
2. Run approximate modes with `search_with_report`.
3. Compare ids with `recall_at_k` / `recallAtK`, or compare hit distances
   with `tie_aware_recall_at_k` / `tieAwareRecallAtK` when duplicate or
   equal-distance vectors should not count as misses.
4. Adjust `routing_page_overfetch`, `max_segments`, and `max_candidates_per_segment`.
   For a new physical index, tune `segment_max_vectors`; for an existing index,
   tune compaction output size with `target_segment_max_vectors`.
5. Watch p95 latency, bytes read, graph bytes, records scored, and resident bytes.

The id recall helpers accept the same `RecordId` shapes as add/get APIs:
strings, compact unsigned integers, and raw binary ids. They compare the
canonical stored id bytes, so a Python `300` matches the same varint bytes as a
TypeScript `300n`.

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
`target_segment_max_vectors` controls the maximum vectors written to each
compacted output leaf. It must be greater than zero when set, and zero is also
rejected during the same preflight validation before routing pages, source
segments, or graph objects are read.

Use compaction explicitly. The intended high-throughput flow is:

1. Add many vectors through the append-only L0 path.
2. Compact on a user-controlled schedule.
3. Query the compacted leaves with `hybrid` or `vamana-pq`.
4. Garbage-collect inactive objects after readers have moved to the new
   manifest.

For billion-scale data, publish computes multiple binary routing layers from
leaf count and the persisted `routing_page_fanout`. The manifest stores
`routing_max_level` and `routing_page_fanout`, and each routing page ref stores
aggregate `leaf_segments`, byte counters, record counters, blooms,
centroid/radius metadata, and persisted per-dimension vector bounds. Higher
layers are routing pages above bounded leaf blobs; they should not be modeled
as ever larger vector payload blobs.

Compaction must stay scoped: it reads only the selected source leaf payloads
for vector data, and it reads only the routing metadata needed to pick that
batch. A normal run derives new graph blocks from those selected records,
writes only dirty leaf routing page objects, and reuses unchanged
content-addressed routing pages through the new version's page index. Dirty leaf
page refs are patched by persisted page ordinal, so compaction works with sparse
routing ordinals without scanning cold sibling pages or treating page refs as a
dense array. It must not read unrelated target-level leaves, unselected source
leaves, or old graph blocks. Graphs are derived outputs of the new leaves; old
graph objects are only listed by garbage collection and deleted when explicitly
requested.
`CompactionReport.bytes_read` and cache counters include the required routing
page-index object, routing page objects, and selected source leaf payloads. A
report also exposes `bytes_written`, `routing_page_indexes_read`,
`routing_pages_read`, `routing_page_indexes_written`, `routing_pages_written`,
`graph_payloads_read`, and `graph_bytes_read` so scoped compaction I/O is visible
from Rust, Python, and TypeScript. `bytes_written` counts the new compacted
segment payloads plus their derived graph payloads; routing metadata writes are
reported separately by the routing page/index counters. For a normal scoped
compaction, `graph_payloads_read` and `graph_bytes_read` should stay zero because
replacement graph blocks are derived from the selected vector records rather
than copied from old graph objects.
A whole-index rebuild is a separate offline operation, not the default
maintenance path.

When routing pages exist, compaction resolves candidate source leaves from the
active routing tree first, even if the handle was opened with resident segment
summaries. Compaction selects source leaves from the active routing page Parquet metadata:
it starts at `routing_max_level`, uses page-index `level_mask` to skip
parent ranges that cannot contain the requested source level, uses
`leaf_segments` for aggregate page counts, and decodes only candidate routing
page objects on the path to L0 until the selected segment budget is satisfied.
It does not decode sibling L0 routing pages after the requested source batch is full.
It still rewrites only the selected source leaf payloads, writes dirty
routing pages only, and does not read unselected segment payloads or old graph
payloads. The default bounded `max_segments` value is the online maintenance
path; `max_segments: None` intentionally compacts every matching source-level
segment and should be treated as an explicit offline rebuild-style operation.
Publishing the compaction leaves the active resident segment-summary table empty,
so later operations stay page-backed. When replacement summaries fit in the dirty
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
`max_segments` is set, top-level page refs are ranked by vector-bound lower
bound with centroid/radius as the compatibility fallback when the metric has a
safe bound, or by centroid metric distance when it does not.
Search deliberately
overfetches routing metadata pages before it reaches L0 so coarse parent pages
or dense routing pages do not destroy recall. The default overfetch multiplier
is 8. At each routing layer the multiplier has a page-level floor as well as a
leaf-segment target, so tied or close sibling metadata pages can be decoded even
when the first page already contains enough leaf segments to satisfy
`max_segments`. Set
`SearchOptions::with_routing_page_overfetch(n)`, Python
`routing_page_overfetch=n`, TypeScript `routingPageOverfetch: n`, or CLI
`--routing-page-overfetch n` when a workload needs more metadata lookahead for
recall. The expensive budget is still enforced at the segment payload layer:
`SearchReport.segments_searched` remains capped by `max_segments`, and graph
payloads are read only for graph-backed modes. Exact search and unbounded
approximate search still decode all active routing pages needed to cover the
request.

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
objects. `GarbageCollectionReport` includes `routing_page_indexes_read`,
`routing_pages_read`, `bytes_read`, `object_cache_hits`, and
`object_cache_misses`, so cleanup and rebuild reports show the metadata I/O
used to prove an object is obsolete.

The CLI is an administration surface:

```bash
borsuk create --uri file:///tmp/docs-index --metric euclidean --dimensions 2 --routing-page-fanout 128 --ram-budget 1GB
borsuk add --uri file:///tmp/docs-index --input records.parquet
borsuk add --uri file:///tmp/docs-index --input records.json --input-format json
borsuk stats --uri file:///tmp/docs-index --paged-routing
borsuk search --uri file:///tmp/docs-index --query '[0.2,0.0]' --mode approx --routing-page-overfetch 8 --report --paged-routing
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
borsuk.recall_at_k([b"\x00\x9f", 300], [300, b"\x00\x9f"], 2)
borsuk.tie_aware_recall_at_k([0.0, 0.1], [0.0, 0.1], 2)
```

```ts
vectorMetricNames();
leafModeNames();
minkowskiMetric(3);
vectorDistance(VectorMetricName.Cosine, [1, 0], [1, 0]);
recallAtK(["doc-a", "doc-b"], ["doc-b", "doc-x"], 2);
recallAtK([new Uint8Array([0, 159]), 300], [300n, new Uint8Array([0, 159])], 2);
tieAwareRecallAtK([0, 0.1], [0, 0.1], 2);
```

Rust byte helpers, CLI `--ram-budget` / `--max-bytes`, Python `ram_budget` /
`max_bytes`, and TypeScript `ramBudget` / `maxBytes` accept raw integer numbers
as byte counts or unit strings such as `128MB`. Supported string units are
`B`, `KB`, `MB`, `GB`, `TB`, `KiB`, `MiB`, `GiB`, and `TiB`.

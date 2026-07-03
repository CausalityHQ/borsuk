# BORSUK API

BORSUK's public APIs currently index records shaped as:

```text
id: string
vector: f32[dimensions]
```

You can add vectors only and let BORSUK return ids, or pass explicit ids. Search
APIs are split by return type: id searches return ids, vector searches return
stored vectors, and report searches return hits plus execution counters.

The storage design should not depend on strings as the primitive id type.
Production-scale indexes should use compact arbitrary binary ids plus dense
internal numeric row ids. String ids remain a convenience binding for Python and
TypeScript, but shorter ids are better: ids are bloomed, indexed, and returned
by query paths.

## Create And Open

| Parameter | Rust | Python | TypeScript | Default | When it can change |
|---|---|---|---|---|---|
| Index URI | `IndexConfig::uri` | `uri` | `uri` | required | Fixed for the handle. Reopen another URI for another index. |
| Metric | `IndexConfig::metric` | `metric` | `metric` | required | Fixed for the physical index. Rebuild to change it. |
| Dimensions | `IndexConfig::dimensions` | `dimensions` or `dim` | `dimensions` or `dim` | required | Fixed for the physical index. Rebuild to change it. |
| Segment size | `segment_max_vectors` | `segment_max_vectors` or `segment_size` | `segmentMaxVectors` or `segmentSize` | 4096 in Python/TypeScript/CLI | New inserts use the persisted value. Compaction can write different output sizes with `target_segment_max_vectors`. |
| Resident RAM budget | `ram_budget_bytes` | `ram_budget` | `ramBudget` | none | Persisted create-time budget stays in the manifest. Open-time budget may be stricter. |
| Read cache | `create_with_cache` / `open_with_cache` | `cache_dir` | `cacheDir` | none | Runtime only. Does not change the index format. |

`segment_max_vectors` is the maximum number of vectors in each immutable L0
segment written by normal ingest. It is a write-path setting. Smaller values
flush smaller objects and can improve early pruning, but create more objects and
more routing metadata. Larger values reduce object count and metadata, but each
fetched segment reads more rows. Start with 4096 for normal use, then tune with
`SearchReport.bytes_read`, `SearchReport.segments_searched`, and
`IndexStats.resident_bytes_estimate`.

When an index is opened without resident segment summaries, `IndexStats`
derives segment count, record count, segment bytes, and graph bytes from the
routing page index aggregate columns. It does not read segment payloads, graph
payloads, or routing page payloads for those counters. Rust exposes
`try_stats()` for metadata-error propagation; Python, TypeScript, and CLI stats
commands use that error-returning path.

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

Record ids must be unique. Generated ids skip existing caller-supplied numeric
ids without scanning old segment payloads on every add.

## Search

| Return shape | Rust | Python | TypeScript |
|---|---|---|---|
| ids | `BorsukIndex::search_ids(query, options)` | `index.search_ids(query, k=10)` | `await index.searchIds(query, { k: 10 })` |
| vectors | `BorsukIndex::search_vectors(query, options)` | `index.search_vectors(query, k=10)` | `await index.searchVectors(query, { k: 10 })` |
| ids, batch | `BorsukIndex::search_ids_batch(queries, options)` | `index.search_ids_batch(queries, k=10)` | `await index.searchIdsBatch(queries, { k: 10 })` |
| vectors, batch | `BorsukIndex::search_vectors_batch(queries, options)` | `index.search_vectors_batch(queries, k=10)` | `await index.searchVectorsBatch(queries, { k: 10 })` |
| report | `BorsukIndex::search_with_report(query, options)` | `index.search_with_report(query, k=10)` | `await index.searchWithReport(query, { k: 10 })` |

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
`max_segments` and does not set `eps`, routing uses the centroid/radius lower
bound and breaks lower-bound ties by preferring summaries whose resident
`vector_signature_bloom` may contain the quantized query signature. That
prevents tied segments from making recall depend on ingest order. Inside each
fetched segment, the leaf mode chooses which rows are exact-scored.

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
| `segments_total` | Active segments ranked by resident routing. | Shows total routing fanout. |
| `segments_searched` | Segment payloads actually fetched. | Lower with tighter `max_segments`, `max_bytes`, or exact pruning. |
| `segments_skipped` | Segments not fetched because pruning or budgets stopped the query. | Useful for checking whether budgets are active. |
| `bytes_read` | Segment Parquet payload bytes read. | Main object-store I/O counter. |
| `graph_bytes_read` | Graph Parquet bytes read. | Nonzero for graph-backed modes. |
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

Compaction is incremental by default. If `max_segments` is omitted, Rust uses
`DEFAULT_COMPACTION_MAX_SEGMENTS` and Python/TypeScript/CLI use the same bounded
batch. Set `max_segments` to tune the batch size. Use `None` in Rust or
`all_matching=True` / `allMatching: true` / `--all-matching` in the public
bridges only when you intentionally want to compact every matching source-level
leaf in one offline run.

Use compaction explicitly. The intended high-throughput flow is:

1. Add many vectors through the append-only L0 path.
2. Compact on a user-controlled schedule.
3. Query the compacted leaves with `hybrid` or `vamana-pq`.
4. Garbage-collect inactive objects after readers have moved to the new
   manifest.

For billion-scale data, compaction also needs to compute multiple binary
routing layers from leaf size, routing fanout, and RAM budget. Those layers are
routing pages above bounded leaf blobs; they should not be modeled as ever
larger vector payload blobs.

Compaction must stay scoped: it reads only the selected source leaf payloads.
A normal run derives new graph blocks from those records, writes only dirty
leaf routing page objects, and reuses unchanged content-addressed routing
pages through the new version's page index. It must not read unrelated
target-level leaves, unselected source leaves, or old graph blocks. A
whole-index rebuild is a separate offline operation, not the default
maintenance path.

If the full resident routing table is empty, compaction resolves candidate
source leaves from the active routing page Parquet metadata first. The
page-index `level_mask` lets it skip pages that cannot contain the requested
source level without reading those routing page objects. It still rewrites only
the selected source leaf payloads, writes dirty routing pages only, and does not
read unselected segment payloads or old graph payloads.

Approximate search uses the leaf page index before reading leaf page objects.
When `max_segments` is set, the page index's centroid/radius rows are ranked
first, and only enough leaf routing pages are decoded to cover that segment
budget. Exact search and unbounded approximate search still decode all active
leaf routing pages.

Approximate search can also open from routing page indexes when the full `routing/segments-*.parquet` summary table is empty.
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
borsuk stats --uri file:///tmp/docs-index
borsuk search --uri file:///tmp/docs-index --query '[0.2,0.0]' --mode approx --report
borsuk compact --uri file:///tmp/docs-index --source-level 0 --target-level 1 --max-segments 32
borsuk compact --uri file:///tmp/docs-index --source-level 0 --target-level 1 --all-matching
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

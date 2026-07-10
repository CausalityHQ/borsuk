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
| Metric | `IndexConfig::metric` | `metric` | `metric` | required | Fixed for the physical index. Rebuild to change it. See [Distance metrics](#distance-metrics) for the full catalog and the pruning tradeoff. |
| Dimensions | `IndexConfig::dimensions` | `dimensions` or `dim` | `dimensions` or `dim` | required | Fixed for the physical index. Rebuild to change it. |
| Segment size | `segment_max_vectors` | `segment_max_vectors` or `segment_size` | `segmentMaxVectors` or `segmentSize` | 4096 in Python/TypeScript/CLI | New inserts use the persisted value. Compaction can write different output sizes with `target_segment_max_vectors`. |
| Routing page fanout | `create_with_routing_page_fanout(..., fanout)` | `routing_page_fanout` | `routingPageFanout` | 128 | Create-time hierarchy knob. Compaction computes the number of routing layers from active leaf count and this fanout. |
| Search routing overfetch | `SearchOptions::with_routing_page_overfetch(n)` | `routing_page_overfetch` | `routingPageOverfetch` | 8 | Per-query approximate-search knob. Reads more cheap routing metadata before spending the segment payload budget. |
| Resident RAM budget | `ram_budget_bytes` | `ram_budget` | `ramBudget` | none | Persisted create-time budget stays in the manifest. Open-time budget may be stricter. |
| Resident routing | `OpenOptions::resident_routing` | `resident_routing` | `residentRouting` | `false` | Runtime only. Defaults to paged routing: segments resolve from routing pages so resident memory stays near zero at any index size. Set to `true` for small, hot indexes that fit in RAM and want to skip routing-page reads. |
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

Paged routing is the default. Open loads only manifest/config metadata and
resolves segment summaries and pivots from routing pages on demand, keeping
segment summaries and pivots out of the resident manifest so resident memory
stays near zero regardless of index size. It does not decode the full
`routing/segments-*.parquet` or `routing/pivots-*.parquet` tables into the
handle. Open does no eager routing-page validation either: a corrupt or missing
routing page index surfaces lazily at search/stats time, not at open, so open
stays O(1) in RAM. The routing page index aggregate columns provide segment
count, record count, segment bytes, graph bytes, active L0 page count, and total
routing content-page count for `IndexStats`. Older page-index files that lack
page-count aggregates fall back to walking parent routing page metadata for
topology only. `routing_max_level = 0` means the top index points directly at
leaf routing pages; higher values mean parent routing layers are present and
paged search starts at that top layer. It does not read segment or graph
payloads for those counters. Rust exposes `try_stats()` for metadata-error
propagation; Python, TypeScript, and CLI stats commands use that error-returning
path.

For small, hot indexes that fit comfortably in RAM, opt into resident routing
with `OpenOptions { resident_routing: true, .. }`, Python
`borsuk.open(uri, resident_routing=True)`, TypeScript
`open(uri, { residentRouting: true })`, or CLI `--resident-routing`. Resident
open decodes the full routing and pivot tables once so search skips routing-page
reads, trading resident RAM for lower per-query latency.

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
    text: false,
    named_vectors: Default::default(),
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
| Load a vector with metadata | `BorsukIndex::get_record(id)` | `index.get_record(id)` | `await index.getRecord(id)` |

To attach per-vector metadata on add and constrain searches by it, see
[Metadata And Filtered Search](#metadata-and-filtered-search).

Record ids must be unique. Generated string ids skip existing caller-supplied
decimal-string ids without scanning old segment payloads on every add. Explicit
binary and integer ids are duplicate-checked by their canonical stored bytes.

**Batch your writes.** Each `add` call writes a new immutable segment and
publishes a fresh manifest, so it pays a fixed per-call cost regardless of how
many vectors it carries — appending one record costs about the same as appending
a few thousand. BORSUK is a batch-oriented writer: pass as many records to a
single `add` as you reasonably can (a few thousand per call is a good default,
matching `segment_max_vectors`) rather than calling `add` once per vector. Bulk
ingest then runs at object-storage speed, and background compaction later packs
the appended segments into read-optimized leaves. The `insert_latency_stays_bounded`
performance smoke test measures both shapes if you want concrete numbers on your
hardware.

## Updates and deletes

Deletes are soft. `delete(ids)` records the ids in a cumulative tombstone that is
filtered out of search and `get_vector` at once; the rows are physically
reclaimed lazily by the next compaction or on demand with `purge`. Re-adding a
currently tombstoned id is rejected until it is purged. The full delete/purge
API, reports, and semantics are in [Deletion](#deletion) below, and the
split/merge rebalancing that keeps the layout healthy as records churn is in
[Maintenance](#maintenance) and [Incremental Maintenance](#incremental-maintenance).

There is no in-place *value* update: `add` never rewrites an existing id's vector
under the same id, and re-adding a live id is a duplicate-id error. To change a
record, delete it, `purge`, then add the new value.

For a wholesale dataset replacement, rebuild the live records into a fresh index
root and let garbage collection remove the superseded objects. `borsuk gc
--delete` (Rust and Python `delete_obsolete` / `--delete-obsolete`, TypeScript
`deleteObsolete`) is the explicit cleanup command; it only removes objects that
no retained manifest version references, and never decides which logical records
should exist.

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

Every search accepts an optional metadata `filter` and an `include_metadata`
flag; see [Metadata And Filtered Search](#metadata-and-filtered-search).

## Metadata And Filtered Search

Every record can carry a JSON-like **metadata** object alongside its vector, and
any search can be constrained by a **filter** over that metadata. This is what
makes BORSUK a drop-in for Pinecone, turbopuffer, and S3 Vectors: attach the
attributes you already store next to each embedding, then narrow results to the
rows that match — `genre = "rock" AND year >= 1990`, `tenant = "acme"`,
`in_stock = true` — without a separate database.

Filtering happens **inside** the search, not as a post-filter on the top-k. A
row is only eligible for ranking if it satisfies the filter, and BORSUK keeps
scanning until it has `k` matches or exhausts the budget — so a selective filter
never silently returns fewer than `k` results just because the nearest vectors
were filtered out.

### The metadata model

Metadata is a string-keyed map whose values are one of: `null`, boolean,
integer, float, string, timestamp (epoch milliseconds), a list, or a nested map.
It is schemaless — different records may carry different keys — and it is stored
in a compact binary column, not JSON. Nested fields are addressed with
dotted paths (`artist.name`, `specs.weight_kg`).

### Start simple: attach and read metadata

Pass one metadata object per vector, positionally aligned with your ids.

```python
index.add(
    [[0.0, 0.0], [1.0, 0.0]],
    ids=["song-1", "song-2"],
    metadata=[
        {"genre": "rock", "year": 1975, "live": False},
        {"genre": "jazz", "year": 1999, "live": True},
    ],
)

# get_record returns the vector and its metadata together.
vector, meta = index.get_record("song-1")
assert meta["genre"] == "rock"
```

```typescript
await index.add(
  [[0, 0], [1, 0]],
  {
    ids: ["song-1", "song-2"],
    metadata: [
      { genre: "rock", year: 1975, live: false },
      { genre: "jazz", year: 1999, live: true },
    ],
  }
);

const record = await index.getRecord("song-1");
record?.metadata.genre; // "rock"
```

```rust
use borsuk::{Metadata, MetaValue, VectorRecord};

let mut meta = Metadata::new();
meta.insert("genre".into(), MetaValue::Str("rock".into()));
meta.insert("year".into(), MetaValue::Int(1975));
index.add(vec![VectorRecord::new("song-1", vec![0.0, 0.0]).with_metadata(meta)])?;

if let Some((vector, meta)) = index.get_record("song-1")? {
    // ...
}
```

```bash
# JSON records carry metadata as a plain object; the CLI accepts a JSON array.
cat > records.json <<'JSON'
[
  {"id": "song-1", "vector": [0.0, 0.0], "metadata": {"genre": "rock", "year": 1975}},
  {"id": "song-2", "vector": [1.0, 0.0], "metadata": {"genre": "jazz", "year": 1999}}
]
JSON
borsuk add --uri "$URI" --input records.json
```

Metadata is only supported with string ids (byte and integer id paths reject it).

### Filter a search

Filters use a Pinecone-style operator dictionary. A bare value is an equality
test; nested objects use `$`-prefixed operators. Metadata is returned only when
you opt in with `include_metadata` / `includeMetadata`, keeping the default
response small.

```python
hits = index.search_ids(
    [0.0, 0.0], k=10,
    filter={"genre": "rock", "year": {"$gte": 1990}},
)

report = index.search_with_report(
    [0.0, 0.0], k=10,
    filter={"genre": "rock"},
    include_metadata=True,
)
report.hits[0].metadata["genre"]  # "rock"
```

```typescript
const ids = await index.searchIds([0, 0], {
  k: 10,
  filter: { genre: "rock", year: { $gte: 1990 } },
});

const report = await index.searchWithReport([0, 0], {
  k: 10,
  filter: { genre: "rock" },
  includeMetadata: true,
});
report.hits[0].metadata?.genre; // "rock"
```

```rust
use borsuk::{Filter, SearchOptions};

let filter = Filter::from_json(&serde_json::json!({
    "genre": "rock",
    "year": { "$gte": 1990 }
}))?;
let options = SearchOptions::exact(10)
    .with_filter(filter)
    .with_include_metadata(true);
let report = index.search_with_report(&query, options)?;
```

```bash
borsuk search --uri "$URI" --query '[0.0,0.0]' --k 10 \
  --filter '{"genre":"rock","year":{"$gte":1990}}' --include-metadata
```

### Filter operators

Each field maps either to a bare value (implicit `$eq`) or to an object of
operators. Multiple operators on one field, and multiple fields at the top
level, are combined with logical AND.

| Operator | Meaning | Example |
|---|---|---|
| bare value | equals | `{"genre": "rock"}` |
| `$eq` / `$ne` | equal / not equal | `{"year": {"$ne": 2020}}` |
| `$gt` `$gte` `$lt` `$lte` | numeric or lexicographic order | `{"year": {"$gte": 1990, "$lt": 2000}}` |
| `$in` / `$nin` | scalar is / is not in a list | `{"genre": {"$in": ["rock", "jazz"]}}` |
| `$contains` | the field's **list** contains a scalar | `{"tags": {"$contains": "live"}}` |
| `$exists` | the path is present / absent | `{"remastered": {"$exists": true}}` |
| `$and` / `$or` / `$not` | boolean composition of sub-filters | `{"$or": [{"genre": "rock"}, {"year": {"$lt": 1970}}]}` |

Semantics are total — a filter never errors on a record, it simply matches or
does not. Rules worth pinning:

- **A missing path fails positive operators** (`$eq`, `$gt`, `$in`, `$contains`,
  …) and **satisfies negative ones** (`$ne`, `$nin`), mirroring Pinecone and
  MongoDB. Use `$exists` to test for presence explicitly.
- **Cross-type comparisons are false.** `Int`, `Float`, and `Timestamp` compare
  numerically with each other; every other cross-kind pairing does not match.
- **`$eq` on a list is not element matching.** `{"tags": "live"}` matches a
  record whose `tags` *equals* the scalar `"live"`; use `$contains` to match an
  element of a list.

### How filtering saves money

Each segment summary carries compact statistics over its metadata — numeric
min/max per path plus a presence bloom filter for strings and value kinds. Before
fetching a segment's payload, BORSUK asks *could any row here satisfy the
filter?* If the statistics prove the answer is no, the segment is skipped
entirely — no object-storage `GET`, no bytes read, no scan. A filter like
`tenant = "acme"` over a multi-tenant index therefore reads only the handful of
segments that actually hold that tenant's rows. The `search_with_report` counters
below quantify it per query.

### The on-demand filter index

The resident statistics are deliberately coarse (a bloom plus min/max) so they
cost almost no memory. To prune *exactly*, each segment also has a small **filter
index** — an exact inverted index over its string and boolean metadata — persisted
as a separate sidecar object next to the segment. It is fetched **only when a
query carries a filter**, used, and dropped: it never sits in RAM, so it does not
grow the resident footprint no matter how large the index gets.

This catches cases the coarse stats cannot. A composite filter like
`{"genre": "rock", "city": "paris"}` might hit a segment whose bloom contains both
`"rock"` and `"paris"` — yet no single row has *both*. The stats can only test each
value independently, so they cannot prune it; the exact index computes the
intersection, sees it is empty, and BORSUK skips the segment's (large) payload
fetch. The sidecar is content-addressed and self-validating, so a missing or
corrupt one simply falls back to reading the segment — it can only save I/O, never
change results. Range and existence filters, which the index cannot answer, skip
the sidecar entirely.

### Ranking only the matches (prefilter)

Segment pruning decides *which segments to read*; inside a segment that is read,
a budgeted (approximate) search decides *which rows to rank*. Rather than pick
the vector-nearest candidates and then discard the ones that fail the filter —
which can find fewer than `k` matches when the matching rows sit outside the
vector-proximity window — BORSUK **prefilters**: it computes the segment's exact
matching rows (via a per-segment inverted index over `Str`/`Bool` metadata, with
a row-by-row fallback for predicates the index cannot answer) and ranks those
directly. A filtered approximate search therefore finds every in-segment match,
skips the segment-local graph read, and stops spending its candidate budget on
rows that cannot qualify — so it reaches `k` sooner and reads fewer segments. The
prefilter engages when a segment's match set fits the per-segment candidate
budget; a broad filter whose matches exceed it falls back to the budgeted path,
and exact search is unchanged (it already scores only matching rows).

### Report counters

`search_with_report` adds three metadata-specific counters to its report:

| Field | Meaning |
|---|---|
| `rows_evaluated` | candidate rows the filter inspected (0 when no filter is set) |
| `rows_passed_filter` | rows that satisfied the filter and were eligible for ranking |
| `segments_pruned_by_filter` | segments skipped whole because their statistics ruled out the filter |

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

> **Production status.** `pq-scan` is the production-recommended leaf mode: it is
> graph-free, has the lowest and most predictable memory footprint, and works with
> the column-projected read path. `sq-scan` and `flat-scan` are also graph-free and
> production-safe. The graph-backed modes — `graph`, `vamana-pq`, and `hybrid` —
> are **experimental**: they can raise recall on some datasets but read extra graph
> objects, cost more memory, and are still being tuned. Prefer `pq-scan` for
> production; reach for the graph modes only when you have measured that they beat
> `pq-scan` on your data.

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

| Mode | Status | How candidates are selected | Reads graph Parquet | Good for |
|---|---|---|---:|---|
| `pq-scan` | **Production** | Sorts rows by per-dimension UInt8 `pq_code` distance. | No | The recommended default: compressed, graph-free, lowest memory. |
| `sq-scan` | Production | Sorts rows by scalar `routing_code` distance to the query. | No | Cheap graph-free candidate reduction. |
| `flat-scan` | Production | Keeps the first budgeted rows from the fetched segment. | No | Baselines and graph-free tests. |
| `graph` | Experimental | Uses scalar entry rows, then walks segment-local graph neighbors. | If budget can expand | L0 insert segments and graph traversal checks. |
| `vamana-pq` | Experimental | Uses PQ entry rows, then walks segment-local graph neighbors. | If budget can expand | Compacted L1+ segments. |
| `hybrid` | Experimental | Uses each segment's stored `leaf_mode`. | Per stored mode and budget | Mixed indexes with L0 and compacted segments. |

Current ingest writes L0 segments with stored `leaf_mode = graph`. Current
compaction rewrites L1+ segments with stored `leaf_mode = vamana-pq` and packs
records by vector locality before splitting output leaves. Hybrid therefore
reads graph blocks for graph-backed segments only when the candidate budget can
add graph neighbors, and uses the stored candidate selector for each segment.
Use `hybrid` when fresh L0 inserts and compacted L1+ leaves coexist, so callers
do not need to track the active segment mix.
The public catalog is available as
`leaf_mode_names()` / `leafModeNames()`.

### How each mode works

The names `pq-scan` and `vamana-pq` come from product quantization and the
Vamana graph, but BORSUK implements lighter, more predictable variants of those
ideas. The descriptions below are what the code actually does. One invariant
holds across every mode: the selected rows are always exact-scored on their full
float32 vectors under the index metric before ranking, so a leaf mode only
changes *which* rows get exact-scored, never the final scores.

- **`flat-scan` (production).** No sketch. Every row in a fetched segment is
  scored on its full vector, so within a searched segment the result is exact. It
  is the ground truth the other modes approximate — a baseline and a graph-free
  test path.
- **`sq-scan` (production).** Each row stores one scalar `routing_code`: a cheap
  signed projection of its vector (an alternating-sign coordinate sum).
  Candidates are the rows whose code is nearest the query's by absolute
  difference, then exact-reranked. The cheapest graph-free candidate reduction,
  at the cost of a coarse one-dimensional filter.
- **`pq-scan` (production, recommended).** Each row stores a per-dimension
  `pq_code` — one `UInt8` per dimension. Every coordinate is log-companded
  (`sign(v)·ln(1+|v|)`) and min–max normalized into `0..=255` using `pq_min` /
  `pq_max` bounds persisted per segment. This is **not** classic product
  quantization: there are no subspace codebooks and no lookup-table (ADC)
  distances. Candidate rows are ranked by the sum of squared per-dimension byte
  differences, then exact-reranked. It is compact, graph-free, has the lowest and
  most predictable memory footprint, and drives the column-projected scan path.
- **`graph` (experimental).** A segment-local proximity graph is stored beside
  the segment as numeric row-reference edges in a separate Parquet block. For
  segments up to 256 rows the graph is an exact k-nearest all-pairs build; larger
  segments use a bounded, windowed construction (candidates drawn from
  vector-locality and routing-code orderings inside a fixed window) instead of a
  full Vamana robust-prune, so write cost stays sub-quadratic. Queries seed entry
  rows by `routing_code`, then walk the graph greedily, exact-scoring each
  neighbour reached until `max_candidates_per_segment` is met. Fresh L0 insert
  segments store this mode.
- **`vamana-pq` (experimental).** The same segment-local graph and greedy
  traversal as `graph`, but entry rows are seeded by `pq_code` distance rather
  than the scalar code. Despite the name it is not the DiskANN/Vamana
  robust-prune construction — it is that greedy graph over PQ-seeded entries.
  Compaction writes L1+ leaves in this mode.
- **`hybrid` (experimental).** Per-segment dispatch: each segment records the
  mode it was written with (L0 `graph`, L1+ `vamana-pq`), and a hybrid query uses
  each segment's own mode, reading graph blocks only for graph-backed segments
  with budget to expand. Use it when fresh L0 and compacted L1+ leaves coexist.

The `graph`, `vamana-pq`, and `hybrid` modes are experimental: they can raise
recall on some datasets but read extra graph objects, cost more memory, and the
large-segment graph construction is still being tuned. Prefer `pq-scan` in
production and switch only after measuring that a graph mode beats it on your
data.

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
| `graph_candidates_added` / `graphCandidatesAdded` | Extra exact-scored candidates reached through segment-local graph edges. | Nonzero only for graph-backed modes; shows how much graph expansion contributed. |
| `resident_bytes_estimate` | Manifest, routing, pivot, bloom, and summary bytes kept resident. | Compare with RAM budgets and stats. |
| `object_cache_hits` / `object_cache_misses` | Immutable object cache behavior. | Validate cache usefulness. |
| `cache_repairs` / `cacheRepairs` | Cached immutable objects that failed checksum validation and were repaired by refetching from backing storage. | Nonzero values indicate local cache corruption or stale local files. |
| `requests` | Object-store requests issued while executing the query, broken out as `gets`, `puts`, `deletes`, `heads`, `lists`, and `total`. | Derive request rate (requests/query) independently of bytes. Search is read-only, so `puts`/`deletes` stay zero; a warm decoded-segment cache lowers `gets`. |

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

Both `SearchReport` and `AddReport` carry a `requests` breakdown
(`gets`/`puts`/`deletes`/`heads`/`lists`/`total`) counting the object-store
requests the operation issued, including retries. This is the primary
request-rate signal: divide `requests.total` by queries for requests-per-query,
or by accepted vectors for requests-per-add. Because counting happens at the
object-store boundary, it is independent of bytes transferred and captures the
true call rate a backing store must serve. The `s3_soak` integration test
(`examples/minio` and `examples/seaweedfs`) reports these against live MinIO and
SeaweedFS.

## Memory And Latency Tradeoffs

Every lever that lowers memory raises latency, and every lever that lowers
latency raises memory. There is no free reduction; pick the point that fits the
deployment.

| Lever | Effect | Tradeoff |
|---|---|---|
| Column-projected pq-scan / sq-scan (automatic when the candidate budget is below the segment length and the decoded cache is off) | Decodes only the chosen candidates' vectors, so per-query decode memory tracks the candidate budget, not the segment size (about 3.3x lower peak RSS at 4096-vector segments). | **Less memory, more wall-time:** a second column-projected read fetches the candidate vectors, costing about 15% more per query. Results are identical to a full decode. Disable per process with `BORSUK_DISABLE_PROJECTED_SCORING=1`. |
| `OpenOptions::max_concurrent_searches` (Rust) | Caps how many searches decode/score at once, so peak working memory tracks the permit count rather than the caller thread count. | **Less memory, more latency under load:** searches beyond the permit count queue, so tail latency rises when concurrency exceeds the cap. |
| `OpenOptions::segment_cache_max_bytes` (Rust) | Shares one decoded `Arc<Segment>` across concurrent queries that touch the same hot segment. | **More memory, less wall-time:** the cache spends up to its byte budget to skip re-decoding hot segments (and disables the projected path, which needs the raw bytes). |
| `max_segments`, `max_candidates_per_segment`, `routing_page_overfetch` | Smaller budgets read and decode fewer segments and candidates. | **Less memory and less I/O, potentially lower recall:** the result may become `degraded`. Compare against exact-oracle recall before tightening. |

For a memory-constrained server holding many concurrent readers, start with a
bounded `max_concurrent_searches`, keep the decoded cache off so pq-scan projects
its reads, and size `max_segments` / `max_candidates_per_segment` to the recall
you need. For a latency-sensitive server with spare memory, enable the decoded
cache and leave concurrency unbounded.

## How BORSUK Keeps Memory Low

The whole design target is to serve a large index from a small machine: resident
memory should stay near flat as the dataset and the number of concurrent readers
grow. That is achieved with a stack of specific mechanisms, not one trick.

- **Paged routing (default).** Open reads only the one-row manifest metadata
  table. Segment summaries and pivots are **not** held resident; a query resolves
  the handful of segments it needs by walking persisted routing pages on demand.
  Resident memory is therefore a few hundred bytes of manifest/config plus the
  blooms, independent of index size — a million-vector index and a hundred-vector
  index have nearly the same resident footprint. Small, hot indexes can opt into
  `resident_routing` to trade that RAM for skipping routing-page reads.

- **The index lives in object storage, not RAM.** Vectors, PQ codes, graphs, and
  routing pages are immutable Parquet objects fetched per query and dropped after
  use. There is no always-resident vector arena. `CURRENT` is one tiny pointer.

- **Column-projected candidate scans.** When the per-segment candidate budget is
  below the segment length (and the decoded cache is off), `pq-scan` and `sq-scan`
  decode the segment with the vector column *masked out*, rank candidates on the
  compact `pq_code`/`routing_code` columns, then read back only the chosen
  candidates' vectors for exact rerank. Per-query decode memory tracks the
  candidate budget, not the segment size — about 3.3× lower peak RSS at
  4096-vector segments — for roughly 15% more wall-time, with identical results.
  Persisted `pq_min`/`pq_max` bounds let the query be quantized without the
  segment's full vectors. Disable with `BORSUK_DISABLE_PROJECTED_SCORING=1`.

- **Bloom fast-paths avoid fetches entirely.** Each segment summary carries a
  128-byte id bloom and a 256-byte vector-signature bloom; the cumulative
  tombstone carries an id bloom in the manifest. Id lookups, duplicate checks,
  and deleted-record filtering consult these resident blooms first, so the common
  "not present / not deleted" answer costs zero object-store I/O.

- **Concurrency does not multiply memory.** Peak working memory is a function of
  how many searches decode at once, not how many callers are connected.
  `max_concurrent_searches` caps concurrent decode/score with a counting
  semaphore, so 1000 connected readers with a cap of N use ~N segments' worth of
  decode memory, not 1000×. Without the cap, memory tracks the caller thread
  count instead.

- **A shared decoded-segment cache, when you want it.** `segment_cache_max_bytes`
  lets concurrent queries that touch the same hot segment share one decoded
  `Arc<Segment>` instead of each decoding its own copy, so peak memory tracks a
  fixed byte budget rather than the number of readers. It trades RAM for fewer
  decodes and fewer object-store `gets`; off by default so the projected path
  stays active.

- **Bounded prefetch.** `prefetch_depth` caps how many selected segment reads are
  in flight, so pipelining latency never turns into unbounded buffered bytes;
  `prefetched_bytes_unused` reports speculative bytes that a budget stop wasted.

- **Content-addressed reuse.** Compaction and republish reuse unchanged routing
  page objects by checksum instead of rewriting them, keeping write amplification
  and transient memory bounded during maintenance.

Every one of these is observable: `SearchReport` exposes `bytes_read`,
`resident_bytes_estimate`, `object_cache_hits`/`misses`, `records_considered`
vs `records_scored`, and the `requests` breakdown, so you can confirm memory and
I/O stay flat as you scale readers and data.

## Deletion

`BorsukIndex::delete(ids)` / `delete_with_report`, Python `Index.delete(ids)`,
TypeScript `index.delete(ids)`, and CLI `borsuk delete --id <id>` logically
delete records. Deletes are **soft**: the ids are recorded in a single
cumulative tombstone whose id bloom rides inside the manifest table (no extra
object fetch), so `search` and `get_vector` skip the deleted records
immediately. The bloom is a fast negative check — a query hit whose id is not in
the tombstone pays zero extra I/O; only a bloom hit fetches the content-addressed
tombstone id list to confirm. Deletes are cheap and do not rewrite segments.

Physical storage is reclaimed two ways:

- **Lazily**, by ordinary compaction: any compaction run drops tombstoned rows
  from the segments it rewrites (`records_rewritten` counts only live rows).
- **On demand**, with `BorsukIndex::purge()` / `purge_with_report`, Python
  `Index.purge()`, TypeScript `index.purge()`, or CLI `borsuk purge`. Purge
  rewrites every active segment without its tombstoned rows, clears the
  tombstone, and re-enables those ids for `add`. It is the heavy, synchronous
  reclaim; prefer running it in a maintenance window on large indexes.

Re-adding a currently-deleted id is rejected until it is purged, so the tombstone
stays authoritative and search never returns a freshly re-added record by
mistake. `DeleteReport` exposes `deleted`, `total_tombstoned`, `published`, and
`requests`; `PurgeReport` exposes `segments_rewritten`, `records_purged`,
`tombstones_cleared`, `published`, and `requests`.

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

`target_segment_max_radius` / `targetSegmentMaxRadius` /
`--target-segment-max-radius` makes compaction **spread-aware**. Routing prunes a
segment by `distance(query, centroid) − radius`, so a dispersed bubble with a
large radius is hard to prune and gets read by many queries. When a radius cap is
set, compaction closes an output segment as soon as the next locality-ordered
record would sit farther than the cap from the segment's running centroid,
splitting one large bubble into several tight, small-radius bubbles. Each bubble's
radius is still computed dynamically from its own contents; the cap only bounds
how large a bubble may grow. It must be greater than zero when set. Use it on
read-shaping compaction runs to trade a few more, smaller segments for sharper
routing and less query I/O.

Use compaction explicitly. The intended high-throughput flow is:

1. Add many vectors through the append-only L0 path.
2. Compact on a user-controlled schedule.
3. Query the compacted leaves with `pq-scan` (production). The experimental
   graph modes (`hybrid`, `vamana-pq`) are available if you have measured that
   they beat `pq-scan` on your data.
4. Garbage-collect inactive objects after readers have moved to the new
   manifest.

For large-scale data, publish computes multiple binary routing layers from
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
borsuk stats --uri file:///tmp/docs-index
borsuk search --uri file:///tmp/docs-index --query '[0.2,0.0]' --mode approx --routing-page-overfetch 8 --report
borsuk stats --uri file:///tmp/docs-index --resident-routing  # opt into resident summaries for a small, hot index
borsuk compact --uri file:///tmp/docs-index --source-level 0 --target-level 1 --max-segments 32
borsuk compact --uri file:///tmp/docs-index --source-level 0 --target-level 1 --all-matching
borsuk rebuild --uri file:///tmp/docs-index --source-level 0 --target-level 1 --delete-obsolete
borsuk maintain --uri file:///tmp/docs-index  # one incremental split/merge pass
borsuk gc --uri file:///tmp/docs-index --delete
```

Python and TypeScript packages call the Rust core directly through native FFI.
They must not shell out to this CLI.

## Incremental Maintenance

`BorsukIndex::run_incremental_maintenance(IncrementalMaintenanceOptions)`, Python
`index.maintain(...)`, TypeScript `index.maintain(...)`, and CLI `borsuk maintain`
rebalance the index **locally**, SPFresh/LIRE style, instead of rewriting whole
levels. One pass:

- **splits** a segment that holds more than `max_segment_vectors` vectors — or
  whose bubble radius exceeds `max_segment_radius` — into several tighter bubbles
  (one read, N writes);
- **merges** a segment whose live vector count fell below `min_segment_vectors`
  (typically after deletes) into its nearest neighbour, dropping the tombstoned
  rows in the same pass, so delete-driven reclaim is local too (two reads, one
  write). A bubble that is fully deleted simply collapses to nothing.

Each pass applies at most `max_operations` split/merge operations and republishes
reusing every unchanged routing page by content address, so it is O(touched), not
O(index) — cheap enough to run continuously. Because BORSUK search prunes by lower
bounds over all candidate bubbles, a vector does not have to live in its strictly
nearest partition for correctness; split and merge only keep each bubble's
centroid and radius honest. `IncrementalReport` records `splits`, `merges`,
`segments_created`/`removed`, `records_moved`, `published`, and `requests`.

**Runs in parallel across nodes.** Incremental maintenance is sharded *by
segment*: a bubble is handled only by the node whose rank its id hashes to, and a
merge picks its neighbour from the same shard, so two nodes never rewrite the same
bubble. Each node collects its changes as a segment delta (ids removed, summaries
added) and publishes with a rebase-safe retry loop — it re-reads `CURRENT`,
re-applies its delta, and compare-and-swaps, so concurrent publishes from other
nodes compose instead of clobbering. No single "who compacts" lease is needed;
every live node compacts its own disjoint slice of the bubbles at once. Schedulers
that drive a fixed pool can call `run_incremental_maintenance_shard(options, rank,
count)` directly; otherwise `start_background_maintenance` derives `(rank, count)`
from the live membership automatically.

## Coordinated Background Maintenance

Incremental maintenance, compaction, purge, and obsolete-object GC can run in the
background and be shared across several processes that open the same object-store
index, without any of them duplicating work or corrupting state.

`BorsukIndex::run_maintenance_once(&MaintenanceConfig)` runs one pass: it reloads
the current manifest, writes this instance's heartbeat, learns the live
membership, and runs only the maintenance units in its shard, each guarded by a
lease. `BorsukIndex::start_background_maintenance(uri, open_options, config,
interval)` spawns a thread that opens its own handle and loops that pass on an
interval, returning a `MaintenanceHandle` that stops and joins the thread when
dropped or when `stop()` is called. Pass errors are swallowed and retried on the
next tick.

Coordination uses two families of small objects under `maintenance/`:

- **Membership** — each instance heartbeats `maintenance/instances/<id>` with the
  current time. Instances whose heartbeat is within `lease_ttl` are the live
  membership; that count is how the work is sharded.
- **Leases** — a unit of work is claimed by creating `maintenance/leases/<key>`
  with a create-if-absent put; a healthy instance reclaims a lease once its owner
  stops heartbeating and the lease expires.

Compaction, purge, and GC are sharded by hashing the *unit key* across the live
membership and guarded by a lease, so N instances split the load and a healthy
instance takes over a dead one's share. Incremental split/merge is finer-grained:
it shards *per segment* and needs no lease, because its rebase-safe delta publish
lets every node compact its own disjoint bubbles at the same time (see
[Incremental Maintenance](#incremental-maintenance)).
Leases only avoid *duplicated* work — correctness still rests on the conditional
`CURRENT` compare-and-swap every publish performs, so a lease race is at worst
wasted effort, never corruption. `MaintenanceConfig` gates which kinds this
instance may run (`incremental`, `compaction`, `garbage_collection`, `purge`;
incremental split/merge is on by default) and sets the `lease_ttl`;
`MaintenanceReport` records the live instance count, this instance's rank, what
it ran, and how many units it skipped due to contention.

## Distance metrics

Each physical index is created with one fixed metric (rebuild to change it). Pass
it by string name — e.g. `metric="cosine"` — or, in Python/TypeScript, via the
typed `VectorMetricName` enum. Minkowski takes a parameter: `metric="minkowski:3"`
or `metric="lp:3"` (equivalently `borsuk.minkowski_metric(3)` / `minkowskiMetric(3)`).

Every metric below returns a **distance**: smaller means more similar, so a search
always keeps the *k* smallest. Even the similarity-shaped metrics are returned in
distance form (cosine as `1 − cosine similarity`, inner product as `−a·b`) so the
ranking direction never changes.

Notation: `aᵢ`, `bᵢ` are the vector components, `n` the dimension count, `a·b` the
dot product, `‖a‖` the Euclidean norm. For the set/binary metrics a component is
*present* when `|xᵢ| > ε`, and `n₁₁ / n₁₀ / n₀₁ / n₀₀` count the dimensions where
both / only-a / only-b / neither are present.

### The pruning tradeoff (read this before picking a metric)

BORSUK routes a query through a tree of segment "bubbles" (a centroid plus a
radius). For metrics that satisfy the triangle inequality it can compute a
*lower bound* on the distance from the query to **every** vector in a bubble —
`max(0, dist(query, centroid) − radius)` — and a matching per-dimension bounding-box
bound. If that lower bound is already worse than the current *k*-th result, the
whole bubble is skipped **without reading it**. This is what lets a search touch a
handful of segments instead of the whole index.

Only the Lp-family metrics get this bound:

| Metric | `name` | Distance (lower = closer) | Notes |
|---|---|---|---|
| Euclidean | `euclidean` | `√ Σ(aᵢ − bᵢ)²` | The default choice; true metric. |
| Manhattan (L1) | `manhattan` | `Σ |aᵢ − bᵢ|` | Robust to outliers. |
| Chebyshev (L∞) | `chebyshev` | `maxᵢ |aᵢ − bᵢ|` | Worst-single-dimension distance. |
| Minkowski (Lp) | `minkowski:<p>` / `lp:<p>` | `(Σ |aᵢ − bᵢ|ᵖ)^(1/p)`, `p ≥ 1` | Interpolates L1↔L2↔L∞. |
| Gower | `gower` | `(1/n) · Σ |aᵢ − bᵢ|` | Manhattan averaged over dimensions. |

**Every other metric still works**, but it does **not** get the lower bound. The
router still orders bubbles by query-to-centroid distance and approximate search
still respects its byte budget, so day-to-day latency is similar. The difference
shows up in **exact** and **recall-guaranteed** searches: without a provable bound
they must scan every candidate segment. If you need exact search over a very large
index, prefer an Lp metric. (Note: `squared-euclidean` ranks identically to
`euclidean` but is *not* pruned — use `euclidean` when you want both.)

### Angle and dot-product metrics

Best for normalized embeddings where direction matters more than magnitude.

| Metric | `name` | Distance (lower = closer) | Notes |
|---|---|---|---|
| Cosine | `cosine` | `1 − (a·b) / (‖a‖ ‖b‖)` | Errors on a zero-norm vector. Range `[0, 2]`. |
| Angular | `angular` | `arccos(cosine similarity) / π` | A true metric on the sphere. Range `[0, 1]`. |
| Inner product | `inner-product` | `− a·b` | Maximum-inner-product search; magnitude matters. |
| Correlation | `correlation` | `1 − corr(a, b)` (Pearson, mean-centered) | Errors on a constant vector. |

### Abundance and ratio metrics (non-negative)

For counts, histograms, or spectra. Components must be `≥ 0` where noted.

| Metric | `name` | Distance (lower = closer) | Notes |
|---|---|---|---|
| Canberra | `canberra` | `Σ |aᵢ − bᵢ| / (|aᵢ| + |bᵢ|)` | Weights differences near zero heavily. |
| Bray–Curtis | `bray-curtis` | `Σ|aᵢ − bᵢ| / Σ|aᵢ + bᵢ|` | Range `[0, 1]`; undefined when all sums are zero. |
| Ruzicka | `ruzicka` | `1 − Σ min(aᵢ, bᵢ) / Σ max(aᵢ, bᵢ)` | Weighted Jaccard for `≥ 0` vectors. |
| Wave–Hedges | `wave-hedges` | `Σ |aᵢ − bᵢ| / max(aᵢ, bᵢ)` | Requires `≥ 0`. |
| Clark | `clark` | `√ Σ ((aᵢ − bᵢ) / (|aᵢ| + |bᵢ|))²` | Normalized L2 of ratios. |
| Lorentzian | `lorentzian` | `Σ ln(1 + |aᵢ − bᵢ|)` | Log-compressed L1. |
| Squared chord | `squared-chord` | `Σ (√aᵢ − √bᵢ)²` | Requires `≥ 0`. |

### Set / binary metrics

A component is *present* when `|xᵢ| > ε`; these compare the resulting presence
sets, so they suit sparse one-hot or bag-of-features vectors.

| Metric | `name` | Distance (lower = closer) | Notes |
|---|---|---|---|
| Hamming | `hamming` | count of dimensions where `aᵢ ≠ bᵢ` | Raw mismatch count (not normalized). |
| Jaccard | `jaccard` | `1 − n₁₁ / (n₁₁ + n₁₀ + n₀₁)` | 1 − intersection/union. |
| Dice | `dice` | `1 − 2·n₁₁ / (2·n₁₁ + n₁₀ + n₀₁)` | Emphasizes shared presence. |
| Simple matching | `simple-matching` | `(n₁₀ + n₀₁) / n` | Counts shared absence too. |
| Russell–Rao | `russell-rao` | `1 − n₁₁ / n` | Only co-presence counts as similar. |
| Rogers–Tanimoto | `rogers-tanimoto` | `2(n₁₀+n₀₁) / (n₁₁ + n₀₀ + 2(n₁₀+n₀₁))` | Mismatches weighted double. |
| Sokal–Sneath | `sokal-sneath` | `2(n₁₀+n₀₁) / (n₁₁ + 2(n₁₀+n₀₁))` | Ignores shared absence. |
| Yule | `yule` | `2·n₁₀·n₀₁ / (n₁₁·n₀₀ + n₁₀·n₀₁)` | Association of the 2×2 table. |

### Probability-distribution metrics

For non-negative vectors read as distributions. Except `chi-square`, each is
computed on the L1-normalized vectors `pᵢ = aᵢ/Σa`, `qᵢ = bᵢ/Σb`; a zero-sum
vector is rejected.

| Metric | `name` | Distance (lower = closer) | Notes |
|---|---|---|---|
| Hellinger | `hellinger` | `√(1 − Σ √(pᵢ qᵢ))` | Bounded `[0, 1]`; symmetric. |
| Chi-square | `chi-square` | `Σ (aᵢ − bᵢ)² / (aᵢ + bᵢ)` | On raw non-negative vectors. |
| Kullback–Leibler | `kullback-leibler` | `Σ pᵢ ln(pᵢ / qᵢ)` | Asymmetric; errors if `qᵢ = 0` where `pᵢ > 0`. |
| Jeffreys | `jeffreys` | `KL(p‖q) + KL(q‖p)` | Symmetrized KL. |
| Jensen–Shannon | `jensen-shannon` | `√(½ KL(p‖m) + ½ KL(q‖m))`, `m = (p+q)/2` | Symmetric, always finite; a true metric. |
| Bhattacharyya | `bhattacharyya` | `− ln Σ √(pᵢ qᵢ)` | Undefined with no shared support. |
| Wasserstein | `wasserstein` | `Σ |CDFₚ(i) − CDF_q(i)|` | 1-D earth-mover over the histograms (index order is the ground distance). |

### Sequence metric

| Metric | `name` | Distance (lower = closer) | Notes |
|---|---|---|---|
| Dynamic time warping | `dynamic-time-warping` | optimal DTW alignment cost with `|aᵢ − bⱼ|` step cost | Treats each vector as a time series; `O(n²)` per comparison. |

`borsuk.vector_metric_names()` / `vectorMetricNames()` return the full list of
string names at runtime, and `borsuk.vector_distance(metric, a, b)` /
`vectorDistance(...)` compute any of them directly for testing or reranking.

## Sparse vectors and full-text (BM25)

Every vector slot is one fixed-width `f32[dimensions]` value in the search
pipeline. Sparse data is only a compact input/storage encoding for that same
value: callers can provide sorted `(index, value)` pairs, BORSUK densifies them,
then segment writes choose dense or sparse physical storage per record. The
automatic rule stores sparse iff `nnz * 2 < dimensions`; Rust callers can override
the per-record choice with `StorageEncoding::{Auto,Dense,Sparse}`.

Sparse encoding is not a retrieval mode. There is no sparse create flag and no
rebuild to switch formats. On read, sparse-encoded rows reconstruct the same
dense vector, so normal `search_ids` / `searchIds` returns the same ids and
distances as the same vector stored densely. Routing, centroids, PQ, graph
candidate selection, and exact scoring all use one unified vector path.

Text remains independent. Create an index with `text=True` only when records need
BM25 terms:

```python
index = borsuk.create(
    uri="file:///tmp/docs-index",
    metric="cosine",
    dimensions=3,
    text=True,
)
```

```ts
const index = await create({
  uri: "file:///tmp/docs-index",
  metric: "cosine",
  dimensions: 3,
  text: true,
});
```

```bash
borsuk create --uri "$URI" --metric cosine --dimensions 3 --text
```

Sparse vector input never needs an index flag. `VectorRecord::from_sparse`,
Python `add(sparse=...)`, TypeScript `add(..., { sparse })`, and CLI JSON
records with `sparse` accept compact input and normalize it to the same logical
vector. Text added to an index created with `text=false` is rejected instead of
silently dropping the payload.

### Attach sparse and text payloads

Python and TypeScript accept optional sparse vector inputs aligned with `add`
rows. A non-null sparse entry supplies that row's vector in compact form and is
densified immediately; use `None` / `null` to keep the dense vector row. Sparse
indices must be sorted, unique, and within `dimensions`.

```python
index.add(
    [[0.1, 0.2, 0.3], [0.3, 0.2, 0.1]],
    ids=["doc-a", "doc-b"],
    sparse=[([0, 2], [0.1, 0.3]), None],
    text=["object storage vector search", "dense-only note"],
)
```

```ts
await index.add(
  [[0.1, 0.2, 0.3], [0.3, 0.2, 0.1]],
  ["doc-a", "doc-b"],
  {
    sparse: [{ indices: [0, 2], values: [0.1, 0.3] }, null],
    text: ["object storage vector search", "dense-only note"],
  }
);
```

```bash
cat > records.json <<'JSON'
[
  {
    "id": "doc-a",
    "sparse": {"indices": [0, 2], "values": [0.1, 0.3]},
    "text": "object storage vector search"
  },
  {"id": "doc-b", "vector": [0.3, 0.2, 0.1], "text": "dense-only note"}
]
JSON
borsuk add --uri "$URI" --input records.json --input-format json
```

Sparse input is searched through the normal vector APIs. Text search ranks by
Okapi BM25 (`k1=1.2`, `b=0.75`). Both return ids by default; the `_with_report` /
`WithReport` forms return `SearchReport` and can include metadata.

```python
index.search_ids([0.1, 0.0, 0.3], k=10)
index.search_text("object storage", k=10)
```

```ts
await index.searchIds([0.1, 0.0, 0.3], { k: 10 });
await index.searchText("object storage", { k: 10 });
```

```bash
borsuk search --uri "$URI" --vector 0.1,0.0,0.3 --k 10
borsuk search-text --uri "$URI" --text "object storage" --k 10
```

### Hybrid fusion

Hybrid search runs the requested vector and/or text legs, then fuses their ranked
lists. The default is Reciprocal Rank Fusion with `rrf_k=60`, which uses ranks
and does not require comparable score scales. `fusion="weighted"` uses a weighted
sum keyed by vector name, plus `@text` for BM25. The primary vector name is the
empty string `""`. This result fusion is separate from the experimental dense
leaf mode named `hybrid`.

```python
index.search_hybrid(
    vectors={"": [0.1, 0.2, 0.3]},
    text="object storage",
    k=10,
)

index.search_hybrid(
    vectors={"": [0.1, 0.2, 0.3]},
    text="object storage",
    k=10,
    fusion="weighted",
    weights={"": 0.4, "@text": 0.6},
)
```

```ts
await index.searchHybrid(
  {
    vectors: { "": [0.1, 0.2, 0.3] },
    text: "object storage",
  },
  { k: 10, fusion: "rrf", rrfK: 60 }
);

await index.searchHybrid(
  {
    vectors: { "": [0.1, 0.2, 0.3] },
    text: "object storage",
  },
  { k: 10, fusion: "weighted", weights: { "": 0.4, "@text": 0.6 } }
);
```

```bash
borsuk search-hybrid --uri "$URI" \
  --vector :0.1,0.2,0.3 \
  --text "object storage" \
  --k 10

borsuk search-hybrid --uri "$URI" \
  --vector :0.1,0.2,0.3 \
  --text "object storage" \
  --fusion weighted --weights =0.4,@text=0.6 \
  --k 10 --report
```

### Tokenization

BM25 tokenization happens in code before term frequencies are written. The Rust
core exposes a tokenizer trait with built-ins for unicode-word lowercase (the
default), whitespace, and lowercased character n-grams. The tokenizer
fingerprint is stored with the manifest so a reader can warn when its tokenizer
does not match the one used at write time.

The rule is simple: use the same tokenizer at write and query time. Python and
TypeScript callers that need custom tokenization can pre-tokenize into the same
normalized space-delimited token stream before `add` and `search_text` /
`searchText`, so both sides feed the same terms into the default tokenizer path.

Sparse vector storage and BM25 keep the object-storage memory model. Sparse
vector rows live in the segment's vector columns and are densified on read; BM25
reads the matching `bidx` sidecar plus resident corpus stats (`N`, `avgdl`). A
plain dense segment omits sparse and text columns entirely. Text sidecars are
content-addressed, read only for the query that needs them, and never kept
resident.

## Named vectors

A BORSUK record always has the primary vector supplied through the usual
`vectors`/`ids` add path. At create time you may declare additional named vectors,
each with its own dimensions and metric. Each declared name gets its own
sub-index; the primary vector path is unchanged, and record ids are shared across
the primary and named sub-indexes.

```python
index = borsuk.create(
    uri="file:///tmp/multi-vector",
    metric="cosine",
    dimensions=3,
    text=True,
    named_vectors={
        "title": {"dimensions": 2, "metric": "cosine"},
        "image": {"dimensions": 4, "metric": "euclidean"},
    },
)

index.add(
    [[0.1, 0.2, 0.3]],
    ids=["doc-1"],
    named_vectors=[
        {
            "title": {"indices": [0], "values": [1.0]},
            "image": [0.2, 0.1, 0.0, 0.4],
        }
    ],
    text=["portable object storage vector search"],
)

index.search_ids([1.0, 0.0], k=5, vector="title")
index.stats().named_vectors  # ["image", "title"] order is stable
```

```ts
const index = await create({
  uri: "file:///tmp/multi-vector",
  metric: "cosine",
  dimensions: 3,
  text: true,
  namedVectors: {
    title: { dimensions: 2, metric: "cosine" },
    image: { dimensions: 4, metric: "euclidean" },
  },
});

await index.add([[0.1, 0.2, 0.3]], ["doc-1"], {
  namedVectors: [
    {
      title: { indices: [0], values: [1] },
      image: [0.2, 0.1, 0, 0.4],
    },
  ],
  text: ["portable object storage vector search"],
});

await index.searchIds([1, 0], { k: 5, vector: "title" });
(await index.stats()).namedVectors; // ["image", "title"] order is stable
```

```bash
borsuk create --uri "$URI" --metric cosine --dimensions 3 --text \
  --named-vector title:2:cosine \
  --named-vector image:4:euclidean

cat > records.json <<'JSON'
[
  {
    "id": "doc-1",
    "vector": [0.1, 0.2, 0.3],
    "named_vectors": {
      "title": {"indices": [0], "values": [1.0]},
      "image": [0.2, 0.1, 0.0, 0.4]
    },
    "text": "portable object storage vector search"
  }
]
JSON
borsuk add --uri "$URI" --input records.json --input-format json
borsuk search --uri "$URI" --query '[1.0,0.0]' --vector title --k 5
```

Named-vector search routes to the sub-index for that name. The query dimension
and metric are the declared ones for that name; omitting `vector` searches the
primary vector. Named vectors accept dense or sparse input, and their segment
storage uses the same dense-or-sparse encoding rule as the primary vector.

Hybrid search can fuse any set of named vectors plus BM25 text. RRF is the
default; weighted fusion uses the same keys as the query vectors and `@text` for
BM25:

```python
index.search_hybrid(
    vectors={"": [0.1, 0.2, 0.3], "title": [1.0, 0.0]},
    text="object storage",
    k=5,
)

index.search_hybrid(
    vectors={"title": {"indices": [0], "values": [1.0]}},
    text="object storage",
    k=5,
    fusion="weighted",
    weights={"title": 0.7, "@text": 0.3},
)
```

```ts
await index.searchHybrid(
  { vectors: { "": [0.1, 0.2, 0.3], title: [1, 0] }, text: "object storage" },
  { k: 5 }
);

await index.searchHybrid(
  {
    vectors: { title: { indices: [0], values: [1] } },
    text: "object storage",
  },
  { k: 5, fusion: "weighted", weights: { title: 0.7, "@text": 0.3 } }
);
```

```bash
borsuk search-hybrid --uri "$URI" \
  --vector :0.1,0.2,0.3 \
  --vector title:1.0,0.0 \
  --text "object storage" \
  --k 5

borsuk search-hybrid --uri "$URI" \
  --sparse-vector title:0:1.0 \
  --text "object storage" \
  --fusion weighted --weights title=0.7,@text=0.3 \
  --k 5
```

The Qdrant drop-in adapter maps Qdrant named dense vectors to BORSUK named
vectors. High-dimensional lexical sparse vectors, such as SPLADE outputs or
Pinecone `sparse_values`, are a different regime from BORSUK's compact dense
vector path; use BM25 text for lexical retrieval. The Qdrant adapter raises a
clear error when Qdrant sparse-vector configuration or sparse query payloads are
used.

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

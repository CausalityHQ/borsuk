# API Notes

## Rust

The Rust crate is the source of truth:

- `BorsukIndex::create(IndexConfig)` creates an index at a local or object-store URI.
- `BorsukIndex::open(uri)` opens an existing local or object-store index.
- `BorsukIndex::create_with_cache(IndexConfig, cache_dir)` and
  `BorsukIndex::open_with_cache(uri, cache_dir)` attach an optional local
  read-through cache for fetched immutable objects.
- `BorsukIndex::open_with_options(uri, OpenOptions)` also accepts a runtime
  resident memory budget. The effective limit is the stricter of the persisted
  index budget and the open-time budget.
- `BorsukIndex::stats()` returns manifest-derived diagnostics without scanning
  storage: metric, dimensions, active segment/record counts, segment and graph
  bytes, active manifest version, effective resident RAM budget, and resident
  metadata estimate.
- `BorsukIndex::add_vectors(vectors)` writes vectors with generated ids and
  returns those ids without scanning existing segment payloads.
- `BorsukIndex::add_vectors_with_ids(vectors, ids)` writes vectors with
  caller-supplied ids.
- `BorsukIndex::add(Vec<VectorRecord>)` remains the lower-level Rust record API
  for callers that already have typed records.
- `BorsukIndex::search_ids(query, SearchOptions)` returns only top-k ids.
- `BorsukIndex::search_vectors(query, SearchOptions)` returns the stored vectors
  for the nearest neighbors.
- `BorsukIndex::get_vector(id)` loads one stored vector by id.
- `BorsukIndex::search_ids_batch(queries, SearchOptions)` searches multiple
  queries and returns one id list per query in input order.
- `BorsukIndex::search_vectors_batch(queries, SearchOptions)` searches multiple
  queries and returns one stored-vector list per query in input order.
- `BorsukIndex::search_with_report(query, SearchOptions)` returns top-k hits plus
  execution counters: segments ranked, segments searched, segments skipped,
  segment bytes read, graph bytes read, object-cache hits and misses, records
  considered, records exact-scored, and elapsed milliseconds, plus an estimate
  of resident manifest/routing memory.
- `BorsukIndex::search_batch_with_report(queries, SearchOptions)` returns the
  same execution counters for each query in input order.
- `SearchOptions::exact(k)` builds exact-search options.
- `SearchOptions::approx(k, LeafMode)` builds typed approximate-search options.
  Chain `with_max_segments`, `with_max_bytes`, `with_max_latency_ms`,
  `with_max_candidates_per_segment`, and `with_eps` for budgeted traversal.
- `IndexConfig::ram_budget_bytes` is optional. When set, create/open/add/compact
  reject manifests whose resident manifest, segment-summary, routing, and pivot
  estimate exceeds the budget.
- `BorsukIndex::compact(CompactionOptions)` rewrites immutable source-level
  segments into target-level Parquet segments out-of-place and publishes a new
  manifest. The report includes source segment counts, rewritten record counts,
  bytes read/written, object-cache hits/misses, and the active manifest version.
- `BorsukIndex::gc_obsolete_segments(GarbageCollectionOptions)` scans segment
  and graph objects, reports inactive objects not referenced by the active
  manifest, and deletes them only when dry-run is disabled.

```rust
use borsuk::{LeafMode, SearchOptions};

let exact_options = SearchOptions::exact(10);
let approx_options = SearchOptions::approx(10, LeafMode::Graph)
    .with_max_segments(32)
    .with_max_bytes(128_000_000)
    .with_max_candidates_per_segment(256);
let vamana_pq_options = SearchOptions::approx(10, LeafMode::VamanaPq)
    .with_max_candidates_per_segment(256);
let hybrid_options = SearchOptions::approx(10, LeafMode::Hybrid)
    .with_max_candidates_per_segment(256);
let product_code_options = SearchOptions::approx(10, LeafMode::PqScan)
    .with_max_candidates_per_segment(256);
let scalar_code_options = SearchOptions::approx(10, LeafMode::SqScan)
    .with_max_candidates_per_segment(256);
```

Implemented leaf modes are `flat-scan`, `sq-scan`, `pq-scan`, `graph`,
`vamana-pq`, and `hybrid`.
`pq-scan` uses the stored per-dimension UInt8 `pq_code` sketch for
vector-shaped compressed candidate ranking before exact rerank, and skips graph
reads.
`sq-scan` ranks records inside fetched segments by the stored scalar routing
code, exact-reranks selected candidates, and skips graph reads.
`vamana-pq` is the initial VamanaPQ-style mode: it uses the same persisted
segment-local graph blocks as `graph` today, reports its own canonical mode,
and keeps exact rerank after graph candidate generation.
`hybrid` uses each segment's persisted `leaf_mode` summary to choose whether
the query should read segment-local graph blocks or stay on the scan path.

## CLI

The `borsuk` binary is optional administration/debug tooling. It must not be
used as the Python or TypeScript runtime transport. CLI JSON output is for
humans and automation scripts only; it is not the storage format and not the
embedding ABI.

```bash
borsuk create --uri file:///tmp/docs-index --metric euclidean --dimensions 2 --ram-budget 1GB
borsuk add --uri file:///tmp/docs-index --input records.parquet
borsuk add --uri file:///tmp/docs-index --input records.json --input-format json
borsuk stats --uri file:///tmp/docs-index
borsuk search --uri file:///tmp/docs-index --query '[0.2,0.0]' --k 2
borsuk search --uri file:///tmp/docs-index --query '[0.2,0.0]' --mode approx --max-bytes 128MB
borsuk search --uri file:///tmp/docs-index --query '[0.2,0.0]' --mode approx --report
borsuk search --uri s3://bucket/docs-index --query '[0.2,0.0]' --cache-dir /mnt/nvme/borsuk-cache --report
borsuk compact --uri file:///tmp/docs-index --source-level 0 --target-level 1 --cache-dir /mnt/nvme/borsuk-cache
borsuk gc --uri file:///tmp/docs-index
borsuk gc --uri file:///tmp/docs-index --delete
```

`borsuk add` uses Parquet for binary vector-record input. JSON input is kept
only for human-readable fixtures and automation scripts that explicitly choose
`--input-format json`.

For S3-compatible storage, use `s3://bucket/prefix` and configure credentials,
endpoint, HTTP allowance, and region through `AWS_*` environment variables.
MinIO and SeaweedFS typically need `AWS_ENDPOINT`, `AWS_ALLOW_HTTP=true`, and
path-style compatible endpoint configuration.

The [`examples/seaweedfs`](../examples/seaweedfs/README.md) stack starts a
local SeaweedFS S3 endpoint and runs the same env-gated integration test used
by CI's MinIO-backed S3-compatible smoke job.

Runnable examples live in
[`crates/borsuk/examples`](../crates/borsuk/examples/local_index.rs),
[`crates/borsuk/examples/s3_index.rs`](../crates/borsuk/examples/s3_index.rs),
[`python/examples`](../python/examples/local_index.py),
[`python/examples/s3_index.py`](../python/examples/s3_index.py),
[`packages/borsuk/examples`](../packages/borsuk/examples/local-index.ts), and
[`packages/borsuk/examples/s3-index.ts`](../packages/borsuk/examples/s3-index.ts).
The S3 examples use `BORSUK_S3_TEST_URI=s3://bucket/prefix`.

## Python API

The Python package is a native extension built with PyO3 and maturin. Python
imports a compiled Rust module and all index operations call the Rust core
directly through FFI.

The binding must stay coarse-grained: Python should call Rust for `create`,
`open`, `add`, `search`, `compact`, and `gc`, not for individual vector rows,
graph nodes, or storage reads. Input vectors should cross the boundary as
contiguous numeric buffers or memory views where practical. Future batch APIs
should use Arrow-compatible schemas/record batches, and can use the Arrow C
Data Interface where a stable batch ABI is needed, so the FFI shape matches the
Parquet storage schema. Python should not use JSON, Avro, Protobuf, or a Rust
CLI subprocess as its data plane.

```python
import borsuk
from array import array

idx = borsuk.create(
    uri="s3://my-bucket/indexes/docs-index",
    metric=borsuk.VectorMetricName.COSINE,
    dim=768,
    segment_size=4096,
    ram_budget="1GB",
    cache_dir="/mnt/nvme/borsuk-cache",
)

generated_ids = idx.add(vectors)
explicit_ids = idx.add(vectors, ids=ids)
buffer_ids = idx.add_buffer(array("f", flat_vectors), ids=ids)
reopened = borsuk.open(
    "s3://my-bucket/indexes/docs-index",
    cache_dir="/mnt/nvme/borsuk-cache",
    ram_budget="2GB",
)
stats = reopened.stats()
print(stats.records, stats.segment_bytes, stats.resident_bytes_estimate)
ids = reopened.search_ids(query, k=20)
vectors = reopened.search_vectors(query, k=20)
vector = reopened.get_vector(ids[0])
print(ids, vectors, vector, generated_ids, explicit_ids, buffer_ids)
ids_from_buffer = reopened.search_ids_buffer(
    array("f", query),
    k=20,
    mode=borsuk.SearchMode.APPROX,
    leaf_mode=borsuk.LeafModeName.GRAPH,
    max_segments=32,
    max_bytes="128MB",
    max_candidates_per_segment=256,
)
vectors_from_buffer = reopened.search_vectors_buffer(
    array("f", query),
    k=20,
    mode=borsuk.SearchMode.APPROX,
    leaf_mode=borsuk.LeafModeName.GRAPH,
    max_segments=32,
    max_bytes="128MB",
    max_candidates_per_segment=256,
)
batch_ids = reopened.search_ids_batch(
    [query, second_query],
    k=20,
    mode=borsuk.SearchMode.APPROX,
    leaf_mode=borsuk.LeafModeName.GRAPH,
    max_segments=32,
    max_bytes="128MB",
    max_candidates_per_segment=256,
)
batch_vectors = reopened.search_vectors_batch(
    [query, second_query],
    k=20,
    mode=borsuk.SearchMode.APPROX,
    leaf_mode=borsuk.LeafModeName.GRAPH,
    max_segments=32,
    max_bytes="128MB",
    max_candidates_per_segment=256,
)
batch_ids_from_buffer = reopened.search_ids_batch_buffer(
    array("f", flat_queries),
    k=20,
    mode=borsuk.SearchMode.APPROX,
    leaf_mode=borsuk.LeafModeName.GRAPH,
    max_segments=32,
    max_bytes="128MB",
    max_candidates_per_segment=256,
)
batch_vectors_from_buffer = reopened.search_vectors_batch_buffer(
    array("f", flat_queries),
    k=20,
    mode=borsuk.SearchMode.APPROX,
    leaf_mode=borsuk.LeafModeName.GRAPH,
    max_segments=32,
    max_bytes="128MB",
    max_candidates_per_segment=256,
)
batch_reports = reopened.search_batch_with_report(
    [query, second_query],
    k=20,
    mode=borsuk.SearchMode.APPROX,
    leaf_mode=borsuk.LeafModeName.GRAPH,
    max_segments=32,
    max_bytes="128MB",
    max_candidates_per_segment=256,
)
batch_reports_from_buffer = reopened.search_batch_with_report_buffer(
    array("f", flat_queries),
    k=20,
    mode=borsuk.SearchMode.APPROX,
    leaf_mode=borsuk.LeafModeName.GRAPH,
    max_segments=32,
    max_bytes="128MB",
    max_candidates_per_segment=256,
)
report = reopened.search_with_report(
    query,
    k=20,
    mode=borsuk.SearchMode.APPROX,
    leaf_mode=borsuk.LeafModeName.GRAPH,
    max_segments=32,
    max_bytes="128MB",
    max_candidates_per_segment=256,
)
report_from_buffer = reopened.search_with_report_buffer(
    array("f", query),
    k=20,
    mode=borsuk.SearchMode.APPROX,
    leaf_mode=borsuk.LeafModeName.GRAPH,
    max_segments=32,
    max_bytes="128MB",
    max_candidates_per_segment=256,
)
print(
    report.hits,
    report.records_scored,
    report.bytes_read,
    report.graph_bytes_read,
    report.object_cache_hits,
    report.object_cache_misses,
    report.graph_candidates_added,
    report.resident_bytes_estimate,
)
compaction = idx.compact(
    source_level=0,
    target_level=1,
    max_segments=32,
    target_segment_max_vectors=65536,
)
print(compaction.segments_read, compaction.object_cache_misses)
gc = idx.gc_obsolete_segments()
print(gc.candidates, gc.bytes_reclaimable)
deleted = idx.gc_obsolete_segments(dry_run=False)
print(deleted.objects_deleted, deleted.bytes_reclaimed)
```

`add` accepts only vectors by default and returns generated string ids. Pass
`ids` when the caller already has identifiers. Record ids must be unique. If
`ids` is omitted, BORSUK returns generated ids that skip any existing
caller-supplied numeric ids. `add_buffer` accepts a flat contiguous float32
buffer laid out row-major using the index's configured dimensions.
`search_ids` returns only ids, `search_vectors` returns stored nearest-neighbor
vectors, and `get_vector` loads one vector by id. `search_ids_buffer` and
`search_vectors_buffer` accept one flat float32 query. `search_ids_batch`,
`search_vectors_batch`, `search_ids_batch_buffer`, and
`search_vectors_batch_buffer` search multiple queries without returning hit
objects. `search_with_report_buffer` accepts one flat float32 query and returns
the same counters as `search_with_report`.
`search_batch_with_report_buffer` returns one report per row-major query.

## TypeScript API

The TypeScript package is a thin wrapper around a Node native extension built
with N-API. Like Python, it must call the Rust core directly and must not spawn
the CLI or exchange JSON with a subprocess. Vector inputs should use typed
arrays or array buffers where practical, with future Arrow-compatible batch APIs
using the same schemas as durable Parquet tables. TypeScript types wrap the
native module; search and insert logic remains in Rust. Avro and Protobuf are
not TypeScript runtime payload formats for index data.

```ts
import { create, LeafModeName, open, SearchMode, VectorMetricName } from "borsuk";

const index = await create({
  uri: "s3://my-bucket/indexes/docs-index",
  metric: VectorMetricName.Cosine,
  dimensions: 768,
  segmentMaxVectors: 4096,
  ramBudget: "1GB",
  cacheDir: "/mnt/nvme/borsuk-cache",
});

const generatedIds = await index.add(vectors);
const explicitIds = await index.add(vectors, ids);
const bufferIds = await index.addBuffer(new Float32Array(flatVectors), ids);
const reopened = open("s3://my-bucket/indexes/docs-index", {
  cacheDir: "/mnt/nvme/borsuk-cache",
  ramBudget: "2GB",
});
const stats = await reopened.stats();
console.log(stats.records, stats.segmentBytes, stats.residentBytesEstimate);
const idsOnly = await reopened.searchIds(query, { k: 20 });
const vectorsOnly = await reopened.searchVectors(query, { k: 20 });
const vector = await reopened.getVector(idsOnly[0]);
console.log(idsOnly, vectorsOnly, vector, generatedIds, explicitIds, bufferIds);
const idsFromBuffer = await reopened.searchIdsBuffer(new Float32Array(query), {
  k: 20,
  mode: SearchMode.Approx,
  leafMode: LeafModeName.Graph,
  maxSegments: 32,
  maxBytes: "128MB",
  maxCandidatesPerSegment: 256,
});
const vectorsFromBuffer = await reopened.searchVectorsBuffer(new Float32Array(query), {
  k: 20,
  mode: SearchMode.Approx,
  leafMode: LeafModeName.Graph,
  maxSegments: 32,
  maxBytes: "128MB",
  maxCandidatesPerSegment: 256,
});
const batchIds = await reopened.searchIdsBatch([query, secondQuery], {
  k: 20,
  mode: SearchMode.Approx,
  leafMode: LeafModeName.Graph,
  maxSegments: 32,
  maxBytes: "128MB",
  maxCandidatesPerSegment: 256,
});
const batchVectors = await reopened.searchVectorsBatch([query, secondQuery], {
  k: 20,
  mode: SearchMode.Approx,
  leafMode: LeafModeName.Graph,
  maxSegments: 32,
  maxBytes: "128MB",
  maxCandidatesPerSegment: 256,
});
const batchIdsFromBuffer = await reopened.searchIdsBatchBuffer(new Float32Array(flatQueries), {
  k: 20,
  mode: SearchMode.Approx,
  leafMode: LeafModeName.Graph,
  maxSegments: 32,
  maxBytes: "128MB",
  maxCandidatesPerSegment: 256,
});
const batchVectorsFromBuffer = await reopened.searchVectorsBatchBuffer(new Float32Array(flatQueries), {
  k: 20,
  mode: SearchMode.Approx,
  leafMode: LeafModeName.Graph,
  maxSegments: 32,
  maxBytes: "128MB",
  maxCandidatesPerSegment: 256,
});
const batchReports = await reopened.searchBatchWithReport([query, secondQuery], {
  k: 20,
  mode: SearchMode.Approx,
  leafMode: LeafModeName.Graph,
  maxSegments: 32,
  maxBytes: "128MB",
  maxCandidatesPerSegment: 256,
});
const batchReportsFromBuffer = await reopened.searchBatchWithReportBuffer(
  new Float32Array(flatQueries),
  {
    k: 20,
    mode: SearchMode.Approx,
    leafMode: LeafModeName.Graph,
    maxSegments: 32,
    maxBytes: "128MB",
    maxCandidatesPerSegment: 256,
  }
);
const report = await reopened.searchWithReport(query, {
  k: 20,
  mode: SearchMode.Approx,
  leafMode: LeafModeName.Graph,
  maxSegments: 32,
  maxBytes: "128MB",
  maxCandidatesPerSegment: 256,
});
const reportFromBuffer = await reopened.searchWithReportBuffer(new Float32Array(query), {
  k: 20,
  mode: SearchMode.Approx,
  leafMode: LeafModeName.Graph,
  maxSegments: 32,
  maxBytes: "128MB",
  maxCandidatesPerSegment: 256,
});
console.log(
  report.hits,
  report.recordsScored,
  report.bytesRead,
  report.graphBytesRead,
  report.objectCacheHits,
  report.objectCacheMisses,
  report.graphCandidatesAdded,
  report.residentBytesEstimate
);
const compaction = await index.compact({
  sourceLevel: 0,
  targetLevel: 1,
  maxSegments: 32,
  targetSegmentMaxVectors: 65536,
});
console.log(compaction.segmentsRead, compaction.segmentsWritten);
const gc = await index.gcObsoleteSegments();
console.log(gc.candidates, gc.bytesReclaimable);
const deleted = await index.gcObsoleteSegments({ dryRun: false });
console.log(deleted.objectsDeleted, deleted.bytesReclaimed);
```

`add` accepts only vectors by default and returns generated string ids. Pass a
string id array directly, or `{ ids }`, when the caller already has
identifiers. Record ids must be unique. If `ids` is omitted, BORSUK returns
generated ids that skip any existing caller-supplied numeric ids. `addBuffer`
accepts the same id forms with flat contiguous `Float32Array` rows using the
index's configured dimensions.
`searchIds` returns only ids, `searchVectors` returns stored nearest-neighbor
vectors, and `getVector` loads one vector by id. `searchIdsBuffer` and
`searchVectorsBuffer` accept one flat `Float32Array` query. `searchIdsBatch`,
`searchVectorsBatch`, `searchIdsBatchBuffer`, and `searchVectorsBatchBuffer`
search multiple queries without returning hit objects. `searchWithReportBuffer`
accepts one flat `Float32Array` query and returns the same counters as
`searchWithReport`.
`searchBatchWithReportBuffer` returns one report per row-major query.

## Metric Names

One physical index has one fixed metric. Built-in dense-vector metric names:

```text
euclidean, l2
squared-euclidean, sqeuclidean, l2-squared
cosine
inner-product, innerproduct, ip, dot, dot-product
angular, angle
manhattan, l1
gower, gower-distance
chebyshev, linf, l-infinity
minkowski:<p>, lp:<p>
canberra
bray-curtis, braycurtis
correlation
hamming
jaccard
dice
simple-matching, simplematching, matching, smc
russell-rao, russellrao
rogers-tanimoto, rogerstanimoto
sokal-sneath, sokalsneath
yule
hellinger
chi-square, chisquare, chi2
kullback-leibler, kullbackleibler, kl, kl-divergence
jeffreys, jeffreys-divergence
jensen-shannon, jensenshannon, js, js-distance
bhattacharyya, bhattacharyya-distance
wasserstein, earth-mover, earthmover, emd
dynamic-time-warping, dynamictimewarping, dtw
ruzicka, weighted-jaccard, weightedjaccard
squared-chord, squaredchord
wave-hedges, wavehedges
lorentzian
clark
```

Python and TypeScript also expose typed vector metric and leaf mode enums,
canonical catalog helpers, direct vector metric helpers, and evaluation helpers for validation,
debugging, reranking, approximate-search recall checks, and non-index use:

```python
borsuk.vector_metric_names()
borsuk.leaf_mode_names()  # ["flat-scan", "sq-scan", "pq-scan", "graph", "vamana-pq", "hybrid"]
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

Python wheels ship `py.typed` and `__init__.pyi`; TypeScript exports
`VectorMetricName`, `LeafModeName`, and `SearchMode` string enums plus
literal/alias types for metric and search configuration. Parameterized
Minkowski remains available as `minkowski:<p>` / `lp:<p>`, with typed
`minkowski_metric(p)` / `minkowskiMetric(p)` helpers for config values.

## Error Types

Runtime failures from the Rust core cross the Python boundary as
`borsuk.BorsukError`, a package-specific subclass of `RuntimeError`. Python
argument-shape errors still use `ValueError`.

The TypeScript package wraps native addon failures in `BorsukError`, an exported
`Error` subclass. The original native error is available as `error.cause` when
the JavaScript runtime provides one.

## Byte Budget Strings

Rust byte helpers, CLI `--ram-budget` / `--max-bytes`, Python `ram_budget` /
`max_bytes`, and TypeScript `ramBudget` / `maxBytes` accept integer byte counts
with optional units: `B`, `KB`, `MB`, `GB`, `TB`, `KiB`, `MiB`, `GiB`, or
`TiB`. Resident RAM budgets are enforced by the Rust core against resident
index metadata. Search byte budgets limit persisted segment payload reads
during approximate search. Approximate-search budgets such as `max_segments`,
`max_bytes`, `max_latency_ms`, and `max_candidates_per_segment` must be greater
than zero when set; `eps` must be finite and non-negative.

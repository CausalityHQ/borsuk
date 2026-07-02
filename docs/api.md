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
- `BorsukIndex::add(Vec<VectorRecord>)` writes immutable L0 segments. Records
  can carry an optional `payload_ref` pointing to an external durable object or
  payload shard; the reference is stored in the segment table.
- `BorsukIndex::search(query, SearchOptions)` returns top-k hits, including
  any stored `payload_ref`.
- `BorsukIndex::search_batch(queries, SearchOptions)` searches multiple
  queries with one Rust call and returns one hit list per query in input order.
- `BorsukIndex::search_with_report(query, SearchOptions)` returns top-k hits plus
  execution counters: segments ranked, segments searched, segments skipped,
  segment bytes read, graph bytes read, object-cache hits and misses, records
  considered, records exact-scored, and elapsed milliseconds, plus an estimate
  of resident manifest/routing memory.
- `BorsukIndex::search_batch_with_report(queries, SearchOptions)` returns the
  same execution counters for each query in input order.
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

## CLI

The `borsuk` binary is optional administration/debug tooling. It must not be
used as the Python or TypeScript runtime transport. CLI JSON output is for
humans and automation scripts only; it is not the storage format and not the
embedding ABI.

```bash
borsuk create --uri file:///tmp/docs.borsuk --metric euclidean --dimensions 2 --ram-budget 1GB
borsuk add --uri file:///tmp/docs.borsuk --input records.json
borsuk stats --uri file:///tmp/docs.borsuk
borsuk search --uri file:///tmp/docs.borsuk --query '[0.2,0.0]' --k 2
borsuk search --uri file:///tmp/docs.borsuk --query '[0.2,0.0]' --mode approx --max-bytes 128MB
borsuk search --uri file:///tmp/docs.borsuk --query '[0.2,0.0]' --mode approx --report
borsuk search --uri s3://bucket/docs.borsuk --query '[0.2,0.0]' --cache-dir /mnt/nvme/borsuk-cache --report
borsuk compact --uri file:///tmp/docs.borsuk --source-level 0 --target-level 1 --cache-dir /mnt/nvme/borsuk-cache
borsuk gc --uri file:///tmp/docs.borsuk
borsuk gc --uri file:///tmp/docs.borsuk --delete
```

For S3-compatible storage, use `s3://bucket/prefix` and configure credentials,
endpoint, HTTP allowance, and region through `AWS_*` environment variables.
MinIO and SeaweedFS typically need `AWS_ENDPOINT`, `AWS_ALLOW_HTTP=true`, and
path-style compatible endpoint configuration.

The [`examples/seaweedfs`](../examples/seaweedfs/README.md) stack starts a
local SeaweedFS S3 endpoint and runs the same env-gated integration test used
by CI's MinIO-backed S3-compatible smoke job.

Runnable examples live in
[`crates/borsuk/examples`](../crates/borsuk/examples/local_index.rs),
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

idx = borsuk.create(
    uri="s3://my-bucket/indexes/docs.borsuk",
    metric="cosine",
    dim=768,
    segment_size=4096,
    ram_budget="1GB",
    cache_dir="/mnt/nvme/borsuk-cache",
)

idx.add(ids, vectors, payload_refs=payload_refs)
reopened = borsuk.open(
    "s3://my-bucket/indexes/docs.borsuk",
    cache_dir="/mnt/nvme/borsuk-cache",
    ram_budget="2GB",
)
stats = reopened.stats()
print(stats.records, stats.segment_bytes, stats.resident_bytes_estimate)
hits = reopened.search(
    query,
    k=20,
    mode="approx",
    max_segments=32,
    max_bytes="128MB",
    max_candidates_per_segment=256,
)
print(hits[0].id, hits[0].distance, hits[0].payload_ref)
batch_hits = reopened.search_batch(
    [query, second_query],
    k=20,
    mode="approx",
    max_segments=32,
    max_bytes="128MB",
    max_candidates_per_segment=256,
)
batch_reports = reopened.search_batch_with_report(
    [query, second_query],
    k=20,
    mode="approx",
    max_segments=32,
    max_bytes="128MB",
    max_candidates_per_segment=256,
)
report = reopened.search_with_report(
    query,
    k=20,
    mode="approx",
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

`payload_refs` is optional. When provided, it must have the same length as the
ids and vectors, and individual entries may be `None` for records that do not
point at an external payload object.

## TypeScript API

The TypeScript package is a thin wrapper around a Node native extension built
with N-API. Like Python, it must call the Rust core directly and must not spawn
the CLI or exchange JSON with a subprocess. Vector inputs should use typed
arrays or array buffers where practical, with future Arrow-compatible batch APIs
using the same schemas as durable Parquet tables. TypeScript types wrap the
native module; search and insert logic remains in Rust. Avro and Protobuf are
not TypeScript runtime payload formats for index data.

```ts
import { create, open } from "borsuk";

const index = await create({
  uri: "s3://my-bucket/indexes/docs.borsuk",
  metric: "cosine",
  dimensions: 768,
  segmentMaxVectors: 4096,
  ramBudget: "1GB",
  cacheDir: "/mnt/nvme/borsuk-cache",
});

await index.add(ids, vectors, { payloadRefs });
const reopened = open("s3://my-bucket/indexes/docs.borsuk", {
  cacheDir: "/mnt/nvme/borsuk-cache",
  ramBudget: "2GB",
});
const stats = await reopened.stats();
console.log(stats.records, stats.segmentBytes, stats.residentBytesEstimate);
const hits = await reopened.search(query, {
  k: 20,
  mode: "approx",
  maxSegments: 32,
  maxBytes: "128MB",
  maxCandidatesPerSegment: 256,
});
console.log(hits[0].id, hits[0].distance, hits[0].payloadRef);
const batchHits = await reopened.searchBatch([query, secondQuery], {
  k: 20,
  mode: "approx",
  maxSegments: 32,
  maxBytes: "128MB",
  maxCandidatesPerSegment: 256,
});
const batchReports = await reopened.searchBatchWithReport([query, secondQuery], {
  k: 20,
  mode: "approx",
  maxSegments: 32,
  maxBytes: "128MB",
  maxCandidatesPerSegment: 256,
});
const report = await reopened.searchWithReport(query, {
  k: 20,
  mode: "approx",
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

`payloadRefs` is optional. When provided, it must have the same length as the
ids and vectors, and individual entries may be `null` or `undefined` for
records that do not point at an external payload object. Search hits expose
missing refs as `payloadRef: null`.

## Metric Names

One physical index has one fixed metric. Built-in dense-vector metric names:

```text
euclidean, l2
squared-euclidean, sqeuclidean, l2-squared
cosine
inner-product, innerproduct, ip, dot, dot-product
angular, angle
manhattan, l1
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

Python and TypeScript also expose direct metric and evaluation helpers for
validation, debugging, reranking, approximate-search recall checks, and
non-index use:

```python
borsuk.vector_distance("minkowski:3", [0.0, 0.0], [1.0, 2.0])
borsuk.string_distance("jaro-winkler", "segment", "segments")
borsuk.recall_at_k(["doc-a", "doc-b"], ["doc-b", "doc-x"], 2)
```

```ts
vectorDistance("cosine", [1, 0], [1, 0]);
stringDistance("damerau-levenshtein", "abcd", "acbd");
recallAtK(["doc-a", "doc-b"], ["doc-b", "doc-x"], 2);
```

Built-in string metric names are: `levenshtein`,
`normalized-levenshtein`, `damerau-levenshtein`,
`normalized-damerau-levenshtein`, `optimal-string-alignment`, `hamming`,
`jaro`, `jaro-winkler`, and `sorensen-dice`.

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
during approximate search.

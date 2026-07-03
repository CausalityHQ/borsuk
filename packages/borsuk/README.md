# borsuk

TypeScript API for BORSUK.

The package loads a Rust N-API native addon. The CLI is not used by the runtime
API.

Supported Node versions are 22, 24, and 26 on Linux x64, Linux arm64, Windows
x64, macOS arm64, and macOS Intel runners. The package metadata declares
`node >=22 <27`.

Run the local example from the repository with:

```bash
npm run example:local
```

Run the S3-compatible example with `BORSUK_S3_TEST_URI` and AWS/object-store
environment variables set:

```bash
npm run example:s3
```

```ts
import {
  BorsukError,
  create,
  leafModeNames,
  LeafModeName,
  minkowskiMetric,
  open,
  recallAtK,
  SearchMode,
  vectorDistance,
  VectorMetricName,
  vectorMetricNames
} from "borsuk";

const index = await create({
  uri: "file:///tmp/docs-index",
  metric: VectorMetricName.Euclidean,
  dimensions: 2,
  ramBudget: "1GB",
  cacheDir: "/tmp/borsuk-cache"
});

await index.add([[0, 0], [1, 0]], ["a", "b"]);
await index.addBuffer(new Float32Array([2, 0, 3, 0]), ["c", "d"]);
const reopened = open("file:///tmp/docs-index", {
  cacheDir: "/tmp/borsuk-cache",
  ramBudget: "2GB"
});
const ids = await reopened.searchIds([0.1, 0], { k: 1 });
const vectors = await reopened.searchVectors([0.1, 0], { k: 1 });
const vector = await reopened.getVector("a");
const bufferIds = await reopened.searchIdsBuffer(new Float32Array([0.1, 0]), { k: 1 });
const bufferVectors = await reopened.searchVectorsBuffer(new Float32Array([0.1, 0]), { k: 1 });
const batchIds = await reopened.searchIdsBatch([[0.1, 0], [0.9, 0]], { k: 1 });
const batchVectors = await reopened.searchVectorsBatch([[0.1, 0], [0.9, 0]], { k: 1 });
const bufferBatchIds = await reopened.searchIdsBatchBuffer(new Float32Array([0.1, 0, 0.9, 0]), {
  k: 1
});
const bufferBatchVectors = await reopened.searchVectorsBatchBuffer(new Float32Array([0.1, 0, 0.9, 0]), {
  k: 1
});
const batchReports = await reopened.searchBatchWithReport([[0.1, 0], [0.9, 0]], { k: 1 });
const bufferBatchReports = await reopened.searchBatchWithReportBuffer(
  new Float32Array([0.1, 0, 0.9, 0]),
  { k: 1 }
);
const stats = await reopened.stats();
const minkowski = minkowskiMetric(3);
const exactDistance = vectorDistance(VectorMetricName.Cosine, [1, 0], [1, 0]);
const minkowskiDistance = vectorDistance(minkowski, [0, 0], [1, 2]);
const vectorMetrics = vectorMetricNames();
const leafModes = leafModeNames();
const recall = recallAtK(["a"], ids, 1);
const report = await reopened.searchWithReport([0.1, 0], {
  k: 1,
  mode: SearchMode.Approx,
  leafMode: LeafModeName.Graph,
  maxBytes: "128MB",
  maxCandidatesPerSegment: 64
});
const bufferReport = await reopened.searchWithReportBuffer(new Float32Array([0.1, 0]), {
  k: 1,
  mode: SearchMode.Approx,
  maxBytes: "128MB",
  maxCandidatesPerSegment: 64
});
console.log(
  report.hits,
  ids,
  vectors,
  vector,
  bufferIds,
  bufferVectors,
  batchIds,
  batchVectors,
  bufferBatchIds,
  bufferBatchVectors,
  report.recordsScored,
  report.bytesRead,
  report.graphBytesRead,
  report.objectCacheHits,
  report.objectCacheMisses,
  report.graphCandidatesAdded,
  stats.residentBytesEstimate
);
const compaction = await index.compact({
  sourceLevel: 0,
  targetLevel: 1,
  maxSegments: 32
});
console.log(compaction.segmentsRead, compaction.segmentsWritten);
const rebuild = await index.rebuild({
  sourceLevel: 0,
  targetLevel: 1,
  deleteObsolete: true
});
console.log(rebuild.compaction.recordsRewritten, rebuild.garbageCollection.objectsDeleted);
const gc = await index.gcObsoleteSegments();
console.log(gc.candidates, gc.bytesReclaimable);

try {
  vectorDistance("euclidean", [1], [1, 2]);
} catch (error) {
  if (error instanceof BorsukError) {
    console.error(error.message);
  }
}
```

`ramBudget` can be set on create or open. `ramBudget` and `maxBytes` accept
integer byte counts with `B`, decimal `KB`/`MB`/`GB`/`TB`, or binary
`KiB`/`MiB`/`GiB`/`TiB` units. Resident budgets are enforced in the Rust core
against manifest, routing, and pivot metadata. Approximate-search budgets such
as `maxSegments`, `maxBytes`, `maxLatencyMs`, and
`maxCandidatesPerSegment` must be greater than zero when set; `eps` must be
finite and non-negative.

`cacheDir` is a read-through local cache. Opens read `CURRENT` from backing
storage and use its checksums to refetch stale or corrupt cached active
manifest/routing/pivot metadata before returning an index handle.
Cached segment, graph, and routing page payloads are also checksum-validated
and repaired from backing storage when only the local cache copy is corrupt.

Open large object-store indexes with `residentRouting: false` to keep segment
summaries and pivots out of the resident manifest and resolve summaries from
routing pages:

```ts
const index = open("s3://bucket/index", {
  residentRouting: false,
  ramBudget: "512MB"
});
```

`add` accepts only vectors by default and returns generated string ids. Pass
`string`, `Uint8Array`, `number`, or `bigint` ids directly, or `{ ids }`, when
the caller already has identifiers. Record ids must be unique. Generated ids
skip existing caller-supplied decimal-string ids; explicit integer ids are
encoded as compact unsigned varint bytes, and `searchIdBytes` returns those
canonical bytes. `addBuffer` accepts the same id forms with flat contiguous
`Float32Array` rows using the index's configured dimensions.
`searchIds` returns only ids, `searchVectors` returns stored nearest-neighbor
vectors, and `getVector` loads one vector by id. `searchIdsBuffer` and
`searchVectorsBuffer` accept one flat `Float32Array` query. `searchIdsBatch`,
`searchVectorsBatch`, `searchIdsBatchBuffer`, and `searchVectorsBatchBuffer`
search multiple queries without returning hit objects. `searchWithReportBuffer`
accepts one flat `Float32Array` query and returns the same counters as
`searchWithReport`. Report hits expose `idBytes` for arbitrary binary or
integer-encoded ids; non-UTF8 ids use a `0x...` display string in `id`.
`searchBatchWithReportBuffer` returns one report per row-major query.

`compact` is bounded by default. Pass `maxSegments` to tune incremental
maintenance, and keep `minSegments <= maxSegments` when both are set. It reads
the selected source leaf payloads plus needed routing metadata, rebuilds graph
blocks from those records, and leaves unrelated leaves and old graph payloads
unread. Use `rebuild` for an explicit full
source-level rewrite; `deleteObsolete` controls whether inactive segment and
graph objects are reported only or also deleted.

The TypeScript package exports `VectorMetricName`, `LeafModeName`, and
`SearchMode` string enums plus literal/alias types for metric and search
configuration. Use `minkowskiMetric(p)` for parameterized Minkowski configs.
`vectorMetricNames()` and `leafModeNames()` expose the canonical runtime
catalogs. Implemented leaf modes are `flat-scan`, `sq-scan`, `pq-scan`,
`graph`, `vamana-pq`, and `hybrid`.

## License

The TypeScript package is distributed under the Business Source License 1.1 with
a revenue-limited Additional Use Grant. See `LICENSE`.

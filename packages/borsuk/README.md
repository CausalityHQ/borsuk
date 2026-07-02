# borsuk

TypeScript API for BORSUK.

The package loads a Rust N-API native addon. The CLI is not used by the runtime
API.

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
import { BorsukError, create, open, recallAtK, stringDistance, vectorDistance } from "borsuk";

const index = await create({
  uri: "file:///tmp/docs.borsuk",
  metric: "euclidean",
  dimensions: 2,
  ramBudget: "1GB",
  cacheDir: "/tmp/borsuk-cache"
});

await index.add(["a", "b"], [[0, 0], [1, 0]], { payloadRefs: ["objects/a.parquet", null] });
const reopened = open("file:///tmp/docs.borsuk", {
  cacheDir: "/tmp/borsuk-cache",
  ramBudget: "2GB"
});
const hits = await reopened.search([0.1, 0], { k: 1 });
const batchHits = await reopened.searchBatch([[0.1, 0], [0.9, 0]], { k: 1 });
const batchReports = await reopened.searchBatchWithReport([[0.1, 0], [0.9, 0]], { k: 1 });
const stats = await reopened.stats();
const exactDistance = vectorDistance("cosine", [1, 0], [1, 0]);
const editDistance = stringDistance("jaro-winkler", "segment", "segments");
const recall = recallAtK(["a"], hits.map((hit) => hit.id), 1);
const report = await reopened.searchWithReport([0.1, 0], {
  k: 1,
  mode: "approx",
  maxBytes: "128MB",
  maxCandidatesPerSegment: 64
});
console.log(
  report.hits,
  hits[0]?.payloadRef,
  report.recordsScored,
  report.bytesRead,
  report.graphBytesRead,
  report.objectCacheHits,
  report.objectCacheMisses,
  report.graphCandidatesAdded,
  stats.residentBytesEstimate
);
const compaction = await index.compact({ sourceLevel: 0, targetLevel: 1 });
console.log(compaction.segmentsRead, compaction.segmentsWritten);
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

`payloadRefs` is optional; if present it must match the id/vector count, and
entries may be `null` or `undefined` for records without external payloads.
Search hits expose missing refs as `payloadRef: null`.

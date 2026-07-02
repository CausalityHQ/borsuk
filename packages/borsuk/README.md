# borsuk

TypeScript API for BORSUK.

The package loads a Rust N-API native addon. The CLI is not used by the runtime
API.

Run the local example from the repository with:

```bash
npm run example:local
```

```ts
import { BorsukError, create, open, stringDistance, vectorDistance } from "borsuk";

const index = await create({
  uri: "file:///tmp/docs.borsuk",
  metric: "euclidean",
  dimensions: 2,
  ramBudget: "1GB",
  cacheDir: "/tmp/borsuk-cache"
});

await index.add(["a"], [[0, 0]]);
const reopened = open("file:///tmp/docs.borsuk", {
  cacheDir: "/tmp/borsuk-cache",
  ramBudget: "2GB"
});
const hits = await reopened.search([0.1, 0], { k: 1 });
const exactDistance = vectorDistance("cosine", [1, 0], [1, 0]);
const editDistance = stringDistance("jaro-winkler", "segment", "segments");
const report = await reopened.searchWithReport([0.1, 0], {
  k: 1,
  mode: "approx",
  maxBytes: "128MB",
  maxCandidatesPerSegment: 64
});
console.log(
  report.hits,
  report.recordsScored,
  report.bytesRead,
  report.graphBytesRead,
  report.objectCacheHits,
  report.objectCacheMisses,
  report.graphCandidatesAdded
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
against manifest, routing, and pivot metadata.

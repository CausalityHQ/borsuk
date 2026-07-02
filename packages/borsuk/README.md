# borsuk

TypeScript API for BORSUK.

The package loads a Rust N-API native addon. The CLI is not used by the runtime
API.

```ts
import { create } from "borsuk";

const index = await create({
  uri: "file:///tmp/docs.borsuk",
  metric: "euclidean",
  dimensions: 2,
  cacheDir: "/tmp/borsuk-cache"
});

await index.add(["a"], [[0, 0]]);
const hits = await index.search([0.1, 0], { k: 1 });
const report = await index.searchWithReport([0.1, 0], {
  k: 1,
  mode: "approx",
  maxCandidatesPerSegment: 64
});
console.log(
  report.hits,
  report.recordsScored,
  report.bytesRead,
  report.graphBytesRead,
  report.graphCandidatesAdded
);
const compaction = await index.compact({ sourceLevel: 0, targetLevel: 1 });
console.log(compaction.segmentsRead, compaction.segmentsWritten);
const gc = await index.gcObsoleteSegments();
console.log(gc.candidates, gc.bytesReclaimable);
```

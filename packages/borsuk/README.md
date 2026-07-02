# borsuk

TypeScript API for BORSUK.

The package loads a Rust N-API native addon. The CLI is not used by the runtime
API.

Run the local example from the repository with:

```bash
npm run example:local
```

```ts
import { create, stringDistance, vectorDistance } from "borsuk";

const index = await create({
  uri: "file:///tmp/docs.borsuk",
  metric: "euclidean",
  dimensions: 2,
  ramBudget: "1GB",
  cacheDir: "/tmp/borsuk-cache"
});

await index.add(["a"], [[0, 0]]);
const hits = await index.search([0.1, 0], { k: 1 });
const exactDistance = vectorDistance("cosine", [1, 0], [1, 0]);
const editDistance = stringDistance("jaro-winkler", "segment", "segments");
const report = await index.searchWithReport([0.1, 0], {
  k: 1,
  mode: "approx",
  maxBytes: 128 * 1024 * 1024,
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

`ramBudget` accepts integer byte counts with `B`, decimal `KB`/`MB`/`GB`/`TB`,
or binary `KiB`/`MiB`/`GiB`/`TiB` units. It is enforced in the Rust core against
resident manifest, routing, and pivot metadata.

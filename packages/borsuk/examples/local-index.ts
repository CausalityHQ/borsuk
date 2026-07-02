import { create, stringDistance, vectorDistance } from "../src/index.js";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

async function main(): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "borsuk-ts-index-"));
  const index = await create({
    uri: `file://${root}`,
    metric: "cosine",
    dimensions: 3,
    segmentMaxVectors: 2
  });

  await index.add(
    ["alpha", "beta", "gamma"],
    [
      [1, 0, 0],
      [0.9, 0.1, 0],
      [0, 1, 0]
    ]
  );

  const report = await index.searchWithReport([1, 0, 0], {
    k: 2,
    mode: "approx",
    maxCandidatesPerSegment: 2
  });
  const ids = report.hits.map((hit) => hit.id);
  if (ids.join(",") !== "alpha,beta") {
    throw new Error(`unexpected hits: ${ids.join(",")}`);
  }
  if (report.bytesRead <= 0) {
    throw new Error("expected the example to read segment bytes");
  }

  const cosine = vectorDistance("cosine", [1, 0], [1, 0]);
  const edit = stringDistance("jaro-winkler", "segment", "segments");
  if (cosine !== 0 || edit <= 0 || edit >= 0.2) {
    throw new Error("metric helpers returned unexpected values");
  }

  console.log(
    `hits=${ids.join(",")} bytesRead=${report.bytesRead} recordsScored=${report.recordsScored}`
  );
}

await main();

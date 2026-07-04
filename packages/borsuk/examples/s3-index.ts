import { randomUUID } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { create, LeafModeName, open, SearchMode, VectorMetricName } from "../src/index.js";

async function main(): Promise<void> {
  const baseUri = process.env.BORSUK_S3_TEST_URI;
  if (!baseUri) {
    throw new Error("set BORSUK_S3_TEST_URI=s3://bucket/prefix before running this example");
  }

  const uri = `${baseUri.replace(/\/+$/, "")}/typescript-example-${randomUUID()}`;
  const cache = mkdtempSync(join(tmpdir(), "borsuk-ts-s3-cache-"));
  const index = await create({
    uri,
    metric: VectorMetricName.Euclidean,
    dimensions: 2,
    segmentMaxVectors: 2
  });

  await index.add(
    [[0, 0], [0, 0.1], [0.1, -0.1], [100, 100]],
    { ids: ["entry", "true-neighbor", "routing-decoy", "far"] }
  );

  const reopened = open(uri, { cacheDir: cache });
  const report = await reopened.searchWithReport([0.04, 0.07], {
    k: 1,
    mode: SearchMode.Approx,
    leafMode: LeafModeName.Graph,
    maxCandidatesPerSegment: 2
  });
  if (report.hits[0]?.id !== "true-neighbor") {
    throw new Error(`unexpected hit: ${report.hits[0]?.id}`);
  }
  const vector = await reopened.getVector("true-neighbor");
  const roundedVector = vector?.map((value) => Number(value.toFixed(6)));
  if (JSON.stringify(roundedVector) !== JSON.stringify([0, 0.1])) {
    throw new Error(`unexpected vector: ${JSON.stringify(vector)}`);
  }
  if (report.bytesRead <= 0 || report.graphBytesRead <= 0 || report.objectCacheMisses <= 0) {
    throw new Error(`unexpected search counters: ${JSON.stringify(report)}`);
  }

  const compaction = await reopened.compact({
    sourceLevel: 0,
    targetLevel: 1,
    maxSegments: 2,
    minSegments: 2,
    targetSegmentMaxVectors: 4
  });
  if (!compaction.compacted) {
    throw new Error(`expected compaction to rewrite segments: ${JSON.stringify(compaction)}`);
  }

  const gc = await reopened.gcObsoleteSegments({ minAgeMs: 0 });
  if (!gc.dryRun || gc.candidates.length === 0) {
    throw new Error(`expected obsolete segment candidates: ${JSON.stringify(gc)}`);
  }

  console.log(
    `uri=${uri} hit=${report.hits[0].id} bytesRead=${report.bytesRead} graphBytesRead=${report.graphBytesRead} objectCacheMisses=${report.objectCacheMisses} compacted=${compaction.compacted} gcCandidates=${gc.candidates.length}`
  );
}

await main();

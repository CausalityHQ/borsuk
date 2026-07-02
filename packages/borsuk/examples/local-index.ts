import { create, recallAtK, stringDistance, vectorDistance } from "../src/index.js";
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
    ],
    {
      payloadRefs: ["objects/alpha.parquet", "objects/beta.parquet", "objects/gamma.parquet"]
    }
  );
  const stats = await index.stats();
  if (
    stats.metric !== "cosine" ||
    stats.dimensions !== 3 ||
    stats.segments !== 2 ||
    stats.records !== 3 ||
    stats.segmentBytes <= 0 ||
    stats.graphBytes <= 0
  ) {
    throw new Error(`unexpected index stats: ${JSON.stringify(stats)}`);
  }

  const report = await index.searchWithReport([1, 0, 0], {
    k: 2,
    mode: "approx",
    maxCandidatesPerSegment: 2
  });
  const ids = report.hits.map((hit) => hit.id);
  if (ids.join(",") !== "alpha,beta") {
    throw new Error(`unexpected hits: ${ids.join(",")}`);
  }
  const payloadRefs = report.hits.map((hit) => hit.payloadRef);
  if (payloadRefs.join(",") !== "objects/alpha.parquet,objects/beta.parquet") {
    throw new Error(`unexpected payload refs: ${payloadRefs.join(",")}`);
  }
  if (report.bytesRead <= 0) {
    throw new Error("expected the example to read segment bytes");
  }
  const batch = await index.searchBatch([[1, 0, 0], [0, 1, 0]], { k: 1 });
  const batchIds = batch.map((hits) => hits.map((hit) => hit.id).join(","));
  if (batchIds.join("|") !== "alpha|gamma") {
    throw new Error(`unexpected batch hits: ${batchIds.join("|")}`);
  }
  const batchReports = await index.searchBatchWithReport([[1, 0, 0], [0, 1, 0]], { k: 1 });
  const batchReportIds = batchReports.map((batchReport) => batchReport.hits[0]?.id);
  if (batchReportIds.join("|") !== "alpha|gamma") {
    throw new Error(`unexpected batch report hits: ${batchReportIds.join("|")}`);
  }
  if (!batchReports.every((batchReport) => batchReport.bytesRead > 0)) {
    throw new Error("expected batch reports to include segment bytes");
  }

  const cosine = vectorDistance("cosine", [1, 0], [1, 0]);
  const edit = stringDistance("jaro-winkler", "segment", "segments");
  const recall = recallAtK(["alpha", "beta"], ids, 2);
  if (cosine !== 0 || edit <= 0 || edit >= 0.2 || recall !== 1) {
    throw new Error("metric helpers returned unexpected values");
  }

  console.log(
    `hits=${ids.join(",")} bytesRead=${report.bytesRead} recallAt2=${recall} objectCacheHits=${report.objectCacheHits} objectCacheMisses=${report.objectCacheMisses} recordsScored=${report.recordsScored} residentBytesEstimate=${report.residentBytesEstimate} segmentBytes=${stats.segmentBytes}`
  );
}

await main();

import {
  create,
  recallAtK,
  SearchMode,
  stringDistance,
  StringMetricName,
  stringMetricNames,
  vectorDistance,
  VectorMetricName,
  vectorMetricNames
} from "../src/index.js";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

async function main(): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "borsuk-ts-index-"));
  const index = await create({
    uri: `file://${root}`,
    metric: VectorMetricName.Cosine,
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
      payloadRefs: ["objects/alpha.parquet", null, "objects/gamma.parquet"]
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
    mode: SearchMode.Approx,
    maxCandidatesPerSegment: 2
  });
  const ids = report.hits.map((hit) => hit.id);
  if (ids.join(",") !== "alpha,beta") {
    throw new Error(`unexpected hits: ${ids.join(",")}`);
  }
  const bufferHits = await index.searchBuffer(new Float32Array([1, 0, 0]), { k: 2 });
  const bufferHitIds = bufferHits.map((hit) => hit.id);
  if (bufferHitIds.join(",") !== ids.join(",")) {
    throw new Error(`unexpected buffer hits: ${bufferHitIds.join(",")}`);
  }
  const bufferReport = await index.searchWithReportBuffer(new Float32Array([1, 0, 0]), {
    k: 2,
    mode: SearchMode.Approx,
    maxCandidatesPerSegment: 2
  });
  const bufferReportIds = bufferReport.hits.map((hit) => hit.id);
  if (bufferReportIds.join(",") !== ids.join(",")) {
    throw new Error(`unexpected buffer report hits: ${bufferReportIds.join(",")}`);
  }
  if (bufferReport.bytesRead <= 0) {
    throw new Error("expected the buffer report to read segment bytes");
  }
  const payloadRefs = report.hits.map((hit) => hit.payloadRef);
  if (JSON.stringify(payloadRefs) !== JSON.stringify(["objects/alpha.parquet", null])) {
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
  const bufferBatch = await index.searchBatchBuffer(new Float32Array([1, 0, 0, 0, 1, 0]), {
    k: 1
  });
  const bufferBatchIds = bufferBatch.map((hits) => hits.map((hit) => hit.id).join(","));
  if (bufferBatchIds.join("|") !== "alpha|gamma") {
    throw new Error(`unexpected buffer batch hits: ${bufferBatchIds.join("|")}`);
  }
  const batchReports = await index.searchBatchWithReport([[1, 0, 0], [0, 1, 0]], { k: 1 });
  const batchReportIds = batchReports.map((batchReport) => batchReport.hits[0]?.id);
  if (batchReportIds.join("|") !== "alpha|gamma") {
    throw new Error(`unexpected batch report hits: ${batchReportIds.join("|")}`);
  }
  if (!batchReports.every((batchReport) => batchReport.bytesRead > 0)) {
    throw new Error("expected batch reports to include segment bytes");
  }
  const bufferBatchReports = await index.searchBatchWithReportBuffer(
    new Float32Array([1, 0, 0, 0, 1, 0]),
    { k: 1 }
  );
  const bufferBatchReportIds = bufferBatchReports.map((batchReport) => batchReport.hits[0]?.id);
  if (bufferBatchReportIds.join("|") !== "alpha|gamma") {
    throw new Error(`unexpected buffer batch report hits: ${bufferBatchReportIds.join("|")}`);
  }
  if (!bufferBatchReports.every((batchReport) => batchReport.bytesRead > 0)) {
    throw new Error("expected buffer batch reports to include segment bytes");
  }

  if (!(vectorMetricNames() as readonly string[]).includes(VectorMetricName.Cosine)) {
    throw new Error("expected cosine in vector metric catalog");
  }
  if (!(stringMetricNames() as readonly string[]).includes(StringMetricName.JaroWinkler)) {
    throw new Error("expected jaro-winkler in string metric catalog");
  }
  const cosine = vectorDistance(VectorMetricName.Cosine, [1, 0], [1, 0]);
  const edit = stringDistance(StringMetricName.JaroWinkler, "segment", "segments");
  const recall = recallAtK(["alpha", "beta"], ids, 2);
  if (cosine !== 0 || edit <= 0 || edit >= 0.2 || recall !== 1) {
    throw new Error("metric helpers returned unexpected values");
  }

  console.log(
    `hits=${ids.join(",")} bytesRead=${report.bytesRead} recallAt2=${recall} objectCacheHits=${report.objectCacheHits} objectCacheMisses=${report.objectCacheMisses} recordsScored=${report.recordsScored} residentBytesEstimate=${report.residentBytesEstimate} segmentBytes=${stats.segmentBytes}`
  );
}

await main();

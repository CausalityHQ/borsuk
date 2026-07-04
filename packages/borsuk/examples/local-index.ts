import {
  create,
  leafModeNames,
  LeafModeName,
  recallAtK,
  SearchMode,
  tieAwareRecallAtK,
  vectorDistance,
  VectorMetricName,
  vectorMetricNames
} from "../src/index.js";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

async function main(): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "borsuk-ts-index-"));
  const index = await create({
    uri: pathToFileURL(root).href,
    metric: VectorMetricName.Cosine,
    dimensions: 3,
    segmentMaxVectors: 3
  });

  await index.add(
    [
      [1, 0, 0],
      [0.9, 0.1, 0],
      [0, 1, 0]
    ],
    ["alpha", "beta", "gamma"]
  );
  const stats = await index.stats();
  if (
    stats.metric !== "cosine" ||
    stats.dimensions !== 3 ||
    stats.segments !== 1 ||
    stats.records !== 3 ||
    stats.segmentBytes <= 0 ||
    stats.graphBytes <= 0
  ) {
    throw new Error(`unexpected index stats: ${JSON.stringify(stats)}`);
  }

  const report = await index.searchWithReport([1, 0, 0], {
    k: 2,
    mode: SearchMode.Approx,
    leafMode: LeafModeName.Graph,
    maxCandidatesPerSegment: 3
  });
  const ids = report.hits.map((hit) => hit.id);
  if (ids.join(",") !== "alpha,beta") {
    throw new Error(`unexpected hits: ${ids.join(",")}`);
  }
  if (report.leafMode !== "graph" || report.graphBytesRead <= 0) {
    throw new Error(`unexpected graph report: ${JSON.stringify(report)}`);
  }
  const exactReport = await index.searchWithReport([1, 0, 0], {
    k: 2,
    mode: SearchMode.Exact
  });
  const vamanaPqReport = await index.searchWithReport([1, 0, 0], {
    k: 2,
    mode: SearchMode.Approx,
    leafMode: LeafModeName.VamanaPq,
    maxCandidatesPerSegment: 3
  });
  const vamanaPqIds = vamanaPqReport.hits.map((hit) => hit.id);
  if (vamanaPqReport.leafMode !== "vamana-pq" || vamanaPqIds.join(",") !== ids.join(",")) {
    throw new Error(`unexpected vamana-pq report: ${JSON.stringify(vamanaPqReport)}`);
  }
  if (vamanaPqReport.graphBytesRead <= 0) {
    throw new Error("expected vamana-pq to read graph bytes");
  }
  const hybridReport = await index.searchWithReport([1, 0, 0], {
    k: 2,
    mode: SearchMode.Approx,
    leafMode: LeafModeName.Hybrid,
    maxCandidatesPerSegment: 3
  });
  const hybridIds = hybridReport.hits.map((hit) => hit.id);
  if (hybridReport.leafMode !== "hybrid" || hybridIds.join(",") !== ids.join(",")) {
    throw new Error(`unexpected hybrid report: ${JSON.stringify(hybridReport)}`);
  }
  if (hybridReport.graphBytesRead <= 0) {
    throw new Error("expected hybrid to read graph bytes for graph-backed segments");
  }
  const pqReport = await index.searchWithReport([1, 0, 0], {
    k: 2,
    mode: SearchMode.Approx,
    leafMode: LeafModeName.PqScan,
    maxCandidatesPerSegment: 2
  });
  const pqIds = pqReport.hits.map((hit) => hit.id);
  if (pqReport.leafMode !== "pq-scan" || pqIds.join(",") !== ids.join(",")) {
    throw new Error(`unexpected pq-scan report: ${JSON.stringify(pqReport)}`);
  }
  if (pqReport.graphBytesRead !== 0) {
    throw new Error("expected pq-scan to skip graph bytes");
  }
  const sqReport = await index.searchWithReport([1, 0, 0], {
    k: 2,
    mode: SearchMode.Approx,
    leafMode: LeafModeName.SqScan,
    maxCandidatesPerSegment: 2
  });
  const sqIds = sqReport.hits.map((hit) => hit.id);
  if (sqReport.leafMode !== "sq-scan" || sqIds.join(",") !== ids.join(",")) {
    throw new Error(`unexpected sq-scan report: ${JSON.stringify(sqReport)}`);
  }
  if (sqReport.graphBytesRead !== 0) {
    throw new Error("expected sq-scan to skip graph bytes");
  }
  const bufferIds = await index.searchIdsBuffer(new Float32Array([1, 0, 0]), { k: 2 });
  if (bufferIds.join(",") !== ids.join(",")) {
    throw new Error(`unexpected buffer hits: ${bufferIds.join(",")}`);
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
  const vectorIds = await index.searchIds([1, 0, 0], { k: 2 });
  if (vectorIds.join(",") !== ids.join(",")) {
    throw new Error(`unexpected id search hits: ${vectorIds.join(",")}`);
  }
  const vectors = await index.searchVectors([1, 0, 0], { k: 2 });
  const roundedVectors = vectors.map((vector) => vector.map((value) => Number(value.toFixed(6))));
  if (JSON.stringify(roundedVectors) !== JSON.stringify([[1, 0, 0], [0.9, 0.1, 0]])) {
    throw new Error(`unexpected vector search hits: ${JSON.stringify(vectors)}`);
  }
  const beta = await index.getVector("beta");
  const roundedBeta = beta?.map((value) => Number(value.toFixed(6)));
  if (JSON.stringify(roundedBeta) !== JSON.stringify([0.9, 0.1, 0])) {
    throw new Error(`unexpected beta vector: ${JSON.stringify(beta)}`);
  }
  if (report.bytesRead <= 0) {
    throw new Error("expected the example to read segment bytes");
  }
  const batchIds = await index.searchIdsBatch([[1, 0, 0], [0, 1, 0]], { k: 1 });
  const batchIdText = batchIds.map((ids) => ids.join(","));
  if (batchIdText.join("|") !== "alpha|gamma") {
    throw new Error(`unexpected batch hits: ${batchIdText.join("|")}`);
  }
  const bufferBatchIds = await index.searchIdsBatchBuffer(new Float32Array([1, 0, 0, 0, 1, 0]), {
    k: 1
  });
  const bufferBatchIdText = bufferBatchIds.map((ids) => ids.join(","));
  if (bufferBatchIdText.join("|") !== "alpha|gamma") {
    throw new Error(`unexpected buffer batch hits: ${bufferBatchIdText.join("|")}`);
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
  if (!(leafModeNames() as readonly string[]).includes(LeafModeName.Graph)) {
    throw new Error("expected graph in leaf mode catalog");
  }
  if (!(leafModeNames() as readonly string[]).includes(LeafModeName.SqScan)) {
    throw new Error("expected sq-scan in leaf mode catalog");
  }
  if (!(leafModeNames() as readonly string[]).includes(LeafModeName.PqScan)) {
    throw new Error("expected pq-scan in leaf mode catalog");
  }
  if (!(leafModeNames() as readonly string[]).includes(LeafModeName.VamanaPq)) {
    throw new Error("expected vamana-pq in leaf mode catalog");
  }
  if (!(leafModeNames() as readonly string[]).includes(LeafModeName.Hybrid)) {
    throw new Error("expected hybrid in leaf mode catalog");
  }
  const cosine = vectorDistance(VectorMetricName.Cosine, [1, 0], [1, 0]);
  const recall = recallAtK(["alpha", "beta"], ids, 2);
  const tieRecall = tieAwareRecallAtK(
    exactReport.hits.map((hit) => hit.distance),
    report.hits.map((hit) => hit.distance),
    2
  );
  if (cosine !== 0 || recall !== 1 || tieRecall !== 1) {
    throw new Error("metric helpers returned unexpected values");
  }

  console.log(
    `hits=${ids.join(",")} pqHits=${pqIds.join(",")} hybridHits=${hybridIds.join(",")} bytesRead=${report.bytesRead} recallAt2=${recall} tieRecallAt2=${tieRecall} objectCacheHits=${report.objectCacheHits} objectCacheMisses=${report.objectCacheMisses} recordsScored=${report.recordsScored} residentBytesEstimate=${report.residentBytesEstimate} segmentBytes=${stats.segmentBytes}`
  );
}

await main();

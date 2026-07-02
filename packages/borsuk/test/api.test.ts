import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  BorsukError,
  create,
  Index,
  open,
  recallAtK,
  stringDistance,
  vectorDistance
} from "../src/index.js";

test("vectorDistance exposes dense metric catalog", () => {
  assert.equal(
    Math.abs(vectorDistance("minkowski:3", [0, 0], [1, 2]) - Math.cbrt(9)) < 1e-6,
    true
  );
  assert.equal(vectorDistance("cosine", [1, 0], [1, 0]), 0);
  assert.equal(
    Math.abs(vectorDistance("rogers-tanimoto", [1, 0, 1, 0], [1, 1, 0, 0]) - 2 / 3) < 1e-6,
    true
  );
  assert.equal(
    Math.abs(vectorDistance("sokal-sneath", [1, 0, 1, 0], [1, 1, 0, 0]) - 0.8) < 1e-6,
    true
  );
  assert.throws(() => vectorDistance("euclidean", [1], [1, 2]), /dimension mismatch/);
});

test("stringDistance exposes edit and similarity metrics", () => {
  assert.equal(stringDistance("damerau-levenshtein", "abcd", "acbd"), 1);
  assert.equal(stringDistance("hamming", "rust", "dust"), 1);

  const jaroWinkler = stringDistance("jaro-winkler", "segment", "segments");
  assert.equal(jaroWinkler > 0, true);
  assert.equal(jaroWinkler < 0.2, true);
  assert.throws(() => stringDistance("not-a-string-metric", "a", "b"), /unknown string metric/);
});

test("recallAtK measures top-k overlap", () => {
  assert.equal(
    Math.abs(
      recallAtK(
        ["doc-a", "doc-b", "doc-c", "doc-d"],
        ["doc-c", "doc-x", "doc-a", "doc-a"],
        3
      ) - 2 / 3
    ) < 1e-6,
    true
  );
  assert.equal(
    Math.abs(recallAtK(["doc-a", "doc-b", "doc-c"], ["doc-c", "doc-b"], 10) - 2 / 3) < 1e-6,
    true
  );
  assert.throws(() => recallAtK(["doc-a"], ["doc-a"], 0), /k must be greater than zero/);
});

test("create/add/search round trip", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: `file://${dir}`,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add(["a", "b"], [[0, 0], [1, 0]]);
  const hits = await index.search([0.2, 0], { k: 2 });

  assert.deepEqual(hits.map((hit) => hit.id), ["a", "b"]);
});

test("searchBatch preserves query order", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: `file://${dir}`,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add(
    ["left", "middle", "right"],
    [[0, 0], [5, 0], [10, 0]]
  );
  const results = await index.searchBatch([[0.1, 0], [9.9, 0]], { k: 1 });

  assert.deepEqual(results.map((hits) => hits.map((hit) => hit.id)), [["left"], ["right"]]);
});

test("searchBatchWithReport preserves query order and counters", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: `file://${dir}`,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add(
    ["left", "middle", "right"],
    [[0, 0], [5, 0], [10, 0]]
  );
  const reports = await index.searchBatchWithReport([[0.1, 0], [9.9, 0]], { k: 1 });

  assert.deepEqual(reports.map((report) => report.hits[0]?.id), ["left", "right"]);
  assert.deepEqual(reports.map((report) => report.segmentsTotal), [3, 3]);
  assert.ok(reports[0]?.bytesRead > 0);
  assert.ok(reports[1]?.bytesRead > 0);
  assert.ok(reports[0]?.residentBytesEstimate > 0);
  assert.ok(reports[1]?.residentBytesEstimate > 0);
});

test("stats expose manifest and resident budget metadata", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: `file://${dir}`,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 2,
    ramBudget: "1MB"
  });

  await index.add(
    ["a", "b", "c"],
    [[0, 0], [1, 0], [10, 0]]
  );
  const stats = await index.stats();

  assert.equal(stats.metric, "euclidean");
  assert.equal(stats.dimensions, 2);
  assert.equal(stats.segmentMaxVectors, 2);
  assert.equal(stats.ramBudgetBytes, 1_000_000);
  assert.equal(stats.manifestVersion, 2);
  assert.equal(stats.segments, 2);
  assert.equal(stats.records, 3);
  assert.ok(stats.segmentBytes > 0);
  assert.ok(stats.graphBytes > 0);
  assert.ok(stats.residentBytesEstimate > 0);

  const reopened = open(`file://${dir}`, { ramBudget: "500KB" });
  assert.equal((await reopened.stats()).ramBudgetBytes, 500_000);
});

test("create enforces ramBudget", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  await assert.rejects(
    () =>
      create({
        uri: `file://${dir}`,
        metric: "euclidean",
        dimensions: 2,
        segmentMaxVectors: 1,
        ramBudget: "1B"
      }),
    /RAM budget exceeded/
  );
});

test("runtime errors use BorsukError", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  await assert.rejects(
    () =>
      create({
        uri: `file://${dir}`,
        metric: "euclidean",
        dimensions: 2,
        segmentMaxVectors: 1,
        ramBudget: "1B"
      }),
    BorsukError
  );
});

test("open enforces runtime ramBudget", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  await create({
    uri: `file://${dir}`,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  assert.throws(
    () =>
      open(`file://${dir}`, {
        ramBudget: "1B"
      }),
    /RAM budget exceeded/
  );
});

test("add rejects mismatched ids and vectors", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: `file://${dir}`,
    metric: "euclidean",
    dimensions: 1
  });
  await assert.rejects(() => index.add(["a"], [[0], [1]]), /same length/);
});

test("searchWithReport exposes query counters", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: `file://${dir}`,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add(["near", "mid", "far"], [[0, 0], [10, 0], [20, 0]]);
  const report = await index.searchWithReport([0, 0], { k: 1 });

  assert.equal(report.hits[0]?.id, "near");
  assert.equal(report.segmentsTotal, 3);
  assert.equal(report.segmentsSearched, 1);
  assert.equal(report.segmentsSkipped, 2);
  assert.ok(report.bytesRead > 0);
  assert.equal(report.objectCacheHits, 0);
  assert.ok(report.objectCacheMisses > 0);
  assert.ok(report.residentBytesEstimate > 0);
  assert.ok(report.elapsedMs >= 0);
});

test("approx search limits exact scoring inside each segment", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: `file://${dir}`,
    metric: "euclidean",
    dimensions: 1,
    segmentMaxVectors: 4
  });

  await index.add(["near", "next", "far-a", "far-b"], [[0], [0.2], [10], [20]]);
  const report = await index.searchWithReport([0.05], {
    k: 1,
    mode: "approx",
    maxCandidatesPerSegment: 2
  });

  assert.equal(report.hits[0]?.id, "near");
  assert.equal(report.recordsConsidered, 4);
  assert.equal(report.recordsScored, 2);
});

test("approx search obeys byte budget", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: `file://${dir}`,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add(["near", "mid", "far"], [[0, 0], [10, 0], [20, 0]]);
  const report = await index.searchWithReport([0, 0], {
    k: 3,
    mode: "approx",
    maxBytes: 1
  });

  assert.deepEqual(report.hits.map((hit) => hit.id), ["near"]);
  assert.equal(report.segmentsSearched, 1);
  assert.equal(report.segmentsSkipped, 2);
  assert.ok(report.bytesRead > 1);
});

test("approx search accepts byte budget string", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: `file://${dir}`,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add(["near", "mid", "far"], [[0, 0], [10, 0], [20, 0]]);
  const report = await index.searchWithReport([0, 0], {
    k: 3,
    mode: "approx",
    maxBytes: "1B"
  });

  assert.deepEqual(report.hits.map((hit) => hit.id), ["near"]);
  assert.equal(report.segmentsSearched, 1);
  assert.equal(report.segmentsSkipped, 2);
});

test("approx search expands segment graph candidates", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: `file://${dir}`,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 4
  });

  await index.add(
    ["entry", "true-neighbor", "routing-decoy", "far"],
    [[0, 0], [0, 0.1], [0.1, -0.1], [100, 100]]
  );
  const report = await index.searchWithReport([0.04, 0.07], {
    k: 1,
    mode: "approx",
    maxCandidatesPerSegment: 2
  });

  assert.equal(report.hits[0]?.id, "true-neighbor");
  assert.equal(report.recordsConsidered, 4);
  assert.equal(report.recordsScored, 2);
  assert.ok(report.graphBytesRead > 0);
  assert.equal(report.graphCandidatesAdded, 1);
});

test("approx search walks segment graph beyond first hop", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: `file://${dir}`,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 10
  });

  await index.add(
    [
      "aa-entry",
      "bb-hop",
      "cc-decoy-0",
      "cc-decoy-1",
      "cc-decoy-2",
      "cc-decoy-3",
      "cc-decoy-4",
      "cc-decoy-5",
      "cc-decoy-6",
      "zz-target"
    ],
    [
      [0, 0],
      [1, 1],
      [-1, -1],
      [-1.1, -1.1],
      [-1.2, -1.2],
      [-1.3, -1.3],
      [-1.4, -1.4],
      [-1.5, -1.5],
      [-1.6, -1.6],
      [2, 2]
    ]
  );
  const report = await index.searchWithReport([2, 2], {
    k: 1,
    mode: "approx",
    maxCandidatesPerSegment: 3
  });

  assert.equal(report.hits[0]?.id, "zz-target");
  assert.equal(report.recordsConsidered, 10);
  assert.equal(report.recordsScored, 3);
  assert.equal(report.graphCandidatesAdded, 2);
});

test("cacheDir populates segment and graph cache", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const cache = mkdtempSync(join(tmpdir(), "borsuk-ts-cache-"));
  const writer = await create({
    uri: `file://${dir}`,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 4
  });

  await writer.add(
    ["entry", "true-neighbor", "routing-decoy", "far"],
    [[0, 0], [0, 0.1], [0.1, -0.1], [100, 100]]
  );
  const index = open(`file://${dir}`, { cacheDir: cache });
  const report = await index.searchWithReport([0.04, 0.07], {
    k: 1,
    mode: "approx",
    maxCandidatesPerSegment: 2
  });

  assert.equal(report.hits[0]?.id, "true-neighbor");
  assert.ok(report.graphBytesRead > 0);
  assert.equal(report.objectCacheHits, 0);
  assert.equal(report.objectCacheMisses, 2);
  assert.equal(hasParquetFiles(join(cache, "segments")), true);
  assert.equal(hasParquetFiles(join(cache, "graphs")), true);
});

test("compact rewrites segments and reports counters", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: `file://${dir}`,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add(["a", "b", "c", "d"], [[0, 0], [1, 0], [8, 0], [9, 0]]);
  const before = await index.searchWithReport([8.5, 0], { k: 2 });
  assert.equal(before.segmentsTotal, 4);

  const report = await index.compact({
    sourceLevel: 0,
    targetLevel: 1,
    maxSegments: 4,
    minSegments: 2,
    targetSegmentMaxVectors: 2
  });

  assert.equal(report.compacted, true);
  assert.equal(report.segmentsRead, 4);
  assert.equal(report.segmentsWritten, 2);
  assert.equal(report.recordsRewritten, 4);
  assert.ok(report.bytesRead > 0);
  assert.ok(report.bytesWritten > 0);
  assert.equal(report.objectCacheHits, 0);
  assert.equal(report.objectCacheMisses, 4);

  const after = await index.searchWithReport([8.5, 0], { k: 2 });
  assert.equal(after.segmentsTotal, 2);
  assert.deepEqual(after.hits.map((hit) => hit.id), ["c", "d"]);
});

test("gcObsoleteSegments dry-runs and deletes inactive segments", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: `file://${dir}`,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add(["a", "b", "c", "d"], [[0, 0], [1, 0], [8, 0], [9, 0]]);
  await index.compact({ targetSegmentMaxVectors: 2 });

  const dryRun = await index.gcObsoleteSegments();
  assert.equal(dryRun.dryRun, true);
  assert.equal(dryRun.objectsScanned, 12);
  assert.equal(dryRun.objectsDeleted, 0);
  assert.equal(dryRun.candidates.length, 8);
  assert.ok(dryRun.bytesReclaimable > 0);

  const deleted = await index.gcObsoleteSegments({ dryRun: false });
  assert.equal(deleted.dryRun, false);
  assert.equal(deleted.objectsDeleted, 8);
  assert.deepEqual(deleted.candidates, dryRun.candidates);
  assert.equal(deleted.bytesReclaimed, dryRun.bytesReclaimable);

  const hits = await index.search([8.5, 0], { k: 2 });
  assert.deepEqual(hits.map((hit) => hit.id), ["c", "d"]);
});

function hasParquetFiles(root: string): boolean {
  if (!existsSync(root)) {
    return false;
  }
  return readdirSync(root, { withFileTypes: true }).some((entry) => {
    const path = join(root, entry.name);
    return entry.isDirectory() ? hasParquetFiles(path) : entry.name.endsWith(".parquet");
  });
}

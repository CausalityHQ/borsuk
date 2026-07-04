import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, readdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";

import {
  BorsukError,
  create,
  Index,
  leafModeNames,
  LeafModeName,
  minkowskiMetric,
  open,
  recallAtK,
  SearchMode,
  tieAwareRecallAtK,
  vectorDistance,
  VectorMetricName,
  vectorMetricNames
} from "../src/index.js";
import type {
  CanonicalVectorMetricName,
  LeafMode,
  MinkowskiMetricName,
  OpenOptions,
  SearchTerminationReason,
  VectorMetric
} from "../src/index.js";

function localUri(path: string): string {
  return pathToFileURL(path).href;
}

function deterministicVector(seed: number, dimensions: number): number[] {
  return Array.from({ length: dimensions }, (_, dimension) =>
    dimension === 0 ? seed : dimension / dimensions
  );
}

test("metric name catalogs expose canonical names", () => {
  const typedVectorMetric: VectorMetric = VectorMetricName.Cosine;
  const typedMinkowskiMetric: VectorMetric = minkowskiMetric(3);
  const typedLeafMode: LeafMode = LeafModeName.FlatScan;
  const typedSqLeafMode: LeafMode = LeafModeName.SqScan;
  const typedPqLeafMode: LeafMode = LeafModeName.PqScan;
  const typedVamanaLeafMode: LeafMode = LeafModeName.VamanaPq;
  const typedHybridLeafMode: LeafMode = LeafModeName.Hybrid;
  const typedOpenOptions: OpenOptions = {
    cacheDir: "/tmp/borsuk-cache",
    ramBudget: "1GB",
    residentRouting: false
  };
  const readonlyVector = [1, 0] as const;
  const readonlyIds = ["doc-a", "doc-b"] as const;
  const readonlyDistances = [0, 0] as const;
  assert.equal(vectorDistance(typedVectorMetric, [1, 0], [1, 0]), 0);
  assert.equal(vectorDistance(typedVectorMetric, readonlyVector, readonlyVector), 0);
  assert.equal(recallAtK(readonlyIds, readonlyIds, 2), 1);
  assert.equal(tieAwareRecallAtK(readonlyDistances, readonlyDistances, 2), 1);
  assert.equal(typedMinkowskiMetric, "minkowski:3");
  assert.equal(typedOpenOptions.ramBudget, "1GB");
  assert.equal(typedOpenOptions.residentRouting, false);
  assert.equal(Math.abs(vectorDistance(typedMinkowskiMetric, [0, 0], [1, 2]) - Math.cbrt(9)) < 1e-6, true);
  assert.throws(() => minkowskiMetric(0.5), /Minkowski power must be greater than or equal to 1/);
  assert.equal(typedLeafMode, "flat-scan");
  assert.equal(typedSqLeafMode, "sq-scan");
  assert.equal(typedPqLeafMode, "pq-scan");
  assert.equal(typedVamanaLeafMode, "vamana-pq");
  assert.equal(typedHybridLeafMode, "hybrid");
  assert.equal(SearchMode.Approx, "approx");

  const vectorNames = vectorMetricNames();
  assert.equal(vectorNames.includes("euclidean"), true);
  assert.equal(vectorNames.includes("cosine"), true);
  assert.equal(vectorNames.includes("gower"), true);
  assert.equal(vectorNames.includes("jensen-shannon"), true);
  assert.equal(vectorNames.includes("dynamic-time-warping"), true);
  assert.equal(vectorNames.includes("clark"), true);
  assert.equal((vectorNames as readonly string[]).includes("l2"), false);
  for (const name of vectorNames) {
    vectorDistance(name, [1, 2, 3], [2, 3, 4]);
  }

  assert.deepEqual(leafModeNames(), [
    "flat-scan",
    "sq-scan",
    "pq-scan",
    "graph",
    "vamana-pq",
    "hybrid"
  ]);
});

test("vectorDistance exposes dense metric catalog", () => {
  assert.equal(
    Math.abs(vectorDistance("minkowski:3", [0, 0], [1, 2]) - Math.cbrt(9)) < 1e-6,
    true
  );
  assert.equal(vectorDistance("cosine", [1, 0], [1, 0]), 0);
  assert.equal(
    Math.abs(vectorDistance("gower", [1, 2, 0, 4], [1, 4, 3, 0]) - 2.25) < 1e-6,
    true
  );
  assert.equal(
    Math.abs(vectorDistance("rogers-tanimoto", [1, 0, 1, 0], [1, 1, 0, 0]) - 2 / 3) < 1e-6,
    true
  );
  assert.equal(
    Math.abs(vectorDistance("sokal-sneath", [1, 0, 1, 0], [1, 1, 0, 0]) - 0.8) < 1e-6,
    true
  );
  assert.equal(
    Math.abs(vectorDistance("jensen-shannon", [0.5, 0.5], [0.25, 0.75]) - 0.18390779) < 1e-6,
    true
  );
  assert.equal(
    Math.abs(vectorDistance("bhattacharyya", [0.5, 0.5], [0.25, 0.75]) - 0.03466823) < 1e-6,
    true
  );
  assert.equal(Math.abs(vectorDistance("earth-mover", [1, 0, 0], [0, 0, 1]) - 2) < 1e-6, true);
  assert.equal(Math.abs(vectorDistance("dtw", [0, 0, 1, 1], [0, 1, 1, 1])) < 1e-6, true);
  assert.equal(Math.abs(vectorDistance("ruzicka", [1, 2, 0], [2, 1, 3]) - 5 / 7) < 1e-6, true);
  assert.equal(
    Math.abs(vectorDistance("squared-chord", [1, 4], [4, 1]) - 2) < 1e-6,
    true
  );
  assert.equal(Math.abs(vectorDistance("wave-hedges", [1, 2, 0], [2, 1, 3]) - 2) < 1e-6, true);
  assert.throws(() => vectorDistance("euclidean", [1], [1, 2]), /dimension mismatch/);
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
  assert.equal(
    Math.abs(
      recallAtK(
        [new Uint8Array([0, 159, 255, 7]), 300, "doc-c"],
        [300n, new Uint8Array([0, 159, 255, 7])],
        3
      ) - 2 / 3
    ) < 1e-6,
    true
  );
  assert.throws(() => recallAtK(["doc-a"], ["doc-a"], 0), /k must be greater than zero/);
  assert.throws(() => recallAtK(["doc-a"], ["doc-a"], 1.5), /k must be an integer/);
  assert.throws(() => recallAtK(["doc-a"], ["doc-a"], Number.NaN), /k must be an integer/);
  assert.throws(
    () => recallAtK(["doc-a"], ["doc-a"], true as unknown as number),
    /k must be an integer/
  );
});

test("tieAwareRecallAtK counts equal-distance hits without ids", () => {
  assert.equal(tieAwareRecallAtK([0, 0], [0, 0], 2), 1);
  assert.equal(
    Math.abs(tieAwareRecallAtK([0, 0, 0.2], [0, 0.2, 0.3], 3) - 2 / 3) < 1e-6,
    true
  );
  assert.throws(() => tieAwareRecallAtK([0], [0], 0), /k must be greater than zero/);
  assert.throws(() => tieAwareRecallAtK([0], [0], 1.5), /k must be an integer/);
  assert.throws(() => tieAwareRecallAtK([0], [0], Number.NaN), /k must be an integer/);
  assert.throws(
    () => tieAwareRecallAtK([0], [0], true as unknown as number),
    /k must be an integer/
  );
});

test("create/add/search round trip", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add([[0, 0], [1, 0]], { ids: ["a", "b"] });
  const ids = await index.searchIds([0.2, 0], { k: 2 });
  const stats = await index.stats();
  const statsMetric: CanonicalVectorMetricName | MinkowskiMetricName = stats.metric;

  assert.deepEqual(ids, ["a", "b"]);
  assert.equal(statsMetric, "euclidean");
});

test("index methods accept readonly vector and id inputs", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-readonly-index-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 4
  });
  const vectors = [[0, 0], [1, 0], [0, 1]] as const;
  const ids = ["origin", "x", "y"] as const;
  const query = [0.9, 0] as const;
  const batch = [[0.9, 0], [0, 0.9]] as const;

  assert.deepEqual(await index.add(vectors, ids), ["origin", "x", "y"]);
  assert.deepEqual(await index.searchIds(query, { k: 2 }), ["x", "origin"]);
  assert.deepEqual(await index.searchVectors(query, { k: 1 }), [[1, 0]]);
  assert.deepEqual(await index.searchIdsBatch(batch, { k: 1 }), [["x"], ["y"]]);
  assert.deepEqual(await index.searchVectorsBatch(batch, { k: 1 }), [[[1, 0]], [[0, 1]]]);
});

test("create rejects conflicting segment size aliases", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-segment-aliases-"));

  await assert.rejects(
    () =>
      create({
        uri: localUri(dir),
        metric: "euclidean",
        dimensions: 2,
        segmentSize: 1,
        segmentMaxVectors: 2
      }),
    /segment_size and segment_max_vectors disagree/
  );
});

test("create rejects non-integer layout options", async () => {
  for (const [options, expected] of [
    [{ dim: 2.5 }, /dim must be an integer when set/],
    [{ dimensions: 2.5 }, /dimensions must be an integer when set/],
    [{ segmentSize: 1.5 }, /segment_size must be an integer when set/],
    [{ segmentMaxVectors: Number.NaN }, /segment_max_vectors must be an integer when set/],
    [
      { routingPageFanout: true as unknown as number },
      /routing_page_fanout must be an integer when set/
    ]
  ] as const) {
    const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-create-options-"));
    await assert.rejects(
      () =>
        create({
          uri: localUri(dir),
          metric: "euclidean",
          dimensions: 2,
          segmentMaxVectors: 2,
          ...options
        }),
      expected
    );
  }
});

test("add accepts vectors with optional ids", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-simple-add-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 2
  });

  const generatedIds = await index.add([[0, 0], [1, 0]]);
  const explicitIds = await index.add([[9, 0]], { ids: ["far"] });
  const directIds = await index.add([[8, 0]], ["direct"]);
  const directBufferIds = await index.addBuffer(new Float32Array([7, 0]), ["buffer-direct"]);

  assert.deepEqual(generatedIds, ["0", "1"]);
  assert.deepEqual(explicitIds, ["far"]);
  assert.deepEqual(directIds, ["direct"]);
  assert.deepEqual(directBufferIds, ["buffer-direct"]);
  assert.deepEqual(await index.searchIds([0.1, 0], { k: 2 }), ["0", "1"]);
});

test("public API has id and vector searches only", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-search-api-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 2
  });

  assert.equal("search" in index, false);
  assert.equal("searchBuffer" in index, false);
  assert.equal("searchBatch" in index, false);
  assert.equal("searchBatchBuffer" in index, false);
  assert.equal(typeof index.searchIds, "function");
  assert.equal(typeof index.searchVectors, "function");
  assert.equal(typeof index.searchIdsBuffer, "function");
  assert.equal(typeof index.searchVectorsBuffer, "function");
  assert.equal(typeof index.searchIdsBatch, "function");
  assert.equal(typeof index.searchVectorsBatch, "function");
  assert.equal(typeof index.searchIdsBatchBuffer, "function");
  assert.equal(typeof index.searchVectorsBatchBuffer, "function");
  assert.equal(typeof index.getVector, "function");
});

test("add rejects duplicate ids and generated ids skip collisions", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-duplicate-ids-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 2
  });

  await assert.rejects(
    () => index.add([[0, 0], [1, 0]], { ids: ["dup", "dup"] }),
    /duplicate record id/
  );

  await index.add([[0, 0]], { ids: ["1"] });
  assert.deepEqual(await index.add([[2, 0], [3, 0]]), ["2", "3"]);

  await assert.rejects(() => index.add([[4, 0]], { ids: ["2"] }), /duplicate record id/);
});

test("searchVectors and getVector return stored vectors", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-vector-lookup-"));
  const uri = localUri(dir);
  const index = await create({
    uri,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 2
  });

  await index.add([[0, 0], [1, 0], [9, 0]], { ids: ["a", "b", "far"] });

  assert.deepEqual(await index.searchIds([0.8, 0], { k: 2 }), ["b", "a"]);
  assert.deepEqual(await index.searchVectors([0.8, 0], { k: 2 }), [[1, 0], [0, 0]]);
  assert.deepEqual(await index.getVector("b"), [1, 0]);
  assert.equal(await index.getVector("missing"), null);
  assert.deepEqual(await (await open(uri)).getVector("far"), [9, 0]);

  await assert.rejects(() => index.getVector(""), /record ids must not be empty/);
  await assert.rejects(() => index.getVector(" \t "), /record ids must not be empty/);
});

test("search buffer variants accept contiguous Float32Array query", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-search-buffer-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add([[0, 0], [1, 0], [9, 0]], { ids: ["a", "b", "c"] });

  assert.deepEqual(await index.searchIdsBuffer(new Float32Array([0.8, 0]), { k: 2 }), ["b", "a"]);
  assert.deepEqual(await index.searchVectorsBuffer(new Float32Array([0.8, 0]), { k: 2 }), [[1, 0], [0, 0]]);
});

test("addBuffer accepts contiguous Float32Array rows", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-buffer-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 2
  });

  await index.addBuffer(
    new Float32Array([0, 0, 1, 0, 9, 0]),
    { ids: ["a", "b", "c"] }
  );
  const ids = await index.searchIds([0.8, 0], { k: 2 });

  assert.deepEqual(ids, ["b", "a"]);
});

test("exact search does not prune equal-distance ties", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-tie-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add([[1, 0], [-1, 0]], { ids: ["z-tie", "a-tie"] });
  const report = await index.searchWithReport([0, 0], { k: 1 });

  assert.deepEqual(report.hits.map((hit) => hit.id), ["a-tie"]);
  assert.equal(report.segmentsSearched, 2);
  assert.equal(report.segmentsSkipped, 0);
});

test("open with cache reads fresh CURRENT after external publish", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-current-"));
  const cache = mkdtempSync(join(tmpdir(), "borsuk-ts-current-cache-"));
  const uri = localUri(dir);
  const cached = await create({
    uri,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 2,
    cacheDir: cache
  });
  assert.equal((await cached.stats()).manifestVersion, 1);

  const writer = open(uri);
  await writer.add([[0, 0]], { ids: ["fresh"] });
  assert.equal((await writer.stats()).manifestVersion, 2);

  const reopened = open(uri, { cacheDir: cache });

  assert.equal((await reopened.stats()).manifestVersion, 2);
  assert.equal((await reopened.stats()).records, 1);
  assert.equal((await reopened.searchIds([0, 0], { k: 1 }))[0], "fresh");
});

test("search batch variants preserve query order", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add(
    [[0, 0], [5, 0], [10, 0]],
    { ids: ["left", "middle", "right"] }
  );

  assert.deepEqual(await index.searchIdsBatch([[0.1, 0], [9.9, 0]], { k: 1 }), [["left"], ["right"]]);
  assert.deepEqual(await index.searchVectorsBatch([[0.1, 0], [9.9, 0]], { k: 1 }), [[[0, 0]], [[10, 0]]]);
});

test("binary ids can be added, searched, and loaded without UTF-8 decoding", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-binary-id-"));
  const uri = localUri(dir);
  const index = await create({
    uri,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 2
  });
  const id = new Uint8Array([0, 159, 255, 7]);

  const added = await index.add([[0, 0]], { ids: [id] });
  assert.deepEqual(added.map((value) => [...value]), [[0, 159, 255, 7]]);
  assert.deepEqual(
    (await index.searchIdBytes([0, 0], { k: 1 })).map((value) => [...value]),
    [[0, 159, 255, 7]]
  );
  assert.deepEqual(await index.getVector(id), [0, 0]);
  assert.deepEqual(await index.searchVectors([0, 0], { k: 1 }), [[0, 0]]);
  const report = await index.searchWithReport([0, 0], { k: 1 });
  assert.equal(report.hits[0].id, "0x009fff07");
  assert.deepEqual([...report.hits[0].idBytes], [0, 159, 255, 7]);
  assert.deepEqual(
    (await open(uri).searchIdBytes([0, 0], { k: 1 })).map((value) => [...value]),
    [[0, 159, 255, 7]]
  );
  await assert.rejects(() => index.searchIds([0, 0], { k: 1 }), /valid UTF-8/);
});

test("integer ids use compact binary encoding", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-integer-id-"));
  const uri = localUri(dir);
  const index = await create({
    uri,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 2
  });

  assert.deepEqual(await index.add([[0, 0]], { ids: [300] }), [300]);
  assert.deepEqual(
    (await index.searchIdBytes([0, 0], { k: 1 })).map((value) => [...value]),
    [[0xac, 0x02]]
  );
  assert.deepEqual(await index.getVector(300), [0, 0]);
  assert.deepEqual(await open(uri).getVector(300), [0, 0]);

  await assert.rejects(
    () => index.add([[1, 0]], { ids: [-1] }),
    /integer record ids must be non-negative/
  );
});

test("search batch buffer variants accept contiguous Float32Array rows", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-buffer-query-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add(
    [[0, 0], [5, 0], [10, 0]],
    { ids: ["left", "middle", "right"] }
  );

  assert.deepEqual(await index.searchIdsBatchBuffer(new Float32Array([0.1, 0, 9.9, 0]), { k: 1 }), [["left"], ["right"]]);
  assert.deepEqual(await index.searchVectorsBatchBuffer(new Float32Array([0.1, 0, 9.9, 0]), { k: 1 }), [[[0, 0]], [[10, 0]]]);
});

test("searchBatchWithReport preserves query order and counters", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add(
    [[0, 0], [5, 0], [10, 0]],
    { ids: ["left", "middle", "right"] }
  );
  const reports = await index.searchBatchWithReport([[0.1, 0], [9.9, 0]], { k: 1 });

  assert.deepEqual(reports.map((report) => report.hits[0]?.id), ["left", "right"]);
  assert.deepEqual(reports.map((report) => report.segmentsTotal), [3, 3]);
  assert.ok(reports[0]?.bytesRead > 0);
  assert.ok(reports[1]?.bytesRead > 0);
  assert.ok(reports[0]?.residentBytesEstimate > 0);
  assert.ok(reports[1]?.residentBytesEstimate > 0);
});

test("searchBatchWithReportBuffer accepts contiguous Float32Array rows", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-buffer-query-report-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add(
    [[0, 0], [5, 0], [10, 0]],
    { ids: ["left", "middle", "right"] }
  );
  const reports = await index.searchBatchWithReportBuffer(new Float32Array([0.1, 0, 9.9, 0]), {
    k: 1
  });

  assert.deepEqual(reports.map((report) => report.hits[0]?.id), ["left", "right"]);
  assert.deepEqual(reports.map((report) => report.segmentsTotal), [3, 3]);
  assert.ok(reports[0]?.bytesRead > 0);
  assert.ok(reports[1]?.bytesRead > 0);
});

test("stats expose manifest and resident budget metadata", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 2,
    ramBudget: "1MB"
  });

  await index.add(
    [[0, 0], [1, 0], [10, 0]],
    { ids: ["a", "b", "c"] }
  );
  const stats = await index.stats();

  assert.equal(stats.metric, "euclidean");
  assert.equal(stats.dimensions, 2);
  assert.equal(stats.segmentMaxVectors, 2);
  assert.equal(stats.ramBudgetBytes, 1_000_000);
  assert.equal(stats.manifestVersion, 2);
  assert.equal(stats.routingMaxLevel, 0);
  assert.equal(stats.routingPageFanout, 128);
  assert.equal(stats.routingLeafPages, 1);
  assert.equal(stats.routingPages, 1);
  assert.equal(stats.segments, 2);
  assert.equal(stats.records, 3);
  assert.ok(stats.segmentBytes > 0);
  assert.ok(stats.graphBytes > 0);
  assert.ok(stats.residentBytesEstimate > 0);

  const reopened = open(localUri(dir), { ramBudget: "500KB" });
  assert.equal((await reopened.stats()).ramBudgetBytes, 500_000);
});

test("stats expose computed routing max level", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add(
    Array.from({ length: 130 }, (_, value) => [value, 0]),
    { ids: Array.from({ length: 130 }, (_, value) => `v${value}`) }
  );

  const stats = await index.stats();
  assert.equal(stats.routingPageFanout, 128);
  assert.equal(stats.routingMaxLevel, 1);
  assert.equal(stats.routingLeafPages, 2);
  assert.equal(stats.routingPages, 3);
});

test("create supports routing page fanout", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1,
    routingPageFanout: 4
  });

  await index.add(
    Array.from({ length: 17 }, (_, value) => [value, 0]),
    { ids: Array.from({ length: 17 }, (_, value) => `v${value}`) }
  );

  const stats = await index.stats();
  assert.equal(stats.routingPageFanout, 4);
  assert.equal(stats.routingMaxLevel, 2);
  assert.equal(stats.routingLeafPages, 5);
  assert.equal(stats.routingPages, 8);

  const reopened = open(localUri(dir), { residentRouting: false });
  const reopenedStats = await reopened.stats();
  assert.equal(reopenedStats.routingPageFanout, 4);
  assert.equal(reopenedStats.routingMaxLevel, 2);
  assert.equal(reopenedStats.routingLeafPages, 5);
  assert.equal(reopenedStats.routingPages, 8);
});

test("create enforces ramBudget", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  await assert.rejects(
    () =>
      create({
        uri: localUri(dir),
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
        uri: localUri(dir),
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
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  assert.throws(
    () =>
      open(localUri(dir), {
        ramBudget: "1B"
      }),
    /RAM budget exceeded/
  );
});

test("open rejects non-boolean residentRouting option", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-open-options-"));
  await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  assert.throws(
    () => open(localUri(dir), { residentRouting: 1 as unknown as boolean }),
    /resident_routing must be a boolean when set/
  );
});

test("open can use paged routing without resident segment summaries", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const uri = localUri(dir);
  const index = await create({
    uri,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add(
    Array.from({ length: 130 }, (_, value) => [value, 0]),
    { ids: Array.from({ length: 130 }, (_, value) => `v${value}`) }
  );
  const fullResidentBytes = (await index.stats()).residentBytesEstimate;

  const reopened = open(uri, {
    ramBudget: `${fullResidentBytes - 1}B`,
    residentRouting: false
  });

  const stats = await reopened.stats();
  assert.equal(stats.segments, 130);
  assert.equal(stats.records, 130);
  assert.equal(stats.residentBytesEstimate < fullResidentBytes, true);
  const report = await reopened.searchWithReport([129, 0], {
    k: 1,
    mode: SearchMode.Approx,
    leafMode: LeafModeName.PqScan,
    maxSegments: 1
  });
  assert.equal(report.hits[0].id, "v129");
  assert.equal(report.segmentsTotal, 130);
  assert.equal(report.segmentsSearched, 1);
  assert.equal(report.residentBytesEstimate < fullResidentBytes, true);
});

test("approx search drills through deep paged routing tree", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-deep-routing-"));
  const uri = localUri(dir);
  const index = await create({
    uri,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1,
    routingPageFanout: 4
  });

  const vectors = Array.from({ length: 64 }, (_, value) => [1000 + value, 0]);
  vectors.push([0, 0]);
  const ids = Array.from({ length: 64 }, (_, value) => `far-${value}`);
  ids.push("near");
  await index.add(vectors, { ids });
  const stats = await index.stats();
  assert.equal(stats.routingPageFanout, 4);
  assert.equal(stats.routingMaxLevel, 3);

  const reopened = open(uri, { residentRouting: false });
  writeFileSync(
    join(
      dir,
      "routing",
      "layers",
      stats.manifestVersion.toString().padStart(20, "0"),
      "L0",
      "pages.parquet"
    ),
    "corrupt global L0 routing page index that deep search must not read"
  );

  const report = await reopened.searchWithReport([0, 0], {
    k: 1,
    mode: SearchMode.Approx,
    leafMode: LeafModeName.PqScan,
    maxSegments: 1,
    routingPageOverfetch: 1
  });

  assert.equal(report.hits[0].id, "near");
  assert.equal(report.segmentsTotal, 65);
  assert.equal(report.segmentsSearched, 1);
  assert.equal(report.routingPageIndexesRead, 1);
  assert.equal(report.routingPagesRead, 4);
});

test("stats propagates corrupt paged routing metadata", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const uri = localUri(dir);
  const index = await create({
    uri,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add([[0, 0]], { ids: ["v0"] });
  const version = (await index.stats()).manifestVersion;
  const reopened = open(uri, { residentRouting: false });

  writeFileSync(
    join(dir, "routing", "layers", version.toString().padStart(20, "0"), "L0", "pages.parquet"),
    "corrupt paged stats routing metadata"
  );

  await assert.rejects(
    () => reopened.stats(),
    /parquet|routing layer page index/i
  );
});

test("add rejects mismatched ids and vectors", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 1
  });
  await assert.rejects(() => index.add([[0], [1]], { ids: ["a"] }), /same length/);
});

test("searchWithReport exposes query counters", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add([[0, 0], [10, 0], [20, 0]], { ids: ["near", "mid", "far"] });
  const report = await index.searchWithReport([0, 0], { k: 1 });

  assert.equal(report.hits[0]?.id, "near");
  assert.equal(report.leafMode, "flat-scan");
  assert.equal(report.segmentsTotal, 3);
  assert.equal(report.segmentsSearched, 1);
  assert.equal(report.segmentsSkipped, 2);
  assert.equal(report.routingPageIndexesRead, 1);
  assert.equal(report.routingPagesRead, 1);
  assert.ok(report.bytesRead > 0);
  assert.equal(report.objectCacheHits, 0);
  assert.ok(report.objectCacheMisses > 0);
  assert.ok(report.residentBytesEstimate > 0);
  assert.ok(report.elapsedMs >= 0);
});

test("searchWithReportBuffer accepts contiguous Float32Array query", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-report-buffer-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add([[0, 0], [10, 0], [20, 0]], { ids: ["near", "mid", "far"] });
  const report = await index.searchWithReportBuffer(new Float32Array([0, 0]), { k: 1 });

  assert.equal(report.hits[0]?.id, "near");
  assert.equal(report.segmentsTotal, 3);
  assert.equal(report.segmentsSearched, 1);
  assert.equal(report.segmentsSkipped, 2);
  assert.ok(report.bytesRead > 0);
  assert.ok(report.objectCacheMisses > 0);
});

test("approx search limits exact scoring inside each segment", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 1,
    segmentMaxVectors: 4
  });

  await index.add([[0], [0.2], [10], [20]], { ids: ["near", "next", "far-a", "far-b"] });
  const report = await index.searchWithReport([0.05], {
    k: 1,
    mode: "approx",
    maxCandidatesPerSegment: 2
  });

  assert.equal(report.hits[0]?.id, "near");
  assert.equal(report.recordsConsidered, 4);
  assert.equal(report.recordsScored, 2);
});

test("approx search enforces candidate budget when k is larger", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 1,
    segmentMaxVectors: 4
  });

  await index.add([[0], [0.2], [10], [20]], { ids: ["near", "next", "far-a", "far-b"] });
  const report = await index.searchWithReport([0.05], {
    k: 3,
    mode: "approx",
    maxCandidatesPerSegment: 2
  });

  assert.equal(report.hits.length, 2);
  assert.equal(report.recordsConsidered, 4);
  assert.equal(report.recordsScored, 2);
});

test("approx flat-scan leaf mode skips segment graph", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 1,
    segmentMaxVectors: 4
  });

  await index.add([[0], [0.2], [10], [20]], { ids: ["near", "next", "far-a", "far-b"] });
  const report = await index.searchWithReport([0.05], {
    k: 1,
    mode: SearchMode.Approx,
    leafMode: LeafModeName.FlatScan,
    maxCandidatesPerSegment: 2
  });

  assert.equal(report.leafMode, "flat-scan");
  assert.equal(report.hits[0]?.id, "near");
  assert.equal(report.recordsConsidered, 4);
  assert.equal(report.recordsScored, 2);
  assert.equal(report.graphBytesRead, 0);
  assert.equal(report.graphCandidatesAdded, 0);
});

test("approx sq-scan leaf mode uses routing codes and skips segment graph", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 4
  });

  await index.add(
    [[0, 0], [0.2, 0], [0, 0.1], [100, 100]],
    { ids: ["entry", "routing-neighbor", "graph-neighbor", "far"] }
  );
  const report = await index.searchWithReport([0.19, 0], {
    k: 1,
    mode: SearchMode.Approx,
    leafMode: LeafModeName.SqScan,
    maxCandidatesPerSegment: 2
  });

  assert.equal(report.leafMode, "sq-scan");
  assert.equal(report.hits[0]?.id, "routing-neighbor");
  assert.equal(report.recordsConsidered, 4);
  assert.equal(report.recordsScored, 2);
  assert.equal(report.graphBytesRead, 0);
  assert.equal(report.graphCandidatesAdded, 0);
});

test("approx pq-scan leaf mode uses compressed scan and skips segment graph", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 4
  });

  await index.add(
    [[0, 0], [0.2, 0], [0, 0.1], [100, 100]],
    { ids: ["entry", "routing-neighbor", "graph-neighbor", "far"] }
  );
  const report = await index.searchWithReport([0.19, 0], {
    k: 1,
    mode: SearchMode.Approx,
    leafMode: LeafModeName.PqScan,
    maxCandidatesPerSegment: 2
  });

  assert.equal(report.leafMode, "pq-scan");
  assert.equal(report.hits[0]?.id, "routing-neighbor");
  assert.equal(report.recordsConsidered, 4);
  assert.equal(report.recordsScored, 2);
  assert.equal(report.graphBytesRead, 0);
  assert.equal(report.graphCandidatesAdded, 0);
});

test("approx vamana-pq leaf mode uses segment graph and reports mode", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 4
  });

  await index.add(
    [[0, 0], [0, 0.1], [0.1, -0.1], [100, 100]],
    { ids: ["entry", "true-neighbor", "routing-decoy", "far"] }
  );
  const report = await index.searchWithReport([0.04, 0.07], {
    k: 1,
    mode: SearchMode.Approx,
    leafMode: LeafModeName.VamanaPq,
    maxCandidatesPerSegment: 2
  });

  assert.equal(report.leafMode, "vamana-pq");
  assert.equal(report.hits[0]?.id, "true-neighbor");
  assert.equal(report.recordsConsidered, 4);
  assert.equal(report.recordsScored, 2);
  assert.ok(report.graphBytesRead > 0);
  assert.equal(report.graphCandidatesAdded, 1);
});

test("approx hybrid leaf mode uses stored segment graph mode and reports mode", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 4
  });

  await index.add(
    [[0, 0], [0, 0.1], [0.1, -0.1], [100, 100]],
    { ids: ["entry", "true-neighbor", "routing-decoy", "far"] }
  );
  const report = await index.searchWithReport([0.04, 0.07], {
    k: 1,
    mode: SearchMode.Approx,
    leafMode: LeafModeName.Hybrid,
    maxCandidatesPerSegment: 2
  });

  assert.equal(report.leafMode, "hybrid");
  assert.equal(report.hits[0]?.id, "true-neighbor");
  assert.ok(report.graphBytesRead > 0);
  assert.equal(report.graphCandidatesAdded, 1);
});

test("local package search reports stay subsecond", async () => {
  const dimensions = 16;
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-perf-"));
  const index = await create({
    uri: localUri(dir),
    metric: VectorMetricName.Euclidean,
    dimensions,
    segmentMaxVectors: 128
  });
  const vectors = Array.from({ length: 1024 }, (_, seed) => deterministicVector(seed, dimensions));
  const ids = Array.from({ length: 1024 }, (_, seed) => `doc-${seed}`);
  await index.add(vectors, { ids });
  const query = deterministicVector(42, dimensions);

  const exactReport = await index.searchWithReport(query, { k: 10 });
  const exactTermination: SearchTerminationReason = exactReport.terminationReason;
  const approxReport = await index.searchWithReport(query, {
    k: 10,
    mode: SearchMode.Approx,
    leafMode: LeafModeName.Hybrid,
    maxCandidatesPerSegment: 32
  });

  assert.equal(exactReport.hits[0]?.id, "doc-42");
  assert.ok(["complete", "exact-pruned"].includes(exactTermination));
  assert.ok(exactReport.elapsedMs < 1000);
  assert.ok(approxReport.elapsedMs < 1000);
  assert.equal(approxReport.leafMode, "hybrid");
  assert.ok(approxReport.bytesRead > 0);
  assert.ok(approxReport.graphBytesRead > 0);
  assert.ok(approxReport.recordsScored < approxReport.recordsConsidered);
  assert.ok(approxReport.residentBytesEstimate > 0);
});

test("approx search obeys byte budget", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add([[0, 0], [10, 0], [20, 0]], { ids: ["near", "mid", "far"] });
  const report = await index.searchWithReport([0, 0], {
    k: 3,
    mode: "approx",
    maxBytes: 1
  });

  assert.deepEqual(report.hits.map((hit) => hit.id), []);
  assert.equal(report.segmentsSearched, 0);
  assert.equal(report.segmentsSkipped, 3);
  assert.ok(report.bytesRead > 1);
  assert.equal(report.terminationReason, "max-bytes");
});

test("approx search accepts byte budget string", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add([[0, 0], [10, 0], [20, 0]], { ids: ["near", "mid", "far"] });
  const report = await index.searchWithReport([0, 0], {
    k: 1,
    mode: "approx",
    maxBytes: "1MiB"
  });

  assert.deepEqual(report.hits.map((hit) => hit.id), ["near"]);
  assert.equal(report.segmentsSearched, 3);
  assert.equal(report.segmentsSkipped, 0);
  assert.equal(report.terminationReason, "complete");
});

test("approx search rejects invalid budgets", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add([[0, 0]], { ids: ["near"] });

  for (const [options, expected] of [
    [{ eps: -0.1 }, /eps must be finite and non-negative when set/],
    [{ eps: Number.POSITIVE_INFINITY }, /eps must be finite and non-negative when set/],
    [{ maxSegments: 0 }, /max_segments must be greater than zero when set/],
    [{ maxBytes: 0 }, /max_bytes must be greater than zero when set/],
    [{ maxLatencyMs: 0 }, /max_latency_ms must be greater than zero when set/],
    [{ routingPageOverfetch: 0 }, /routing_page_overfetch must be greater than zero when set/],
    [
      { maxCandidatesPerSegment: 0 },
      /max_candidates_per_segment must be greater than zero when set/
    ]
  ] as const) {
    await assert.rejects(
      () =>
        index.searchWithReport([0, 0], {
          k: 1,
          mode: "approx",
          ...options
        }),
      expected
    );
  }

  for (const [options, expected] of [
    [{ maxSegments: 1.5 }, /max_segments must be an integer when set/],
    [{ maxBytes: 1.5 }, /max_bytes must be an integer when set/],
    [{ maxLatencyMs: Number.NaN }, /max_latency_ms must be an integer when set/],
    [
      { routingPageOverfetch: true as unknown as number },
      /routing_page_overfetch must be an integer when set/
    ],
    [
      { maxCandidatesPerSegment: 1.5 },
      /max_candidates_per_segment must be an integer when set/
    ]
  ] as const) {
    await assert.rejects(
      () =>
        index.searchWithReport([0, 0], {
          k: 1,
          mode: "approx",
          ...options
        }),
      expected
    );
  }
});

test("search rejects zero k", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-zero-k-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add([[0, 0]], { ids: ["near"] });

  await assert.rejects(() => index.searchIds([0, 0], { k: 0 }), /k must be greater than zero/);
  await assert.rejects(() => index.searchIds([0, 0], { k: 1.5 }), /k must be an integer/);
  await assert.rejects(
    () => index.searchIds([0, 0], { k: Number.NaN }),
    /k must be an integer/
  );
  await assert.rejects(
    () => index.searchIds([0, 0], { k: true as unknown as number }),
    /k must be an integer/
  );
  await assert.rejects(
    () =>
      index.searchWithReport([0, 0], {
        k: 0,
        mode: "approx"
      }),
    /k must be greater than zero/
  );
  await assert.rejects(
    () =>
      index.searchWithReport([0, 0], {
        k: 1.5,
        mode: "approx"
      }),
    /k must be an integer/
  );
});

test("approx search expands segment graph candidates", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 4
  });

  await index.add(
    [[0, 0], [0, 0.1], [0.1, -0.1], [100, 100]],
    { ids: ["entry", "true-neighbor", "routing-decoy", "far"] }
  );
  const report = await index.searchWithReport([0.04, 0.07], {
    k: 1,
    mode: "approx",
    maxCandidatesPerSegment: 2
  });

  assert.equal(report.hits[0]?.id, "true-neighbor");
  assert.equal(report.leafMode, "graph");
  assert.equal(report.recordsConsidered, 4);
  assert.equal(report.recordsScored, 2);
  assert.ok(report.graphBytesRead > 0);
  assert.equal(report.graphCandidatesAdded, 1);
});

test("approx search walks segment graph beyond first hop", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 10
  });

  await index.add(
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
    ],
    {
      ids: [
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
      ]
    }
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
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 4
  });

  await writer.add(
    [[0, 0], [0, 0.1], [0.1, -0.1], [100, 100]],
    { ids: ["entry", "true-neighbor", "routing-decoy", "far"] }
  );
  const index = open(localUri(dir), { cacheDir: cache });
  const report = await index.searchWithReport([0.04, 0.07], {
    k: 1,
    mode: "approx",
    maxCandidatesPerSegment: 2
  });

  assert.equal(report.hits[0]?.id, "true-neighbor");
  assert.ok(report.graphBytesRead > 0);
  assert.equal(report.objectCacheHits, 0);
  assert.equal(report.objectCacheMisses, 4);
  assert.equal(hasParquetFiles(join(cache, "segments")), true);
  assert.equal(hasParquetFiles(join(cache, "graphs")), true);
});

test("S3-compatible storage round trips when configured", async (t) => {
  const baseUri = process.env.BORSUK_S3_TEST_URI;
  if (!baseUri) {
    t.skip("BORSUK_S3_TEST_URI is not set");
    return;
  }

  const uri = `${baseUri.replace(/\/+$/, "")}/typescript-${randomUUID()}`;
  const cache = mkdtempSync(join(tmpdir(), "borsuk-ts-s3-cache-"));
  const index = await create({
    uri,
    metric: "euclidean",
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
    mode: "approx",
    maxCandidatesPerSegment: 2
  });

  assert.equal(report.hits[0]?.id, "true-neighbor");
  assert.ok(report.graphBytesRead > 0);
  assert.ok(report.objectCacheMisses > 0);
  assert.equal(hasParquetFiles(join(cache, "segments")), true);
  assert.equal(hasParquetFiles(join(cache, "graphs")), true);

  const compaction = await reopened.compact({
    sourceLevel: 0,
    targetLevel: 1,
    maxSegments: 2,
    minSegments: 2,
    targetSegmentMaxVectors: 4
  });
  assert.equal(compaction.compacted, true);
  assert.equal(compaction.segmentsWritten, 1);

  const gc = await reopened.gcObsoleteSegments();
  assert.equal(gc.dryRun, true);
  assert.equal(gc.candidates.length > 0, true);
});

test("compact rewrites segments and reports counters", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add([[0, 0], [1, 0], [8, 0], [9, 0]], { ids: ["a", "b", "c", "d"] });
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
  assert.equal(report.routingPageIndexesRead, 1);
  assert.equal(report.routingPagesRead, 1);
  assert.equal(report.routingPageIndexesWritten >= 1, true);
  assert.equal(report.routingPagesWritten >= 1, true);
  assert.equal(report.graphPayloadsRead, 0);
  assert.equal(report.graphBytesRead, 0);
  assert.equal(report.objectCacheHits, 0);
  assert.equal(report.objectCacheMisses, 6);

  const after = await index.searchWithReport([8.5, 0], { k: 2 });
  assert.equal(after.segmentsTotal, 2);
  assert.deepEqual(after.hits.map((hit) => hit.id), ["c", "d"]);
});

test("compact default uses bounded source batch", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add(
    Array.from({ length: 34 }, (_, value) => [value, 0]),
    { ids: Array.from({ length: 34 }, (_, value) => `v${value}`) }
  );

  const report = await index.compact({
    minSegments: 1,
    targetSegmentMaxVectors: 1
  });

  assert.equal(report.compacted, true);
  assert.equal(report.segmentsRead, 32);
  assert.equal(report.recordsRewritten, 32);
  assert.equal((await index.stats()).segments, 34);
  assert.deepEqual(await index.getVector("v33"), [33, 0]);
});

test("compact rejects impossible batch thresholds", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await assert.rejects(
    () => index.compact({ maxSegments: 1, minSegments: 2 }),
    /min_segments must be less than or equal to max_segments when max_segments is set/
  );
});

test("compact rejects non-integer options", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  for (const [options, expected] of [
    [{ sourceLevel: 0.5 }, /source_level must be an integer when set/],
    [{ targetLevel: 1.5 }, /target_level must be an integer when set/],
    [{ maxSegments: 1.5 }, /max_segments must be an integer when set/],
    [{ minSegments: Number.NaN }, /min_segments must be an integer when set/],
    [
      { targetSegmentMaxVectors: true as unknown as number },
      /target_segment_max_vectors must be an integer when set/
    ]
  ] as const) {
    await assert.rejects(() => index.compact(options), expected);
  }
});

test("compact rejects non-boolean allMatching option", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await assert.rejects(
    () => index.compact({ allMatching: "yes" as unknown as boolean }),
    /all_matching must be a boolean when set/
  );
});

test("rebuild compacts all matching segments and deletes obsolete objects", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add([[0, 0], [1, 0], [8, 0], [9, 0]], { ids: ["a", "b", "c", "d"] });
  const report = await index.rebuild({
    sourceLevel: 0,
    targetLevel: 1,
    minSegments: 1,
    targetSegmentMaxVectors: 2,
    deleteObsolete: true
  });

  assert.equal(report.compaction.compacted, true);
  assert.equal(report.compaction.segmentsRead, 4);
  assert.equal(report.compaction.segmentsWritten, 2);
  assert.equal(report.garbageCollection.dryRun, false);
  assert.equal(report.garbageCollection.objectsDeleted, 8);
  assert.equal(report.garbageCollection.candidates.length, 8);
  const ids = await index.searchIds([8.5, 0], { k: 2 });
  assert.deepEqual(ids, ["c", "d"]);
});

test("rebuild rejects non-integer options", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  for (const [options, expected] of [
    [{ sourceLevel: 0.5 }, /source_level must be an integer when set/],
    [{ targetLevel: 1.5 }, /target_level must be an integer when set/],
    [{ minSegments: Number.NaN }, /min_segments must be an integer when set/],
    [
      { targetSegmentMaxVectors: true as unknown as number },
      /target_segment_max_vectors must be an integer when set/
    ]
  ] as const) {
    await assert.rejects(() => index.rebuild(options), expected);
  }
});

test("rebuild rejects non-boolean deleteObsolete option", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await assert.rejects(
    () => index.rebuild({ deleteObsolete: 1 as unknown as boolean }),
    /delete_obsolete must be a boolean when set/
  );
});

test("gcObsoleteSegments dry-runs and deletes inactive segments", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await index.add([[0, 0], [1, 0], [8, 0], [9, 0]], { ids: ["a", "b", "c", "d"] });
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

  assert.deepEqual(await index.searchIds([8.5, 0], { k: 2 }), ["c", "d"]);
});

test("gcObsoleteSegments rejects non-boolean dryRun option", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1
  });

  await assert.rejects(
    () => index.gcObsoleteSegments({ dryRun: 1 as unknown as boolean }),
    /dry_run must be a boolean when set/
  );
});

test("gcObsoleteSegments removes cached inactive objects", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-cache-gc-"));
  const cache = mkdtempSync(join(tmpdir(), "borsuk-ts-cache-gc-cache-"));
  const index = await create({
    uri: localUri(dir),
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1,
    cacheDir: cache
  });

  await index.add([[0, 0], [1, 0], [8, 0], [9, 0]], { ids: ["a", "b", "c", "d"] });
  await index.compact({
    sourceLevel: 0,
    targetLevel: 1,
    maxSegments: 4,
    minSegments: 2,
    targetSegmentMaxVectors: 2
  });

  assert.equal(parquetFiles(join(cache, "segments", "L0")).length, 4);
  assert.equal(parquetFiles(join(cache, "graphs", "L0")).length, 4);

  const deleted = await index.gcObsoleteSegments({ dryRun: false });

  assert.equal(deleted.objectsDeleted, 8);
  assert.equal(parquetFiles(join(cache, "segments", "L0")).length, 0);
  assert.equal(parquetFiles(join(cache, "graphs", "L0")).length, 0);
  assert.equal(parquetFiles(join(cache, "segments", "L1")).length, 2);
  assert.equal(parquetFiles(join(cache, "graphs", "L1")).length, 2);
});

function hasParquetFiles(root: string): boolean {
  return parquetFiles(root).length > 0;
}

function parquetFiles(root: string): string[] {
  if (!existsSync(root)) {
    return [];
  }
  return readdirSync(root, { withFileTypes: true }).flatMap((entry) => {
    const path = join(root, entry.name);
    if (entry.isDirectory()) {
      return parquetFiles(path);
    }

    return entry.name.endsWith(".parquet") ? [path] : [];
  });
}

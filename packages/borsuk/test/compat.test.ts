// Drop-in adapter tests: each emulated SDK surface must round-trip data through
// BORSUK — create, upsert with metadata, filtered query, fetch/get, delete.
import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";

import { Pinecone } from "../src/compat/pinecone.js";
import { client as s3vectorsClient } from "../src/compat/s3vectors.js";
import { Turbopuffer } from "../src/compat/turbopuffer.js";
import { mapMetric, translateTurbopufferFilter } from "../src/compat/common.js";

function baseUri(): string {
  return pathToFileURL(mkdtempSync(join(tmpdir(), "borsuk-compat-"))).href;
}

test("pinecone adapter round-trips upsert, filtered query, fetch, delete", async () => {
  const pc = new Pinecone({
    apiKey: "ignored",
    baseUri: baseUri(),
    dimension: 2,
    metric: "cosine",
  });
  const index = pc.Index("songs");
  await index.upsert(
    [
      { id: "a", values: [1, 0], metadata: { genre: "rock", year: 1975 } },
      ["b", [0, 1], { genre: "jazz", year: 1999 }],
      { id: "c", values: [1, 0.1], metadata: { genre: "rock", year: 2001 } },
    ],
    "store-1",
  );

  const res = await index.query({
    vector: [1, 0],
    topK: 10,
    filter: { genre: { $eq: "rock" } },
    includeMetadata: true,
    includeValues: true,
    namespace: "store-1",
  });
  assert.deepEqual(new Set(res.matches.map((m) => m.id)), new Set(["a", "c"]));
  assert.ok(res.matches.every((m) => (m.metadata as { genre: string }).genre === "rock"));
  assert.equal(res.matches[0].values?.length, 2);

  const fetched = await index.fetch(["a"], "store-1");
  assert.equal((fetched.vectors.a.metadata as { year: number }).year, 1975);

  // Namespaces are isolated.
  const other = await index.query({ vector: [1, 0], namespace: "other" });
  assert.deepEqual(other.matches, []);

  await index.delete({ ids: ["a"], namespace: "store-1" });
  const after = await index.query({
    vector: [1, 0],
    filter: { genre: "rock" },
    namespace: "store-1",
  });
  assert.deepEqual(
    after.matches.map((m) => m.id),
    ["c"],
  );

  const stats = await index.describeIndexStats();
  assert.equal(stats.dimension, 2);
});

test("pinecone upsert overwrites an existing id", async () => {
  const pc = new Pinecone({ baseUri: baseUri(), dimension: 2, metric: "euclidean" });
  const index = pc.Index("i");
  await index.upsert([["x", [0, 0], { v: 1 }]]);
  await index.upsert([["x", [9, 9], { v: 2 }]]);
  const fetched = await index.fetch(["x"]);
  assert.equal((fetched.vectors.x.metadata as { v: number }).v, 2);
});

test("pinecone list / listPaginated enumerate ids with prefix and cursor", async () => {
  const pc = new Pinecone({ baseUri: baseUri(), dimension: 2, metric: "euclidean" });
  const index = pc.Index("c");
  await index.upsert(
    Array.from(
      { length: 5 },
      (_, i) => [`a${i}`, [i, 0], {}] as [string, number[], Record<string, unknown>],
    ),
  );
  await index.upsert([["target", [0, 1], {}] as [string, number[], Record<string, unknown>]]);

  // list() auto-follows the cursor across every id.
  const seen: string[] = [];
  for await (const ids of index.list({ limit: 2 })) {
    seen.push(...ids);
  }
  assert.deepEqual([...seen].sort(), ["a0", "a1", "a2", "a3", "a4", "target"].sort());

  // A prefix matching only a late record is still found in one page.
  const page = await index.listPaginated({ prefix: "target", limit: 2 });
  assert.deepEqual(
    page.vectors.map((v) => v.id),
    ["target"],
  );
  assert.notEqual(page.pagination.next, undefined);

  // A non-positive limit is rejected rather than looping forever.
  await assert.rejects(() => index.listPaginated({ limit: 0 }));
});

test("s3vectors adapter round-trips put, filtered query, get, delete", async () => {
  const s3v = s3vectorsClient("s3vectors", { baseUri: baseUri() });
  s3v.createVectorBucket({ vectorBucketName: "media" });
  s3v.createIndex({
    vectorBucketName: "media",
    indexName: "movies",
    dimension: 2,
    distanceMetric: "cosine",
  });
  await s3v.putVectors({
    vectorBucketName: "media",
    indexName: "movies",
    vectors: [
      { key: "star-wars", data: { float32: [1, 0] }, metadata: { genre: "scifi" } },
      { key: "casablanca", data: { float32: [0, 1] }, metadata: { genre: "drama" } },
    ],
  });

  const res = await s3v.queryVectors({
    vectorBucketName: "media",
    indexName: "movies",
    queryVector: { float32: [1, 0] },
    topK: 5,
    filter: { genre: "scifi" },
    returnMetadata: true,
    returnDistance: true,
  });
  assert.deepEqual(
    res.vectors.map((v) => v.key),
    ["star-wars"],
  );
  assert.equal((res.vectors[0].metadata as { genre: string }).genre, "scifi");
  assert.ok(typeof res.vectors[0].distance === "number");

  const got = await s3v.getVectors({
    vectorBucketName: "media",
    indexName: "movies",
    keys: ["star-wars"],
    returnData: true,
    returnMetadata: true,
  });
  assert.deepEqual(got.vectors[0].data?.float32, [1, 0]);

  await s3v.deleteVectors({ vectorBucketName: "media", indexName: "movies", keys: ["star-wars"] });
  const after = await s3v.queryVectors({
    vectorBucketName: "media",
    indexName: "movies",
    queryVector: { float32: [1, 0] },
    topK: 5,
  });
  assert.deepEqual(
    after.vectors.map((v) => v.key),
    ["casablanca"],
  );
});

test("s3vectors query on a missing index throws", async () => {
  const s3v = s3vectorsClient("s3vectors", { baseUri: baseUri() });
  await assert.rejects(() =>
    s3v.queryVectors({
      vectorBucketName: "nope",
      indexName: "nope",
      queryVector: { float32: [1, 0] },
    }),
  );
});

test("turbopuffer adapter writes rows and queries with a tuple filter", async () => {
  const tpuf = new Turbopuffer({ baseUri: baseUri(), dimension: 2 });
  const ns = tpuf.namespace("products");
  await ns.write({
    upsertRows: [
      { id: "1", vector: [1, 0], category: "animal", public: 1 },
      { id: "2", vector: [0, 1], category: "plant", public: 1 },
      { id: "3", vector: [1, 0.1], category: "animal", public: 0 },
    ],
    distanceMetric: "cosine_distance",
  });

  const results = await ns.query({
    rankBy: ["vector", "ANN", [1, 0]],
    topK: 10,
    filters: [
      "And",
      [
        ["category", "Eq", "animal"],
        ["public", "Eq", 1],
      ],
    ],
    includeAttributes: ["category"],
  });
  assert.deepEqual(
    results.map((row) => row.id),
    ["1"],
  );
  assert.equal(results[0].category, "animal");

  await ns.write({ deletes: ["1"] });
  const after = await ns.query({
    rankBy: ["vector", "ANN", [1, 0]],
    filters: ["category", "Eq", "animal"],
  });
  assert.deepEqual(
    after.map((row) => row.id),
    ["3"],
  );
});

test("filter translation and metric mapping", () => {
  assert.deepEqual(translateTurbopufferFilter(["genre", "Eq", "rock"]), { genre: { $eq: "rock" } });
  assert.deepEqual(
    translateTurbopufferFilter([
      "And",
      [
        ["g", "In", ["a", "b"]],
        ["Not", ["y", "Lt", 2000]],
      ],
    ]),
    { $and: [{ g: { $in: ["a", "b"] } }, { $not: { y: { $lt: 2000 } } }] },
  );
  assert.throws(() => translateTurbopufferFilter(["g", "Glob", "r*"]));

  assert.equal(mapMetric("pinecone", "dotproduct"), "inner-product");
  assert.equal(mapMetric("turbopuffer", "euclidean_squared"), "squared-euclidean");
  assert.equal(mapMetric("s3vectors", "cosine"), "cosine");
  assert.throws(() => mapMetric("pinecone", "hamming"));
});

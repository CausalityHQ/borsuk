// BORSUK cookbook: every retrieval mode and the ways to mix them.
//
// Runs end-to-end (exercised in CI), so every snippet is guaranteed to work
// against the current build. Covers dense search, versioned upsert (overwrite),
// metadata filtering, full-text BM25, sparse (lexical) named vectors, hybrid
// fusion (dense + text, RRF and weighted), a retrieve-then-rerank RAG pattern,
// and query cost / explain.

import { create, VectorMetricName } from "../src/index.js";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { strict as assert } from "node:assert";

async function withIndex<T>(
  options: Record<string, unknown>,
  body: (index: Awaited<ReturnType<typeof create>>) => Promise<T>,
): Promise<T> {
  const root = mkdtempSync(join(tmpdir(), "borsuk-cookbook-"));
  try {
    const index = await create({ uri: pathToFileURL(root).href, ...options } as never);
    return await body(index);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

async function denseSearchAndUpsert(): Promise<void> {
  await withIndex({ metric: VectorMetricName.Euclidean, dimensions: 2 }, async (index) => {
    await index.add(
      [
        [0, 0],
        [1, 0],
        [0, 1],
      ],
      { ids: ["a", "b", "c"] },
    );
    assert.deepEqual(await index.searchIds([0.1, 0], { k: 2 }), ["a", "b"]);

    // upsert overwrites "a" in place (add would reject the existing id).
    await index.upsert([[0, 9]], ["a"]);
    const nearOrigin = await index.searchIds([0, 0], { k: 3 });
    assert.equal(nearOrigin[0], "b");
    assert.equal(nearOrigin.filter((id) => id === "a").length, 1);
    console.log("dense + upsert:", nearOrigin);
  });
}

async function metadataFiltering(): Promise<void> {
  await withIndex({ metric: VectorMetricName.Euclidean, dimensions: 2 }, async (index) => {
    await index.add(
      [
        [0, 0],
        [0.1, 0],
        [0.2, 0],
      ],
      {
        ids: ["a", "b", "c"],
        metadata: [{ genre: "comedy" }, { genre: "drama" }, { genre: "comedy" }],
      },
    );
    const report = await index.searchWithReport([0, 0], {
      k: 5,
      filter: { genre: { $eq: "comedy" } },
      includeMetadata: true,
    });
    const ids = report.hits.map((hit) => hit.id);
    assert.deepEqual(ids, ["a", "c"]);
    console.log("filtered (genre=comedy):", ids);
  });
}

async function fullTextBm25(): Promise<void> {
  await withIndex(
    { metric: VectorMetricName.Euclidean, dimensions: 2, text: true },
    async (index) => {
      await index.add(
        [
          [0, 0],
          [1, 0],
          [2, 0],
        ],
        {
          ids: ["a", "b", "c"],
          text: ["the quick brown fox", "a needle in a haystack", "needle needle everywhere"],
        },
      );
      const ids = await index.searchText("needle", { k: 2 });
      assert.deepEqual([...ids].sort(), ["b", "c"]);
      console.log("bm25 'needle':", ids);
    },
  );
}

async function sparseLexicalNamedVector(): Promise<void> {
  await withIndex(
    {
      metric: VectorMetricName.Euclidean,
      dimensions: 2,
      namedVectors: { lexical: { dimensions: 100000, metric: "inner-product", kind: "sparse" } },
    },
    async (index) => {
      await index.add(
        [
          [0, 0],
          [1, 0],
        ],
        {
          ids: ["a", "b"],
          namedVectors: [
            { lexical: { indices: [5, 7], values: [1, 2] } },
            { lexical: { indices: [5, 9], values: [3, 1] } },
          ],
        },
      );
      assert.deepEqual([...(await index.searchSparseNamed("lexical", [5], [1], 5))].sort(), [
        "a",
        "b",
      ]);
      assert.deepEqual(await index.searchSparseNamed("lexical", [7], [1], 5), ["a"]);
      console.log("sparse lexical (term 7):", await index.searchSparseNamed("lexical", [7], [1]));
    },
  );
}

async function hybridFusion(): Promise<void> {
  await withIndex(
    { metric: VectorMetricName.Euclidean, dimensions: 2, text: true },
    async (index) => {
      await index.add(
        [
          [0, 0],
          [1, 0],
          [0, 1],
        ],
        { ids: ["a", "b", "c"], text: ["red apple", "green apple pie", "blue sky"] },
      );
      const rrf = await index.searchHybrid(
        { vectors: { "": [0, 0] }, text: "apple" },
        { k: 3, fusion: "rrf" },
      );
      console.log("hybrid rrf:", rrf);
      const weighted = await index.searchHybrid(
        { vectors: { "": [0, 0] }, text: "apple" },
        { k: 3, fusion: "weighted", weights: { "": 0.2, "@text": 1.0 } },
      );
      console.log("hybrid weighted (text-heavy):", weighted);
      assert.ok(rrf.length > 0 && weighted.length > 0);
    },
  );
}

async function ragRetrieveThenRerank(): Promise<void> {
  await withIndex({ metric: VectorMetricName.Euclidean, dimensions: 2 }, async (index) => {
    await index.add(
      [
        [0, 0],
        [0.1, 0],
        [0.2, 0],
        [0.3, 0],
      ],
      {
        ids: ["a", "b", "c", "d"],
        metadata: [{ priority: 1 }, { priority: 4 }, { priority: 3 }, { priority: 2 }],
      },
    );
    // 1) Retrieve a wide candidate set with metadata.
    const report = await index.searchWithReport([0, 0], { k: 4, includeMetadata: true });
    // 2) Rerank with your model (here: descending priority).
    const reranked = report.hits
      .map((hit) => ({ id: hit.id, priority: (hit.metadata as { priority: number }).priority }))
      .sort((left, right) => right.priority - left.priority);
    // 3) Keep the top-2 as LLM context.
    const contextIds = reranked.slice(0, 2).map((entry) => entry.id);
    assert.deepEqual(contextIds, ["b", "c"]);
    console.log("rag reranked top-2:", contextIds);
  });
}

async function queryCostExplain(): Promise<void> {
  await withIndex({ metric: VectorMetricName.Euclidean, dimensions: 2 }, async (index) => {
    await index.add(
      Array.from({ length: 20 }, (_, i) => [i, 0]),
      { ids: Array.from({ length: 20 }, (_, i) => `k${i}`) },
    );
    const plan = await index.explain([0, 0], { k: 5 });
    console.log("explain:", {
      hits: plan.hits.length,
      getRequests: plan.getRequests,
      bytesRead: plan.bytesRead,
      estimatedCostUsd: plan.estimatedCostUsd,
      cacheHitRatio: plan.cacheHitRatio,
    });
    assert.ok(plan.estimatedCostUsd >= 0);
  });
}

async function main(): Promise<void> {
  await denseSearchAndUpsert();
  await metadataFiltering();
  await fullTextBm25();
  await sparseLexicalNamedVector();
  await hybridFusion();
  await ragRetrieveThenRerank();
  await queryCostExplain();
  console.log("cookbook ok");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});

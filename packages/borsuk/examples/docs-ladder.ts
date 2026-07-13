// The example ladder shown on the docs site, from a first search to production.
// Every snippet the website renders is extracted verbatim from the `docs:` marker
// regions below, and this example runs in CI, so the code on the page always
// works. Keep the marker regions self-contained and copy-pasteable; put throwaway
// setup (temp directories, cleanup) outside the markers.

import { create, LeafModeName, open, SearchMode, VectorMetricName } from "../src/index.js";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

async function rungHello(): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "borsuk-ladder-hello-"));
  // docs:hello:start
  // Create an index. It lives entirely as files under `uri` — a local path here,
  // or an `s3://…` URI for object storage. Nothing else to run.
  const index = await create({
    uri: pathToFileURL(root).href,
    metric: VectorMetricName.Euclidean,
    dimensions: 3,
    segmentMaxVectors: 4096,
  });

  // Add a few vectors with your own ids.
  await index.add(
    [
      [0, 0, 0],
      [1, 0, 0],
      [0, 5, 0],
    ],
    ["alpha", "beta", "gamma"],
  );

  // Ask for the 2 nearest neighbours. `k` with exact mode returns the true top-k.
  const ids = await index.searchIds([0.1, 0, 0], { k: 2 });
  console.log("nearest:", ids);
  // docs:hello:end
  if (ids.join(",") !== "alpha,beta") throw new Error(`unexpected hits: ${ids.join(",")}`);
  rmSync(root, { recursive: true, force: true });
}

async function rungReport(): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "borsuk-ladder-report-"));
  const index = await create({
    uri: pathToFileURL(root).href,
    metric: VectorMetricName.Euclidean,
    dimensions: 3,
    segmentMaxVectors: 4096,
  });
  await index.add(
    [
      [0, 0, 0],
      [1, 0, 0],
      [0, 5, 0],
    ],
    ["alpha", "beta", "gamma"],
  );

  // docs:report:start
  // `searchWithReport` returns the hits plus everything the query touched: bytes
  // read, segments searched, and the object-store requests it issued.
  const report = await index.searchWithReport([0.1, 0, 0], { k: 2, mode: SearchMode.Exact });
  console.log(
    `hits=${report.hits.map((hit) => hit.id).join(",")} ` +
      `bytesRead=${report.bytesRead} segmentsSearched=${report.segmentsSearched} ` +
      `requests=${report.requests.total} (gets=${report.requests.gets}, heads=${report.requests.heads})`,
  );
  // docs:report:end
  rmSync(root, { recursive: true, force: true });
}

async function rungFilter(): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "borsuk-ladder-filter-"));
  const index = await create({
    uri: pathToFileURL(root).href,
    metric: VectorMetricName.Euclidean,
    dimensions: 2,
    segmentMaxVectors: 4096,
  });
  // docs:filter:start
  // Attach schemaless metadata to any vector, then constrain a search with a
  // Pinecone-style operator dict. The filter is applied *before* ranking, so a
  // selective filter is fast and exact — whole segments that cannot match are
  // skipped unread.
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
  console.log("filtered (genre=comedy):", ids);
  // docs:filter:end
  if (ids.join(",") !== "a,c") throw new Error(`unexpected hits: ${ids.join(",")}`);
  rmSync(root, { recursive: true, force: true });
}

async function rungUpsert(): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "borsuk-ladder-upsert-"));
  const index = await create({
    uri: pathToFileURL(root).href,
    metric: VectorMetricName.Euclidean,
    dimensions: 2,
    segmentMaxVectors: 4096,
  });
  // docs:upsert:start
  // `add` is insert-only; `upsert` inserts-or-replaces by id in one atomic
  // publish. Reads immediately see only the new version, and there is only ever
  // one live copy of an id — the superseded one is reclaimed by compaction.
  await index.add(
    [
      [0, 0],
      [1, 0],
    ],
    ["a", "b"],
  );
  await index.upsert([[0, 9]], ["a"]); // move "a" away from the origin

  const nearOrigin = await index.searchIds([0, 0], { k: 3 });
  console.log("after upsert, nearest origin:", nearOrigin);
  // docs:upsert:end
  if (nearOrigin[0] !== "b" || nearOrigin.filter((id) => id === "a").length !== 1)
    throw new Error(`unexpected hits: ${nearOrigin.join(",")}`);
  rmSync(root, { recursive: true, force: true });
}

async function rungHybrid(): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "borsuk-ladder-hybrid-"));
  const index = await create({
    uri: pathToFileURL(root).href,
    metric: VectorMetricName.Euclidean,
    dimensions: 2,
    text: true,
  });
  // docs:hybrid:start
  // Turn on `text` to index BM25 alongside the vectors, then fuse both legs in
  // one query. Reciprocal-rank fusion (the default) needs no tuning; switch to
  // weighted fusion when you want to lean on one leg.
  await index.add(
    [
      [0, 0],
      [1, 0],
      [0, 1],
    ],
    { ids: ["a", "b", "c"], text: ["red apple", "green apple pie", "blue sky"] },
  );
  const hits = await index.searchHybrid({ vectors: { "": [0, 0] }, text: "apple" }, { k: 3 });
  console.log("hybrid (dense + text):", hits);
  // docs:hybrid:end
  if (hits.length === 0) throw new Error("hybrid returned no hits");
  rmSync(root, { recursive: true, force: true });
}

async function rungTuning(): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "borsuk-ladder-tuning-"));
  const index = await create({
    uri: pathToFileURL(root).href,
    metric: VectorMetricName.Euclidean,
    dimensions: 3,
    segmentMaxVectors: 2,
  });
  await index.add(
    [
      [0, 0, 0],
      [1, 0, 0],
      [0, 5, 0],
      [9, 0, 0],
    ],
    ["alpha", "beta", "gamma", "delta"],
  );

  // docs:tuning:start
  // Approximate search spends three explicit budgets instead of hidden magic: how
  // many segments to read, how much routing metadata to look ahead, and how many
  // rows to exact-score per segment. Tighten budgets while watching the report —
  // smaller budgets read less but can lower recall.
  const query = [0.1, 0, 0];
  const cheap = await index.searchWithReport(query, {
    k: 2,
    mode: SearchMode.Approx,
    leafMode: LeafModeName.PqScan,
    maxSegments: 1,
    maxCandidatesPerSegment: 2,
  });
  const thorough = await index.searchWithReport(query, {
    k: 2,
    mode: SearchMode.Approx,
    leafMode: LeafModeName.PqScan,
    maxSegments: 8,
    routingPageOverfetch: 8,
  });
  console.log(
    `cheap: ${cheap.segmentsSearched} segments, ${cheap.bytesRead} bytes | ` +
      `thorough: ${thorough.segmentsSearched} segments, ${thorough.bytesRead} bytes`,
  );
  // docs:tuning:end
  rmSync(root, { recursive: true, force: true });
}

async function rungProduction(): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "borsuk-ladder-production-"));
  const cache = mkdtempSync(join(tmpdir(), "borsuk-ladder-cache-"));
  const uri = pathToFileURL(root).href;
  const seed = await create({
    uri,
    metric: VectorMetricName.Euclidean,
    dimensions: 3,
    segmentMaxVectors: 4096,
  });
  await seed.add(
    [
      [0, 0, 0],
      [1, 0, 0],
      [0, 5, 0],
    ],
    ["alpha", "beta", "gamma"],
  );

  // docs:production:start
  // Open for serving. Paged routing (the default) keeps resident memory near zero;
  // a local `cacheDir` keeps fetched objects on fast disk. Every report carries the
  // object-store requests it issued, so you can chart requests-per-query straight
  // from production traffic.
  const index = open(uri, { cacheDir: cache });
  const report = await index.searchWithReport([0.1, 0, 0], {
    k: 2,
    mode: SearchMode.Approx,
    leafMode: LeafModeName.PqScan,
  });
  console.log(
    `requests/query: ${report.requests.total} ` +
      `(gets=${report.requests.gets}, heads=${report.requests.heads}, lists=${report.requests.lists})`,
  );
  // docs:production:end
  rmSync(root, { recursive: true, force: true });
  rmSync(cache, { recursive: true, force: true });
}

async function main(): Promise<void> {
  await rungHello();
  await rungReport();
  await rungFilter();
  await rungUpsert();
  await rungHybrid();
  await rungTuning();
  await rungProduction();
  console.log("docs ladder ok");
}

await main();

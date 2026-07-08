// Cross-language parity: the shared fixture must produce identical results here
// and in the Python binding (python/tests/test_parity.py).
import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";

import { create } from "../src/index.js";
import type { VectorMetric } from "../src/index.js";

function locateFixture(): string {
  const relative = join("tests", "fixtures", "metadata_parity.json");
  let dir = dirname(fileURLToPath(import.meta.url));
  for (let depth = 0; depth < 8; depth += 1) {
    const candidate = join(dir, relative);
    if (existsSync(candidate)) {
      return candidate;
    }
    dir = dirname(dir);
  }
  throw new Error(`could not locate ${relative} above the test directory`);
}

const fixturePath = locateFixture();

interface FixtureRecord {
  id: string;
  vector: number[];
  metadata: Record<string, unknown>;
}

interface FixtureQuery {
  name: string;
  vector: number[];
  k: number;
  filter: Record<string, unknown>;
  expectedIds: string[];
}

interface Fixture {
  metric: VectorMetric;
  dimensions: number;
  segmentMaxVectors: number;
  records: FixtureRecord[];
  queries: FixtureQuery[];
}

function localUri(path: string): string {
  return pathToFileURL(path).href;
}

test("shared metadata fixture matches expected results", async () => {
  const spec = JSON.parse(readFileSync(fixturePath, "utf8")) as Fixture;
  const byId = new Map(spec.records.map((record) => [record.id, record]));

  const dir = mkdtempSync(join(tmpdir(), "borsuk-parity-"));
  const index = await create({
    uri: localUri(dir),
    metric: spec.metric,
    dimensions: spec.dimensions,
    segmentMaxVectors: spec.segmentMaxVectors
  });

  await index.add(
    spec.records.map((record) => record.vector),
    {
      ids: spec.records.map((record) => record.id),
      metadata: spec.records.map((record) => record.metadata)
    }
  );

  for (const query of spec.queries) {
    const report = await index.searchWithReport(query.vector, {
      k: query.k,
      filter: query.filter,
      includeMetadata: true
    });
    assert.deepEqual(
      report.hits.map((hit) => hit.id),
      query.expectedIds,
      `ids for ${query.name}`
    );
    for (const hit of report.hits) {
      assert.deepEqual(
        hit.metadata,
        byId.get(hit.id)?.metadata,
        `metadata for ${hit.id} in ${query.name}`
      );
    }

    const idsOnly = await index.searchIds(query.vector, {
      k: query.k,
      filter: query.filter
    });
    assert.deepEqual(idsOnly, query.expectedIds, `searchIds for ${query.name}`);
  }

  for (const record of spec.records) {
    const stored = await index.getRecord(record.id);
    assert.ok(stored, `getRecord(${record.id})`);
    assert.deepEqual(stored?.vector, record.vector);
    assert.deepEqual(stored?.metadata, record.metadata);
  }
});

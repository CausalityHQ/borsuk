import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";

import { create, open } from "../src/index.js";

test("preload option and warm report", async () => {
  const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-warm-"));
  const uri = pathToFileURL(dir).href;
  await create({
    uri,
    metric: "euclidean",
    dimensions: 2,
    segmentMaxVectors: 1,
  });

  const index = open(uri, { preload: true });
  await index.add(
    [
      [0, 0],
      [1, 0],
      [2, 0],
    ],
    { ids: ["a", "b", "c"] },
  );
  assert.ok((await index.stats()).segments >= 2);

  const report = await index.warm();

  assert.ok(report.segmentsLoaded >= 1);
  assert.ok(report.bytesResident > 0);
});

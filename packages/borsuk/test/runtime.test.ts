import assert from "node:assert/strict";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";

import { create, Index } from "../src/index.js";

function configuredCpuThreads(): number {
  const parsed = Number(process.env.BORSUK_CPU_THREADS);
  return Number.isInteger(parsed) && parsed >= 1 && parsed <= 64 ? parsed : 4;
}

function threadCount(): number {
  const match = /^Threads:\s+(\d+)$/m.exec(readFileSync("/proc/self/status", "utf8"));
  assert.ok(match, "Linux process status must report a thread count");
  return Number(match[1]);
}

test("opening many indexes keeps native worker threads bounded", async (t) => {
  if (process.platform !== "linux") {
    t.skip("Linux /proc thread accounting is required");
    return;
  }
  const before = threadCount();
  const indexes: Index[] = [];
  for (let index = 0; index < 100; index += 1) {
    const dir = mkdtempSync(join(tmpdir(), "borsuk-ts-runtime-"));
    indexes.push(
      await create({
        uri: pathToFileURL(dir).href,
        metric: "euclidean",
        dimensions: 2,
      }),
    );
  }

  assert.equal(indexes.length, 100);
  assert.ok(
    threadCount() - before <= configuredCpuThreads() + 12,
    "storage handles must share a bounded process worker pool",
  );
});

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

function configuredBackingGetConcurrency(): number {
  const parsed = Number(process.env.BORSUK_BACKING_GET_CONCURRENCY);
  return Number.isInteger(parsed) && parsed >= 1 && parsed <= 1024 ? parsed : 64;
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
  let afterWarmPool = before;
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
    if (indexes.length === 10) {
      afterWarmPool = threadCount();
      assert.ok(
        afterWarmPool - before <=
          Math.max(configuredCpuThreads(), configuredBackingGetConcurrency()) + 12,
        "the shared process runtime must respect its configured worker bounds",
      );
    }
  }

  assert.equal(indexes.length, 100);
  const afterAllHandles = threadCount();
  assert.ok(
    afterAllHandles - before <=
      Math.max(configuredCpuThreads(), configuredBackingGetConcurrency()) + 14,
    "opening more handles must not allocate a worker pool per handle",
  );
});

import { execFileSync } from "node:child_process";
import { join } from "node:path";
import test from "node:test";

test("local TypeScript example runs", () => {
  execFileSync(process.execPath, [join(import.meta.dirname, "..", "examples", "local-index.js")], {
    encoding: "utf8",
  });
});

test("docs ladder TypeScript example runs", () => {
  execFileSync(process.execPath, [join(import.meta.dirname, "..", "examples", "docs-ladder.js")], {
    encoding: "utf8",
  });
});

test("cookbook TypeScript example runs", () => {
  execFileSync(process.execPath, [join(import.meta.dirname, "..", "examples", "cookbook.js")], {
    encoding: "utf8",
  });
});

test("S3-compatible TypeScript example runs when configured", (t) => {
  if (!process.env.BORSUK_S3_TEST_URI) {
    t.skip("BORSUK_S3_TEST_URI is not set");
    return;
  }

  execFileSync(process.execPath, [join(import.meta.dirname, "..", "examples", "s3-index.js")], {
    encoding: "utf8",
  });
});

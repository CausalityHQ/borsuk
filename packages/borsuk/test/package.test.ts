import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

interface PackageJson {
  files?: string[];
}

interface PackFile {
  path: string;
}

interface PackResult {
  files: PackFile[];
}

test("published package includes native runtime artifacts", () => {
  const packageJson = JSON.parse(
    readFileSync(join(import.meta.dirname, "..", "..", "package.json"), "utf8")
  ) as PackageJson;

  assert.deepEqual(packageJson.files, [
    "dist/src",
    "dist/examples",
    "index.cjs",
    "*.node",
    "native.d.ts",
    "README.md",
    "LICENSE-MIT",
    "LICENSE-APACHE"
  ]);
});

test("published package excludes compiled tests", () => {
  const output = execFileSync("npm", ["pack", "--dry-run", "--json"], {
    cwd: join(import.meta.dirname, "..", ".."),
    encoding: "utf8"
  });
  const [pack] = JSON.parse(output) as PackResult[];
  const paths = pack.files.map((file) => file.path);

  assert(paths.includes("dist/src/index.js"));
  assert(paths.includes("dist/examples/local-index.js"));
  assert(paths.includes("LICENSE-MIT"));
  assert(paths.includes("LICENSE-APACHE"));
  assert(!paths.some((path) => path.includes("/test/") || path.includes("api.test")));
});

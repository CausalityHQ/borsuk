import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

interface PackageJson {
  license?: string;
  engines?: {
    node?: string;
  };
  files?: string[];
}

interface PackageLock {
  packages?: Record<
    string,
    {
      license?: string;
      engines?: {
        node?: string;
      };
    }
  >;
}

interface PackFile {
  path: string;
}

interface PackResult {
  files: PackFile[];
  filename?: string;
}

function packageRoot(): string {
  return join(import.meta.dirname, "..", "..");
}

function packedPaths(): string[] {
  const output = execFileSync("npm", ["pack", "--dry-run", "--json"], {
    cwd: packageRoot(),
    encoding: "utf8"
  });
  const [pack] = JSON.parse(output) as PackResult[];
  return pack.files.map((file) => file.path);
}

test("published package includes native runtime artifacts", () => {
  const packageJson = JSON.parse(
    readFileSync(join(packageRoot(), "package.json"), "utf8")
  ) as PackageJson;

  assert.deepEqual(packageJson.files, [
    "dist/src",
    "dist/examples",
    "index.cjs",
    "*.node",
    "README.md",
    "LICENSE"
  ]);

  const paths = packedPaths();
  assert(paths.includes("index.cjs"));
  assert(paths.includes("dist/src/index.d.ts"));
  assert(paths.includes("README.md"));
  assert(
    paths.some((path) => /^index\.[a-z0-9_-]+-[a-z0-9_-]+\.node$/.test(path)),
    `package must include at least one platform native addon: ${paths.join(", ")}`
  );
});

test("published package declares supported Node runtime range", () => {
  const packageJson = JSON.parse(
    readFileSync(join(packageRoot(), "package.json"), "utf8")
  ) as PackageJson;
  const packageLock = JSON.parse(
    readFileSync(join(packageRoot(), "package-lock.json"), "utf8")
  ) as PackageLock;

  assert.equal(packageJson.engines?.node, ">=22 <27");
  assert.equal(packageLock.packages?.[""]?.engines?.node, ">=22 <27");
});

test("published package metadata declares public project urls", () => {
  const packageJson = JSON.parse(
    readFileSync(join(packageRoot(), "package.json"), "utf8")
  ) as PackageJson & {
    homepage?: string;
    repository?: {
      url?: string;
    };
    bugs?: {
      url?: string;
    };
  };

  assert.equal(packageJson.homepage, "http://causality.pl/borsuk/");
  assert.equal(packageJson.repository?.url, "git+https://github.com/CausalityHQ/borsuk.git");
  assert.equal(packageJson.bugs?.url, "https://github.com/CausalityHQ/borsuk/issues");
});

test("published package excludes compiled tests", () => {
  const paths = packedPaths();

  assert(paths.includes("dist/src/index.js"));
  assert(paths.includes("dist/src/index.d.ts"));
  assert(paths.includes("dist/examples/local-index.js"));
  assert(paths.includes("dist/examples/s3-index.js"));
  assert(paths.includes("LICENSE"));
  assert(!paths.some((path) => path.includes("/test/") || path.includes("api.test")));
});

test("published package excludes raw native bridge declarations", () => {
  const paths = packedPaths();

  assert(!paths.includes("native.d.ts"));
  assert(paths.includes("dist/src/index.d.ts"));
});

test("published declarations hide native bridge constructor details", () => {
  const declarations = readFileSync(join(packageRoot(), "dist", "src", "index.d.ts"), "utf8");

  assert.match(declarations, /constructor\(uri: string\);/);
  assert.doesNotMatch(declarations, /constructor\(uri: string, inner\?: NativeIndex\);/);
});

test("packed package installs and imports from a clean project", () => {
  const output = execFileSync("npm", ["pack", "--json"], {
    cwd: packageRoot(),
    encoding: "utf8"
  });
  const [pack] = JSON.parse(output) as PackResult[];
  assert(pack.filename, `npm pack did not report a tarball filename: ${output}`);

  const tarball = join(packageRoot(), pack.filename);
  const consumer = mkdtempSync(join(tmpdir(), "borsuk-npm-consumer-"));
  try {
    writeFileSync(join(consumer, "package.json"), "{\"type\":\"module\"}\n");
    execFileSync("npm", ["install", "--ignore-scripts", tarball], {
      cwd: consumer,
      stdio: "pipe"
    });
    writeFileSync(
      join(consumer, "smoke.mjs"),
      [
        "import { vectorDistance, VectorMetricName, vectorMetricNames } from 'borsuk';",
        "if (!vectorMetricNames().includes(VectorMetricName.Cosine)) {",
        "  throw new Error('missing cosine metric in packed package');",
        "}",
        "if (vectorDistance(VectorMetricName.Cosine, [1, 0], [1, 0]) !== 0) {",
        "  throw new Error('packed package native binding returned wrong distance');",
        "}",
        ""
      ].join("\n")
    );
    execFileSync("node", ["smoke.mjs"], {
      cwd: consumer,
      stdio: "pipe"
    });
  } finally {
    rmSync(tarball, { force: true });
    rmSync(consumer, { force: true, recursive: true });
  }
});

test("published package license contains BUSL revenue grant", () => {
  const packageJson = JSON.parse(
    readFileSync(join(packageRoot(), "package.json"), "utf8")
  ) as PackageJson;
  const licenseText = readFileSync(join(packageRoot(), "LICENSE"), "utf8");
  const packageLock = JSON.parse(
    readFileSync(join(packageRoot(), "package-lock.json"), "utf8")
  ) as PackageLock;

  assert.equal(packageJson.license, "BUSL-1.1");
  assert.equal(packageLock.packages?.[""]?.license, "BUSL-1.1");
  assert.match(licenseText, /Business Source License 1\.1/);
  assert.match(licenseText, /US \$100,000/);
  assert.match(licenseText, /Change Date: 2030-07-02/);
  assert.match(licenseText, /Change License: MIT License/);
});

#!/usr/bin/env node
// Fill the docs-site Quickstart ladder from the real, CI-run example files, so
// the code on the page can never drift from code that actually compiles and runs.
//
// Each `<code data-ladder="RUNG:LANG">` slot in docs/web/docs.html is filled from
// the `docs:RUNG:start` / `docs:RUNG:end` marker region in the matching example
// file. Run with no arguments to write the page; run with `--check` to fail
// (exit 1) when the page and the sources disagree — that is the CI drift guard.

import { readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const docsPath = join(root, "docs", "web", "docs.html");

const RUNGS = ["hello", "report", "s3", "tuning", "production"];
const LANGS = ["rust", "python", "typescript"];

// Local rungs come from the dedicated ladder example; the S3 rung comes from the
// existing S3 example that CI runs against real object storage.
const SOURCES = {
  rust: {
    local: "crates/borsuk/examples/docs_ladder.rs",
    s3: "crates/borsuk/examples/s3_index.rs",
  },
  python: {
    local: "python/examples/docs_ladder.py",
    s3: "python/examples/s3_index.py",
  },
  typescript: {
    local: "packages/borsuk/examples/docs-ladder.ts",
    s3: "packages/borsuk/examples/s3-index.ts",
  },
};

function sourceFile(rung, lang) {
  return SOURCES[lang][rung === "s3" ? "s3" : "local"];
}

function extractRegion(text, rung, file) {
  const lines = text.split("\n");
  const start = lines.findIndex((line) => line.includes(`docs:${rung}:start`));
  const end = lines.findIndex((line) => line.includes(`docs:${rung}:end`));
  if (start === -1 || end === -1 || end <= start) {
    throw new Error(`missing docs:${rung} markers in ${file}`);
  }
  const region = lines.slice(start + 1, end);
  const indents = region
    .filter((line) => line.trim().length > 0)
    .map((line) => line.match(/^\s*/)[0].length);
  const dedent = indents.length ? Math.min(...indents) : 0;
  return region.map((line) => line.slice(dedent)).join("\n").trim();
}

function escapeHtml(text) {
  return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

async function buildSnippets() {
  const snippets = {};
  for (const rung of RUNGS) {
    for (const lang of LANGS) {
      const file = sourceFile(rung, lang);
      const text = await readFile(join(root, file), "utf8");
      snippets[`${rung}:${lang}`] = escapeHtml(extractRegion(text, rung, file));
    }
  }
  return snippets;
}

function fillSlots(html, snippets) {
  const slots = new Set();
  const filled = html.replace(
    /(<code data-ladder="([a-z0-9]+:[a-z]+)">)([\s\S]*?)(<\/code>)/g,
    (match, open, key, _body, close) => {
      const snippet = snippets[key];
      if (snippet === undefined) throw new Error(`docs.html has an unknown ladder slot: ${key}`);
      slots.add(key);
      return `${open}${snippet}${close}`;
    },
  );
  const missing = Object.keys(snippets).filter((key) => !slots.has(key));
  if (missing.length) {
    throw new Error(`docs.html is missing ladder slots: ${missing.join(", ")}`);
  }
  return filled;
}

async function main() {
  const check = process.argv.includes("--check");
  const snippets = await buildSnippets();
  const current = await readFile(docsPath, "utf8");
  const next = fillSlots(current, snippets);
  if (check) {
    if (next !== current) {
      console.error(
        "docs.html ladder snippets are stale. Run `node scripts/sync_docs_examples.mjs` to refresh them from the example sources.",
      );
      process.exit(1);
    }
    console.log("docs.html ladder snippets match the example sources.");
    return;
  }
  await writeFile(docsPath, next);
  console.log(`Synced ${Object.keys(snippets).length} ladder snippets into docs.html.`);
}

await main();

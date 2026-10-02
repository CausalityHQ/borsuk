#!/usr/bin/env node
import assert from "node:assert/strict";
import { readFile, stat } from "node:fs/promises";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const webRoot = join(root, "docs", "web");
const expectedCsvPaths = [
  "assets/benchmarks/sequential.csv",
  "assets/benchmarks/parallel.csv",
  "assets/benchmarks/lifecycle.csv",
  "assets/benchmarks/scale.csv",
  "assets/benchmarks/large-scale.csv",
  "assets/benchmarks/hundred-million-read.csv",
  "assets/benchmarks/routing-overfetch.csv",
  "assets/benchmarks/filtering.csv",
  "assets/benchmarks/sparsity.csv",
  "assets/benchmarks/workload.csv",
  "assets/benchmarks/dataset-scaling.csv",
  "assets/benchmarks/mixture-workload.csv",
  "assets/benchmarks/sparse_inverted.csv",
  "assets/benchmarks/production_recall.csv",
];

class FakeClassList {
  #classes = new Set();

  toggle(name, force) {
    if (force) {
      this.#classes.add(name);
      return true;
    }
    this.#classes.delete(name);
    return false;
  }

  contains(name) {
    return this.#classes.has(name);
  }
}

class FakeElement {
  constructor(dataset = {}, tagName = "div") {
    this.dataset = dataset;
    this.tagName = tagName;
    this.children = [];
    this.attributes = new Map();
    this.listeners = new Map();
    this.classList = new FakeClassList();
    this.hidden = false;
    this.value = "";
    this.textContent = "";
    this._innerHTML = "";
  }

  append(...children) {
    this.children.push(...children);
    return this;
  }

  setAttribute(name, value) {
    this.attributes.set(name, String(value));
  }

  addEventListener(type, listener) {
    const listeners = this.listeners.get(type) || [];
    listeners.push(listener);
    this.listeners.set(type, listeners);
  }

  querySelector(selector) {
    return this.querySelectorAll(selector)[0] || null;
  }

  querySelectorAll(selector) {
    const matches = [];
    const visit = (node) => {
      if (matchesSelector(node, selector)) matches.push(node);
      node.children.forEach(visit);
    };
    this.children.forEach(visit);
    return matches;
  }

  set innerHTML(value) {
    this._innerHTML = String(value);
    if (this.tagName === "select") {
      const options = [...this._innerHTML.matchAll(/<option value="([^"]+)"([^>]*)>/g)];
      const selected = options.find((option) => option[2].includes("selected")) || options[0];
      this.value = selected?.[1] || "";
    }
  }

  get innerHTML() {
    return this._innerHTML;
  }
}

class FakeDocument extends FakeElement {
  constructor() {
    super({}, "document");
  }

  createElement(tagName) {
    return new FakeElement({}, tagName);
  }
}

function matchesSelector(node, selector) {
  const match = selector.match(/^\[data-([a-z0-9-]+)\]$/);
  if (!match) return false;
  return Object.prototype.hasOwnProperty.call(node.dataset, datasetKey(match[1]));
}

function datasetKey(name) {
  return name.replace(/-([a-z0-9])/g, (_, char) => char.toUpperCase());
}

function el(dataset = {}, tagName = "div") {
  return new FakeElement(dataset, tagName);
}

function buildDocument() {
  const document = new FakeDocument();

  const codeTabs = el({ codeTabs: "" }).append(
    el({ codeTab: "rust" }, "button"),
    el({ codeTab: "python" }, "button"),
    el({ codeTab: "typescript" }, "button"),
    el({ codePanel: "rust" }, "pre"),
    el({ codePanel: "python" }, "pre"),
    el({ codePanel: "typescript" }, "pre"),
  );

  const archTitle = el({ archTitle: "" }, "h3");
  const archBody = el({ archBody: "" }, "p");
  const archPanel = el({ archPanel: "" }).append(archTitle, archBody);
  const archStages = ["ingest", "route", "leaf", "graph", "publish"].map((stage) =>
    el({ stage }, "button"),
  );
  const hierarchyRoot = el({ hierarchyRoot: "" }).append(
    el({ hierarchyVectors: "" }, "select"),
    el({ hierarchySegmentSize: "" }, "select"),
    el({ hierarchyFanout: "" }, "select"),
    el({ hierarchyLevels: "" }),
    el({ hierarchyNodes: "" }),
    el({ hierarchySummary: "" }),
  );

  const charts = {
    performance: chartRoot("performanceRoot", ["selectDataset", "selectMetric"]),
    scale: chartRoot("scaleRoot", ["selectFamily", "selectMode", "selectMetric"]),
    largeScale: chartRoot("largeScaleRoot", ["selectMetric"]),
    hundredMillionRead: chartRoot("hundredMillionReadRoot", ["selectMetric"]),
    parallel: chartRoot("parallelRoot", ["selectDataset", "selectMode", "selectMetric"]),
    lifecycle: chartRoot("lifecycleRoot", ["selectMetric"]),
    overfetch: chartRoot("overfetchRoot", ["selectDataset", "selectMode", "selectMetric"]),
  };

  document.append(
    codeTabs,
    archPanel,
    ...archStages,
    hierarchyRoot,
    ...Object.values(charts).map((chart) => chart.root),
  );
  return { document, archTitle, archBody, charts, codeTabs };
}

function chartRoot(rootKey, selectKeys) {
  const chart = el({ chart: "" });
  const table = el({ table: "" });
  const selects = Object.fromEntries(selectKeys.map((key) => [key, el({ [key]: "" }, "select")]));
  const root = el({ [rootKey]: "" }).append(...Object.values(selects), chart, table);
  return { root, chart, table, selects };
}

async function main() {
  const researchHtml = await readFile(join(webRoot, "research.html"), "utf8");
  const staleGloveCard =
    /GloVe(?=[^<]*(?:<[^>]+>[^<]*)*0\.98\d*)(?=[\s\S]{0,400}256\s*(?:cand|candidate))(?=[\s\S]{0,400}28(?:\.0+)?\s*(?:MB|MiB))(?=[\s\S]{0,400}2(?:\.0+)?\s*(?:s|sec|seconds))/i;
  assert.doesNotMatch(
    researchHtml,
    staleGloveCard,
    "research page must not render the stale GloVe 0.98 / 256-candidate / 28 MB / 2 s card",
  );

  const { document, archTitle, archBody, charts, codeTabs } = buildDocument();
  const fetched = new Set();
  const errors = [];
  const context = vm.createContext({
    document,
    setTimeout,
    clearTimeout,
    console: {
      ...console,
      error: (...args) => errors.push(args.join(" ")),
    },
    fetch: async (path) => {
      fetched.add(path);
      try {
        const text = await readFile(join(webRoot, path), "utf8");
        return { ok: true, status: 200, text: async () => text };
      } catch {
        return { ok: false, status: 404, text: async () => "" };
      }
    },
  });

  const appPath = join(webRoot, "app.js");
  vm.runInContext(readFileSync(appPath, "utf8"), context, { filename: appPath });
  for (const listener of document.listeners.get("DOMContentLoaded") || []) {
    listener();
  }
  for (let i = 0; i < 10; i += 1) {
    await new Promise((resolve) => setTimeout(resolve, 0));
  }

  assert.deepEqual([...fetched].sort(), expectedCsvPaths.sort());
  assert.deepEqual(errors, []);
  assert.equal(archTitle.textContent, "Routing Layers");
  assert.match(archBody.textContent, /top routing layer/);
  assert.equal(codeTabs.querySelector("[data-code-tab]").classList.contains("is-active"), true);
  assertHierarchy(document);

  assertRenderedChart(charts.performance, "mode evaluation");
  assertRenderedChart(charts.scale, "scale");
  assertRenderedChart(charts.largeScale, "large-scale");
  assertRenderedChart(charts.hundredMillionRead, "100M read probe");
  assertRenderedChart(charts.parallel, "parallel pressure");
  assertRenderedChart(charts.lifecycle, "lifecycle");
  assertRenderedChart(charts.overfetch, "routing overfetch");
  assertTableIncludes(charts.performance, "mode evaluation", /Termination/);
  assertTableIncludes(charts.performance, "mode evaluation", /exact-pruned=100|max-segments=100/);
  assertTableIncludes(charts.performance, "mode evaluation", /Routing overfetch/);
  assertTableIncludes(charts.performance, "mode evaluation", /Cache hits/);
  assertTableIncludes(charts.performance, "mode evaluation", /Cache misses/);
  assertTableIncludes(charts.performance, "mode evaluation", /Routing indexes/);
  assertTableIncludes(charts.performance, "mode evaluation", /Routing pages/);
  assertSelectIncludes(
    charts.performance.selects.selectMetric,
    "mode evaluation metric",
    /cache misses\/query/,
  );
  assertSelectIncludes(
    charts.performance.selects.selectMetric,
    "mode evaluation metric",
    /routing pages\/query/,
  );
  assertTableIncludes(charts.scale, "scale", /Termination/);
  assertTableIncludes(charts.scale, "scale", /max-segments=100/);
  assertTableIncludes(charts.scale, "scale", /Routing overfetch/);
  assertTableIncludes(charts.scale, "scale", /Cache hits/);
  assertTableIncludes(charts.scale, "scale", /Cache misses/);
  assertTableIncludes(charts.scale, "scale", /Routing indexes/);
  assertTableIncludes(charts.scale, "scale", /Routing pages/);
  assertSelectIncludes(charts.scale.selects.selectMetric, "scale metric", /cache misses\/query/);
  assertSelectIncludes(charts.scale.selects.selectMetric, "scale metric", /routing pages\/query/);
  assertTableIncludes(charts.largeScale, "large-scale", /Termination/);
  assertTableIncludes(charts.largeScale, "large-scale", /Id recall@10/);
  assertTableIncludes(charts.largeScale, "large-scale", /max-segments/);
  assertTableIncludes(charts.largeScale, "large-scale", /Routing overfetch/);
  assertTableIncludes(charts.largeScale, "large-scale", /Routing indexes/);
  assertTableIncludes(charts.largeScale, "large-scale", /Routing pages/);
  assertTableIncludes(charts.largeScale, "large-scale", /RSS delta/);
  assertTableIncludes(charts.largeScale, "large-scale", /Graph candidates/);
  assertTableIncludes(charts.largeScale, "large-scale", /Ingest ms/);
  assertTableIncludes(charts.largeScale, "large-scale", /Exact ms/);
  assertTableIncludes(charts.largeScale, "large-scale", /Compaction bytes read/);
  assertTableIncludes(charts.largeScale, "large-scale", /Compaction bytes written/);
  assertTableIncludes(charts.largeScale, "large-scale", /Considered rows/);
  assertTableIncludes(charts.largeScale, "large-scale", /GC ms/);
  assertTableIncludes(charts.largeScale, "large-scale", /GC objects scanned/);
  assertTableIncludes(charts.largeScale, "large-scale", /GC objects deleted/);
  assertTableIncludes(charts.largeScale, "large-scale", /GC bytes reclaimed/);
  assertSelectIncludes(
    charts.largeScale.selects.selectMetric,
    "large-scale metric",
    /GC bytes reclaimed/,
  );
  assertSelectIncludes(
    charts.largeScale.selects.selectMetric,
    "large-scale metric",
    /id recall@10/,
  );
  assertSelectIncludes(
    charts.largeScale.selects.selectMetric,
    "large-scale metric",
    /RSS peak delta/,
  );
  assertSelectIncludes(
    charts.largeScale.selects.selectMetric,
    "large-scale metric",
    /graph candidates/,
  );
  assertSelectIncludes(
    charts.largeScale.selects.selectMetric,
    "large-scale metric",
    /exact reference time/,
  );
  assertSelectIncludes(
    charts.largeScale.selects.selectMetric,
    "large-scale metric",
    /compaction bytes written/,
  );
  assertTableIncludes(charts.hundredMillionRead, "100M read probe", /max-segments/);
  assertTableIncludes(charts.hundredMillionRead, "100M read probe", /Candidate rows\/segment/);
  assertTableIncludes(charts.hundredMillionRead, "100M read probe", /Found seed id/);
  assertTableIncludes(charts.hundredMillionRead, "100M read probe", /Graph bytes/);
  assertTableIncludes(charts.hundredMillionRead, "100M read probe", /Exact-scored rows/);
  assertTableIncludes(
    charts.hundredMillionRead,
    "100M read probe",
    /after-first-2m-l0-to-l1-batch/,
  );
  assertSelectIncludes(
    charts.hundredMillionRead.selects.selectMetric,
    "100M read probe metric",
    /query latency/,
  );
  assertSelectIncludes(
    charts.hundredMillionRead.selects.selectMetric,
    "100M read probe metric",
    /graph bytes\/query/,
  );
  assertSelectIncludes(
    charts.hundredMillionRead.selects.selectMetric,
    "100M read probe metric",
    /cache misses\/query/,
  );
  assertTableIncludes(charts.parallel, "parallel pressure", /Termination/);
  assertTableIncludes(charts.parallel, "parallel pressure", /Tie recall@10/);
  assertTableIncludes(charts.parallel, "parallel pressure", /Id recall@10/);
  assertTableIncludes(charts.parallel, "parallel pressure", /Bytes/);
  assertTableIncludes(charts.parallel, "parallel pressure", /exact-pruned=100|max-segments=100/);
  assertTableIncludes(charts.parallel, "parallel pressure", /Routing overfetch/);
  assertTableIncludes(charts.parallel, "parallel pressure", /Resident bytes/);
  assertTableIncludes(charts.parallel, "parallel pressure", /Cache hits/);
  assertTableIncludes(charts.parallel, "parallel pressure", /Cache misses/);
  assertTableIncludes(charts.parallel, "parallel pressure", /Routing indexes/);
  assertTableIncludes(charts.parallel, "parallel pressure", /Routing pages/);
  assertSelectIncludes(
    charts.parallel.selects.selectMetric,
    "parallel pressure metric",
    /tie-aware recall@10/,
  );
  assertSelectIncludes(
    charts.parallel.selects.selectMetric,
    "parallel pressure metric",
    /id recall@10/,
  );
  assertSelectIncludes(
    charts.parallel.selects.selectMetric,
    "parallel pressure metric",
    /bytes read\/query/,
  );
  assertSelectIncludes(
    charts.parallel.selects.selectMetric,
    "parallel pressure metric",
    /resident metadata/,
  );
  assertSelectIncludes(
    charts.parallel.selects.selectMetric,
    "parallel pressure metric",
    /cache misses\/query/,
  );
  assertSelectIncludes(
    charts.parallel.selects.selectMetric,
    "parallel pressure metric",
    /routing pages\/query/,
  );
  selectValue(charts.parallel.selects.selectDataset, "synthetic-uniform-n100000");
  selectValue(charts.parallel.selects.selectMode, "graph");
  assertTableIncludes(charts.parallel, "parallel pressure 100k graph rows", /100,000/);
  assertTableIncludes(charts.parallel, "parallel pressure 100k graph rows", /RSS delta/);
  assertTableIncludes(charts.parallel, "parallel pressure 100k graph rows", /Graph bytes/);
  assert.doesNotMatch(
    charts.parallel.table.innerHTML,
    /100 KB/,
    "parallel pressure records must render as counts, not byte units",
  );
  assertTableIncludes(charts.lifecycle, "lifecycle", /Compaction bytes read/);
  assertTableIncludes(charts.lifecycle, "lifecycle", /Compaction bytes written/);
  assertTableIncludes(charts.overfetch, "routing overfetch", /Routing overfetch/);
  assertTableIncludes(charts.overfetch, "routing overfetch", /Tie recall@10/);
  assertTableIncludes(charts.overfetch, "routing overfetch", /Routing pages/);
  assertSelectIncludes(
    charts.overfetch.selects.selectMetric,
    "routing overfetch metric",
    /routing pages\/query/,
  );

  // Current public pages use static copy; archived chart behavior is tested above.
  const publicPages = new Map();
  for (const name of ["index.html", "docs.html", "research.html"]) {
    const html = await readFile(join(webRoot, name), "utf8");
    publicPages.set(name, html);
    assert.match(html, /Experimental · Unreleased/, `${name} must show experimental status`);
    assert.match(html, /<title>[^<]*Experimental/, `${name} title must mark the library experimental`);
    assert.match(html, /class="skip-link"/, `${name} must offer a skip link`);
    assert.match(html, /two-bit SOURCE/, `${name} must describe the current nomination path`);
    assert.match(html, /SQ8/, `${name} must describe quantized range scoring`);
    assert.doesNotMatch(html, /src="app\.js/, `${name} must not promote archived charts`);
    const visible = html.replace(/<[^>]+>/g, " ");
    assert.doesNotMatch(visible, /\b(?:V\d+|v\d+|TurboQuant|pq-scan|100M|GCS|Azure|production-ready|lossless reranking)\b/i,
      `${name} must not render stale routes or unsupported claims`);
    for (const key of ["description", "og:title", "og:description", "twitter:title", "twitter:description"]) {
      assert.match(html, new RegExp(`<meta (?:name|property)="${key}" content="[^"]*[Ee]xperimental`),
        `${name} must mark ${key} experimental`);
    }
  }
  const docsHtml = publicPages.get("docs.html");
  assert.match(docsHtml, /Native Rust API/);
  assert.match(docsHtml, /mutation snapshots and recovery/);
  assert.match(docsHtml, /shared local filesystem/);
  assert.match(docsHtml, /separate create\/add\/search APIs/);
  assert.match(docsHtml, /href="research\.html#latest"/);
  const indexHtml = publicPages.get("index.html");
  assert.match(indexHtml, /href="docs\.html"/);
  assert.match(indexHtml, /href="research\.html/);
  assert.match(researchHtml, /64 sealed queries/);
  assert.match(researchHtml, /96\.71875%/);
  assert.match(researchHtml, /541\.10 ms/);
  assert.match(researchHtml, /7\.597044/);
  assert.match(researchHtml, /Sustained capacity/);
  assert.match(researchHtml, /unmeasured/);

  // Verify every local page, asset and fragment, plus repository-backed links.
  for (const [name, html] of publicPages) {
    for (const match of html.matchAll(/(?:href|src)="([^"]+)"/g)) {
      const link = match[1];
      const url = new URL(link, `https://docs.invalid/${name}`);
      if (url.hostname === "github.com" && url.pathname.startsWith("/CausalityHQ/borsuk/")) {
        const [, , , kind, revision, ...parts] = url.pathname.split("/");
        if (kind === "blob" || kind === "tree") {
          assert.ok(revision === "main" || /^[a-f0-9]{40}$/.test(revision), `invalid source revision: ${link}`);
          await stat(join(root, decodeURIComponent(parts.join("/"))));
        }
      } else if (url.hostname === "docs.invalid") {
        const target = decodeURIComponent(url.pathname.slice(1));
        const targetText = await readFile(join(webRoot, target), "utf8");
        if (url.hash) {
          assert.ok(targetText.includes(`id="${decodeURIComponent(url.hash.slice(1))}"`), `${name} has a broken fragment: ${link}`);
        }
      }
    }
  }
}

function assertRenderedChart(chart, label) {
  assert.match(chart.chart.innerHTML, /<svg\b/, `${label} chart did not render an SVG`);
  assert.match(chart.table.innerHTML, /<table>/, `${label} table did not render`);
  assert.doesNotMatch(
    chart.root.textContent,
    /Benchmark data could not be loaded/,
    `${label} fell back to the benchmark load error`,
  );
}

function assertTableIncludes(chart, label, pattern) {
  assert.match(chart.table.innerHTML, pattern, `${label} table did not expose ${pattern}`);
}

function assertSelectIncludes(select, label, pattern) {
  assert.match(select.innerHTML, pattern, `${label} selector did not expose ${pattern}`);
}

function selectValue(select, value) {
  select.value = value;
  for (const listener of select.listeners.get("change") || []) listener();
}

function assertHierarchy(document) {
  const root = document.querySelector("[data-hierarchy-root]");
  assert.ok(root, "hierarchy calculator root is missing");
  assert.match(
    root.querySelector("[data-hierarchy-summary]").textContent,
    /100,000 vectors.+98 leaf blobs.+1 L0 routing page.+2 routing objects/s,
    "hierarchy summary should explain the default 100k/fanout-128 shape",
  );
  assert.match(
    root.querySelector("[data-hierarchy-levels]").innerHTML,
    /L0.+top.+1 page/s,
    "hierarchy levels should expose the computed top level",
  );
  assert.match(
    root.querySelector("[data-hierarchy-nodes]").innerHTML,
    /Vector leaf blobs.+98/s,
    "hierarchy nodes should include bounded vector leaf count",
  );
  const vectors = root.querySelector("[data-hierarchy-vectors]");
  vectors.value = "100000000";
  for (const listener of vectors.listeners.get("change") || []) listener();
  assert.match(
    root.querySelector("[data-hierarchy-summary]").textContent,
    /100,000,000 vectors.+97,657 leaf blobs.+763 L0 routing pages.+773 routing objects/s,
    "hierarchy summary should grow into multiple routing layers at hundred-million-vector scale",
  );
  assert.match(
    root.querySelector("[data-hierarchy-levels]").innerHTML,
    /L2.+top.+1 page.+L1.+6 pages.+L0.+763 pages/s,
    "hierarchy levels should show computed L2 -> L1 -> L0 routing for hundred-million-vector scale",
  );
}

await main();

#!/usr/bin/env node
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
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
  "assets/benchmarks/routing-overfetch.csv",
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

  const codeTabs = el({ codeTabs: "" })
    .append(
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
  assertSelectIncludes(charts.performance.selects.selectMetric, "mode evaluation metric", /cache misses\/query/);
  assertSelectIncludes(charts.performance.selects.selectMetric, "mode evaluation metric", /routing pages\/query/);
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
  assertSelectIncludes(charts.largeScale.selects.selectMetric, "large-scale metric", /id recall@10/);
  assertSelectIncludes(charts.largeScale.selects.selectMetric, "large-scale metric", /RSS peak delta/);
  assertSelectIncludes(charts.largeScale.selects.selectMetric, "large-scale metric", /graph candidates/);
  assertSelectIncludes(charts.largeScale.selects.selectMetric, "large-scale metric", /exact reference time/);
  assertSelectIncludes(charts.largeScale.selects.selectMetric, "large-scale metric", /compaction bytes written/);
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
  assertSelectIncludes(charts.parallel.selects.selectMetric, "parallel pressure metric", /tie-aware recall@10/);
  assertSelectIncludes(charts.parallel.selects.selectMetric, "parallel pressure metric", /id recall@10/);
  assertSelectIncludes(charts.parallel.selects.selectMetric, "parallel pressure metric", /bytes read\/query/);
  assertSelectIncludes(charts.parallel.selects.selectMetric, "parallel pressure metric", /resident metadata/);
  assertSelectIncludes(charts.parallel.selects.selectMetric, "parallel pressure metric", /cache misses\/query/);
  assertSelectIncludes(charts.parallel.selects.selectMetric, "parallel pressure metric", /routing pages\/query/);
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
  assertSelectIncludes(charts.overfetch.selects.selectMetric, "routing overfetch metric", /routing pages\/query/);
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
  vectors.value = "1000000000";
  for (const listener of vectors.listeners.get("change") || []) listener();
  assert.match(
    root.querySelector("[data-hierarchy-summary]").textContent,
    /1,000,000,000 vectors.+976,563 leaf blobs.+7,630 L0 routing pages.+7,694 routing objects/s,
    "hierarchy summary should grow into multiple routing layers at billion-vector scale",
  );
  assert.match(
    root.querySelector("[data-hierarchy-levels]").innerHTML,
    /L2.+top.+1 page.+L1.+60 pages.+L0.+7,630 pages/s,
    "hierarchy levels should show computed L2 -> L1 -> L0 routing for billion-vector scale",
  );
}

await main();

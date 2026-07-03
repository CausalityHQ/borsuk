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

  const charts = {
    performance: chartRoot("performanceRoot", ["selectDataset", "selectMetric"]),
    scale: chartRoot("scaleRoot", ["selectFamily", "selectMode", "selectMetric"]),
    largeScale: chartRoot("largeScaleRoot", ["selectMetric"]),
    parallel: chartRoot("parallelRoot", ["selectDataset", "selectMode", "selectMetric"]),
    lifecycle: chartRoot("lifecycleRoot", ["selectMetric"]),
  };

  document.append(codeTabs, archPanel, ...archStages, ...Object.values(charts).map((chart) => chart.root));
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

  assertRenderedChart(charts.performance, "mode evaluation");
  assertRenderedChart(charts.scale, "scale");
  assertRenderedChart(charts.largeScale, "large-scale");
  assertRenderedChart(charts.parallel, "parallel pressure");
  assertRenderedChart(charts.lifecycle, "lifecycle");
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

await main();

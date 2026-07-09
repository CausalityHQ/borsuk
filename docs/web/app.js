const MODE_LABELS = {
  exact: "Exact",
  "flat-scan": "Flat Scan",
  "sq-scan": "SQ Scan",
  "pq-scan": "PQ Scan",
  graph: "Graph",
  "vamana-pq": "VamanaPQ",
  hybrid: "Hybrid",
};

const METRICS = {
  p95_ms: { label: "p95 latency", unit: "ms", decimals: 1 },
  tie_aware_recall_at_10: { label: "tie-aware recall@10", unit: "", decimals: 2 },
  id_recall_at_10: { label: "id recall@10", unit: "", decimals: 2 },
  avg_bytes_read: { label: "bytes read/query", unit: "B", decimals: 0 },
  avg_graph_bytes_read: { label: "graph bytes/query", unit: "B", decimals: 0 },
  avg_routing_page_indexes_read: { label: "routing indexes/query", unit: "count", decimals: 1 },
  avg_routing_pages_read: { label: "routing pages/query", unit: "count", decimals: 1 },
  avg_resident_bytes: { label: "resident metadata", unit: "B", decimals: 0 },
  avg_cache_hits: { label: "cache hits/query", unit: "count", decimals: 1 },
  avg_cache_misses: { label: "cache misses/query", unit: "count", decimals: 1 },
  prefetch_depth_1_cold_p95_ms: { label: "cold p95, depth 1", unit: "ms", decimals: 1 },
  prefetch_depth_1_warm_p95_ms: { label: "warm p95, depth 1", unit: "ms", decimals: 1 },
  prefetch_depth_8_cold_p95_ms: { label: "cold p95, depth 8", unit: "ms", decimals: 1 },
  prefetch_depth_8_warm_p95_ms: { label: "warm p95, depth 8", unit: "ms", decimals: 1 },
  prefetch_depth_1_cold_avg_cache_misses: {
    label: "cold cache misses, depth 1",
    unit: "count",
    decimals: 1,
  },
  prefetch_depth_1_warm_avg_cache_hits: {
    label: "warm cache hits, depth 1",
    unit: "count",
    decimals: 1,
  },
  prefetch_depth_8_cold_avg_cache_misses: {
    label: "cold cache misses, depth 8",
    unit: "count",
    decimals: 1,
  },
  prefetch_depth_8_warm_avg_cache_hits: {
    label: "warm cache hits, depth 8",
    unit: "count",
    decimals: 1,
  },
};

const PARALLEL_METRICS = {
  p95_ms: { label: "p95 latency", unit: "ms", decimals: 1 },
  qps: { label: "queries/sec", unit: "qps", decimals: 1 },
  tie_aware_recall_at_10: { label: "tie-aware recall@10", unit: "", decimals: 2 },
  id_recall_at_10: { label: "id recall@10", unit: "", decimals: 2 },
  avg_bytes_read: { label: "bytes read/query", unit: "B", decimals: 0 },
  rss_peak_delta: { label: "RSS peak delta", unit: "B", decimals: 0 },
  avg_graph_bytes_read: { label: "graph bytes/query", unit: "B", decimals: 0 },
  avg_routing_page_indexes_read: { label: "routing indexes/query", unit: "count", decimals: 1 },
  avg_routing_pages_read: { label: "routing pages/query", unit: "count", decimals: 1 },
  avg_resident_bytes: { label: "resident metadata", unit: "B", decimals: 0 },
  avg_cache_hits: { label: "cache hits/query", unit: "count", decimals: 1 },
  avg_cache_misses: { label: "cache misses/query", unit: "count", decimals: 1 },
};

const LIFECYCLE_METRICS = {
  ingest_vectors_per_sec: { label: "ingest vectors/sec", unit: "rate", decimals: 0 },
  compaction_vectors_per_sec: { label: "compaction vectors/sec", unit: "rate", decimals: 0 },
  ingest_ms: { label: "ingest time", unit: "ms", decimals: 1 },
  compaction_ms: { label: "compaction time", unit: "ms", decimals: 1 },
  compaction_read_bytes_per_sec: { label: "compaction read/sec", unit: "Bps", decimals: 0 },
  compaction_write_bytes_per_sec: { label: "compaction write/sec", unit: "Bps", decimals: 0 },
  routing_pages_read: { label: "routing pages read", unit: "count", decimals: 0 },
  routing_pages_written: { label: "routing pages written", unit: "count", decimals: 0 },
  graph_payloads_read: { label: "old graph payloads read", unit: "count", decimals: 0 },
};

const SCALE_METRICS = {
  p95_ms: { label: "p95 latency", unit: "ms", decimals: 1 },
  tie_aware_recall_at_10: { label: "tie-aware recall@10", unit: "", decimals: 2 },
  id_recall_at_10: { label: "id recall@10", unit: "", decimals: 2 },
  avg_bytes_read: { label: "bytes read/query", unit: "B", decimals: 0 },
  avg_graph_bytes_read: { label: "graph bytes/query", unit: "B", decimals: 0 },
  avg_routing_page_indexes_read: { label: "routing indexes/query", unit: "count", decimals: 1 },
  avg_routing_pages_read: { label: "routing pages/query", unit: "count", decimals: 1 },
  avg_resident_bytes: { label: "resident metadata", unit: "B", decimals: 0 },
  avg_records_scored: { label: "exact-scored rows/query", unit: "count", decimals: 0 },
  avg_cache_hits: { label: "cache hits/query", unit: "count", decimals: 1 },
  avg_cache_misses: { label: "cache misses/query", unit: "count", decimals: 1 },
  prefetch_depth_1_cold_p95_ms: { label: "cold p95, depth 1", unit: "ms", decimals: 1 },
  prefetch_depth_1_warm_p95_ms: { label: "warm p95, depth 1", unit: "ms", decimals: 1 },
  prefetch_depth_8_cold_p95_ms: { label: "cold p95, depth 8", unit: "ms", decimals: 1 },
  prefetch_depth_8_warm_p95_ms: { label: "warm p95, depth 8", unit: "ms", decimals: 1 },
  prefetch_depth_1_cold_avg_cache_misses: {
    label: "cold cache misses, depth 1",
    unit: "count",
    decimals: 1,
  },
  prefetch_depth_1_warm_avg_cache_hits: {
    label: "warm cache hits, depth 1",
    unit: "count",
    decimals: 1,
  },
  prefetch_depth_8_cold_avg_cache_misses: {
    label: "cold cache misses, depth 8",
    unit: "count",
    decimals: 1,
  },
  prefetch_depth_8_warm_avg_cache_hits: {
    label: "warm cache hits, depth 8",
    unit: "count",
    decimals: 1,
  },
};

const LARGE_SCALE_METRICS = {
  query_ms: { label: "query latency", unit: "ms", decimals: 0 },
  tie_aware_recall_at_10: { label: "tie-aware recall@10", unit: "", decimals: 2 },
  id_recall_at_10: { label: "id recall@10", unit: "", decimals: 2 },
  bytes_read: { label: "bytes read/query", unit: "B", decimals: 0 },
  graph_bytes_read: { label: "graph bytes/query", unit: "B", decimals: 0 },
  routing_page_indexes_read: { label: "routing indexes/query", unit: "count", decimals: 0 },
  routing_pages_read: { label: "routing pages/query", unit: "count", decimals: 0 },
  resident_bytes: { label: "resident metadata", unit: "B", decimals: 0 },
  rss_peak_delta: { label: "RSS peak delta", unit: "B", decimals: 0 },
  records_scored: { label: "exact-scored rows/query", unit: "count", decimals: 0 },
  records_considered: { label: "considered rows/query", unit: "count", decimals: 0 },
  graph_candidates_added: { label: "graph candidates", unit: "count", decimals: 0 },
  compaction_ms: { label: "compaction time", unit: "ms", decimals: 0 },
  ingest_ms: { label: "ingest time", unit: "ms", decimals: 0 },
  exact_ms: { label: "exact reference time", unit: "ms", decimals: 0 },
  compaction_bytes_read: { label: "compaction bytes read", unit: "B", decimals: 0 },
  compaction_bytes_written: { label: "compaction bytes written", unit: "B", decimals: 0 },
  gc_ms: { label: "GC time", unit: "ms", decimals: 0 },
  gc_objects_scanned: { label: "GC objects scanned", unit: "count", decimals: 0 },
  gc_objects_deleted: { label: "GC objects deleted", unit: "count", decimals: 0 },
  gc_bytes_reclaimed: { label: "GC bytes reclaimed", unit: "B", decimals: 0 },
};

const HUNDRED_MILLION_READ_METRICS = {
  elapsed_ms: { label: "query latency", unit: "ms", decimals: 0 },
  bytes_read: { label: "bytes read/query", unit: "B", decimals: 0 },
  graph_bytes_read: { label: "graph bytes/query", unit: "B", decimals: 0 },
  segments_searched: { label: "segments searched", unit: "count", decimals: 0 },
  routing_pages_read: { label: "routing pages/query", unit: "count", decimals: 0 },
  records_considered: { label: "considered rows/query", unit: "count", decimals: 0 },
  records_scored: { label: "exact-scored rows/query", unit: "count", decimals: 0 },
  graph_candidates_added: { label: "graph candidates", unit: "count", decimals: 0 },
  object_cache_hits: { label: "cache hits/query", unit: "count", decimals: 0 },
  object_cache_misses: { label: "cache misses/query", unit: "count", decimals: 0 },
};

const OVERFETCH_METRICS = {
  tie_aware_recall_at_10: { label: "tie-aware recall@10", unit: "", decimals: 2 },
  id_recall_at_10: { label: "id recall@10", unit: "", decimals: 2 },
  p95_ms: { label: "p95 latency", unit: "ms", decimals: 1 },
  avg_bytes_read: { label: "bytes read/query", unit: "B", decimals: 0 },
  avg_graph_bytes_read: { label: "graph bytes/query", unit: "B", decimals: 0 },
  avg_routing_pages_read: { label: "routing pages/query", unit: "count", decimals: 1 },
  avg_records_scored: { label: "exact-scored rows/query", unit: "count", decimals: 0 },
  avg_cache_misses: { label: "cache misses/query", unit: "count", decimals: 1 },
  prefetch_depth_1_cold_p95_ms: { label: "cold p95, depth 1", unit: "ms", decimals: 1 },
  prefetch_depth_1_warm_p95_ms: { label: "warm p95, depth 1", unit: "ms", decimals: 1 },
  prefetch_depth_8_cold_p95_ms: { label: "cold p95, depth 8", unit: "ms", decimals: 1 },
  prefetch_depth_8_warm_p95_ms: { label: "warm p95, depth 8", unit: "ms", decimals: 1 },
};

const FILTERING_METRICS = {
  avg_bytes_read: { label: "bytes read/query", unit: "B", decimals: 0 },
  avg_segments_searched: { label: "segments searched/query", unit: "count", decimals: 1 },
  avg_segments_pruned_by_filter: { label: "segments pruned/query", unit: "count", decimals: 1 },
  p50_ms: { label: "p50 latency", unit: "ms", decimals: 1 },
  p95_ms: { label: "p95 latency", unit: "ms", decimals: 1 },
  matching_records: { label: "records matching filter", unit: "count", decimals: 0 },
  avg_rows_passed_filter: { label: "rows passed filter/query", unit: "count", decimals: 0 },
  id_recall_at_10: { label: "id recall@10", unit: "", decimals: 2 },
};

const WORKLOAD_METRICS = {
  vectors: { label: "vectors in index", unit: "count", decimals: 0 },
  resident_bytes: { label: "resident memory", unit: "B", decimals: 0 },
  read_p50_ms: { label: "read p50 latency", unit: "ms", decimals: 1 },
  add_p50_ms: { label: "add+compact p50 latency", unit: "ms", decimals: 1 },
};

const WORKLOAD_COLORS = ["#2f7f73", "#c14d32", "#6f4a31", "#3b6ea5", "#8a5a9e", "#b0892e"];

const SCALING_METRICS = {
  resident_bytes: { label: "resident memory", unit: "B", decimals: 0 },
  p50_ms: { label: "query p50 latency", unit: "ms", decimals: 1 },
  p95_ms: { label: "query p95 latency", unit: "ms", decimals: 1 },
  tie_aware_recall_at_10: { label: "recall@10", unit: "", decimals: 3 },
  id_recall_at_10: { label: "id recall@10", unit: "", decimals: 3 },
  avg_bytes_read: { label: "bytes read/query", unit: "B", decimals: 0 },
  avg_segments_searched: { label: "segments searched/query", unit: "count", decimals: 1 },
};

const SPARSITY_METRICS = {
  avg_records_scored: { label: "rows exact-scored/query", unit: "count", decimals: 0 },
  id_recall_at_10: { label: "id recall@10", unit: "", decimals: 2 },
  matching_records: { label: "records matching filter", unit: "count", decimals: 0 },
  p50_ms: { label: "p50 latency", unit: "ms", decimals: 1 },
  p95_ms: { label: "p95 latency", unit: "ms", decimals: 1 },
  avg_segments_searched: { label: "segments searched/query", unit: "count", decimals: 1 },
  avg_bytes_read: { label: "bytes read/query", unit: "B", decimals: 0 },
};

const ARCH_STAGES = {
  ingest: {
    title: "Ingest",
    body: "Vectors are validated, split into immutable Parquet blobs, and appended as L0 segments. In paged mode, generated-id append reads the top routing index and fills the rightmost append branch when it is readable.",
  },
  route: {
    title: "Routing Layers",
    body: "Approximate search starts at the manifest's computed top routing layer, ranks persisted vector bounds, overfetches cheap routing metadata for recall, then fetches capped segment and graph leaves.",
  },
  leaf: {
    title: "Vector-Local Leaves",
    body: "Bounded compaction batches read selected source leaves, pack nearby vectors into L1+ leaves, and rebuild graph blocks from those records.",
  },
  graph: {
    title: "Graph Expansion",
    body: "Graph and VamanaPQ modes read segment-local graph Parquet blocks with numeric row references instead of repeated external ids.",
  },
  publish: {
    title: "Publish",
    body: "Compaction writes new Parquet objects out-of-place, reuses unchanged routing pages, promotes oversized top routing refs from metadata only, leaves old graph payloads unread, then CURRENT atomically points readers at the new manifest.",
  },
};

const HIERARCHY_VECTOR_OPTIONS = [100000, 1000000, 10000000, 100000000];
const HIERARCHY_SEGMENT_SIZE_OPTIONS = [512, 1024, 4096, 16384];
const HIERARCHY_FANOUT_OPTIONS = [64, 128, 256, 512];

document.addEventListener("DOMContentLoaded", () => {
  initCodeTabs();
  initCopyButtons();
  initDocNav();
  initArchitectureDiagram();
  initHierarchyDiagram();
  initPerformance();
});

function initCopyButtons() {
  document.querySelectorAll("[data-code-tabs]").forEach((root) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "copy-btn";
    button.textContent = "Copy";
    button.setAttribute("aria-label", "Copy code to clipboard");
    const tabRow = root.querySelector('[role="tablist"]') || root;
    tabRow.append(button);
    button.addEventListener("click", async () => {
      const panel = [...root.querySelectorAll("[data-code-panel]")].find(
        (candidate) => !candidate.hidden,
      );
      const code = panel?.querySelector("code");
      const text = code ? code.textContent : "";
      try {
        await navigator.clipboard.writeText(text);
        button.textContent = "Copied";
        button.classList.toggle("is-copied", true);
        setTimeout(() => {
          button.textContent = "Copy";
          button.classList.toggle("is-copied", false);
        }, 1500);
      } catch {
        /* Clipboard access is unavailable (e.g. file:// origin); leave the label. */
      }
    });
  });
}

function initDocNav() {
  const nav = document.querySelector("[data-doc-toc]");
  if (!nav || typeof IntersectionObserver === "undefined") return;
  const links = [...nav.querySelectorAll("a")];
  const sections = links
    .map((link) => document.getElementById(link.getAttribute("href").slice(1)))
    .filter(Boolean);
  let activeId = null;
  const setActive = (id) => {
    if (!id || id === activeId) return;
    activeId = id;
    links.forEach((link) =>
      link.classList.toggle("is-active", link.getAttribute("href") === `#${id}`),
    );
  };
  const observer = new IntersectionObserver(
    (entries) => {
      const visible = entries
        .filter((entry) => entry.isIntersecting)
        .sort((a, b) => a.boundingClientRect.top - b.boundingClientRect.top);
      if (visible[0]) setActive(visible[0].target.id);
    },
    { rootMargin: "-15% 0px -75% 0px", threshold: 0 },
  );
  sections.forEach((section) => observer.observe(section));
}

function initCodeTabs() {
  document.querySelectorAll("[data-code-tabs]").forEach((root) => {
    const buttons = [...root.querySelectorAll("[data-code-tab]")];
    const panels = [...root.querySelectorAll("[data-code-panel]")];
    const selectTab = (name) => {
      buttons.forEach((button) => {
        const selected = button.dataset.codeTab === name;
        button.classList.toggle("is-active", selected);
        button.setAttribute("aria-selected", selected ? "true" : "false");
      });
      panels.forEach((panel) => {
        panel.hidden = panel.dataset.codePanel !== name;
      });
    };
    buttons.forEach((button) => {
      button.setAttribute("role", "tab");
      button.addEventListener("click", () => selectTab(button.dataset.codeTab));
    });
    panels.forEach((panel) => {
      panel.setAttribute("role", "tabpanel");
    });
    selectTab(buttons[0]?.dataset.codeTab || "rust");
  });
}

function initArchitectureDiagram() {
  const panel = document.querySelector("[data-arch-panel]");
  if (!panel) return;
  const title = panel.querySelector("[data-arch-title]");
  const body = panel.querySelector("[data-arch-body]");
  const buttons = [...document.querySelectorAll("[data-stage]")];
  const selectStage = (stage) => {
    const content = ARCH_STAGES[stage];
    if (!content) return;
    title.textContent = content.title;
    body.textContent = content.body;
    buttons.forEach((button) => {
      button.classList.toggle("is-active", button.dataset.stage === stage);
    });
  };
  buttons.forEach((button) => {
    button.addEventListener("click", () => selectStage(button.dataset.stage));
  });
  selectStage("route");
}

function initHierarchyDiagram() {
  const root = document.querySelector("[data-hierarchy-root]");
  if (!root) return;
  const vectorsSelect = root.querySelector("[data-hierarchy-vectors]");
  const segmentSizeSelect = root.querySelector("[data-hierarchy-segment-size]");
  const fanoutSelect = root.querySelector("[data-hierarchy-fanout]");
  const levels = root.querySelector("[data-hierarchy-levels]");
  const nodes = root.querySelector("[data-hierarchy-nodes]");
  const summary = root.querySelector("[data-hierarchy-summary]");

  fillSelect(
    vectorsSelect,
    HIERARCHY_VECTOR_OPTIONS.map((value) => ({ value, label: formatInteger(value) })),
    100000,
  );
  fillSelect(
    segmentSizeSelect,
    HIERARCHY_SEGMENT_SIZE_OPTIONS.map((value) => ({ value, label: formatInteger(value) })),
    1024,
  );
  fillSelect(
    fanoutSelect,
    HIERARCHY_FANOUT_OPTIONS.map((value) => ({ value, label: formatInteger(value) })),
    128,
  );

  const render = () => {
    const shape = computeRoutingShape({
      vectors: Number(vectorsSelect.value),
      segmentSize: Number(segmentSizeSelect.value),
      fanout: Number(fanoutSelect.value),
    });
    summary.textContent =
      `${formatInteger(shape.vectors)} vectors with ${formatInteger(shape.segmentSize)} vectors per segment ` +
      `produce ${formatInteger(shape.leafBlobs)} leaf blobs, ${formatInteger(shape.leafRoutingPages)} ` +
      `L0 routing page${plural(shape.leafRoutingPages)}, and ${formatInteger(shape.routingObjects)} routing objects ` +
      `including ${formatInteger(shape.pageIndexTables)} page-index table${plural(shape.pageIndexTables)}.`;
    levels.innerHTML = shape.levels
      .slice()
      .reverse()
      .map(
        (level) => `
          <div class="hierarchy-level${level.level === shape.topLevel ? " is-top" : ""}">
            <strong>L${level.level}${level.level === shape.topLevel ? " top" : ""}</strong>
            <span>${formatInteger(level.pages)} page${plural(level.pages)}</span>
            <small>${formatInteger(level.childRefs)} child refs</small>
          </div>`,
      )
      .join("");
    nodes.innerHTML = `
      <div><strong>Vector leaf blobs</strong><span>${formatInteger(shape.leafBlobs)}</span></div>
      <div><strong>Routing content pages</strong><span>${formatInteger(shape.contentPages)}</span></div>
      <div><strong>Page-index tables</strong><span>${formatInteger(shape.pageIndexTables)}</span></div>
      <div><strong>Top routing level</strong><span>L${shape.topLevel}</span></div>`;
  };

  [vectorsSelect, segmentSizeSelect, fanoutSelect].forEach((select) => {
    select.addEventListener("change", render);
  });
  render();
}

function computeRoutingShape({ vectors, segmentSize, fanout }) {
  const leafBlobs = Math.max(1, Math.ceil(vectors / segmentSize));
  const levels = [];
  let childRefs = leafBlobs;
  let routingLevel = 0;
  while (true) {
    const pages = Math.max(1, Math.ceil(childRefs / fanout));
    levels.push({ level: routingLevel, pages, childRefs });
    if (pages <= 1) break;
    childRefs = pages;
    routingLevel += 1;
  }
  const contentPages = levels.reduce((total, level) => total + level.pages, 0);
  const pageIndexTables = levels.length;
  return {
    vectors,
    segmentSize,
    fanout,
    leafBlobs,
    leafRoutingPages: levels[0].pages,
    levels,
    topLevel: levels[levels.length - 1].level,
    contentPages,
    pageIndexTables,
    routingObjects: contentPages + pageIndexTables,
  };
}

async function initPerformance() {
  const perfRoot = document.querySelector("[data-performance-root]");
  const scaleRoot = document.querySelector("[data-scale-root]");
  const largeScaleRoot = document.querySelector("[data-large-scale-root]");
  const hundredMillionReadRoot = document.querySelector("[data-hundred-million-read-root]");
  const parallelRoot = document.querySelector("[data-parallel-root]");
  const lifecycleRoot = document.querySelector("[data-lifecycle-root]");
  const overfetchRoot = document.querySelector("[data-overfetch-root]");
  const filteringRoot = document.querySelector("[data-filtering-root]");
  const sparsityRoot = document.querySelector("[data-sparsity-root]");
  const workloadRoot = document.querySelector("[data-workload-root]");
  const scalingRoot = document.querySelector("[data-scaling-root]");
  if (
    !perfRoot &&
    !scaleRoot &&
    !largeScaleRoot &&
    !hundredMillionReadRoot &&
    !parallelRoot &&
    !lifecycleRoot &&
    !overfetchRoot &&
    !filteringRoot &&
    !sparsityRoot &&
    !workloadRoot &&
    !scalingRoot
  ) {
    return;
  }
  try {
    const [
      sequential,
      parallel,
      lifecycle,
      scale,
      largeScale,
      hundredMillionRead,
      overfetch,
      filtering,
      sparsity,
      workload,
      scaling,
    ] = await Promise.all([
      loadCsv("assets/benchmarks/sequential.csv"),
      loadCsv("assets/benchmarks/parallel.csv"),
      loadCsv("assets/benchmarks/lifecycle.csv"),
      loadCsv("assets/benchmarks/scale.csv"),
      loadCsv("assets/benchmarks/large-scale.csv"),
      loadCsv("assets/benchmarks/hundred-million-read.csv"),
      loadCsv("assets/benchmarks/routing-overfetch.csv"),
      loadCsv("assets/benchmarks/filtering.csv"),
      loadCsv("assets/benchmarks/sparsity.csv"),
      loadCsv("assets/benchmarks/workload.csv"),
      loadCsv("assets/benchmarks/dataset-scaling.csv"),
    ]);
    if (perfRoot) setupSequentialChart(perfRoot, sequential);
    if (scaleRoot) setupScaleChart(scaleRoot, scale);
    if (largeScaleRoot) setupLargeScaleChart(largeScaleRoot, largeScale);
    if (hundredMillionReadRoot)
      setupHundredMillionReadChart(hundredMillionReadRoot, hundredMillionRead);
    if (parallelRoot) setupParallelChart(parallelRoot, parallel);
    if (lifecycleRoot) setupLifecycleChart(lifecycleRoot, lifecycle);
    if (overfetchRoot) setupOverfetchChart(overfetchRoot, overfetch);
    if (filteringRoot) setupFilteringChart(filteringRoot, filtering);
    if (sparsityRoot) setupSparsityChart(sparsityRoot, sparsity);
    if (workloadRoot) setupWorkloadChart(workloadRoot, workload);
    if (scalingRoot) setupScalingChart(scalingRoot, scaling);
  } catch (error) {
    const message = "Benchmark data could not be loaded.";
    if (perfRoot) perfRoot.textContent = message;
    if (scaleRoot) scaleRoot.textContent = message;
    if (largeScaleRoot) largeScaleRoot.textContent = message;
    if (hundredMillionReadRoot) hundredMillionReadRoot.textContent = message;
    if (parallelRoot) parallelRoot.textContent = message;
    if (lifecycleRoot) lifecycleRoot.textContent = message;
    if (overfetchRoot) overfetchRoot.textContent = message;
    if (filteringRoot) filteringRoot.textContent = message;
    if (sparsityRoot) sparsityRoot.textContent = message;
    if (workloadRoot) workloadRoot.textContent = message;
    if (scalingRoot) scalingRoot.textContent = message;
    console.error(error);
  }
}

async function loadCsv(path) {
  const response = await fetch(path);
  if (!response.ok) throw new Error(`${path}: ${response.status}`);
  return parseCsv(await response.text());
}

function parseCsv(text) {
  const [headerLine, ...lines] = text.trim().split(/\r?\n/);
  const headers = headerLine.split(",");
  return lines.map((line) => {
    const values = line.split(",");
    const row = {};
    headers.forEach((header, index) => {
      const value = values[index];
      const numberValue = Number(value);
      row[header] = Number.isFinite(numberValue) && value !== "" ? numberValue : value;
    });
    return row;
  });
}

function setupSequentialChart(root, rows) {
  const datasets = unique(rows.map((row) => row.dataset));
  const datasetSelect = root.querySelector("[data-select-dataset]");
  const metricSelect = root.querySelector("[data-select-metric]");
  fillSelect(datasetSelect, datasets, datasets[0]);
  fillSelect(
    metricSelect,
    Object.keys(METRICS).map((key) => ({ value: key, label: METRICS[key].label })),
    "p95_ms",
  );
  const render = () => {
    const dataset = datasetSelect.value;
    const metric = metricSelect.value;
    const filtered = rows.filter((row) => row.dataset === dataset);
    renderBars(root.querySelector("[data-chart]"), filtered, metric, METRICS[metric]);
    renderRows(root.querySelector("[data-table]"), filtered, [
      ["mode", "Mode"],
      ["records", "Records"],
      ["tie_aware_recall_at_10", "Tie recall@10"],
      ["id_recall_at_10", "Id recall@10"],
      ["termination_reasons", "Termination"],
      ["routing_page_overfetch", "Routing overfetch"],
      ["p95_ms", "p95 ms"],
      ["prefetch_depth_1_cold_p95_ms", "Cold p95 d1"],
      ["prefetch_depth_1_warm_p95_ms", "Warm p95 d1"],
      ["prefetch_depth_8_cold_p95_ms", "Cold p95 d8"],
      ["prefetch_depth_8_warm_p95_ms", "Warm p95 d8"],
      ["avg_bytes_read", "Bytes"],
      ["avg_graph_bytes_read", "Graph bytes"],
      ["avg_routing_page_indexes_read", "Routing indexes"],
      ["avg_routing_pages_read", "Routing pages"],
      ["avg_resident_bytes", "Resident bytes"],
      ["avg_cache_hits", "Cache hits"],
      ["avg_cache_misses", "Cache misses"],
      ["prefetch_depth_1_cold_avg_cache_misses", "Cold misses d1"],
      ["prefetch_depth_1_warm_avg_cache_hits", "Warm hits d1"],
      ["prefetch_depth_8_cold_avg_cache_misses", "Cold misses d8"],
      ["prefetch_depth_8_warm_avg_cache_hits", "Warm hits d8"],
    ]);
  };
  datasetSelect.addEventListener("change", render);
  metricSelect.addEventListener("change", render);
  render();
}

function setupScaleChart(root, rows) {
  const families = unique(rows.map((row) => row.family));
  const modes = unique(rows.map((row) => row.mode));
  const familySelect = root.querySelector("[data-select-family]");
  const modeSelect = root.querySelector("[data-select-mode]");
  const metricSelect = root.querySelector("[data-select-metric]");
  fillSelect(familySelect, families, families[0]);
  fillSelect(
    modeSelect,
    modes.map((mode) => ({ value: mode, label: MODE_LABELS[mode] || mode })),
    "pq-scan",
  );
  fillSelect(
    metricSelect,
    Object.keys(SCALE_METRICS).map((key) => ({ value: key, label: SCALE_METRICS[key].label })),
    "p95_ms",
  );
  const render = () => {
    const filtered = rows
      .filter((row) => row.family === familySelect.value && row.mode === modeSelect.value)
      .sort((left, right) => left.records - right.records);
    const metric = metricSelect.value;
    renderRecordScaleLine(
      root.querySelector("[data-chart]"),
      filtered,
      metric,
      SCALE_METRICS[metric],
    );
    renderRows(root.querySelector("[data-table]"), filtered, [
      ["records", "Records"],
      ["dataset", "Dataset"],
      ["mode", "Mode"],
      ["tie_aware_recall_at_10", "Tie recall@10"],
      ["id_recall_at_10", "Id recall@10"],
      ["termination_reasons", "Termination"],
      ["routing_page_overfetch", "Routing overfetch"],
      ["p95_ms", "p95 ms"],
      ["prefetch_depth_1_cold_p95_ms", "Cold p95 d1"],
      ["prefetch_depth_1_warm_p95_ms", "Warm p95 d1"],
      ["prefetch_depth_8_cold_p95_ms", "Cold p95 d8"],
      ["prefetch_depth_8_warm_p95_ms", "Warm p95 d8"],
      ["avg_bytes_read", "Bytes"],
      ["avg_graph_bytes_read", "Graph bytes"],
      ["avg_routing_page_indexes_read", "Routing indexes"],
      ["avg_routing_pages_read", "Routing pages"],
      ["avg_resident_bytes", "Resident bytes"],
      ["avg_records_scored", "Scored rows"],
      ["avg_cache_hits", "Cache hits"],
      ["avg_cache_misses", "Cache misses"],
      ["prefetch_depth_1_cold_avg_cache_misses", "Cold misses d1"],
      ["prefetch_depth_1_warm_avg_cache_hits", "Warm hits d1"],
      ["prefetch_depth_8_cold_avg_cache_misses", "Cold misses d8"],
      ["prefetch_depth_8_warm_avg_cache_hits", "Warm hits d8"],
    ]);
  };
  familySelect.addEventListener("change", render);
  modeSelect.addEventListener("change", render);
  metricSelect.addEventListener("change", render);
  render();
}

function setupLargeScaleChart(root, rows) {
  const metricSelect = root.querySelector("[data-select-metric]");
  fillSelect(
    metricSelect,
    Object.keys(LARGE_SCALE_METRICS).map((key) => ({
      value: key,
      label: LARGE_SCALE_METRICS[key].label,
    })),
    "query_ms",
  );
  const render = () => {
    if (rows.length === 0) {
      root.querySelector("[data-chart]").textContent =
        "Large-scale benchmark artifact is empty. Regenerate assets/benchmarks/large-scale.csv with the ignored release gate.";
      renderRows(
        root.querySelector("[data-table]"),
        [],
        [
          ["mode", "Mode"],
          ["tie_aware_recall_at_10", "Tie recall@10"],
          ["id_recall_at_10", "Id recall@10"],
          ["termination_reason", "Termination"],
          ["routing_page_overfetch", "Routing overfetch"],
          ["query_ms", "Query ms"],
          ["bytes_read", "Bytes"],
          ["graph_bytes_read", "Graph bytes"],
          ["routing_page_indexes_read", "Routing indexes"],
          ["routing_pages_read", "Routing pages"],
          ["resident_bytes", "Resident bytes"],
          ["rss_peak_delta", "RSS delta"],
          ["records_considered", "Considered rows"],
          ["ingest_ms", "Ingest ms"],
          ["exact_ms", "Exact ms"],
          ["compaction_bytes_read", "Compaction bytes read"],
          ["compaction_bytes_written", "Compaction bytes written"],
          ["gc_ms", "GC ms"],
          ["gc_objects_scanned", "GC objects scanned"],
          ["gc_objects_deleted", "GC objects deleted"],
          ["gc_bytes_reclaimed", "GC bytes reclaimed"],
          ["graph_candidates_added", "Graph candidates"],
        ],
      );
      return;
    }
    const metric = metricSelect.value;
    renderBars(root.querySelector("[data-chart]"), rows, metric, LARGE_SCALE_METRICS[metric]);
    renderRows(root.querySelector("[data-table]"), rows, [
      ["mode", "Mode"],
      ["records", "Records"],
      ["tie_aware_recall_at_10", "Tie recall@10"],
      ["id_recall_at_10", "Id recall@10"],
      ["termination_reason", "Termination"],
      ["routing_page_overfetch", "Routing overfetch"],
      ["query_ms", "Query ms"],
      ["segments_searched", "Segments"],
      ["bytes_read", "Bytes"],
      ["graph_bytes_read", "Graph bytes"],
      ["routing_page_indexes_read", "Routing indexes"],
      ["routing_pages_read", "Routing pages"],
      ["resident_bytes", "Resident bytes"],
      ["rss_peak_delta", "RSS delta"],
      ["records_considered", "Considered rows"],
      ["records_scored", "Scored rows"],
      ["graph_candidates_added", "Graph candidates"],
      ["ingest_ms", "Ingest ms"],
      ["exact_ms", "Exact ms"],
      ["compaction_ms", "Compaction ms"],
      ["compaction_bytes_read", "Compaction bytes read"],
      ["compaction_bytes_written", "Compaction bytes written"],
      ["gc_ms", "GC ms"],
      ["gc_objects_scanned", "GC objects scanned"],
      ["gc_objects_deleted", "GC objects deleted"],
      ["gc_bytes_reclaimed", "GC bytes reclaimed"],
    ]);
  };
  metricSelect.addEventListener("change", render);
  render();
}

function setupHundredMillionReadChart(root, rows) {
  const metricSelect = root.querySelector("[data-select-metric]");
  fillSelect(
    metricSelect,
    Object.keys(HUNDRED_MILLION_READ_METRICS).map((key) => ({
      value: key,
      label: HUNDRED_MILLION_READ_METRICS[key].label,
    })),
    "elapsed_ms",
  );
  const render = () => {
    const metric = metricSelect.value;
    const chartRows = rows.map((row) => ({
      ...row,
      dataset: `${MODE_LABELS[row.leaf_mode] || row.leaf_mode} ${formatInteger(row.max_segments)} seg / ${formatInteger(
        row.max_candidates_per_segment,
      )} cand`,
    }));
    renderBars(
      root.querySelector("[data-chart]"),
      chartRows,
      metric,
      HUNDRED_MILLION_READ_METRICS[metric],
    );
    renderRows(root.querySelector("[data-table]"), rows, [
      ["records", "Records"],
      ["dimensions", "Dimensions"],
      ["compaction_state", "Compaction state"],
      ["query_seed", "Query seed"],
      ["leaf_mode", "Leaf mode"],
      ["max_segments", "max-segments"],
      ["routing_page_overfetch", "Routing overfetch"],
      ["max_candidates_per_segment", "Candidate rows/segment"],
      ["hit_own_id", "Found seed id"],
      ["termination_reason", "Termination"],
      ["elapsed_ms", "Elapsed ms"],
      ["segments_total", "Total segments"],
      ["segments_searched", "Segments searched"],
      ["routing_page_indexes_read", "Routing indexes"],
      ["routing_pages_read", "Routing pages"],
      ["bytes_read", "Bytes"],
      ["graph_bytes_read", "Graph bytes"],
      ["object_cache_hits", "Cache hits"],
      ["object_cache_misses", "Cache misses"],
      ["records_considered", "Considered rows"],
      ["records_scored", "Exact-scored rows"],
      ["graph_candidates_added", "Graph candidates"],
      ["resident_bytes", "Resident bytes"],
    ]);
  };
  metricSelect.addEventListener("change", render);
  render();
}

function setupParallelChart(root, rows) {
  const datasets = unique(rows.map((row) => row.dataset));
  const modes = unique(rows.map((row) => row.mode));
  const datasetSelect = root.querySelector("[data-select-dataset]");
  const modeSelect = root.querySelector("[data-select-mode]");
  const metricSelect = root.querySelector("[data-select-metric]");
  fillSelect(datasetSelect, datasets, datasets[0]);
  fillSelect(
    modeSelect,
    modes.map((mode) => ({ value: mode, label: MODE_LABELS[mode] || mode })),
    "graph",
  );
  fillSelect(
    metricSelect,
    Object.keys(PARALLEL_METRICS).map((key) => ({
      value: key,
      label: PARALLEL_METRICS[key].label,
    })),
    "rss_peak_delta",
  );
  const render = () => {
    const filtered = rows
      .filter((row) => row.dataset === datasetSelect.value && row.mode === modeSelect.value)
      .sort((left, right) => left.parallelism - right.parallelism);
    const metric = metricSelect.value;
    renderLine(root.querySelector("[data-chart]"), filtered, metric, PARALLEL_METRICS[metric]);
    renderRows(root.querySelector("[data-table]"), filtered, [
      ["parallelism", "Parallelism"],
      ["records", "Records"],
      ["qps", "QPS"],
      ["p95_ms", "p95 ms"],
      ["tie_aware_recall_at_10", "Tie recall@10"],
      ["id_recall_at_10", "Id recall@10"],
      ["termination_reasons", "Termination"],
      ["routing_page_overfetch", "Routing overfetch"],
      ["avg_bytes_read", "Bytes"],
      ["rss_peak_delta", "RSS delta"],
      ["avg_graph_bytes_read", "Graph bytes"],
      ["avg_routing_page_indexes_read", "Routing indexes"],
      ["avg_routing_pages_read", "Routing pages"],
      ["avg_resident_bytes", "Resident bytes"],
      ["avg_cache_hits", "Cache hits"],
      ["avg_cache_misses", "Cache misses"],
    ]);
  };
  datasetSelect.addEventListener("change", render);
  modeSelect.addEventListener("change", render);
  metricSelect.addEventListener("change", render);
  render();
}

function setupLifecycleChart(root, rows) {
  const metricSelect = root.querySelector("[data-select-metric]");
  fillSelect(
    metricSelect,
    Object.keys(LIFECYCLE_METRICS).map((key) => ({
      value: key,
      label: LIFECYCLE_METRICS[key].label,
    })),
    "ingest_vectors_per_sec",
  );
  const render = () => {
    const metric = metricSelect.value;
    const sorted = [...rows].sort((left, right) =>
      left.records === right.records
        ? String(left.dataset).localeCompare(String(right.dataset))
        : left.records - right.records,
    );
    renderBars(root.querySelector("[data-chart]"), sorted, metric, LIFECYCLE_METRICS[metric]);
    renderRows(root.querySelector("[data-table]"), sorted, [
      ["dataset", "Dataset"],
      ["records", "Records"],
      ["ingest_vectors_per_sec", "Ingest vectors/sec"],
      ["compaction_vectors_per_sec", "Compact vectors/sec"],
      ["ingest_ms", "Ingest ms"],
      ["compaction_ms", "Compact ms"],
      ["pre_compaction_segments", "Pre segments"],
      ["post_compaction_segments", "Post segments"],
      ["compacted_segments_read", "Segments read"],
      ["compacted_segments_written", "Segments written"],
      ["compaction_bytes_read", "Compaction bytes read"],
      ["compaction_bytes_written", "Compaction bytes written"],
      ["routing_page_indexes_read", "Routing indexes read"],
      ["routing_pages_read", "Routing pages read"],
      ["routing_page_indexes_written", "Routing indexes written"],
      ["routing_pages_written", "Routing pages written"],
      ["graph_payloads_read", "Old graphs read"],
      ["graph_bytes_read", "Old graph bytes"],
    ]);
  };
  metricSelect.addEventListener("change", render);
  render();
}

function setupOverfetchChart(root, rows) {
  const datasets = unique(rows.map((row) => row.dataset));
  const modes = unique(rows.map((row) => row.mode));
  const datasetSelect = root.querySelector("[data-select-dataset]");
  const modeSelect = root.querySelector("[data-select-mode]");
  const metricSelect = root.querySelector("[data-select-metric]");
  fillSelect(datasetSelect, datasets, datasets[0]);
  fillSelect(
    modeSelect,
    modes.map((mode) => ({ value: mode, label: MODE_LABELS[mode] || mode })),
    "pq-scan",
  );
  fillSelect(
    metricSelect,
    Object.keys(OVERFETCH_METRICS).map((key) => ({
      value: key,
      label: OVERFETCH_METRICS[key].label,
    })),
    "tie_aware_recall_at_10",
  );
  const render = () => {
    const filtered = rows
      .filter((row) => row.dataset === datasetSelect.value && row.mode === modeSelect.value)
      .sort((left, right) => left.routing_page_overfetch - right.routing_page_overfetch);
    const metric = metricSelect.value;
    renderOverfetchLine(
      root.querySelector("[data-chart]"),
      filtered,
      metric,
      OVERFETCH_METRICS[metric],
    );
    renderRows(root.querySelector("[data-table]"), filtered, [
      ["routing_page_overfetch", "Routing overfetch"],
      ["mode", "Mode"],
      ["records", "Records"],
      ["tie_aware_recall_at_10", "Tie recall@10"],
      ["id_recall_at_10", "Id recall@10"],
      ["termination_reasons", "Termination"],
      ["p95_ms", "p95 ms"],
      ["prefetch_depth_1_cold_p95_ms", "Cold p95 d1"],
      ["prefetch_depth_1_warm_p95_ms", "Warm p95 d1"],
      ["prefetch_depth_8_cold_p95_ms", "Cold p95 d8"],
      ["prefetch_depth_8_warm_p95_ms", "Warm p95 d8"],
      ["avg_bytes_read", "Bytes"],
      ["avg_graph_bytes_read", "Graph bytes"],
      ["avg_routing_page_indexes_read", "Routing indexes"],
      ["avg_routing_pages_read", "Routing pages"],
      ["avg_records_scored", "Scored rows"],
      ["avg_cache_misses", "Cache misses"],
    ]);
  };
  datasetSelect.addEventListener("change", render);
  modeSelect.addEventListener("change", render);
  metricSelect.addEventListener("change", render);
  render();
}

function setupFilteringChart(root, rows) {
  // Label each selectivity level so the shared bar renderer can title the bars.
  const ordered = [...rows]
    .map((row) => ({ ...row, dataset: row.selectivity }))
    .sort((left, right) => right.selectivity_target - left.selectivity_target);
  const metricSelect = root.querySelector("[data-select-metric]");
  fillSelect(
    metricSelect,
    Object.keys(FILTERING_METRICS).map((key) => ({
      value: key,
      label: FILTERING_METRICS[key].label,
    })),
    "avg_bytes_read",
  );
  const render = () => {
    const metric = metricSelect.value;
    renderBars(root.querySelector("[data-chart]"), ordered, metric, FILTERING_METRICS[metric]);
    renderRows(root.querySelector("[data-table]"), ordered, [
      ["selectivity", "Selectivity"],
      ["matching_records", "Matching records"],
      ["segments_total", "Segments total"],
      ["avg_segments_searched", "Segments searched"],
      ["avg_segments_pruned_by_filter", "Segments pruned"],
      ["avg_bytes_read", "Bytes read/query"],
      ["p50_ms", "p50 ms"],
      ["p95_ms", "p95 ms"],
      ["avg_rows_evaluated", "Rows evaluated"],
      ["avg_rows_passed_filter", "Rows passed"],
      ["id_recall_at_10", "Id recall@10"],
    ]);
  };
  metricSelect.addEventListener("change", render);
  render();
}

function setupSparsityChart(root, rows) {
  const ordered = [...rows]
    .map((row) => ({ ...row, dataset: `${row.rejection_pct}%` }))
    .sort((left, right) => left.rejection_pct - right.rejection_pct);
  const metricSelect = root.querySelector("[data-select-metric]");
  fillSelect(
    metricSelect,
    Object.keys(SPARSITY_METRICS).map((key) => ({
      value: key,
      label: SPARSITY_METRICS[key].label,
    })),
    "avg_records_scored",
  );
  const render = () => {
    const metric = metricSelect.value;
    renderBars(root.querySelector("[data-chart]"), ordered, metric, SPARSITY_METRICS[metric]);
    renderRows(root.querySelector("[data-table]"), ordered, [
      ["rejection_pct", "Rejection %"],
      ["matching_records", "Matching records"],
      ["avg_records_scored", "Rows scored/query"],
      ["avg_segments_searched", "Segments searched"],
      ["avg_bytes_read", "Bytes read/query"],
      ["p50_ms", "p50 ms"],
      ["p95_ms", "p95 ms"],
      ["id_recall_at_10", "Id recall@10"],
    ]);
  };
  metricSelect.addEventListener("change", render);
  render();
}

function setupWorkloadChart(root, rows) {
  const pcts = unique(rows.map((row) => row.read_pct)).sort((a, b) => a - b);
  const metricSelect = root.querySelector("[data-select-metric]");
  fillSelect(
    metricSelect,
    Object.keys(WORKLOAD_METRICS).map((key) => ({
      value: key,
      label: WORKLOAD_METRICS[key].label,
    })),
    "vectors",
  );
  const render = () => {
    const metric = metricSelect.value;
    renderWorkloadLines(
      root.querySelector("[data-chart]"),
      rows,
      pcts,
      metric,
      WORKLOAD_METRICS[metric],
    );
    const finals = pcts.map((pct) => {
      const series = rows.filter((row) => row.read_pct === pct).sort((a, b) => a.ops - b.ops);
      return { ...series[series.length - 1], dataset: `${pct}% reads` };
    });
    renderRows(root.querySelector("[data-table]"), finals, [
      ["read_pct", "Reads %"],
      ["ops", "Ops"],
      ["vectors", "Vectors"],
      ["resident_bytes", "Resident bytes"],
      ["read_p50_ms", "Read p50 ms"],
      ["add_p50_ms", "Add p50 ms"],
    ]);
  };
  metricSelect.addEventListener("change", render);
  render();
}

function renderWorkloadLines(target, rows, pcts, metric, metricInfo) {
  const width = 760;
  const height = 320;
  const top = 24;
  const right = 118;
  const bottom = 46;
  const left = 66;
  const axisY = height - bottom;
  const maxX = Math.max(...rows.map((row) => row.ops), 1);
  const maxY = Math.max(...rows.map((row) => row[metric]), 1);
  const px = (ops) => left + ((width - left - right) * ops) / maxX;
  const py = (value) => axisY - ((axisY - top) * value) / maxY;
  const series = pcts.map((pct, index) => {
    const color = WORKLOAD_COLORS[index % WORKLOAD_COLORS.length];
    const points = rows.filter((row) => row.read_pct === pct).sort((a, b) => a.ops - b.ops);
    const path = points
      .map((row, i) => `${i === 0 ? "M" : "L"} ${px(row.ops)} ${py(row[metric])}`)
      .join(" ");
    const dots = points
      .map(
        (row) =>
          `<circle cx="${px(row.ops)}" cy="${py(row[metric])}" r="2.4" style="fill:${color}"></circle>`,
      )
      .join("");
    const legendY = top + 6 + index * 17;
    const legend = `<g>
      <rect x="${width - right + 16}" y="${legendY - 8}" width="10" height="10" style="fill:${color}"></rect>
      <text class="x-label" x="${width - right + 32}" y="${legendY + 1}">${pct}% reads</text>
    </g>`;
    return `<path d="${path}" style="fill:none;stroke:${color};stroke-width:2"></path>${dots}${legend}`;
  });
  target.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${metricInfo.label} over the workload">
      <line x1="${left}" y1="${axisY}" x2="${width - right}" y2="${axisY}"></line>
      <line x1="${left}" y1="${top}" x2="${left}" y2="${axisY}"></line>
      ${series.join("")}
      <text class="x-label" x="${(left + width - right) / 2}" y="${height - 12}" text-anchor="middle">operations over time &#8594;</text>
    </svg>`;
}

function renderBars(target, rows, metric, metricInfo) {
  const width = 760;
  const top = 28;
  const right = 18;
  const left = 52;
  const labelOf = (row) =>
    row.mode ? MODE_LABELS[row.mode] || row.mode : String(row.dataset ?? "");
  const band = (width - left - right) / rows.length;
  const barW = Math.max(12, band - 16);
  // Rotate the x-axis labels when a horizontal label would spill past its bar,
  // so long labels (e.g. "PQ Scan 512 seg / 128 cand") stay readable.
  const rotate = rows.some((row) => labelOf(row).length * 6.4 > barW + 8);
  const bottom = rotate ? 92 : 58;
  const height = 300;
  const axisY = height - bottom;
  const max = Math.max(...rows.map((row) => row[metric]), 1);
  const bars = rows.map((row, index) => {
    const value = row[metric];
    const barHeight = ((axisY - top) * value) / max;
    const x = left + index * band + 8;
    const y = axisY - barHeight;
    const cx = x + barW / 2;
    const label = labelOf(row);
    const xLabel = rotate
      ? `<text class="x-label" x="${cx}" y="${axisY + 13}" text-anchor="end" transform="rotate(-35 ${cx} ${axisY + 13})">${label}</text>`
      : `<text x="${cx}" y="${axisY + 20}" text-anchor="middle">${label}</text>`;
    return `
      <g>
        <rect x="${x}" y="${y}" width="${barW}" height="${barHeight}" rx="3"></rect>
        ${xLabel}
        <text x="${cx}" y="${y - 7}" text-anchor="middle">${formatValue(value, metricInfo)}</text>
      </g>`;
  });
  target.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${metricInfo.label}">
      <line x1="${left}" y1="${axisY}" x2="${width - right}" y2="${axisY}"></line>
      <line x1="${left}" y1="${top}" x2="${left}" y2="${axisY}"></line>
      ${bars.join("")}
    </svg>`;
}

function renderRecordScaleLine(target, rows, metric, metricInfo) {
  const width = 760;
  const height = 300;
  const top = 30;
  const right = 38;
  const bottom = 54;
  const left = 64;
  if (rows.length === 0) {
    target.textContent = "No benchmark rows for this selection.";
    return;
  }
  const minX = Math.min(...rows.map((row) => row.records));
  const maxX = Math.max(...rows.map((row) => row.records), minX + 1);
  const maxY = Math.max(...rows.map((row) => row[metric]), 1);
  const points = rows.map((row) => {
    const x = left + ((width - left - right) * (row.records - minX)) / (maxX - minX || 1);
    const y = height - bottom - ((height - top - bottom) * row[metric]) / maxY;
    return { x, y, row };
  });
  const path = points
    .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`)
    .join(" ");
  const circles = points.map(
    ({ x, y, row }) => `
    <g>
      <circle cx="${x}" cy="${y}" r="5"></circle>
      <text x="${x}" y="${y - 12}" text-anchor="middle">${formatValue(row[metric], metricInfo)}</text>
      <text x="${x}" y="${height - 28}" text-anchor="middle">${formatRecordCount(row.records)}</text>
    </g>`,
  );
  target.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${metricInfo.label} by record count">
      <line x1="${left}" y1="${height - bottom}" x2="${width - right}" y2="${height - bottom}"></line>
      <line x1="${left}" y1="${top}" x2="${left}" y2="${height - bottom}"></line>
      <path d="${path}"></path>
      ${circles.join("")}
    </svg>`;
}

function renderOverfetchLine(target, rows, metric, metricInfo) {
  const width = 760;
  const height = 300;
  const top = 30;
  const right = 38;
  const bottom = 54;
  const left = 64;
  if (rows.length === 0) {
    target.textContent = "No benchmark rows for this selection.";
    return;
  }
  const minX = Math.min(...rows.map((row) => row.routing_page_overfetch));
  const maxX = Math.max(...rows.map((row) => row.routing_page_overfetch), minX + 1);
  const maxY = Math.max(...rows.map((row) => row[metric]), 1);
  const points = rows.map((row) => {
    const x =
      left + ((width - left - right) * (row.routing_page_overfetch - minX)) / (maxX - minX || 1);
    const y = height - bottom - ((height - top - bottom) * row[metric]) / maxY;
    return { x, y, row };
  });
  const path = points
    .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`)
    .join(" ");
  const circles = points.map(
    ({ x, y, row }) => `
    <g>
      <circle cx="${x}" cy="${y}" r="5"></circle>
      <text x="${x}" y="${y - 12}" text-anchor="middle">${formatValue(row[metric], metricInfo)}</text>
      <text x="${x}" y="${height - 28}" text-anchor="middle">${row.routing_page_overfetch}x</text>
    </g>`,
  );
  target.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${metricInfo.label} by routing overfetch">
      <line x1="${left}" y1="${height - bottom}" x2="${width - right}" y2="${height - bottom}"></line>
      <line x1="${left}" y1="${top}" x2="${left}" y2="${height - bottom}"></line>
      <path d="${path}"></path>
      ${circles.join("")}
    </svg>`;
}

function renderLine(target, rows, metric, metricInfo) {
  const width = 760;
  const height = 300;
  const top = 30;
  const right = 38;
  const bottom = 54;
  const left = 58;
  const maxX = Math.max(...rows.map((row) => row.parallelism), 1);
  const maxY = Math.max(...rows.map((row) => row[metric]), 1);
  const points = rows.map((row) => {
    const x = left + ((width - left - right) * row.parallelism) / maxX;
    const y = height - bottom - ((height - top - bottom) * row[metric]) / maxY;
    return { x, y, row };
  });
  const path = points
    .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`)
    .join(" ");
  const circles = points.map(
    ({ x, y, row }) => `
    <g>
      <circle cx="${x}" cy="${y}" r="5"></circle>
      <text x="${x}" y="${y - 12}" text-anchor="middle">${formatValue(row[metric], metricInfo)}</text>
      <text x="${x}" y="${height - 28}" text-anchor="middle">${row.parallelism}x</text>
    </g>`,
  );
  target.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${metricInfo.label} by parallelism">
      <line x1="${left}" y1="${height - bottom}" x2="${width - right}" y2="${height - bottom}"></line>
      <line x1="${left}" y1="${top}" x2="${left}" y2="${height - bottom}"></line>
      <path d="${path}"></path>
      ${circles.join("")}
    </svg>`;
}

function setupScalingChart(root, rows) {
  const points = [...rows].sort((a, b) => a.records - b.records);
  const metricSelect = root.querySelector("[data-select-metric]");
  fillSelect(
    metricSelect,
    Object.keys(SCALING_METRICS).map((key) => ({
      value: key,
      label: SCALING_METRICS[key].label,
    })),
    "resident_bytes",
  );
  const render = () => {
    const metric = metricSelect.value;
    renderScalingLine(root.querySelector("[data-chart]"), points, metric, SCALING_METRICS[metric]);
    renderRows(root.querySelector("[data-table]"), points, [
      ["records", "Records"],
      ["tie_aware_recall_at_10", "recall@10"],
      ["p50_ms", "p50 ms"],
      ["p95_ms", "p95 ms"],
      ["resident_bytes", "Resident bytes"],
      ["avg_bytes_read", "Bytes/query"],
      ["avg_segments_searched", "Segments/query"],
    ]);
  };
  metricSelect.addEventListener("change", render);
  render();
}

// Records span three-plus orders of magnitude (10k -> 10M), so the x-axis is
// spaced evenly by index (categorical) rather than linearly, which would bunch
// every point but the largest against the right edge.
function renderScalingLine(target, points, metric, metricInfo) {
  const width = 760;
  const height = 320;
  const top = 24;
  const right = 28;
  const bottom = 46;
  const left = 72;
  const axisY = height - bottom;
  const maxY = Math.max(...points.map((row) => row[metric]), metricInfo.decimals >= 3 ? 1 : 0.0001);
  const stepX = points.length > 1 ? (width - left - right) / (points.length - 1) : 0;
  const px = (index) => left + stepX * index;
  const py = (value) => axisY - ((axisY - top) * value) / maxY;
  const path = points
    .map((row, i) => `${i === 0 ? "M" : "L"} ${px(i)} ${py(row[metric])}`)
    .join(" ");
  const dots = points
    .map(
      (row, i) =>
        `<circle cx="${px(i)}" cy="${py(row[metric])}" r="3" style="fill:#2f7f73"></circle>`,
    )
    .join("");
  const xLabels = points
    .map(
      (row, i) =>
        `<text class="x-label" x="${px(i)}" y="${height - 26}" text-anchor="middle">${formatInteger(row.records)}</text>`,
    )
    .join("");
  target.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${metricInfo.label} as the dataset grows">
      <line x1="${left}" y1="${axisY}" x2="${width - right}" y2="${axisY}"></line>
      <line x1="${left}" y1="${top}" x2="${left}" y2="${axisY}"></line>
      <text class="x-label" x="${left - 8}" y="${top + 4}" text-anchor="end">${formatValue(maxY, metricInfo)}</text>
      <text class="x-label" x="${left - 8}" y="${axisY}" text-anchor="end">0</text>
      <path d="${path}" style="fill:none;stroke:#2f7f73;stroke-width:2"></path>
      ${dots}
      ${xLabels}
      <text class="x-label" x="${(left + width - right) / 2}" y="${height - 8}" text-anchor="middle">records in index &#8594;</text>
    </svg>`;
}

function renderRows(target, rows, columns) {
  target.innerHTML = `
    <table>
      <thead><tr>${columns.map(([, label]) => `<th>${label}</th>`).join("")}</tr></thead>
      <tbody>
        ${rows
          .map(
            (row) =>
              `<tr>${columns
                .map(
                  ([key]) =>
                    `<td>${key === "mode" ? MODE_LABELS[row[key]] || row[key] : formatTableValue(row[key], key)}</td>`,
                )
                .join("")}</tr>`,
          )
          .join("")}
      </tbody>
    </table>`;
}

function fillSelect(select, values, selected) {
  select.innerHTML = values
    .map((value) => {
      const option = typeof value === "string" ? { value, label: value } : value;
      return `<option value="${option.value}"${option.value === selected ? " selected" : ""}>${option.label}</option>`;
    })
    .join("");
}

function unique(values) {
  return [...new Set(values)];
}

function formatValue(value, metricInfo) {
  if (metricInfo.unit === "B") return formatBytes(value);
  if (metricInfo.unit === "Bps") return `${formatBytes(value)}/s`;
  if (metricInfo.unit === "rate") return `${value.toFixed(metricInfo.decimals)}/s`;
  if (metricInfo.unit === "qps") return `${value.toFixed(metricInfo.decimals)} qps`;
  if (metricInfo.unit === "ms") return `${value.toFixed(metricInfo.decimals)} ms`;
  return value.toFixed(metricInfo.decimals);
}

function formatTableValue(value, key = "") {
  if (typeof value !== "number") return value;
  if (isByteField(key)) return formatBytes(value);
  if (isCountField(key)) return formatInteger(value);
  if (Math.abs(value) >= 1000) return value.toFixed(0);
  if (Number.isInteger(value)) return String(value);
  return value.toFixed(2);
}

function isByteField(key) {
  return key.includes("bytes") || key.startsWith("rss_") || key.endsWith("_resident_bytes");
}

function isCountField(key) {
  return (
    key === "records" ||
    key === "queries" ||
    key === "parallelism" ||
    key === "batch_records" ||
    key === "routing_page_overfetch" ||
    key === "manifest_version" ||
    key.endsWith("_records") ||
    key.endsWith("_segments") ||
    key.endsWith("_pages") ||
    key.endsWith("_indexes") ||
    key.includes("records_") ||
    key.includes("segments_") ||
    key.includes("routing_pages") ||
    key.includes("routing_page_indexes")
  );
}

function formatBytes(value) {
  const units = ["B", "KB", "MB", "GB"];
  let scaled = Math.abs(value);
  let unit = 0;
  while (scaled >= 1000 && unit < units.length - 1) {
    scaled /= 1000;
    unit += 1;
  }
  const sign = value < 0 ? "-" : "";
  const decimals = scaled >= 100 || unit === 0 ? 0 : 1;
  return `${sign}${scaled.toFixed(decimals)} ${units[unit]}`;
}

function formatRecordCount(value) {
  if (value >= 1000000) return `${(value / 1000000).toFixed(value % 1000000 === 0 ? 0 : 1)}M`;
  if (value >= 1000) return `${(value / 1000).toFixed(value % 1000 === 0 ? 0 : 1)}k`;
  return String(value);
}

function formatInteger(value) {
  return new Intl.NumberFormat("en-US").format(value);
}

function plural(value) {
  return value === 1 ? "" : "s";
}

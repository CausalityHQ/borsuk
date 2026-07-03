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
  avg_resident_bytes: { label: "resident metadata", unit: "B", decimals: 0 },
};

const PARALLEL_METRICS = {
  p95_ms: { label: "p95 latency", unit: "ms", decimals: 1 },
  qps: { label: "queries/sec", unit: "qps", decimals: 1 },
  rss_peak_delta: { label: "RSS peak delta", unit: "B", decimals: 0 },
  avg_graph_bytes_read: { label: "graph bytes/query", unit: "B", decimals: 0 },
};

const ARCH_STAGES = {
  ingest: {
    title: "Ingest",
    body: "Vectors are validated, split into immutable Parquet blobs, and appended as L0 segments. This path stays fast and does not compact inline.",
  },
  route: {
    title: "Routing Layers",
    body: "Approximate search starts at the manifest's top routing layer, ranks centroid/radius rows, walks selected parent pages, then fetches selected leaves.",
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
    body: "Compaction writes new Parquet objects out-of-place, reuses unchanged routing pages, leaves old graph payloads unread, then CURRENT atomically points readers at the new manifest.",
  },
};

document.addEventListener("DOMContentLoaded", () => {
  initCodeTabs();
  initArchitectureDiagram();
  initPerformance();
});

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

async function initPerformance() {
  const perfRoot = document.querySelector("[data-performance-root]");
  const parallelRoot = document.querySelector("[data-parallel-root]");
  if (!perfRoot && !parallelRoot) return;
  try {
    const [sequential, parallel] = await Promise.all([
      loadCsv("assets/benchmarks/sequential.csv"),
      loadCsv("assets/benchmarks/parallel.csv"),
    ]);
    if (perfRoot) setupSequentialChart(perfRoot, sequential);
    if (parallelRoot) setupParallelChart(parallelRoot, parallel);
  } catch (error) {
    const message = "Benchmark data could not be loaded.";
    if (perfRoot) perfRoot.textContent = message;
    if (parallelRoot) parallelRoot.textContent = message;
    console.error(error);
  }
}

async function loadCsv(path) {
  const response = await fetch(path);
  if (!response.ok) throw new Error(`${path}: ${response.status}`);
  const text = await response.text();
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
      ["p95_ms", "p95 ms"],
      ["avg_bytes_read", "Bytes"],
      ["avg_graph_bytes_read", "Graph bytes"],
      ["avg_resident_bytes", "Resident bytes"],
    ]);
  };
  datasetSelect.addEventListener("change", render);
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
  fillSelect(modeSelect, modes.map((mode) => ({ value: mode, label: MODE_LABELS[mode] || mode })), "graph");
  fillSelect(
    metricSelect,
    Object.keys(PARALLEL_METRICS).map((key) => ({ value: key, label: PARALLEL_METRICS[key].label })),
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
      ["rss_peak_delta", "RSS delta"],
      ["avg_graph_bytes_read", "Graph bytes"],
    ]);
  };
  datasetSelect.addEventListener("change", render);
  modeSelect.addEventListener("change", render);
  metricSelect.addEventListener("change", render);
  render();
}

function renderBars(target, rows, metric, metricInfo) {
  const width = 760;
  const height = 300;
  const top = 28;
  const right = 18;
  const bottom = 58;
  const left = 52;
  const max = Math.max(...rows.map((row) => row[metric]), 1);
  const band = (width - left - right) / rows.length;
  const bars = rows.map((row, index) => {
    const value = row[metric];
    const barHeight = ((height - top - bottom) * value) / max;
    const x = left + index * band + 8;
    const y = height - bottom - barHeight;
    const label = MODE_LABELS[row.mode] || row.mode;
    return `
      <g>
        <rect x="${x}" y="${y}" width="${Math.max(12, band - 16)}" height="${barHeight}" rx="3"></rect>
        <text x="${x + Math.max(12, band - 16) / 2}" y="${height - 32}" text-anchor="middle">${label}</text>
        <text x="${x + Math.max(12, band - 16) / 2}" y="${y - 7}" text-anchor="middle">${formatValue(value, metricInfo)}</text>
      </g>`;
  });
  target.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${metricInfo.label} by mode">
      <line x1="${left}" y1="${height - bottom}" x2="${width - right}" y2="${height - bottom}"></line>
      <line x1="${left}" y1="${top}" x2="${left}" y2="${height - bottom}"></line>
      ${bars.join("")}
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
  const path = points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");
  const circles = points.map(({ x, y, row }) => `
    <g>
      <circle cx="${x}" cy="${y}" r="5"></circle>
      <text x="${x}" y="${y - 12}" text-anchor="middle">${formatValue(row[metric], metricInfo)}</text>
      <text x="${x}" y="${height - 28}" text-anchor="middle">${row.parallelism}x</text>
    </g>`);
  target.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${metricInfo.label} by parallelism">
      <line x1="${left}" y1="${height - bottom}" x2="${width - right}" y2="${height - bottom}"></line>
      <line x1="${left}" y1="${top}" x2="${left}" y2="${height - bottom}"></line>
      <path d="${path}"></path>
      ${circles.join("")}
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
                .map(([key]) => `<td>${key === "mode" ? MODE_LABELS[row[key]] || row[key] : formatTableValue(row[key])}</td>`)
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
  if (metricInfo.unit === "qps") return `${value.toFixed(metricInfo.decimals)} qps`;
  if (metricInfo.unit === "ms") return `${value.toFixed(metricInfo.decimals)} ms`;
  return value.toFixed(metricInfo.decimals);
}

function formatTableValue(value) {
  if (typeof value !== "number") return value;
  if (Math.abs(value) >= 100000) return formatBytes(value);
  if (Math.abs(value) >= 1000) return value.toFixed(0);
  if (Number.isInteger(value)) return String(value);
  return value.toFixed(2);
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

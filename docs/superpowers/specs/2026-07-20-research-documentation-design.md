# Research Documentation Separation Design

## Objective

Make BORSUK's default documentation describe only the supported production
architecture, defaults, cache semantics, and operational tuning. Move every
experimental result, method comparison, configuration sweep, external-system
comparison, resource graph, publication claim, and reproduction procedure into
a dedicated `docs/research/` hierarchy.

The research documentation must be evidence-led: a result is presented only
when a checked-in artifact identifies the dataset, corpus size, dimensions,
metric, method, complete configuration, recall definition, cache state,
concurrency, client environment, and resource telemetry. Missing combinations
remain visible in a coverage matrix and are never inferred from another method
or dataset.

## Considered structures

### One monolithic benchmark document

This preserves the existing layout but mixes production guidance, historical
results, local synthetic characterization, AWS public-corpus runs, and vendor
context. It is difficult to cite and makes obsolete configurations appear to be
current defaults. Rejected.

### Web-only research dashboard

This gives attractive interactive charts but makes peer review, diffs, and
offline citation difficult. It also risks hiding methodological caveats behind
the chart implementation. Rejected as the canonical source; the web page remains
a presentation layer.

### Structured Markdown research hierarchy with checked-in data

This is the selected design. Small, focused Markdown pages own methodology,
standard datasets, method evaluation, configuration ablations, scaling,
external comparisons, and reproducibility. The web research page links to and
visualizes these canonical artifacts. Default docs link to the research index
but contain no deep benchmark tables.

## Information architecture

- `docs/research/README.md`: scope, evidence rules, cache-state definitions,
  coverage matrix, and navigation.
- `docs/research/standard-datasets.md`: the six public ANN corpora, full-corpus
  AWS TurboQuant pq-scan recall/latency/resource results, repetitions, and
  per-dataset curves.
- `docs/research/methods.md`: exact, flat-scan, SQ-scan, TurboQuant pq-scan,
  graph, Vamana-PQ, and hybrid semantics plus the currently available controlled
  comparative evidence. It explicitly states that only pq-scan currently has
  full six-corpus AWS evidence.
- `docs/research/configuration-ablation.md`: `nprobe`, candidate budget,
  prefetch width, cell layout, query/decode caps, single-flight behavior, cache
  states, and uncapped overload.
- `docs/research/scale-and-workloads.md`: metric pruning, dense/sparse/text
  mixtures, filtering, updates, parallelism, 1M, and 100M experiments.
- `docs/research/systems-comparison.md`: direct S3 Vectors result, separately
  labeled vendor-reported context, nearest related systems, and publication
  positioning.
- `docs/research/reproducibility.md`: commands, environment, artifact schemas,
  chart generation, raw telemetry, and qualification gates.
- `docs/benchmarks.md`: short production benchmark contract and entrypoint to
  `docs/research/`, not the research corpus itself.

## Evidence classes

1. **Direct standard-dataset AWS evidence**: Fashion-MNIST, GloVe, SIFT,
   NYTimes, GIST, and Deep-Image on full shipped corpora and ground truth.
2. **Controlled method evidence**: sklearn-digits plus uniform, clustered, and
   adversarial synthetic families at 10k/100k, with all seven leaf methods.
3. **Scale/workload evidence**: synthetic datasets used to isolate a particular
   mechanism or reach 1M/100M scale.
4. **Direct external evidence**: identical Fashion-MNIST query set against
   Amazon S3 Vectors.
5. **Vendor-reported context**: values from primary vendor documentation, never
   plotted or summarized as a direct benchmark.

These classes must remain visually and textually separate.

## Standard-dataset method matrix

The existing six-corpus indexes are TurboQuant pq-scan-only production indexes.
The documentation therefore marks the remaining six-corpus method cells as
`not measured`, rather than copying local method results into the table. A
standard-dataset method-matrix runner will enumerate a bounded matrix of leaf
modes, `nprobe`, candidate budgets, layouts, cache states, and resource
telemetry. Graph-backed methods require a graph-enabled index and cannot reuse a
pq-scan-only index.

The bounded matrix is:

- methods: exact, flat-scan, SQ-scan, pq-scan, graph, Vamana-PQ, hybrid;
- standard datasets: all six corpora;
- recall sweep: dataset-specific `nprobe` frontier and candidate budgets
  16/32/64/128 where applicable;
- layout: the dimension-aware default, with the Fashion 128/256/512/1024/4096
  ablation retained separately;
- serving states: startup, uncached, disk-cached, and optional memory-preloaded;
- production concurrency: query cap 4 and globally bounded decode-cap ablations,
  with 24 selected by the final cross-corpus pass;
- research ceiling: explicitly uncapped and isolated from production results;
- telemetry: p50/p95/p99, recall@10, QPS, bytes/GETs, CPU, RSS/VMS, process disk
  reads/writes, and cache footprint.

## Default documentation boundary

The README, API guide, architecture guide, and web documentation may state the
current default and explain how to tune it. They must not contain historical
benchmark tables, exhaustive method comparisons, recall curves, uncapped
results, or vendor performance figures. Those pages link to the research index
for evidence.

## Validation

A repository-local validator checks:

- every research page and referenced artifact exists;
- the six standard datasets appear in production, recall, uncapped, and
  resource evidence;
- every claimed method is represented in the controlled method artifact;
- cache-state and resource columns are present;
- standard-dataset method coverage is reported honestly;
- default documentation does not reintroduce research-only headings/tables.

The existing chart and resource-script unit tests remain required, together
with Markdown link checking and `git diff --check`.

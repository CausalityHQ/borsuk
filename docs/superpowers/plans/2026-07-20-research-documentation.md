# Research Documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Separate production documentation from a complete, evidence-led research section covering standard datasets, all leaf methods, configuration ablations, resources, scale, external comparisons, and reproduction.

**Architecture:** Canonical Markdown pages under `docs/research/` reference checked-in CSV/SVG/raw artifacts. A validator enforces dataset/method/schema coverage and the production/research boundary. A matrix runner enumerates the missing full-corpus method experiments without treating unmeasured combinations as results.

**Tech Stack:** Markdown, Python 3 standard library, Bash, Rust `production_bench`, checked-in CSV/SVG artifacts.

---

### Task 1: Research information architecture

**Files:**
- Create: `docs/research/README.md`
- Create: `docs/research/standard-datasets.md`
- Create: `docs/research/methods.md`
- Create: `docs/research/configuration-ablation.md`
- Create: `docs/research/scale-and-workloads.md`
- Create: `docs/research/systems-comparison.md`
- Create: `docs/research/reproducibility.md`

- [ ] **Step 1: Create the research index**

Write the five evidence classes, cache-state definitions, seven-method × six-dataset coverage matrix, qualification gates, and links to every focused page.

- [ ] **Step 2: Write the standard-dataset evaluation**

Document corpus sizes, dimensions, metrics, shipped ground-truth recall, selected full-corpus AWS profiles, repetitions, recall curves, and resource-graph links from:

```text
docs/web/assets/benchmarks/aws-production-profiles.csv
docs/web/assets/benchmarks/aws-production-repetitions-2026-07-20.csv
docs/web/assets/benchmarks/aws-recall-latency-2026-07-20.csv
docs/web/assets/benchmarks/raw/2026-07-20/
docs/web/assets/benchmarks/resources/
```

- [ ] **Step 3: Write the leaf-method evaluation**

Define exact, flat-scan, SQ-scan, pq-scan, graph, Vamana-PQ, and hybrid. Report the controlled sklearn-digits/synthetic matrix from `sequential.csv`, `parallel.csv`, and `routing-overfetch.csv`; label the six-corpus non-pq cells `not measured`.

- [ ] **Step 4: Write configuration and concurrency ablations**

Document the independent effects of cell rows, `nprobe`, candidate budget, prefetch width, query cap, decode cap, single-flight sharing, and cache state. Include the Fashion 4096/1024/512/256/128 table and bounded-vs-uncapped results.

- [ ] **Step 5: Write scale/workload and comparison pages**

Move the metric, filtering, mixture, update, parallel, 1M, and 100M studies into `scale-and-workloads.md`. Move direct S3 Vectors, vendor context, related systems, and novelty guidance into `systems-comparison.md`.

- [ ] **Step 6: Write reproduction page**

Include exact commands for fetching datasets, full-corpus runs, method matrix, resource wrapper, chart generation, artifact locations, required environment variables, and acceptance criteria.

### Task 2: Production documentation boundary

**Files:**
- Modify: `README.md`
- Modify: `docs/api.md`
- Replace: `docs/benchmarks.md`
- Modify: `docs/web/docs.html`
- Modify: `docs/web/research.html`
- Modify: `docs/publication-notes.md`

- [ ] **Step 1: Reduce README performance copy**

Keep only the selected production architecture/defaults and link to `docs/research/README.md`; remove deep result descriptions.

- [ ] **Step 2: Remove measured ablations from API guidance**

Keep the dimension-aware formula, tuning direction, and concurrency/cache semantics. Replace Fashion-specific numbers with a research link.

- [ ] **Step 3: Replace the monolithic benchmark page**

Make `docs/benchmarks.md` a short production benchmark contract: cache terminology, qualification gate, required metrics, canonical command, and research navigation.

- [ ] **Step 4: Align web docs**

Ensure `docs.html` contains production guidance only. Expand `research.html` navigation to the standard datasets, methods, configurations, resources, scale, comparison, and reproduction pages/artifacts.

- [ ] **Step 5: Keep publication notes under research ownership**

Turn `docs/publication-notes.md` into a compatibility pointer to `docs/research/systems-comparison.md`, avoiding duplicate claims.

### Task 3: Evidence validator

**Files:**
- Create: `scripts/validate_research_docs.py`
- Create: `scripts/test_validate_research_docs.py`

- [ ] **Step 1: Write validator tests**

Test successful repository validation plus failures for a missing standard dataset, missing leaf method, missing resource column, broken local research link, and research-only content in a default document.

- [ ] **Step 2: Run tests and confirm the validator is absent**

Run:

```bash
PYTHONPATH=scripts python3 -m unittest scripts/test_validate_research_docs.py
```

Expected: failure because `validate_research_docs` does not exist.

- [ ] **Step 3: Implement validator**

Use only `csv`, `pathlib`, `re`, and `html.parser`. Validate the six dataset ids, seven method ids, required CSV columns, resource timelines, local Markdown links, and forbidden research headings in README/API/architecture/web docs.

- [ ] **Step 4: Run validator tests and repository validation**

Run:

```bash
PYTHONPATH=scripts python3 -m unittest scripts/test_validate_research_docs.py
python3 scripts/validate_research_docs.py
```

Expected: all tests pass and the repository validator prints the dataset/method/artifact counts.

### Task 4: Standard-dataset method-matrix runner

**Files:**
- Create: `scripts/bench_standard_method_matrix.sh`
- Create: `scripts/test_bench_standard_method_matrix.py`

- [ ] **Step 1: Write runner contract tests**

Use temporary fake `cargo` and resource-wrapper executables to assert that the runner enumerates six datasets and seven methods, isolates graph-enabled from pq-scan-only index prefixes, forwards dimension-aware layout defaults, and continues while recording failed cells.

- [ ] **Step 2: Run tests and confirm the runner is absent**

Run:

```bash
python3 -m unittest scripts/test_bench_standard_method_matrix.py
```

Expected: failure because `bench_standard_method_matrix.sh` does not exist.

- [ ] **Step 3: Implement bounded enumeration**

The script must default to a dry run and require `BORSUK_RUN_STANDARD_MATRIX=1` before issuing paid S3 writes/reads. It emits one output directory per dataset/method and a `coverage.csv` with status, index capability, nprobe list, candidate list, cell rows, cache states, and resource path.

- [ ] **Step 4: Run runner contract tests**

Run:

```bash
python3 -m unittest scripts/test_bench_standard_method_matrix.py
```

Expected: all enumeration and failure-accounting tests pass without AWS access.

### Task 5: Final validation

**Files:**
- Verify all files above.

- [ ] **Step 1: Run all benchmark documentation tests**

```bash
PYTHONPATH=scripts python3 -m unittest \
  scripts/test_benchmark_with_resources.py \
  scripts/test_render_resource_charts.py \
  scripts/test_render_recall_latency_charts.py \
  scripts/test_bench_s3_full.py \
  scripts/test_validate_research_docs.py \
  scripts/test_bench_standard_method_matrix.py
```

Expected: all tests pass.

- [ ] **Step 2: Run repository hygiene checks**

```bash
cargo fmt --all -- --check
git diff --check
python3 scripts/validate_research_docs.py
```

Expected: all commands exit zero.

- [ ] **Step 3: Inspect the final boundary**

Confirm default docs contain no recall tables, uncapped results, or external
latency comparisons, and every removed result remains reachable under
`docs/research/` with its artifact link.

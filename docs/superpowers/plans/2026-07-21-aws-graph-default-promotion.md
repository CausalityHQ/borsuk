# AWS Graph Default Promotion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce publication-grade AWS evidence comparing optimized graph search with TurboQuant `pq-scan` across six public and four controlled datasets, and change the production default only if the recorded promotion gates pass.

**Architecture:** Extend the production benchmark with an explicit index-capability control, drive paired graph-free/graph-enabled experiments from one resumable Python orchestrator, and evaluate results with a deterministic promotion-gate script. Raw rows remain immutable and dated; generated summaries, charts, documentation, and any default change are downstream products of the validated evidence manifest.

**Tech Stack:** Rust/Cargo, Python 3 standard library, Bash, AWS CLI/SSO, Amazon S3, EC2 Graviton `c7g.8xlarge`, SVG chart renderers, HTML/Markdown documentation.

---

### Task 1: Make production benchmark index capability explicit

**Files:**
- Modify: `crates/borsuk/examples/production_bench.rs`
- Test: `crates/borsuk/examples/production_bench.rs`

- [ ] **Step 1: Write failing argument tests**

Add tests beside the existing default-leaf tests:

```rust
#[test]
fn leaf_capability_control_accepts_public_names() {
    assert_eq!(parse_leaf_capability("pq-scan-only").unwrap(), LeafCapability::PqScanOnly);
    assert_eq!(parse_leaf_capability("graph-enabled").unwrap(), LeafCapability::GraphEnabled);
}

#[test]
fn default_build_capability_is_graph_free() {
    assert_eq!(default_build_leaf_capability(), LeafCapability::PqScanOnly);
}
```

- [ ] **Step 2: Run the focused tests and verify failure**

Run:

```bash
cargo test --locked -p borsuk --example production_bench leaf_capability
```

Expected: compilation fails because the parser/default functions do not exist.

- [ ] **Step 3: Parse and print `BORSUK_BENCH_LEAF_CAPABILITY`**

Add `leaf_capability: LeafCapability` to `ResolvedConfig`, parse it with:

```rust
let leaf_capability = non_empty_env("BORSUK_BENCH_LEAF_CAPABILITY")
    .map_or(Ok(default_build_leaf_capability()), |value| parse_leaf_capability(&value))?;
```

Use the public `LeafCapability::from_str` parser, print it in `print_config`, and create indexes with:

```rust
let mut index = BorsukIndex::create_with_leaf_capability(
    IndexConfig { /* existing fields unchanged */ },
    config.leaf_capability,
)?;
```

Reject graph-backed recall or serving modes before ingest when capability is
`PqScanOnly`.

- [ ] **Step 4: Run focused and capability integration tests**

Run:

```bash
cargo test --locked -p borsuk --example production_bench
cargo test --locked -p borsuk --test leaf_capability
```

Expected: all tests pass; existing default assertions remain graph-free.

- [ ] **Step 5: Commit**

```bash
git add crates/borsuk/examples/production_bench.rs
git commit -m "bench: make graph index capability explicit"
```

### Task 2: Add a resumable graph-promotion matrix orchestrator

**Files:**
- Create: `scripts/bench_graph_promotion.py`
- Create: `scripts/test_bench_graph_promotion.py`
- Modify: `scripts/bench_standard_method_matrix.sh`

- [ ] **Step 1: Write failing manifest and command tests**

Test that dry-run planning emits 6 public × 3 paired layouts/methods plus four
controlled families, uses no placeholder S3 URI during execution, includes
three repetitions, and never runs graph against a graph-free index:

```python
def test_plan_covers_public_and_controlled_datasets(self):
    rows = promotion.plan_matrix(bucket="s3://bucket", repetitions=3)
    public = [row for row in rows if row.dataset_kind == "public"]
    self.assertEqual(len(public), 6 * 3)
    self.assertEqual(
        {row.dataset for row in rows if row.dataset_kind == "controlled"},
        {"sklearn-digits", "synthetic-uniform", "synthetic-clustered", "synthetic-adversarial"},
    )

def test_graph_requires_graph_enabled_index(self):
    for row in promotion.plan_matrix("s3://bucket", 3):
        if row.method == "graph":
            self.assertEqual(row.index_capability, "graph-enabled")
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
PYTHONPATH=scripts python3 -m unittest scripts/test_bench_graph_promotion.py
```

Expected: import failure because `bench_graph_promotion.py` does not exist.

- [ ] **Step 3: Implement immutable experiment specifications**

Define frozen records with explicit dimensions, metrics, layouts, sweep grids,
and selected production controls:

```python
@dataclass(frozen=True)
class DatasetSpec:
    name: str
    dimensions: int
    metric: str
    segment_rows: int
    probes: tuple[int, ...]
    candidates: tuple[int, ...]
    recall_target: float

@dataclass(frozen=True)
class MatrixRow:
    dataset_kind: str
    dataset: str
    method: str
    index_capability: str
    index_variant: str
    repetitions: int
```

Public rows are `pq-scan/pq-scan-only`, `pq-scan/graph-enabled`, and
`graph/graph-enabled`. The controlled suite uses the existing graph-enabled
`benchmark_report` so exact ground truth and all leaf modes share one index.

- [ ] **Step 4: Implement dry-run, execute, resume, and artifact metadata**

Support:

```text
--datasets-root PATH
--output-root PATH
--bucket s3://...
--execute
--build-indexes
--resume
--repetitions 3
--source-sha SHA
```

For each public corpus:

1. build graph-free and graph-enabled S3 prefixes once;
2. run full-query recall sweeps for `pq-scan` and graph;
3. select recall-matched points deterministically;
4. run three fresh-process production repetitions for uncached/disk-cached;
5. run separately labeled memory-preloaded repetitions;
6. run capped concurrency `1,2,4,8,16`; and
7. run uncapped research concurrency `1,2,4,8,16`.

Every subprocess receives `BORSUK_BENCH_LEAF_CAPABILITY`, exact method/layout,
`BORSUK_BENCH_QUERIES=100`, `BORSUK_BENCH_READ_ONLY=1`, and either bounded caps
`4/24` or explicit uncapped `0/0`. Write `experiment.json`, command text, stdout,
stderr, result CSVs, and `resources.csv` under a unique dated run directory.

- [ ] **Step 5: Correct the legacy seven-method shell runner**

Pass `BORSUK_BENCH_LEAF_CAPABILITY=graph-enabled` when the shell runner builds
the shared research index, and do not set `BORSUK_BENCH_READ_ONLY=1` until after
the build invocation. Add a shell-source assertion to
`scripts/test_bench_standard_method_matrix.py`.

- [ ] **Step 6: Run orchestrator tests and dry run**

Run:

```bash
PYTHONPATH=scripts python3 -m unittest \
  scripts/test_bench_graph_promotion.py \
  scripts/test_bench_standard_method_matrix.py
python3 scripts/bench_graph_promotion.py \
  --datasets-root /tmp/borsuk-datasets \
  --output-root /tmp/graph-promotion-plan \
  --bucket s3://dry-run
```

Expected: tests pass and the manifest contains every required cell without
executing Cargo or AWS requests.

- [ ] **Step 7: Commit**

```bash
git add scripts/bench_graph_promotion.py scripts/test_bench_graph_promotion.py \
  scripts/bench_standard_method_matrix.sh scripts/test_bench_standard_method_matrix.py
git commit -m "bench: add graph default promotion matrix"
```

### Task 3: Add deterministic promotion evaluation

**Files:**
- Create: `scripts/evaluate_graph_promotion.py`
- Create: `scripts/test_evaluate_graph_promotion.py`

- [ ] **Step 1: Write failing gate tests**

Create fixtures proving that graph passes only when recall, all repetition
latencies, throughput, RSS, caps, and cache I/O satisfy the design:

```python
def test_rejects_one_slow_graph_repetition(self):
    rows = passing_rows()
    rows["graph"][2]["p95_ms"] = rows["pq-scan"][2]["p95_ms"] + 0.001
    decision = evaluate_dataset(rows, ram_budget_bytes=1 << 30)
    self.assertFalse(decision.passed)
    self.assertIn("p95", decision.reasons)

def test_rejects_disk_cached_network_io(self):
    rows = passing_rows()
    rows["graph"][0]["network_gets"] = 1
    self.assertFalse(evaluate_dataset(rows, 1 << 30).passed)
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
PYTHONPATH=scripts python3 -m unittest scripts/test_evaluate_graph_promotion.py
```

Expected: import failure.

- [ ] **Step 3: Implement the evaluator**

Implement per-dataset decisions and an overall decision:

```python
@dataclass(frozen=True)
class PromotionDecision:
    dataset: str
    passed: bool
    reasons: tuple[str, ...]

overall = "universal-graph" if all_public_pass else (
    "adaptive" if any_public_pass else "keep-pq-scan"
)
```

Reject missing repetitions, fewer than 100 public queries, mixed source SHA,
recall loss greater than 0.001, any slower p95/p99 repetition, lower capped
throughput, RSS over budget or 1.2× graph-free RSS, nonzero disk-cached backing
I/O, missing caps, or multi-second graph decode/validation outliers. Emit
`promotion-decision.csv` and `promotion-decision.json` with every reason.

- [ ] **Step 4: Run evaluator tests**

Run:

```bash
PYTHONPATH=scripts python3 -m unittest scripts/test_evaluate_graph_promotion.py
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add scripts/evaluate_graph_promotion.py scripts/test_evaluate_graph_promotion.py
git commit -m "research: enforce graph promotion gates"
```

### Task 4: Make stale publication results fail validation

**Files:**
- Modify: `scripts/validate_research_docs.py`
- Modify: `scripts/test_validate_research_docs.py`
- Modify: `scripts/test_docs_web.mjs`

- [ ] **Step 1: Add failing provenance/staleness tests**

Require a `current-results.csv` manifest with:

```text
claim_id,dataset,method,index_capability,cache_state,queries,source_artifact,status
```

Test that current claims fail when their artifact is missing, status is not
`current`, query count is below the public full-query count, or website prose
contains a numeric `data-claim-id` absent from the manifest. Add a regression
fixture for the stale GloVe “28 MB / ~2 s / 256 candidates / 0.98 recall” card.

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
PYTHONPATH=scripts python3 -m unittest scripts/test_validate_research_docs.py
node scripts/test_docs_web.mjs
```

Expected: new provenance tests fail.

- [ ] **Step 3: Implement manifest-backed validation**

Load `docs/web/assets/benchmarks/current-results.csv`, resolve every
`source_artifact` relative to the repository, and require every current result
table/card in `README.md`, `docs/research/*.md`, and `docs/web/*.html` to carry a
matching claim id. Historical material must use `status=historical` and render a
visible “historical” label.

- [ ] **Step 4: Run validation tests**

Run the commands from Step 2. Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add scripts/validate_research_docs.py scripts/test_validate_research_docs.py \
  scripts/test_docs_web.mjs
git commit -m "docs: reject stale benchmark claims"
```

### Task 5: Execute public-corpus AWS matrix

**Files:**
- Create: `docs/web/assets/benchmarks/raw/2026-07-21/graph-promotion/**`
- Create: `docs/web/assets/benchmarks/aws-graph-promotion-public-2026-07-21.csv`

- [ ] **Step 1: Start and verify the benchmark host**

Run:

```bash
aws sso login --profile causality
aws --profile causality --region eu-central-1 ec2 start-instances \
  --instance-ids i-0e73bacb470807838
aws --profile causality --region eu-central-1 ec2 wait instance-running \
  --instance-ids i-0e73bacb470807838
```

Record the fresh public IP and verify `c7g.8xlarge`, AArch64, vCPU count, RAM,
disk, and the instance role.

- [ ] **Step 2: Sync and build exact source**

Archive the working source without build outputs, transfer it to
`/home/ec2-user/borsuk-current`, and run:

```bash
cargo build --locked --release -p borsuk --example production_bench
cargo build --locked --release -p borsuk --example benchmark_report
```

Record the local commit plus a SHA-256 of the transferred source archive.

- [ ] **Step 3: Dry-run and execute the six-corpus matrix**

On the host:

```bash
python3 scripts/bench_graph_promotion.py \
  --datasets-root /home/ec2-user/borsuk-datasets \
  --output-root /home/ec2-user/graph-promotion-2026-07-21 \
  --bucket s3://borsuk-bench-453182569524-euc1 \
  --repetitions 3 --source-sha "$SOURCE_SHA"

python3 scripts/bench_graph_promotion.py \
  --datasets-root /home/ec2-user/borsuk-datasets \
  --output-root /home/ec2-user/graph-promotion-2026-07-21 \
  --bucket s3://borsuk-bench-453182569524-euc1 \
  --repetitions 3 --source-sha "$SOURCE_SHA" \
  --execute --build-indexes --resume
```

Expected: graph-free and graph-enabled prefixes exist for all six corpora; every
selected public point has three production, three memory-preloaded, capped
concurrency, and uncapped research results with resource CSVs.

- [ ] **Step 4: Evaluate and copy immutable evidence**

Run the promotion evaluator on the host, then copy the raw tree and consolidated
CSV to `docs/web/assets/benchmarks/raw/2026-07-21/graph-promotion/` and
`aws-graph-promotion-public-2026-07-21.csv`. Verify file counts and checksums.

- [ ] **Step 5: Commit public evidence**

```bash
git add docs/web/assets/benchmarks/raw/2026-07-21/graph-promotion \
  docs/web/assets/benchmarks/aws-graph-promotion-public-2026-07-21.csv
git commit -m "research: add six-corpus graph promotion evidence"
```

### Task 6: Execute controlled AWS matrix

**Files:**
- Create: `docs/web/assets/benchmarks/raw/2026-07-21/graph-promotion-controlled/**`
- Create: `docs/web/assets/benchmarks/aws-graph-promotion-controlled-2026-07-21.csv`

- [ ] **Step 1: Run controlled datasets on the same host**

Use deterministic record-count and dimension sweeps:

```bash
python3 scripts/benchmark_with_resources.py \
  --output /home/ec2-user/graph-promotion-controlled/resources.csv \
  -- target/release/examples/benchmark_report \
  --synthetic-records-list 10000,100000,1000000 \
  --dimensions 64 \
  --segment-max-vectors 256 \
  --max-segments 8 \
  --routing-page-overfetch 8 \
  --max-candidates-per-segment 64 \
  --queries 100 \
  --parallelism 1,2,4,8,16 \
  --artifacts-dir /home/ec2-user/graph-promotion-controlled/d64
```

Repeat at dimensions 96, 256, 784, and 960. Run sklearn-digits through the same
binary. Keep all seven leaf modes in controlled artifacts, but use graph versus
`pq-scan` for the promotion analysis.

- [ ] **Step 2: Verify controlled artifacts**

Require sequential and parallel rows for digits, uniform, clustered, and
adversarial datasets at every requested dimension/scale, with recall and RSS.

- [ ] **Step 3: Copy and commit controlled evidence**

Copy raw data and consolidated CSV to the paths above, then:

```bash
git add docs/web/assets/benchmarks/raw/2026-07-21/graph-promotion-controlled \
  docs/web/assets/benchmarks/aws-graph-promotion-controlled-2026-07-21.csv
git commit -m "research: add controlled graph promotion evidence"
```

### Task 7: Render evidence and decide the default

**Files:**
- Modify: `scripts/render_recall_latency_charts.py`
- Modify: `scripts/render_resource_charts.py`
- Create: `docs/web/assets/benchmarks/current-results.csv`
- Create: `docs/web/assets/charts/graph-promotion/**`
- Modify: `docs/research/README.md`
- Modify: `docs/research/methods.md`
- Modify: `docs/research/standard-datasets.md`
- Modify: `docs/research/reproducibility.md`
- Modify: `docs/publication-notes.md`
- Modify: `docs/web/research.html`
- Modify if and only if gates pass: `crates/borsuk/src/index.rs`, bindings, CLI, API/architecture/default docs and tests

- [ ] **Step 1: Render complete curves and resource plots**

Generate per-dataset graph/pq recall-latency plots plus CPU/RSS/disk/cache/S3-I/O
plots for every selected production and research-ceiling run. Labels include
method, cache state, query count, and whether the row is current or historical.

- [ ] **Step 2: Generate the current-results manifest**

Every result shown in current docs receives a stable claim id and dated source
artifact. Move superseded GloVe cards/plots to an explicitly historical section
or remove them from the rendered website.

- [ ] **Step 3: Apply the evaluator decision**

- `universal-graph`: change default creation capability and default leaf mode to
  graph, preserving explicit `pq-scan` override and legacy compatibility.
- `adaptive`: implement a deterministic planner whose selected mode and reason
  are visible in `explain`/search reports; retain graph-free fallback.
- `keep-pq-scan`: make no default code change and document each failing graph
  gate.

Write failing default/planner tests before changing code, then run focused Rust,
Python, Node, and CLI tests.

- [ ] **Step 4: Update publication and website prose**

Publish all wins and failures, cache-state semantics, three-run ranges, resource
envelopes, footprint/build costs, and the promotion decision. Do not merge
memory-preloaded graph rows with disk-cached production rows.

- [ ] **Step 5: Commit generated evidence and decision**

```bash
git add docs scripts crates packages python
git commit -m "research: publish graph default promotion decision"
```

### Task 8: Final verification, archive, and shutdown

**Files:**
- Verify all changed code, tests, artifacts, documentation, and web pages.

- [ ] **Step 1: Run complete local validation**

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets
PYTHONPATH=scripts python3 -m unittest discover -s scripts -p 'test_*.py'
python3 scripts/validate_research_docs.py
node scripts/test_docs_web.mjs
git diff --check
```

Build and test the actual Python wheel and Node native package using the commands
in `README.md`.

- [ ] **Step 2: Browser-check documentation**

Verify desktop and mobile research/default pages, chart readability, zero
horizontal overflow, zero broken images, and zero console errors. Confirm the
visible GloVe row matches `current-results.csv`.

- [ ] **Step 3: Archive immutable AWS evidence**

Tar only raw result/log/profile directories, compute SHA-256, upload to:

```text
s3://borsuk-bench-453182569524-euc1/research-archive/2026-07-21/graph-default-promotion-raw.tar.gz
```

Verify object length, ETag, timestamp, and local artifact checksums.

- [ ] **Step 4: Stop the benchmark instance**

```bash
aws --profile causality --region eu-central-1 ec2 stop-instances \
  --instance-ids i-0e73bacb470807838
aws --profile causality --region eu-central-1 ec2 wait instance-stopped \
  --instance-ids i-0e73bacb470807838
```

Expected: instance state is `stopped`; no paid compute is left running.


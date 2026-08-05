# Realistic Ingest and Read Qualification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Qualify and improve BORSUK until durable writes scale from sub-200 ms single-record acknowledgement to competitor-class batched throughput while real 768D and 1536D corpora preserve high recall and low read latency.

**Architecture:** Keep the immutable object-store WAL as the acknowledgement boundary and vary only explicit batching and independent commit lanes. Use official VectorDBBench Cohere 1M/768D and DBpedia OpenAI 1M/1536D artifacts with shipped or recomputed pinned ground truth; build a fresh immutable base per repetition, preserve every raw sample and resource trace, and reject campaigns before reading measurements unless terminal markers and structural validation pass.

**Tech Stack:** Rust, `BorsukIndex`, `GroupCommitWriter`, Arrow/Parquet, Python fail-closed validators, AWS S3 and EC2 via profile `causality`, systemd, official VectorDBBench datasets.

## Global Constraints

- Do not create pull requests; commit verified slices and fast-forward push directly to `origin/main` without force.
- Use AWS profile `causality`; verify no competing workload before launch.
- Never inspect an incomplete cell's measurement CSV. Monitor only phase markers, process/service health, and resource telemetry until that cell is terminal.
- Preserve frozen source archives, manifests, raw artifacts, dataset descriptors, SHA-256 identities, and resource traces.
- Use five repetitions for a publication campaign; three repetitions are sufficient only for a labelled architecture qualification.
- Report single-record latency and batched throughput as separate workload points. Do not extrapolate either one from the other.
- Compare external products only with commercial first-party or paper numbers, or with honest paired reproductions under disclosed equivalent conditions.
- Production gates are write p95 below 200 ms, recall@10 at least 0.95 at the selected serving point, and read p95 below 200 ms. The initial durable bulk-throughput parity target is 10,000 vectors/s at 768D.

---

### Task 1: Make durable batch size and mutation count explicit

**Files:**
- Modify: `crates/borsuk/examples/production_bench.rs`
- Modify: `scripts/validate_benchmark_artifacts.py`
- Test: `scripts/test_validate_benchmark_artifacts.py`
- Modify: `docs/research/market-benchmark-matrix.md`

**Interfaces:**
- Consumes: `BORSUK_BENCH_WRITE_BATCH_SIZE`, `BORSUK_BENCH_WRITE_OPS`.
- Produces: `configured_batch_records` in `bench_write_costs.csv` and `bench_lifecycle.csv`, plus unchanged raw `bench_write_samples.csv` rows.

- [x] **Step 1: Add RED tests for explicit batch length and fail-closed mutation count**

  Assert `write_batch_len(5000, 4900, 1024) == 100`, default one-million-row mutations equal 50,000, an explicit 3,200 remains 3,200, and an override above the dataset size fails.

- [x] **Step 2: Run the RED test**

  Run: `cargo test -p borsuk --example production_bench lifecycle_write_batch_size_is_an_explicit_experiment_factor --no-run`

  Expected: compile failure because `write_batch_len` does not exist.

- [x] **Step 3: Implement and record the two experiment factors**

  Parse positive `BORSUK_BENCH_WRITE_BATCH_SIZE`, optional positive `BORSUK_BENCH_WRITE_OPS`, use the batch size for mutation source decoding and insert/upsert/delete calls, and reject a mutation count above the pinned source size.

- [x] **Step 4: Require the batch factor in structural validation**

  Add `configured_batch_records` to the required aggregate write and lifecycle columns and update the validator fixture.

- [x] **Step 5: Verify**

  Run: `cargo test -p borsuk --example production_bench`

  Run: `cargo clippy -p borsuk --example production_bench --all-features -- -D warnings`

  Run: `PYTHONPATH=scripts python3 -m unittest scripts.test_validate_benchmark_artifacts scripts.test_validate_research_docs`

- [x] **Step 6: Commit**

  Commit: `464a8b9 bench: expose durable write batch factors`

### Task 2: Add a bounded insert-only qualification phase

**Files:**
- Modify: `crates/borsuk/examples/production_bench.rs`
- Modify: `scripts/validate_benchmark_artifacts.py`
- Test: `scripts/test_validate_benchmark_artifacts.py`
- Test: `scripts/test_bench_production_lifecycle_aws.py`

**Interfaces:**
- Consumes: `BORSUK_BENCH_INSERT_ONLY=1`, a previously built immutable base URI, `BORSUK_BENCH_WRITE_BATCH_SIZE`, and `BORSUK_BENCH_WRITE_OPS`.
- Produces: `bench_write_costs.csv`, `bench_write_samples.csv`, `INSERT_VISIBILITY_COMPLETE`, and no fabricated fully-indexed or consolidation fields.

- [x] **Step 1: Write a RED phase-selection test**

  Add `validate_insert_only(insert_only, build_only, read_only)` and assert insert-only rejects build-only/read-only combinations but accepts an ordinary mutable run.

- [x] **Step 2: Run the RED test**

  Run: `cargo test -p borsuk --example production_bench insert_only_is_a_distinct_mutation_phase --no-run`

  Expected: unresolved import/function failure for `validate_insert_only`.

- [x] **Step 3: Extract aggregate/sample artifact writing**

  Move the existing write-cost and write-sample serialization into `write_cost_artifacts(config: &ResolvedConfig, rows: &[WriteRow]) -> BenchResult<()>` without changing headers or numeric formulas.

- [x] **Step 4: Implement insert-only execution**

  After build-only handling and before recall/query phases, open the base, call `measure_inserts`, validate all sampled inserted IDs with one `get_records` call, write only the insert row/raw samples, create `INSERT_VISIBILITY_COMPLETE`, and return.

- [x] **Step 5: Verify**

  Run: `cargo test -p borsuk --example production_bench`

  Run: `cargo clippy -p borsuk --example production_bench --all-features -- -D warnings`

- [x] **Step 6: Commit and fast-forward push**

  Commit message: `bench: isolate durable insert qualification`

### Task 3: Add a fail-closed realistic durable-write campaign

**Files:**
- Create: `docs/research/realistic-durable-write-campaign.json`
- Create: `scripts/bench_realistic_durable_write.sh`
- Create: `scripts/validate_realistic_durable_write.py`
- Create: `scripts/test_validate_realistic_durable_write.py`
- Create: `scripts/test_bench_realistic_durable_write.py`

**Interfaces:**
- Consumes: frozen source SHA-256, dataset descriptor SHA-256, fresh S3 base/result prefixes, batch sizes `1,32,128,1024`, and repetitions.
- Produces: per-cell phase markers, raw write samples, resource telemetry, aggregate summaries, `REALISTIC_DURABLE_WRITE_COMPLETE`, or `REALISTIC_DURABLE_WRITE_FAILED`.

- [x] **Step 1: Write RED validator fixtures**

  Cover missing root completion, a root failure marker, source/dataset/manifest hash mismatch, missing raw samples, unreconciled batch counts, batch records above the configured factor, write p95 at or above 200 ms, visibility below 1.0, and a missing resource exit status.

- [x] **Step 2: Run RED tests**

  Run: `PYTHONPATH=scripts python3 -m unittest scripts.test_validate_realistic_durable_write`

  Expected: import failure for `validate_realistic_durable_write`.

- [x] **Step 3: Implement the validator**

  Require each planned `(dataset, repetition, batch_records)` cell, exact manifest copy, exact hashes, at least 100 raw durable batches, p95 below 200 ms, all sampled inserts visible, reconciled object-store request totals, time-aligned resource rows, and successful process exit.

- [x] **Step 4: Implement the runner**

  Refuse reused outputs and indexes; create phase markers only after successful phases; wrap each cell with `benchmark_with_resources.py`; upload raw artifacts after the cell becomes terminal; install a trap that writes/uploads the root failure marker.

- [x] **Step 5: Run local structural smoke**

  Use a generated 64-row/8D local Parquet fixture, two batch factors, and one repetition. Run the validator over the terminal smoke directory.

- [x] **Step 6: Commit and fast-forward push**

  Commit: `6f5ba5d bench: preregister realistic durable writes`

### Task 4: Pin real 768D and 1536D datasets

**Files:**
- Modify: `scripts/fetch_vdbbench_dataset.py`
- Test: `scripts/test_fetch_vdbbench_dataset.py`
- Modify: `docs/research/reproducibility.md`

**Interfaces:**
- Consumes: official unshuffled VectorDBBench Parquet objects and the public DBpedia OpenAI source.
- Produces: `dataset.json`, `meta.json`, original Parquet files, per-file bytes/SHA-256, aggregate source SHA-256, row/dimension/query/neighbor validation.

- [x] **Step 1: Finish and validate Cohere Medium 1M/768D**

  Require 1,000,000 train rows, 768 float32 dimensions, aligned test/neighbor counts, at least ten ground-truth neighbours, original filenames, and immutable hashes.

- [ ] **Step 2: Add a RED DBpedia descriptor test**

  Assert the alias identifies 1,000,000 source rows, 990,000 indexed rows after the seeded 10,000-query split, 1,536 dimensions, cosine distance, source URL, license, and exact preparation parameters.

- [ ] **Step 3: Implement checksum-pinned DBpedia preparation**

  Preserve source embeddings, seed and split identities, recompute exact ground truth only for the pinned indexed/query split, and record preparation CPU/time separately from BORSUK build/query timing.

- [ ] **Step 4: Verify and commit**

  Run: `PYTHONPATH=scripts python3 -m unittest scripts.test_fetch_vdbbench_dataset scripts.test_market_benchmark_matrix`

  Commit message: `data: pin realistic embedding workloads`

### Task 5: Qualify commit-lane scalability without weakening durability

**Files:**
- Modify: `crates/borsuk/examples/group_commit_bench.rs`
- Create: `docs/research/realistic-group-commit-campaign.json`
- Modify: `scripts/bench_group_commit_scalability.sh`
- Test: `scripts/test_bench_group_commit_scalability_runner.py`

**Interfaces:**
- Consumes: Cohere train Parquet, worker lanes `1,2,4,8`, writers `1,8,32`, pipeline depth `4`, 768 dimensions, and real vectors.
- Produces: per-ticket durable latency, acknowledgement and drain-inclusive records/s, MiB/s, group size, lane, request counts, visibility, and sampled resource telemetry.

- [x] **Step 1: Write RED tests for dataset-backed tickets and lane factors**

  Require the runner to reject random-vector production mode, require the dataset descriptor/hash, and enumerate worker lanes independently of writer count.

- [x] **Step 2: Implement bounded Parquet vector streaming**

  Decode vectors before starting each ticket's latency clock, retain original vectors only until visibility validation, and never include dataset I/O in durable acknowledgement latency.

- [x] **Step 3: Preserve safety invariants**

  Run prepare-failure, crash-recovery, same-ID last-write-wins, drain barrier, and request-reconciliation gates for every worker-lane factor.

- [ ] **Step 4: Verify and commit**

  Run: `cargo test -p borsuk --example group_commit_bench`

  Run: `cargo test -p borsuk --test group_commit`

  Run: `cargo clippy -p borsuk --all-targets --all-features -- -D warnings`

  Commit message: `bench: qualify realistic commit lanes`

  The initial three-repetition burst protocol was rejected after a GPT-5.6 Sol
  adversarial review: it excluded final drain from throughput, changed record
  IDs across lane treatments, omitted the production-default eight-lane factor,
  and trusted derived summaries. The replacement preregistration uses five
  repetitions, identical record IDs, `1/2/4/8` lane factors, 1,000 operations
  per writer to cross repeated materialization boundaries, raw-derived
  validation, and a 10,000 vectors/s drain-inclusive gate for 32-writer cells.
  Its synthetic-base inserted-ID check remains visibility evidence only;
  production corpus recall/read latency remains Task 7. A factor-spanning
  integration gate now verifies acknowledged reopen visibility, sequential
  last-write-wins, and drain/reopen correctness for `1/2/4/8` worker lanes.

### Task 6: Run isolated AWS qualifications and react to terminal evidence

**Files:**
- Modify: `docs/research/group-commit-scalability-attempt-ledger.md`
- Create: `docs/research/realistic-durable-write-attempt-ledger.md`
- Modify: `docs/research/market-benchmark-matrix.md`

**Interfaces:**
- Consumes: committed source revision, frozen manifests, dataset hashes, fresh prefixes, idle Causality host.
- Produces: immutable terminal artifacts and exact failure/root-cause records.

- [x] **Step 1: Verify launch preconditions**

  Fetch `origin/main`; verify exact source archive SHA-256 and manifest SHA-256 remotely; require empty result/index prefixes, no benchmark process, at least 32 GiB available memory, and enough disk for dataset plus peak scratch.

- [x] **Step 2: Launch sustained architecture qualification**

  Run Cohere 1M/768D batch and commit-lane matrices from a committed source. Preserve raw artifacts and resource telemetry.

- [ ] **Step 3: Monitor periodically**

  Use an attached 15-minute shell sleep, observe its completion, then check markers, systemd/process health, memory, disk, and newly terminal cells. Never read an active cell's CSV.

- [ ] **Step 4: Validate terminal artifacts before measurements**

  Run repository fail-closed validators. On failure, record exact markers and diagnose from logs/resource/request telemetry; on success, inspect raw/aggregate results and report uncertainty.

- [ ] **Step 5: Iterate causally**

  Change one architecture factor per campaign. Any storage/layout change gets a new format marker and fresh base indexes; old artifacts remain historical evidence only.

  v30 failed only the explicit write-p95 gate at the first one-writer cell.
  The next architecture replaces the two-serial-PUT lane acknowledgement with
  one conditional HEAD containing the complete inline block. Background spill
  externalizes inline blocks before a fenced HEAD replacement. Pure LIST
  discovery is rejected because it needs a larger epoch-sealing and zombie-
  fencing protocol; acknowledgement-before-asynchronous-HEAD is the same LIST
  design in disguise.

  Format v26 now makes that boundary explicit. Index creation writes every
  empty ownership-lane HEAD before publishing `CURRENT`, and readers fail
  closed if any HEAD is later missing. Each foreground group acknowledges one
  inline HEAD CAS. At four inline blocks or 8 MiB, the owning worker returns
  the receipt first, uploads checksum-addressed blocks, then installs external
  descriptors with one fenced HEAD CAS; a failed upload leaves the inline copy
  authoritative and must be retried before that lane accepts another append.
  Raw group samples now record exact acknowledgement HEAD bytes, and the
  validator reconciles total and maximum bytes from those samples. A local
  structural smoke generated and independently validated a complete artifact
  tree under this schema. AWS remains blocked on the full repository gate and
  a committed source archive.

- [ ] **Step 6: Run five repetitions at the selected revision**

  Freeze defaults only after write latency/throughput and realistic read recall/latency gates pass in the architecture qualification.

### Task 7: Production-read qualification and completion audit

**Files:**
- Modify: `docs/research/market-benchmark-matrix.md`
- Modify: `docs/research/publication-notes.md`
- Modify: `docs/research/production-hardening-audit-2026-07-28.md`

**Interfaces:**
- Consumes: frozen Cohere/DBpedia indexes, shipped/recomputed ground truth, uncached/disk-cached/mixed cache profiles, concurrency `1,2,4,8,16`.
- Produces: recall@10 curves, p50/p95/p99, QPS, GETs/bytes, memory, index size, startup, drain/indexing costs, and defensible production claims.

- [ ] **Step 1: Run recall/candidate curves**

  Select a serving point only where recall@10 is at least 0.95; retain the full curve and exact control.

- [ ] **Step 2: Run cache and concurrency profiles**

  Measure uncached, disk-cached, and 0/25/50/75/100 mixed coverage with independent cache directories and raw per-query samples.

- [ ] **Step 3: Audit every objective requirement**

  Require: sub-200 ms write p95 for the declared single/batched points, at least 10,000 vectors/s durable 768D bulk ingest or an explicitly documented unresolved gap, sub-200 ms read p95 at the selected cache/concurrency scope, recall@10 at least 0.95, bounded memory, recoverability, and complete public API/documentation gates.

- [ ] **Step 4: Commit and fast-forward push terminal history**

  Commit message: `docs: record realistic production qualification`

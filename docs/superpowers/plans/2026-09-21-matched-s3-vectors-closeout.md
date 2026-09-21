# Matched S3 Vectors Closeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Measure Amazon S3 Vectors once on the exact frozen ReLAION-1M development workload used by V108, independently recompute the result, delete the temporary service resources, and publish a tagged architecture closeout.

**Architecture:** A bounded-memory Python harness streams the authenticated source Parquet file in 500-vector `PutVectors` batches, then queries all 1,000 authenticated development vectors twice at `topK=100`. It writes per-query evidence as Parquet and a canonical metadata/result receipt as JSON; a separate reducer authenticates the Parquet evidence and recomputes recall and latency aggregates. One c7i.8xlarge Spot instance runs the immutable attempt and always deletes the vector index/bucket and terminates.

**Tech Stack:** Python 3.12, boto3 S3 Vectors, PyArrow/Parquet, unittest, AWS EC2 Spot, S3 terminal receipts.

**Spec:** Active native goal `72-hour ship-or-closeout phase` plus `docs/research/algorithm-first-page-layout-ledger.md` V108 authority.

## Global Constraints

- Use exactly the frozen ReLAION-1M source (1,000,000 × 768 float32), 1,000 development queries, exact GT100, Euclidean metric, and `topK=100`.
- Label the first fresh-index pass and repeated pass precisely; S3 Vectors cache state is vendor-managed and opaque.
- Preserve per-query R@10, R@100, latency, pagination, response bytes, and status in Parquet.
- Run one attempt on Causality AWS Spot; never restart an interrupted/failed measurement cell.
- Delete the temporary vector index and vector bucket on every terminal path; terminate compute immediately.
- Treat Turbopuffer as access-blocked unless an API credential/tenant is actually present.
- Do not represent published vendor numbers as matched evidence.

## Review Focus

- Feature IDs, rather than Parquet row offsets, must be used as S3 Vectors keys and truth identities.
- The registered `topK=100` call must return exactly one complete 100-result
  response; a continuation token is an API-contract failure because the pinned
  boto3 surface has no continuation input.
- Source reading must stay bounded to one 500-row request batch.
- R@10 must compare the first ten returned keys with the first ten truth IDs; R@100 compares the full hundred.
- Cleanup must run after producer failure as well as success and must be recorded in terminal evidence.

---

### Task 1: Matched Parquet Harness

**Files:**
- Create: `scripts/benchmark_s3_vectors_parquet.py`
- Create: `scripts/test_benchmark_s3_vectors_parquet.py`

**Interfaces:**
- Consumes: strict source/query/truth Parquet schemas already enforced by `scripts.v97_row_width_screen.load_screen_inputs`.
- Produces: `run_matched_benchmark(config, client) -> BenchmarkResult`, `samples.parquet`, and canonical `result.json`.

- [ ] **Step 1: Write the failing tests**

Create synthetic source/query/truth Parquet fixtures whose feature IDs differ from row offsets. Require streaming batches of at most 500, one complete top-100 ranked response, literal R@10/R@100 values, two exact pass labels, response byte accounting, and index/bucket cleanup.

- [ ] **Step 2: Verify RED**

Run `uv run --offline --python 3.12 --with-requirements scripts/requirements-format-bench.txt python -m unittest scripts.test_benchmark_s3_vectors_parquet` and require failure only because `scripts.benchmark_s3_vectors_parquet` is absent.

- [ ] **Step 3: Implement the minimal harness**

Use frozen dataclasses for identities/config/results; exact PyArrow schemas; `ParquetFile.iter_batches(batch_size=500)`; a bounded five-worker upload pool; one complete top-100 `QueryVectors` response; canonical JSON via `json.dumps(asdict(...), sort_keys=True, separators=(",", ":"))`; and Parquet per-query samples.

- [ ] **Step 4: Verify GREEN and static checks**

Run the exact unittest command, Ruff on the two files, `py_compile`, and `git diff --check`.

- [ ] **Step 5: Commit and push**

Commit the plan, test, and harness with the configured operator identity; verify fast-forward ancestry before pushing to `origin/main`.

### Task 2: One Immutable Spot Cell

**Files:**
- Create: `scripts/run_matched_s3_vectors_1m_remote.sh`
- Create: `scripts/launch_matched_s3_vectors_1m_spot.py`
- Create: `scripts/test_launch_matched_s3_vectors_1m_spot.py`

**Interfaces:**
- Consumes: the Task 1 CLI, exact source commit/archive, three frozen input identities, one registered output prefix, and the `borsuk-bench-profile` S3 Vectors permissions.
- Produces: create-only reservation/launch/terminal receipts and evidence identities for result, samples, resources, and worker log.

- [ ] **Step 1: Write launcher REDs**

Require one one-time Spot instance across registered eu-central-1 zones, 48-GiB virtual-memory cap, PSI/swap/interruption watcher, exact input hashes and lengths, one-attempt prefix reservation, service-resource cleanup evidence, and unconditional EC2 termination.

- [ ] **Step 2: Verify RED**

Run `python3 -m unittest scripts.test_launch_matched_s3_vectors_1m_spot` and require missing launcher/runner symbols only.

- [ ] **Step 3: Implement and verify GREEN**

Implement the minimum immutable launcher/runner, run its focused unittest, Ruff, `py_compile`, shell syntax check, and `git diff --check`.

- [ ] **Step 4: Commit, push, and execute once**

Archive the clean source revision, reserve the output prefix, launch exactly one Spot instance, monitor only terminal/infrastructure health, and terminate at terminal or registered stop.

### Task 3: Independent Reduction and Closeout

**Files:**
- Modify: `docs/research/algorithm-first-page-layout-ledger.md`

**Interfaces:**
- Consumes: terminal-authenticated `samples.parquet`, result/resources receipts, official S3 Vectors pricing basis, and the Turbopuffer access audit.
- Produces: independent recall/latency recomputation, matched comparison disposition, and an annotated architecture-closeout tag.

- [ ] **Step 1: Independently recompute evidence**

Download only terminal-bound evidence, authenticate every length/digest, independently recompute pass counts, R@10/R@100, latency p50/p95/p99, response bytes, and upload throughput from Parquet primitives, and compare exact values with the producer receipt.

- [ ] **Step 2: Record matched and blocked evidence**

Record the measured S3 Vectors result with dataset/split/metric/k/cache caveat/cost basis. Record Turbopuffer as access-blocked with the exact credential/tenant checks; retain its published figures as context-only.

- [ ] **Step 3: Validate and publish closeout**

Run `python3 scripts/validate_research_docs.py` and `git diff --check`, commit/push the ledger, create and push one annotated architecture-closeout tag, and verify HEAD, `origin/main`, `ls-remote`, clean worktree, no service resources, and no EC2 instance remain.

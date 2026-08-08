# Adaptive Global Rerank Planning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep uncached global-PQ exact reranking below the production read-latency gate by replacing dense tiny-range S3 fanout with one cost-justified bounded range GET without reducing the exact candidate budget or recall.

**Architecture:** Retain standard Arrow global-PQ bundles and the existing 4 MiB physical range cap. Build the ordinary 64 KiB sparse plan first; widen to the full shortlist envelope only when that envelope fits under 4 MiB and its extra transfer costs no more than 128 KiB per avoided GET. Large or genuinely sparse cells keep the existing policy.

**Tech Stack:** Rust, `object_store`, Arrow IPC, repository storage request telemetry, Cohere 768D AWS qualification.

## Global Constraints

- Use AWS profile `causality` and S3-only coordination/storage.
- Preserve standard Arrow/Parquet durable artifacts; do not introduce a custom vector container.
- Preserve the 16-candidate exact rerank and inserted-ID recall@10 gate.
- Never inspect incomplete campaign measurement CSVs; validate terminal artifacts first.
- Do not create pull requests or force-push. Push verified fast-forward commits directly to `origin/main`.
- A production pass requires write p95 below 200 ms, read p95 below 200 ms, recall@10 at least 1.0 in the frozen gate, and the preregistered throughput thresholds.

---

### Task 1: Preserve the terminal causal evidence

**Files:**
- Modify: `docs/research/group-commit-scalability-attempt-ledger.md`

**Interfaces:**
- Consumes: terminal run `20260808T054400Z-v4-622ab0c` and repository validator output.
- Produces: immutable v64 attempt history and the next-candidate rationale.

- [x] **Step 1: Confirm terminality without reading measurements**

Run:

```bash
aws --profile causality s3api list-objects-v2 --region eu-central-1 \
  --bucket borsuk-bench-453182569524-euc1 \
  --prefix research/group-commit-scalability/20260808T054400Z-v4-622ab0c/results/ \
  --query 'Contents[?contains(Key, `COMPLETE`) || contains(Key, `FAILED`)].Key'
```

Expected: root `GROUP_COMMIT_SCALABILITY_FAILED`, completed `w1`, and terminal `w8` with only `PRODUCTION_READ_P95_FAILED`.

- [x] **Step 2: Run fail-closed validation before measurement inspection**

Run the root validator and terminal-cell validator for `c2000/r01/l1/w1` and `c2000/r01/l1/w8`.

Expected: the root rejects the incomplete campaign; both terminal cells reject the root failure marker.

- [x] **Step 3: Establish the causal request shape**

Record that `w8` preserved 128,000/128,000 records, recall@10 1.0, 84.498 ms write p95, 6,570.980 records/s, and 138.087 ms active-tail p95, while post-drain p95 was 277.243 ms. Preserve that the terminal delta descriptor contains 128,000 vectors, 256 cells, one chunk per cell, and nine Arrow bundles; one probed cell reranks 16 scattered 768D exact rows.

### Task 2: Add adaptive bounded rerank coalescing with TDD

**Files:**
- Modify: `crates/borsuk/src/storage.rs`
- Test: `crates/borsuk/src/storage.rs` module `tests`

**Interfaces:**
- Consumes: `plan_bounded_ranges(ranges, max_gap, SIDECAR_MAX_PHYSICAL_RANGE_BYTES)`.
- Produces: `global_rerank_coalesce_bytes(ranges: &[Range<u64>]) -> u64`, used by `Storage::read_global_rerank_ranges`.

- [x] **Step 1: Write the failing dense-shortlist test**

Create sixteen 3,072-byte ranges at 96 KiB strides inside a 1.44 MiB object. Assert that `read_global_rerank_ranges` returns every requested row, performs one GET, and reports the bounded envelope bytes.

- [x] **Step 2: Verify RED**

Run:

```bash
cargo test --locked -p borsuk --lib \
  storage::tests::global_rerank_ranges_fold_a_dense_768d_shortlist_into_one_bounded_get -- --exact
```

Expected before implementation: FAIL with 16 GETs observed instead of 1.

- [x] **Step 3: Implement the minimal adaptive policy**

Compute the existing sparse plan. Return the 4 MiB gap only when the complete envelope is at most 4 MiB and:

```rust
envelope_bytes - sparse_physical_bytes
    <= (sparse_gets - 1) * 128 * 1024
```

Otherwise return the existing 64 KiB coalescing gap.

- [x] **Step 4: Verify GREEN and the sparse counterexample**

Run the dense shortlist, sparse half-MiB gap, and 32-request concurrency tests.

Expected: all three pass; the dense case uses one GET and the two-row sparse case remains two GETs.

### Task 3: Verify, deliver, and rerun the frozen AWS gate

**Files:**
- Modify: `docs/research/group-commit-scalability-attempt-ledger.md`
- Modify: `docs/superpowers/plans/2026-08-08-adaptive-global-rerank-planning.md`

**Interfaces:**
- Consumes: the adaptive storage planner and frozen realistic campaign manifest.
- Produces: one clean fast-forward commit on `origin/main` and a new immutable AWS attempt prefix.

- [x] **Step 1: Run affected suites**

Run storage unit tests, global-PQ unit tests, the group-commit integration suite, formatting, and `git diff --check`.

Expected: 42 storage tests, 39 global-PQ tests, and 43 group-commit tests pass.

- [x] **Step 2: Run full repository assurance**

Run strict Clippy, the full locked Rust workspace test gate, pinned Python tests, and repository policy checks once.

Expected: no warnings, test failures, or policy failures.

- [x] **Step 3: Commit and validate a structural smoke from the exact revision**

Commit the coherent slice, run the bulk structural group-commit smoke from that exact `HEAD`, and validate its terminal artifacts with `validate_group_commit_scalability.py`.

Expected: smoke root and cell completion markers plus a passing validator.

- [x] **Step 4: Fast-forward push and launch the next immutable AWS attempt**

Fetch `origin/main`, prove it is an ancestor of `HEAD`, push `HEAD:main` without force, verify a clean worktree, confirm the AWS worker is idle and healthy, then launch `scripts/launch_aws_group_commit_scalability.sh` from a fresh run ID.

Expected: the launcher records the exact Git archive SHA-256 and starts one detached campaign with no competing workload.

- [ ] **Step 5: Validate terminal AWS evidence**

Monitor only terminal markers, EC2 health, the exact `ec2-user` tmux pane, and non-measurement phase markers. At terminality, run fail-closed validators before opening CSVs.

Expected for qualification: every 2K/16K × 1/8/32-writer cell completes across five repetitions with all frozen latency, throughput, visibility, and recall gates passing.

### Task 4: Remove the dependent rerank wave for bounded Arrow cells

**Files:**
- Modify: `crates/borsuk/src/index.rs`
- Test: `crates/borsuk/src/index.rs` module `tests`

**Interfaces:**
- Consumes: `GlobalPqChunkRef` code, typed-column, and exact-vector offsets plus the existing query-local range reuse path.
- Produces: `global_pq_code_read_range(chunks: &[GlobalPqChunkRef]) -> Result<Range<usize>>`.

- [x] **Step 1: Write and verify the failing bounded-prefetch test**

Assert that a 512-row, 768D Arrow cell under 4 MiB returns the complete code-to-exact envelope, while an exact payload at the 4 MiB boundary returns only the compact code range. Before implementation the test must fail to compile because `global_pq_code_read_range` is absent.

- [x] **Step 2: Implement one bounded query-local range**

For each already-planned code group, compute the ordinary code range and the complete end of its exact buffers. Prefetch the complete span only when it is no larger than 4 MiB; otherwise retain the code-only range. Continue verifying each code slice checksum from the returned bytes and pass the retained range to exact reranking through `QueryLocalRange`.

- [x] **Step 3: Run affected correctness suites**

Run the new regression, the complete global-PQ unit subset, and `crates/borsuk/tests/group_commit.rs`.

Expected: one new regression, 40 global-PQ tests, and 43 group-commit tests pass without changing candidates or recall behavior.

- [ ] **Step 4: Run repository assurance and exact-revision smoke**

Run formatting, strict Clippy, full locked Rust workspace tests, pinned Python tests, repository policy, and the bulk structural smoke from the committed revision.

- [ ] **Step 5: Deliver and rerun AWS qualification**

Fast-forward push the verified commit to `origin/main`, launch a fresh immutable campaign after proving worker exclusivity, and apply the same terminal-marker/validator boundary before inspecting results.

# Bounded Fenced Delta Materialization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep uncached active-tail and post-drain reads below 200 ms while direct immutable ingest scales, by enforcing fixed per-stripe tail quotas and publishing complete indexed L0 deltas through a monotonically fenced materializer.

**Architecture:** Each of 64 stripes owns non-borrowable record/byte/extent quotas whose sums prove the collection hard bound without a foreground shared counter. A background materializer obtains a monotonic fencing epoch, captures exact stripe prefixes, builds production-routed multimodal L0 artifacts, and publishes them with one predecessor/fence-checked collection-root CAS. L0 compaction and query admission bound accumulated fanout; caches remain optional accelerators.

**Tech Stack:** Rust, S3 conditional writes, versioned JSON active-stripe directories/lane heads, immutable Parquet and Arrow IPC artifacts, fail-closed local/AWS validators.

## Global Constraints

- This plan begins only after the direct mutation-version plan is green and pushed.
- The two-PUT extent-plus-stripe-head acknowledgement remains unchanged below the local hard quota; maintenance adds no foreground request.
- Hard bounds decompose into fixed stripe quotas; do not claim an exact dynamic aggregate cap from stale observations.
- Maximum age is an active-maintenance SLO, not a structural guarantee after all writers exit.
- Materializer leases are efficiency hints; monotonic fencing plus root CAS provide correctness.
- Build every required modality artifact before publication and use the persisted production router/quantizer, never scalar fallback.
- Bound records, bytes, raw extent GETs, L0 segment count/bytes, CPU, RSS, and cold latency.
- Do not make cache warmth a production gate or inspect incomplete campaign CSVs.
- Persist every data artifact as stock-readable Parquet or Arrow IPC and every small control artifact as versioned JSON; no custom binary framing or packed file layouts.

---

### Task 1: Enforce exact per-stripe tail quotas

**Files:**
- Modify: `crates/borsuk/src/lane_log.rs`
- Modify: `crates/borsuk/src/group_commit.rs`
- Test: `crates/borsuk/src/lane_log.rs`
- Test: `crates/borsuk/tests/group_commit.rs`
- Test: `crates/borsuk/tests/fault_injection.rs`

**Interfaces:**
- Produces: `StripeTailBudget { max_records: 512, max_bytes: 2 MiB, max_extents: 8 }`.
- Produces: cumulative durable/materialized counters in versioned JSON head/seal state and `TailBackpressure` before extent creation.

- [ ] **Step 1: Write RED quota tests**

  Fill a stripe independently to each limit, require the last permitted extent to acknowledge, and require the next predicted extent to fail before any PUT. Prove another stripe retains its full quota. Sum all 64 quotas and assert the 32,768-record, 128-MiB, 512-extent collection maximum.

- [ ] **Step 2: Write RED recovery tests**

  Crash with a stale progress watermark, take over after fencing, reconstruct counters from immutable extents, and reject an append that would exceed the remaining exact quota. A materialized checkpoint must free only its covered prefix.

- [ ] **Step 3: Verify RED**

  Run: `rtk cargo test -p borsuk --lib lane_log::tests::tail_budget -- --nocapture`

  Run: `rtk cargo test -p borsuk --test fault_injection tail_budget -- --nocapture`

- [ ] **Step 4: Implement counters and admission**

  Persist cumulative record/byte/extent totals in Parquet extent metadata and versioned JSON head/seal controls. Compute `durable - materialized` using checked arithmetic. Admission receives the encoded extent byte length and record count, and returns a typed retryable error before sequence allocation/PUT when any limit would be crossed.

- [ ] **Step 5: Verify GREEN and commit**

  Run RED commands plus the full group-commit suite.

  Commit: `storage: enforce fixed stripe tail budgets`

### Task 2: Add monotonically fenced materializer ownership

**Files:**
- Modify: `crates/borsuk/src/maintenance.rs`
- Modify: `crates/borsuk/src/storage.rs`
- Modify: `crates/borsuk/src/index.rs`
- Create: `crates/borsuk/tests/maintenance.rs`
- Test: `crates/borsuk/tests/fault_injection.rs`

**Interfaces:**
- Produces: versioned JSON `MaterializerFence { epoch, owner, expires_at_ms, version }` and captured `StripePrefix` values.
- Produces: root publication validation of fence epoch, expected predecessor, and exact prefixes.

- [ ] **Step 1: Write RED stale-holder tests**

  Pause holder A after build, expire/take over with B, let B publish, then resume A. Require A's root CAS and every retirement/checkpoint to fail without altering B's state. Inject a stale holder deleting a replacement lease and require the replacement to survive.

- [ ] **Step 2: Verify RED**

  Run: `rtk cargo test -p borsuk --test maintenance materializer_fence -- --nocapture`

- [ ] **Step 3: Implement monotonic conditional fencing**

  Replace delete-and-create ownership with a checked object whose epoch increments by conditional update. Capture exact `(stripe, lease_epoch, sequence)` prefixes. Include fence epoch and predecessor checksum in staged root state. Only a winning root CAS authorizes checkpoint/retirement.

- [ ] **Step 4: Verify GREEN and commit**

  Run the RED command plus maintenance/fault-injection suites.

  Commit: `storage: fence background delta publication`

### Task 3: Build complete incremental production L0 deltas

**Files:**
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/src/manifest.rs`
- Modify: `crates/borsuk/src/group_commit.rs`
- Test: `crates/borsuk/tests/group_commit.rs`
- Test: `crates/borsuk/tests/named_vectors.rs`
- Test: `crates/borsuk/tests/sparse_named_vectors.rs`
- Test: `crates/borsuk/tests/text_storage.rs`
- Test: `crates/borsuk/tests/late_interaction_index.rs`

**Interfaces:**
- Produces: immutable L0 references with dense/PQ/exact/sparse/text/late artifacts, max version/digest metadata, and captured prefixes.
- Removes: per-stripe `sequence % 1024` trigger and unconditional `ScalarBounds` delta construction.

- [ ] **Step 1: Write RED trigger and single-builder tests**

  Eight processes each append fewer than 1,024 groups while aggregate records/bytes/extents cross soft pressure. Require one fenced builder to run. Losing contenders must not duplicate visible data or convert a root conflict into a write failure.

- [ ] **Step 2: Write RED modality and router tests**

  Require the L0 build to use the persisted production router/quantizer configuration and produce every modality sidecar before root publication. Fail each sidecar in turn and require no root change.

- [ ] **Step 3: Verify RED**

  Run: `rtk cargo test -p borsuk --test group_commit aggregate_tail -- --nocapture`

  Run the named/sparse/text/late integration tests filtered by `l0`.

- [ ] **Step 4: Implement incremental builds and active maintenance**

  Trigger on aggregate soft 8,192 records, 32 MiB, 32 raw extents, or active-writer age SLO 250 ms. Build only the captured unmaterialized prefix into new L0 artifacts; never rebuild older L0s in this pass. Publish all modality manifests through one fenced collection root, then checkpoint captured stripes.

- [ ] **Step 5: Verify GREEN and commit**

  Run the RED commands, then:

  `rtk cargo test -p borsuk --test group_commit -- --nocapture`

  `rtk cargo test -p borsuk --test consistency -- --nocapture`

  `rtk cargo test -p borsuk --test named_vectors -- --nocapture`

  `rtk cargo test -p borsuk --test sparse_named_vectors -- --nocapture`

  `rtk cargo test -p borsuk --test text_storage -- --nocapture`

  `rtk cargo test -p borsuk --test late_interaction_index -- --nocapture`

  Commit: `perf: materialize indexed multimodal L0 deltas`

### Task 4: Bound L0 fanout and uncached query work

**Files:**
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/src/record.rs`
- Modify: `crates/borsuk/src/observability.rs`
- Test: `crates/borsuk/tests/performance_smoke.rs`
- Test: `crates/borsuk/tests/group_commit.rs`

**Interfaces:**
- Produces: query report fields for raw records/bytes/extents and L0 segments/bytes/GETs.
- Produces: L0 compaction at 8 segments/256 MiB and observed-debt backpressure at 32 segments/1 GiB.

- [ ] **Step 1: Write RED fanout tests**

  Build many small extents/L0s under byte limits and require GET-count limits to trigger maintenance/backpressure rather than an unbounded cold query. Require a query to search every admitted branch and preserve recall; skipping a branch is failure.

- [ ] **Step 2: Write RED cold-query structural test**

  Disable decoded caches, query a bounded raw tail plus maximum normal L0 fanout, and assert reported work is within the configured records/bytes/GET limits. Keep elapsed timing diagnostic locally.

- [ ] **Step 3: Implement bounded L0 compaction and telemetry**

  Compact oldest compatible L0s incrementally using production quantizer/sidecar policies. Backpressure writers on observed hard L0 debt. Add report and storage-trace fields that reconcile physical requests and bytes without cache dependence.

- [ ] **Step 4: Verify GREEN and commit**

  Run performance smoke, group commit, consistency, and storage access trace suites.

  Commit: `perf: bound raw-tail and L0 query fanout`

### Task 5: Qualify locally and on AWS

**Files:**
- Modify: `crates/borsuk/examples/group_commit_bench.rs`
- Modify: `scripts/bench_group_commit_scalability.sh`
- Modify: `scripts/validate_group_commit_scalability.py`
- Modify: `scripts/test_bench_group_commit_scalability_runner.py`
- Modify: `scripts/test_validate_group_commit_scalability.py`
- Modify: `docs/research/group-commit-scalability-attempt-ledger.md`
- Modify: `docs/superpowers/plans/2026-08-05-realistic-ingest-read-qualification.md`
- Create: `docs/research/direct-convergent-ingest-campaign.json`

**Interfaces:**
- Produces: terminal raw tail/L0 work, resource telemetry, cold latency, recall, and throughput evidence from one exact frozen revision.

- [ ] **Step 1: Extend fail-closed validators**

  Require exact stripe quotas, aggregate reconciliation, fence epochs/prefixes, raw and L0 GET counts, complete modality artifacts, no scalar fallback, and no skipped query branches.

- [ ] **Step 2: Run local structurally valid 768D smoke**

  Exercise 1/8 distinct processes, pressure crossings, materializer takeover, uncached active/post-drain reads, conflict convergence, visibility, and recall 1.0. Apply validators only after terminal markers.

- [ ] **Step 3: Run one full repository gate and push**

  Run all exact assurance layers once; repair focused failures and rerun one final full gate. Commit and fast-forward push the frozen qualification revision.

- [ ] **Step 4: Run the preregistered AWS matrix**

  With profile `causality`, verify an idle host and unused S3 prefixes, then run five paired repetitions for 2K/16K cells and 1/8/32 independent writers. Monitor only terminal markers and infrastructure health while incomplete. Require write/active/post-drain p95 below 200 ms, visibility and recall 1.0, bounded work, and honest throughput.

- [ ] **Step 5: Run realistic quality and scale gates**

  From the exact qualifying revision, run uncached 1M/768D and 1M/1536D with recall@10 at least 0.95 and read p95 below 200 ms. Only then start 100M scale and modality-specific recall/latency/resource campaigns.

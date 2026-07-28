# Bounded Quantized Decode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve recall-matched latency while preventing selected-cell decode fanout from multiplying process RSS across users.

**Architecture:** First select a smaller immutable-cell layout using full-corpus AWS ablations. Then add a process-wide projected-decode gate shared by all searches on an index, independently of per-query width and whole-query admission. Keep packed 4-bit storage as a separately versioned follow-up only if compatible changes miss the RSS gate.

**Tech Stack:** Rust, Arrow/Parquet, object_store, AWS S3/EC2, Python telemetry/SVG tools.

---

### Task 1: Establish the layout frontier

**Files:**
- Update: `docs/web/assets/benchmarks/aws-recall-latency-2026-07-20.csv`
- Update: `docs/benchmarks.md`
- Create: `docs/web/assets/benchmarks/raw/2026-07-20/fashion-cell-layout/`

- [ ] Build TurboQuant Fashion indexes with `segment_max_vectors=512` and 1024.
- [ ] Sweep full-corpus recall at single-cell `nprobe` resolution and candidate budgets around 10–32.
- [ ] Measure selected profiles with 100 uncached and disk-cached queries at widths 8, 16, and full `nprobe`.
- [ ] Reject any disk-cached result with nonzero backing GETs or bytes.
- [ ] Retain resource telemetry and generated CPU/RAM/disk SVGs.

### Task 2: Specify a failing process-wide decode-bound test

**Files:**
- Modify: `crates/borsuk/src/index.rs`
- Test: `crates/borsuk/src/index.rs`

- [ ] Add a test-only active/peak counter around projected cell work.
- [ ] Construct two concurrent searches whose per-query width exceeds the
  intended global cell limit.
- [ ] Assert the observed peak exceeds the intended limit before the fix while
  search results remain correct.
- [ ] Run the focused test and record the expected failure.

### Task 3: Implement shared decode admission

**Files:**
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Modify: `crates/borsuk/examples/production_bench.rs`
- Modify: `docs/api.md`

- [ ] Add a validated `OpenOptions` cell-decode limit with a bounded default.
- [ ] Store one shared gate on `BorsukIndex` and its named-index handles.
- [ ] Acquire a permit immediately before projected read/decode and release it
  after compaction or error.
- [ ] Make the benchmark print and accept the global decode limit explicitly.
- [ ] Run the focused test and the existing prefetch/admission/cache tests.

### Task 4: Remove avoidable full-buffer copies

**Files:**
- Modify: `crates/borsuk/src/format.rs`
- Modify: `crates/borsuk/src/storage.rs`
- Test: `crates/borsuk/src/format.rs`
- Test: `crates/borsuk/src/storage.rs`

- [ ] Add a failing test that counts ownership/copy boundaries for a lean
  segment decode from shared `Bytes`.
- [ ] Change lean header and row projections to share one immutable Parquet
  byte buffer rather than copying the entire object for each reader.
- [ ] Verify corrupt/checksum behavior and decoded candidates remain identical.

### Task 5: Verify and publish

**Files:**
- Update: `docs/benchmarks.md`
- Update: `docs/publication-notes.md`
- Update: `docs/web/assets/benchmarks/aws-s3vectors-fashion-comparison.csv`

- [ ] Run the chosen recall-matched profile twice at callers 1 and 4.
- [ ] Compare latency, RSS, CPU, disk, cache, bytes, and GETs with the 2.63 GiB
  baseline and direct S3 Vectors result.
- [ ] Run formatting, full Rust tests, strict Clippy, Python tests, CSV integrity,
  and diff checks.
- [ ] Stop the EC2 instance and verify the stopped state.

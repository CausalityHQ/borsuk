# V28 Hierarchical PQ S3 Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an S3-native hierarchical IVF-PQ index that scans under 1% of 100M compact codes, fetches at most ten Arrow pages, and exact-reranks for high Recall@10.

**Architecture:** Reuse V27's strict Arrow page codec and hierarchy concepts, replace page summaries with leaf-ordered row-level PQ4 evidence, and derive page ownership from code offsets. Exact vectors and IDs remain only in S3 pages; the resident routing plane is bounded below 3 GiB.

**Tech Stack:** Rust 2024, Apache Arrow IPC, Parquet, Rayon, NEON/AVX table lookup, SHA-256, Python 3.12 controllers, AWS S3/EC2 Spot profile `causality`.

**Spec:** `docs/superpowers/specs/2026-09-03-v28-hierarchical-pq-s3-index-design.md`

## Global Constraints

- V28 is a clean pre-release format; no V27 compatibility reader, aliases, or migration layer.
- Exact vectors and IDs exist only in immutable S3 Arrow pages; no complete vector snapshot is downloaded or resident.
- Persistent scientific data uses Arrow IPC or Parquet; authority and terminals use sorted compact JSON plus LF.
- Query work is at most 1,000,000 codes, ten pages, one S3 wave, and 4,587,520 bytes.
- Resident projection is below 3,221,225,472 bytes; CPU p99 is at most 15 ms and cold p99 may not exceed 150 ms.
- Construction receives no query, truth, prior-result, or page-read capability.
- Run the seconds-long 100K gate during iteration; run Clippy/full workspace assurance once at a stable milestone.

---

### Task 1: Freeze the V28 packed-code authority

**Files:**
- Create: `crates/borsuk/src/v28_s3_pq.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Consumes: V26 PQ4 distance-table and SIMD block semantics.
- Produces: `V28PqWidth`, `V28PqCodebook`, `V28CodeBlock`, `encode_v28_code`, `score_v28_blocks`, and strict Arrow codecs.

- [ ] **Step 1: Write REDs.** Add `v28_s3_pq_` tests for 16-byte/32x3D and 24-byte/48x2D widths, scalar/SIMD equality, deterministic ties, block padding, reversed traversal, exact Arrow schema, checksum drift, nonfinite rejection, and valid exact-zero leaf residuals.
- [ ] **Step 2: Run RED.** Run `cargo test -p borsuk --lib v28_s3_pq_ -- --nocapture`; require only unresolved V28 symbols and at least six selected tests.
- [ ] **Step 3: Implement the minimal codec/kernel.** Use explicit width dispatch, 32-row transposed blocks, fixed 16-entry tables, `u16` accumulation, and no corpus-sized score allocation.
- [ ] **Step 4: Run GREEN and commit.** Run the same selector, `cargo fmt --all -- --check`, and `git diff --check`; commit only Task 1 files.

### Task 2: Bind hierarchy offsets to codes and pages

**Files:**
- Create: `crates/borsuk/src/v28_s3_layout.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Consumes: `V27Hierarchy`, V27 strict page codec, and Task 1 codebook/encoder.
- Produces: `V28LayoutBuilder`, `V28LeafRange`, `V28PageRange`, `V28LayoutManifest`, and authenticated Arrow/Parquet artifacts.

- [ ] **Step 1: Write REDs.** Require one primary owner for every source ordinal, bounded external sort by `(leaf,pq_code,source_ordinal)`, page size at most 1,024, exact leaf/block/page offsets, no replica plane, complete primary union, and code-position-to-page equality at every boundary.
- [ ] **Step 2: Run RED.** Run `cargo test -p borsuk --lib v28_s3_layout_ -- --nocapture`; require missing layout symbols only.
- [ ] **Step 3: Implement the bounded builder.** Stream normalized rows, assign a primary leaf, subtract that leaf centroid, encode the residual at the explicitly requested width, spill fixed records under an explicit memory cap, merge deterministically, and emit code blocks and Arrow pages in the identical order. Query ADC subtracts the same selected-leaf centroid. The 100K controller may invoke separate width builds; no process opens both code planes.
- [ ] **Step 4: Authenticate formats.** Validate exact root/leaf/codebook/code/page-offset/page schemas, digests, lengths, counts, offsets, and source bindings before exposing a layout.
- [ ] **Step 5: Run GREEN and commit.** Run the focused selector, fmt, and diff-check; commit only Task 2 files.

### Task 3: Add bounded hierarchical PQ page selection

**Files:**
- Create: `crates/borsuk/src/v28_s3_search.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Consumes: authenticated hierarchy, codebook, leaf blocks, and page offsets.
- Produces: `V28SearchArm`, `V28PageSelection`, `V28RoutingWork`, and `V28Router::select_pages`.

- [ ] **Step 1: Write REDs.** Table-drive widths 16/24, root beams 8/16/32, leaf beams 64/128/256/512, evidence depths 3,072/6,144/12,288, deterministic distance/page ties, exact ten-page cap, and truthful root/leaf/code/page counters.
- [ ] **Step 2: Lock boundedness.** Add tests that reject more than 1,000,000 scanned codes, prohibit a corpus-sized score buffer, and prove that unselected leaf blocks are never touched through an observer.
- [ ] **Step 3: Run RED.** Run `cargo test -p borsuk --lib v28_s3_search_ -- --nocapture`; require missing router symbols only.
- [ ] **Step 4: Implement selection.** Score roots/leaves with bounded heaps, build one PQ table set, scan selected leaf ranges, retain bounded candidate evidence, reduce it to unique pages by `(best_adc,page_ordinal)`, and return no more than ten identities.
- [ ] **Step 5: Run GREEN and commit.** Run the selector, fmt, and diff-check; commit only Task 3 files.

### Task 4: Fetch one S3 wave and exact-rerank

**Files:**
- Modify: `crates/borsuk/src/v28_s3_search.rs`
- Create: `crates/borsuk/examples/v28_s3_qualify.rs`

**Interfaces:**
- Consumes: `V28PageSelection` and an explicit `V28PageStore`.
- Produces: `V28Index::search` and canonical per-query work/latency evidence.

- [ ] **Step 1: Write REDs.** Require one store call, at most ten unique pages, at most 4,587,520 bytes, complete-byte authentication before Arrow decode, all-or-nothing failure, exact f32 reranking, and exact GET/byte/row counters.
- [ ] **Step 2: Run RED.** Run `cargo test -p borsuk --lib v28_s3_fetch_ -- --nocapture`; require missing fetch/search symbols only.
- [ ] **Step 3: Implement the boundary.** Define `V28PageStore::read_wave(&[V27PageIdentity])`, decode only returned pages, merge duplicate source ordinals deterministically, and return exact top-k.
- [ ] **Step 4: Implement the thin qualifier.** Accept explicit local authority paths plus either a local object directory or an explicit S3 bucket/prefix; reject mixed modes, legacy flags, endpoints, loaders, and D3 flags.
- [ ] **Step 5: Run GREEN and commit.** Run library/example selectors, fmt, and diff-check; commit only Task 4 files.

### Task 5: Make the 100K quality gate the iteration loop

**Files:**
- Create: `scripts/run_v28_reduced_quality.py`
- Create: `scripts/test_run_v28_reduced_quality.py`
- Modify: `scripts/check_v26_fast.py`
- Modify: `scripts/test_check_v26_fast.py`

**Interfaces:**
- Consumes: the immutable Deep 100K construction fixture and 32 development queries/truth.
- Produces: one arm-ladder result, simulated-S3 projection, and a fast release-contract selector.

- [ ] **Step 1: Write controller REDs.** Require all fixed arms in lexicographic order, exact 320-neighbor recomputation, no truth in ranking, query-independent build receipt, injected S3 request/throughput latency, PID cleanup, and canonical Parquet/JSON evidence.
- [ ] **Step 2: Run RED.** Run `python3 -m unittest scripts.test_run_v28_reduced_quality`; require missing controller interfaces only.
- [ ] **Step 3: Implement the reduced controller.** Reuse one frozen 100K layout, execute the real Rust qualifier, stop at the smallest perfect arm, and reject every arm that exceeds code/page/byte/memory/latency bounds.
- [ ] **Step 4: Add the fail-fast gate.** Add only the focused V28 Rust selectors and reduced controller contracts to `scripts/check_v26_fast.py`; keep scientific data outside ordinary unit tests.
- [ ] **Step 5: Run GREEN and commit.** Run the complete controller file, V28 selectors, scoped Ruff, py_compile, fmt, and diff-check; commit only Task 5 files.

### Task 6: Qualify real S3 latency before large quality runs

**Files:**
- Create: `scripts/run_v28_s3_campaign.py`
- Create: `scripts/test_run_v28_s3_campaign.py`
- Modify after terminal evidence: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: the one frozen perfect 100K arm and its immutable pages.
- Produces: Standard-S3 cold request/byte/latency evidence and a fail-fast disposition.

- [ ] **Step 1: Write campaign REDs.** Require `causality` Spot across independent zones, one original process, 30-second observations, one concurrent GET wave, empty-cache cold classification, request/byte decomposition, pressure stops, terminal publication, and immediate termination.
- [ ] **Step 2: Run controller GREEN.** Run `python3 -m unittest scripts.test_run_v28_s3_campaign`, Ruff, py_compile, and diff-check.
- [ ] **Step 3: Run one bounded S3 screen.** Read only the selected registered pages for the 32 development queries. Require p99 at most 150 ms and report the 100-ms target separately.
- [ ] **Step 4: Persist evidence and commit.** Validate the ledger with `python3 scripts/validate_research_docs.py`, run `git diff --check`, and commit only controller/evidence paths.

### Task 7: Sealed 9.99M qualification

**Files:**
- Modify: `scripts/run_v28_s3_campaign.py`
- Modify: `scripts/test_run_v28_s3_campaign.py`
- Modify after evidence: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: one committed arm selected only on the 100K development cohort.
- Produces: sealed Deep quality, CPU, S3, memory, and authority evidence.

- [ ] **Step 1: Build once on `causality` Spot.** Stream the 9.99M corpus without query/truth capability, publish immutable artifacts, verify the primary union, and terminate the builder.
- [ ] **Step 2: Open the sealed cohort once.** Require 995,000-ppm aggregate recall, 997,500-ppm floor compliance, 800,000-ppm minimum, under-15-ms CPU p99, under-150-ms cold p99, at most ten pages, and under 3 GiB.
- [ ] **Step 3: Stop on any failed gate.** Preserve the original terminal and do not tune from sealed failures; a new architecture requires a new version and development cohort.
- [ ] **Step 4: Validate and commit evidence.** Run the docs validator and diff-check; commit only the ledger and frozen authority.

### Task 8: Scale to 100M and finish release assurance

**Files:**
- Create after evidence: `docs/research/v28-s3-pq-production-authority.json`
- Modify after evidence: `docs/research/publication-v3-attempt-ledger.md`
- Modify only from passing evidence: `README.md`
- Modify only from passing evidence: `docs/production-readiness.md`

**Interfaces:**
- Consumes: the frozen V28 source, arm, binary inventory, and ten disjoint construction ranges.
- Produces: authenticated 100M scale evidence and accurately scoped product claims.

- [ ] **Step 1: Build ten ranges on Spot.** Use independent prefixes, discard/restart only explicit interruptions, and verify exactly 100,000,000 unique primary ordinals with no replicas.
- [ ] **Step 2: Run one sealed 100M serving attempt.** Require under 1,000,000 scanned codes, ten pages, 4,587,520 bytes, under 3 GiB, under-15-ms CPU p99, and under-150-ms cold p99.
- [ ] **Step 3: Run milestone assurance once.** Run the V28 affected gate, strict locked workspace/all-targets Clippy, and one locked workspace/all-targets test on a pressure-qualified host.
- [ ] **Step 4: Publish the disposition.** Validate docs, commit/push fast-forward to `origin/main`, verify `HEAD==origin/main==ls-remote`, clean worktree, no live instances, and no competitor claim without paired disclosed reproduction.

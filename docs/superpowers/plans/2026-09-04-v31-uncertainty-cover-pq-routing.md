# V31 Uncertainty-Cover PQ Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recover exact 320/320 recall on the fast 100K Deep Image gate while retaining bounded resident metadata and at most sixteen small S3 pages.

**Architecture:** Persist one conservative u8 PQ reconstruction-radius code per logical row. Flat-score leaves, scan bounded variable-rate PQ ranges, select ten pages by row ADC and six disjoint pages by lower-bound uncertainty, then exact-rerank only authenticated Arrow page bodies.

**Tech Stack:** Rust 2024, Apache Arrow IPC, Parquet, Python 3.12 controllers, AWS S3, Causality EC2 Spot.

**Spec:** `docs/superpowers/specs/2026-09-04-v31-uncertainty-cover-pq-routing-design.md`

## Global constraints

- No compatibility reader, page-centroid production fallback, query-derived construction input, corpus-sized row-score allocation, or local corpus download.
- Run the narrowest selector after each TDD slice. Strict Clippy and the full workspace suite run only once after a passing 100K candidate is stable.
- Every scientific attempt uses one monitored Spot process, immutable S3 evidence, explicit terminal/cleanup, and immediate instance termination.
- D3, 100M, physical coalescing, and claims remain fenced.

### Task 1: Persist conservative error radii

**Files:**
- Modify: `crates/borsuk/src/v30_s3_pq.rs`
- Modify: `crates/borsuk/src/v30_s3_layout.rs`

- [ ] Write REDs for exact upward u8 quantization, zero error, maximum code,
  per-leaf scales, range/order equality, full logical coverage, schema/type/
  nullability mutations, and a decoded bound below the construction error.
- [ ] Run `cargo test -p borsuk --lib v31_error_radius_ -- --nocapture`.
- [ ] Implement `V31ErrorRadiusPlane` and strict Arrow/range serialization with
  no query-facing inputs.
- [ ] Repeat the selector, then the affected V30 PQ/layout selectors, fmt, and
  diff-check. Commit the coherent artifact slice.

### Task 2: Implement bounded 10+6 routing

**Files:**
- Modify: `crates/borsuk/src/v30_s3_search.rs`

- [ ] Write a causal RED where ten-page ADC excludes a truth page and the six
  smallest lower-bound pages recover it.
- [ ] Add differential REDs for scalar/full-sort equality, ties, subnormals,
  reversed blocks, disjointness, exact cardinality, maximum leaves/codes/pages,
  and bounded page-record storage.
- [ ] Run `cargo test -p borsuk --lib v31_uncertainty_cover_ -- --nocapture`.
- [ ] Implement flat leaf scoring, bounded variable-rate ADC, conservative
  lower-bound reduction, and exact ten-primary/six-reserve selection. Delete
  page-centroid influence from production selection.
- [ ] Repeat the selector and affected search gate, then fmt/diff-check. Commit
  the routing slice.

### Task 3: Replace authority and controllers

**Files:**
- Modify: `crates/borsuk/examples/v30_s3_build.rs`
- Modify: `crates/borsuk/examples/v30_s3_qualify.rs`
- Modify: `scripts/run_v30_s3_campaign.py`
- Modify: `scripts/test_run_v30_s3_campaign.py`
- Modify: `scripts/run_v30_untouched_quality.py`
- Modify: `scripts/test_run_v30_untouched_quality.py`

- [ ] Write REDs requiring the V31 error-plane identity, exact 10+6 split,
  zero page-centroid authority, conservative-range work, and independent
  recomputation of quality/bytes/timings.
- [ ] Run the focused build/qualifier example tests and affected Python unit
  modules only.
- [ ] Implement one prerelease manifest/CLI/result schema and remove obsolete
  page-centroid fields and aliases.
- [ ] Repeat focused tests, scoped Ruff/pycompile, fmt, and diff-check. Commit
  the authority/controller slice.

### Task 4: Execute the fast 100K decision gate

**Files:**
- Modify after terminal: `docs/research/publication-v3-attempt-ledger.md`

- [ ] Archive the exact clean source and launch one query-blind 100K
  construction on Causality Spot, streaming registered Parquet shards and
  writing Arrow/Parquet artifacts and pages to S3.
- [ ] On a fresh Spot worker, run the single frozen 10+6 arm over 32 burned
  queries with registered truth. Preserve recall, page identities, bytes,
  GETs, CPU/elapsed phases, RSS, PSI, swap, and terminal markers.
- [ ] Require 320/320 hits, 32/32 perfect queries, minimum at least 800,000 ppm,
  at most 1 MiB, routing CPU p99 at most 15 ms, and all authority/resource
  gates. Stop immediately on failure; do not tune after inspecting misses.
- [ ] Record and validate the evidence ledger. If GREEN, freeze the revision,
  run strict Clippy/full workspace assurance once, and design the separate
  9.99M/coalescing gates. If RED, reject V31 before scale.

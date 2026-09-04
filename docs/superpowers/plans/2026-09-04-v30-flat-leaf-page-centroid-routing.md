# V30 Flat-Leaf Page-Centroid Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recover release-grade recall while keeping V30 serving bounded to 16 small immutable S3 pages by routing directly over resident leaf and page centroids.

**Architecture:** Extend the authenticated page-range Parquet artifact with one f16[96] centroid per balanced geometric page. Replace root-gated row-PQ-induced page selection with a flat bounded leaf-centroid scan followed by a bounded page-centroid scan; retain exact Arrow page reads and exact f32 reranking.

**Tech Stack:** Rust 2024, `borsuk-fma`, Apache Arrow IPC, Parquet, Python 3.12 controllers, AWS S3, Causality EC2 Spot.

**Spec:** `docs/superpowers/specs/2026-09-04-v30-flat-leaf-page-centroid-routing-design.md`

## Global Constraints

- Construction is query-blind and receives no query, truth, result, page-read, or D3 capability.
- Metadata remains Parquet; typed hierarchy/PQ/page artifacts remain Arrow IPC; receipts remain sorted compact JSON plus LF.
- Page centroids are authenticated non-null f16[96], derived from exact page members, and never replace exact reranking.
- Serving scores all 32,768 projected leaves, at most 32,768 page centroids beneath 512 leaves, and fetches at most 16 logical pages; construction rejects more than 64 pages in any leaf.
- No legacy schema, optional fallback, alias, compatibility dispatch, corpus-sized score allocation, or query-derived construction input is permitted.
- Per-edit tests are narrow; strict Clippy and the full locked workspace suite run once only after the focused scientific gate is stable.
- D3, 100M, physical coalescing, cache claims, and competitor claims remain fenced.

---

### Task 1: Persist authenticated page centroids

**Files:**
- Modify: `crates/borsuk/src/v30_s3_layout.rs`

**Interfaces:**
- Consumes: each final balanced page's exact `V30LayoutRecord` vectors.
- Produces: `V30PageRange.centroid: [f16; 96]` and strict `page-offsets.parquet` read/write validation.

- [ ] **Step 1: Write schema and derivation REDs.** Extend the geometric builder test to independently hand-compute normalized means and require exact f16 values. Add table-driven mutations for missing/extra columns, outer/list/child nullability, child name/type, list width, non-finite/zero centroid, page order, leaf ownership, and row ranges.
- [ ] **Step 2: Run RED.** Run `cargo test -p borsuk --lib v30_s3_layout_page_centroid_ -- --nocapture`. Expect unresolved centroid field/schema only.
- [ ] **Step 3: Implement the minimal writer/reader.** Compute one normalized f32 mean after final page membership, round to f16 once, write a required `centroid` fixed-size-list column, and validate all physical and relational fields on read.
- [ ] **Step 4: Run GREEN and regression.** Run the identical selector, then `cargo test -p borsuk --lib v30_s3_layout_ -- --nocapture`, `cargo fmt --all -- --check`, and `git diff --check`.
- [ ] **Step 5: Commit.** Commit only the layout slice with `feat: bind V30 geometric page centroids`.

### Task 2: Select pages directly from resident centroids

**Files:**
- Modify: `crates/borsuk/src/v30_s3_search.rs`

**Interfaces:**
- Consumes: normalized query, all leaf centroids, page ranges with centroids, geometry-bound leaf beam (`192` in the 100K screen, `512` at 100M), and `page_count=16`.
- Produces: deterministic `V30PageSelection` ordered by `(distance,page_ordinal)` and exact bounded work counts.

- [ ] **Step 1: Write causal REDs.** Build a fixture where root routing excludes a truth leaf and row PQ ordering excludes a truth page, while direct leaf/page centroids select it. Require all leaves scored, only pages beneath retained leaves scored, 512/16 bounded heaps, and no PQ-code read through an observer.
- [ ] **Step 2: Write differential/boundary REDs.** Compare bounded selection with scalar full sort for random, ties, subnormals, reversed page blocks, minimum cardinality, 512 leaves, and 32,768 pages. Reject non-finite queries, insufficient page candidates, more than 64 pages per leaf, and count overflow.
- [ ] **Step 3: Run RED.** Run `cargo test -p borsuk --lib v30_s3_search_page_centroid_ -- --nocapture`. Expect current root/PQ router to select a different page set.
- [ ] **Step 4: Implement bounded selection.** Flat-score leaf centroids, retain 512, scan their page centroids, retain 16, and emit work counts. Delete root/PQ influence from the production page-selection path; keep diagnostic methods explicitly separate.
- [ ] **Step 5: Run GREEN and affected tests.** Run the identical selector, `cargo test -p borsuk --lib v30_s3_search_ -- --nocapture`, and `cargo test -p borsuk --example v30_s3_qualify v30_s3_qualify_ -- --nocapture`.
- [ ] **Step 6: Format and commit.** Run `cargo fmt --all -- --check` and `git diff --check`; commit with `feat: route V30 through page centroids`.

### Task 3: Bind manifest, CLI, and fast evidence

**Files:**
- Modify: `crates/borsuk/examples/v30_s3_build.rs`
- Modify: `crates/borsuk/examples/v30_s3_qualify.rs`
- Modify: `scripts/run_v30_s3_campaign.py`
- Modify: `scripts/test_run_v30_s3_campaign.py`
- Modify: `scripts/run_v30_untouched_quality.py`
- Modify: `scripts/test_run_v30_untouched_quality.py`

**Interfaces:**
- Consumes: exact page-centroid artifact identity and frozen 100K query/truth inputs.
- Produces: authenticated `flat-leaf-page-centroid-v1` manifest/CLI authority and canonical work/quality receipt.

- [ ] **Step 1: Write manifest/CLI REDs.** Require exact algorithm, leaf beam, page count, centroid schema and Parquet identity; reject aliases, old manifests, omitted fields, storage overrides, or construction query/truth capability.
- [ ] **Step 2: Write receipt REDs.** Require exact leaf/page centroid score counts, zero production root/PQ selection counts, 16 GET cap, encoded-byte hard stop, independently recomputed recall, and separate routing/page/rerank CPU and elapsed fields.
- [ ] **Step 3: Run focused REDs.** Run `cargo test -p borsuk --example v30_s3_build v30_s3_build_ -- --nocapture`, `cargo test -p borsuk --example v30_s3_qualify v30_s3_qualify_ -- --nocapture`, and `python3 -m unittest scripts.test_run_v30_s3_campaign scripts.test_run_v30_untouched_quality`.
- [ ] **Step 4: Implement thin authority changes.** Serialize/validate only the new prerelease schema and work fields; keep vectors out of JSON.
- [ ] **Step 5: Run GREEN/static checks.** Repeat the focused gates, pinned Ruff on changed Python, `python3 -m py_compile`, `cargo fmt --all -- --check`, and `git diff --check`.
- [ ] **Step 6: Commit.** Commit the authority/controller slice with `feat: orchestrate V30 page-centroid routing`.

### Task 4: Run the 100K causal falsifier

**Files:**
- Modify after terminal evidence: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: frozen Deep Image 100K corpus, 32 sealed queries/truth, committed source archive, and `page_rows=128`.
- Produces: immutable construction/quality receipts and an accept/reject decision.

- [ ] **Step 1: Run focused assurance only.** Run the layout/search/build/qualifier/Python gates, scoped Ruff/pycompile, fmt, and diff-check. Do not run a full suite yet.
- [ ] **Step 2: Build once on Causality Spot.** Stream the registered 100K Parquet corpus into geometric Arrow pages and authenticated centroid Parquet; upload content-addressed artifacts and terminate. No query/truth capability is present.
- [ ] **Step 3: Evaluate exactly one 16-page arm.** On a fresh same-region Spot worker, run the sealed 32 queries once. Preserve original terminal, page bytes/GETs, recall, work, CPU, PSI, swap, and cleanup evidence.
- [ ] **Step 4: Apply gates.** Advance only at aggregate recall at least 995,000 ppm, minimum at least 800,000 ppm, at least 31/32 perfect, maximum bytes at most 1,048,576, maximum routing CPU at most 5,000,000 ns, and no authority/resource failure.
- [ ] **Step 5: Persist disposition.** Update and validate the evidence ledger whether accepted or rejected. Do not tune from individual query misses.
- [ ] **Step 6: Release checkpoint.** If accepted, run strict workspace Clippy and `cargo test --locked --workspace --all-targets` once, commit/push, then design the separate 9.99M confirmation and physical four-way coalescing. If rejected, stop before larger construction.

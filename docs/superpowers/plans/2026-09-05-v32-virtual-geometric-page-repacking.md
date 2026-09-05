# V32 Virtual Geometric Page Repacking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Falsify or validate deterministic within-microleaf geometric page repacking against the authenticated one-million-row V32 candidate order without reading page bodies or rebuilding the corpus.

**Architecture:** Add a private PQ reconstruction primitive, build a query-independent virtual page map from resident codes plus authenticated logical-source identities, and replay the existing global-prefix candidate order against current and virtual layouts. Extend the existing Rust qualifier and Python authority/controller boundary so the current-layout control must reproduce exactly before the treatment can be reduced.

**Tech Stack:** Rust 2024, `borsuk-fma`, Apache Arrow IPC, Parquet, Python 3.12, canonical JSON, Causality AWS Spot.

**Spec:** `docs/superpowers/specs/2026-09-05-v32-virtual-geometric-page-repacking-design.md`

## Global Constraints

- Layout construction receives no query, truth, result, page-body, S3 page-prefix, or D3 capability.
- The frozen global route remains 768 microleaves, at most 262,144 scanned codes, 12,288 retained candidates, and first-distinct 16-page reduction.
- The control must reproduce 308/320 first-distinct and 298/320 reciprocal-rank hits before treatment output is accepted.
- The treatment advances only at 320/320, minimum 10/10, 32/32 perfect, exactly 16 pages, zero page reads, and at most 3,145,728 projected bytes.
- Persistent vectors and pages use Arrow IPC or Parquet. Receipts use sorted compact JSON plus LF.
- Focused gates run during development. Strict Clippy and the locked workspace suite run once after the no-page result is stable.
- No page download, corpus download, layout materialization, D3, 100-million-row build, compatibility reader, or competitor claim is authorized by this plan.

---

### Task 1: Reconstruct frozen PQ geometry

**Files:**
- Modify: `crates/borsuk/src/v30_s3_pq.rs`

**Interfaces:**
- Consumes: `&V30PqCodebook`, one exact-width code slice.
- Produces: `pub(crate) fn reconstruct_v30_code(codebook: &V30PqCodebook, code: &[u8]) -> Result<[f32; 96]>`.

- [ ] **Step 1: Write the failing tests.** Add `v32_virtual_geometric_reconstruction_matches_literal_centroids` and `v32_virtual_geometric_reconstruction_rejects_width_and_nonfinite_state`. Use literal codebooks for both 24-byte and 48-byte widths and require exact subvector placement, deterministic bytes, exact code width, and finite output.
- [ ] **Step 2: Run the focused RED.** Run `cargo test -p borsuk --lib v32_virtual_geometric_reconstruction_ -- --nocapture`. Require unresolved reconstruction API only.
- [ ] **Step 3: Implement the minimal decoder.** For each subquantizer, use its code byte as the centroid ordinal and copy exactly `width.dimensions()` values into the corresponding output slice. Reuse `V30PqCodebook::validate`; do not normalize or add a parent centroid in this primitive.
- [ ] **Step 4: Run GREEN.** Run the identical selector; require both tests passing and zero warnings.
- [ ] **Step 5: Run mechanical checks and commit.** Run `cargo fmt --all -- --check` and `git diff --check`; commit only `v30_s3_pq.rs` with message `feat: reconstruct V32 diagnostic PQ geometry`.

### Task 2: Build a deterministic virtual page map

**Files:**
- Modify: `crates/borsuk/src/v30_s3_layout.rs`
- Modify: `crates/borsuk/src/v30_s3_search.rs`

**Interfaces:**
- Consumes: decoded hierarchy/layout/code planes, base/high codebooks, and `logical_sources: &[u64]` whose index is logical ordinal.
- Produces: crate-private `V32VirtualPageLayout` with `page_for_logical(u64) -> Result<u32>`, `page_row_count(u32) -> Result<u16>`, `page_count() -> usize`, and `truth_page_count(&[u64]) -> Result<usize>`.

- [ ] **Step 1: Write ownership and determinism REDs.** Add `v32_virtual_geometric_layout_is_complete_balanced_and_query_blind`. Require every logical row exactly once, groups of 1 through 480, no cross-microleaf group, identical output after reversing the supplied row iteration, and source ordinal as every tie break.
- [ ] **Step 2: Write authority and impossibility REDs.** Add `v32_virtual_geometric_layout_rejects_source_and_geometry_drift` and `v32_virtual_geometric_layout_reports_eight_page_obstruction`. Reject duplicate/missing/out-of-range logical-source identities, nonfinite/zero reconstructed vectors, wrong code width, and a truth set occupying nine microleaves. Require the obstruction without any query or page source.
- [ ] **Step 3: Run the focused RED.** Run `cargo test -p borsuk --lib v32_virtual_geometric_layout_ -- --nocapture`. Require missing virtual-layout API only.
- [ ] **Step 4: Implement the minimal layout.** Process routing microleaves in ordinal order. Reconstruct the row residual, add its stored code-parent centroid, normalize once, construct `V30LayoutRecord`, call the existing deterministic balanced splitter with 480 rows, and fill one `Vec<u32>` page owner plus per-page row counts. Do not retain reconstructed vectors after each microleaf.
- [ ] **Step 5: Run GREEN and focused regressions.** Run the identical selector, then `cargo test -p borsuk --lib v30_s3_layout_geometric_ -- --nocapture`. Require all selected tests passing and zero warnings.
- [ ] **Step 6: Format, diff-check, and commit.** Run `cargo fmt --all -- --check` and `git diff --check`; commit only the two Rust modules with message `feat: model virtual V32 geometric pages`.

### Task 3: Replay the unchanged candidate order

**Files:**
- Modify: `crates/borsuk/src/v30_s3_search.rs`

**Interfaces:**
- Consumes: `&V32VirtualPageLayout`, query, frozen `V32SearchArm`, global leaf limit 768, and ten diagnostic logical ordinals.
- Produces: `V32VirtualRoutingDiagnostic` containing current and virtual selected pages, target stages, truth-bearing microleaf/page counts, recovered/newly-lost rows, and unchanged routing work.

- [ ] **Step 1: Write current-control RED.** Add `v32_virtual_geometric_replay_preserves_candidate_order_and_current_control`. On a literal synthetic router, require the current-layout result and selected pages to be byte-identical to `diagnose_logicals_with_global_prefix`; mutation of any candidate score/order, selected leaf, work count, or truth-independent selection must fail.
- [ ] **Step 2: Write treatment and containment REDs.** Add `v32_virtual_geometric_replay_selects_pages_before_truth` and `v32_virtual_geometric_replay_counts_recovered_and_lost_rows`. Require first-distinct selection over the unchanged ranked candidates, exactly 16 unique pages, truth join afterward, and correct recovered/newly-lost sets when virtual ownership changes.
- [ ] **Step 3: Run the focused RED.** Run `cargo test -p borsuk --lib v32_virtual_geometric_replay_ -- --nocapture`. Require missing replay boundary only.
- [ ] **Step 4: Implement minimal replay.** Refactor candidate production into one internal details path shared by current and virtual diagnostics. Map each ranked candidate through the supplied virtual owner, stop after 16 unique pages, and derive target containment without rescoring or reranking candidates.
- [ ] **Step 5: Run GREEN and routing regressions.** Run the identical selector, then `cargo test -p borsuk --lib v32_routing_ -- --nocapture`. Require unchanged existing diagnostics and zero warnings.
- [ ] **Step 6: Format, diff-check, and commit.** Run `cargo fmt --all -- --check` and `git diff --check`; commit `v30_s3_search.rs` with message `feat: replay V32 virtual page containment`.

### Task 4: Authenticate and serialize the no-page comparison

**Files:**
- Modify: `crates/borsuk/examples/v30_s3_qualify.rs`
- Modify: `scripts/run_v32_no_page_containment.py`
- Modify: `scripts/test_run_v32_no_page_containment.py`

**Interfaces:**
- Consumes: existing manifest/resident artifact directory, authenticated `logical-sources.arrow`, query and truth Parquet, truth receipt, and the exact governing terminal identity.
- Produces: canonical claim-ineligible control/treatment result and explicit pass/fail gates.

- [ ] **Step 1: Write Rust boundary REDs.** Require an explicit `--virtual-geometric-pages` flag only in global diagnostic mode, authenticated logical-source Arrow schema/cardinality, zero page source, and canonical per-query virtual selection/occupancy output. Reject page, storage, endpoint, D3, truth-at-layout-build, and unknown flags.
- [ ] **Step 2: Write Python authority/reduction REDs.** Require the exact governing terminal URI, 262,537-byte length, SHA-256, source/archive/index identities, exact 308/320 and 298/320 control, the same twelve misses, 16 pages, zero page reads, and the 320/320 treatment advance gate. Mutation-lock every input digest/length/URI, control metric, recovered/lost set, occupancy bound, and treatment aggregate.
- [ ] **Step 3: Run narrow REDs.** Run `cargo test -p borsuk --example v30_s3_qualify v32_virtual_geometric_ -- --nocapture`, then the Python class `python3 -m unittest scripts.test_run_v32_no_page_containment.V32VirtualGeometricPackingTests`. Require missing CLI/result fields only.
- [ ] **Step 4: Implement the thin boundary.** Load and authenticate logical-source Arrow in Rust, invoke the virtual layout/replay, and emit canonical query records. In Python authenticate the frozen control terminal before executing treatment commands, independently recompute aggregates and gates, and write sorted compact JSON plus LF.
- [ ] **Step 5: Run the focused GREEN bundle.** Run the same Rust and Python selectors, scoped Ruff on the two Python files, `python3 -m py_compile` on them, `cargo fmt --all -- --check`, and `git diff --check`.
- [ ] **Step 6: Commit the diagnostic slice.** Commit only the example/controller/test files with message `test: replay V32 virtual geometric pages`.

### Task 5: Execute one authenticated one-million-row replay

**Files:**
- Modify after terminal evidence: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: frozen one-million-row resident artifacts, logical-source Arrow, query ordinals 64 through 95, truth Parquet/receipt, and governing terminal SHA-256/length.
- Produces: one immutable canonical treatment result/terminal and accept/reject disposition.

- [ ] **Step 1: Run only the fast affected assurance.** Run focused reconstruction/layout/replay/example/Python selectors, scoped Ruff/py_compile, formatting, and diff-check. Do not run the full workspace suite.
- [ ] **Step 2: Run one page-free Causality Spot diagnostic.** Download only registered resident/query/truth artifacts, execute the frozen control and treatment once, enforce zero page-body GETs, preserve exact output hashes, and terminate the instance.
- [ ] **Step 3: Apply the preregistered gate.** Reject on any control mismatch. Advance treatment only at 320/320, minimum 10/10, 32/32 perfect, exactly 16 pages, unchanged routing work, zero page reads, and at most 3,145,728 projected bytes. Record the eight-page microleaf and virtual-page obstruction separately.
- [ ] **Step 4: Persist evidence.** Update `docs/research/publication-v3-attempt-ledger.md` with exact source, input/output authorities, metrics, resources, cleanup, and D3 disposition; run `python3 scripts/validate_research_docs.py` and `git diff --check`; commit and fast-forward push.
- [ ] **Step 5: Run release assurance only after a passing treatment.** If and only if the treatment advances, run strict locked workspace Clippy and one locked workspace/all-targets test gate. A rejected treatment stops before full assurance, materialization, or a larger cohort.

# V30 Geometric Page Packing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Concentrate V30's existing high-quality PQ candidates into fewer immutable S3 Arrow pages without changing hierarchy recall, PQ scoring, or serving-resident memory.

**Architecture:** Insert a bounded deterministic balanced-cosine packer between the existing leaf-grouped external merge and the layout assembler. The packer buffers one leaf, recursively partitions exact normalized vectors into balanced page groups, and sends the reordered records through the unchanged code/page emitter. Compare it causally with the frozen lexicographic builder at identical 128-row pages and 4/8/16-page search arms.

**Tech Stack:** Rust 2024, `borsuk-fma`, Apache Arrow IPC, Parquet, Python 3.12 controllers, AWS S3 and Causality EC2 Spot.

**Spec:** `docs/superpowers/specs/2026-09-04-v30-geometric-page-packing-design.md`

## Global Constraints

- Construction receives no query, truth, prior-result, page-read, or D3 capability.
- Persistent metadata stays Parquet; typed serving artifacts and exact pages stay Arrow IPC; receipts stay sorted compact JSON plus LF.
- The hierarchy, PQ codebooks and bytes, 50,000-ppm fidelity set, scan limits, candidate ordering, and exact reranker do not change.
- A leaf buffer contains at most 65,536 rows and is released immediately after leaf emission.
- Production uses only `balanced-cosine-v1`; the frozen source at `2bce312c1bc7759efc1e540e2787750775ff85e8` supplies the lexicographic control.
- Per-edit verification is focused. Strict Clippy and the full locked workspace suite run once at the release checkpoint.
- No 9.99M, 100M, D3, caching, compatibility, or competitor-claim work is authorized by this plan.

---

### Task 1: Lock deterministic balanced leaf partitioning

**Files:**
- Modify: `crates/borsuk/src/v30_s3_layout.rs`

**Interfaces:**
- Consumes: `Vec<V30LayoutRecord>`, `page_rows: usize`.
- Produces: private `partition_v30_leaf_pages(rows: Vec<V30LayoutRecord>, page_rows: usize) -> Result<Vec<Vec<V30LayoutRecord>>>`.

- [ ] **Step 1: Write the failing geometric partition tests.** Add `v30_s3_layout_geometric_pages_are_balanced_local_and_deterministic` with deliberately permuted base-code labels. Construct four angular clusters in one leaf, call `partition_v30_leaf_pages`, and require: complete source union, no duplicate owner, group sizes differing by at most one, byte-identical source ordering after reversed input, and lower literal within-page cosine dispersion than `(base_code,source)` order.

```rust
let first = partition_v30_leaf_pages(rows.clone(), 8).unwrap();
let second = partition_v30_leaf_pages(rows.into_iter().rev().collect(), 8).unwrap();
assert_eq!(page_sources(&first), page_sources(&second));
assert_eq!(page_sources(&first).concat(), (0_u64..32).collect::<Vec<_>>());
assert!(within_page_dispersion(&first) < within_page_dispersion(&lexicographic));
```

- [ ] **Step 2: Write boundary REDs.** Add `v30_s3_layout_geometric_pages_reject_invalid_vectors_and_leaf_overflow`. Require rejection for empty input, zero page size, non-finite/zero-norm vectors, mixed leaf ordinals, duplicate source ordinals, and a count above `MAX_GEOMETRIC_LEAF_ROWS = 65_536`. Test the row-count predicate directly rather than allocating 65,537 full records.

- [ ] **Step 3: Run the focused RED.** Run `cargo test -p borsuk --lib v30_s3_layout_geometric_ -- --nocapture`. Expected: unresolved `partition_v30_leaf_pages` and bound validator only.

- [ ] **Step 4: Implement the minimal partitioner.** Add normalized centroid, cosine margin, deterministic farthest-seed, four-update balanced split, and recursive page partitioning. Preserve `source_ordinal` as every tie break and reject any non-finite intermediate.

```rust
fn partition_v30_leaf_pages(
    rows: Vec<V30LayoutRecord>,
    page_rows: usize,
) -> Result<Vec<Vec<V30LayoutRecord>>> {
    validate_geometric_leaf(&rows, page_rows)?;
    let pages = rows.len().div_ceil(page_rows);
    partition_v30_group(rows, pages)
}
```

- [ ] **Step 5: Run GREEN and static checks.** Run the identical selector, `cargo fmt --all -- --check`, and `git diff --check`.

- [ ] **Step 6: Commit the isolated primitive.** Commit only `v30_s3_layout.rs` with message `feat: partition V30 leaves into geometric pages`.

### Task 2: Integrate packing into bounded construction

**Files:**
- Modify: `crates/borsuk/src/v30_s3_layout.rs`

**Interfaces:**
- Consumes: the leaf-grouped stream emitted by `sort_v30_layout_records`.
- Produces: private `V30GeometricLeafPacker` that flushes reordered records into `V30LayoutAssembler::push`.

- [ ] **Step 1: Write the failing integration test.** Add `v30_s3_layout_geometric_builder_keeps_codes_offsets_and_pages_aligned`. Build at least two leaves with more than one page each. Decode every Arrow page and independently require that each logical code position, fidelity bit/rank, page range, and decoded source owner describes the same reordered row.

- [ ] **Step 2: Lock boundedness and cleanup in RED.** Extend the scratch/page sink failure test so a page-write failure occurs after one complete leaf. Require the packer leaf buffer to be empty after the error, scratch runs removed, no later leaf emitted, and a recorded peak leaf count no greater than 65,536.

- [ ] **Step 3: Run the focused RED.** Run `cargo test -p borsuk --lib v30_s3_layout_geometric_builder_ -- --nocapture`. Expected: the current lexicographic output violates geometric page membership while ownership and cleanup remain intact.

- [ ] **Step 4: Implement the streaming leaf packer.** Keep `V30LayoutAssembler` as the only code/page emitter. The merge callback pushes into `V30GeometricLeafPacker`; on a leaf change it partitions and drains the prior leaf into the assembler, then clears the buffer. `finish()` flushes the final leaf exactly once.

```rust
sort_v30_layout_records(records, config.sort_memory_rows, scratch, &mut |record| {
    packer.push(record)
})?;
packer.finish()?;
assembler.finish(config.fidelity_ppm)
```

- [ ] **Step 5: Run focused regression gates.** Run `cargo test -p borsuk --lib v30_s3_layout_ -- --nocapture`, then `cargo test -p borsuk --lib v30_s3_search_ -- --nocapture`. Require unchanged scoring and exact rerank behavior.

- [ ] **Step 6: Format, diff-check, and commit.** Run `cargo fmt --all -- --check` and `git diff --check`; commit with message `feat: pack V30 pages by residual geometry`.

### Task 3: Add a fast equal-budget page-concentration gate

**Files:**
- Modify: `crates/borsuk/examples/v30_s3_qualify.rs`
- Modify: `scripts/run_v30_reduced_quality.py`
- Modify: `scripts/test_run_v30_reduced_quality.py`

**Interfaces:**
- Consumes: exact construction/layout identities, query/truth Parquet, page-count arms `[4, 8, 16]`, and injected S3 latency samples.
- Produces: canonical per-arm recall, unique oracle-page count, GET count, encoded bytes, routing/rerank CPU, and simulated cold-latency decomposition.

- [ ] **Step 1: Write controller/result REDs.** Require the fixed arm ladder `[4,8,16]`, `page_rows=128`, exact treatment/control source identities, equal candidate depth, independent truth recomputation, and a result field `oracle_page_count` derived from exact-neighbor owners rather than selected candidates. Reject query or truth capability in construction.

- [ ] **Step 2: Write latency-model REDs.** For each query, compute one concurrent-wave latency as `max(GET samples) + routing + decode/rerank`, never `sum(GET samples)`. Require literal GET count/bytes and report both simulated Standard-S3 and measured CPU values without sleeping.

- [ ] **Step 3: Run the Python RED.** Run `python3 -m unittest scripts.test_run_v30_reduced_quality.V30PagePackingTests`. Expected: missing arm-ladder/oracle-page evidence fields only.

- [ ] **Step 4: Implement the thin evidence changes.** Rust emits page ownership and exact candidate evidence; Python validates/reduces it into sorted compact JSON. Keep vectors/pages in Arrow and queries/truth in Parquet; add no JSON vector payload.

- [ ] **Step 5: Run focused GREEN.** Run the same unittest class, `cargo test -p borsuk --example v30_s3_qualify v30_ -- --nocapture`, pinned Ruff on the two Python files, `python3 -m py_compile` on them, `cargo fmt --all -- --check`, and `git diff --check`.

- [ ] **Step 6: Commit the fast gate.** Commit only the example/controller/tests with message `test: compare V30 page packing at equal budgets`.

### Task 4: Run one bounded 100K causal decision on Causality Spot

**Files:**
- Modify after terminal evidence: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: frozen 100K Deep Image corpus/query/truth authorities, frozen control source `2bce312c1bc7759efc1e540e2787750775ff85e8`, the committed treatment source, and the exact `[4,8,16]` arm manifest.
- Produces: immutable control/treatment construction manifests, per-query Parquet evidence, canonical result/terminal JSON, and an accept/reject disposition.

- [ ] **Step 1: Run the fast local assurance bundle.** Run focused layout/search/example/Python gates, scoped Ruff and py_compile, formatting, and diff-check. Do not run the full workspace suite.

- [ ] **Step 2: Build two query-isolated layouts on one disposable Spot builder.** Stream the registered 100K corpus once per frozen source, use `page_rows=128`, upload content-addressed Arrow/Parquet/pages, preserve manifests and terminals, and terminate. The worker must not receive query/truth credentials.

- [ ] **Step 3: Evaluate the preregistered ladder once.** On a fresh same-region Spot worker, evaluate control and treatment for page counts 4, 8, and 16 over the same frozen 32-query regression set. Preserve per-query evidence and stop without parameter changes.

- [ ] **Step 4: Apply the causal gate.** Reject if treatment reduces aggregate/minimum recall or fails to reduce oracle-page concentration and fetched bytes. Advance only if the eight-page treatment reaches at least 995,000-ppm aggregate, 800,000-ppm minimum, and 31/32 perfect queries.

- [ ] **Step 5: Freeze a disjoint 512-query confirmation.** If Step 4 passes, select ordinals before truth computation, generate one exact 100K truth Parquet, freeze it, and evaluate once. Do not tune from misses.

- [ ] **Step 6: Persist evidence and run release assurance once.** Update the attempt ledger, validate research docs, then run strict workspace Clippy and `cargo test --locked --workspace --all-targets` once. Commit/push only after all required gates are terminal GREEN. Otherwise preserve the rejection and stop before 9.99M/100M.

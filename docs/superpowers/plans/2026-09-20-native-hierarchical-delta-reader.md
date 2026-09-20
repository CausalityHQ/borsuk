# Native Hierarchical Delta Reader Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the benchmark-only fast reader and the rejected centroid-based mutation reader with one authenticated object-storage-native reader that preserves BORSUK's measured recall/latency while providing query-visible writes, snapshots, and compaction below the 100M/<3 GiB memory bound.

**Architecture:** Add a next-generation native ANN authority beside the current implementation only while it is being proven, then make one atomic pre-release cutover and remove the retired persisted/read paths. Reuse `Storage`, the immutable global-leaf Arrow page encoding, mutation stamps/directories, WAL publication, and conditional collection commits; add only the cross-language PQ16 router artifacts and bounded two-level routing runtime that those components lack.

**Tech Stack:** Rust 2024, Arrow IPC, Apache Parquet, `object_store`, Tokio, existing BORSUK storage/admission/WAL primitives, SHA-256, canonical JSON, SIMD ADC with scalar differential controls.

**Spec:** `docs/superpowers/specs/2026-09-20-native-hierarchical-delta-reader-design.md`

## Global Constraints

- There is no compatibility reader: the final cutover rejects every retired experimental layout and deletes aliases, migrations, and duplicate query paths.
- Queries and truth never appear in index artifacts; all persisted production data is canonical JSON, Arrow IPC, or Parquet.
- Release gates are average Recall@10 >= 960,000 ppm, average Recall@100 >= 975,000 ppm, p05 Recall@100 >= 900,000 ppm, zero query failures, uncached in-region p50 <= 100 ms, p95 <= 250 ms, and <= 16 MiB page bodies/query.
- Query-visible batch writes must reach >= 100,000 vectors/s with visibility p95 <= 250 ms on the qualification host.
- Complete projected resident state at 100M rows, including sixteen 16-MiB responses and a 512-MiB runtime reserve, must remain below 3 GiB.
- Routing is fixed-block and bounded: no full `(distance, row)` allocation/sort and no nested query-level work-stealing pool.
- Use AWS profile `causality`, Spot by default, one immutable attempt per paid cell, immediate termination, and no 10M/100M promotion before the preceding gate is green.

## Review Focus

- A generation whose object bytes are valid but whose URI/digest/length or previous-generation binding drifts must fail before semantic use; Task 1 tests every role and root binding.
- Parquet with the right logical values but wrong physical type, nullability, row order, row count, or non-finite codebook entries must fail closed; Task 2 mutates each physical property.
- Ties, subnormals, reversed block order, tail widths, and candidate counts smaller than the heap bound must produce byte-identical scalar/SIMD order; Task 3 differential-tests them.
- A base row shadowed by a newer update or tombstone in any visible run must never escape through routing, rerank, deduplication, or refresh; Task 4 tests every boundary.
- Publication races and compaction enumeration order must not change the visible generation or canonical bytes; Task 5 tests conditional commits, interruption, and order independence.

---

### Task 1: Typed generation and router authority

**Files:**
- Create: `crates/borsuk/src/native_ann.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: `crates/borsuk/src/native_ann.rs`

**Interfaces:**
- Consumes: `crate::metric::VectorMetric` and `crate::mutation::MutationVersionRange`.
- Produces: `NativeArtifactRef`, `NativeAnnRef`, `NativeRouterRef`, `NativeRunRef`, `NativeSq8Authority`, `native_ann_root_bytes(&NativeAnnRef) -> Result<Vec<u8>>`, and `native_ann_root_from_bytes(&[u8]) -> Result<NativeAnnRef>`.

- [ ] **Step 1: Write authority tests before the types exist**

  Add tests named `native_ann_authority_round_trips_canonical_cross_language_root` and `native_ann_authority_rejects_every_identity_schema_and_generation_drift`. Construct one literal authority with page rows `256`, PQ width `16`, two summary codes/page, ordered base/delta runs, an exact SQ8 authority, mutation-directory ref, generation/previous-generation digests, and source identity. Mutate one field at a time: missing/extra JSON key, URI, SHA-256, byte length, role, dimension, metric, page rows, PQ width, summary count, duplicate/unordered run, generation, previous generation, and quantizer identity.

- [ ] **Step 2: Verify the focused RED**

  Run: `cargo test -p borsuk --lib native_ann_authority_ -- --nocapture`

  Expected: compile failure only for the missing `native_ann` types/functions.

- [ ] **Step 3: Implement strict authority and canonical bytes**

  Define the root with exact serde schemas:

  ```rust
  #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
  #[serde(deny_unknown_fields)]
  pub(crate) struct NativeAnnRef {
      pub(crate) format_version: u16,
      pub(crate) generation: u64,
      pub(crate) previous_generation_sha256: Option<String>,
      pub(crate) source_identity: String,
      pub(crate) dimensions: u32,
      pub(crate) metric: VectorMetric,
      pub(crate) page_rows: u32,
      pub(crate) router: NativeRouterRef,
      pub(crate) mutation_directory: ArtifactRef,
      pub(crate) base_runs: Vec<NativeRunRef>,
      pub(crate) delta_runs: Vec<NativeRunRef>,
      pub(crate) sq8: NativeSq8Authority,
  }
  ```

  Define `NativeArtifactRef { role, uri, sha256, encoded_bytes }` rather than
  reusing the path-only row-bundle reference. Validate fixed format values
  (`page_rows=256`, `pq_width=16`, `summary_codes_per_page=2`), exact lower-case
  64-hex digests, absolute object URIs, nonzero lengths, unique expected roles,
  strictly ordered unique runs, nonoverlapping version ranges, and root/object
  cross-bindings. Serialize through a recursive lexicographic JSON transform
  and append exactly one LF; on read, compare input bytes to the reserialization
  before returning the typed value.

- [ ] **Step 4: Verify authority GREEN and static cleanliness**

  Run: `cargo test -p borsuk --lib native_ann_authority_ -- --nocapture && cargo fmt --all -- --check && git diff --check`

  Expected: both authority tests pass; formatting/diff checks exit zero.

- [ ] **Step 5: Commit the authority slice**

  ```bash
  git add crates/borsuk/src/native_ann.rs crates/borsuk/src/lib.rs
  git commit -m "feat(borsuk): add native ANN generation authority"
  ```

### Task 2: Strict Parquet router artifacts and memory accounting

**Files:**
- Create: `crates/borsuk/src/native_ann_format.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: `crates/borsuk/src/native_ann_format.rs`

**Interfaces:**
- Consumes: `NativeRouterRef` from Task 1 and authenticated bytes supplied by `Storage`.
- Produces: `NativeRouterArtifacts { row_codebooks, summary_codebooks, row_codes, summary_codes, page_count, physical_rows }`, `decode_native_router(...) -> Result<NativeRouterArtifacts>`, and `NativeResidentWorksheet::validate_under_3_gib() -> Result<u64>`.

- [ ] **Step 1: Write schema, order, authentication, and worksheet tests**

  The passing fixture uses exact schemas: codebooks `(kind:Utf8 nonnull, subspace:UInt16 nonnull, codeword:UInt16 nonnull, centroid:FixedSizeList<Float32 nonnull, width=ceil(dimensions/16)> nonnull)`; row codes `(code:FixedSizeList<UInt8 nonnull,16> nonnull)`; summaries `(page:UInt32 nonnull, block:UInt8 nonnull, code:FixedSizeList<UInt8 nonnull,16> nonnull)`. Codebook subspace `s` owns `[floor(s*d/16), floor((s+1)*d/16))`, and unused centroid suffix lanes must be positive zero. Add passing 97d and 768d fixtures plus mutations for every field name/type/nullability, extra/missing columns, row count/order/duplicates, invalid `kind`, non-finite or nonzero padding, incomplete 256-codeword subspace, page gaps, block outside `0..2`, checksum/length drift, and hidden decoded capacity beyond the worksheet.

- [ ] **Step 2: Verify the format RED**

  Run: `cargo test -p borsuk --lib native_ann_format_ -- --nocapture`

  Expected: compile failure only for the missing decoder and worksheet.

- [ ] **Step 3: Implement strict streaming decoders**

  Parse authenticated Parquet metadata first, compare the complete Arrow physical schema, then stream record batches into exactly sized `Vec<u8>`/`Vec<f32>` allocations. Reject before allocation when declared counts overflow, exceed registered counts, or violate the memory worksheet. Do not retain Parquet decoder buffers in `NativeRouterArtifacts`.

- [ ] **Step 4: Implement the exact 100M worksheet**

  Compute each term from authenticated counts and runtime limits, including `row_count*16`, `page_count*2*16`, codebooks/SQ8, mutation entries, resident delta rows, sixteen response permits, workspaces, and `536_870_912` reserve. The exact 100M fixture must equal `2_729_806_368` bytes and reject `>= 3*1024*1024*1024`.

- [ ] **Step 5: Verify and commit**

  Run: `cargo test -p borsuk --lib native_ann_format_ -- --nocapture && cargo fmt --all -- --check && git diff --check`

  ```bash
  git add crates/borsuk/src/native_ann_format.rs crates/borsuk/src/lib.rs
  git commit -m "feat(borsuk): decode authenticated native router artifacts"
  ```

### Task 3: Bounded hierarchical PQ16 router

**Files:**
- Create: `crates/borsuk/src/native_ann_router.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: `crates/borsuk/src/native_ann_router.rs`

**Interfaces:**
- Consumes: `NativeRouterArtifacts` and a finite query slice of exactly `dimensions` f32 values.
- Produces: `NativeRoutePlan { pages: Vec<u32>, candidate_rows: Vec<u64>, estimated_body_bytes: u64 }` through `route_native_query(artifacts, query, limits) -> Result<NativeRoutePlan>`.

- [ ] **Step 1: Write scalar authority and bounded-work tests**

  Add a literal tiny router where exhaustive reconstructed-distance order is hand-computable. Assert two summary scores/page, bounded best-page heap, row ADC only inside retained pages, physical-row/page mapping, unique `(distance,row)` order, page-byte cap, and no allocation proportional to total rows beyond the resident row-code plane. Differential cases cover random fixed seeds, exact ties, signed zero, finite subnormals, reversed block traversal, `P=1/7/32`, row tails, and queries rejected for NaN/infinity/wrong width.

- [ ] **Step 2: Verify the router RED**

  Run: `cargo test -p borsuk --lib native_ann_router_ -- --nocapture`

  Expected: compile failure only for the missing route types/functions.

- [ ] **Step 3: Implement bounded scalar routing**

  Build two `[f32; 4096]` ADC tables per query and retain candidates with fixed-capacity max-heaps keyed by reverse `(distance, ordinal)`. Iterate row codes only for retained pages; never materialize all score pairs. Enforce candidate/page/body limits as errors, not silent truncation.

- [ ] **Step 4: Add SIMD ADC behind one tested dispatch**

  Use existing `simd_control` primitives where possible. Preserve fused/non-fused behavior explicitly and compare every SIMD output/order to the scalar control with a registered maximum numeric delta; exact tie ordering remains `(distance, ordinal)`.

- [ ] **Step 5: Verify and commit**

  Run: `cargo test -p borsuk --lib native_ann_router_ -- --nocapture && cargo fmt --all -- --check && git diff --check`

  ```bash
  git add crates/borsuk/src/native_ann_router.rs crates/borsuk/src/lib.rs
  git commit -m "feat(borsuk): add bounded hierarchical PQ router"
  ```

### Task 4: Snapshot reader with latest-wins delta semantics

**Files:**
- Create: `crates/borsuk/src/native_ann_read.rs`
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: `crates/borsuk/src/native_ann_read.rs`

**Interfaces:**
- Consumes: pinned `NativeAnnRef`, `Storage`, `NativeRouterArtifacts`, existing immutable global-leaf Arrow page ranges, existing mutation-directory state, and resident delta rows.
- Produces: `NativeAnnSnapshot::open(...)`, `NativeAnnSnapshot::search(&[f32], SearchOptions) -> Result<SearchReport>`, and refresh-safe `Arc<NativeAnnSnapshot>` installation in `BorsukIndex`.

- [ ] **Step 1: Write deterministic snapshot/read tests**

  Use `object_store::memory::InMemory` with authenticated artifacts. Cover one pinned generation, the exact nonnullable Arrow page schema `(id:i64, sequence:u64, state:u8, code:fixed-size-list<u8,dimensions>)`, bounded/coalesced range reads that never cross object/run boundaries, exact-body checksum validation, latest sequence wins across base and multiple delta runs, tombstones, stable-ID deduplication, exact `(distance,id)` ordering, refresh swapping only after a complete new snapshot, and rejection of stale/missing mutation-directory entries. Mutate every page field/type/nullability and row order. Assert request/body counters and a 16-MiB hard cap.

- [ ] **Step 2: Verify the reader RED**

  Run: `cargo test -p borsuk --lib native_ann_read_ -- --nocapture`

  Expected: compile failure only for the missing snapshot reader.

- [ ] **Step 3: Implement open and bounded fetch**

  Authenticate the root first; load router/SQ8/mutation directory and bounded delta once; route; convert pages to registered immutable byte ranges; coalesce only adjacent ranges through the existing storage planner; acquire existing byte/request admission permits before every GET; authenticate each decoded Arrow page.

- [ ] **Step 4: Implement merge and final admission**

  Suppress shadowed base IDs before heap admission, score live delta rows in the same metric, discard tombstones, deduplicate by stable ID and highest mutation stamp, then sort/truncate by `(distance,id)`. A refresh prepares an entire candidate snapshot and atomically replaces the old `Arc`; failed refresh leaves the old generation queryable.

- [ ] **Step 5: Verify and commit**

  Run: `cargo test -p borsuk --lib native_ann_read_ -- --nocapture && cargo fmt --all -- --check && git diff --check`

  ```bash
  git add crates/borsuk/src/native_ann_read.rs crates/borsuk/src/index.rs crates/borsuk/src/lib.rs
  git commit -m "feat(borsuk): query pinned native ANN snapshots"
  ```

### Task 5: Query-visible publication, compaction, and atomic pre-release cutover

**Files:**
- Create: `crates/borsuk/src/native_ann_build.rs`
- Modify: `crates/borsuk/src/manifest.rs`
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Modify/Delete: retired global-ANN persisted/read code identified by `rg 'global_ann_ref|GlobalAnnRef|search_resident_global_ann' crates/borsuk/src`
- Test: `crates/borsuk/src/native_ann_build.rs`
- Test: affected integration tests in `crates/borsuk/src/index.rs`

**Interfaces:**
- Consumes: existing WAL/positioned mutation batches, immutable object writer, collection conditional commit, Tasks 1--4 authority/format/router/reader.
- Produces: one production manifest field `native_ann_ref: NativeAnnRef`, query-visible batch acknowledgement, deterministic compaction, and the only dense ANN query path.

- [ ] **Step 1: Write publication and compaction RED tests**

  Assert create-only immutable object writes, generation JSON before conditional `HEAD` publication, acknowledgement only after the successful CAS, losing writers returning conflict without visibility, reopen/refresh observing the exact committed batch, one/ten/one-hundred run query equivalence, deterministic compaction under reversed input enumeration, latest-wins/tombstones, interruption before/after every publication boundary, and no retired layout accepted after cutover. Build the router twice from the same rows under reversed batch/chunk enumeration and require byte-identical codebooks/codes; provide no query or truth input to the builder API, proving training is query-blind.

- [ ] **Step 2: Verify the build/cutover RED**

  Run: `cargo test -p borsuk --lib native_ann_build_ -- --nocapture`

  Expected: tests fail because native publication/cutover is not wired.

- [ ] **Step 3: Implement query-independent build and commit**

  Encode SQ8 page runs plus the two PQ16 router levels from base rows; upload immutable artifacts create-only; write canonical `NativeAnnRef`; conditionally publish through the existing collection commit protocol. Return a mutation report only after the winning head is readable and digest-identical.

- [ ] **Step 4: Implement deterministic bounded compaction**

  Stream input runs in canonical mutation order, retain the highest stamp per ID, remove tombstones from the emitted base, rebuild only the native router/base generation, and publish through the same CAS. Bound prepared vectors, sort/spill runs, response buffers, and decoder capacity by explicit admission budgets.

- [ ] **Step 5: Perform the atomic pre-release format switch**

  Replace `Manifest::global_ann_ref`/cell-card dense serving authority with `native_ann_ref`; route `search_with_report`, bulk-load finalization, refresh, mutation visibility, and compaction only through Tasks 1--4. Delete retired persisted/read branches, serde defaults, compatibility aliases, and benchmark-only production dependencies. Keep historical benchmark crates/artifacts only as evidence, never as runtime readers.

- [ ] **Step 6: Verify focused and repository gates once**

  Run focused gates first:

  ```bash
  cargo test -p borsuk --lib native_ann_ -- --nocapture
  cargo test -p borsuk --lib mutation -- --nocapture
  cargo test -p borsuk --lib compaction -- --nocapture
  cargo fmt --all -- --check
  cargo clippy --locked --workspace --all-targets -- -D warnings
  cargo test --locked --workspace --all-targets
  git diff --check
  ```

  Expected: all commands exit zero; there is one dense production reader and no compatibility path.

- [ ] **Step 7: Commit the cutover**

  ```bash
  git add crates/borsuk/src
  git commit -m "feat(borsuk): cut over to native hierarchical delta reader"
  ```

### Task 6: Fail-fast real-data qualification and promotion

**Files:**
- Create: `scripts/native_ann_qualification.py`
- Create: `scripts/test_native_ann_qualification.py`
- Create: `scripts/native_ann_run_remote.sh`
- Modify: `docs/research/algorithm-first-page-layout-ledger.md`

**Interfaces:**
- Consumes: exact source commit, registered dataset/query/truth authorities, production `BorsukIndex` API, AWS profile `causality`.
- Produces: canonical per-query evidence, recomputed aggregate receipt, resource/traffic/write measurements, 100M memory worksheet, and explicit promote/reject classification.

- [ ] **Step 1: Write validator tests before the runner**

  Require literal dataset/split/source identities, every per-query Recall@10/100 sample, sorted percentiles, error count, page/request/byte counts, cold/reused-client latency separation, batch write/visibility samples, one/ten/one-hundred run equivalence, mutation/compaction evidence, allocation worksheet, Spot instance identity, and terminal/resource receipts. Mutate every aggregate and gate independently and require rejection.

- [ ] **Step 2: Implement and locally verify the validator/runner boundary**

  Run: `python3 -m unittest scripts.test_native_ann_qualification && ruff check scripts/native_ann_qualification.py scripts/test_native_ann_qualification.py && python3 -m py_compile scripts/native_ann_qualification.py scripts/test_native_ann_qualification.py && bash -n scripts/native_ann_run_remote.sh && git diff --check`

- [ ] **Step 3: Run the 100k fail-fast screen**

  On one Spot attempt, require every correctness/mutation/compaction gate and reject immediately if average Recall@100 < 975,000 ppm, p05 < 900,000 ppm, bytes > 16 MiB, or resident projection >= 3 GiB. Do not tune on its held-out cell after opening it.

- [ ] **Step 4: Run the 1M ReLAION qualification**

  Use the frozen 1M×768 development selection and 1,000-query held-out validation split, reporting development only as selection history. Require the complete release contract; compare against the verified V75 baseline (99.272% Recall@100, p50 49.0 ms, p95 137.9 ms, p99 250.1 ms) and vendor claims without treating them as paired measurements.

- [ ] **Step 5: Promote unchanged to Deep Image 9.99M, then decide 100M**

  Only after 1M passes, run the exact revision on Deep Image 9,990,000×96 and compare with verified V82 (98.430% Recall@100, worst 95%, p50 41.6 ms, p95 69.5 ms, 4.2 MiB/query). Start a 100M build only if both datasets pass and the measured complete worksheet remains below 3 GiB.

- [ ] **Step 6: Record evidence and commit**

  Run: `python3 scripts/validate_research_docs.py && git diff --check`

  ```bash
  git add scripts/native_ann_qualification.py scripts/test_native_ann_qualification.py scripts/native_ann_run_remote.sh docs/research/algorithm-first-page-layout-ledger.md
  git commit -m "research: qualify native hierarchical delta reader"
  ```

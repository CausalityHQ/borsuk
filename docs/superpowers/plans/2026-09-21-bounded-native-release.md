# Bounded Native Reader Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the scientifically rejected two-summary PQ16 serving path with the already-measured bounded SQ8/PQ64 reader while preserving authenticated generations, mutation visibility, compaction, and the public BORSUK search API.

**Architecture:** Reuse the existing `native_ann*` module boundaries but make a breaking format-v3 cutover: typed canonical authority, float32 page summaries, PQ64 row codes, SQ8 Arrow pages, explicit memory admission, and one bounded fetch wave. The existing index API dispatches only to this native path; no benchmark manifest or `pq-scan` fallback remains.

**Tech Stack:** Rust 2024, Serde structs, canonical JSON, Apache Parquet, Arrow IPC, `object_store`, Tokio, fixed-size SIMD kernels, SHA-256, existing BORSUK storage/admission/WAL primitives, AWS Spot qualification.

**Spec:** `docs/superpowers/specs/2026-09-21-bounded-native-release-design.md`

## Global Constraints

- Pre-release format replacement: no legacy reader, migration layer, alias, or dual write.
- Persistent production objects are typed canonical JSON, Parquet, or Arrow IPC; production `serde_json::json!`, native binary layouts, NumPy, and pickle are forbidden.
- Queries and ground truth never appear in index artifacts.
- One fetch wave, no serving fallback, no nested Rayon, no per-query work-stealing pool, and no unbounded task queue.
- Qualified gates are Recall@10 >= 960,000 ppm, average Recall@100 >= 975,000 ppm, p05 Recall@100 >= 900,000 ppm, cold p95 <= 250 ms, and zero failed queries.
- Every open computes a complete resident-memory worksheet and rejects before allocation when it exceeds the configured budget.
- Heavy compilation and real-data gates run on `causality` Spot; local commands remain narrow and stop above 3 GiB RSS or memory PSI full avg10 > 0.5.
- Each paid cell has one immutable commit, one attempt, canonical per-query evidence, independent recomputation, immediate instance termination, and explicit scratch cleanup.

## Review Focus

- A canonical root with valid bytes but a cross-role URI/digest/length reuse must fail before child-object semantic parsing; Task 1 mutates every role and reuse boundary.
- A router whose decoded capacity or Parquet decoder lifetime escapes the worksheet must fail before allocation; Task 1 measures materialized capacities and Task 2 verifies runtime permits.
- Concurrent queries must not create nested Rayon or block Tokio I/O workers; Task 2 uses a fixed executor/permit stress test and a saturation rejection test.
- Pending WAL rows and tombstones must remain visible when routing selects no committed page containing that ID; Task 3 locks overlay-before-final-admission behavior.
- A builder or compactor must never publish a root whose SQ8/page/router identities disagree; Task 4 injects stops at every immutable-upload and head-CAS boundary.

---

### Task 1: Format-v3 authority, typed router objects, and exact memory admission

**Files:**
- Modify: `crates/borsuk/src/native_ann.rs`
- Modify: `crates/borsuk/src/native_ann_format.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: `crates/borsuk/src/native_ann.rs`
- Test: `crates/borsuk/src/native_ann_format.rs`

**Interfaces:**
- Consumes: `VectorMetric`, `Storage`, authenticated object bytes, and existing mutation/run references.
- Produces: `NativeAnnRef` format version 3, `NativeBoundedRouterRef`, `NativeBoundedRouterArtifacts`, `decode_native_bounded_router(...)`, and `NativeResidentWorksheet::validate(configured_budget_bytes) -> Result<u64>`.

- [ ] **Step 1: Write the format-v3 authority RED**

  Replace the passing authority fixture with a literal format-v3 root containing `route-summaries`, `route-codebooks`, `route-row-codes`, page directory, mutation directory, base runs, delta runs, SQ8 low/step authority, route limits, and configured resident budget. Add table-driven mutations for format version, role, URI, SHA-256, length, duplicate URI, generation/predecessor, dimensions, metric, page rows, PQ width 64, summary blocks, shortlist, coalescing gap, response limits, and resident budget.

  ```rust
  #[test]
  fn native_bounded_authority_rejects_every_identity_shape_and_budget_drift() {
      let canonical = valid_bounded_authority();
      assert_eq!(native_ann_root_from_bytes(&native_ann_root_bytes(&canonical).unwrap()).unwrap(), canonical);
      for mutated in authority_mutations(&canonical) {
          assert!(mutated.validate().is_err());
      }
  }
  ```

- [ ] **Step 2: Run the authority RED remotely**

  Run: `cargo test -p borsuk --lib native_bounded_authority_ -- --nocapture`

  Expected: compile failures only for the missing format-v3 router/SQ8 types and constants.

- [ ] **Step 3: Implement typed format-v3 authority**

  Define Serde structs with `#[serde(deny_unknown_fields)]`; serialize through the existing recursive lexicographic canonicalizer with one trailing LF. Delete the PQ16/two-summary constants. Validate exact lower-case digests, absolute object URIs, unique roles/URIs, nonzero lengths, contiguous run ordinals, ordered nonoverlapping mutation ranges, `page_rows == 256`, `pq_width == 64`, nonzero fixed limits, and checked page/row counts.

  ```rust
  pub(crate) struct NativeBoundedRouterRef {
      pub(crate) pq_width: u8,
      pub(crate) summary_blocks_per_page: u8,
      pub(crate) physical_rows: u64,
      pub(crate) page_count: u32,
      pub(crate) summaries: NativeArtifactRef,
      pub(crate) codebooks: NativeArtifactRef,
      pub(crate) row_codes: NativeArtifactRef,
      pub(crate) limits: NativeBoundedRouteLimits,
  }
  ```

- [ ] **Step 4: Write strict Parquet and worksheet REDs**

  Use 97d and 768d fixtures. Mutate every field name, physical type, outer/child nullability, list width, finite value, row count, page/block order, codeword order, hidden vector capacity, and object authentication field. Assert a 1M worksheet exact total and that one-byte-over-budget fails before decoder materialization.

- [ ] **Step 5: Run the format RED remotely**

  Run: `cargo test -p borsuk --lib native_bounded_format_ -- --nocapture`

  Expected: failures only for missing strict decoders and budget-aware worksheet.

- [ ] **Step 6: Implement strict streaming materialization**

  Parse Parquet metadata and compare the full Arrow physical schema before allocations. Stream into exactly sized `Box<[f32]>`/`Box<[u8]>`, shrink before return, and drop builders/batches. Compute resident terms from authenticated counts, actual materialized capacities, cache limit, response permits, fixed workspaces, delta rows, and runtime reserve using checked arithmetic.

- [ ] **Step 7: Verify and commit Task 1**

  Run remotely, serially: `cargo test -p borsuk --lib native_bounded_authority_ -- --nocapture` then `cargo test -p borsuk --lib native_bounded_format_ -- --nocapture`

  Run locally: `cargo fmt --all -- --check && git diff --check`

  ```bash
  git add crates/borsuk/src/native_ann.rs crates/borsuk/src/native_ann_format.rs crates/borsuk/src/lib.rs
  git commit -m "feat(borsuk): define bounded native format"
  ```

### Task 2: Bounded PQ64 router and fixed CPU/I/O execution

**Files:**
- Modify: `crates/borsuk/src/native_ann_router.rs`
- Modify: `crates/borsuk/src/native_ann_read.rs`
- Test: `crates/borsuk/src/native_ann_router.rs`
- Test: `crates/borsuk/src/native_ann_read.rs`

**Interfaces:**
- Consumes: `NativeBoundedRouterArtifacts`, a finite query slice, and `NativeBoundedRouteLimits`.
- Produces: `route_native_bounded_query(...) -> Result<NativeRoutePlan>` and fixed `NativeCpuAdmission`/`ByteAdmissionGate` ownership for each query.

- [ ] **Step 1: Write scalar-authority, SIMD, and bounded-work REDs**

  Build hand-computable page summaries and PQ64 codes. Assert exact scalar ranking, `(distance,ordinal)` ties, summary frontier, row shortlist, unique page order, one-wave byte estimate, random/tie/subnormal/reversed-block SIMD equality, and no population-sized score vector. Add saturation tests proving the configured active CPU permits and waiters never grow.

  ```rust
  #[test]
  fn native_bounded_router_matches_scalar_and_rejects_saturated_work() {
      let fixture = bounded_router_fixture();
      assert_eq!(route_native_bounded_query(&fixture.artifacts, &fixture.query, fixture.limits).unwrap(), fixture.expected);
      assert!(fixture.cpu_gate.acquire_bounded().is_some());
      assert!(fixture.cpu_gate.acquire_bounded().is_none());
  }
  ```

- [ ] **Step 2: Run the router RED remotely**

  Run: `cargo test -p borsuk --lib native_bounded_router_ -- --nocapture`

  Expected: failures at the missing PQ64 route and CPU admission APIs.

- [ ] **Step 3: Implement fixed-block PQ64 routing**

  Score summaries into a fixed-capacity max heap, score PQ64 rows only inside retained pages, retain shortlist rows with a fixed heap, map to unique pages, and coalesce only registered adjacent ranges. Use the existing SIMD primitive with one scalar control. Delete PQ16 serving functions rather than leaving a fallback.

- [ ] **Step 4: Isolate CPU from async I/O**

  Route and page-score work executes under a collection-scoped fixed CPU permit. Range reads keep the existing bounded completion wave and byte admission. Do not invoke Rayon from a query. Saturation returns a typed capacity error; it does not spawn work or block a Tokio I/O thread.

- [ ] **Step 5: Verify and commit Task 2**

  Run remotely: `cargo test -p borsuk --lib native_bounded_router_ -- --nocapture`

  Run locally: `cargo fmt --all -- --check && git diff --check`

  ```bash
  git add crates/borsuk/src/native_ann_router.rs crates/borsuk/src/native_ann_read.rs
  git commit -m "feat(borsuk): route bounded native queries"
  ```

### Task 3: SQ8 snapshot search with latest-wins overlays

**Files:**
- Modify: `crates/borsuk/src/native_ann_read.rs`
- Modify: `crates/borsuk/src/index.rs`
- Test: `crates/borsuk/src/native_ann_read.rs`
- Test: `crates/borsuk/src/index.rs`

**Interfaces:**
- Consumes: pinned format-v3 generation, SQ8 page objects, native mutation directory, resident delta, and pending WAL overlay.
- Produces: `NativeAnnSnapshot::search_with_overlay(...) -> Result<NativeSearchOutcome>` with exact counters and no fallback.

- [ ] **Step 1: Write strict SQ8 and visibility REDs**

  Change the page fixture to `(id:Binary, sequence:UInt64, state:UInt8, code:FixedSizeList<UInt8,dimensions>)`. Cover checksum, full schema/nullability, SQ8 low/step binding, finite decoded scores, pending put visibility, pending tombstone suppression, immutable delta latest-wins, stable-ID deduplication, exact ties, one-wave physical GETs, and generation refresh failure isolation.

- [ ] **Step 2: Run the snapshot RED remotely**

  Run: `cargo test -p borsuk --lib native_bounded_snapshot_ -- --nocapture`

  Expected: failures at the old float32 page schema/decoder and missing bounded counters.

- [ ] **Step 3: Implement authenticated SQ8 decode and merge**

  Authenticate every response slice before Arrow decode, decode only registered rows, compute squared distance from the exact low/step authority, suppress shadowed base rows before heap admission, score live resident delta and WAL rows, discard tombstones, retain highest sequence, and return `(distance,id)` order.

- [ ] **Step 4: Cut index dispatch to format-v3 only**

  The approximate search path uses the loaded bounded native snapshot whenever a valid format-v3 head exists. A missing or incompatible native root is a typed error. Remove the runtime branch to `pq-scan`; exact/flat search remains an explicit user-selected mode, never an implicit fallback.

- [ ] **Step 5: Verify and commit Task 3**

  Run remotely: `cargo test -p borsuk --lib native_bounded_ -- --nocapture`

  Run locally: `cargo fmt --all -- --check && git diff --check`

  ```bash
  git add crates/borsuk/src/native_ann_read.rs crates/borsuk/src/index.rs
  git commit -m "feat(borsuk): search bounded native snapshots"
  ```

### Task 4: Query-blind build, publication, and deterministic compaction

**Files:**
- Modify: `crates/borsuk/src/native_ann_build.rs`
- Modify: `crates/borsuk/src/manifest.rs`
- Modify: `crates/borsuk/src/index.rs`
- Test: `crates/borsuk/src/native_ann_build.rs`
- Test: `crates/borsuk/src/index.rs`

**Interfaces:**
- Consumes: bounded source blocks, native build configuration, existing conditional storage publication, and mutation runs.
- Produces: format-v3 immutable objects/root, create-only staging, conditional head publication, and byte-deterministic compaction.

- [ ] **Step 1: Write build/publication REDs**

  Assert query-blind construction, deterministic summary/PQ64/SQ8 output under reversed input block enumeration, exact object identities, memory-bound rejection, latest-wins compaction, and injected stops after every immutable object, root upload, and before/after head CAS. Assert no production artifact contains query/truth roles or bytes.

- [ ] **Step 2: Run the build RED remotely**

  Run: `cargo test -p borsuk --lib native_bounded_build_ -- --nocapture`

  Expected: failures at the old PQ16/f32 builder and format-v2 publication.

- [ ] **Step 3: Implement format-v3 construction and atomic publication**

  Stream source blocks into bounded external runs, build page summaries and PQ64 codes query-independently, encode SQ8 Arrow pages, create Parquet router objects, authenticate all bytes, upload immutable objects create-only, upload the canonical root, and publish visibility only through conditional head update.

- [ ] **Step 4: Delete retired persisted/read paths**

  Remove format-v2 native constants/fixtures and any automatic `pq-scan` fallback reachable from approximate search. Keep historical benchmark crates and evidence read-only; production does not import them.

- [ ] **Step 5: Verify and commit Task 4**

  Run remotely: `cargo test -p borsuk --lib native_bounded_ -- --nocapture`

  Run locally: `cargo fmt --all -- --check && git diff --check`

  ```bash
  git add crates/borsuk/src/native_ann_build.rs crates/borsuk/src/manifest.rs crates/borsuk/src/index.rs
  git commit -m "feat(borsuk): publish bounded native generations"
  ```

### Task 5: 100k semantic and fail-fast quality gate

**Files:**
- Create: `scripts/run_bounded_native_100k.py`
- Create: `scripts/validate_bounded_native_result.py`
- Create: `scripts/test_validate_bounded_native_result.py`
- Modify: `docs/research/borsuk-benchmark-table-v1.parquet`
- Modify: `docs/research/borsuk-benchmark-table-v1.md`

**Interfaces:**
- Consumes: clean immutable source commit, frozen ReLAION 100k corpus/query/truth authorities, and main-crate public API.
- Produces: canonical per-query Parquet evidence, canonical JSON receipt, independent validation, and a measured table row.

- [ ] **Step 1: Write validator REDs before the runner**

  Mutate query ordinal, returned-ID order, hits, recall, latency, GETs, bytes, generation, source commit, object identities, 1/10/100 equivalence, mutation visibility, compaction equivalence, and aggregate percentiles. The validator independently recomputes every aggregate from per-query rows and rejects missing/extra schema fields.

- [ ] **Step 2: Run validator RED and GREEN locally**

  Run: `python3 -m unittest scripts.test_validate_bounded_native_result`

  Expected RED: missing validator functions. Expected GREEN after minimal implementation: every mutation rejected and canonical fixture accepted.

- [ ] **Step 3: Implement the one-shot 100k runner**

  Use typed dataclasses/Serde-compatible dictionaries only at the script boundary, never production JSON magic. Run exact control plus bounded native on the same 1,000 queries, then one/ten/one-hundred-run, pending put/delete, reopen, and compaction equivalence. Record build time, index bytes, bytes/vector, RSS, latency, QPS, GETs, bytes/query, recall distribution, hardware, command, commit, seed, repetitions, CI metadata, cost, and raw paths.

- [ ] **Step 4: Run one Causality Spot cell and decide**

  Launch one preregistered Spot attempt with a two-hour wall cap. Stop at the first failing quality, equivalence, memory, or authority gate. Terminate immediately and validate from the immutable terminal receipt.

- [ ] **Step 5: Update the tables and commit**

  Update the Parquet table and concise Markdown table with measured/estimated/blocked status on every field. Do not infer a 100k-to-1M slope across different formats.

  ```bash
  git add scripts/run_bounded_native_100k.py scripts/validate_bounded_native_result.py scripts/test_validate_bounded_native_result.py docs/research/borsuk-benchmark-table-v1.parquet docs/research/borsuk-benchmark-table-v1.md
  git commit -m "bench: qualify bounded native reader at 100k"
  ```

### Task 6: One immutable ReLAION-1M release qualification

**Files:**
- Modify: `docs/research/borsuk-benchmark-table-v1.parquet`
- Modify: `docs/research/borsuk-benchmark-table-v1.md`
- Modify: `docs/research/algorithm-first-page-layout-ledger.md`

**Interfaces:**
- Consumes: the exact unchanged Task 5 commit, frozen ReLAION-1M corpus and 1,000-query development split, and the authenticated validator.
- Produces: one terminal 1M receipt, updated tables, a release decision, and either a release-candidate tag or finite closeout.

- [ ] **Step 1: Freeze and preregister the exact cell**

  Record source commit, artifact identities, instance type/AMI/region, seed, route limits, memory budget, concurrency cells `(1,32,128)`, two query passes, two-hour cap, Spot interruption rule, scratch paths, cleanup, and terminal URI before launch.

- [ ] **Step 2: Run exactly one Spot attempt**

  Measure build time, index bytes/bytes-vector, peak RSS, cold/reused p50/p95/p99, QPS c1/32/128, GET distribution, bytes/query distribution, Recall@10, average/p05/worst Recall@100, and total cost. An interruption discards the attempt and follows the registered restart rule; a scientific failure is terminal and is not retuned.

- [ ] **Step 3: Independently validate and classify**

  Run the validator against immutable samples and receipt. Pass only when all release gates and authentication checks pass. Otherwise publish the exact limiting gate and retain the strongest honest bounded mode as non-promoted evidence.

- [ ] **Step 4: Run final release assurance remotely**

  Run once: `cargo fmt --all -- --check && cargo clippy --locked --workspace --all-targets -- -D warnings && cargo test --locked --workspace --all-targets`

  Run docs/static: `python3 scripts/validate_research_docs.py && git diff --check`

- [ ] **Step 5: Commit, push, and tag or close out**

  Fast-forward push the verified revision. If the 1M gate passes, create one annotated release-candidate tag tied to the receipt hash. If it fails, create one annotated closeout tag tied to the limiting receipt; do not open a successor architecture in this plan.

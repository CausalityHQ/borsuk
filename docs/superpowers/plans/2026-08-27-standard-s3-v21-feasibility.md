# Standard-S3 V21 Selector Feasibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a claim-ineligible, read-only diagnostic that projects the V21 resident selector and one-wave exact plan from an authenticated V20 generation, then proves whether real Deep Image geometry can meet the V21 memory, recall, request, and byte gates before changing the persistent format.

**Architecture:** Decode authenticated V20 exact pages once on the build-class diagnostic host, merge only contiguous pages from the same cell and group into candidate V21 bundles, derive deterministic region representatives from the generation codebook, and evaluate the frozen arm matrix against routed publication queries. The diagnostic never mutates the index, never publishes a V21 manifest, and emits receipt-bound raw evidence that is explicitly ineligible for a performance claim.

**Tech Stack:** Rust 2024, Arrow IPC, existing BORSUK global router/quantizer and exact metric kernels, Python 3.12 publication authority, AWS S3 Standard and EC2 Spot.

**Spec:** `docs/superpowers/specs/2026-08-27-standard-s3-v21-cold-read-design.md`

## Global Constraints

- S3 Standard only; no S3 Express, local serving tier, or query-populated object cache.
- Deep Image gates remain recall@10 `>= 0.975`, selector GT coverage `>= 0.990`, at most four actual GETs, at most `1 MiB`, decoded directory at most `40,000,000` bytes, and projected peak RSS at most `768 MiB`.
- The diagnostic matrix is exactly bundle row targets `{128, 256}`, selector spans `{32, 64}`, and hedge delays `{off, 20 ms, 35 ms}`; a hedge reserves one of four physical request slots.
- The diagnostic is read-only and claim-ineligible. It must not publish a V21 root, modify a manifest, or validate as an ordinary recall result.
- Existing V20 build, read, write, WAL, compaction, and publication behavior must remain byte-for-byte unchanged when the diagnostic is absent.
- Every production behavior follows RED, observed expected failure, minimal GREEN, and focused regression before the next behavior.

---

## File Structure

- Create `crates/borsuk/src/v21_feasibility.rs`: arm authority, projected directory types, deterministic bundle/region construction, scoring, planning, and capacity accounting.
- Modify `crates/borsuk/src/lib.rs`: register the private module and export only the hidden diagnostic request/report types needed by the benchmark.
- Modify `crates/borsuk/src/global_pq_sidecar.rs`: expose one crate-private prepared-code scoring seam shared by V20 and the diagnostic.
- Modify `crates/borsuk/src/index.rs`: authenticated V20 exact-page projection and the read-only `diagnose_v21_selector_feasibility` entrypoint.
- Modify `crates/borsuk/examples/production_bench.rs`: bounded environment/config authority and canonical raw diagnostic CSV writers.
- Modify `scripts/run_publication_v3_cell.py`: invoke and semantically validate the exact V21 matrix as a claim-ineligible document.
- Modify `scripts/publication_v3_execution.py`: upload raw V21 diagnostic artifacts and bind their digests in the terminal receipt.
- Modify `scripts/publication_v3_controller.py`: add the bounded `diagnose-v21-selector` execution path using an existing completed build.
- Modify `scripts/test_run_publication_v3_cell.py`, `scripts/test_publication_v3_execution.py`, and `scripts/test_publication_v3_controller.py`: publication authority and mutation tests.

---

### Task 1: Arm Authority and Quantizer Scoring Seam

**Files:**
- Create: `crates/borsuk/src/v21_feasibility.rs`
- Modify: `crates/borsuk/src/lib.rs:1-40`
- Modify: `crates/borsuk/src/global_pq_sidecar.rs:169-280`
- Test: `crates/borsuk/src/v21_feasibility.rs` (`#[cfg(test)]` module)

**Interfaces:**
- Produces:
  - `pub struct V21FeasibilityArm { pub bundle_row_limit: u16, pub selector_span: u16, pub hedge_delay_ms: Option<u16> }`
  - `impl V21FeasibilityArm { pub fn validate(&self) -> Result<()>; pub(crate) fn primary_request_limit(&self) -> usize; }`
  - `GlobalScanQuantizer::score_codes(&self, query: &[f32], codes: impl IntoIterator<Item=&[u8]>) -> Result<Vec<f32>>`
- Consumes: existing `BorsukError`, `Result`, and `GlobalScanQuantizer`.

- [ ] **Step 1: Write the failing arm-authority tests**

Add tests whose wished-for API is:

```rust
#[test]
fn v21_feasibility_arm_accepts_only_the_frozen_matrix() {
    for bundle_row_limit in [128, 256] {
        for selector_span in [32, 64] {
            for hedge_delay_ms in [None, Some(20), Some(35)] {
                V21FeasibilityArm { bundle_row_limit, selector_span, hedge_delay_ms }
                    .validate()
                    .unwrap();
            }
        }
    }
    assert_eq!(
        V21FeasibilityArm { bundle_row_limit: 256, selector_span: 64, hedge_delay_ms: None }
            .primary_request_limit(),
        4
    );
    assert_eq!(
        V21FeasibilityArm { bundle_row_limit: 256, selector_span: 64, hedge_delay_ms: Some(20) }
            .primary_request_limit(),
        3
    );
}

#[test]
fn v21_feasibility_arm_rejects_unregistered_values_and_bool_like_zeroes() {
    for arm in [
        V21FeasibilityArm { bundle_row_limit: 0, selector_span: 32, hedge_delay_ms: None },
        V21FeasibilityArm { bundle_row_limit: 192, selector_span: 32, hedge_delay_ms: None },
        V21FeasibilityArm { bundle_row_limit: 256, selector_span: 16, hedge_delay_ms: None },
        V21FeasibilityArm { bundle_row_limit: 256, selector_span: 64, hedge_delay_ms: Some(25) },
    ] {
        assert!(arm.validate().is_err());
    }
}
```

- [ ] **Step 2: Run the tests and observe the missing module/types failure**

Run: `rtk proxy cargo test -p borsuk v21_feasibility_arm --lib -- --nocapture`

Expected: compilation fails because `V21FeasibilityArm` and the module do not exist.

- [ ] **Step 3: Implement the exact arm authority**

Create the module, use literal `matches!` validation for the registered values, and calculate the primary limit as `4 - usize::from(hedge_delay_ms.is_some())`. Do not accept a general integer range.

- [ ] **Step 4: Add the failing shared-scoring test**

Extend an existing quantizer fixture to assert that `GlobalScanQuantizer::score_codes(query, codes)` produces exactly the same vector as `ResidentGlobalPq::score_cell_card_codes` for the same codebook/query/codes.

- [ ] **Step 5: Run RED, add the minimal wrapper, and run GREEN**

Run RED: `rtk proxy cargo test -p borsuk v21_quantizer_score_codes_matches_resident_scoring --lib -- --nocapture`

Implement `score_codes` as one prepared query plus ordered calls to the existing `distance`; do not add another metric implementation.

Run GREEN: `rtk proxy cargo test -p borsuk v21_ --lib -- --nocapture`

- [ ] **Step 6: Commit**

```bash
git add crates/borsuk/src/lib.rs crates/borsuk/src/global_pq_sidecar.rs crates/borsuk/src/v21_feasibility.rs
git commit -m "Add V21 feasibility arm authority"
```

---

### Task 2: Deterministic Projected Bundle Directory

**Files:**
- Modify: `crates/borsuk/src/v21_feasibility.rs`
- Test: `crates/borsuk/src/v21_feasibility.rs`

**Interfaces:**
- Consumes: `V21FeasibilityArm`, `GlobalScanQuantizer`, `VectorElementType`, canonical decoded V20 pages.
- Produces:
  - `pub(crate) struct V21ProjectedPage { cell_index: u32, leaf_ordinal: u32, group_ordinal: u32, group_path: String, group_checksum: [u8; 32], offset: u64, physical_bytes: u64, rows: Vec<V21ProjectedRow> }`
  - `pub(crate) struct V21ProjectedRow { id: RecordId, source_ordinal: u64, code: Vec<u8>, exact: Vec<u8> }`
  - `pub(crate) struct V21ProjectedDirectory { bundles: Vec<V21ProjectedBundle>, selector_capacity_bytes: u64, diagnostic_working_set_bytes: u64, rows: u64, regions: u64 }`
  - `build_v21_projected_directory(pages, dimensions, element_type, normalize, quantizer, arm) -> Result<V21ProjectedDirectory>`

- [ ] **Step 1: Write the failing bundle construction tests**

Use a hand fixture with two cells, two group ordinals, shuffled input pages, and row counts `128 + 128 + 1`. Assert:

```rust
assert_eq!(directory.bundle_row_counts(), [256, 1]);
assert_eq!(directory.region_row_counts(), [64, 64, 64, 64, 1]);
assert_eq!(directory.canonical_ids(), expected_ids);
```

Also assert that pages from different cells, groups, noncontiguous offsets, or nonconsecutive leaf ordinals never merge, and that 768-dimensional float rows derive a shorter fitting bundle than 256.

- [ ] **Step 2: Run RED**

Run: `rtk proxy cargo test -p borsuk v21_projected_directory --lib -- --nocapture`

Expected: compilation fails on missing projected types/functions.

- [ ] **Step 3: Implement canonical page merge and region derivation**

Sort pages by `(cell_index, group_ordinal, leaf_ordinal, offset)`. Merge adjacent pages only when all authority fields are contiguous and the merged rows satisfy the arm row target, 96-KiB exact payload, and 128-KiB encoded-bundle cap. Divide the final row sequence with `chunks(selector_span)`.

For each region, decode exact geometry in canonical row order, accumulate each component sequentially in f64, normalize when the metric requires it, encode the centroid with the generation quantizer, score the region's already-authenticated row codes through `GlobalScanQuantizer::score_codes`, and round the finite maximum outward to f16. Reject non-finite inputs and an f16 round-down that fails to cover the f32 score.

- [ ] **Step 4: Write and satisfy deterministic/capacity tests**

Add a permutation test that reverses authenticated page input order while preserving each page's authenticated internal row order, then asserts byte-identical representative codes, f16 bits, spans, and bundle ordering. Add `selector_capacity_bytes` tests that calculate the capacities of exactly the production SoA group dictionary, fixed bundle columns, code, spread, span, and cell-offset slabs. Exact vectors and IDs retained only to score the diagnostic are reported separately as `diagnostic_working_set_bytes` and may never be counted as selector authority.

Run: `rtk proxy cargo test -p borsuk v21_projected --lib -- --nocapture`

- [ ] **Step 5: Commit**

```bash
git add crates/borsuk/src/v21_feasibility.rs
git commit -m "Build deterministic V21 projected directories"
```

---

### Task 3: Incremental Four-Request Feasibility Planner

**Files:**
- Modify: `crates/borsuk/src/v21_feasibility.rs`
- Test: `crates/borsuk/src/v21_feasibility.rs`

**Interfaces:**
- Consumes: projected directory, routed cell indexes, query, arm.
- Produces:
  - `pub(crate) enum V21LimitingBound { Exhausted, Requests, Bytes, Amplification, FirstBundle }`
  - `pub(crate) struct V21FeasibilityRead { group_ordinal: u32, range: Range<u64>, selected_bytes: u64, bundle_indexes: Vec<u32> }`
  - `pub(crate) struct V21FeasibilityPlan { selected_bundle_indexes: Vec<u32>, reads: Vec<V21FeasibilityRead>, selected_rows: u32, maximum_actual_requests: usize, selected_bytes: u64, physical_bytes: u64, limiting_bound: V21LimitingBound }`
  - `plan_v21_feasibility_query(directory, routed_cells, query, quantizer, arm) -> Result<V21FeasibilityPlan>`

- [ ] **Step 1: Write failing hand-derived ranking and planning tests**

Create regions whose prepared scores and spreads produce an exact expected adjusted order. Include shuffled physical order and the poisoning shape from the V20 planner regressions: a low-ranked middle range may not enlarge or split the accepted higher-ranked prefix. Assert literal selected bundle indexes, reads, rows, bytes, and limiting bound.

- [ ] **Step 2: Run RED**

Run: `rtk proxy cargo test -p borsuk v21_feasibility_plan --lib -- --nocapture`

Expected: compilation failure on the absent planner.

- [ ] **Step 3: Implement monotone incremental planning**

Score all regions in routed cells once, reduce to one adjusted bundle score, and stable-sort bundles. Maintain mutable coalesced ranges only for the accepted prefix. Before committing each bundle, clone or transactionally stage the small range state; reject it if the resulting primary reads exceed `arm.primary_request_limit()`, physical bytes exceed `1_048_576`, amplification exceeds 2x, or a coalesced range exceeds 1,048,576 bytes. Never let a rejected bundle mutate accepted state.

- [ ] **Step 4: Add hedge and starvation tests**

Prove `None` permits four primary reads, a 20/35-ms hedge permits three plus at most one duplicate, and no outcome exceeds four actual GETs. Assert a byte-limited plan reports `Bytes` and candidate starvation rather than claiming the target was met.

Run: `rtk proxy cargo test -p borsuk v21_feasibility_plan --lib -- --nocapture`

- [ ] **Step 5: Commit**

```bash
git add crates/borsuk/src/v21_feasibility.rs
git commit -m "Plan bounded V21 feasibility waves"
```

---

### Task 4: Authenticated Read-Only Index Diagnostic

**Files:**
- Modify: `crates/borsuk/src/index.rs:18220-19110,29640-30120`
- Modify: `crates/borsuk/src/lib.rs`
- Modify: `crates/borsuk/src/v21_feasibility.rs`
- Test: `crates/borsuk/src/index.rs` (`#[cfg(test)]` module)

**Interfaces:**
- Consumes: a V20 `BorsukIndex`, frozen queries, UTF-8 ground-truth IDs, ordinary `SearchOptions`, and exact arm list.
- Produces:
  - `pub struct V21FeasibilityQuerySample { arm_index, query_index, routed_cells, selected_rows, selected_bundles, primary_requests, maximum_actual_requests, selected_bytes, physical_bytes, gt_hits, recall_hits, limiting_bound }`
  - `pub struct V21FeasibilityReport { arm, bundle_count, region_count, projected_directory_bytes, rows, samples }`
  - `#[doc(hidden)] pub fn BorsukIndex::diagnose_v21_selector_feasibility(&self, queries: &[Vec<f32>], ground_truth: &[Vec<String>], options: &SearchOptions, arms: &[V21FeasibilityArm]) -> Result<Vec<V21FeasibilityReport>>`

- [ ] **Step 1: Write the failing no-mutation integration test**

Build a tiny V20 index in the existing instrumented object store, snapshot manifest bytes and object roster, call the wished-for method, and assert exact arm-major/query-major samples. Afterward assert manifest bytes, object roster, request-independent index reads, and visible search results are unchanged.

- [ ] **Step 2: Run RED**

Run: `rtk proxy cargo test -p borsuk v21_feasibility_diagnostic --lib -- --nocapture`

Expected: compilation fails because the diagnostic method and report types do not exist.

- [ ] **Step 3: Implement authenticated projection**

Load the pinned V20 root/codebook through existing validation. Stream one complete authenticated V20 group at a time directly from backing storage through a diagnostic read that neither consults nor populates the local cache; validate its code plane and every exact block before joining their independently authenticated rows and codes. Preserve group/card/leaf authority and canonical source order, then release the group bytes before reading the next group. Store projected row payloads behind shared immutable ownership so the twelve arm constructions cannot deep-copy the corpus. Reuse `resident_global_selected_cells` for routing. Record an arm's exact selector capacity even when it exceeds `40,000,000` bytes; mark that arm over-gate rather than aborting the remaining matrix.

For each query, call the pure planner, compute GT bundle coverage by ID before exact scoring, exact-score all selected rows with the existing metric kernel, and compute final top-10 hits with the existing stable tie break. Reject mismatched query/truth counts, duplicate truth IDs, invalid k, any non-V20 source generation, and any storage/request error.

- [ ] **Step 4: Add failure and memory tests**

Mutate one exact checksum and require failure before a sample is emitted. Exercise a projected directory over the configured cap. Assert all transient permits and decoded pages are released after success and error and no request remains active.

Run: `rtk proxy cargo test -p borsuk v21_feasibility --lib -- --nocapture`

- [ ] **Step 5: Commit**

```bash
git add crates/borsuk/src/index.rs crates/borsuk/src/lib.rs crates/borsuk/src/v21_feasibility.rs
git commit -m "Diagnose V21 selector feasibility"
```

---

### Task 5: Canonical Benchmark Artifacts

**Files:**
- Modify: `crates/borsuk/examples/production_bench.rs:40-180,2800-3800,5680-5850`
- Test: `crates/borsuk/examples/production_bench.rs` (`#[cfg(test)]` module)

**Interfaces:**
- Consumes: `BORSUK_BENCH_V21_FEASIBILITY=1` and the fixed matrix; current dataset queries/ground truth and completed V20 index.
- Produces:
  - `bench_v21_feasibility_arms.csv`
  - `bench_v21_feasibility_samples.csv`
  - `bench_v21_feasibility_summary.json`

- [ ] **Step 1: Write failing config and schema tests**

Assert the mode is mutually exclusive with build, ordinary recall, concurrency, lifecycle, and write measurement. Assert its resolved matrix has exactly 12 arm rows in bundle-major, span-major, hedge-major order and cannot be overridden by ambient candidate/nprobe values.

- [ ] **Step 2: Run RED**

Run: `rtk proxy cargo test -p borsuk --example production_bench v21_feasibility -- --nocapture`

Expected: failures on absent environment/config fields and writers.

- [ ] **Step 3: Implement canonical writers and semantic self-validation**

Write arm rows and query samples in exact arm-major/query-major order. Summary JSON must use `allow_nan = false` equivalent serde behavior, include schema `borsuk-v21-selector-feasibility-v1`, `claim_eligible:false`, exact source/index/dataset/query identities, actual capacity totals, coverage/recall minima, all limit maxima, and eligible arm indexes. Before rename, parse the temporary files and recompute every aggregate from samples; publish with the existing exclusive temp/link/fsync pattern.

- [ ] **Step 4: Add mutation and no-clobber tests**

Mutate every identifier, numeric bound, ordering field, aggregate, matrix cell, and finite float; require rejection. Assert an existing destination is rejected before index open or S3 work and post-link fsync failure preserves the validated output.

Run: `rtk proxy cargo test -p borsuk --example production_bench v21_feasibility -- --nocapture`

- [ ] **Step 5: Commit**

```bash
git add crates/borsuk/examples/production_bench.rs
git commit -m "Emit V21 feasibility evidence"
```

---

### Task 6: Publication V3 Claim-Ineligible Execution

**Files:**
- Modify: `scripts/run_publication_v3_cell.py`
- Modify: `scripts/publication_v3_execution.py`
- Modify: `scripts/publication_v3_controller.py`
- Test: `scripts/test_run_publication_v3_cell.py`
- Test: `scripts/test_publication_v3_execution.py`
- Test: `scripts/test_publication_v3_controller.py`

**Interfaces:**
- Consumes: an exact completed Deep Image build receipt and `diagnose-v21-selector`.
- Produces: namespace `runtime-v21-feasibility/arms/0000/attempts/NNNN`, three raw artifacts, one canonical result, and a terminal receipt binding all four SHA-256 digests with `claim_eligible:false`.

- [ ] **Step 1: Write failing controller/execution tests**

Assert the operation reuses the canonical completed build, uses Spot and the registered runtime RAM/timeout, has a distinct namespace, passes only `BORSUK_BENCH_V21_FEASIBILITY=1`, uploads all artifacts, and terminates on complete/failure/timeout. Raw generated/unfrozen authority must fail before AWS.

- [ ] **Step 2: Run RED**

Run: `.venv/bin/pytest -q scripts/test_publication_v3_execution.py scripts/test_publication_v3_controller.py -k v21_feasibility`

Expected: tests fail because the operation and digest authority are absent.

- [ ] **Step 3: Implement bounded execution and receipt authority**

Add one exact operation, not a generic diagnostic dictionary. Validate the frozen manifest/build receipt before launch. Worker shell must require nonempty artifacts, hash them, call the semantic parser, upload raw files first, result next, and terminal receipt last. Terminal complete requires all four exact digests; markerless termination follows existing durable controller-observation semantics.

- [ ] **Step 4: Write failing parser/mutation tests**

Construct all 12 arms and canonical query indexes. Remove, duplicate, reorder, or drift one arm/sample; mutate a GT hit, recall hit, capacity, request, byte, or eligibility field. Each mutation must fail while an exact below-gate report remains valid but claim-ineligible.

- [ ] **Step 5: Implement strict parser and run GREEN**

Run: `.venv/bin/pytest -q scripts/test_run_publication_v3_cell.py scripts/test_publication_v3_execution.py scripts/test_publication_v3_controller.py -k v21_feasibility`

- [ ] **Step 6: Commit**

```bash
git add scripts/run_publication_v3_cell.py scripts/publication_v3_execution.py scripts/publication_v3_controller.py scripts/test_run_publication_v3_cell.py scripts/test_publication_v3_execution.py scripts/test_publication_v3_controller.py
git commit -m "Run V21 feasibility diagnostics"
```

---

### Task 7: Assurance, Freeze, and Paid Feasibility Decision

**Files:**
- Modify: `docs/research/` with the frozen diagnostic protocol/result only after terminal evidence exists.

**Interfaces:**
- Consumes: Tasks 1-6 and exact source/archive/lock digests.
- Produces: one immutable feasibility decision: either a single frozen eligible arm or an explicit failure that triggers independently fetchable selector regions without loosening gates.

- [ ] **Step 1: Run focused Rust and Python gates serially**

```bash
rtk proxy cargo test -p borsuk v21_feasibility --lib -- --nocapture
rtk proxy cargo test -p borsuk v21_feasibility --lib -- --nocapture
rtk proxy cargo test -p borsuk --example production_bench v21_feasibility -- --nocapture
.venv/bin/pytest -q scripts/test_run_publication_v3_cell.py scripts/test_publication_v3_execution.py scripts/test_publication_v3_controller.py -k v21_feasibility
```

- [ ] **Step 2: Run repository assurance once**

Run the repository's checked-in full assurance command, monitoring the original process only. Stop without replacement on the registered memory-pressure criterion; otherwise retain the exact terminal output.

- [ ] **Step 3: Request read-only cross-provider review**

Ask Claude to inspect the frozen diff and prove whether the simulator can false-pass because it differs from V21 build geometry, routing, codebook, exact scoring, memory accounting, or publication authority. Resolve every Critical/Important finding with another RED/GREEN cycle.

- [ ] **Step 4: Freeze and push source**

Verify the worktree is clean, fetch `origin/main`, require `origin/main` to be an ancestor, and push a fast-forward source commit. Freeze its source archive and lockfile digests in the diagnostic execution authority.

- [ ] **Step 5: Launch one Spot diagnostic and terminate immediately**

Use AWS profile `causality` and the exact completed Deep Image build. Monitor terminal markers and instance health only; do not inspect incomplete CSV. Terminate at terminal marker and retain instance identity, interruption state, all artifact digests, peak RSS, swap/OOM evidence, requests, bytes, coverage, and recall.

- [ ] **Step 6: Apply the preregistered decision**

Select the lowest-memory arm only if directory bytes `<= 40,000,000`, per-query requests `<= 4`, bytes `<= 1,048,576`, minimum GT coverage `>= 0.990`, training-only recall `>= 0.975`, and projected RSS `<= 768 MiB`. If none passes, record the terminal negative result and write the next design/plan for independently authenticated selector-region fetch units. Do not start a V21 paid build and do not relax any gate.

---

## Plan Self-Review

- Spec coverage: Phase 0 arm authority, actual geometry, deterministic representatives, four-GET/1-MiB planner, real capacity accounting, GT coverage, final recall, claim-ineligible AWS execution, source freeze, and fail-closed decision each map to a task above.
- Deliberately deferred: V21 persistent root/group format, open/preparation, production query execution, complete-generation materialization, and paid publication/competitor gates. Those begin only after this plan produces an eligible arm, exactly as required by the spec.
- Type consistency: Tasks 2-4 consume the exact `V21FeasibilityArm` and report types defined in Task 1; Tasks 5-6 serialize the report produced in Task 4; no later task renames those interfaces.
- No compatibility path: the diagnostic reads V20 as historical authority but never makes V21 read V20. It is deleted or kept behind `#[doc(hidden)]` after the V21 format qualifies.

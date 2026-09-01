# V23 Balanced Page Router Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and qualify a claim-ineligible balanced two-level page layout whose selector freezes the smallest passing page budget in `8, 12, 16` while meeting frozen Deep Image recall, latency, and memory gates.

**Architecture:** Rust constructs query-independent balanced spherical supercells/pages, evaluates the nine fixed `(page budget, replica arm)` pairs on corpus pseudoqueries, then freezes the first passing pair before opening the burned cohort. Persistent bulk vectors and assignments use strict Parquet; typed canonical JSON carries only manifests, receipts, progress, and results. Python stages immutable inputs and runs one offline executable on disposable Spot compute.

**Tech Stack:** Rust 2024, Arrow/Parquet 58.3, `half`, `rayon`, fused-f32 SIMD, `serde`, `sha2`, `blake3`, Python 3.12, boto3, EC2 Spot.

**Spec:** `docs/superpowers/specs/2026-09-01-v23-balanced-page-router-design.md`

## Global Constraints

- Every result is `claim_eligible=false`; D3 and sealed holdout stay fenced.
- Production uses `S=min(8192,next_power_of_two(ceil(N/12288)))`, 12,288 rows/supercell, 384 primary rows/page, top 96 supercells, and a page budget frozen from `8, 12, 16` on pseudoqueries.
- Supercell populations are 6,144--24,576; page primary counts differ by at most one and never exceed 384.
- Arms are exactly `(amp-1125,48)`, `(amp-1250,96)`, `(amp-1500,192)` in order; one replica/row and at most 576 occurrences/page.
- Pseudoquery gates: 993,750 aggregate ppm, 900,000 minimum-query ppm, 995,000 oracle attainment ppm, exactly the candidate budget, at most 1,966,080 projected page bytes, and at most 4,000,000 dimensions/query. Candidate order is budget-major `8,12,16`, then arm order `1125,1250,1500`.
- Development gates: 318/320 hits, 9/10 minimum/query, 995,000 oracle attainment ppm, exactly the frozen page budget, p99 below 15,000,000 ns, under 3 GiB RAM, at most 1,966,080 projected page bytes, and identical scalar/fused pages. A failure cannot try another budget on the burned cohort.
- Manifest schema v3 binds `page_budgets=[8,12,16]` and removes scalar `selected_pages`. Receipt schema v3 preserves all nine pair metrics, binds the selected budget and arm before official query/neighbor Parquet is parsed, and preserves the ten construction identities plus typed `quality` stop when no pair passes. Registered query/neighbor bytes may be hash-authenticated before that boundary but cannot be parsed or supplied to selection. No v2 receipt reader is retained.
- Persistent bulk interchange is exact Parquet. Bounded scratch uses Arrow IPC and is deleted after terminal/PID clearance. JSON is only small authority/evidence.
- Construction cannot read official queries/neighbors. Pseudoqueries are excluded from fitting. No tuning occurs after opening the burned cohort.
- Reduced shapes cannot serialize production receipts. Add no compatibility reader, alias, migration, page body, production search, storage, or D3 API.
- AWS uses profile `causality`, same-region Spot, outcome-blind monitoring, terminal evidence, and immediate termination.
- Use `apply_patch`, preserve Git identity, add no AI attribution, and push verified slices fast-forward to `origin/main`.

---

## File Structure

- Create `crates/borsuk/src/v23_balanced_pages.rs`: authority, shapes, receipts, projections, progress, local request.
- Create `crates/borsuk/src/v23_balanced_pages_arrow.rs`: strict supercell/page/row-assignment Parquet.
- Create `crates/borsuk/src/v23_balanced_pages_train.rs`: reservoir split, balanced tree, routing, exhaustive scoring.
- Create `crates/borsuk/src/v23_balanced_pages_build.rs`: bounded external sort, pages, replicas, final metadata.
- Create `crates/borsuk/src/v23_balanced_pages_eval.rs`: pseudoquery selection, serving selector, controls, result.
- Modify `crates/borsuk/src/lib.rs`: private modules and one doc-hidden local request.
- Create `crates/borsuk/examples/v23_balanced_page_falsifier.rs`: strict local-only CLI.
- Create `scripts/run_v23_balanced_page_falsifier.py` and its test: offline runner/monitor.
- Create `scripts/launch_v23_balanced_pages_spot.py` and its test: staging/Spot lifecycle.
- Create `docs/research/v23-balanced-page-router-manifest.json`: frozen one-cell scientific authority, committed before allocation.

## Plan Delivery Checkpoint

- [ ] Force-add this plan, run the docs validator and cached diff-check, commit only this file, require `origin/main` is an ancestor, and push `HEAD:main`.

### Task 1: Authority and projections

**Files:** Create `v23_balanced_pages.rs`; modify `lib.rs`; tests in the new module.

**Interfaces:** Produces `V23BalancedShape`, `V23BalancedArm`, `V23BalancedIdentity`, `V23BalancedManifest`, `V23BalancedReceipt`, `V23BalancedStop`, `project_v23_balanced_shape`, `validate_v23_balanced_manifest`, `canonical_v23_balanced_receipt_bytes`.

- [ ] **Step 1: Write the failing tests.**
```rust
#[test]
fn v23_balanced_authority_rejects_identity_shape_and_role_drift() {
    let manifest = production_manifest_fixture(100_000_000);
    assert!(validate_v23_balanced_manifest(&manifest).is_ok());
    for changed in manifest_mutations(&manifest) { assert!(validate_v23_balanced_manifest(&changed).is_err()); }
}
#[test]
fn v23_balanced_projection_is_exact_at_100m() {
    let p = project_v23_balanced_shape(100_000_000).unwrap();
    assert_eq!((p.supercells, p.maximum_pages, p.maximum_scored_dimensions), (8_192, 268_608, 1_376_256));
    assert!(p.serving_bytes < 3 * 1024 * 1024 * 1024);
}
```
- [ ] **Step 2:** Run `cargo test -p borsuk --lib v23_balanced_authority_ -- --nocapture`; require compile RED only at missing types/functions.
- [ ] **Step 3:** Implement checked projection arithmetic and strict typed/canonical authority. Require exact schemas/types, role-specific digest algorithms, nonzero lengths, unique roles/URIs, fixed arms/order, source/archive/dataset bindings, no official-query construction role, and production/reduced separation.
```rust
let supercells = rows.div_ceil(12_288).next_power_of_two().min(8_192);
let maximum_pages = rows.div_ceil(384).checked_add(supercells - 1)
    .ok_or_else(|| BorsukError::InvalidConfig("balanced projection overflow".into()))?;
```
- [ ] **Step 4:** Run focused GREEN, `cargo fmt --all -- --check`, `git diff --check`.
- [ ] **Step 5:** Commit the two files as `feat: add balanced page authority`.

### Task 2: Exact Parquet contracts

**Files:** Create `v23_balanced_pages_arrow.rs`; modify `lib.rs`; tests in new module.

**Interfaces:** Produces `V23SupercellRow`, `V23PageRow`, `V23RowPage`, strict `write/read_v23_{supercells,pages,row_pages}`.

- [ ] **Step 1: Add round-trip/mutation REDs.**
```rust
#[test]
fn v23_balanced_arrow_round_trips_exact_physical_schemas() {
    write_v23_supercells(&path(), &supercell_fixture()).unwrap();
    assert_eq!(read_v23_supercells(&path(), &identity()).unwrap(), supercell_fixture());
}
#[test]
fn v23_balanced_arrow_rejects_schema_order_count_and_binding_drift() {
    for a in malformed_parquet_artifacts() { assert!(read_role_artifact(&a.path, &a.identity).is_err()); }
}
```
- [ ] **Step 2:** Run `cargo test -p borsuk --lib v23_balanced_arrow_ -- --nocapture`; require missing codec RED.
- [ ] **Step 3:** Implement nonnullable `fixed_size_list<float16>[96]` with nonnullable child `element`; validate complete physical schema, all rows/order/finiteness/bounds/sentinel/counts and digest/length before semantics. Never encode bulk rows as JSON.
- [ ] **Step 4:** Run GREEN/fmt/diff-check; commit `feat: add balanced page parquet contracts`.

### Task 3: Deterministic supercell geometry

**Files:** Create `v23_balanced_pages_train.rs`; modify `lib.rs`; tests in new module.

**Interfaces:** Produces `V23BalancedTree`, `V23SupercellModel`, `split_v23_balanced_reservoir`, `train_v23_balanced_tree`, `route_v23_supercell_beam2`, `score_all_v23_supercells`.

- [ ] **Step 1: Add split/balance/tie/determinism/exhaustive-score REDs.**
```rust
#[test]
fn v23_balanced_training_excludes_pseudoqueries_and_is_worker_deterministic() {
    let one = train_fixture(1).unwrap(); let four = train_fixture(4).unwrap();
    assert_eq!(encode_tree(&one), encode_tree(&four));
    assert!(one.training_ordinals().is_disjoint(one.pseudoquery_ordinals()));
}
#[test]
fn v23_balanced_scoring_is_exhaustive_and_matches_independent_f64_order() {
    let m = trained_fixture();
    assert_eq!(page_order_f64(&m, query()), page_order_fused(&m, query()).unwrap());
    assert_eq!(m.last_score_count(), m.supercell_count());
}
```
- [ ] **Step 2:** Run `cargo test -p borsuk --lib v23_balanced_training_ -- --nocapture`; require missing training RED.
- [ ] **Step 3:** Implement hash split 2,096,128/1,024, four Lloyd refinements/split, source-ordinal ties, f16 persistent centroids, population rejection, boundary-consistent runner-up write routing, and fixed-block exhaustive read scoring over once-decoded f32 serving centroids. Fused is authority; an independent f64 reference is the page-set control.
- [ ] **Step 4:** Run GREEN/fmt/diff-check; commit `feat: train balanced page supercells`.

### Task 4: Bounded pages and replicas

**Files:** Create `v23_balanced_pages_build.rs`; modify Arrow module and `lib.rs`; tests in build/Arrow modules.

**Interfaces:** Produces `V23PrimaryPage`, `V23PageAssignment`, `V23BoundedSort`, `V23ReplicaCandidate`, `V23BalancedArmArtifacts`, `build_v23_primary_pages`, `build_v23_replica_arms`, `finalize_v23_page_metadata`, `cleanup_v23_balanced_scratch`.

- [ ] **Step 1: Add bounded-construction REDs.**
```rust
#[test]
fn v23_balanced_pages_are_complete_balanced_and_bounded() {
    let b = build_fixture(1).unwrap();
    assert_eq!(b.source_ordinals(), expected_ordinals());
    assert!(b.pages.iter().all(|p| p.primary_rows <= 384));
    assert!(b.count_spread_per_supercell().all(|spread| spread <= 1));
    assert!(b.scratch_paths_after_terminal.is_empty());
}
#[test]
fn v23_balanced_replica_arms_apply_exact_caps() {
    let a = replica_fixture().unwrap();
    assert_arm(&a[0], "amp-1125", 1_125_000, 48);
    assert_arm(&a[1], "amp-1250", 1_250_000, 96);
    assert_arm(&a[2], "amp-1500", 1_500_000, 192);
}
```
- [ ] **Step 2:** Run `cargo test -p borsuk --lib v23_balanced_build_ -- --nocapture`; require missing builder RED.
- [ ] **Step 3:** Implement bounded Arrow IPC runs sorted `(supercell,source)`, bounded-fan-in merge, exact `ceil(n/384)` spherical partitions, and loss/duplicate/empty/imbalance rejection.
- [ ] **Step 4:** Implement authoritative primary plus boundary-runner page scoring, closest-distinct `(ratio,source)` candidates, bounded fan-in-64 external merge, fixed 16-byte/source primary-plus-three-arm decisions, one replica/row, cap-limited global/page acceptance, digest-bound three-pass replay, final occurrence-weighted arm centroids/radii, and max 576 rows/page.
- [ ] **Step 5:** Run GREEN/fmt/diff-check; commit `feat: build balanced pages and replicas`.

### Task 5: Pseudoquery selection and evaluator

**Files:** Create `v23_balanced_pages_eval.rs`; modify `lib.rs`; tests in new module.

**Interfaces:** Produces `V23BalancedPageBudget`, `V23BalancedSelectedPair`, `V23BalancedSample`, `V23BalancedResult`, `select_v23_balanced_pair`, `select_v23_balanced_pages`, `evaluate_v23_balanced_development`, `canonical_v23_balanced_receipt_bytes`, and `canonical_v23_balanced_result_bytes`.

- [ ] **Step 1: Add outcome-blind/page-budget-ladder/causal/timing/result REDs.**
```rust
#[test]
fn v23_balanced_selection_freezes_first_pass_without_official_inputs() {
    let s = select_v23_balanced_pair(pseudo_fixture(), arm_fixtures()).unwrap();
    assert_eq!((s.page_budget, s.arm), (12, V23BalancedArm::Amp1250));
    assert_eq!(s.official_query_reads, 0);
}
#[test]
fn v23_balanced_result_recomputes_samples_gates_and_class() {
    for m in result_mutations(valid_result()) { assert!(canonical_v23_balanced_result_bytes(&m).is_err()); }
}
```
- [ ] **Step 2:** Run `cargo test -p borsuk --lib v23_balanced_eval_ -- --nocapture`; require missing evaluator RED.
- [ ] **Step 3:** During the full-corpus routing pass compute bounded leave-self-out pseudoquery top-ten heaps; implement supercell-radius top-96 cells, all child pages, page centroid-minus-radius order, budget-major `8,12,16` outcome-blind selection followed by fixed arm order, pseudoquery and development containment gates, set-cover controls at the candidate/frozen budget, 1,024 warmups, 10,000 resident timings, fixed percentile, classification precedence, and independent serialization recomputation. Insufficient candidates fail only the affected unfrozen pair. Materialize all nine metrics in receipt v3; bind `selected_page_budget` and selected arm before official inputs are parsed and into the typed canonical result. If none passes, preserve the ten construction outputs and typed `quality` stop without opening the burned cohort. Reject all other sample lengths and recompute the exact GET/byte projection.
- [ ] **Step 4:** Run GREEN/fmt/diff-check; commit `feat: evaluate balanced page routing`.

- [ ] **Step 5:** Before page-body integration, replace rather than reuse the historical eight-page D2/D3 wave contract. The new version must prove at most 122,880 bytes/page, the frozen 8/12/16 GET count, at most 1,966,080 bytes/query, transient-capacity safety, and typed failures. D3 stays fenced until this separate contract passes.

### Task 6: Thin local executable

**Files:** Modify authority module/`lib.rs`; create `examples/v23_balanced_page_falsifier.rs`; tests in example/module.

**Interfaces:** Produces doc-hidden `V23BalancedLocalRequest` and `run_v23_balanced_local_request`; accepts one manifest, input directory, empty output directory, explicit preflight/execute.

- [ ] **Step 1:** Test missing/duplicate/unknown flags and reject bucket/endpoint/credential/page-body/D3/holdout/loader/mount flags; require exact inventory and empty output.
- [ ] **Step 2:** Run `cargo test -p borsuk --example v23_balanced_page_falsifier v23_balanced_cli_ -- --nocapture`; require missing high-level API RED.
- [ ] **Step 3:** Implement strict parser/main which invokes library exactly once; canonical terminal on stdout, errors on stderr, no network/storage/page type.
```rust
fn main() -> ExitCode {
    match parse_args(std::env::args_os()).and_then(run_v23_balanced_local_request) {
        Ok(bytes) => write_canonical_stdout(&bytes), Err(error) => fail_stderr(error),
    }
}
```
- [ ] **Step 4:** Run example GREEN, grouped library GREEN, fmt/diff; commit `feat: add balanced page local runner`.

### Task 7: Offline runner and Spot lifecycle

**Files:** Create four Python runner/launcher and test files named in File Structure.

**Interfaces:** Produces `stage_registered_inputs`, `build_offline_command`, `monitor_process_group`, `run_balanced_cell`, `launch_spot_cell`, `terminate_cell`.

- [ ] **Step 1: Add Python REDs.**
```python
def test_worker_is_offline_outcome_blind_and_cleans_named_paths(self):
    command = build_offline_command(fixture_policy())
    self.assertNotIn("aws", command.scientific_argv)
    self.assertEqual(command.output_dir_initial_entries, [])
    self.assertEqual(command.cleanup_paths, fixture_policy().explicit_named_paths)
def test_launcher_terminates_every_terminal(self):
    for terminal in terminals():
        cloud = FakeCloud(terminal); launch_spot_cell(cloud, fixture_request())
        self.assertEqual(cloud.terminate_calls, [cloud.instance_id])
```
- [ ] **Step 2:** Run `python3 -m unittest scripts.test_run_v23_balanced_page_falsifier scripts.test_launch_v23_balanced_pages_spot`; require missing modules RED.
- [ ] **Step 3:** Implement credentialed exact staging and offline child without AWS/network. Monitor wall/PGID RSS/PSI/swap/progress/EC2/terminal; parent uploads evidence; unlink named paths after PID clearance; terminate immediately.
- [ ] **Step 4:** Run GREEN, Ruff 0.15.20, py_compile, diff; commit `feat: orchestrate balanced page falsifier`.

### Task 8: Reduced preflight, assurance, and one corpus screen

**Files:** Modify only files with focused RED defects; create `docs/research/v23-balanced-page-router-manifest.json` before allocation; update `docs/research/publication-v3-attempt-ledger.md` only after authenticated terminal.

**Interfaces:** Produces deterministic worker-1/worker-4 preflight, repository assurance, one authenticated result/stop, immediate termination.

- [ ] **Step 1:** Run checked-in reduced fixture in separate processes at workers 1 and 4; require identical artifact digests, caps, work/RAM, and empty scratch.
- [ ] **Step 2:** Repair defects only via focused RED/GREEN, then run exactly one final progression:
```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets
uv run --offline --python 3.12 --with-requirements scripts/requirements-format-bench.txt python -m unittest discover -s scripts -p 'test_*.py'
python3 scripts/validate_research_docs.py
git diff --check
```
- [ ] **Step 3:** Commit/push verified slices fast-forward and prove HEAD=origin=ls-remote with clean status.
- [ ] **Step 4:** Commit a manifest freezing revision, instance/region/Spot, arms, seed/split, S/top96/page384, gates, work/resource stops, scratch names, roles, no restart.
- [ ] **Step 5:** Launch exactly one cell: `python3 scripts/launch_v23_balanced_pages_spot.py --profile causality --manifest docs/research/v23-balanced-page-router-manifest.json --spot`.
- [ ] **Step 6:** Monitor only progress/resource/EC2/terminal. Recompute identities, schemas, counts, samples, aggregates, timing, memory, SIMD, classification; launcher exit is not PASS.
- [ ] **Step 7:** Record exact evidence, validate, commit only ledger, push fast-forward.

If `balanced-page-candidate`, freeze format/revision for separate page-body and sealed-holdout qualification. If `balanced-layout-rejected`, proceed to witness routing. Otherwise follow only the spec's causal disposition. D3 stays fenced.

## Self-Review Record

- Every spec requirement maps to Tasks 1--8.
- Types originate once; only Task 6 exposes the high-level local request.
- Bulk data remains Parquet/Arrow; JSON remains authority/evidence.
- No placeholder, compatibility task, or undefined follow-on remains.

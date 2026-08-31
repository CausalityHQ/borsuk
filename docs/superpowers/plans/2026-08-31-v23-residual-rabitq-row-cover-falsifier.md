# V23 Residual RaBitQ Row-Cover Falsifier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a bounded, claim-ineligible residual RaBitQ row scorer that can prove or reject exact eight-page near-perfect recall before any production or D3 campaign.

**Architecture:** A new prerelease Arrow index stores one 96-bit residual sign code, two f32 estimator factors, and two u32 page ordinals per row, ordered by the existing 65,536-leaf tree. Queries probe 32/64/128 leaves, score at most 262,144 rows with differential scalar/SIMD implementations, retain top 4,096 rows, and reuse the deterministic eight-page cover. Separate exact-f16 and exhaustive controls make failures causal.

**Tech Stack:** Rust 2024, Arrow IPC 58.3, Parquet 58.3, serde/serde_json, blake3/SHA-256, borsuk-fma SIMD, Python 3.12, boto3, stdlib unittest, AWS EC2 Spot.

**Spec:** `docs/superpowers/specs/2026-08-31-v23-residual-rabitq-row-cover-falsifier-design.md`

## Global Constraints

- New prerelease format only: no compatibility reader, alias, migration, or fallback.
- Bulk artifacts use Arrow IPC/Parquet; receipts and results use strict typed canonical newline JSON.
- Serving projection is at most 2,920,622,772 bytes; hard ceiling is 3,221,225,472 bytes.
- Exactly eight pages, at most 262,144 scored rows, 4,096 retained rows, and 8,192 page assignments.
- Development opens only burned query ordinals 0--31 and must attain all 318 oracle-reachable hits.
- Sealed holdout, if separately authorized, requires at least 991,000 aggregate ppm, 900,000 minimum-query ppm, 995,000 oracle-attainment ppm, and 15 ms resident CPU p99 over at least 10,000 raw samples.
- Development has no page-body or holdout capability. D3 remains fenced.
- AWS uses profile `causality`, `eu-central-1`, Spot, and immediate termination after terminal evidence.
- SIMD is production authority; scalar is a differential oracle, never a silent serving fallback.
- Preserve configured Git identity and add no AI attribution.

---

### Task 1: Typed authority, projection, and receipts

**Files:**
- Create: `crates/borsuk/src/v23_rabitq.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Produces `V23RaBitQObjectIdentity`, `V23RaBitQManifest`, `V23RaBitQReceipt`, `V23RaBitQPhase`, `V23RaBitQRunMode`, and `V23RaBitQServingProjection`.
- Produces `validate_v23_rabitq_manifest`, canonical manifest/receipt serializers, and `project_v23_rabitq_serving_bytes`.

- [ ] **Step 1: Write failing authority tests**

Add `v23_rabitq_authority_rejects_role_schema_digest_and_phase_drift`, `v23_rabitq_authority_receipt_binds_manifest_inputs_outputs_and_terminal_state`, and `v23_rabitq_authority_projects_exact_100m_resident_bytes`. Mutation tables cover missing/extra roles, wrong digest algorithms, malformed digests, zero lengths, duplicate or overlapping URIs, phase/source/index/dataset mismatch, unknown JSON keys, output mutation, and stop/terminal mutation.

The projection assertion is:

```rust
let value = project_v23_rabitq_serving_bytes(100_000_000).unwrap();
assert_eq!(value.total_bytes, 2_920_622_772);
assert_eq!(value.ceiling_bytes, 3_221_225_472);
```

- [ ] **Step 2: Run RED**

Run `cargo test -p borsuk --lib v23_rabitq_authority_ -- --nocapture`.

Expected: compile failure only for the new boundary.

- [ ] **Step 3: Implement the minimal boundary**

Use these exact signatures:

```rust
pub enum V23RaBitQPhase { Construction, Development, Holdout }
pub enum V23RaBitQRunMode { Preflight(V23RaBitQPhase), Execute(V23RaBitQPhase) }
pub struct V23RaBitQObjectIdentity {
    pub role: String,
    pub uri: String,
    pub sha256: String,
    pub blake3: Option<String>,
    pub encoded_bytes: u64,
}
pub(crate) fn validate_v23_rabitq_manifest(value: &V23RaBitQManifest) -> Result<()>;
pub(crate) fn canonical_v23_rabitq_manifest_bytes(value: &V23RaBitQManifest) -> Result<Vec<u8>>;
pub(crate) fn canonical_v23_rabitq_receipt_bytes(value: &V23RaBitQReceipt) -> Result<Vec<u8>>;
pub(crate) fn project_v23_rabitq_serving_bytes(rows: u64) -> Result<V23RaBitQServingProjection>;
```

Use `#[serde(deny_unknown_fields)]`. Exact input roles are `tree-receipt`, `incidence-tree`, `source-pages`, `d2-report`, `query-parquet`; exact output roles are `row-codes`, `leaf-offsets`, `centroids`, `rotation`, `f16-control`, `construction-receipt`, `screen-result`. Projection is `rows*28 + 65_537*8 + 65_536*96*2 + 40_369_836 + 96*96*4 + 64*1024*1024`.

- [ ] **Step 4: Verify and commit**

Run the focused selector, `cargo fmt --all -- --check`, and `git diff --check`. Commit `crates/borsuk/src/v23_rabitq.rs` and `lib.rs` as `feat: add RaBitQ falsifier authority`.

---

### Task 2: Strict Arrow IPC artifacts

**Files:**
- Create: `crates/borsuk/src/v23_rabitq_arrow.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Consumes Task 1 identities.
- Produces `V23RaBitQRowPlanes`, `V23RaBitQGeometry`, and strict Arrow encoders/readers.

- [ ] **Step 1: Write exact-schema REDs**

Define tests around:

```rust
pub(crate) struct V23RaBitQRowPlanes {
    pub sign_codes: Vec<[u8; 12]>,
    pub residual_norms: Vec<f32>,
    pub alignments: Vec<f32>,
    pub primary_pages: Vec<u32>,
    pub replica_pages: Vec<u32>,
}
pub(crate) struct V23RaBitQGeometry {
    pub leaf_offsets: Vec<u64>,
    pub centroids: Vec<[half::f16; 96]>,
    pub rotation: [[f32; 96]; 96],
}
```

Lock row schema to five nonnullable fields with `sign_code: fixed_size_binary[12]`; offsets to one nonnullable u64 column; centroids/f16 control to nonnullable fixed-size-list f16[96]; rotation to 96 nonnullable fixed-size-list f32[96] rows. Reject extra/reordered fields, nullability/child/type/width changes, multiple batches, plane-length drift, nonmonotonic offsets, wrong terminal offset, nonfinite factors/rotation, nonorthogonality, invalid replica sentinel, and byte identity drift.

- [ ] **Step 2: Run RED**

Run `cargo test -p borsuk --lib v23_rabitq_arrow_ -- --nocapture`.

- [ ] **Step 3: Implement standard Arrow readers/writers**

Use uncompressed Arrow IPC `FileWriter`/`FileReader`; add no custom container magic. Authenticate exact bytes before parsing, load each column once into typed aligned arrays, and release encoded bytes before science.

```rust
pub(crate) fn encode_v23_rabitq_row_planes(value: &V23RaBitQRowPlanes) -> Result<Vec<u8>>;
pub(crate) fn read_v23_rabitq_row_planes(bytes: &[u8], id: &V23RaBitQObjectIdentity) -> Result<V23RaBitQRowPlanes>;
pub(crate) fn encode_v23_rabitq_geometry(value: &V23RaBitQGeometry) -> Result<V23RaBitQGeometryBytes>;
```

- [ ] **Step 4: Verify and commit**

Run the focused selector, fmt, and diff-check. Commit as `feat: add RaBitQ Arrow artifacts`.

---

### Task 3: Deterministic rotation and scalar RaBitQ oracle

**Files:**
- Create: `crates/borsuk/src/v23_rabitq_quantizer.rs`
- Create: `crates/borsuk/tests/fixtures/v23_rabitq_reference.json`
- Modify: `crates/borsuk/src/lib.rs`
- Modify: `NOTICE` only if upstream expressions are copied.

**Interfaces:**
- Produces `V23RaBitQCode`, `build_v23_rabitq_rotation`, `encode_v23_rabitq_residual`, `score_v23_rabitq_scalar`, and `score_v23_rabitq_f64_reference`.

- [ ] **Step 1: Add reference-vector REDs**

Cover seed reproduction, f32 orthogonality, exact zero residual, sign ties, finite requirements, scale behavior, four-bit query quantization, estimator-bound evidence, and monotonic exact distances. Check in 16 deterministic 96d row/query cases generated from the official Apache-2.0 RaBitQ reference; do not add C++ as a runtime dependency.

```rust
pub(crate) struct V23RaBitQCode {
    pub sign_code: [u8; 12],
    pub residual_norm: f32,
    pub alignment: f32,
}
pub(crate) fn build_v23_rabitq_rotation(seed: [u8; 32]) -> Result<[[f32; 96]; 96]>;
pub(crate) fn encode_v23_rabitq_residual(residual: &[f32; 96], rotation: &[[f32; 96]; 96]) -> Result<V23RaBitQCode>;
pub(crate) fn score_v23_rabitq_scalar(query_residual: &[f32; 96], code: &V23RaBitQCode, rotation: &[[f32; 96]; 96]) -> Result<V23RaBitQEstimate>;
```

- [ ] **Step 2: Run RED**

Run `cargo test -p borsuk --lib v23_rabitq_quantizer_ -- --nocapture`.

- [ ] **Step 3: Implement the audited one-bit algorithm**

Port only the SIGMOD 2024 one-bit index formula and four-bit query estimator. Generate the 96x96 orthogonal matrix using deterministic f64 QR with canonical positive diagonal and f32 round-trip validation. Zero residual uses all-zero code, norm zero, alignment 1.0, and exact centroid distance. Cite the paper in module docs and record upstream commit/license if code is copied.

- [ ] **Step 4: Verify and commit**

Run focused tests, fmt, and diff-check. Commit as `feat: add scalar RaBitQ quantizer`.

---

### Task 4: SIMD scoring, bounded heap, and page cover

**Files:**
- Create: `crates/borsuk/src/v23_rabitq_eval.rs`
- Modify: `crates/borsuk/src/v23_rabitq_quantizer.rs`
- Modify: `crates/borsuk/src/v23_incidence_tree.rs`
- Modify: `crates/borsuk/src/v23_diagnostic.rs`

**Interfaces:**
- Consumes tree beam ranking and `best_v23_page_coverage` without duplicating them.
- Produces `V23RaBitQBackend`, `rank_v23_rabitq_rows`, `select_v23_rabitq_pages`, and `V23RaBitQQueryEvidence`.

- [ ] **Step 1: Add SIMD and bound REDs**

Cover x86 AVX2/FMA and aarch64 NEON fused backends, unavailable-backend stop, at-most-eight-ULP scalar differential agreement and exact selected-page equality on random/ties/zeros/subnormals/reversed blocks, nonfinite rejection, scan cap 262,144, heap cap 4,096, page-assignment cap 8,192, exactly eight unique pages, and permutation-independent cover output.

```rust
pub(crate) enum V23RaBitQBackend { Aarch64Neon, X86Avx2Fma, ScalarControl }
pub(crate) struct V23RaBitQQueryEvidence {
    pub query_ordinal: u32,
    pub probe_count: u16,
    pub scored_rows: u32,
    pub retained_rows: u16,
    pub page_assignments: u16,
    pub page_ordinals: [u32; 8],
    pub max_estimator_error_ppm: u64,
    pub scalar_pages_equal: bool,
    pub backend: V23RaBitQBackend,
}
```

- [ ] **Step 2: Run RED**

Run `cargo test -p borsuk --lib v23_rabitq_eval_ -- --nocapture`.

- [ ] **Step 3: Implement bounded production scoring**

Runtime-detect target features before entering target-feature functions. Production rejects `ScalarControl`. Maintain a max-heap of 4,096 `(distance,row_ordinal)` entries; never allocate or sort all scored rows. Make existing tree and page-cover helpers `pub(crate)` only.

- [ ] **Step 4: Verify and commit**

Run the focused selector plus `v23_incidence_`, fmt, and diff-check. Commit as `feat: add SIMD RaBitQ row scoring`.

---

### Task 5: One-pass streaming constructor

**Files:**
- Create: `crates/borsuk/src/v23_rabitq_build.rs`
- Modify: `crates/borsuk/src/v23_rabitq.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Produces `V23RaBitQSourceRow`, `V23RaBitQBuildRequest`, and `build_v23_rabitq_artifacts`.
- Consumes authenticated unique primary rows, the tree, Task 2 encoders, and Task 3 quantizer.

- [ ] **Step 1: Add reduced-shape streaming REDs**

Cover duplicate primary rows, replica-before-primary, conflicting vectors/page bindings, missing rows, noncanonical IDs, leaf-order drift, bounded sort runs, deterministic merge, interrupted-progress rejection, digest mutation, f16-control order equality, and exact one-pass source consumption.

```rust
pub(crate) struct V23RaBitQSourceRow {
    pub canonical_record_id: Vec<u8>,
    pub vector: [f32; 96],
    pub primary_page: u32,
    pub replica_page: Option<u32>,
}
pub(crate) fn build_v23_rabitq_artifacts<I: Iterator<Item = Result<V23RaBitQSourceRow>>>(request: V23RaBitQBuildRequest<'_, I>) -> Result<V23RaBitQBuiltArtifacts>;
```

- [ ] **Step 2: Run RED**

Run `cargo test -p borsuk --lib v23_rabitq_build_ -- --nocapture`.

- [ ] **Step 3: Implement bounded construction**

Write temporary Arrow runs capped at 256 MiB, merge by `(leaf_ordinal, canonical_record_id)`, encode each unique row once, and emit row planes, geometry, f16 control, progress, and receipt through explicit known-file paths. Accept no query input; perform no recursive deletion.

- [ ] **Step 4: Verify and commit**

Run focused tests, fmt, and diff-check. Commit as `feat: build RaBitQ row artifacts`.

---

### Task 6: Four-control causal evaluator and canonical result

**Files:**
- Modify: `crates/borsuk/src/v23_rabitq_eval.rs`
- Modify: `crates/borsuk/src/v23_rabitq.rs`

**Interfaces:**
- Produces `V23RaBitQControl`, `V23RaBitQClassification`, `V23RaBitQCellResult`, `V23RaBitQScreenResult`, `evaluate_v23_rabitq_development`, and the canonical result serializer.

- [ ] **Step 1: Add classification and result REDs**

Test all five classes and every `3 probe counts x 2 row scorers` serving cell. Mutate per-query selections/hits, all aggregates, scan/retain counts, backend, scalar equality, projection, authority roles/digests/URIs, source identity, claim flag, and class. Serialization independently recomputes every derivable field.

```rust
pub(crate) enum V23RaBitQControl { ExactExhaustive, ExactTree, RaBitQExhaustive, RaBitQTree }
pub(crate) enum V23RaBitQClassification {
    AuthorityStop,
    TreePruningRejected,
    RaBitQRepresentationRejected,
    TreeRaBitQCompositionRejected,
    DevelopmentCandidateAccepted,
}
```

- [ ] **Step 2: Run RED, implement, and run GREEN**

Run `cargo test -p borsuk --lib v23_rabitq_screen_ -- --nocapture`. Implement exact precedence from the spec. Acceptance is exactly 318 oracle hits, 993,750 aggregate ppm, 900,000 minimum ppm, 1,000,000 oracle attainment, plus all resource and determinism gates.

- [ ] **Step 3: Verify and commit**

Run fmt and diff-check. Commit as `feat: classify RaBitQ causal screen`.

---

### Task 7: Thin executable, Python stager, and Spot controller

**Files:**
- Create: `crates/borsuk/examples/v23_rabitq_falsifier.rs`
- Create: `scripts/run_v23_rabitq_falsifier.py`
- Create: `scripts/test_run_v23_rabitq_falsifier.py`
- Create: `scripts/launch_v23_rabitq_spot.py`
- Create: `scripts/test_launch_v23_rabitq_spot.py`
- Create: `scripts/fixtures/v23_rabitq_manifest.json`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Produces doc-hidden `V23RaBitQLocalRunRequest` and `run_v23_rabitq_local_request`.
- Rust accepts exact local files only; Python owns downloads, AWS, and explicit cleanup.

- [ ] **Step 1: Stage Rust example and Python REDs**

The example requires explicit paths and identities for manifest, receipt, tree, row codes, offsets, centroids, rotation, f16 control, D2 report, and query Parquet plus `--execute-development`. Reject missing/duplicate/unknown/relative values and every endpoint, bucket, page-prefix, holdout, or D3 flag. Python tests require exact downloads, one binary call, atomic output, 7,200-second timeout, known-file cleanup, Spot-only launch, idempotent token, terminal-before-termination, and no holdout/D3 inputs.

- [ ] **Step 2: Run REDs**

Run `cargo test -p borsuk --example v23_rabitq_falsifier v23_rabitq_ -- --nocapture`, then the two Python test modules under pinned `scripts/requirements-format-bench.txt`.

- [ ] **Step 3: Implement thin boundaries**

Rust `main` parses, calls the library, and writes canonical bytes. Python stages inputs and explicit cleanup. The controller uses `causality`, `eu-central-1`, Spot, immutable preflight/execution prefixes, RSS/PSI/swap/progress stops, and immediate termination. Add no dynamic loader or alternate schema.

- [ ] **Step 4: Verify and commit**

Run the Rust example tests, both Python modules, fmt, Ruff 0.15.20 on the new scripts, py_compile, and diff-check. Commit as `feat: orchestrate RaBitQ falsifier`.

---

### Task 8: Full assurance and bounded scientific decision

**Files:**
- Modify: `docs/research/publication-v3-attempt-ledger.md` only after terminal evidence.

- [ ] **Step 1: Run focused affected gates**

Run `cargo test -p borsuk --lib v23_rabitq_ -- --nocapture`, the example selector, and both Python modules. Keep heavy processes strictly serial.

- [ ] **Step 2: Run full assurance once**

Run fmt check, strict locked workspace/all-targets Clippy, full locked workspace/all-targets tests, dependency-complete Python unittest discovery, Ruff 0.15.20, research-doc validation, and diff-check. On failure repair only the failing layer, then perform one final full gate.

- [ ] **Step 3: Freeze and push the pre-run checkpoint**

Verify HEAD/origin/ls-remote equality and clean worktree. Record source archive, optimized binary identity, manifest identity, all input identities, Spot price ceiling, expected GETs/bytes/runtime, pressure/progress stops, explicit scratch files, and terminal prefixes. Do not start AWS in this step.

- [ ] **Step 4: Run construction preflight and construction once**

Use one Spot instance per terminal attempt. Preflight opens no query object. Execution streams the corpus once, publishes five Arrow artifacts and a canonical receipt, and terminates. Interrupted cells discard and restart under the frozen protocol.

- [ ] **Step 5: Run the development screen once**

Open only burned ordinals 0--31. Preserve exact-f16 exhaustive/tree and RaBitQ exhaustive/tree controls, six candidate cells, timings, resources, canonical classification, and output digest. Do not open page bodies or holdout during evaluation.

- [ ] **Step 6: Record and enforce the decision**

If the class is not `development-candidate-accepted`, record the causal rejection and stop. If accepted, write a separate sealed-holdout spec and plan before opening holdout. Never launch D3 from this task. Validate the ledger, commit, and fast-forward push it to `origin/main`.

# V41 Learned Residual Page Router Falsifier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a claim-ineligible, artifact-only one-million-row falsifier that learns a deterministic query-conditioned residual score over the 123 frozen V38 pages and rejects weak models before validation, holdout, transport, or larger datasets.

**Architecture:** A focused Rust `v41_learned_router` module owns strict authorities, the development split, exact labels, the 64-wide residual model, deterministic training, selection, evaluation, and source-free performance preflight. A thin local-files-only example exposes capability-separated phases, while a Python controller runs one authenticated phase per transient AWS Spot instance and stops the ladder at the first failed gate.

**Tech Stack:** Rust, `borsuk-fma`, Rayon, ChaCha20, Arrow IPC, Parquet, Serde canonical JSON, Python 3.12, boto3, AWS EC2 Spot/S3 with profile `causality`.

**Spec:** `docs/superpowers/specs/2026-09-13-v41-learned-residual-page-router-falsifier-design.md`

## Global Constraints

- Frozen population: ReLAION2B cohort A, 1,000,000 non-null `f32[768]` rows; burned development, validation, and sealed-holdout splits contain 1,000 queries each with exact GT@100.
- Primary selection emits exactly 21 unique pages; quality passes at `>=998_000` ppm aggregate recall and `>=800_000` ppm minimum-query recall.
- Training opens only development query, GT, and V38 relation artifacts; selection opens query and model artifacts but no GT/relation; evaluation opens sealed selection, GT, and relation but no query/model/optimizer.
- The development split keeps duplicate query bit patterns together and uses the exact SHA-256 ordering rule from the spec; validation never selects a checkpoint and holdout is opened at most once.
- Model width is 64, training is exactly 50 epochs with the frozen initialization, rollout, batch, AdamW, clipping, and fused-f32 rules from the spec.
- Cross-language tables use strict Parquet, model tensors use Arrow IPC, and control artifacts use recursively sorted compact JSON plus one LF.
- Source-free preflight requires p99 `<=5_000_000 ns`, cgroup memory `<=256 MiB`, zero swap growth, and memory PSI full avg10 `<=0.75` at both 123 and 12,208 pages.
- V41 reads no corpus vectors or page bodies and performs no physical S3 simulation unless holdout passes; 10M, 100M, fine-vector fetch, write-path changes, and production defaults remain fenced.
- All V41 results are `claim_eligible=false`; a failed phase is preserved and closes its arm without repair or repetition.
- Compilation and scientific execution use AWS Spot under profile `causality`; local verification stays narrow until one final remote assurance run.

## File map

- Create `crates/borsuk/src/v41_learned_router.rs`: authority, split, labels, model, trainer, selector, evaluator, codecs, preflight, and local phase runner.
- Modify `crates/borsuk-fma/src/lib.rs`: safe detected fused-f32 kernels for the exact 64- and 768-element accumulation orders.
- Modify `crates/borsuk/src/lib.rs`: declare the module and export only the doc-hidden high-level local request/runner boundary.
- Create `crates/borsuk/examples/v41_learned_router.rs`: strict local-files-only CLI with no network or page-store type.
- Create `scripts/run_v41_learned_router_spot.py`: Spot lifecycle, phase capability staging, pressure/progress monitoring, authenticated publication, and immediate termination.
- Create `scripts/test_run_v41_learned_router_spot.py`: controller, namespace, stop, and terminal contracts.
- Create `scripts/requirements-v41-learned-router.txt`: pinned controller dependencies.
- Modify `docs/research/publication-v3-attempt-ledger.md`: terminal evidence only after a scientific phase exits.

---

### Task 0: Freeze and deliver the reviewed design documents

**Files:**
- Add: `docs/superpowers/specs/2026-09-13-v41-learned-residual-page-router-falsifier-design.md`
- Add: `docs/superpowers/plans/2026-09-13-v41-learned-residual-page-router-falsifier.md`

**Interfaces:**
- Consumes: authenticated V36/V38/V39/V40 evidence recorded in the repository ledger.
- Produces: a tracked immutable V41 scientific contract and this executable TDD sequence.

- [ ] **Step 1: Validate the two documents**

Run:

```bash
python3 scripts/validate_research_docs.py
git add -f docs/superpowers/specs/2026-09-13-v41-learned-residual-page-router-falsifier-design.md \
  docs/superpowers/plans/2026-09-13-v41-learned-residual-page-router-falsifier.md
git diff --cached --check
```

Expected: validator exit 0; no whitespace errors; exactly the two V41 documents staged.

- [ ] **Step 2: Commit and fast-forward push the contract**

```bash
git commit -m "docs: preregister v41 learned router falsifier"
git fetch origin main
git merge-base --is-ancestor origin/main HEAD
git push origin HEAD:main
test "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)"
test "$(git rev-parse HEAD)" = "$(git ls-remote origin refs/heads/main | cut -f1)"
```

Expected: the new commit is the identical local, tracking, and canonical remote head.

### Task 1: Authorities, capabilities, and deterministic split

**Files:**
- Create: `crates/borsuk/src/v41_learned_router.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Consumes: exact local artifact identity tuples `(role, path, uri, sha256, blake3, encoded_bytes)` and frozen V36/V38/V39/V40 control artifacts.
- Produces: `V41LocalArtifact`, `V41LocalOutput`, `V41LocalRunMode`, `V41LocalRunRequest`, `V41QuerySplitAudit`, `V41DevelopmentSplit`, and strict partition manifest/projection types. Task 6 owns descriptor-retained byte authentication and `validate_v41_authority` once every frozen role schema exists.

- [ ] **Step 1: Write the authority and capability REDs**

Add tests named `v41_authority_rejects_identity_schema_and_generation_drift`, `v41_authority_modes_admit_only_phase_local_capabilities`, `v41_authority_split_audits_reject_cross_role_vector_duplicates`, `v41_authority_development_split_is_duplicate_safe_and_order_exact`, and `v41_authority_partition_children_bind_parent_ordinals_and_manifest`. Use literal digests and construct duplicate bit-identical query groups within development, across development/validation, and across both development/validation with holdout.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V41LocalRunMode {
    AuditDevelopmentValidation,
    PartitionDevelopment,
    Preflight,
    TrainDiagnostic,
    SelectDiagnostic,
    EvaluateDiagnostic,
    TrainFinal,
    SelectValidation,
    EvaluateValidation,
    SelectHoldout,
    AuditHoldout,
    EvaluateHoldout,
}

assert!(request.input("development-gt").is_none()); // selection
assert!(request.input("model").is_none());          // evaluation
assert!(split.training.iter().all(|q| !split.diagnostic.contains(q)));
```

- [ ] **Step 2: Run the RED**

Run: `cargo test -p borsuk --lib v41_authority_ -- --nocapture`

Expected: unresolved V41 authority/split symbols only; the filter names exactly five staged tests once the symbols exist.

- [ ] **Step 3: Implement strict types and split**

Validate exact role allowlists per phase, full 64-hex digests, nonzero lengths, and unique input/output paths and URIs. `AuditDevelopmentValidation` admits exactly those two query Parquet roles and the V36 freeze receipt, no GT/holdout role. Its receipt records exact sorted vector fingerprints, row counts, and set digests. `AuditHoldout` is unavailable until validation passes and admits only holdout query plus the prior authenticated fingerprint receipt; it checks membership without reopening development or validation queries. Every training phase requires the first audit terminal role. Task 6 performs descriptor-retained single opens, byte authentication, strict frozen schemas, source/archive/index/backend/generation cross-bindings, and predecessor-PASS validation before semantic use. Compute each within-development duplicate-group key as:

```rust
sha256(b"borsuk-v41-development-split-v1" || concat(query_f32_bits_le))
```

Sort by `(digest, minimum_query_ordinal)`, place a whole group in training only when its addition remains at most 800 rows, and place every remaining group in diagnostic. Record actual counts.

`PartitionDevelopment` is model-free. It opens the authenticated full development query/GT parents plus the split receipt and creates four strict Parquet children: training query/GT and diagnostic query/GT. Each child records sorted original query ordinals and binds both complete parent byte identities, the split-rule digest, and its exact role. Validation compares the children against the authenticated parents and the independently recomputed split, so coordinated parent or ordinal substitution fails. After all four children authenticate, one canonical partition manifest binds their four complete identities without circular child hashes. The trusted phase derives three least-authority receipts bound to the opaque manifest digest: training query/GT, diagnostic query only, and diagnostic GT only. `TrainDiagnostic` receives the split audit, training receipt, and training children; `SelectDiagnostic` receives only the diagnostic-query receipt and query; `EvaluateDiagnostic` receives only the diagnostic-evaluation receipt, diagnostic GT, sealed selection, and relation.

- [ ] **Step 4: Run GREEN and commit**

Run:

```bash
cargo test -p borsuk --lib v41_authority_ -- --nocapture
cargo fmt --all -- --check
git diff --check
```

```bash
git add crates/borsuk/src/v41_learned_router.rs crates/borsuk/src/lib.rs
git commit -m "feat(v41): add strict learned router authority"
```

### Task 2: Owner mapping and independently recomputed marginal labels

**Files:**
- Modify: `crates/borsuk/src/v38_boundary_spill.rs`
- Modify: `crates/borsuk/src/v41_learned_router.rs`

**Interfaces:**
- Consumes: authenticated V38 relation/posting summary and one query's exact GT@100 feature IDs.
- Produces: crate-visible immutable owner rows and `v41_marginal_targets(owners_by_feature, gt_feature_ids, selected, page_count) -> Result<Vec<f32>>`.

- [ ] **Step 1: Write exhaustive target REDs**

Add `v41_target_counts_each_neighbor_once_and_matches_exhaustive` and `v41_target_rejects_owner_gt_and_mask_drift`. Enumerate all owner pairs, sparse nonidentity feature IDs, GT lists, and selected subsets for a four-page fixture. The independent reference is:

```rust
let gain = gt_feature_ids.iter().filter(|feature_id| {
    let (_, primary, alternate) = owners_by_feature
        .binary_search_by_key(feature_id, |row| row.0)
        .map(|index| owners_by_feature[index])
        .unwrap();
    let row_owners = [Some(primary), alternate];
    !row_owners.iter().flatten().any(|p| selected.contains(p))
        && row_owners.iter().flatten().any(|p| *p == page)
}).count();
assert_eq!(target[page as usize].to_bits(), ((gain as f32) / 100.0).to_bits());
```

Cover duplicate owners, same primary/alternate, unknown/duplicate GT feature IDs, unsorted/duplicate selected pages, all-covered states, and checked count conversion.

- [ ] **Step 2: Run the RED**

Run: `cargo test -p borsuk --lib v41_target_ -- --nocapture`

Expected: unresolved target function/owner access only; the filter names exactly two staged tests once the symbols exist.

- [ ] **Step 3: Implement the literal relation**

Reuse `v38_v40_owner_rows_from_artifacts` rather than copying a relation decoder. Its rows are sorted by feature ID; resolve every GT feature ID by exact binary search and reject an unknown or duplicate feature ID. For every resolved neighbor, test whether either owner is already selected; if not, increment each distinct unselected owner once. Divide checked `u32` gains by the literal denominator 100. Reject wrong GT cardinality, duplicate GT feature ID, invalid owner, invalid selected mask, and non-finite output.

- [ ] **Step 4: Run GREEN and commit**

Run: `cargo test -p borsuk --lib v41_target_ -- --nocapture`

```bash
git add crates/borsuk/src/v38_boundary_spill.rs crates/borsuk/src/v41_learned_router.rs
git commit -m "feat(v41): derive exact residual page labels"
```

### Task 3: Deterministic residual model and canonical Arrow codec

**Files:**
- Modify: `crates/borsuk-fma/src/lib.rs`
- Modify: `crates/borsuk/src/v41_learned_router.rs`

**Interfaces:**
- Consumes: one finite 768-dimensional query, a sorted unique selected-page set, and a validated model.
- Produces: `V41ResidualModel`, `V41Selection`, `select_v41_pages`, `encode_v41_model`, and `decode_v41_model`.

- [ ] **Step 1: Write forward, selector, and codec REDs**

In `borsuk-fma`, add `v41_fma_dot64_matches_registered_scalar_bits` and `v41_fma_matvec64_and_768_match_registered_scalar_bits`. In `borsuk`, add `v41_model_forward_matches_scalar_reference_bits`, `v41_model_selector_is_exactly_k21_masked_and_tie_stable`, `v41_model_arrow_round_trips_and_rejects_physical_drift`, and `v41_model_parameter_and_work_arithmetic_is_exact`.

```rust
assert_eq!(v41_parameter_count(123).unwrap(), 61_307);
assert_eq!(v41_parameter_bytes(123).unwrap(), 245_228);
assert_eq!(v41_parameter_count(12_208).unwrap(), 846_832);
assert_eq!(v41_inference_macs(123).unwrap(), 300_480);
assert_eq!(v41_inference_macs(12_208).unwrap(), 16_542_720);
assert_eq!(selection.pages.len(), 21);
```

The scalar reference executes the exact registered fused accumulation order. Add golden differential cases for 64-element dot, `64x64` matvec, and `64x768` matvec on random, tie, signed-zero, subnormal, maximum-finite, and reversed-lane inputs. Mutate tensor name/order/shape/type/nullability, duplicate batches, trailing bytes, signed zero, NaN/Inf, backend, page count, selected mask, and equal-score page ties.

- [ ] **Step 2: Run the RED**

Run:

```bash
cargo test -p borsuk-fma --lib v41_fma_ -- --nocapture
cargo test -p borsuk --lib v41_model_ -- --nocapture
```

Expected: missing fused-kernel and V41 model/codec/selector symbols only; the filters name exactly two `borsuk-fma` and four `borsuk` tests once implemented.

- [ ] **Step 3: Implement the model boundary**

Add safe detected `borsuk-fma` interfaces `FusedDot64::dot`, `FusedMatVec64::matrix_vector_64x64`, and `FusedMatVec768::matrix_vector_64x768`. For width 64, lane `l` consumes `8*l+s` for `s=0..7`; for width 768 it consumes `96*l+s` for `s=0..95`; both reduce lanes 0 through 7. Each matrix row uses the corresponding dot order. AArch64 uses two four-lane `vfmaq_f32` accumulators; x86/x86_64 uses one runtime-detected AVX/FMA `_mm256_fmadd_ps` accumulator. Scientific construction fails if the fused backend is unavailable; scalar code exists only in tests.

Store tensors in row-major `Vec<f32>` with checked dimensions. Cache `q_state=W_q*q+b` once, update `s_state` by adding only the newly selected `e_s` in selection-rank order, compute `relu(q_state+W_s*s_state)`, and score all pages with the registered `borsuk-fma` backend. Select maximum finite score and break exact ties by smaller page ordinal; mask selected pages and require exactly 21 unique outputs.

Arrow tensor fields are exactly `w_q`, `b`, `w_s`, `page_embeddings`, and `page_bias`; record exact shapes, backend, page count, source identities, training split identity, and model SHA-256 in the canonical manifest. Decode one bounded IPC file and reject framing/trailing-batch drift.

- [ ] **Step 4: Run GREEN and commit**

Run:

```bash
cargo test -p borsuk-fma --lib v41_fma_ -- --nocapture
cargo test -p borsuk --lib v41_model_ -- --nocapture
```

```bash
git add crates/borsuk-fma/src/lib.rs crates/borsuk/src/v41_learned_router.rs
git commit -m "feat(v41): add residual page scorer"
```

### Task 4: Frozen rollout trainer

**Files:**
- Modify: `crates/borsuk/src/v41_learned_router.rs`

**Interfaces:**
- Consumes: authenticated development query/GT/relation rows and a `V41TrainingSpec` fixed to the spec constants.
- Produces: `train_v41_model(...) -> Result<V41TrainedModel>` and non-null training-state Parquet.

- [ ] **Step 1: Write optimizer and training REDs**

Add `v41_training_adamw_step_matches_literal_reference`, `v41_training_is_worker_and_serialization_invariant`, `v41_training_epoch_rollouts_use_frozen_weights`, and `v41_training_tiny_fixture_learns_remaining_page_gain`. Use a 12-query, 24-page learnable fixture plus a deliberately zero-gain state so the production K21 selector is valid.

```rust
assert_eq!(spec.epochs, 50);
assert_eq!(spec.batch_states, 64);
assert_eq!(spec.learning_rate.to_bits(), 0.001_f32.to_bits());
assert_eq!(spec.gradient_clip.to_bits(), 1.0_f32.to_bits());
assert_eq!(serial_model_bytes, parallel_model_bytes);
```

Mutation cases cover initialization seed, zero bias/page-bias initialization, Fisher-Yates epoch-key derivation, batch boundary, selection-rank embedding accumulation, gradient flow through selected and candidate embeddings, positive-state batch-mean normalization, whole-zero-gain batch step suppression, gradient parameter order, clipping, Adam moments/bias correction/step, decoupled weight decay, non-finite intermediates, and any checkpoint selection.

- [ ] **Step 2: Run the RED**

Run: `cargo test -p borsuk --lib v41_training_ -- --nocapture`

Expected: unresolved trainer/spec symbols only; the filter names exactly four staged tests once the symbols exist.

- [ ] **Step 3: Implement deterministic training**

Use the 32-byte initialization digest directly as the ChaCha20 key. Xavier-initialize matrices/embeddings and initialize both bias tensors to positive zero. At each epoch, clone the current model as the immutable rollout model, generate only that epoch's `query_count*21` ordered page-prefix states, and derive the shuffle RNG key as `sha256(initialization_digest || epoch.to_le_bytes())`. The live batch model accumulates selected embeddings in rollout-rank order and differentiates through selected and candidate embeddings. Mean listwise cross-entropy over positive-gain states only; a whole-zero-gain batch does not update or increment the Adam step. Reduce gradients in state then parameter order, clip one checked global norm, then apply literal bias-corrected Adam moments and decoupled weight decay. Stream training-state Parquet in at most 4,096-row groups; never retain all 50 epochs. Golden fixtures lock forward/backward/update bits.

- [ ] **Step 4: Run GREEN and commit**

Run: `cargo test -p borsuk --lib v41_training_ -- --nocapture`

```bash
git add crates/borsuk/src/v41_learned_router.rs
git commit -m "feat(v41): train deterministic residual router"
```

### Task 5: Sealed selection, fail-fast evaluation, and preflight

**Files:**
- Modify: `crates/borsuk/src/v41_learned_router.rs`

**Interfaces:**
- Consumes: model/query for selection, or sealed selection/GT/relation for evaluation, or a registered synthetic model/query for preflight.
- Produces: strict selection Parquet, canonical evaluation result, canonical preflight samples/result, and phase terminals.

- [ ] **Step 1: Write selector/evaluator REDs**

Add `v41_evaluation_selection_parquet_is_ordered_complete_and_model_bound`, `v41_evaluation_recomputes_complete_hits_and_gates`, `v41_evaluation_rejected_prefix_never_claims_full_metrics`, and `v41_evaluation_validation_and_holdout_cannot_train_or_reopen_inputs`.

```rust
assert_eq!(selection_rows.len(), query_count * 21);
assert_eq!(evaluation.aggregate_recall_ppm, total_hits * 10_000 / query_count);
assert_eq!(evaluation.minimum_recall_ppm, minimum_hits * 10_000);
assert_eq!(evaluation.allowed_misses, query_count * 100 * 2 / 1_000);
```

Define `V41EvaluationResult::{Complete, RejectedPrefix}` as separately tagged strict schemas. `Complete` contains every per-query hit count, aggregate, minimum, and pass. `RejectedPrefix` contains fixed total count, evaluated prefix count, cumulative hits/misses, stopping query ordinal, and stopping reason, and has no aggregate/minimum/pass fields. Mutate row order/cardinality, duplicate page, score bits, model identity, query ordinal, owner role, GT hit, aggregate/min/pass, prefix counters, and variant-only fields. Require immediate rejection on `<80` hits for one query or cumulative misses above the exact split-dependent allowance, but never early success.

- [ ] **Step 2: Write source-free preflight REDs**

Add `v41_preflight_measures_complete_hot_inference_at_both_page_counts` and `v41_preflight_rejects_sample_resource_and_backend_drift`. Require 1,024 warmups and 10,000 raw `u64` nanosecond samples for `P=123` and `P=12_208`; sort a copy and select zero-based index 9,899. Model validation and allocation occur before timing.

- [ ] **Step 3: Run the REDs**

Run:

```bash
cargo test -p borsuk --lib v41_evaluation_ -- --nocapture
cargo test -p borsuk --lib v41_preflight_ -- --nocapture
```

Expected: unresolved evaluation/preflight types and functions only; the filters name exactly four evaluation and two preflight tests once implemented.

- [ ] **Step 4: Implement and run GREEN**

Use non-null Parquet fields `(query_ordinal u32, selection_rank u8, page_ordinal u32, score_bits u32, backend string, model_sha256 string)`. Evaluation counts each GT feature once when either owner is selected and emits the honest complete or rejected-prefix variant. Preflight measures the complete 21-round scoring loop including masks, reductions, checks, and tie-breaking; its MAC field is arithmetic evidence only. It reads cgroup memory/swap and `/proc/pressure/memory` and emits a create-only terminal.

Run:

```bash
cargo test -p borsuk --lib v41_evaluation_ -- --nocapture
cargo test -p borsuk --lib v41_preflight_ -- --nocapture
cargo fmt --all -- --check
git diff --check
```

- [ ] **Step 5: Commit**

```bash
git add crates/borsuk/src/v41_learned_router.rs
git commit -m "feat(v41): seal learned router evaluation"
```

### Task 6: Capability-separated local executable

**Files:**
- Modify: `crates/borsuk/src/v41_learned_router.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Create: `crates/borsuk/examples/v41_learned_router.rs`

**Interfaces:**
- Consumes: `V41LocalRunRequest` with exact phase-local inputs and outputs.
- Produces: `run_v41_local_request(request) -> Result<Vec<u8>>` and a strict CLI whose stdout is canonical result bytes only.

- [ ] **Step 1: Stage library-runner and CLI REDs**

Add `v41_local_runner_opens_each_input_once_and_publishes_create_only`, `v41_cli_parses_every_phase_exactly`, and `v41_cli_rejects_remote_page_and_cross_phase_capabilities`.

```rust
let _runner: fn(V41LocalRunRequest) -> borsuk::Result<Vec<u8>> = run_v41_local_request;
for forbidden in ["--bucket", "--endpoint", "--page-prefix", "--d3", "--corpus"] {
    assert!(parse_v41_args(with_flag(forbidden)).is_err());
}
```

Cover missing, duplicate, unknown, wrong-phase, zero-length, digest-drift, path/URI overlap, output preexistence, invalid workers, absent `--execute-v41`, and trailing positional values.

- [ ] **Step 2: Run the REDs**

Run:

```bash
cargo test -p borsuk --lib v41_local_ -- --nocapture
cargo test -p borsuk --example v41_learned_router v41_cli_ -- --nocapture
```

Expected: missing high-level runner/parser symbols only; the filters name exactly one library runner test and two CLI tests once implemented.

- [ ] **Step 3: Implement the thin boundary**

Each mode receives only its allowlisted roles, opens each input once for declared length plus one overflow byte, retains those descriptors through parsing, authenticates strict frozen schemas and source/archive/index/backend/generation bindings before semantic use, validates every required predecessor PASS, and uses exclusive create-new outputs. `validate_v41_authority` owns that boundary. The example parses exact identity tuples and `--workers`, requires `--execute-v41`, imports no object-store/AWS/page-store type, writes progress to stderr, writes canonical result bytes to stdout, and exits nonzero on every error.

- [ ] **Step 4: Run GREEN and commit**

Run:

```bash
cargo test -p borsuk --lib v41_local_ -- --nocapture
cargo test -p borsuk --example v41_learned_router v41_cli_ -- --nocapture
cargo fmt --all -- --check
git diff --check
```

```bash
git add crates/borsuk/src/v41_learned_router.rs crates/borsuk/src/lib.rs \
  crates/borsuk/examples/v41_learned_router.rs
git commit -m "feat(v41): add local learned router phases"
```

### Task 7: Spot controller with phase-local staging and fast aborts

**Files:**
- Create: `scripts/run_v41_learned_router_spot.py`
- Create: `scripts/test_run_v41_learned_router_spot.py`
- Create: `scripts/requirements-v41-learned-router.txt`

**Interfaces:**
- Consumes: one immutable V41 phase plan, source archive, release binary, predecessor terminals, and exact phase inputs.
- Produces: one create-only progress/result/terminal namespace per Spot attempt and immediate instance termination.

- [ ] **Step 1: Write controller REDs**

Add unittest cases for exact plan schema, three-AZ Spot fallback, phase role allowlists, no GT in selection, no query/model in evaluation, exact successful-predecessor transition table, campaign-wide holdout consumption, create-only namespace, cgroup v2 limits, phase-specific memory/scratch/output/progress/wall stops, interruption discard, idempotent terminal verification, and mandatory termination even when publication or terminal verification fails.

```python
self.assertEqual(phase_input_roles("select-validation"), (
    "validation-query", "model", "source-archive", "binary"))
self.assertEqual(phase_input_roles("audit-development-validation"), (
    "development-query", "validation-query", "freeze-receipt"))
self.assertNotIn("validation-gt", phase_input_roles("select-validation"))
self.assertEqual(instance_market_options(), {"MarketType": "spot"})
```

- [ ] **Step 2: Run the RED**

Run:

```bash
uv run --offline --python 3.12 \
  --with-requirements scripts/requirements-v41-learned-router.txt \
  python -m unittest scripts.test_run_v41_learned_router_spot
```

Expected: missing V41 controller module only.

- [ ] **Step 3: Implement the controller**

Follow the tested `publication_v3_aws.py`/V38 lifecycle primitives. Register these science caps: audit/partition `1 GiB memory, 256 MiB scratch/output, 60 s progress, 600 s wall`; diagnostic/final training `3 GiB memory, 1 GiB scratch, 256 MiB output, 120 s progress, 3,600/5,400 s wall`; selection/evaluation `512 MiB memory, 256 MiB scratch/output, 60 s progress, 600 s wall`; preflight retains `256 MiB memory`, zero swap, PSI 0.75, and 300 s wall. Start one transient cgroup slice, stage only allowlisted objects, run one binary process group, sample liveness at at most 30-second intervals, enforce scratch/output during execution, publish outputs and one terminal with conditional create, validate that terminal from S3, then terminate in `finally` even if publication/validation fails.

An EC2 interruption discards the incomplete attempt and permits a new attempt only from a complete successful predecessor. Before opening any holdout input, conditionally create the campaign-wide `holdout-consumed` marker. Afterward, interruption publishes `holdout-indeterminate` and closes the arm unless a complete authenticated holdout result already exists; no retry may reopen holdout.

- [ ] **Step 4: Run GREEN and static checks**

Run:

```bash
uv run --offline --python 3.12 \
  --with-requirements scripts/requirements-v41-learned-router.txt \
  python -m unittest scripts.test_run_v41_learned_router_spot
uv run --offline --python 3.12 --with ruff==0.15.20 ruff check \
  scripts/run_v41_learned_router_spot.py scripts/test_run_v41_learned_router_spot.py
python3 -m py_compile scripts/run_v41_learned_router_spot.py \
  scripts/test_run_v41_learned_router_spot.py
git diff --check
```

- [ ] **Step 5: Commit**

```bash
git add scripts/run_v41_learned_router_spot.py \
  scripts/test_run_v41_learned_router_spot.py \
  scripts/requirements-v41-learned-router.txt
git commit -m "feat(v41): orchestrate learned router on spot"
```

### Task 8: Execute the one-million-row fail-fast ladder

**Files:**
- Modify after each terminal: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: frozen V36/V38/V39/V40 roles and the exact V41 source/binary/plan identities.
- Produces: authenticated phase artifacts, terminal disposition, and a decision to stop or advance.

- [ ] **Step 1: Build once and run both source-free preflights**

Build the release binary on a transient Spot builder, authenticate it, then run the development/validation query-only duplicate audit and deterministic development partition. Run `P=123` and `P=12_208` preflight in one registered preflight phase. Stop on p99 above 5 ms, memory above 256 MiB, any swap growth, PSI full avg10 above 0.75, backend drift, or sample-count drift.

- [ ] **Step 2: Run the 800/train-to-200/diagnostic screen**

Train from the fixed development training partition. Seal all diagnostic selections before evaluation opens diagnostic truth. Abort evaluation on the first query below 80 hits or once cumulative misses exceed:

```text
floor(0.002 * 100 * diagnostic_query_count)
```

Require aggregate at least 998,000 ppm and minimum at least 800,000 ppm. On failure, publish `diagnostic-rejected`, terminate the instance, update the ledger, and stop V41 immediately.

- [ ] **Step 3: Run final development training and validation only after diagnostic pass**

Restart from the identical initialization and train once on all 1,000 development queries. Freeze the model and source/binary authorities. Seal all 1,000 validation selections, then evaluate with immediate failure on one query below 80 hits or cumulative misses above 200. Do not choose or retrain a checkpoint from validation output.

- [ ] **Step 4: Open holdout once only after validation pass**

Conditionally create `holdout-consumed` before opening any holdout input. Then run the holdout query-only duplicate audit against the prior development/validation bitset receipts. Use the unchanged model, K21, gates, and stops; seal selection before opening holdout GT. A scientific failure publishes `holdout-rejected`; an interruption after consumption publishes `holdout-indeterminate`; a pass publishes `learned-router-feasible`. Every disposition ends V41 and none permits a holdout rerun.

- [ ] **Step 5: Qualify transport only after holdout pass**

Create a separate preregistered transport plan for coarse/fine objects, cache state, concurrency, GET count, bytes, p99, throughput, and reranking. This step does not execute physical S3 inside V41 and does not unlock 10M/100M automatically.

- [ ] **Step 6: Record evidence after every terminal**

Record exact dataset/split/count, baseline, measured units, source/binary/input/output identities, training loss and stop counters, recall distribution, timing, memory, PSI, swap, instance identity, disposition, and unopened capabilities.

Run: `python3 scripts/validate_research_docs.py && git diff --check`

```bash
git add docs/research/publication-v3-attempt-ledger.md
git commit -m "research: record v41 learned router evidence"
```

### Task 9: Final assurance, independent review, and delivery

**Files:**
- Verify: every file changed in Tasks 1-8.

**Interfaces:**
- Consumes: stable implementation and evidence commits.
- Produces: one independently reviewed fast-forward canonical `origin/main`.

- [ ] **Step 1: Run narrow affected gates first**

Run the exact `v41_` Rust filters, example tests, controller unittest, Ruff, py_compile, docs validator, formatting, and `git diff --check`. Repair only the failing layer test-first and rerun only that layer.

- [ ] **Step 2: Run one full assurance remotely after the diff is stable**

Run serially on one pressure-monitored Spot worker:

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest discover -s scripts -p 'test_*.py'
python3 scripts/validate_research_docs.py
git diff --check
```

Stop at the first failure. After a repair, rerun the failed narrow layer and then exactly one final full sequence.

- [ ] **Step 3: Reconcile one final dual Fable/Astra critique**

Provide both reviewers the final spec, plan, diff, and terminal evidence. Independently verify each Critical/Important finding against code and artifacts. Repair confirmed defects test-first and rerun the affected gate plus the single final assurance sequence.

- [ ] **Step 4: Push and verify canonical equality**

```bash
git fetch origin main
git merge-base --is-ancestor origin/main HEAD
git push origin HEAD:main
test "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)"
test "$(git rev-parse HEAD)" = "$(git ls-remote origin refs/heads/main | cut -f1)"
test -z "$(git status --short)"
```

Expected: fast-forward push, identical local/tracking/remote commit, and clean worktree.

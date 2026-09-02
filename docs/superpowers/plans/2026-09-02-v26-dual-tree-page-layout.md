# V26 Dual-Tree Page Layout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the rejected inherited page layout with a deterministic query-independent dual-tree layout and reject it cheaply unless its exact eight-page oracle clears 975,000 ppm.

**Architecture:** A small V26 crate builds two balanced projection trees from construction vectors only and emits disjoint primary/replica pages of at most 704 rows. A phase-separated evaluator joins frozen truth only after construction closes, runs the layout oracle first, and permits exact-global scoring and tree routing only after their preceding gates pass.

**Tech Stack:** Rust 2024, Arrow/Parquet 58.3, Rayon, SHA-256, Python 3.12 standard library, pinned Ruff 0.15.20, AWS EC2 Spot through profile `causality`.

**Spec:** `docs/superpowers/specs/2026-09-02-v26-dual-tree-page-layout-design.md`

## Global Constraints

- V26 is a clean format; do not add V24/V25 readers, aliases, migrations, or version dispatch.
- Use Parquet/Arrow IPC for bulk cross-language data and canonical JSON only for small authority/evidence objects.
- The construction process cannot open pseudoquery, truth, prior-result, benchmark-query, or page-quality roles.
- Page capacity is exactly 704, every row has one primary and one replica page, and tree page ranges are disjoint.
- Run one named RED/GREEN while iterating, one affected crate gate per coherent slice, and strict Clippy/full workspace only at the milestone.
- The warm named gate must complete in under one second. A cold dependency build
  is paid once and reported separately; it is never repeated after a logic
  failure.
- Stop before exact-global scoring unless layout aggregate recall is at least 975,000 ppm and minimum-query recall at least 800,000 ppm.
- Stop before routing unless exact-global aggregate recall is at least 975,000 ppm and oracle attainment at least 995,000 ppm.
- Scientific work uses one `causality` Spot worker with multi-AZ fallback, no DGX, zero page-body reads, zero swap growth, and immediate terminal shutdown.

---

## File Structure

- Create `crates/borsuk-v26/Cargo.toml`: minimal Arrow/Parquet, Rayon, serde, and digest dependencies.
- Create `crates/borsuk-v26/src/lib.rs`: authority types, constants, causal result validation, and canonical serializers.
- Create `crates/borsuk-v26/src/tree.rs`: deterministic dual-tree construction and query traversal.
- Create `crates/borsuk-v26/src/local.rs`: strict Parquet readers/writers and phase-separated local requests.
- Modify root `Cargo.toml`: add only the `borsuk-v26` workspace member.
- Create `crates/borsuk/examples/v26_page_layout.rs`: thin offline CLI with no storage client.
- Create `scripts/run_v26_page_layout.py` and its unittest: process monitor, progress, terminal, cleanup.
- Create `scripts/launch_v26_page_layout_spot.py` and its unittest: `causality` Spot multi-AZ launch and termination.
- Create `docs/research/v26-page-layout-open-manifest.json` only after the implementation milestone is verified.
- Update `docs/research/publication-v3-attempt-ledger.md` only after an authenticated terminal.

### Task 1: Pure tree and authority contracts

**Files:**
- Create: `crates/borsuk-v26/Cargo.toml`
- Create: `crates/borsuk-v26/src/lib.rs`
- Create: `crates/borsuk-v26/src/tree.rs`
- Modify: `Cargo.toml`

**Interfaces:**
- Produces: `V26ObjectIdentity`, `V26LayoutAuthority`, `V26LayoutReceipt`, `V26Tree`, `V26Node`, `V26RowPages`, `build_v26_dual_tree_layout`, `route_v26_pages`, `canonical_v26_layout_receipt_bytes`.

```rust
pub struct V26ObjectIdentity {
    pub role: String, pub uri: String, pub digest_algorithm: String,
    pub digest: String, pub encoded_bytes: u64, pub generation: String,
}
pub struct V26LayoutAuthority {
    pub schema: String, pub generation: String, pub primary_seed: u64,
    pub replica_seed: u64, pub page_capacity: u32, pub expected_rows: u64,
}
pub struct V26LayoutReceipt {
    pub authority: V26LayoutAuthority, pub inputs: Vec<V26ObjectIdentity>,
    pub outputs: Vec<V26ObjectIdentity>, pub projection_steps: u64,
    pub query_role_opens: u64, pub claim_eligible: bool,
}
pub enum V26Disposition {
    AuthorityStop, LayoutRejected, RankReducerRejected,
    TreeRouterRejected, BoundedLayoutCandidate,
}
pub struct V26ConstructionRow { pub source_ordinal: u64, pub vector: [f32; 96] }
pub struct V26Node {
    pub node_ordinal: u32,
    pub left: Option<u32>,
    pub right: Option<u32>,
    pub direction_ordinal: u8,
    pub threshold: f32,
    pub leaf_page: Option<u32>,
}
pub struct V26Tree { pub seed: u64, pub root: u32, pub nodes: Vec<V26Node> }
pub struct V26RowPages { pub source_ordinal: u64, pub primary_page: u32, pub replica_page: u32 }
pub fn build_v26_dual_tree_layout(
    authority: &V26LayoutAuthority,
    rows: &[V26ConstructionRow],
    worker_count: usize,
) -> Result<(V26Tree, V26Tree, Vec<V26RowPages>)>;
pub fn route_v26_pages(
    primary: &V26Tree,
    replica: &V26Tree,
    query: &[f32; 96],
    page_budget: usize,
) -> Result<Vec<u32>>;
```

- [ ] **Step 1: Write the tree RED.** Add `v26_tree_balances_aligned_leaves_and_is_byte_deterministic`. Use 1,409 literal 96D rows so each tree requires three leaves. Assert complete ordinal inventory, maximum 704 rows per leaf, disjoint page ranges, distinct primary/replica pages, the exact left-maximum threshold and `<=` branch rule, deterministic sibling-margin ties, and byte-identical one-worker/four-worker output.
- [ ] **Step 2: Write the authority RED.** Add `v26_tree_layout_receipt_recomputes_counts_work_and_identities`. Mutation-lock schema, both seeds, capacity, source/archive/generation, role/URI/digest/length, row and page counts, actual projection steps, worker count, RSS/PSI/swap, query-role-open count zero, and claim eligibility false.
- [ ] **Step 3: Run focused RED.** Run `cargo test -p borsuk-v26 v26_tree_ -- --nocapture`. Require unresolved V26 symbols only.
- [ ] **Step 4: Implement the minimal tree.** Generate 16 SplitMix sign directions per node; compute fixed 96-step f32 scores; choose maximum adjacent split gap then direction ordinal; sort by `(score, source_ordinal)`; split with the registered aligned formula; number nodes/leaves by preorder.
- [ ] **Step 5: Implement strict canonical authority.** Validate all derivable counts and identities, serialize sorted compact JSON plus one LF, and reject any query/evaluation role in construction inputs.
- [ ] **Step 6: Run GREEN and mechanical checks.** Run `cargo test -p borsuk-v26 v26_tree_ -- --nocapture`, `cargo fmt --all -- --check`, and `git diff --check`. Commit `Add V26 dual-tree layout contracts`.

### Task 2: Parquet construction boundary

**Files:**
- Create: `crates/borsuk-v26/src/local.rs`
- Modify: `crates/borsuk-v26/src/lib.rs`

**Interfaces:**
- Consumes: the registered clean construction/source-map Parquet schemas as immutable research inputs, without importing V24 or V25 code.
- Produces: `V26LayoutBuildRequest`, `run_v26_layout_build`, `tree-a.parquet`, `tree-b.parquet`, `page-assignments.parquet`, `layout-receipt.json`.

```rust
pub struct V26LocalObjectPath { pub identity: V26ObjectIdentity, pub path: PathBuf }
pub struct V26LayoutBuildRequest {
    pub manifest: V26LayoutAuthority,
    pub construction_rows: V26LocalObjectPath,
    pub source_map: V26LocalObjectPath,
    pub output_dir: PathBuf,
    pub worker_count: usize,
}
pub fn run_v26_layout_build(request: &V26LayoutBuildRequest) -> Result<V26LayoutReceipt>;
```

- [ ] **Step 1: Write schema/identity RED.** Add `v26_layout_local_authenticates_construction_only_and_emits_parquet`. Require nonnullable `u64 source_ordinal`, nonnullable fixed-list `f32[96] vector` with child `element`, exact source-map equality, full finite normalized rows, and exact file identities before parsing.
- [ ] **Step 2: Write capability RED.** Add `v26_layout_local_rejects_query_truth_and_result_roles`. Pass each forbidden role and assert rejection before any output path exists.
- [ ] **Step 3: Write output mutation RED.** Add `v26_layout_local_rejects_output_schema_topology_and_identity_drift`. Reopen all three Parquets and reject outer/child nullability, field/type/order, duplicate/missing ordinal, page overlap, capacity, tree topology, seed, generation, and digest drift.
- [ ] **Step 4: Run focused RED.** Run `cargo test -p borsuk-v26 v26_layout_local_ -- --nocapture`.
- [ ] **Step 5: Implement streaming input and bounded output.** Decode construction in registered batches, build the two trees, write fixed schemas with Parquet 2.0 and zstd, sync files, compute identities, reopen and validate before returning the receipt.
- [ ] **Step 6: Run GREEN and commit.** Run the same selector and fmt/diff. Commit `Build authenticated V26 page layouts`.

### Task 3: Layout-only oracle gate

**Files:**
- Modify: `crates/borsuk-v26/src/local.rs`
- Modify: `crates/borsuk-v26/src/lib.rs`

**Interfaces:**
- Produces: `V26LayoutEvaluationRequest`, `V26LayoutSample`, `V26LayoutResult`, `evaluate_v26_layout_oracle`, `canonical_v26_layout_result_bytes`.

```rust
pub struct V26LayoutEvaluationRequest {
    pub layout_terminal: V26LocalObjectPath,
    pub page_assignments: V26LocalObjectPath,
    pub pseudoqueries: V26LocalObjectPath,
    pub truth: V26LocalObjectPath,
    pub expected_queries: u32,
}
pub struct V26LayoutSample {
    pub query_ordinal: u32, pub selected_pages: Vec<u32>,
    pub hits: u32, pub recall_ppm: u64,
}
pub struct V26LayoutResult {
    pub schema: String, pub query_count: u32, pub aggregate_recall_ppm: u64,
    pub minimum_query_recall_ppm: u64, pub disposition: V26Disposition,
    pub page_body_reads: u64, pub claim_eligible: bool,
}
pub fn evaluate_v26_layout_oracle(
    request: &V26LayoutEvaluationRequest,
) -> Result<(Vec<V26LayoutSample>, V26LayoutResult)>;
pub fn canonical_v26_layout_result_bytes(
    result: &V26LayoutResult,
    samples: &[V26LayoutSample],
) -> Result<Vec<u8>>;
```

- [ ] **Step 1: Write exact-oracle RED.** Add `v26_layout_oracle_uses_both_pages_and_prefers_shorter_lexicographic_cover`. Use ten literal neighbor assignments including a redundant longer full cover; require maximum hits then lexicographically smallest page vector.
- [ ] **Step 2: Write phase-boundary RED.** Add `v26_layout_oracle_evaluation_opens_truth_only_after_layout_terminal`. Require exact layout terminal and assignment identities; reject missing/failed terminal and construction directories containing evaluation roles.
- [ ] **Step 3: Write metric/result RED.** Add `v26_layout_oracle_result_recomputes_samples_gates_and_disposition`. Mutation-lock 512 ordered queries, ten unique neighbors, assignment bindings, page lists, hits, aggregate/minimum recall, 975,000/800,000/995,000 gates, causal disposition, zero page reads, and claim eligibility false.
- [ ] **Step 4: Run focused RED.** Run `cargo test -p borsuk-v26 v26_layout_oracle_ -- --nocapture`.
- [ ] **Step 5: Implement the smallest evaluator.** Read only page assignments and truth after authenticating the closed layout; do not open construction vectors or run distance scoring. Recompute each exact cover and aggregate independently during serialization.
- [ ] **Step 6: Run GREEN and commit.** Run the same selector and fmt/diff. Commit `Fail fast on V26 layout containment`.

### Task 4: Offline executable and monitored Spot boundary

**Files:**
- Create: `crates/borsuk/examples/v26_page_layout.rs`
- Create: `scripts/run_v26_page_layout.py`
- Create: `scripts/test_run_v26_page_layout.py`
- Create: `scripts/launch_v26_page_layout_spot.py`
- Create: `scripts/test_launch_v26_page_layout_spot.py`

**Interfaces:**
- Produces strict modes `--build-layout` and `--evaluate-layout`; each accepts one manifest, one input directory, one empty output directory, and explicit `--execute`.

```text
v26_page_layout --manifest <file> --input-dir <dir> --output-dir <empty-dir> \
  (--build-layout | --evaluate-layout) --execute
```

- [ ] **Step 1: Write CLI RED.** Test required/duplicate/unknown flags, mutually exclusive phase modes, exact inventory, no bucket/endpoint/page/D3 flags, canonical stdout, and nonzero errors.
- [ ] **Step 2: Write monitor RED.** Test process exit, wall/RSS/PSI/swap/no-progress stops, process-group termination, terminal preservation, and explicit known-file cleanup.
- [ ] **Step 3: Write Spot RED.** Test profile exactly `causality`, one-time terminate-on-interruption Spot, ordered three-AZ capacity fallback, one original per phase, terminal termination, and no On-Demand fallback.
- [ ] **Step 4: Run narrow REDs.** Run the example selector, then `python3 -m unittest scripts.test_run_v26_page_layout scripts.test_launch_v26_page_layout_spot`.
- [ ] **Step 5: Implement thin boundaries.** CLI calls only the library requests; Python performs credentialed staging outside the scientific process and strips AWS/proxy variables before execution.
- [ ] **Step 6: Run affected GREEN.** Run the same selectors, pinned Ruff, py_compile, fmt, and diff. Commit `Launch fail-fast V26 layout screens`.

### Task 5: Authentic smoke and open layout decision

**Files:**
- Create: `docs/research/v26-page-layout-open-manifest.json`
- Update after terminal: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes exact V25 construction/source-map/pseudoquery/truth identities from the terminal V25 screen.
- Produces one authenticated `layout-rejected` or `layout-candidate` result.

- [ ] **Step 1: Freeze the manifest.** Bind source commit/archive, binary, exact four Parquets, seeds, capacity 704, expected 262,144 rows, 373 leaves/tree, 746 pages, 512 queries, gates, 1 GiB RSS, 0.5 PSI, zero swap growth, 300-second wall/progress, outputs, and no-restart semantics.
- [ ] **Step 2: Run authentic 4,096-row structural smoke.** Use one Spot process and the first 4,096 source ordinals selected by literal ordinal range from the authenticated construction/source-map inputs. Require exact inventory, two-copy assignments, maximum capacity, byte-identical repeated validation, zero query/truth-role opens, zero page reads, RSS below 512 MiB, and wall below 30 seconds. Do not compute or report recall from this smoke.
- [ ] **Step 3: Run construction once.** Stage only construction/source-map roles, build both trees and assignments, upload terminal, and terminate the builder.
- [ ] **Step 4: Run layout-only evaluation once.** Stage the closed layout plus truth/pseudoquery roles, compute 512 exact covers, and stop if either 975,000 aggregate or 800,000 minimum recall misses.
- [ ] **Step 5: Authenticate and record.** Recompute every sample from Parquet, verify terminal/resource/cleanup evidence, terminate compute, and commit only the manifest and ledger with `Record V26 layout causal screen`.

### Task 6: Conditional exact-global and router gates

**Condition:** Execute only if Task 5 returns `layout-candidate`.

**Files:**
- Modify: `crates/borsuk-v26/src/local.rs`
- Modify: `crates/borsuk-v26/src/tree.rs`
- Update after terminal: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Produces exact-global rank-reducer evidence, then fixed best-first dual-tree routing evidence.

```rust
pub struct V26ExactGlobalRequest {
    pub construction_rows: V26LocalObjectPath,
    pub layout: V26LayoutEvaluationRequest,
    pub ranked_row_limits: Vec<u32>,
}
pub struct V26TreeRouterRequest {
    pub primary_tree: V26LocalObjectPath, pub replica_tree: V26LocalObjectPath,
    pub layout: V26LayoutEvaluationRequest, pub page_budget: u32,
}
pub fn evaluate_v26_exact_global(
    request: &V26ExactGlobalRequest,
) -> Result<Vec<V26ContainmentSample>>;
pub fn evaluate_v26_tree_router(
    request: &V26TreeRouterRequest,
) -> Result<Vec<V26ContainmentSample>>;
```

- [ ] **Step 1: Reuse the V25 exact-global scorer contract.** Test rank limits 10 through 4,096, own-page exclusion, best-row-per-page ties, and independent sample/aggregate recomputation against the new assignments.
- [ ] **Step 2: Run exact-global open gate.** Stop below 975,000 aggregate or 995,000 oracle attainment; do not train or tune a router.
- [ ] **Step 3: Test fixed tree routing.** Best-first expand sibling margins from both roots, emit exactly eight unique leaves, prohibit outcome-dependent widening and exhaustive fallback, and require scalar/parallel page equality.
- [ ] **Step 4: Run routing open gate.** Require 975,000 aggregate, 995,000 oracle attainment, projected memory below 3 GiB, warm p99 below 12 ms, and no more than eight page reads.
- [ ] **Step 5: Run milestone assurance once.** Run strict workspace/all-targets Clippy and one locked workspace/all-targets test only after all focused gates are GREEN. Commit `Qualify V26 bounded page routing`.

### Task 7: Sealed sentry and release progression

**Condition:** Execute only after Task 6 passes without changing frozen parameters.

**Files:**
- Create: `docs/research/v26-page-layout-sentry-authority.json`
- Update: `docs/research/publication-v3-attempt-ledger.md`

- [ ] **Step 1: Freeze a disjoint 1,048,576-row sentry before opening its query/truth roles.** Bind all split, binary, layout, router, resource, and output identities.
- [ ] **Step 2: Run one Spot sentry.** No retries after a scientific terminal and no parameter selection from sentry outcomes.
- [ ] **Step 3: Stop or promote.** Any authority, recall, oracle, RSS, p99, or page-read miss records rejection. A complete pass alone authorizes full construction planning; it does not itself make a competitor claim.

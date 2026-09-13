# V40 Boundary-Spill Router Falsifier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a capability-separated 1M falsifier that tries the frozen V38 ownership tree directly at K21, conditionally tries an overlap-aware accepted-spill router, and models coarse S3 request/byte cost without reading page bodies.

**Architecture:** A new `v40_spill_router` module reuses strict V37/V38 tree and relation codecs but owns new authority, summary, selection, evaluation, and simulation formats. Direct best-bin-first routing is mandatory; only its authenticated failure enables the one-hop accepted-spill challenger. A thin Rust example and Python Spot controller keep construction, query selection, truth evaluation, and orchestration separate.

**Tech Stack:** Rust, Arrow IPC, Parquet, Serde canonical JSON, `borsuk-fma`, Python 3.12, boto3, AWS EC2 Spot/S3 with profile `causality`.

**Spec:** `docs/superpowers/specs/2026-09-13-v40-boundary-spill-router-falsifier-design.md`

## Global Constraints

- Frozen population: ReLAION2B cohort A, 1,000,000 non-null `f32[768]` rows, 1,000 burned development queries, exact GT@100.
- Exactly 21 distinct postings; pass at `>=998_000` ppm aggregate and `>=800_000` ppm minimum-query containment.
- Construction has no query/GT capability; selection has no GT capability; evaluation has no query-vector or selector capability.
- Direct always precedes challenger. Freeze frontier64, top32 alternates, candidate cap2,112, node-pop cap1,024.
- Parquet carries tables, Arrow IPC carries packed arrays, and recursively sorted compact JSON plus one LF carries control artifacts.
- The simulator has no network/page API; its bound is 21 logical GETs and 11,010,048 modeled bytes/query.
- Rust compilation and science use `causality` AWS Spot while the local devbox remains swap-pressured.
- No validation, holdout, 10M, 100M, physical S3, fine-vector fetch, compatibility reader, alias, or migration path belongs to V40.

## Accelerated execution order

Task numbering groups related components; it is not the execution order after
Task 1. Implement the direct-only portions of Tasks 4, 5, and 6 immediately
after Task 1, then execute Task 7 Steps 1-2. If the authenticated direct arm
passes, skip Tasks 2-3 and every accepted-spill mode permanently. Only an
authenticated `direct-failed` terminal authorizes Tasks 2-3 and the
challenger-only portions of Tasks 4-7. This preserves every quality and
resource gate while avoiding work on a fallback before the baseline needs it.

---

### Task 1: Deterministic K21 tree frontier

**Files:**
- Modify: `crates/borsuk/src/v37_relation_router.rs`
- Create: `crates/borsuk/src/v40_spill_router.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Consumes: `V37BalancedTree`, the existing fused hyperplane scorer, and runtime query vectors.
- Produces: `select_v37_tree_postings` as a crate-visible generalized primitive and `select_v40_tree_frontier(...) -> Result<V40TreeFrontier>`.

- [ ] **Step 1: Write the failing tests**

Add `v40_tree_frontier_matches_exhaustive_order_and_fails_closed` and
`v40_tree_frontier_handles_equal_margins_signed_zero_and_shortage`. The test
reference enumerates every leaf path, computes maximum squared wrong-side
margin, and sorts `(penalty.total_cmp,leaf_node,posting)`.

```rust
pub(crate) struct V40TreeFrontier {
    pub(crate) posting_ordinals: Vec<u32>,
    pub(crate) node_pops: u32,
    pub(crate) scored_internal_nodes: u32,
}
assert_eq!(frontier.posting_ordinals, vec![0, 2, 1, 3]);
assert!(select_v40_tree_frontier(&tree, backend, &query, 5, 7).is_err());
```

- [ ] **Step 2: Run the RED**

Run: `cargo test -p borsuk --lib v40_tree_frontier_ -- --nocapture`

Expected: unresolved V40 frontier type/function only.

- [ ] **Step 3: Implement the minimal wrapper**

Make the existing generalized helper crate-visible without copying traversal:

```rust
pub(crate) fn select_v37_tree_postings(
    tree: &V37BalancedTree,
    kernel: borsuk_fma::FusedDot8x12,
    maximum_node_visits: usize,
    posting_limit: usize,
    query: &[f32],
) -> Result<V37DirectSelection>;
```

The V40 wrapper accepts posting limit `1..=64`, pops `1..=1024`, exact backend,
finite runtime dimensions, and checked `u32` counters.

- [ ] **Step 4: Run GREEN and commit**

Run: `cargo test -p borsuk --lib v40_tree_frontier_ -- --nocapture`

Run: `cargo fmt --all -- --check && git diff --check`

```bash
git add crates/borsuk/src/v37_relation_router.rs crates/borsuk/src/v40_spill_router.rs crates/borsuk/src/lib.rs
git commit -m "feat(v40): add deterministic tree frontier"
```

### Task 2: Accepted-spill counts and packed Q24 summary

**Files:**
- Modify: `crates/borsuk/src/v38_boundary_spill.rs`
- Modify: `crates/borsuk/src/v40_spill_router.rs`

**Interfaces:**
- Consumes: decoded `V38SpillRecord` and `V38PostingSummary` values.
- Produces: `build_v40_spill_counts`, `pack_v40_spill_summary`, and strict Parquet/Arrow codecs.

- [ ] **Step 1: Stage field-access and summary REDs**

Expose only the immutable decoded V38 fields to sibling crate modules. Add
table-driven cases for no alternate, one alternate, more than 32 alternates,
equal-count ordinal ties, omitted mass, maximum remainder, overflow, malformed
owner order, duplicate owners, capacity drift, and posting-summary drift.

```rust
pub(crate) const V40_Q24_TOTAL: u32 = 1 << 24;
pub(crate) struct V40PackedSpillSummary {
    pub(crate) offsets: Vec<u64>,
    pub(crate) alternate_postings: Vec<u32>,
    pub(crate) masses_q24: Vec<u32>,
    pub(crate) residual_masses_q24: Vec<u32>,
}
```

Run: `cargo test -p borsuk --lib v40_spill_summary_ -- --nocapture`

Expected: missing summary/codec symbols after the visibility-only boundary is
made testable.

- [ ] **Step 2: Implement streaming counts and exact apportionment**

Hold one primary posting's `BTreeMap<u32,u64>` at a time. Retain top32 by
`(count desc,alternate asc)`. Fold omitted alternates and no-alternate rows into
residual. Compute floor masses with checked `u128(count)*2^24/population`, then
assign remaining units by `(remainder desc,alternate-before-residual,posting)`.
Require every posting's masses to sum exactly to `2^24`.

Use exact schemas:

```text
Parquet: primary_posting u32!, category_role u8!, alternate_posting u32?,
         count u64!, primary_population u64!
Arrow:   offsets u64[], alternate_postings u32[], masses_q24 u32[],
         residual_masses_q24 u32[]
```

Reject schema/null/order/count/framing/trailing-batch drift.

- [ ] **Step 3: Run GREEN and commit**

Run: `cargo test -p borsuk --lib v40_spill_summary_ -- --nocapture`

Run: `cargo fmt --all -- --check && git diff --check`

```bash
git add crates/borsuk/src/v38_boundary_spill.rs crates/borsuk/src/v40_spill_router.rs
git commit -m "feat(v40): encode accepted spill summary"
```

### Task 3: Overlap-aware marginal selection

**Files:**
- Modify: `crates/borsuk/src/v40_spill_router.rs`

**Interfaces:**
- Consumes: `V40TreeFrontier`, `V40PackedSpillSummary`.
- Produces: `select_v40_accepted_spill_postings(...) -> Result<V40Selection>`.

- [ ] **Step 1: Write exhaustive selector REDs**

```rust
pub(crate) enum V40RouterArm { DirectTree, AcceptedSpill }
pub(crate) struct V40Selection {
    pub(crate) arm: V40RouterArm,
    pub(crate) posting_ordinals: Vec<u32>,
    pub(crate) objective_value: u128,
    pub(crate) candidate_count: u32,
    pub(crate) marginal_recomputations: u64,
}
```

For up to eight postings, independently recompute covered categories after
every greedy choice. Include duplicate-heavy input where additive votes waste
a slot, zero-gain completion, shortage, overflow, and every tie dimension.

Run: `cargo test -p borsuk --lib v40_marginal_selector_ -- --nocapture`

Expected: unresolved selector/type boundary only.

- [ ] **Step 2: Implement bounded marginal selection**

Create candidates from at most 64 frontier postings plus 32 alternates each,
deduplicate by posting ordinal, and reject more than 2,112. For 21 rounds,
recompute checked `u128` gain and order by `(gain desc,best_frontier_rank
asc,posting asc)`. Retained `(i,j)` mass is covered by either endpoint;
residual is covered only by `i`. Use the same order for zero-gain fill.

- [ ] **Step 3: Run GREEN and commit**

Run: `cargo test -p borsuk --lib v40_marginal_selector_ -- --nocapture`

```bash
git add crates/borsuk/src/v40_spill_router.rs
git commit -m "feat(v40): select spill postings by marginal coverage"
```

### Task 4: Authorities, evaluation, and transport simulator

**Files:**
- Modify: `crates/borsuk/src/v40_spill_router.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Consumes: exact V38/query/GT identities, tree/relation/postings, optional summary, and sealed selections.
- Produces: `V40LocalRunMode`, local artifact/output/request types, `run_v40_local_request_with_progress`, and canonical result validators.

- [ ] **Step 1: Write authority/capability REDs**

```rust
pub enum V40LocalRunMode {
    Preflight,
    SelectDirect,
    BuildSpillSummary,
    SelectAcceptedSpill,
    EvaluateDirect,
    EvaluateAcceptedSpill,
    SimulateTransport,
}
```

Mutate missing/extra/concrete type, URI, SHA-256, BLAKE3, length, source,
archive, index, projection/backend, K, gates, predecessor, and input/output
overlap. Assert every mode rejects roles outside its exact allowlist.

Run: `cargo test -p borsuk --lib v40_authority_ -- --nocapture`

Expected: unresolved V40 authority/local-run symbols only.

- [ ] **Step 2: Write evaluator/simulator REDs**

Use duplicate primary/alternate GT hits and require one count per feature.
Mutate ranks, K, duplicate postings, owners, samples, aggregate/min/pass, and
every binding. Lock literal transport arithmetic:

```rust
assert_eq!(simulation.logical_gets_per_query, 21);
assert_eq!(simulation.maximum_modeled_bytes_per_query, 11_010_048);
assert_eq!(simulation.maximum_requested_records_per_query, 215_040);
assert_eq!(simulation.maximum_full_f32_bytes, 660_602_880);
assert!(!simulation.page_body_accessed);
```

- [ ] **Step 3: Implement capability-separated local phases**

Open inputs once, read declared length plus one overflow byte, retain the
descriptor, and reuse strict V37/V38 decoders. Emit selections as non-null
Parquet ordered `(query_ordinal,selection_rank)`. Recompute all truth metrics
from sealed selection/relation/GT. Give `SimulateTransport` no storage trait or
network type. Canonicalize JSON, append LF, reparse, independently validate,
and require byte equality before create-only publication.

- [ ] **Step 4: Run grouped GREEN and commit**

Run: `cargo test -p borsuk --lib v40_ -- --nocapture`

Run: `cargo fmt --all -- --check && git diff --check`

```bash
git add crates/borsuk/src/v40_spill_router.rs crates/borsuk/src/lib.rs
git commit -m "feat(v40): add sealed router evaluation phases"
```

### Task 5: Thin local executable

**Files:**
- Create: `crates/borsuk/examples/v40_spill_router.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Consumes: doc-hidden V40 request/runner symbols.
- Produces: strict local-files-only phase CLI.

- [ ] **Step 1: Stage CLI REDs**

Cover all modes and roles, duplicates, missing/unknown/type-invalid values,
digest length, zero length, output overlap, and explicit refusal of bucket,
endpoint, page, network, and D3 flags.

Run: `cargo test -p borsuk --example v40_spill_router v40_cli_ -- --nocapture`

Expected: missing parser/main symbols only.

- [ ] **Step 2: Implement parser and main**

Require one `--phase`, complete input identity tuples, exact phase outputs,
`--workers`, and `--execute-v40`. Send progress to stderr and canonical bytes
only to stdout; every error exits nonzero. Import no storage client.

- [ ] **Step 3: Run GREEN and commit**

Run: `cargo test -p borsuk --example v40_spill_router v40_cli_ -- --nocapture`

```bash
git add crates/borsuk/examples/v40_spill_router.rs crates/borsuk/src/lib.rs
git commit -m "feat(v40): add local router diagnostic CLI"
```

### Task 6: AWS Spot controller

**Files:**
- Create: `scripts/run_v40_spill_router_spot.py`
- Create: `scripts/test_run_v40_spill_router_spot.py`
- Create: `scripts/requirements-v40-spill-router.txt`

**Interfaces:**
- Consumes: V40 manifests, frozen archives/binary/inputs, profile `causality`.
- Produces: create-only progress/result/terminal objects, one phase per Spot instance.

- [ ] **Step 1: Write controller REDs**

Cover exact schemas, Spot-only launch, instance profile, region, role allowlists,
dependency terminals, cgroup memory/PSI/swap/progress stops, interruption
discard/restart, terminal create-only semantics, and mandatory termination.

Run:

```bash
uv run --offline --python 3.12 --with-requirements scripts/requirements-v40-spill-router.txt \
  python -m unittest scripts.test_run_v40_spill_router_spot
```

Expected: missing controller import.

- [ ] **Step 2: Implement controller and run GREEN**

Reuse `publication_v3_aws.py` and V38 lifecycle helpers. One invocation creates
one transient systemd slice, downloads only phase-allowlisted inputs, runs the
binary once, uploads outputs/terminal, verifies terminal identity, and
terminates the instance.

Run the Step 1 command, then:

```bash
uv run --offline --python 3.12 --with ruff==0.15.20 ruff check \
  scripts/run_v40_spill_router_spot.py scripts/test_run_v40_spill_router_spot.py
python3 -m py_compile scripts/run_v40_spill_router_spot.py scripts/test_run_v40_spill_router_spot.py
git diff --check
```

- [ ] **Step 3: Commit**

```bash
git add scripts/run_v40_spill_router_spot.py scripts/test_run_v40_spill_router_spot.py scripts/requirements-v40-spill-router.txt
git commit -m "feat(v40): orchestrate router falsifier on spot"
```

### Task 7: Execute the fail-fast 1M ladder

**Files:**
- Modify after terminal evidence: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: frozen V38 artifacts and registered development query/GT identities.
- Produces: direct result, optional challenger, optional simulator, ledger evidence.

- [ ] **Step 1: Run one source-free Spot preflight**

Require exact binary/archive/backend/workers, worst-case frontier/candidates,
10,000 timed queries, p99 `<=5_000_000 ns`, selection memory `<=256 MiB`, zero
swap growth, and PSI full avg10 `<=0.75`.

- [ ] **Step 2: Run direct selection then truth evaluation**

Selection receives no GT. Only after its authenticated terminal does evaluation
open GT without queries. Stop on `direct-passed`; record recall, work, timing,
memory, PSI, swap, instance, and hashes.

- [ ] **Step 3: Run challenger only after `direct-failed`**

Run exactly `build-spill-summary`, `select-accepted-spill`, then
`evaluate-accepted-spill`. Do not alter any frozen constant after direct output.

- [ ] **Step 4: Run simulation only after a logical pass**

Require exactly 21 logical GETs/query and at most 11,010,048 modeled bytes.
Keep physical S3 and fine vectors fenced.

- [ ] **Step 5: Record and commit evidence**

Record exact dataset/split, all identities, metrics, resource evidence,
disposition, and claim/holdout fences.

Run: `python3 scripts/validate_research_docs.py && git diff --check`

```bash
git add docs/research/publication-v3-attempt-ledger.md
git commit -m "research: record v40 router falsifier"
```

### Task 8: Final assurance and delivery

**Files:**
- Verify: every file changed in Tasks 1-7.

**Interfaces:**
- Consumes: stable V40 implementation/evidence commits.
- Produces: reviewed fast-forward `origin/main`.

- [ ] **Step 1: Run affected gates, then one full assurance on Spot**

Repair only failing layers. Once stable, run serially:

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest discover -s scripts -p 'test_*.py'
python3 scripts/validate_research_docs.py
git diff --check
```

- [ ] **Step 2: Reconcile one final dual Fable/Astra critique**

Give both reviewers the final diff and terminals. Independently verify every
Critical/Important finding. Repair confirmed defects test-first, rerun the
affected gate, then rerun the single final assurance sequence.

- [ ] **Step 3: Push and verify canonical equality**

```bash
git fetch origin main
git merge-base --is-ancestor origin/main HEAD
git push origin HEAD:main
test "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)"
test "$(git rev-parse HEAD)" = "$(git ls-remote origin refs/heads/main | cut -f1)"
git status --short
```

Expected: fast-forward push, equal HEAD/origin/remote identities, clean worktree.

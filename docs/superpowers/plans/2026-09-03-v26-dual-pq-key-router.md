# V26 Dual PQ-Key Router Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and truth-test a distance-aligned two-plane PQ16 router that preserves perfect-recall potential within 3 GiB and 15 ms.

**Architecture:** Keep source-order PQ16 codes and add two deterministic counting indexes keyed by code-byte pairs `(0, 8)` and `(4, 12)`. Query both planes with exact partial-PQ key distances, full-PQ score their deduplicated rows, exactly rerank the best 2,048 Arrow rows, and select ten pages.

**Tech Stack:** Rust, Rayon, Apache Arrow IPC, Parquet evidence, SHA-256, AWS EC2 Spot/S3/SSM.

**Spec:** `docs/superpowers/specs/2026-09-03-v26-dual-pq-key-router-design.md`

## Global Constraints

- Fixed key pairs are `(0, 8)` and `(4, 12)`; no query-derived or caller-tunable choice.
- Fixed arm ladder is 128, 512, and 1,536 keys per plane with a top-2,048 PQ heap.
- Select exactly ten pages and keep the 975k/800k/995k/15ms gates unchanged.
- Projected resident memory at 100M rows must equal 2,938,017,816 bytes.
- Persist only strict, non-nullable Arrow IPC; emit Parquet samples and canonical JSON summaries.
- Authenticate the full query/truth/serving authority before selecting the first 32 queries.
- Read no page bodies and launch only causality Spot compute for scientific evaluation.

---

### Task 1: Deterministic dual-key core

**Files:**
- Modify: `crates/borsuk-v26/src/lib.rs`

**Interfaces:**
- Produces: `V26DualPqKeyIndex`, `build_v26_dual_pq_key_index`, `rank_v26_dual_pq_key_candidates`, and exact memory projection.
- Consumes: existing `V26PackedPq16Index` source-order codes and PQ lookup-table logic.

- [ ] Write tests named `v26_fast_dual_pq_key_*` proving the exact key pairs, counting offsets, stable source order, two-plane deduplication, scalar reference equality, deterministic ties, top-2,048 bound, and 2,938,017,816-byte projection.
- [ ] Run `cargo test -p borsuk-v26 --lib v26_fast_dual_pq_key_ -- --nocapture`; require intended missing-boundary failures and at least one executed test.
- [ ] Implement the minimal types, counting builder, fixed key ranking, deduplicated full-PQ heap, and validation.
- [ ] Rerun the same selector and require every selected test GREEN with no warnings.
- [ ] Commit only the core/test slice.

### Task 2: Strict Arrow persistence and exact rerank

**Files:**
- Modify: `crates/borsuk-v26/src/local.rs`
- Modify: `crates/borsuk-v26/src/lib.rs`

**Interfaces:**
- Produces: strict writers/readers for `pq16-dual-key-offsets.arrow` and `pq16-dual-key-ordinals.arrow`, plus `select_v26_dual_pq_key_pages_from_arrow`.
- Consumes: Task 1 index and existing authenticated cold-vector Arrow reader/page cover.

- [ ] Stage tests for exact Arrow schemas, round-trip equality, corrupt digest/schema/order/cardinality rejection, sparse exact reads, exactly ten pages, and zero page bodies.
- [ ] Run the focused `v26_fast_dual_pq_key_arrow_` selector and preserve RED.
- [ ] Implement strict Arrow codecs and exact top-2,048 cold reranking without loading construction rows.
- [ ] Rerun the selector for GREEN, then run `cargo fmt --all -- --check` and `git diff --check`.
- [ ] Commit only the persistence/selection slice.

### Task 3: Truth-bound three-arm runner

**Files:**
- Modify: `crates/borsuk-v26/src/local.rs`

**Interfaces:**
- Produces: `V26DualPqKeyPreflightRequest`, 96 Parquet samples, canonical result/authority, and `run_v26_dual_pq_key_preflight`.
- Consumes: Task 2 selection, existing full-query/full-truth authentication, and fixed first-32 evaluation convention.

- [ ] Stage tests that independently recompute all arm aggregates/gates, enforce `[128,512,1536]`, bind every artifact/evidence identity, and reject result mutations.
- [ ] Run focused `v26_dual_pq_key_preflight_` tests for RED.
- [ ] Implement the fixed runner and canonical serializer with no tuning or page/storage surface.
- [ ] Rerun focused tests for GREEN and add their narrow selectors to `scripts/check_v26_fast.py`.
- [ ] Run `python3 scripts/check_v26_fast.py`; require all steps GREEN in under 60 seconds.
- [ ] Commit only the runner/fast-gate slice.

### Task 4: Offline CLI and preserved-artifact builder

**Files:**
- Create: `crates/borsuk-v26/examples/v26_dual_pq_key_preflight.rs`
- Modify: `crates/borsuk-v26/src/local.rs`

**Interfaces:**
- Produces: a strict direct executable that accepts explicit local serving/query/truth paths plus registered URI/SHA-256/length identities.
- Consumes: Task 3 runner and preserved V2 codebook/codes/cold-vector artifacts.

- [ ] Stage CLI tests for exact required flags and rejection of duplicate, missing, unknown, AWS, page, D3, and tuning flags.
- [ ] Run `cargo test -p borsuk-v26 --example v26_dual_pq_key_preflight v26_ -- --nocapture` for RED.
- [ ] Implement minimal parsing/main and a local builder that derives only the two counting-index Arrow files from authenticated source-order PQ16 codes.
- [ ] Rerun the example tests, the focused library selectors, fmt, and diff-check.
- [ ] Commit and fast-forward push the verified implementation.

### Task 5: One full-scale Spot falsifier

**Files:**
- Modify after terminal evidence: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: immutable V2 serving bundle, query Parquet, 512-row truth Parquet, and Task 4 release binary.
- Produces: authenticated two-plane Arrow files, 96-row Parquet evidence, canonical JSON result, monitor, and terminal receipt.

- [ ] Build the release example offline and record exact binary SHA-256/length/source commit.
- [ ] Launch one `causality` Spot instance in an available eu-central-1 zone; use SSM rather than cloud-init for the managed run.
- [ ] Enforce 3 GiB projected memory, 24 GiB build RSS, PSI full avg10 at most 1%, zero swap, and 7,200-second wall stops.
- [ ] Preserve the original terminal without restart; terminate the instance immediately.
- [ ] If no arm passes, record rejection and return to design without changing gates. If an arm passes, run the sub-minute gate, strict workspace Clippy, and one locked workspace/all-targets test.
- [ ] Validate the evidence ledger, commit, fast-forward push, and verify a clean worktree.


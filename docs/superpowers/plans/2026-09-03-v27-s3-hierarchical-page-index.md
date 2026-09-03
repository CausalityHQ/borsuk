# V27 S3-Native Hierarchical Page Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a high-recall ANN index that keeps only a compact hierarchy and page metadata resident, fetches at most ten immutable Arrow pages from S3 per query, and exact-reranks at most 10,240 vectors.

**Architecture:** A query-independent two-level IVF hierarchy and co-designed 1,024-row pages replace the unreleased row-proportional PQ4 serving layout. The core library selects authenticated page identities without network access; an explicit page-store boundary performs one concurrent read wave and returns bytes for strict Arrow decoding and exact reranking.

**Tech Stack:** Rust 2024, Apache Arrow IPC, Parquet, f16 centroids, Rayon, SHA-256, Python 3.12 controllers, boto3/AWS CLI, EC2 Spot with profile `causality`.

**Spec:** `docs/superpowers/specs/2026-09-03-v27-s3-hierarchical-page-index-design.md`

## Global Constraints

- No compatibility reader, alias, migration layer, dynamic loader, linker manipulation, or old V23--V26 artifact acceptance.
- Serving has no resident row-proportional code or vector plane and projects below 512 MiB; observed RSS must remain below 768 MiB.
- Each page holds at most 1,024 primary-plus-replica rows; global replication is at most 15%.
- Each query selects at most ten pages, one concurrent object-store wave, at most 4,587,520 encoded bytes, and at most 10,240 exact rows.
- Construction is query-independent; query/truth capabilities do not exist until all construction receipts are immutable.
- Persistent vectors/pages use Arrow IPC, inventories/evidence use Parquet, and authority/terminal receipts use sorted compact JSON plus one LF.
- The Deep sealed gate requires 1,000,000 ppm aggregate and minimum Recall@10. CPU p99 is at most 15 ms; Standard-S3 cold p99 targets 100 ms and may not exceed 150 ms.
- Run focused selectors after repairs, one affected gate at stable boundaries, and Clippy/full workspace assurance only once at a release milestone.

---

### Task 1: Freeze V27 authority and page format

**Files:**
- Create: `crates/borsuk/src/v27_s3_page.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Produces: `V27PageIdentity`, `V27PageRow`, `V27Page`, `encode_v27_page`, and `decode_v27_page`.
- Consumes: exact SHA-256, encoded length, page ordinal, row counts, and the concrete `id:fixed-binary[8], vector:fixed-list<element:f32;96>` schema.

- [ ] **Step 1: Write page authority REDs.** Add unit tests that encode literal rows, authenticate the exact digest/length/schema, round-trip IDs/vectors, and reject missing/extra/null/wrong-width/nonfinite/zero-vector/reordered/duplicate-ID mutations. The test must assert that 1,025 rows are rejected and that no S3/path type appears in the page codec API.
- [ ] **Step 2: Run the page RED.** Run `cargo test -p borsuk --lib v27_s3_page_ -- --nocapture`; require unresolved V27 types/functions and at least one selected test.
- [ ] **Step 3: Implement the strict Arrow codec.** Define:

```rust
pub struct V27PageIdentity {
    pub ordinal: u32,
    pub sha256: String,
    pub encoded_bytes: u64,
    pub primary_rows: u16,
    pub replica_rows: u16,
}

pub struct V27PageRow { pub source_ordinal: u64, pub vector: [f32; 96] }
pub struct V27Page { pub identity: V27PageIdentity, pub rows: Vec<V27PageRow> }
pub fn encode_v27_page(identity: &V27PageIdentity, rows: &[V27PageRow]) -> Result<Vec<u8>>;
pub fn decode_v27_page(identity: &V27PageIdentity, bytes: &[u8]) -> Result<V27Page>;
```

Use Arrow IPC file encoding, reject all schema/type/nullability drift, authenticate bytes before decoding, and allocate only the one page.
- [ ] **Step 4: Run page GREEN and commit.** Run the exact selector, `cargo fmt --all -- --check`, and `git diff --check`; commit only Task 1 files.

### Task 2: Build and authenticate the compact hierarchy

**Files:**
- Create: `crates/borsuk/src/v27_s3_router.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Produces: `V27Hierarchy`, `V27HierarchyConfig`, `fit_v27_hierarchy`, `encode_v27_hierarchy`, and `decode_v27_hierarchy`.
- Consumes: a deterministic query-independent hash sample iterator; 1,024 roots, 65,536 leaves, f16 storage, and f32 accumulation.

- [ ] **Step 1: Write hierarchy REDs.** On a literal reduced fixture, require deterministic roots/leaves across worker counts, exact 64-child ownership, deterministic empty-cluster repair, `(distance,ordinal)` ties, strict f16 artifact authority, and rejection of query/truth capabilities.
- [ ] **Step 2: Run hierarchy RED.** Run `cargo test -p borsuk --lib v27_s3_router_ -- --nocapture`; require missing hierarchy symbols only.
- [ ] **Step 3: Implement bounded training and encoding.** Define:

```rust
pub struct V27HierarchyConfig {
    pub roots: usize,
    pub leaves: usize,
    pub iterations: usize,
    pub seed: u64,
}
pub fn fit_v27_hierarchy<I>(sample: I, config: &V27HierarchyConfig) -> Result<V27Hierarchy>
where I: IntoIterator<Item = [f32; 96]>;
```

Require the production constants exactly, use bounded accumulators rather than corpus storage, and persist roots/leaves as strict Arrow IPC.
- [ ] **Step 4: Run hierarchy GREEN and commit.** Run the focused selector, fmt, and diff-check; commit only Task 2 files.

### Task 3: Stream, replicate, and pack pages

**Files:**
- Create: `crates/borsuk/src/v27_s3_build.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Produces: `V27BuildConfig`, `V27PagePosting`, `V27BuildReceipt`, and `V27PageBuilder::build`.
- Consumes: a `V27Hierarchy`, a bounded corpus iterator, and an explicit scratch/output sink.

- [ ] **Step 1: Write layout REDs.** Require exactly one primary assignment per source ordinal, at most one replica, no more than 15% replicas, at most 1,024 rows/page, stable external-sort order, role-disjoint page identities, and complete corpus union. Mutate each arithmetic/count/digest/ordering branch independently.
- [ ] **Step 2: Run layout RED.** Run `cargo test -p borsuk --lib v27_s3_build_ -- --nocapture`; require missing builder symbols only.
- [ ] **Step 3: Implement the two-pass builder.** Define:

```rust
pub struct V27BuildConfig {
    pub page_rows: usize,
    pub replica_margin_ppm: u32,
    pub replica_ceiling_ppm: u32,
    pub sort_memory_bytes: u64,
}
pub struct V27PageBuilder;
impl V27PageBuilder {
    pub fn build<I, S>(rows: I, hierarchy: &V27Hierarchy, config: &V27BuildConfig, sink: &mut S)
        -> Result<V27BuildReceipt>
    where I: IntoIterator<Item = V27PageRow>, S: V27PageSink;
}
```

Spill bounded sorted runs, merge by `(leaf,projection_key,source_ordinal)`, encode pages, compute up to four deterministic modes/page, and write exact postings/receipts. Do not retain the corpus.
- [ ] **Step 4: Run layout GREEN and commit.** Run the focused selector, fmt, and diff-check; commit only Task 3 files.

### Task 4: Select a bounded page frontier

**Files:**
- Create: `crates/borsuk/src/v27_s3_search.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Produces: `V27Router`, `V27SearchArm`, `V27PageSelection`, and `V27Router::select_pages`.
- Consumes: hierarchy, postings, page modes, and one `[f32;96]` query; it has no page-store capability.

- [ ] **Step 1: Write selection REDs.** Table-drive root beams 8/16/32, leaf beams 64/128/256, exact ten-page cap, finite deterministic scoring, duplicate posting removal, bounded accumulators, and page ordering by `(distance,page_ordinal)`. Assert truthful root/leaf/posting/page work counters.
- [ ] **Step 2: Run selection RED.** Run `cargo test -p borsuk --lib v27_s3_search_ -- --nocapture`; require missing router symbols only.
- [ ] **Step 3: Implement selection.** Define:

```rust
pub struct V27SearchArm { pub root_beam: usize, pub leaf_beam: usize, pub page_count: usize }
pub struct V27PageSelection { pub pages: Vec<V27PageIdentity>, pub work: V27RoutingWork }
impl V27Router {
    pub fn select_pages(&self, query: &[f32; 96], arm: V27SearchArm) -> Result<V27PageSelection>;
}
```

Use fixed-size top-k heaps and sparse page accumulation; prohibit full 65,536-pair and page-count-sized per-query allocations.
- [ ] **Step 4: Run selection GREEN and commit.** Run the focused selector, fmt, and diff-check; commit only Task 4 files.

### Task 5: Fetch one wave and exact-rerank

**Files:**
- Modify: `crates/borsuk/src/v27_s3_search.rs`
- Create: `crates/borsuk/examples/v27_s3_qualify.rs`

**Interfaces:**
- Produces: `V27PageStore`, `V27SearchIndex::search`, and an explicit local/S3 qualification executable.
- Consumes: one `V27PageSelection`; the core receives returned bytes and never owns credentials, bucket defaults, endpoints, or caches.

- [ ] **Step 1: Write fetch/rerank REDs.** Use a real in-memory store implementation and require one `read_wave` call, at most ten unique identities, at most 4,587,520 returned bytes, strict authentication before decode, all-or-nothing failure, exact ranking, and truthful GET/byte/row counters. A reduced test must fail if the reported rows differ from the decoded rows.
- [ ] **Step 2: Run fetch/rerank RED.** Run `cargo test -p borsuk --lib v27_s3_fetch_ -- --nocapture`; require missing store/search symbols only.
- [ ] **Step 3: Implement the explicit boundary.** Define:

```rust
pub trait V27PageStore: Send + Sync {
    fn read_wave(&self, pages: &[V27PageIdentity]) -> Result<Vec<Vec<u8>>>;
}
impl<S: V27PageStore> V27SearchIndex<S> {
    pub fn search(&self, query: &[f32; 96], k: usize) -> Result<V27SearchResult>;
}
```

The library makes exactly one store call and exact-reranks decoded rows. The example accepts only explicit authority and local-page-directory or explicit S3 bucket/prefix mode; reject mixed modes and all legacy flags.
- [ ] **Step 4: Run fetch/rerank GREEN and commit.** Run the library selector and example selector, fmt, and diff-check; commit only Task 5 files.

### Task 6: Add the seconds-long reduced campaign gate

**Files:**
- Create: `scripts/run_v27_s3_page_campaign.py`
- Create: `scripts/test_run_v27_s3_page_campaign.py`
- Modify: `scripts/check_v26_fast.py`
- Modify: `scripts/test_check_v26_fast.py`

**Interfaces:**
- Produces: reduced build/search orchestration, an injected-latency page store, exact request/byte projection, and 30-second Spot monitoring.
- Consumes: frozen binaries and explicit Parquet/Arrow paths; no shell-generated loader or runtime-linker behavior.

- [ ] **Step 1: Write controller REDs.** Require a real separate-process 100K corpus build, ten-page read wave, deterministic 40 ms/request and 350 MiB/s transfer simulation, exact latency decomposition, cleanup/PID clearance, authority-first phase receipts, cross-zone Spot selection, and no duplicate launch during silence.
- [ ] **Step 2: Run controller RED.** Run `python3 -m unittest scripts.test_run_v27_s3_page_campaign`; require missing controller interfaces only.
- [ ] **Step 3: Implement the reduced controller.** Use `subprocess` argument arrays, explicit temporary paths, injected monotonic clock/sleep in tests, and immutable phase receipts. Abort before launch when request, byte, quality, or projected latency gates fail.
- [ ] **Step 4: Run reduced GREEN and commit.** Run the complete controller file, the exact V27 Rust selectors, Ruff, py_compile, fmt, and diff-check; commit only Task 6 paths.

### Task 7: Deep quality and real S3 fail-fast gates

**Files:**
- Modify: `scripts/run_v27_s3_page_campaign.py`
- Modify: `scripts/test_run_v27_s3_page_campaign.py`
- Modify after terminal evidence: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: immutable Deep training/query/truth artifacts and one committed V27 build.
- Produces: no-page-body containment evidence, bounded real-page S3 latency evidence, and one selected development arm.

- [ ] **Step 1: Run no-page-body quality screen.** Build from training only, freeze the layout, evaluate all development arms, and require one arm to recover all ten truth neighbors for every development query. Stop without S3 serving if none passes.
- [ ] **Step 2: Run bounded S3 latency screen.** Upload only reduced immutable pages, issue the selected arm's one-wave reads with an empty cache, and require exact request/byte bounds plus cold p99 at most 150 ms. Compare Standard S3 and S3 Express only as separately labeled stores.
- [ ] **Step 3: Freeze the one sealed arm.** Select the smallest lexicographic passing development arm, commit its authority, and prohibit further tuning from sealed outputs.
- [ ] **Step 4: Validate evidence and commit.** Run `python3 scripts/validate_research_docs.py` and `git diff --check`; commit only the ledger update and frozen authority.

### Task 8: Sealed Deep, synthetic 100M, and release assurance

**Files:**
- Modify after evidence: `docs/research/publication-v3-attempt-ledger.md`
- Create after evidence: `docs/research/v27-s3-page-production-authority.json`
- Modify only from passing evidence: `README.md`
- Modify only from passing evidence: `docs/production-readiness.md`

**Interfaces:**
- Consumes: one frozen source, binary inventory, selected arm, Deep authority, and synthetic 100M authority.
- Produces: terminal Deep/100M results and accurately scoped release claims.

- [ ] **Step 1: Run milestone assurance once.** Run the V27 affected gate, strict locked workspace/all-targets Clippy, and one locked workspace/all-targets test on a pressure-qualified host.
- [ ] **Step 2: Run sealed Deep on `causality` Spot.** Poll every 30 seconds, preserve the original attempt, require perfect sealed Recall@10 and all S3/resource gates, upload terminal evidence, and terminate immediately.
- [ ] **Step 3: Run synthetic 100M construction.** Stream ten disjoint 10M ranges on cross-zone Spot workers, publish immutable pages/receipts, replace only explicit Spot interruptions, and verify exactly 100M primary ordinals plus at most 15% replicas.
- [ ] **Step 4: Run one sealed 100M serving attempt.** Use the frozen arm and store, empty-cache cold queries, exact work counters, and the same latency/memory/S3 gates. Do not restart after a scientific failure.
- [ ] **Step 5: Publish the disposition.** Validate docs, commit/push evidence, verify `HEAD==origin/main==ls-remote`, a clean worktree, and zero active campaign instances. Make no competitor claim without a paired equivalent run.

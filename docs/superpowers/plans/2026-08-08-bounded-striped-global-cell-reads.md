# Bounded Striped Global-Cell Reads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace multi-megabyte global-cell S3 GETs with a bounded parallel stripe wave while preserving exact candidates, recall, standard Arrow bundles, and a true query-wide prefetch budget.

**Architecture:** The storage layer reads one logical byte range as ordered adjacent stripes with deterministic disk-cache reuse. The global-PQ planner assigns full-envelope reads before I/O under 8 MiB and 16-stripe stage budgets; planned envelope reads remain authoritative when code bytes are memory-cached, while code-only groups retain the existing shortcut.

**Tech Stack:** Rust, `object_store`, Tokio, Rayon I/O pool, Arrow IPC, BLAKE3, repository storage telemetry.

## Global Constraints

- Do not change the global-PQ descriptor or standard Arrow/Parquet durable formats.
- Preserve routing, the exact candidate set, candidate count, and recall behavior.
- A full envelope is at most 4 MiB and each remote stripe is at most 1 MiB.
- Each base or delta query stage may retain at most 8 MiB and schedule at most 16 envelope stripes.
- Code-only ranges do not consume the exact-envelope reuse budget.
- Use TDD and observe every new regression fail for the intended reason before implementation.
- Use `RUSTC_WRAPPER=/usr/local/libexec/devbox-rustc-wrapper` and `SCCACHE_DIR=/data/cache/sccache` for Rust gates.
- Never inspect incomplete frozen-campaign CSVs; run fail-closed validators before terminal inspection.
- Commit coherent verified slices and fast-forward push directly to `origin/main`; no PR and no force push.

---

### Task 1: Add a cacheable striped range read

**Files:**
- Modify: `crates/borsuk/src/storage.rs`
- Test: `crates/borsuk/src/storage.rs` module `tests`

**Interfaces:**
- Consumes: existing `Storage::read_ranges_with_policy` and `ReadBytes`.
- Produces: `Storage::read_striped_range(&self, relative: &str, range: Range<u64>, stripe_bytes: u64, max_parallel: usize) -> Result<ReadBytes>`.
- Produces: `split_contiguous_range(range: Range<u64>, max_bytes: u64) -> Result<Vec<Range<u64>>>`.

- [ ] **Step 1: Write the failing striped-read regression**

Create a local object containing exactly `3 * 1024 * 1024` deterministic bytes. Call:

```rust
let first = storage
    .read_striped_range("global-pq/bundles/test.arrow", 0..bytes.len() as u64, 1024 * 1024, 16)
    .unwrap();
assert_eq!(first.bytes, bytes);
assert_eq!(storage.request_counts().delta(&before).gets, 3);
```

Repeat the same call and require zero additional GETs. Add a `512 KiB` object case that requires one GET, plus empty, reversed, and zero-stripe-width error cases.

- [ ] **Step 2: Run RED**

Run:

```bash
RUSTC_WRAPPER=/usr/local/libexec/devbox-rustc-wrapper \
SCCACHE_DIR=/data/cache/sccache \
cargo test --locked -p borsuk --lib storage::tests::striped_range_reads_large_ranges_in_parallel_parts -- --exact
```

Expected: compile failure because `read_striped_range` does not exist.

- [ ] **Step 3: Implement the minimal storage API**

Split the logical range into adjacent ordered ranges of at most `stripe_bytes`.
Extend `read_ranges_with_policy` with a `max_physical_range_bytes: u64`
parameter; existing callers pass `SIDECAR_MAX_PHYSICAL_RANGE_BYTES`, while
`read_striped_range` passes `stripe_bytes`, zero gap, and `max_parallel`.
Concatenate returned chunks into one `ReadBytes`; propagate `cache_hit` and set
`cache_repaired` to false. Reject empty/reversed ranges, zero stripe bytes, and
zero parallelism.

- [ ] **Step 4: Run GREEN and storage coverage**

Run the new regression, `disk_cache_reuses_range_and_suffix_reads_without_store_requests`, and the complete storage unit subset. Expected: exact bytes, 3/1/0 GET behavior, and all storage tests pass.

- [ ] **Step 5: Commit the independently verified storage slice**

```bash
git add crates/borsuk/src/storage.rs
git commit -m "storage: stripe bounded range reads"
```

### Task 2: Plan envelope reads under cumulative byte and stripe budgets

**Files:**
- Modify: `crates/borsuk/src/index.rs`
- Test: `crates/borsuk/src/index.rs` module `tests`

**Interfaces:**
- Consumes: `GlobalPqChunkRef` code and exact offsets.
- Produces: `GlobalPqCodeReadPlan { range: Range<usize>, prefetch_exact: bool, stripes: usize }`.
- Produces: `global_pq_code_read_plans(groups: &[(String, Vec<GlobalPqChunkRef>)], remaining_bytes: usize, remaining_stripes: usize) -> Result<Vec<GlobalPqCodeReadPlan>>`.

- [ ] **Step 1: Write failing cumulative-budget tests**

Create three synthetic groups whose full envelopes are 3 MiB and code ranges
are 64 KiB. With an 8 MiB/16-stripe budget, require the first two plans to be
full-envelope and the third code-only. Create seventeen 64 KiB full envelopes
and require exactly sixteen full-envelope plans under the stripe cap. Assert
that code-only plan bytes do not reduce the byte budget available to a later
eligible group.

- [ ] **Step 2: Run RED**

Run the exact new test filter. Expected: compile failure because
`global_pq_code_read_plans` and `GlobalPqCodeReadPlan` are absent.

- [ ] **Step 3: Implement deterministic planning**

Add constants:

```rust
const DEFAULT_GLOBAL_PQ_PREFETCH_STRIPE_BYTES: usize = 1024 * 1024;
const DEFAULT_GLOBAL_PQ_PREFETCH_STRIPES: usize = 16;
```

Compute each code-only range and complete code-to-exact range with checked
arithmetic. Select the complete range only when it is at most 4 MiB and both
remaining budgets cover it. Debit budgets only for selected complete ranges;
set code-only `stripes` to zero.

- [ ] **Step 4: Run GREEN and existing planner tests**

Run the new tests plus
`global_pq_code_read_range_prefetches_only_bounded_arrow_cells`, code-group
coalescing, wave-bound, and query-local-range tests. Expected: all pass.

- [ ] **Step 5: Commit the planner slice**

```bash
git add crates/borsuk/src/index.rs
git commit -m "search: bound global cell prefetch plans"
```

### Task 3: Make planned envelope reads authoritative

**Files:**
- Modify: `crates/borsuk/src/index.rs`
- Test: `crates/borsuk/src/index.rs` module `tests`

**Interfaces:**
- Consumes: `Storage::read_striped_range` and `global_pq_code_read_plans`.
- Produces: one global-code load path whose full-envelope plans perform I/O regardless of code-cache residency.

- [ ] **Step 1: Write the failing cache-state policy regression**

Add a production helper `global_pq_code_group_requires_io(plan: &GlobalPqCodeReadPlan, all_codes_cached: bool) -> bool`. Require:

```rust
assert!(global_pq_code_group_requires_io(&full_envelope, true));
assert!(!global_pq_code_group_requires_io(&code_only, true));
assert!(global_pq_code_group_requires_io(&code_only, false));
```

Run it and observe RED because the helper is absent.

- [ ] **Step 2: Refactor the query page into planned groups**

Build code groups from every selected scan chunk before applying the code-cache
shortcut. Create plans using the remaining query-local byte and stripe budgets.
For a full-envelope plan, call `read_striped_range` with the 1 MiB/16 values
even when every code slice is memory-cached. For code-only plans, skip I/O only
when every code slice is cached. Verify every returned code checksum exactly as
before and retain only full-envelope bytes in `QueryLocalRange`.

- [ ] **Step 3: Preserve bounded accounting**

Increment `query_local_range_bytes` and a new
`query_local_range_stripes` only from full-envelope plans. Pass remaining
budgets into each later wave. Remove the post-I/O best-effort retention check;
the pre-I/O plan is authoritative. Continue adding all fetched bytes to
`SearchReport::bytes_read`.

- [ ] **Step 4: Run GREEN and affected suites**

Run the cache-state helper regression, all global-PQ unit tests, all storage
tests, and `cargo test --locked -p borsuk --test group_commit`. Expected: the
new policy and all existing exact-candidate/recall invariants pass.

- [ ] **Step 5: Commit the integrated read path**

```bash
git add crates/borsuk/src/index.rs
git commit -m "search: stripe planned global cell reads"
```

### Task 4: Reproduce the cache bug and verify local structure

**Files:**
- Modify: `docs/research/group-commit-scalability-attempt-ledger.md`
- Modify: `docs/superpowers/plans/2026-08-08-bounded-striped-global-cell-reads.md`

**Interfaces:**
- Consumes: the preserved 128K hierarchical local index and production benchmark harness.
- Produces: terminal local evidence that the former 100-query disk-cache failure no longer performs backing I/O.

- [ ] **Step 1: Build the exact release revision**

Build `production_bench` with the shared sccache wrapper and record the Git
archive SHA-256.

- [ ] **Step 2: Rerun the exact reproducer**

Use the preserved index at
`/data/home/rb/borsuk-local-qual/ed18e25/hier16-128k-r01/index`, a fresh cache
and output directory, 100 Cohere queries, `hierarchical-16`, `nprobe=32`,
`candidates=128`, and cache profile `all`. Expected: process exit zero and the
disk-cached row reports zero network GETs. This is a cache/read-path regression,
not promotable recall or latency evidence.

- [ ] **Step 3: Run the standard bulk structural smoke**

Run the repository group-commit bulk smoke from the exact committed revision
and validate its terminal artifacts with
`scripts/validate_group_commit_scalability.py`. Expected: root and cell
completion markers and validator exit zero.

- [ ] **Step 4: Record honest local evidence**

Record the rejected 1,024-cell recall curve, the reproduced pre-fix four-GET
failure, and the post-fix zero-GET result. Do not promote local-filesystem
latency or the self-query recall gate into a production claim.

### Task 5: Full assurance, delivery, and immutable AWS qualification

**Files:**
- Modify: `docs/research/group-commit-scalability-attempt-ledger.md`
- Modify: `docs/superpowers/plans/2026-08-08-bounded-striped-global-cell-reads.md`

**Interfaces:**
- Consumes: exact integrated revision and frozen realistic campaign manifest.
- Produces: verified fast-forward `origin/main` commit and one fresh immutable AWS prefix.

- [ ] **Step 1: Run repository assurance once**

Run formatting, `git diff --check`, repository policy, strict all-target/all-feature
Clippy, full locked Rust workspace tests, and the pinned Python suite with the
shared sccache wrapper. Expected: all exit zero with no warnings.

- [ ] **Step 2: Commit and fast-forward push**

Commit the evidence/docs slice. Fetch `origin/main`, prove it is an ancestor of
`HEAD`, push `HEAD:main`, verify `HEAD == origin/main`, and require a clean
worktree.

- [ ] **Step 3: Prove AWS worker exclusivity and launch**

Using profile `causality`, require healthy EC2 status, idle load, no benchmark
process, no competing tmux pane, and fresh disjoint S3 result/index prefixes.
Launch one immutable frozen campaign and preserve its source archive,
manifest, resource telemetry, storage trace, and terminal markers.

- [ ] **Step 4: Monitor without reading incomplete CSVs**

Observe the original benchmark session through retained 15-minute sleeps.
After each sleep, check root/cell phase markers, EC2 health, and the exact
process only. Do not open a measurement CSV until the campaign is terminal.

- [ ] **Step 5: Validate terminal evidence and continue**

At terminality, run the root and terminal-cell fail-closed validators before
opening any CSV. If a cell fails, inspect its terminal raw measurements and
trace, record the causal failure, and return to TDD. If the entire five-repeat
2K/16K by 1/8/32 matrix passes, proceed to the separate 100M and feature-parity
qualification plans; do not call the library production-ready from this matrix
alone.

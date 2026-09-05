# V32 Global Geometric Ownership Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Falsify or qualify one query-blind global480-row ownership partition on the frozen1M development replay before any corpus rebuild.

**Architecture:** Compact scalar balanced geometry owns every reconstructed logical row once. Authenticate controls before treatment, preserve candidate replay, and apply unchanged first-distinct8. This diagnostic does not qualify the eventual3GiB production router or cold transport.

**Tech Stack:** Rust, Arrow/Parquet, Python unittest, canonical JSON, Causality EC2 Spot.

**Spec:** `docs/superpowers/specs/2026-09-05-v32-global-geometric-ownership-design.md`

## Global Constraints

- Exactly1M scientific rows;480 rows/page;2084 global pages;32 queries64..95.
- Private core accepts1..1M rows and1..480 capacity for tests.
-512MiB geometry budget,2GiB measured total RSS,7200s science cap.
- No query/truth/leaf identifiers in partition input; no network/page client.
- Full frozen controls must match before constructing the treatment map.
- Require320/320, min10/10, exactly8 selected pages and zero page reads.
- Failed partition is not universal rejection of unique-owner layouts.
- No retries after scientific terminal; no100M/D3/competitive claims.

### Task 1: Compact global balanced splitter

**Files:** Create `crates/borsuk/src/v32_global_pages.rs`; modify module declaration in `crates/borsuk/src/lib.rs`. Tests are in the new module.

**Interfaces:**

```rust
pub(crate) struct GlobalPageOwners {
    pub(crate) owners: Vec<u32>,
    pub(crate) row_counts: Vec<u16>,
}
pub(crate) fn global_balanced_pages(
    vectors: &[[f32; 96]], sources: &[u64], capacity: usize,
) -> crate::Result<GlobalPageOwners>;
```

- [ ] Stage `v32_global_pages_balances_and_reverses`, `v32_global_pages_rejects_invalid_authority`, and `v32_global_pages_matches_existing_scalar_splitter`. First test uses33 axis-varying positive vectors with unique nonconsecutive sources, capacity4: nine pages, each3/4 rows, complete ownership; reverse input and compare `(source,owner)` pairs. Invalid table covers empty, cardinality, duplicate, cap0/481, NaN, infinity, zero and overflow norm. Differential test wraps the same rows in `V30LayoutRecord` with one leaf/base24 and compares per-source page membership against `partition_v30_leaf_pages`.
- [ ] Run `rtk proxy cargo test -p borsuk --lib v32_global_pages_ -- --nocapture`; preserve missing-symbol RED.
- [ ] Implement compact arrays and exact spec recursion. Use checked shape before allocating, source-sorted u32 permutation for duplicate checks, inverse norms/margins indexed by logical. Compute margins before sort; use `sort_unstable_by` only on indices. At each split `left_size=(p/2)*(n/p)+(n%p).min(p/2)`. Normalize centroids accumulated in current order, four refinements then fifth sort. Assign owner at terminal nodes; return exact counts.
- [ ] Repeat same selector for GREEN. Run fmt/diff only after passing; commit the verified isolated core.

### Task 2: Reconstruction and immutable replay

**Files:** `crates/borsuk/src/v30_s3_search.rs`, `crates/borsuk/examples/v30_s3_qualify.rs`.

**Interfaces:** Router method `global_geometric_page_layout(&self, logical_sources: &[u64]) -> Result<V32VirtualPageLayout>` reconstructs exactly1M once and calls Task1. Internal candidate replay is extracted from existing routing-details path, not regenerated separately for each reducer. Dedicated diagnostic mode uses algorithm identity `v32-global-balanced-cosine-v1` and preserves current control mode.

- [ ] Add focused `v32_global_layout_` tests with mixed code parents/microleaves and exact source binding. Lock output coverage, leaf crossing and absence of old leaf rejection. Add replay mutation tests: different layout cannot change candidate hash/current controls; altered truth changes only truth joins. Exact8 derives from first16 prefix.
- [ ] Run the narrow Rust filter and preserve intended RED.
- [ ] Reuse validated24/48 reconstructor, add parent centroid, normalize, collect contiguous vectors; never manufacture duplicate base codes or a source BTreeMap. Construct `V32VirtualPageLayout` from validated compact output. Refactor current details into immutable replay consumed by existing current and virtual reducers, preserving operation order.
- [ ] Run narrow filters, then existing `v32_virtual_` regressions once. Commit only when those contracts pass.

### Task 3: Authority-first two-phase controller

**Files:** `scripts/run_v32_no_page_containment.py`, its test file, `scripts/run_v30_s3_campaign.py`, its test file, qualifier CLI tests.

**Interfaces:** Add an explicit global-geometry diagnostic mode, not an alias for microleaf-exclusive mode. Control invocation authenticates current bytes before treatment invocation. Treatment receipt contains control records/replay hashes again; controller independently checks exact equality before publishing treatment.

- [ ] Add unittest cases where forged governing control prevents the treatment runner from being called; unchanged control permits exactly one treatment; output-control drift suppresses treatment; microleaf_count10 is evidence not rejection for the global mode. Fake subprocess calls record order and exact arguments. Retain exact registered URI/SHA/length checks.
- [ ] Run only these new unittest nodes for RED.
- [ ] Implement control-first sequencing and one global map per treatment batch. Keep queries in the strict Arrow request. Record algorithm, source, map/replay hashes, exact input identities and zero-page-read counter. Gate per-query page count/row-derived encoded bound/containment. No corpus/page flags.
- [ ] Run affected complete Python files and qualifier CLI test filter; then scoped Ruff/py_compile/fmt/diff. Commit verified controller slice.

### Task 4: Single bounded scientific result

**Files:** Evidence ledger and immutable S3 receipt only after verified source commit.

- [ ] Run grouped affected Rust and Python gates, then strict Clippy once after stable diff; repair only failing layer. Verify fast-forward source and clean checkout. Build native qualifier once on Causality Spot, preserve exact binary/archive hashes and terminate build instance.
- [ ] Preregister one execution: frozen authorities from predecessor controller, global mode,2084 pages, exact gates,2GiB RSS and7200s cap, heartbeat/progress/pressure handling, named scratch cleanup, terminal sync and instance termination. No page GETs. Confirm no active duplicate before launching.
- [ ] Launch one Spot process, retain original session/instance, poll terminal/health only. On interruption discard measurement and record identity; no interpreting partial science. On scientific failure preserve full terminal, terminate compute and consult Astra with exact failing stage/metrics.
- [ ] Verify authenticated result and append ledger. Pass authorizes only preregistered untouched-cohort test and separate actual-router/transport qualification; failure authorizes analysis, not tuning or larger build.

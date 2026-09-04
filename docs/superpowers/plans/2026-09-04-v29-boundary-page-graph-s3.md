# V29 Boundary Page-Graph S3 Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans
> to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Recover high-dimensional boundary neighbors while retaining one S3
wave, ten pages, sub-1% code scan, and less than 3 GiB resident memory.

**Architecture:** Collapse query-independent alternate-leaf row assignments
into a degree-16 page graph. Use bounded PQ page evidence to select eight seeds
and two graph frontier pages, then exact-rerank one authenticated Arrow wave.

**Spec:** `docs/superpowers/specs/2026-09-04-v29-boundary-page-graph-s3-design.md`

### Task 1: Freeze graph authority and codec

**Files:** Create `crates/borsuk/src/v29_s3_graph.rs`; modify
`crates/borsuk/src/lib.rs`.

- [ ] Write REDs for deterministic undirected votes, degree 16, canonical tie
  breaks, invalid leaves/pages, overflow, asymmetric mutation, exact Parquet
  schema, digest drift, and source/layout/code/roster bindings.
- [ ] Run `cargo test -p borsuk --lib v29_s3_graph_ -- --nocapture`; require
  unresolved V29 symbols only.
- [ ] Implement bounded external vote reduction and strict Parquet codec.
- [ ] Run the same selector, fmt, and diff-check; commit the coherent slice.

### Task 2: Add bounded graph page selection

**Files:** Modify `crates/borsuk/src/v28_s3_search.rs`; modify
`crates/borsuk/src/v29_s3_graph.rs`.

- [ ] Write REDs for 128 unique evidence pages, eight seeds, exactly 2,048
  edge visits maximum, fixed integer reciprocal-rank voting, two distinct
  frontier pages, deterministic ties, and exact ten-page output.
- [ ] Lock the synthetic causal fixture: seed-only misses one boundary page;
  graph selection includes it without truth or page-body access.
- [ ] Implement a shared bounded V28 page-evidence reducer and the V29 wrapper.
- [ ] Run `cargo test -p borsuk --lib v29_s3_select_ -- --nocapture`, fmt, and
  diff-check; commit.

### Task 3: Preserve one authenticated S3 wave

**Files:** Modify `crates/borsuk/src/v29_s3_graph.rs`; create
`crates/borsuk/examples/v29_s3_qualify.rs`.

- [ ] Write REDs requiring one store call, ten unique objects, complete-byte
  authentication, 4,587,520-byte cap, exact f32 rerank, and truthful counters.
- [ ] Implement `V29Index::search` over the existing `V28PageStore`; expose no
  second-wave or local-vector-snapshot surface.
- [ ] Add a thin explicit local/S3 qualifier using Arrow/Parquet artifacts and
  canonical JSON/Parquet evidence.
- [ ] Run focused library/example selectors, fmt, and diff-check; commit.

### Task 4: Make 100K the release-contract gate

**Files:** Create `scripts/run_v29_reduced_quality.py` and its unittest; update
the fast-gate script.

- [ ] Write controller REDs for query-blind graph build, seed-only control,
  exact 320-neighbor recomputation, one wave, injected S3 latency, process
  cleanup, and canonical evidence.
- [ ] Implement the immutable 100K runner and stop immediately unless the graph
  arm reaches 320/320 within all work and memory bounds.
- [ ] Run the complete controller file, focused Rust selectors, Ruff,
  py_compile, fmt, and diff-check; commit.

### Task 5: Qualify latency and sealed scale

- [ ] Run one same-region `causality` Spot S3 screen for the passing 100K arm;
  record GETs, bytes, wave p99, throughput, CPU, RSS, PSI, swap, and spend.
- [ ] If and only if it passes, build once and evaluate one sealed 9.99M cohort.
- [ ] If and only if sealed quality/resources pass, build and validate 100M.
- [ ] Run strict locked Clippy and the full locked workspace suite once at the
  stable release milestone; validate and commit the evidence ledger.


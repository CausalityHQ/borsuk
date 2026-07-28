# TurboQuant, Mixed Tier, and Locality Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce 100M object-store latency, add full TurboQuant controls, and execute cached global cells with graphs while scanning uncovered cells.

**Architecture:** Fresh builds make hierarchical cell IDs and bundles parent-contiguous, add distinct MSE-only and full TurboQuant codecs, and introduce a per-cell execution planner that merges local graph and remote scan candidates before one exact rerank. All resources remain protected by process-wide byte, decode, I/O, and search gates. No migration or backward-compatibility work is performed.

**Tech Stack:** Rust 2024, `object_store`, Rayon, deterministic FWHT/SplitMix projections, Parquet/Arrow sidecars, Python benchmark orchestration, AWS S3/EC2.

---

### Task 1: Parent-contiguous cell identity and bundles

**Files:**
- Modify: `crates/borsuk/src/global_pq_sidecar.rs`
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/src/manifest.rs`
- Test: inline unit tests in the modified Rust modules

- [ ] Add a failing test proving sorted child cells from one parent are contiguous.
- [ ] Run the focused test and confirm the current low-byte parent encoding fails.
- [ ] Change cell encoding to `(parent << 8) | child` and centralize encode/decode helpers.
- [ ] Add a failing test proving bundles do not cross parent boundaries.
- [ ] Implement parent-boundary bundle flushing; all experiments recreate indexes from raw data.
- [ ] Run the focused and descriptor-corruption tests.

### Task 2: Costed code-range planner and diagnostics

**Files:**
- Create: `crates/borsuk/src/global_read_planner.rs`
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/src/record.rs`
- Test: `crates/borsuk/src/global_read_planner.rs`

- [ ] Add failing table tests for sparse-slice versus contiguous-parent-span decisions.
- [ ] Implement a deterministic planner returning physical spans, requested bytes, gap bytes, and predicted GETs.
- [ ] Route global-code reads through the planner without changing ranking.
- [ ] Extend `SearchReport` and every binding with predicted/actual code and exact-read counters.
- [ ] Verify that byte/read caps remain global under concurrent searches.

### Task 3: Breaking TurboQuant codec identities

**Files:**
- Modify: `crates/borsuk/src/record.rs`
- Modify: `crates/borsuk/src/manifest.rs`
- Modify: Rust/CLI/Python/Node public bindings and tests

- [x] Add failing parser/serde/default tests for the two structured Fast-TurboQuant controls.
- [x] Remove ambiguous aliases and the nonfunctional reference option.
- [ ] Remove compatibility aliases; experiment scripts always delete/recreate target indexes.
- [ ] Update all language bindings and examples and run their focused tests.

### Task 4: Full structured TurboQuant_prod

**Files:**
- Modify: `crates/borsuk/src/turboquant.rs`
- Modify: `crates/borsuk/src/global_pq_sidecar.rs`
- Modify: `crates/borsuk/src/index.rs`

- [ ] Add failing code-width tests showing `b-1` scalar bits plus one full residual bit and two norms.
- [ ] Add failing unbiasedness and top-k tests over seeded random vectors.
- [ ] Implement residual encoding with an independent full structured projection.
- [ ] Implement query preparation once per query and allocation-free per-code scoring.
- [ ] Add corrupt/truncated/odd-dimension/zero-vector tests and run the complete codec suite.

### Task 5: Persisted global-cell graph bundles

**Files:**
- Modify: `crates/borsuk/src/global_graph.rs`
- Modify: `crates/borsuk/src/global_pq_sidecar.rs`
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/src/manifest.rs`

- [ ] Add failing round-trip and corruption tests for a cell-local compact graph.
- [ ] Add deterministic serialization and checksums.
- [ ] Build one bounded graph at a time from the external cell spool and publish its reference atomically.
- [ ] Add decoded-byte accounting and single-flight loading tests.

### Task 6: Per-cell mixed execution

**Files:**
- Create: `crates/borsuk/src/cache_execution.rs`
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/src/segment_cache.rs`
- Modify: `crates/borsuk/src/record.rs`

- [ ] Add a failing mixed-coverage test where covered cells use graph and uncovered cells use SRHT-PQ.
- [ ] Add failing eviction, manifest-replacement, and corrupt-cache tests proving pre-query scan fallback.
- [ ] Implement manifest-pinned cell partitioning and shared graph/scan candidate merging.
- [ ] Extend reports with observed coverage and work by engine.
- [ ] Run bounded 16-user tests and verify duplicate decodes are single-flight.

### Task 7: Verification and AWS experiments

**Files:**
- Modify: `scripts/bench_scan_codec_matrix.sh`
- Modify: `scripts/bench_cache_execution_matrix.sh`
- Modify: `scripts/benchmark_with_resources.py`
- Modify: `docs/research/*`
- Modify: `docs/web/assets/benchmarks/*`

- [ ] Run formatting, focused Rust tests, workspace tests, Python harness tests, Node tests, Python binding tests, docs validation, and repository policy checks.
- [ ] Deploy the verified release build to both c7g.8xlarge benchmark machines.
- [ ] Run the staged codec and cache qualification matrices with three repetitions for selected points.
- [ ] Run six-corpus, synthetic, concurrency, resource, cost, and final 100M experiments.
- [ ] Check in raw artifacts, render charts, document rejected points, and change defaults only if every automated promotion gate passes.

# V26 PQ4 Production Promotion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Promote the holdout-qualified PQ4 fast scan into a public immutable index that builds in parallel and serves complete local searches through Arrow and Parquet.

**Architecture:** Add a focused `pq4` subsystem to the public `borsuk` crate, extend the safe `borsuk-fma` boundary with the AArch64 table-lookup kernel, and persist one authenticated local snapshot. Keep distribution outside the hot path and replace no legacy format because this is a new pre-release API.

**Tech Stack:** Rust 2024, Rayon, AArch64 NEON behind `borsuk-fma`, Apache Arrow IPC, Parquet, SHA-256, mmap, canonical JSON, AWS EC2 Spot.

**Spec:** `docs/superpowers/specs/2026-09-03-v26-pq4-production-promotion-design.md`

## Global Constraints

- Use 32 subquantizers, three dimensions, 16 centroids, 32-row transposed blocks, and 16 code bytes per row.
- Expose a new public `Pq4Index`; do not add compatibility readers, version dispatch, aliases, or a dependency on `borsuk-v26`.
- Keep all unsafe SIMD code in `borsuk-fma`; `borsuk` retains `#![forbid(unsafe_code)]`.
- Persist dense arrays as Arrow IPC, page records as Parquet, and only the manifest as canonical newline JSON.
- Search is local-only and performs zero network calls.
- Candidate depth is 2,048 and page budget is exactly ten for the qualified production configuration.
- The exact single-search 100-million-row projection is 2,336,975,744 bytes and the process budget is below 3 GiB.
- Release quality gates are 975,000/800,000/995,000 ppm and end-to-end p99 is at most 15,000,000 ns.
- Use exact-node and `v26_release_contract_` gates while iterating; run the full workspace suite once for the final candidate.

---

### Task 1: Safe PQ4 SIMD kernel

**Files:**
- Modify: `crates/borsuk-fma/src/lib.rs`

**Interfaces:**
- Produces: `Pq4BlockScorer::detect() -> Result<Pq4BlockScorer, Pq4Unavailable>` and `Pq4BlockScorer::score(&self, block: &[u8; 512], tables: &[[u8; 16]; 32]) -> [u16; 32]`.
- Consumes: one validated transposed block and one validated query table tensor.

- [ ] **Step 1: Write the failing scalar/NEON differential test.** Cover zero/15 nibbles, alternating nibbles, random blocks, score ties, reversed input construction, and the maximum score 8,160.
- [ ] **Step 2: Run the exact RED.** Run `cargo test -p borsuk-fma pq4_block_ -- --nocapture`; require missing PQ4 kernel symbols.
- [ ] **Step 3: Implement the safe detected wrapper.** Detect AArch64 NEON once; keep pointer arithmetic and `vqtbl1q_u8` inside one private target-feature function and expose no raw pointers.
- [ ] **Step 4: Run the exact GREEN.** Run `cargo test -p borsuk-fma pq4_block_ -- --nocapture`; require scalar-equivalent scores for every fixture.
- [ ] **Step 5: Run fmt and commit.** Run `cargo fmt --all -- --check` and `git diff --check`; commit `crates/borsuk-fma/src/lib.rs` as `feat(pq4): add safe fast-scan kernel`.

### Task 2: Public PQ4 core and bounded ranking

**Files:**
- Create: `crates/borsuk/src/pq4/core.rs`
- Create: `crates/borsuk/src/pq4/mod.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Produces: crate-private `Pq4Codebook`, `Pq4Blocks`, `Pq4QueryTables`, `Pq4RankedRow`, `fit_codebook`, `encode_blocks`, and `rank_candidates`.
- Consumes: finite nonzero `[f32; 96]` rows and Task 1's safe scorer.

- [ ] **Step 1: Port test contracts before implementation.** Add `v26_release_contract_pq4_core_` tests for deterministic stratified sampling, four Lloyd iterations, lower-centroid ties, nibble orientation, padding, source order, exact projection, histogram threshold, and `(score, ordinal)` ordering.
- [ ] **Step 2: Preserve the RED.** Run `cargo test -p borsuk --lib v26_release_contract_pq4_core_ -- --nocapture`; require missing core boundaries only.
- [ ] **Step 3: Implement fitting and parallel encoding.** Train subquantizers with Rayon, encode fixed record batches in parallel, and merge them in source order.
- [ ] **Step 4: Implement bounded parallel ranking.** Fill one owned `u16` score buffer, reduce per-chunk histograms deterministically, and allocate only the top-2,048 pair vector.
- [ ] **Step 5: Run focused GREEN and commit.** Run the exact selector plus `cargo test -p borsuk --lib v26_release_contract_ -- --nocapture`, then fmt/diff-check and commit the three files.

### Task 3: Strict snapshot format and local memory maps

**Files:**
- Create: `crates/borsuk/src/pq4/format.rs`
- Create: `crates/borsuk/src/pq4/snapshot.rs`
- Modify: `crates/borsuk/src/pq4/mod.rs`
- Modify: `crates/borsuk/Cargo.toml`

**Interfaces:**
- Produces: `Pq4Manifest`, `Pq4SnapshotWriter`, and `Pq4Snapshot` with typed codebook, blocks, source vectors, row mappings, page row-group directory, and authenticated identities.
- Consumes: the concrete five-file snapshot from the spec.

- [ ] **Step 1: Write strict schema and mutation REDs.** Mutate every role, digest, length, field name, type, nullability, dimension, row count, padding count, source order, page row-group boundary, and generation binding.
- [ ] **Step 2: Run the exact RED.** Run `cargo test -p borsuk --lib v26_release_contract_pq4_snapshot_ -- --nocapture`; require missing format/snapshot symbols.
- [ ] **Step 3: Implement Arrow/Parquet writers.** Write `codebook.arrow`, `codes.arrow`, `vectors.arrow`, `row-map.arrow`, and `pages.parquet` into a temporary directory, fsync them, generate canonical `manifest.json`, then atomically rename the directory.
- [ ] **Step 4: Implement strict open.** Authenticate bytes before parsing, validate every complete schema and cross-binding, and memory-map only the three dense serving files. Do not add an object-store client.
- [ ] **Step 5: Run focused GREEN and commit.** Run the snapshot selector and release-contract group; fmt/diff-check; commit the four files and lockfile changes.

### Task 4: Parallel public builder

**Files:**
- Create: `crates/borsuk/src/pq4/builder.rs`
- Modify: `crates/borsuk/src/pq4/mod.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Produces: public `Pq4BuildConfig`, `Pq4BuildReport`, and `Pq4Builder::build_parquet`.
- Consumes: strict input Parquet `id: binary, vector: f32[96]` and Task 3's snapshot writer.

- [ ] **Step 1: Write public builder REDs.** Require exact input schema, query-independent training, deterministic output identities across worker counts, bounded batch memory, primary/replica row caps, roster equality, and atomic failure cleanup.
- [ ] **Step 2: Run the exact RED.** Run `cargo test -p borsuk --lib v26_release_contract_pq4_builder_ -- --nocapture`; require missing public builder types.
- [ ] **Step 3: Implement pass-one training.** Validate all rows and retain the exact deterministic 8,192-row sample without loading the corpus; train 32 subquantizers in the configured Rayon pool.
- [ ] **Step 4: Implement pass-two construction.** Encode bounded batches in parallel, preserve source order, generate deterministic page assignments, and write all snapshot files through Task 3.
- [ ] **Step 5: Run focused GREEN and commit.** Run builder and release-contract selectors, fmt/diff-check, and commit the builder/public exports.

### Task 5: Public search and concurrency admission

**Files:**
- Create: `crates/borsuk/src/pq4/index.rs`
- Modify: `crates/borsuk/src/pq4/mod.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Produces: public `Pq4OpenOptions`, `Pq4Index`, `Pq4Match`, `Pq4Index::open`, and `Pq4Index::search`.
- Consumes: Task 3's local snapshot and Task 2's ranker.

- [ ] **Step 1: Write reduced end-to-end REDs.** Build a structurally faithful snapshot with multiple and partial blocks, duplicated page assignments, ten page row groups, tied scores, and literal exact neighbors; require deterministic matches and zero network surface.
- [ ] **Step 2: Write concurrency REDs.** Run simultaneous searches and require independent score buffers, deterministic results, bounded admission, and explicit timeout/error instead of RAM oversubscription.
- [ ] **Step 3: Run the exact RED.** Run `cargo test -p borsuk --lib v26_release_contract_pq4_search_ -- --nocapture`; require missing public index symbols.
- [ ] **Step 4: Implement open and scratch admission.** Freeze the Rayon pool, mmap snapshot arrays, allocate bounded reusable scratch slots, and reject a configuration whose admitted projection exceeds 3 GiB.
- [ ] **Step 5: Implement search.** Scan, exact-rerank 2,048 candidate vectors, reduce to ten pages, decode those Parquet row groups, deduplicate replicas, and return exact top-k by `(distance, source_ordinal)`.
- [ ] **Step 6: Run focused GREEN and commit.** Run the search selector and release-contract group, fmt/diff-check, and commit the production query surface.

### Task 6: Fast gate and public examples

**Files:**
- Modify: `scripts/check_v26_fast.py`
- Modify: `scripts/test_check_v26_fast.py`
- Create: `crates/borsuk/examples/pq4_build.rs`
- Create: `crates/borsuk/examples/pq4_search.rs`
- Modify: `README.md`

**Interfaces:**
- Produces: one documented build/search flow and the seconds-level release-contract command.
- Consumes: Tasks 4 and 5 public APIs only.

- [ ] **Step 1: Add gate self-tests.** Require the fast script to run the exact release-contract selector and fail immediately without invoking Clippy or the workspace suite.
- [ ] **Step 2: Add minimal examples.** Parse explicit local input/output paths and call only public APIs; add no S3, endpoint, loader, or compatibility flags.
- [ ] **Step 3: Update the README.** Document input schema, snapshot files, explicit materialization requirement, parallel build, local serving, memory admission, and current evidence caveat.
- [ ] **Step 4: Verify the fast boundary.** Run script self-tests, both example tests, default smoke, release-contract, and affected gate once.
- [ ] **Step 5: Commit.** Run fmt/diff-check and commit the five documentation/gate/example files.

### Task 7: Final assurance and one end-to-end Spot validation

**Files:**
- Modify after evidence exists: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: the frozen public builder/search commit and authenticated Deep Image source/query/truth Parquets.
- Produces: one production snapshot, typed holdout evidence Parquet, canonical result/receipt, and the release disposition.

- [ ] **Step 1: Run final local assurance once.** Run `python3 scripts/check_v26_fast.py --affected`, strict locked workspace/all-targets Clippy, and one locked workspace/all-targets test; stop at the first failure and repair only that layer before one final rerun.
- [ ] **Step 2: Build the release examples.** Build offline/locked release binaries, record SHA-256 and length, and upload only those frozen binaries plus a manifest.
- [ ] **Step 3: Launch one `causality` Spot build.** Use any available `eu-central-1` zone, instance NVMe, a 12 GiB build RSS stop, PSI full avg10 at most 1%, zero swap growth, a 7,200-second cap, and immediate termination.
- [ ] **Step 4: Run development fail-fast.** Execute queries 0..31 through public `Pq4Index::search`; require exact ten pages, 975,000/800,000/995,000 ppm, under-3-GiB RSS, and 15 ms p99 before opening holdout.
- [ ] **Step 5: Run the sealed holdout once.** Freeze all parameters, execute ordinals 32..511, record p50/p95/p99/max, quality, page reads, bytes, RSS, PSI, swap, source/binary/snapshot identities, and terminate immediately.
- [ ] **Step 6: Persist the release disposition.** Validate the evidence ledger, commit/push it, verify `HEAD==origin/main==ls-remote` and clean status. Mark the API release-ready only if every public end-to-end gate passes.

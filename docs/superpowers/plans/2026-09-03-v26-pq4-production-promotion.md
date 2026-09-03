# V26 PQ4 Direct-Row Production Promotion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a public immutable PQ4 shard that builds in parallel from Parquet and returns exact-reranked rows from an authenticated Arrow IPC snapshot within the measured quality, memory, and latency gates.

**Architecture:** The focused `borsuk-pq4` crate owns each local shard and its fast contract tests; `borsuk` re-exports only the finished public API. Each shard reads 3,072 exact candidate vectors before returning local top-k. There is no page stage or network client. A 100-million-row deployment trains and searches roughly 10-million-row shards concurrently and merges bounded exact results outside the shard.

**Tech Stack:** Rust 2024, Rayon, AArch64 NEON, x86_64 AVX2/SSSE3, Apache Arrow IPC, Parquet, SHA-256, canonical JSON, AWS EC2 Spot.

**Spec:** `docs/superpowers/specs/2026-09-03-v26-pq4-production-promotion-design.md`

## Global Constraints

- Use exactly 32 three-dimensional subquantizers, 16 centroids, four Lloyd iterations, 32-row transposed blocks, and 16 code bytes per row.
- Freeze candidate depth at 3,072 for the next sealed validation.
- Snapshot roles are only `manifest.json`, `codebook.arrow`, `codes.arrow`, `vectors.arrow`, and `ids.arrow`.
- Keep `borsuk` free of unsafe code, storage clients, compatibility readers, page APIs, dynamic loaders, and hidden network access.
- Release gates are 995,000-ppm aggregate direct-row Recall@10, 997,500-ppm query-floor compliance at 800,000 ppm, 15,000,000-ns p99, and the exact 2,336,975,744-byte single-query projection below 3 GiB.
- Use exact-node tests per edit, the release-contract selector per stable component, and one full workspace gate only for the final candidate.

---

### Task 1: Preserve the qualified core and add portable SIMD

**Files:**
- Modify: `crates/borsuk-fma/src/lib.rs`
- Modify: `crates/borsuk-pq4/src/core.rs`

**Interfaces:**
- Produces: `Pq4BlockScorer::detect()` and `Pq4BlockScorer::score(&[u8; 512], &[[u8; 16]; 32]) -> [u16; 32]` on AArch64 and x86_64.
- Consumes: the already committed deterministic training, encoding, tables, projection, and bounded ranking core.

- [x] **Step 1: Preserve the AArch64 RED/GREEN.** The scalar/NEON differential covers zero/15 nibbles, alternating nibbles, random blocks, ties, reversal, and maximum score 8,160.
- [x] **Step 2: Preserve the public core RED/GREEN.** The four focused contracts cover deterministic fit, nibble packing, projection, and bounded ranking.
- [ ] **Step 3: Write the x86 RED.** Extend the same literal blocks so `Pq4BlockScorer::detect()` must choose an x86 SIMD backend and match scalar output bit-for-bit; an unsupported CPU must return `Pq4Unavailable`.
- [ ] **Step 4: Run the x86 RED.** Run `cargo test -p borsuk-fma pq4_block_ -- --nocapture`; require the missing x86 backend only.
- [ ] **Step 5: Implement x86 scoring.** Use runtime feature detection and private target-feature functions with `_mm_shuffle_epi8` table lookup and widened integer accumulation. Expose no raw pointer and do not fall back silently to scalar in production.
- [ ] **Step 6: Run GREEN and commit.** Run the focused scorer and `cargo test -p borsuk-pq4 --lib v26_release_contract_pq4_core_ -- --nocapture`, then fmt and diff-check; commit only the two affected files.

### Task 2: Write and open the direct-row snapshot

**Files:**
- Create: `crates/borsuk-pq4/src/format.rs`
- Create: `crates/borsuk-pq4/src/snapshot.rs`
- Modify: `crates/borsuk-pq4/src/lib.rs`
- Modify: `crates/borsuk-pq4/Cargo.toml`

**Interfaces:**
- Produces: crate-private `Pq4Manifest`, `Pq4SnapshotWriter`, and `Pq4Snapshot`.
- Consumes: `Pq4Codebook` and `Pq4Blocks` from Task 1.

- [ ] **Step 1: Write strict mutation tests.** Test every role, digest, length, field, concrete Arrow type, child name/nullability, dimensions, row count, padding, source order, generation, extra file, and missing file under `v26_release_contract_pq4_snapshot_`.
- [ ] **Step 2: Run RED.** Run `cargo test -p borsuk-pq4 --lib v26_release_contract_pq4_snapshot_ -- --nocapture`; require only missing snapshot types.
- [ ] **Step 3: Implement the writer.** Write the four Arrow files into a sibling temporary directory, fsync each, compute identities, serialize a sorted compact newline manifest, fsync the directory, and rename atomically.
- [ ] **Step 4: Implement strict open.** Authenticate file bytes before parsing, require exact schemas and cross-bindings, load codes into owned memory, and retain safe `FileExt::read_exact_at` handles plus validated batch offsets for vectors and IDs.
- [ ] **Step 5: Run GREEN and commit.** Run the exact selector and release-contract group, then fmt/diff-check; commit only Task 2 files and legitimate lockfile changes.

### Task 3: Build shards in parallel from Parquet

**Files:**
- Create: `crates/borsuk-pq4/src/builder.rs`
- Modify: `crates/borsuk-pq4/src/lib.rs`

**Interfaces:**
- Produces: public `Pq4BuildConfig`, `Pq4BuildReport`, and `Pq4Builder::build_parquet(input, output, config)`.
- Consumes: Task 2's snapshot writer.

- [ ] **Step 1: Write builder tests.** Require exact `id`/`vector` Parquet schema, complete finiteness/nonzero validation, deterministic identities across worker counts, bounded batch memory, identical row order across all four Arrow roles, and cleanup after injected failure.
- [ ] **Step 2: Run RED.** Run `cargo test -p borsuk-pq4 --lib v26_release_contract_pq4_builder_ -- --nocapture`; require missing public builder symbols.
- [ ] **Step 3: Implement pass one.** Stream all rows, validate them, retain the exact stratified 8,192-row sample, and train 32 subquantizers in the configured Rayon pool.
- [ ] **Step 4: Implement pass two.** Encode bounded batches in parallel, merge by source batch/row order, and feed codes, vectors, and IDs to the atomic snapshot writer.
- [ ] **Step 5: Run GREEN and commit.** Run the builder selector and release-contract group, then fmt/diff-check; commit the builder and public exports.

### Task 4: Serve exact local rows with bounded concurrency

**Files:**
- Create: `crates/borsuk-pq4/src/index.rs`
- Modify: `crates/borsuk-pq4/src/lib.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Produces: public `Pq4OpenOptions`, `Pq4Index`, `Pq4Match`, `Pq4Index::open`, and `Pq4Index::search`.
- Consumes: Task 2's snapshot and Task 1's ranker.

- [ ] **Step 1: Write direct-row REDs.** Build a reduced snapshot with multiple blocks, a partial last block, tied scores, literal exact neighbors, and variable binary IDs; require exact `(distance, source_ordinal)` order and zero page/network surface.
- [ ] **Step 2: Write admission REDs.** Concurrent calls must own independent score buffers, preserve results, and return an explicit admission timeout when the exact projection would exceed 3 GiB.
- [ ] **Step 3: Run RED.** Run `cargo test -p borsuk-pq4 --lib v26_release_contract_pq4_search_ -- --nocapture`; require missing public index symbols.
- [ ] **Step 4: Implement open and admission.** Validate candidate depth equals 3,072, construct the fixed query pool, and allocate bounded reusable score/candidate scratch slots from the configured budget.
- [ ] **Step 5: Implement search.** Scan codes, retain 3,072 candidates, positionally read exact vectors, rerank, read only final IDs, and return top-k without page or network calls.
- [ ] **Step 6: Run GREEN and commit.** Run the search selector and release-contract group, then fmt/diff-check; commit only Task 4 files.

### Task 5: Add deterministic shard-result merging

**Files:**
- Create: `crates/borsuk-pq4/src/shards.rs`
- Modify: `crates/borsuk-pq4/src/lib.rs`

**Interfaces:**
- Produces: `merge_pq4_shard_matches(Vec<(u32, Vec<Pq4Match>)>, k) -> Result<Vec<Pq4Match>>`.
- Consumes: exact local top-k outputs from Task 4.

- [ ] **Step 1: Write merge REDs.** Cover empty/duplicate shard ordinals, fewer than k rows, equal distances, opaque ID preservation, permutation invariance, and the proof that global top-k is contained in the union of exact local top-k lists.
- [ ] **Step 2: Run RED.** Run `cargo test -p borsuk-pq4 --lib v26_release_contract_pq4_shards_ -- --nocapture`; require the missing merge function.
- [ ] **Step 3: Implement bounded merge.** Validate unique shard ordinals and merge at most `shards * k` rows using `(distance, shard_ordinal, source_ordinal)`.
- [ ] **Step 4: Run GREEN and commit.** Run the shard selector and release-contract group, then fmt/diff-check; commit only Task 5 files.

### Task 6: Make the seconds-level gate and examples the default loop

**Files:**
- Modify: `scripts/check_v26_fast.py`
- Modify: `scripts/test_check_v26_fast.py`
- Create: `crates/borsuk/examples/pq4_build.rs`
- Create: `crates/borsuk/examples/pq4_search.rs`
- Modify: `README.md`

**Interfaces:**
- Produces: a documented Parquet-to-Arrow build/search flow and `--affected` fast gate.
- Consumes: Tasks 3 through 5 public APIs only.

- [ ] **Step 1: Write gate self-tests.** Assert `--affected` runs exact changed-node filters first, stops at the first failure, and never invokes Clippy or workspace tests.
- [ ] **Step 2: Add examples.** Parse explicit local paths and threads, invoke only public APIs, and emit matches as sorted compact newline JSON; add no bucket, endpoint, page, loader, or compatibility flags.
- [ ] **Step 3: Document the contract.** Document exact Parquet input, Arrow snapshot files, parallel build, direct-row semantics, local-only serving, shard merge, and bounded admission.
- [ ] **Step 4: Verify and commit.** Run script self-tests, both example tests, `python3 scripts/check_v26_fast.py --affected`, fmt, and diff-check; commit only Task 6 files.

### Task 7: Final assurance and fresh sealed validation

**Files:**
- Modify after evidence exists: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: the frozen public build/search commit and authenticated source/query data.
- Produces: fresh truth Parquet, one shard snapshot, typed holdout Parquet, canonical result/receipt, and release disposition.

- [ ] **Step 1: Run final local assurance once.** Run `python3 scripts/check_v26_fast.py --affected`, strict locked workspace/all-targets Clippy, and one locked workspace/all-targets test. After a failure, repair only that layer and run one final assurance sequence.
- [ ] **Step 2: Freeze release binaries.** Build offline/locked examples, record SHA-256 and byte lengths, and upload only binaries plus their canonical authority manifest.
- [ ] **Step 3: Build on one `causality` Spot instance.** Use any available `eu-central-1` zone, instance NVMe, 12 GiB build RSS cap, PSI full avg10 at most 1%, zero swap growth, 7,200-second wall cap, and immediate termination.
- [ ] **Step 4: Generate sealed truth.** Compute exact top-10 truth for unused query ordinals 512..991, upload typed Parquet and a terminal receipt, and do not inspect per-query or aggregate results before the source and gates are frozen.
- [ ] **Step 5: Run one sealed holdout.** Measure the complete public `Pq4Index::search` for ordinals 512..991 and require 995,000-ppm aggregate recall, 997,500-ppm query-floor compliance at 800,000 ppm, p99 at most 15 ms, RSS below 3 GiB, PSI at most 1%, zero swap growth, and no network/page reads.
- [ ] **Step 6: Persist disposition.** Validate the ledger, commit/push it, verify `HEAD==origin/main==ls-remote` and a clean worktree, and terminate all infrastructure. Keep distributed 100-million-row, competitor, and D3 claims fenced until separately measured.

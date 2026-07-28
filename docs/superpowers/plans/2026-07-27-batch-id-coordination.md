# Batch ID Coordination Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace one object-store claim per explicit record with fixed-shard
batch coordination while retaining linearizable insert-only duplicate
semantics and crash fencing.

**Architecture:** Checked binary transaction-state and claim-shard objects
provide fencing. A short gate makes parallel routing-independent shard
acquisition all-or-none. Writers refresh and validate unless every shard
version matches their current snapshot checkpoint, prepare immutable cell-WAL
runs, fence the transaction `committing`, then publish the existing atomic
commit marker.

**Tech Stack:** Rust, `object_store` conditional PUTs, BLAKE3 checked binary
codecs, existing cell-WAL transaction descriptors, Cargo integration tests.

---

### Task 1: Lock the request-bound contract

**Files:**
- Modify: `crates/borsuk/tests/cell_wal.rs`
- Modify: `crates/borsuk/src/index.rs`

- [x] Add a test that creates a WAL-enabled in-memory index, inserts 500
  caller-supplied IDs in one batch, and asserts that PUTs are bounded by the
  fixed claim-shard protocol rather than `records + WAL overhead`.
- [x] Run
  `cargo test -p borsuk --test cell_wal explicit_id_batch_coordination_is_bounded_by_claim_shards -- --exact`
  and verify that it fails because the current implementation emits at least
  508 PUTs.
- [x] Add the fixed claim-shard count and ID-to-shard helper only after the
  failing result exists.

### Task 2: Add checked fenced-state and shard-lock codecs

**Files:**
- Modify: `crates/borsuk/src/cell_wal.rs`
- Test: `crates/borsuk/tests/cell_wal.rs`

- [x] Add failing round-trip, checksum-corruption, invalid-state, and
  trailing-byte tests for transaction states and claim locks.
- [x] Run the focused codec tests and verify the missing APIs fail compilation.
- [x] Implement explicit-version checked binary codecs for prepared, committed,
  aborted, available, and owned states.
- [x] Run the focused codec tests and verify they pass.

### Task 3: Fence transaction visibility

**Files:**
- Modify: `crates/borsuk/src/cell_wal.rs`
- Modify: `crates/borsuk/src/index.rs`
- Test: `crates/borsuk/tests/cell_wal.rs`

- [ ] Add tests proving prepared and aborted transactions are invisible,
  committed transactions are visible, and an aborted transaction cannot commit.
- [ ] Verify the tests fail against the create-only commit marker.
- [x] Create prepared state before run publication, fence
  prepared-to-committing, and retain the commit marker as the visibility point.
- [x] Recover committing owners by completing the descriptor-pinned marker.
- [x] Verify all transaction visibility and crash-recovery tests pass.

### Task 4: Replace per-ID claims with batch shard guards

**Files:**
- Modify: `crates/borsuk/src/index.rs`
- Test: `crates/borsuk/tests/cell_wal.rs`
- Test: `crates/borsuk/tests/local_index.rs`

- [x] Add failing tests for concurrent duplicate inserts, multi-shard rollback,
  committed-owner reclamation, expired prepared-owner fencing, and stale-handle
  refresh under a claim.
- [ ] Verify each test fails for the expected old-protocol reason.
- [x] Implement gated parallel claim-shard acquisition, conditional release,
  prepared transaction abort on failure, and checkpointed
  refresh-before-validation.
- [x] Delete per-ID persistent insert-claim creation and purge cleanup.
- [x] Verify focused concurrency and lifecycle tests pass.

### Task 5: Parallelize independent preparation

**Files:**
- Modify: `crates/borsuk/src/cell_wal.rs`
- Test: `crates/borsuk/tests/cell_wal.rs`

- [ ] Add a barrier-backed test proving two independent immutable run uploads
  can overlap while lane-head publication remains ordered per lane.
- [ ] Verify it fails because the current preparation loop is sequential.
- [x] Use the repository's bounded parallel executor for content-addressed
  payload preparation and independent cell/lane publication groups.
- [x] Verify all cell-WAL protocol tests pass. A dedicated barrier-backed upload
  overlap test remains follow-up coverage.

### Task 6: Version, documentation, and benchmark evidence

**Files:**
- Modify: `crates/borsuk/src/format.rs`
- Modify: `docs/architecture.md`
- Modify: `docs/storage-format.md`
- Modify: `docs/production-readiness.md`
- Modify: `crates/borsuk/examples/wal_layout_bench.rs`
- Modify: `scripts/validate_wal_layout_qualification.py`

- [x] Bump the pre-release table/protocol version and reject prior experimental
  transaction-state layouts.
- [x] Document fixed claim shards, fencing, request bounds, and crash recovery.
- [ ] Extend the WAL benchmark output with ID mode and claim-shard request
  counters while preserving fail-closed protocol identity checks.
- [x] Run focused tests, full Rust tests, test-binary build, Clippy, formatting,
  research validators, and the local explicit/generated ingest A/B.
- [x] Freeze a fresh AWS ingest diagnostic only after every local gate passes;
  never mix its results with v14 campaign artifacts.

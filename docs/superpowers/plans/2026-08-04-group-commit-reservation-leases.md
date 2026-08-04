# Group-Commit Carried Reservation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce the steady-state durable group-commit critical path from root reservation GET+CAS, immutable upload, and visibility CAS to immutable upload followed by one visibility CAS without weakening S3 durability, snapshot atomicity, or GC fencing.

**Architecture:** A group-commit lane's visibility CAS replaces its current reservation with the commit and, in the same checked HEAD write, installs exactly one successor reservation with a fresh one-hour expiry. The lane consumes at most four transactions in one shard, then commits without a successor and performs the ordinary reservation protocol in a newly selected shard. This preserves the existing one-slot crash/GC fence, bounds shard pressure below the eight-transaction soft threshold, and leaves ordinary `BorsukIndex` mutations unchanged.

**Tech Stack:** Rust, `object_store` conditional writes, existing packed collection-control records, integration fault injection, group-commit request tracing.

## Global Constraints

- Pre-release format/API changes are allowed, but no compatibility reader or duplicate write path may be retained solely for old experiments.
- A receipt is acknowledged only after immutable payloads and descriptors exist and the checked collection-frontier CAS makes their transaction visible.
- Every staged object must remain owned by a live root reservation until the same CAS replaces that reservation with its commit.
- At most one unconsumed carried reservation exists per group-commit lane, and its expiry is stamped by the CAS that creates it.
- CAS ambiguity must be resolved from root reachability; cached HEAD content/version is invalidated after every non-success result.
- Incomplete AWS campaign CSV files are never inspected; a changed architecture requires a fresh campaign identity and source archive.
- Development uses TDD; commits are verified, fast-forward only, and pushed directly to `origin/main` without a pull request.

---

### Task 1: Prove the current coordination amplification

**Files:**
- Modify: `crates/borsuk/tests/group_commit.rs`

**Interfaces:**
- Consumes: `GroupCommitWriter::append`, `FaultInjectingObjectStore::with_operation_log`, collection frontier `HEAD` paths.
- Produces: regression `steady_state_groups_amortize_root_reservation_coordination`.

- [ ] **Step 1: Write the failing request-amplification test**

  Create one in-memory writer lane with `max_records = 1`, append eight measured groups, and count GET/PUT operations whose path starts with `collection/wal-frontier/` and ends with `/HEAD`. Require at most two reservation GETs, exactly eight visibility PUTs plus at most two refill PUTs, and successful reopen/readback of all IDs. The production change that makes this pass is reusing a real root reservation; local queue changes cannot alter these HEAD counts.

- [ ] **Step 2: Run the exact test and verify RED**

  Run `cargo test -p borsuk --test group_commit steady_state_groups_amortize_root_reservation_coordination -- --exact`. Expected: FAIL because all eight current groups perform a reservation GET+PUT before their visibility PUT.

- [ ] **Step 3: Commit the red regression**

  Run `git add crates/borsuk/tests/group_commit.rs && git commit -m 'test: bound group commit reservation coordination'`.

---

### Task 2: Atomically carry one successor reservation

**Files:**
- Modify: `crates/borsuk/src/storage.rs`
- Test: `crates/borsuk/src/storage.rs`

**Interfaces:**
- Produces: `create_collection_commit_from_reservation` accepts an optional `(transaction_id, schema_fingerprint)` successor and returns `CollectionWalCommitOutcome { root_pressure: bool, successor: Option<CollectionWalReservationReceipt> }`.
- Consumes: checked HEAD parser/writer, hard capacity 64, soft committed-transaction threshold 8, one-hour reservation TTL.

- [ ] **Step 1: Write failing storage tests**

  Add exact unit tests proving: one conditional HEAD PUT both commits the current transaction and installs one same-shard successor; the successor receives a full fresh TTL; mixed-shard successors and capacity overflow fail before a write; and the returned successor receipt chains into the next commit without an intervening GET.

- [ ] **Step 2: Run the exact tests and verify RED**

  Run each new test with `cargo test -p borsuk --lib storage::tests::<name> -- --exact`. Expected: compile failure because the successor input and outcome do not exist.

- [ ] **Step 3: Implement the minimal checked transition**

  Validate the successor ID/schema before mutating the cached HEAD. Require the same shard, prune expired reservations, reject duplicate/conflicting IDs, enforce `reservations + transactions + 1 <= 64`, remove the current reservation, append the commit and optional freshly stamped successor, sort canonically, increment `head.generation`, and conditional-write the HEAD. Preserve the existing reread-and-reapply commit fallback on version conflict; that fallback returns no successor because it cannot safely install one from stale cached content. Return the opaque version from a successful conditional write in the successor receipt.

- [ ] **Step 4: Run storage and collection-control gates GREEN**

  Run `cargo test -p borsuk --lib storage::tests` and `cargo test -p borsuk --lib collection_control::tests`. Expected: PASS.

- [ ] **Step 5: Commit the storage primitive**

  Run `git add crates/borsuk/src/storage.rs && git commit -m 'feat: carry collection root reservations'`.

---

### Task 3: Consume carried reservations only in group-commit lanes

**Files:**
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/src/group_commit.rs`
- Modify: `crates/borsuk/tests/group_commit.rs`
- Test: `crates/borsuk/tests/fault_injection.rs`
- Test: `crates/borsuk/tests/crash_recovery.rs`

**Interfaces:**
- Produces: private lane-local `GroupCommitReservationLease { receipt, commits_in_shard }`, with maximum run length four; `BorsukIndex::group_commit_add` is its only consumer.
- Consumes: Task 2 successor transition; existing ordinary transaction functions remain unchanged; the process-shared last-write-wins generation lease remains shared across lanes.

- [ ] **Step 1: Add failing lifecycle and concurrency tests**

  Require: (a) Task 1's request bound; (b) a foreign write to the same shard forces authoritative reread/reconcile without losing either commit; (c) accepted-but-retryable final CAS is recognized as durable and invalidates the carried receipt; (d) an expired carried reservation is rejected before staging and ordinarily re-reserved; (e) independent writer clones never share a carried transaction ID; (f) unused carried reservations survive GC while live and are reclaimed after expiry; and (g) more than eight commits rotate across at least two shards without repeated soft-pressure maintenance.

- [ ] **Step 2: Run each exact test and verify RED**

  Run the named tests individually in `group_commit`, `fault_injection`, and `crash_recovery`. Expected: FAIL because group-commit lanes currently reserve one random transaction before each stage and never receive a successor.

- [ ] **Step 3: Implement lane-local carry and rotation**

  Store carried state outside fields shared by `BorsukIndex::clone`; clear it in `reset_independent_writer_state`. When no valid receipt exists, use ordinary reservation. Before staging, discard a receipt whose expiry has passed. For commits one through three, generate a same-shard successor ID and request it in the final CAS; commit four requests no successor so the next group rotates through ordinary random-shard admission. Cache a successor only from an unambiguous successful CAS. Keep the last-write-wins generation lease shared and leave public `put`, `add`, `upsert`, named, and text paths unchanged.

- [ ] **Step 4: Run exact tests and full correctness suites GREEN**

  Run `cargo test -p borsuk --test group_commit`, `cargo test -p borsuk --test consistency`, `cargo test -p borsuk --test crash_recovery`, `cargo test -p borsuk --test fault_injection`, and `cargo test -p borsuk --test cell_wal`. Expected: PASS.

- [ ] **Step 5: Commit the group-commit carry path**

  Run `git add crates/borsuk/src/index.rs crates/borsuk/src/group_commit.rs crates/borsuk/tests/group_commit.rs crates/borsuk/tests/fault_injection.rs crates/borsuk/tests/crash_recovery.rs && git commit -m 'perf: carry group commit root reservations'`.

---

### Task 4: Verify, document, and qualify from a fresh revision

**Files:**
- Modify: `docs/api.md`
- Modify: `docs/consistency.md`
- Modify: `docs/research/group-commit-scalability-attempt-ledger.md`
- Modify: `docs/research/group-commit-scalability-campaign.json`

**Interfaces:**
- Consumes: green implementation and unchanged five-repetition 2K/16K by 1/8/32-writer protocol.
- Produces: a fresh source archive/campaign identity whose comparison contract discloses the carried-reservation bound and unchanged S3 acknowledgement semantics.

- [ ] **Step 1: Update architecture and campaign disclosure**

  Document one-slot ownership, fresh expiry, four-commit shard rotation, ordinary-API exclusion, GC invariant, conflict fallback, and request-count expectation. Do not reuse v13 artifacts or imply cross-architecture comparability.

- [ ] **Step 2: Run the complete local release gate**

  Run formatting/diff checks, Clippy with `-D warnings`, group-commit/WAL/consistency/crash/fault suites, validator unit tests, and the structurally valid local smoke. Expected: every command exits zero.

- [ ] **Step 3: Obtain a read-only adversarial review and fix only verified defects**

  Ask the other model to inspect the final diff without editing, independently reproduce each material concern, and add a red regression before any correction.

- [ ] **Step 4: Commit and fast-forward push**

  Fetch `origin/main`, require `git merge-base --is-ancestor origin/main HEAD`, require an empty status after commit, then run `git push origin HEAD:main` without force.

- [ ] **Step 5: Launch only after isolation checks**

  Verify no competing benchmark process/service, fresh S3 prefixes, sufficient disk/memory, and matching source/manifest hashes. Launch a new five-repetition campaign under `AWS_PROFILE=causality`; while incomplete inspect only terminal markers, service/process health, non-measurement progress, and resources. On terminal success, run the repository fail-closed validator before reading results; on failure, record only defensible markers and investigate from source/health evidence.

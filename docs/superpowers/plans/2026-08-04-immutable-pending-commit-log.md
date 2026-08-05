# Immutable Pending Commit Log Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace mutable collection WAL frontier heads and foreground global materialization with immutable pending commits, bracketed read discovery, and fenced background checkpoints.

**Architecture:** A group first writes immutable WAL payloads and descriptors, then creates one epoch-scoped pending commit with `PutMode::Create`; that creation is its durability/visibility ACK. Readers bracket a complete pending-prefix LIST with two `CURRENT` reads. A checkpointer materializes a stable pending set, publishes a consumed fence with one catalog CAS, and only then deletes fenced pending objects.

**Tech Stack:** Rust, `object_store`, packed collection-control codec, S3 conditional PUT and strongly consistent LIST, existing WAL/manifest builders and fault-injection store.

## Global Constraints

- This is a pre-release format replacement. Increment the collection codec/version marker and reject v2 artifacts; do not add a legacy reader or dual-write path.
- An acknowledgement is returned only after every referenced immutable object exists and the pending commit's create-if-absent succeeds or identical existing content is verified.
- No manifest build, global refresh, checkpoint, or pruning may occur on the acknowledgement critical path.
- Readers accept a catalog/pending snapshot only when the two `CURRENT` observations match.
- Pending deletion is legal only after a currently published consumed fence names the exact commit.
- Use store-clock-derived age and reachability proof for GC; client wall time alone is insufficient.
- More than 2,000 observed pending objects is a fail-closed read error. At 1,000 observed objects writers synchronously help or wait for checkpoint progress before admitting more work.
- Preserve last-write-wins generations, exact visibility, recall@1, snapshot isolation, crash safety, and multi-process correctness.
- Never inspect incomplete campaign CSV files. Each format change requires fresh source, manifest, index, and result prefixes.
- Commit verified coherent slices directly to `origin/main` by fast-forward only; no pull request and no force push.

---

### Task 1: Encode and conditionally publish one immutable pending commit

**Files:**
- Modify: `crates/borsuk/src/collection_control.rs`
- Modify: `crates/borsuk/src/storage.rs`
- Test: `crates/borsuk/src/collection_control.rs`
- Test: `crates/borsuk/src/storage.rs`

**Interfaces:**
- Produces: `PendingCollectionCommit { epoch, created_at_ms, commit: CollectionCommit }`, `pending_collection_commit_path(epoch: &str, transaction_id: &str)`, codec functions, and `CollectionStorage::create_pending_collection_commit(&PendingCollectionCommit) -> Result<()>`.
- Consumes: existing `CollectionCommit`, packed codec helpers, checksum validation, and `Storage::write_bytes_if_absent`.

- [x] **Step 1: Write codec and conditional-create regressions**

  Add literal-fixture tests proving round-trip canonical encoding, rejection of a path/transaction mismatch, rejection of trailing or corrupt bytes, idempotent recreation of identical content, and conflict on different content at the same path. The storage test must count exactly one PUT and zero GET/HEAD operations for the first successful creation.

- [x] **Step 2: Run the exact tests and verify RED**

  Run `cargo test -p borsuk --lib collection_control::tests::pending_collection_commit_codec_is_canonical -- --exact` and `cargo test -p borsuk --lib storage::tests::pending_collection_commit_create_is_one_immutable_put -- --exact`. Expected: compile failure because the pending type and create operation do not exist.

- [x] **Step 3: Implement the smallest immutable primitive**

  Increment `COLLECTION_CODEC_VERSION`, add a distinct `BCPC` magic, validate epoch/transaction/checksums, and serialize the existing `CollectionCommit` fields plus epoch and store-clock creation time. Create with `PutMode::Create`; on `AlreadyExists`, read and byte-compare the object so identical retry is success and different content is an integrity error.

- [x] **Step 4: Run codec and storage suites GREEN**

  Run `cargo test -p borsuk --lib collection_control::tests` and `cargo test -p borsuk --lib storage::tests`. Expected: PASS.

- [x] **Step 5: Commit the primitive**

  Commit `collection_control.rs` and `storage.rs` as `feat: add immutable pending commit objects` after `cargo fmt --check` and `git diff --check`.

### Task 2: Prove constant-cost acknowledgements remain visible after reopen

**Files:**
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/src/group_commit.rs`
- Modify: `crates/borsuk/src/storage.rs`
- Modify: `crates/borsuk/tests/group_commit.rs`
- Test: `crates/borsuk/tests/fault_injection.rs`
- Test: `crates/borsuk/tests/crash_recovery.rs`

**Interfaces:**
- Produces: `BorsukIndex::publish_pending_group_commit`, with `GroupCommitReceipt.requests` ending at the pending-object PUT, plus the minimal `CURRENT/LIST/CURRENT` pending discovery required for reopen correctness.
- Consumes: Task 1's conditional pending creation and existing staged collection descriptors/WAL runs.

- [x] **Step 1: Write the RED request-amplification test**

  Append 600 four-record groups to an in-memory store. Assert every receipt has zero frontier HEAD/GET/PUT operations, no `collection/CURRENT` PUT, no segment build, and exactly one `write-epochs/*/pending/*.commit` PUT. Reopen and require all 2,400 IDs visible. This fails if foreground maintenance merely moves to another threshold.

- [x] **Step 2: Add crash-boundary RED tests**

  Inject failure immediately before and immediately after pending creation. Before creation, no record may be visible and retry is safe. An accepted-but-retryable creation must be resolved by identical-object verification and return a durable receipt exactly once.

- [x] **Step 3: Run exact tests and verify RED**

  Run the named `group_commit`, `fault_injection`, and `crash_recovery` tests individually. Expected: request bound fails because current root reservation/publication and foreground maintenance remain.

- [x] **Step 4: Cut group commit to the pending path**

  Retain payload/descriptor staging and generation allocation, replace reservation plus frontier publication with Task 1's immutable create, remove carried-reservation state and shard scheduling from group-commit workers, and return the receipt before any maintenance. Add a complete bracketed pending-prefix discovery to collection open/refresh so the acknowledged commits survive reopen. Do not add pagination limits or checkpoint filtering in this slice; Task 3 adds their fail-closed proofs and bounds. Do not change ordinary public mutation paths in this slice.

- [x] **Step 5: Run group-commit, fault, and crash suites GREEN**

  Run `cargo test -p borsuk --test group_commit`, `cargo test -p borsuk --test fault_injection`, and `cargo test -p borsuk --test crash_recovery`. Expected: PASS.

- [x] **Step 6: Commit the constant-cost ACK path**

  Commit as `perf: publish group commits as immutable pending objects` after strict Clippy and formatting gates.

**Post-task cutover:** ordinary `add`/`put`/`upsert` mutations now use the same
immutable pending publication instead of mutable frontier admission. Ordinary
APIs retain post-ACK threshold maintenance; `GroupCommitWriter` alone defers
that work. Exact fault and GC regressions require root-authorized staged
transactions to be recoverable from their committed `STATE` descriptor as well
as lane-WAL `COMMIT` markers.

### Task 3: Discover a race-free bounded pending snapshot

**Files:**
- Modify: `crates/borsuk/src/storage.rs`
- Modify: `crates/borsuk/src/index.rs`
- Test: `crates/borsuk/tests/consistency.rs`
- Test: `crates/borsuk/tests/group_commit.rs`

**Interfaces:**
- Produces: `PendingCommitSnapshot { current: LoadedCollectionSnapshot, commits: BTreeMap<String, CollectionCommit>, list_pages: u64, retries: u64 }` and `CollectionStorage::load_collection_view_with_pending()`.
- Consumes: complete paginated LIST for the catalog's epoch, pending codec validation, and the current snapshot loader.

- [ ] **Step 1: Write RED snapshot-race tests**

  Use the real fault-injection store to pause after `C0`, publish a checkpoint-like `CURRENT` change, mutate the pending prefix, then resume. Require a retry and a result from one catalog generation only. Add a commit concurrent with LIST and prove it is either wholly present or absent according to the linearization point, never partially decoded.

- [ ] **Step 2: Write RED bound and cost tests**

  Require complete pagination at exactly 1,000 and 2,000 pending objects, consumed-fence duplicate suppression, and an explicit error before reading object 2,001. Require an empty/short pending view to issue two CURRENT observations plus one LIST sequence and no 64-head reads.

  Progress: the explicit pre-body-read 2,001 fail-closed regression is GREEN,
  and accepted pending bodies are fetched on the bounded shared I/O pool. The
  exact open-path proof is also GREEN with zero reads of the 64 obsolete
  frontier heads; pending authorization is LIST-only and does not fetch commit
  bodies. Pagination and complete checkpoint-fence request-shape proofs remain
  open; this is not a completed step or a read-latency qualification.

- [ ] **Step 3: Run exact tests and verify RED**

  Expected: compile failure because bracketed pending discovery is absent.

- [ ] **Step 4: Implement bracketed discovery and integrate refresh/open**

  Load `C0`, list and validate all pending pages for its epoch, load `C1`, retry unless the pointer version/checksum matches, filter IDs already in the catalog's consumed fence, and build the existing cell-WAL snapshot from the accepted commits. Enforce 2,000 before fetching another pending body.

- [ ] **Step 5: Run storage, consistency, cell-WAL, and group-commit suites GREEN**

  Run `cargo test -p borsuk --lib storage::tests`, `cargo test -p borsuk --test consistency`, `cargo test -p borsuk --test cell_wal`, and `cargo test -p borsuk --test group_commit`.

- [ ] **Step 6: Commit bracketed reads**

  Commit as `feat: read bracketed pending commit snapshots`.

### Task 4: Publish fenced checkpoints and drain captured commits

**Files:**
- Modify: `crates/borsuk/src/collection_control.rs`
- Modify: `crates/borsuk/src/storage.rs`
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/src/group_commit.rs`
- Test: `crates/borsuk/tests/consistency.rs`
- Test: `crates/borsuk/tests/crash_recovery.rs`
- Test: `crates/borsuk/tests/group_commit.rs`

**Interfaces:**
- Produces: exact deletion proof from the already-published per-modality
  `Manifest.cell_wal_consumed_runs` fences,
  `BorsukIndex::checkpoint_pending(high_water: &BTreeSet<String>)`, and public
  `GroupCommitWriter::drain() -> Result<()>`.
- Consumes: Task 3's stable snapshot, existing WAL flush/segment materialization, conditional collection publication, and fenced deletion.

- [ ] **Step 1: Write RED checkpoint ordering tests**

  Inject crashes before catalog CAS, after catalog CAS, and during pending deletion. Require unpublished output to remain invisible, published output to contain every captured ID once, and deletion to refuse any transaction unless every captured run is present in the currently reloaded manifest fence for its modality. Retained old manifests must keep the committed transaction descriptor and payloads reachable after pending deletion.

- [ ] **Step 2: Write RED concurrent checkpointer and drain tests**

  Race two checkpointers over the same pending set; one catalog CAS wins and the loser does not delete. Capture drain high-water H, publish later commit L, and prove drain returns when H is fenced without claiming L.

- [ ] **Step 3: Run exact tests and verify RED**

  Expected: compile failure because checkpoint/fence/drain interfaces are absent.

- [ ] **Step 4: Implement one conditional checkpoint**

  Materialize the stable pending set with existing builders, CAS-publish the
  next collection snapshot, then reload that exact winner before deleting
  pending objects. The exact run identities already published in each
  modality's `cell_wal_consumed_runs` are the deletion fence; do not add an
  unbounded transaction-ID set to `CollectionSnapshot`. Treat a losing CAS as
  retryable coordination, never as permission to delete.

- [ ] **Step 5: Add writer lifecycle and cooperative triggering**

  Share checkpoint state across worker lanes. Trigger at pending count/bytes/age, serialize process-local helpers, and make `drain()` capture the caller-visible acknowledged IDs and join progress until they are fenced. At 1,000 observed pending objects, an append must help or wait before staging another group.

  Progress: explicit `GroupCommitWriter::drain()` is GREEN. It barriers every
  worker lane, refreshes one lane over the globally acknowledged pending set,
  root-authorizes the captured staged transactions in parallel, flushes them,
  and retires the exact run-fenced pending objects. The immutable pending PUT
  is now the acknowledgement boundary and no transaction `STATE` read or write
  remains on that path. A 600-group,
  four-lane regression reopens all records with zero pending objects. The
  benchmark records foreground ingest separately from `drain_ms` and drains
  before read qualification. Drain now also rebuilds an existing immutable
  global search artifact over the materialized delta, preventing base and delta
  from each consuming the full query segment budget. The v22 32-writer timeout
  exposed that rebuild's two full-segment reads as strictly sequential and 98%
  I/O-wait-bound. Both passes now consume deterministic, bounded eight-segment
  I/O waves; this retains one wave at a time and preserves artifact order while
  overlapping object-store latency. Cooperative 1,000-object admission remains
  open.

- [ ] **Step 6: Run correctness and lifecycle suites GREEN**

  Run group-commit, consistency, crash, fault, and cell-WAL suites plus strict Clippy.

- [ ] **Step 7: Commit checkpoint and drain**

  Commit as `feat: checkpoint and drain pending commits`.

### Task 5: Seal write epochs and make GC reachability complete

**Files:**
- Modify: `crates/borsuk/src/collection_control.rs`
- Modify: `crates/borsuk/src/storage.rs`
- Modify: `crates/borsuk/src/index.rs`
- Test: `crates/borsuk/tests/consistency.rs`
- Test: `crates/borsuk/tests/crash_recovery.rs`
- Test: `crates/borsuk/tests/gc.rs`

**Interfaces:**
- Produces: `WriteEpochState`, renewable `WriteEpochLease`, seal/drain/open transition, and pending/build reachability in GC.
- Consumes: store-clock probe, existing checked coordination writes, Task 4 drain and consumed fence.

- [ ] **Step 1: Write RED epoch race tests**

  Pause a writer with a live lease, attempt schema seal, and prove the schema transition cannot finish. Expire/release the lease, acknowledge its final pending commit, drain it, and prove the new epoch opens only after the old commit is catalogued.

- [ ] **Step 2: Write RED GC tests**

  Cover unacknowledged staged payloads, acknowledged pending payloads, losing checkpoint output, consumed-but-not-deleted pending objects, and old-reader catalog pins. Use a controllable store-clock fixture and literal grace boundaries.

  This is a hard AWS blocker. The obsolete mutable-root reservation tests were
  removed when ordinary mutations moved to immutable pending publication. Add
  replacement RED tests proving a live immutable staging/epoch lease protects
  pre-ACK payloads from concurrent GC and that abandoned staging becomes
  reclaimable from store-clock age. Do not launch qualification until these
  replacements are GREEN.

  Progress: pre-ACK staging reachability is GREEN. Root-authorized staging
  creates a transaction-scoped `Prepared` state before immutable uploads. GC
  obtains a server-assigned timestamp through a unique create/head/delete clock
  probe, protects prepared states for exactly the lease interval measured from
  their object-store `last_modified`, and reclaims an old abandoned payload
  even when its legacy client-encoded expiry remains in the future. Epoch
  seal/drain races and the complete checkpoint lifecycle remain open, so AWS
  qualification is still blocked.

- [ ] **Step 3: Run exact tests and verify RED**

  Expected: compile failure because epoch state and pending reachability are absent.

- [ ] **Step 4: Implement leases, sealing, and reachability**

  Acquire one checked epoch lease per writer, renew before half-TTL, reject publication after seal/expiry, make schema mutation seal then drain before changing fingerprints, and extend GC roots to pending descriptors, live builds, consumed fences, and pinned catalogs.

- [ ] **Step 5: Run consistency, crash, GC, and storage suites GREEN**

  Include fault injection at every state transition and strict Clippy.

- [ ] **Step 6: Commit epoch and GC safety**

  Commit as `feat: fence pending commits with write epochs`.

### Task 6: Remove v2 frontier format and qualify v3

**Files:**
- Modify: `crates/borsuk/src/collection_control.rs`
- Modify: `crates/borsuk/src/storage.rs`
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/tests/*`
- Modify: `crates/borsuk/examples/group_commit_bench.rs`
- Modify: `scripts/validate_group_commit_scalability.py`
- Modify: `scripts/test_validate_group_commit_scalability.py`
- Modify: `docs/api.md`
- Modify: `docs/consistency.md`
- Modify: `docs/research/group-commit-scalability-campaign.json`
- Modify: `docs/research/group-commit-scalability-attempt-ledger.md`

**Interfaces:**
- Produces: v3-only production format and a fresh fail-closed scalability campaign with pending/checkpoint telemetry.
- Consumes: Tasks 1--5 and the unchanged 2K/16K by 1/8/32 writer, five-repetition performance protocol.

- [ ] **Step 1: Delete obsolete frontier code and reject v2**

  Remove frontier shard codecs, reservations, carried leases, shard schedulers, snapshot double-collect, and legacy tests. Add one fixture proving v2 rejection names the unsupported collection codec clearly.

- [ ] **Step 2: Extend benchmark evidence and validator**

  Add pending LIST pages, backlog high-water, checkpoint count/duration/requests, and foreground request counts to raw samples/summary. Make the validator require finite fields, reconcile counts, enforce the 2,000 bound, and reject missing raw telemetry.

- [ ] **Step 3: Run the complete local release gate**

  Run formatting/diff checks, strict Clippy, all library/integration suites, validator unit tests, and a structural smoke that crosses at least one checkpoint and reopens with exact recall.

- [ ] **Step 4: Obtain adversarial review and correct only reproduced defects**

  Ask Claude to review the final diff read-only. Reproduce every material concern and add a RED regression before correction.

- [ ] **Step 5: Commit and fast-forward push**

  Commit docs, validator, and cutover as coherent slices. Before each push fetch `origin/main`, require it is an ancestor of `HEAD`, require a clean worktree, and push `HEAD:main` without force.

- [ ] **Step 6: Launch fresh isolated AWS qualification**

  Under `AWS_PROFILE=causality`, verify no competing workload, matching source/manifest hashes, fresh prefixes, and sufficient resources. Launch five paired repetitions. While incomplete inspect only terminal markers, service/process health, non-measurement progress, and resource telemetry; never inspect partial CSVs.

- [ ] **Step 7: Validate terminal evidence before claims**

  On terminal success, sync immutable artifacts and run the fail-closed validator before opening results. Require write p95 below 200 ms, at least 5 records/s/writer, read p95 below 200 ms, recall@1 1.0, bounded pending pages, and reconciled checkpoint/foreground requests for every matrix cell. On any failure, record the exact markers/evidence, stop claims, and return to root-cause TDD.

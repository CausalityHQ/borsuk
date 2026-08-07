# Direct Immutable Mutation Versions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the collection-wide S3 generation counter with locally allocated, deterministic convergent mutation versions so ordinary durable upsert/delete acknowledgement performs one immutable extent PUT per touched writer stripe.

**Architecture:** An internal 192-bit `MutationVersion` combines a 64-bit HLC prefix with a complete 128-bit writer identity. One `Arc<MutationClock>` belongs to a logical handle, and one canonical put/delete envelope carries the version and digest through WAL, materialization, sidecars, compaction, and reopen. The persistent cutover is atomic at standard-schema v30: hot immutable mutation extents are Arrow IPC, materialized tables are Parquet or Arrow IPC by access pattern, and control objects are versioned JSON. Old experimental indexes are rejected.

**Tech Stack:** Rust, Arrow/Parquet, `object_store`, immutable S3 extents, BLAKE3, UUID, existing fail-closed benchmark validators.

## Global Constraints

- S3 is the only durable service; do not add DynamoDB or a required RPC service.
- Normal upsert/delete acknowledgement performs no global generation-counter request, manifest publication, or materialization.
- Preserve one deterministic winner per ID and fail closed on equal-version unequal-digest mutations.
- Cross-host unobserved writes are convergent, not linearizable; do not retain tests or docs that claim acknowledgement order defines the winner.
- Mutation versions are internal and cannot be supplied by callers.
- One entity mutation is atomic across primary dense, named dense/sparse, text, and late-interaction data; multi-stripe batches retain explicit partial-durable-success receipts.
- Reject old experimental formats; do not add a legacy reader, migration path, or dual write.
- Persist only stock-readable Parquet, Arrow IPC, or versioned JSON. Do not add magic prefixes, bespoke outer frames, packed row files, opaque graph blobs, or hand-written binary controls.
- Use TDD, run one full gate only after focused layers are green, and fast-forward push each verified coherent slice directly to `origin/main`.
- Do not inspect incomplete AWS measurement CSVs.

## File structure

- Create `crates/borsuk/src/mutation.rs`: mutation version, HLC clock, canonical operation/stamp/digest, and range allocation.
- Modify `crates/borsuk/src/record.rs`: remove public generation state and carry an internal mutation stamp.
- Modify `crates/borsuk/src/format.rs`: standard-schema v30 typed mutation columns for Parquet materialized tables and Arrow IPC extents/sidecars.
- Modify `crates/borsuk/src/lane_log.rs`: Arrow IPC mutation extents, JSON controls, stable identities, and no generation bases.
- Modify `crates/borsuk/src/index.rs`: clock ownership, direct put/delete stamping, tombstone/ID overlays, refresh observation, and materialization merge.
- Modify `crates/borsuk/src/group_commit.rs`: allocate before dedup/fan-out and remove the global counter from acknowledgement.
- Modify `crates/borsuk/src/{arrow_vector_sidecar,global_pq_sidecar,late_interaction_sidecar,bm25,lexical_build,lexical_root}.rs`: preserve full logical versions through optimized artifacts.
- Modify `crates/borsuk/src/lib.rs`: internal module wiring and public documentation changes only where semantics are exposed.
- Modify `crates/borsuk/tests/{group_commit,consistency,fault_injection,vector_encoding}.rs`: multi-writer convergence, request counts, ambiguous PUT, and format lifecycle coverage.

---

### Task 1: Add the mutation version and clock foundation

**Files:**
- Create: `crates/borsuk/src/mutation.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: `crates/borsuk/src/mutation.rs`

**Interfaces:**
- Produces: `MutationVersion { hlc: u64, writer: [u8; 16] }` with a total-order in-memory 24-byte big-endian comparison key; this is not a persisted file codec.
- Produces: `MutationClock::new(writer)`, `allocate_range_at(now_ms, count)`, and `observe(version)`.
- Produces: `MutationVersionRange::at(ordinal)`.

- [ ] **Step 1: Write RED ordering and codec tests**

  Add unit tests that require this interface:

  ```rust
  let writer = [7_u8; 16];
  let clock = MutationClock::new(writer);
  let range = clock.allocate_range_at(1_000, 3).unwrap();
  assert!(range.at(0).unwrap() < range.at(1).unwrap());
  assert_eq!(MutationVersion::from_bytes(range.at(2).unwrap().to_bytes()).unwrap(), range.at(2).unwrap());
  assert_eq!(range.at(2).unwrap().to_bytes().cmp(&range.at(1).unwrap().to_bytes()), Ordering::Greater);
  ```

  Add cases for zero count, pre-epoch input, `2^48` physical overflow, range overflow, and trailing/short canonical bytes.

- [ ] **Step 2: Verify RED**

  Run: `rtk cargo test -p borsuk --lib mutation::tests -- --nocapture`

  Expected: compilation fails because `mutation` and its types do not exist.

- [ ] **Step 3: Add RED HLC causality and concurrency tests**

  Require rollback monotonicity, 65,536-value logical carry, disjoint allocation from 32 threads, and the critical observed-prefix case:

  ```rust
  let observed = MutationVersion::from_parts((1_000 << 16) | 60_000, [9; 16]).unwrap();
  clock.observe(observed).unwrap();
  assert!(clock.allocate_range_at(1_000, 1).unwrap().at(0).unwrap() > observed);
  ```

- [ ] **Step 4: Implement the minimal core**

  Use an `AtomicU64` for the complete HLC prefix and CAS a contiguous range:

  ```rust
  pub(crate) struct MutationClock {
      prefix: AtomicU64,
      writer: [u8; 16],
  }

  pub(crate) fn allocate_range_at(&self, now_ms: u64, count: usize) -> Result<MutationVersionRange>;
  pub(crate) fn observe(&self, version: MutationVersion) -> Result<()>;
  ```

  Treat `(now_ms << 16)` as the first prefix only when it is greater than the observed/local prefix; otherwise allocate from `floor + 1`. Use checked arithmetic and never compare writer identity when advancing the HLC floor.

- [ ] **Step 5: Verify GREEN and quality**

  Run: `rtk cargo test -p borsuk --lib mutation::tests -- --nocapture`

  Run: `rtk cargo fmt --all -- --check`

  Run: `rtk cargo clippy -p borsuk --lib --all-features -- -D warnings`

- [ ] **Step 6: Commit and fast-forward push**

  Commit: `storage: add convergent mutation clocks`

### Task 2: Atomically cut all record persistence to mutation stamps

**Files:**
- Modify: `crates/borsuk/src/mutation.rs`
- Modify: `crates/borsuk/src/record.rs`
- Modify: `crates/borsuk/src/format.rs`
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/src/arrow_vector_sidecar.rs`
- Modify: `crates/borsuk/src/global_pq_sidecar.rs`
- Modify: `crates/borsuk/src/late_interaction_sidecar.rs`
- Modify: `crates/borsuk/src/bm25.rs`
- Modify: `crates/borsuk/src/lexical_build.rs`
- Modify: `crates/borsuk/src/lexical_root.rs`
- Test: unit tests in each modified module
- Test: `crates/borsuk/tests/vector_encoding.rs`

**Interfaces:**
- Produces: `MutationStamp { version: MutationVersion, digest: [u8; 32] }` and `CanonicalMutation::{Put, Delete}`.
- Produces: stock-readable `mutation_hlc`, `mutation_writer`, and `mutation_digest` logical fields; native Arrow/Parquet implementations may apply standard dictionary encoding.
- Changes: table `CURRENT_VERSION` from 29 to 30 and rejects v29.

- [x] **Step 1: Write RED canonical-envelope tests**

  Construct logically equal records with different map insertion order and require equal canonical digests. Change every field—primary vector, named vector, sparse values, text terms, late-interaction tokens, metadata, storage declaration, and operation tag—and require a different digest. Require `Put` and `Delete` for one ID/version to conflict.

- [ ] **Step 2: Write RED format round-trip tests**

  Require segment, WAL, Arrow exact, global PQ, lexical, BM25, and late-interaction artifacts to round-trip two versions sharing one HLC but using different writer identities through official Parquet/Arrow readers. Assert artifact `max_mutation_version` equals the semantic maximum and typed columns reconstruct the semantic values.

- [ ] **Step 3: Verify RED**

  Run the exact affected module tests:

  `rtk cargo test -p borsuk --lib mutation::tests -- --nocapture`

  `rtk cargo test -p borsuk --lib format::tests -- --nocapture`

  `rtk cargo test -p borsuk --lib arrow_vector_sidecar::tests -- --nocapture`

  `rtk cargo test -p borsuk --lib global_pq_sidecar::tests -- --nocapture`

  `rtk cargo test -p borsuk --lib late_interaction_sidecar::tests -- --nocapture`

  `rtk cargo test -p borsuk --lib bm25::tests -- --nocapture`

  `rtk cargo test -p borsuk --lib lexical_root::tests -- --nocapture`

  Expected: compilation failures for missing mutation stamps/columns.

- [ ] **Step 4: Implement the internal envelope and public API break**

  Remove `VectorRecord::generation`. Add a crate-private stamp initialized to zero by constructors and excluded from caller serde input. Canonicalization must produce:

  ```rust
  pub(crate) enum MutationOperation { Put(VectorRecord), Delete }
  pub(crate) struct CanonicalMutation {
      pub(crate) id: Vec<u8>,
      pub(crate) stamp: MutationStamp,
      pub(crate) operation: MutationOperation,
  }
  ```

  Callers cannot mutate the stamp. Equal version plus unequal digest returns `BorsukError::InvalidStorage` at every merge boundary.

  The clock and canonical envelope foundations were delivered in `b7246e7`
  and `f737363`. The large put payload is stored beside a compact operation tag
  rather than boxed solely to equalize enum variant sizes.

- [ ] **Step 5: Implement v30 standard columnar layouts**

  Store `mutation_hlc: UInt64`, `mutation_writer: FixedSizeBinary(16)`, and `mutation_digest: FixedSizeBinary(32)` as logical Arrow/Parquet columns. Use Arrow IPC for foreground mutation extents to minimize encoding and footer overhead; use Parquet after materialization when compression and scans amortize its cost. Native writers may dictionary-encode columns. Persist the maximum semantic version in documented schema metadata and standard statistics. Replace packed global PQ rows with Arrow IPC typed arrays and binary lexical controls with Parquet/JSON. Reject old schemas instead of defaulting a missing version column to zero.

  The terminal local qualification at `cc518dd` selected uncompressed Arrow IPC
  streams for foreground extents. See
  `docs/research/mutation-extent-standard-format-qualification.md`; this is a
  codec decision only, not an end-to-end latency claim.

- [ ] **Step 6: Replace tombstone and ID-directory generations**

  Change `TombstoneOverlay`, `LiveDeleteRecord`, `CellWalIdDirectoryEntry`, live-WAL indexes, generation fences, and compaction comparisons to `MutationVersion`/`MutationStamp`. Deletes participate in the same greatest-version merge as puts. Foreground delete reports accepted durable mutations, while exact corpus counts remain materialized statistics.

- [ ] **Step 7: Verify GREEN across persistence**

  Run the RED command, then:

  `rtk cargo test -p borsuk --test vector_encoding -- --nocapture`

  `rtk cargo test -p borsuk --test consistency -- --nocapture`

  `rtk cargo fmt --all -- --check`

  Do not commit if the old record/row field remains in record-bearing artifacts:

  `! rtk rg -n 'record\.generation|row\.generation|row_generation\([^)]*\) -> Option<u64>' crates/borsuk/src/{format,arrow_vector_sidecar,global_pq_sidecar,late_interaction_sidecar,bm25,lexical_build,lexical_root,index,lane_log}.rs`

- [ ] **Step 8: Commit and fast-forward push**

  Commit: `storage: persist canonical mutation versions`

### Task 3: Remove the global counter from direct and grouped mutation

**Files:**
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/src/group_commit.rs`
- Modify: `crates/borsuk/src/lane_log.rs`
- Modify: `crates/borsuk/src/format.rs`
- Test: `crates/borsuk/tests/group_commit.rs`
- Test: `crates/borsuk/tests/fault_injection.rs`

**Interfaces:**
- Produces: an Arrow IPC mutation extent with stable `(stripe, lease_epoch, sequence)`, typed mutation payload/version/digest columns, and max-version schema metadata.
- Removes: `reserve_lane_log_generation_range`, `reserve_record_generations`, and `id-directory/last-write-wins/NEXT` from ordinary/group mutation.

- [ ] **Step 1: Write RED request-count tests**

  Replace `repeated_groups_use_one_generation_cas_and_one_extent` with `repeated_groups_use_one_extent_and_no_generation_coordination`. For 12 groups require exactly 12 extent PUTs and zero operations whose path is `id-directory/last-write-wins/NEXT`. Require each normal receipt to report one PUT, zero GET/HEAD/LIST, and its exact extent identity.

- [ ] **Step 2: Write RED convergence and documented-skew tests**

  Replace acknowledgement-order assertions with deterministic version assertions. Inject clocks so two independent writers conflict in both stripe orders, reopen/drain/compact, and require the same winner. Add a permanent case where a later acknowledged but unobserved clock-skewed write loses, proving the documented non-linearizable contract rather than accidentally preserving the old test.

- [ ] **Step 3: Write RED ambiguous-PUT tests**

  Inject accept-then-timeout. Require the stripe to block later work, read the exact same extent key, validate identical checksum/bytes, return the original extent identity, and only then allocate the next sequence. Inject unequal existing bytes and require fencing failure.

- [ ] **Step 4: Verify RED**

  Run:

  `rtk cargo test -p borsuk --test group_commit no_generation_coordination -- --nocapture`

  `rtk cargo test -p borsuk --test fault_injection mutation_extent -- --nocapture`

- [ ] **Step 5: Implement clock ownership and front-end allocation**

  Add `Arc<MutationClock>` and an `Arc<Mutex<Option<LaneEpochWriter>>>` direct-mutation writer to `BorsukIndex`; ordinary clones share them and independent opens/reset writers receive a new identity/stripe. Route ordinary `put` and `delete` batches through that lazily claimed stripe, so they use the same direct extent protocol as group commit. Transfer the consumed clock into `GroupCommitWriter`. Allocate stamps before dedup/fan-out. Worker stripes receive already canonicalized mutations and never rewrite versions.

- [ ] **Step 6: Implement standard-format extents and delete the counter**

  Replace `first_generation`/`generation_end` with typed Arrow IPC mutation fields and documented schema metadata. Keep stable sequence-addressed create-only keys and exact checksum reconciliation. Store heads/directories as versioned JSON. Remove the counter path and its startup floor read entirely. Increment the standard schema marker and reject earlier custom extents.

- [ ] **Step 7: Verify GREEN and lifecycle safety**

  Run both RED commands, then:

  `rtk cargo test -p borsuk --test group_commit -- --nocapture`

  `rtk cargo test -p borsuk --test fault_injection -- --nocapture`

  `rtk cargo test -p borsuk --test consistency -- --nocapture`

  Require this source scan to be empty outside immutable historical docs/tests:

  `rtk rg -n 'last-write-wins/NEXT|reserve_lane_log_generation_range|reserve_record_generations' crates/borsuk/src`

- [ ] **Step 8: Commit and fast-forward push**

  Commit: `ingest: acknowledge direct convergent extents`

### Task 4: Make one mutation envelope atomic across modalities

**Files:**
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/src/group_commit.rs`
- Modify: `crates/borsuk/src/format.rs`
- Modify: `crates/borsuk/src/lane_log.rs`
- Test: `crates/borsuk/tests/named_vectors.rs`
- Test: `crates/borsuk/tests/sparse_named_vectors.rs`
- Test: `crates/borsuk/tests/text_storage.rs`
- Test: `crates/borsuk/tests/late_interaction_index.rs`
- Test: `crates/borsuk/tests/group_commit.rs`

**Interfaces:**
- Removes: `ensure_lane_log_payloads_supported` rejection for text/named modalities.
- Produces: one entity envelope whose derived modality manifests publish through one collection-root commit.

- [ ] **Step 1: Write RED atomic-modality tests**

  Commit one entity containing primary dense, named dense, sparse, text, metadata, and late-interaction values. Inject failure while building each derived sidecar and require no modality manifest to publish. On success require every modality to expose one ID/version/digest and exact values after reopen.

- [ ] **Step 2: Write RED conflict tests**

  Independently upsert the same ID with different complete entity payloads. Require the winning version to select the complete entity; no named/text/late field may leak from the losing version. A versioned delete suppresses all modalities together.

- [ ] **Step 3: Verify RED**

  Run:

  `rtk cargo test -p borsuk --test group_commit multimodal -- --nocapture`

  `rtk cargo test -p borsuk --test late_interaction mutation_version -- --nocapture`

- [ ] **Step 4: Implement collection mutation materialization**

  Preserve all modality data in the one canonical WAL envelope. Build primary, exact, named dense/sparse, lexical, and late-interaction artifacts from that envelope with the parent stamp. Stage all child manifests and publish one root descriptor/collection snapshot only after every checksum validates. Keep multi-stripe partial receipts explicit.

- [ ] **Step 5: Verify GREEN and the complete modality matrix**

  Run the RED commands, then all four modality test binaries listed above and `rtk cargo test -p borsuk --test consistency`.

- [ ] **Step 6: Commit and fast-forward push**

  Commit: `storage: commit atomic multimodal mutations`

### Task 5: Update qualification contracts and run the full gate

**Files:**
- Modify: `crates/borsuk/examples/group_commit_bench.rs`
- Modify: `scripts/bench_group_commit_scalability.sh`
- Modify: `scripts/validate_group_commit_scalability.py`
- Modify: `scripts/test_validate_group_commit_scalability.py`
- Modify: `docs/research/group-commit-scalability-attempt-ledger.md`
- Modify: `docs/api.md`

**Interfaces:**
- Produces: raw version semantics, extent identities, zero-counter request reconciliation, conflict outcome, and per-modality visibility evidence.

- [ ] **Step 1: Write RED validator tests**

  Reject any completed cell that reports a global-counter request, lacks extent identities/digests, uses caller-supplied versions, claims cross-host linearizable acknowledgement order, or omits modality visibility. Preserve the existing PID, raw/summary, marker, recall, latency, and resource checks.

- [ ] **Step 2: Verify RED and implement the harness contract**

  Run: `rtk python3 -m unittest scripts.test_validate_group_commit_scalability`

  Add the raw fields and fail-closed checks, then rerun until green.

- [ ] **Step 3: Run a local 768D structural smoke**

  Run 1/8 distinct local processes against one object-store prefix, validate terminal artifacts, and require zero global-counter requests, exact visibility, deterministic conflicts, and recall 1.0. Timings remain diagnostic.

- [ ] **Step 4: Run one full repository assurance gate**

  Run formatting, repository policy, web docs, strict all-feature/all-target Clippy, Python suites, and `rtk cargo test --locked --workspace --all-targets` once. Rerun only a failing layer until repaired, then one final full gate.

- [ ] **Step 5: Commit and fast-forward push**

  Commit: `bench: qualify direct convergent ingest`

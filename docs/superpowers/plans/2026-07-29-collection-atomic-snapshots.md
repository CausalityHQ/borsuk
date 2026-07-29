# Collection-Atomic Snapshots Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make manifests and foreground WAL mutations atomically visible across the primary, named dense, sparse/text, and late-interaction modalities.

**Architecture:** Add checked root collection snapshot and transaction records, then make root publication the only visibility boundary. Modality manifests and WAL descriptors remain immutable and independently prepared, while open, refresh, mutation, flush, and compaction install or publish only complete collection state.

**Tech Stack:** Rust, `object_store` conditional writes, BLAKE3 checked binary records, Parquet/Vortex immutable tables, cell-sharded WAL, integration fault injection.

---

## File map

- Create `crates/borsuk/src/collection_control.rs`: collection paths, canonical
  control types, checked codecs, schema fingerprinting, and unit tests.
- Modify `crates/borsuk/src/lib.rs`: register the internal control module.
- Modify `crates/borsuk/src/storage.rs`: stage exact manifest objects; load exact
  checksum-pinned manifests; publish/load root snapshot and root commits.
- Modify `crates/borsuk/src/cell_wal.rs`: separate descriptor fencing from local
  visibility and load a descriptor by exact root-authorized reference.
- Modify `crates/borsuk/src/index.rs`: own one root snapshot, construct children
  from exact references, prepare collection transactions, and atomically install
  refresh/maintenance results.
- Modify `crates/borsuk/tests/fault_injection.rs`: mutation visibility cut-point
  coverage.
- Modify `crates/borsuk/tests/crash_recovery.rs`: reopen and post-root-commit
  recovery coverage.
- Modify `crates/borsuk/tests/named_vectors.rs`: collection snapshot and named
  dense atomicity.
- Modify `crates/borsuk/tests/late_interaction_index.rs`: replacement transaction
  atomicity.
- Modify `crates/borsuk/tests/storage_access_trace.rs`: bounded root coordination
  request assertion.

### Task 1: Checked collection control records

**Files:**
- Create: `crates/borsuk/src/collection_control.rs`
- Modify: `crates/borsuk/src/lib.rs`

- [ ] **Step 1: Write codec round-trip and rejection tests**

Add unit tests covering a snapshot with `@primary`, `dense`, and `late`
manifest references and a commit with two descriptor references:

```rust
#[test]
fn collection_snapshot_round_trips_canonical_modalities() {
    let snapshot = sample_snapshot();
    let bytes = collection_snapshot_bytes(&snapshot).unwrap();
    assert_eq!(
        collection_snapshot_from_slice(&bytes, "collection/snapshots/test.bin").unwrap(),
        snapshot
    );
}

#[test]
fn collection_snapshot_rejects_non_canonical_modalities() {
    let mut snapshot = sample_snapshot();
    snapshot.modalities.swap(0, 1);
    let error = collection_snapshot_bytes(&snapshot).unwrap_err();
    assert!(error.to_string().contains("canonical modality order"));
}

#[test]
fn collection_control_rejects_damage_and_trailing_bytes() {
    let bytes = collection_snapshot_bytes(&sample_snapshot()).unwrap();
    for damaged in [
        bytes[..bytes.len() - 1].to_vec(),
        {
            let mut value = bytes.clone();
            value[8] ^= 1;
            value
        },
        {
            let mut value = bytes;
            value.push(0);
            value
        },
    ] {
        assert!(collection_snapshot_from_slice(&damaged, "damaged").is_err());
    }
}
```

- [ ] **Step 2: Run the new module test and verify it fails**

Run:

```bash
cargo test -p borsuk collection_control --lib
```

Expected: compilation fails because `collection_control` and its record types do
not exist.

- [ ] **Step 3: Implement canonical types and checked codecs**

Define:

```rust
pub(crate) const PRIMARY_MODALITY: &str = "@primary";
pub(crate) const COLLECTION_CURRENT: &str = "collection/CURRENT";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CollectionManifestRef {
    pub modality: String,
    pub prefix: String,
    pub version: u64,
    pub manifest_path: String,
    pub manifest_checksum: String,
    pub routing_path: String,
    pub routing_checksum: String,
    pub pivots_path: String,
    pub pivots_checksum: String,
    pub consumed_wal_frontier_checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CollectionSnapshot {
    pub generation: u64,
    pub schema_fingerprint: String,
    pub previous_snapshot_checksum: Option<String>,
    pub modalities: Vec<CollectionManifestRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CollectionDescriptorRef {
    pub modality: String,
    pub prefix: String,
    pub descriptor_path: String,
    pub descriptor_checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CollectionCommit {
    pub transaction_id: String,
    pub snapshot_generation: u64,
    pub schema_fingerprint: String,
    pub descriptors: Vec<CollectionDescriptorRef>,
}
```

Use distinct four-byte magic values, version `1`, little-endian length-prefixed
strings, and a trailing 32-byte BLAKE3 checksum. Encoding and decoding both call
one validator that enforces the primary modality first, remaining modalities
in bytewise order, no duplicates, 64 lowercase hexadecimal checksums, safe
relative prefixes, and transaction-ID syntax matching the cell WAL.

- [ ] **Step 4: Run codec tests**

Run:

```bash
cargo test -p borsuk collection_control --lib
```

Expected: all collection-control tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/borsuk/src/collection_control.rs crates/borsuk/src/lib.rs
git commit -m "feat: add checked collection control records"
```

### Task 2: Stage and load exact manifest references

**Files:**
- Modify: `crates/borsuk/src/storage.rs`
- Modify: `crates/borsuk/src/collection_control.rs`

- [ ] **Step 1: Write storage tests for exact manifest loading**

Add storage unit tests that stage two manifest versions, pin the first returned
reference, publish the second through the legacy pointer, and assert
`load_manifest_ref(&first, true)` still returns version one. Corrupt the pinned
routing bytes and assert `ChecksumMismatch`.

- [ ] **Step 2: Run the exact-load tests and verify failure**

Run:

```bash
cargo test -p borsuk storage::tests::exact_manifest --lib
```

Expected: compilation fails because `stage_manifest` and `load_manifest_ref` do
not exist.

- [ ] **Step 3: Extract manifest staging from legacy publication**

Implement:

```rust
pub(crate) struct StagedManifest {
    pub manifest: Manifest,
    pub reference: CollectionManifestRef,
}

pub(crate) fn stage_manifest(
    &self,
    modality: &str,
    manifest: &Manifest,
    previous: Option<&Manifest>,
) -> Result<StagedManifest>;

pub(crate) fn load_manifest_ref(
    &self,
    reference: &CollectionManifestRef,
    resident_routing: bool,
) -> Result<Manifest>;
```

`stage_manifest` writes immutable manifest, routing, and pivots tables and
returns their BLAKE3 checksums without writing `CURRENT`. Existing
`publish_manifest*` calls this method and then writes the legacy pointer until
Task 3 removes legacy collection publication. `load_manifest_ref` reads all
three exact versioned paths through checksum-aware storage reads, decodes the
full manifest when `resident_routing` is true, and otherwise decodes manifest
metadata after validating the routing and pivot checksums.

- [ ] **Step 4: Run storage and format tests**

Run:

```bash
cargo test -p borsuk storage::tests::exact_manifest --lib
cargo test -p borsuk format --lib
```

Expected: both commands pass.

- [ ] **Step 5: Commit**

```bash
git add crates/borsuk/src/storage.rs crates/borsuk/src/collection_control.rs
git commit -m "feat: stage and load exact collection manifests"
```

### Task 3: Root snapshot creation and exact reopen

**Files:**
- Modify: `crates/borsuk/src/storage.rs`
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/tests/named_vectors.rs`

- [ ] **Step 1: Write the root-only reopen test**

Create a primary plus named dense collection, delete every modality-local
`CURRENT`, reopen it, and verify both searches return the inserted ID. Also
assert that deleting `collection/CURRENT` produces `IndexNotFound`.

- [ ] **Step 2: Run the test and verify failure**

Run:

```bash
cargo test -p borsuk --test named_vectors collection_snapshot_reopens_without_child_current
```

Expected: reopen fails because current code still reads modality-local
`CURRENT`.

- [ ] **Step 3: Add root snapshot storage operations**

Implement:

```rust
pub(crate) struct LoadedCollectionSnapshot {
    pub snapshot: CollectionSnapshot,
    pub checksum: String,
    pub current_version: UpdateVersion,
}

pub(crate) fn create_collection_snapshot(
    &self,
    snapshot: &CollectionSnapshot,
) -> Result<LoadedCollectionSnapshot>;

pub(crate) fn load_collection_snapshot(&self) -> Result<LoadedCollectionSnapshot>;

pub(crate) fn compare_and_swap_collection_snapshot(
    &self,
    expected: UpdateVersion,
    snapshot: &CollectionSnapshot,
) -> Result<LoadedCollectionSnapshot>;
```

Immutable snapshot paths include the body checksum. `collection/CURRENT` stores
that path and checksum in a checked pointer. Creation uses `PutMode::Create`;
replacement uses the exact version returned by the prior read.

- [ ] **Step 4: Construct all modalities before root publication**

Add `collection_snapshot: LoadedCollectionSnapshot` to `BorsukIndex`. During
create, stage all primary/child manifests, validate exact references, and then
publish the first root snapshot. During open, load root truth first and build
the primary and every child from its exact manifest reference. Reject a missing,
extra, or kind-incompatible named modality before returning a handle.

- [ ] **Step 5: Run creation/reopen tests**

Run:

```bash
cargo test -p borsuk --test named_vectors
cargo test -p borsuk --test local_index
```

Expected: both suites pass and no collection creation writes child `CURRENT`.

- [ ] **Step 6: Commit**

```bash
git add crates/borsuk/src/storage.rs crates/borsuk/src/index.rs crates/borsuk/tests/named_vectors.rs
git commit -m "feat: open collections from one atomic snapshot"
```

### Task 4: Root-authorized foreground WAL transactions

**Files:**
- Modify: `crates/borsuk/src/cell_wal.rs`
- Modify: `crates/borsuk/src/storage.rs`
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/tests/fault_injection.rs`
- Modify: `crates/borsuk/tests/crash_recovery.rs`

- [ ] **Step 1: Write pre-root and post-root failure tests**

Use a failpoint object store to fail after the primary descriptor, after each
named descriptor, immediately before the root commit, and immediately after the
root commit. Before-root failures must expose none of the new ID after reopen;
the after-root failure must expose it in all modalities after reopen.

- [ ] **Step 2: Run focused failure tests and verify failure**

Run:

```bash
cargo test -p borsuk --test fault_injection collection_transaction
cargo test -p borsuk --test crash_recovery collection_transaction
```

Expected: at least the failure between primary and named publication exposes a
partial logical row.

- [ ] **Step 3: Split cell-WAL fence and local commit**

Add:

```rust
pub(crate) struct FencedCellWalTransaction {
    pub prepared: PreparedCellWalTransaction,
    pub state_version: UpdateVersion,
}

pub(crate) fn fence_prepared(
    &self,
    prepared: &PreparedCellWalTransaction,
) -> Result<FencedCellWalTransaction>;

pub(crate) fn mark_root_committed(
    &self,
    fenced: &FencedCellWalTransaction,
) -> Result<CommittedCellWalTransaction>;

pub(crate) fn load_authorized_descriptor(
    &self,
    reference: &CollectionDescriptorRef,
) -> Result<CommittedCellWalTransaction>;
```

`fence_prepared` validates the descriptor and advances local state to
`committing` without creating a local visibility marker.
`mark_root_committed` best-effort advances local state after root publication.
`load_authorized_descriptor` validates exact descriptor path, checksum,
transaction ID, modality prefix, and every run.

- [ ] **Step 4: Publish one root commit**

Prepare every participating modality under one transaction ID, fence all of
them, canonicalize descriptor references, and create
`collection/transactions/<id>/COMMIT`. A create conflict reloads and bytewise
compares the existing checked commit. Install all committed descriptors in the
handle only after root publication succeeds.

- [ ] **Step 5: Authorize WAL reads from root commits**

Replace child-local commit-marker admission with the root descriptor map for
collection indexes. Prepared frontier runs absent from the root map are
ignored. A root reference whose descriptor or run cannot be validated returns
`InvalidStorage`.

- [ ] **Step 6: Run mutation and recovery suites**

Run:

```bash
cargo test -p borsuk --test fault_injection collection_transaction
cargo test -p borsuk --test crash_recovery collection_transaction
cargo test -p borsuk --test upsert
cargo test -p borsuk --test wal
cargo test -p borsuk --test cell_wal
```

Expected: all commands pass.

- [ ] **Step 7: Commit**

```bash
git add crates/borsuk/src/cell_wal.rs crates/borsuk/src/storage.rs crates/borsuk/src/index.rs crates/borsuk/tests/fault_injection.rs crates/borsuk/tests/crash_recovery.rs
git commit -m "feat: commit multimodal writes through root truth"
```

### Task 5: Atomic late-interaction replacement

**Files:**
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/tests/late_interaction_index.rs`

- [ ] **Step 1: Write replacement cut-point test**

Upsert one entity from token matrix A to matrix B through a failpoint between
record-run and tombstone-run preparation. Reopen after every cut point and
assert exactly one complete entity version is searchable, never old plus new
tokens or neither.

- [ ] **Step 2: Run the test and verify failure**

Run:

```bash
cargo test -p borsuk --test late_interaction_index replacement_is_one_collection_transaction
```

Expected: current add-then-delete behavior fails a cut-point assertion.

- [ ] **Step 3: Build one child mutation**

Replace the sequential child `upsert` plus `delete_with_report` calls with one
prepared child transaction containing new token record runs and old token
tombstone/ID-directory runs. Pin that child descriptor in the same root commit
as the primary entity mutation.

- [ ] **Step 4: Run late-interaction and multimodal suites**

Run:

```bash
cargo test -p borsuk --test late_interaction_index
cargo test -p borsuk --test feature_matrix
cargo test -p borsuk --test hybrid_search
```

Expected: all commands pass.

- [ ] **Step 5: Commit**

```bash
git add crates/borsuk/src/index.rs crates/borsuk/tests/late_interaction_index.rs
git commit -m "fix: replace late-interaction entities atomically"
```

### Task 6: Prepare-then-swap refresh

**Files:**
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/tests/fault_injection.rs`

- [ ] **Step 1: Write refresh cut-point tests**

Inject failures while loading the primary reference, each child reference,
root-authorized WAL descriptors, and derived routing data. After every failure,
query the existing handle and assert it retains the complete prior generation.

- [ ] **Step 2: Run the test and verify failure**

Run:

```bash
cargo test -p borsuk --test fault_injection refresh_keeps_complete_old_collection
```

Expected: a child refresh failure leaves the primary advanced.

- [ ] **Step 3: Add prepared collection state**

Introduce an internal `PreparedCollectionState` containing the loaded root
snapshot, primary manifest/WAL state, and a complete named-index map. Build it
without mutating `self`; then use one `install_prepared_collection_state`
method to replace all collection fields and invalidate version-keyed caches.

- [ ] **Step 4: Run refresh and consistency suites**

Run:

```bash
cargo test -p borsuk --test fault_injection refresh_keeps_complete_old_collection
cargo test -p borsuk --test consistency
cargo test -p borsuk --test concurrency_stress
```

Expected: all commands pass.

- [ ] **Step 5: Commit**

```bash
git add crates/borsuk/src/index.rs crates/borsuk/tests/fault_injection.rs
git commit -m "fix: refresh complete collection snapshots atomically"
```

### Task 7: Collection-atomic flush and compaction

**Files:**
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/tests/crash_recovery.rs`
- Modify: `crates/borsuk/tests/paged_delete_compaction.rs`

- [ ] **Step 1: Write maintenance publication tests**

Fail after staging each replacement modality manifest and before/after the root
snapshot CAS. Reopen and assert the collection uses either all old manifests or
all new manifests while every foreground commit beyond the consumed frontier
remains visible.

- [ ] **Step 2: Run focused tests and verify failure**

Run:

```bash
cargo test -p borsuk --test crash_recovery collection_flush_snapshot
cargo test -p borsuk --test paged_delete_compaction collection_compaction_snapshot
```

Expected: sequential child publication exposes mixed manifest generations.

- [ ] **Step 3: Stage maintenance outputs and CAS once**

Make primary/child flush and compaction return `StagedManifest` plus consumed
frontier metadata without writing a pointer. Validate every staged reference,
create the next `CollectionSnapshot`, and CAS root `CURRENT` once. On a CAS
loss, reload root truth and leave the staged immutable objects unreachable.

- [ ] **Step 4: Run all maintenance suites**

Run:

```bash
cargo test -p borsuk --test crash_recovery
cargo test -p borsuk --test paged_delete_compaction
cargo test -p borsuk --test production_workload
```

Expected: all commands pass.

- [ ] **Step 5: Commit**

```bash
git add crates/borsuk/src/index.rs crates/borsuk/tests/crash_recovery.rs crates/borsuk/tests/paged_delete_compaction.rs
git commit -m "feat: publish maintenance as one collection snapshot"
```

### Task 8: Request-bound and full production verification

**Files:**
- Modify: `crates/borsuk/tests/storage_access_trace.rs`
- Modify: `docs/research/production-hardening-audit-2026-07-28.md`

- [ ] **Step 1: Assert bounded atomicity overhead**

Trace a 500-row mutation spanning primary and two named modalities. Assert the
root transaction emits exactly one create PUT, descriptor-validation reads are
bounded by three modalities, and no root protocol request count depends on row
count.

- [ ] **Step 2: Run the request test**

Run:

```bash
cargo test -p borsuk --test storage_access_trace collection_commit_overhead_is_modality_bounded
```

Expected: pass with the exact request-count assertions.

- [ ] **Step 3: Run repository verification**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -j2 -- -D warnings
cargo test --locked --workspace --all-targets -j2
BORSUK_TEST_BUILD_JOBS=2 bash scripts/check_rust_test_build.sh
.borsuk-scratch/methodology-venv/bin/python -m unittest discover -s scripts -p 'test_*.py'
```

Expected: every command exits zero.

- [ ] **Step 4: Update the audit with verified evidence**

Mark only the collection-atomicity P0 closed. Record the exact commands, test
names, commit SHA, and request-count result. Leave WAL memory amplification and
cell-by-lane coordination scans open for their following plans.

- [ ] **Step 5: Commit**

```bash
git add crates/borsuk/tests/storage_access_trace.rs docs/research/production-hardening-audit-2026-07-28.md
git commit -m "test: verify collection atomicity production gate"
```

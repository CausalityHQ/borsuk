# Gate-Free Explicit-ID Claims Design

**Status:** approved by the existing production-readiness directive; the user
explicitly requested autonomous continuation without questions.

## Goal

Remove the collection-wide explicit-ID claim `GATE` so disjoint writers can
coordinate independently through the fixed claim shards, without weakening
duplicate detection, crash recovery, or version-safe rollback.

## Current behavior

`CellWalStore::claim_ids` hashes every caller-owned ID into one of the fixed
claim shards. `acquire_claim_shards` then acquires a collection-wide `GATE`,
tries all required shard locks, and releases the gate. This prevents deadlock
but serializes every explicit-ID batch even when two batches use disjoint
shards.

The shard locks already carry the owning transaction ID and object version.
Rollback writes an `Available` value only against the exact owned version, and
an abandoned owner can be reclaimed only through its durable transaction
state. Those mechanisms are sufficient for safe shard-local coordination.

## Considered approaches

### Retain the global gate

This is the lowest implementation risk, but it preserves the known
collection-wide write bottleneck and cannot meet the concurrent-writer target.

### Parallel try-all without the gate

All shard lock attempts can run concurrently, with partial acquisitions
released before retry. This minimizes uncontended wall time, but two overlapping
writers can repeatedly acquire complementary subsets. Random backoff reduces
but does not structurally eliminate that livelock pattern.

### Deterministic ascending shard acquisition

Every writer sorts and deduplicates its shard IDs, then acquires them one at a
time in ascending order. On contention or error it version-safely releases its
partial set, backs off, and retries from the first shard. Because all writers
use the same total order, circular wait is impossible. Disjoint batches never
touch a shared coordination object.

This is the selected design.

## Data flow

1. Validate or create the transaction's durable `Prepared` state.
2. Hash the batch IDs to the existing fixed shard set.
3. Iterate the sorted shard paths.
4. For each path, call the existing `try_acquire_claim`.
5. If all paths are acquired, return the existing `CellWalClaimGuard`.
6. On contention, release every partial lock with the existing version-fenced
   `release_claims`, apply deterministic jitter, and retry.
7. On a storage or corruption error, release partial locks and return the
   original error.
8. Commit, abort, guard drop, checkpoint validation, and expired-owner
   reclamation remain unchanged.

The `id-directory/claim-shards/GATE` path and its helper are removed. Existing
indexes require no migration because the gate was transient coordination, not
reader-visible state. A stale historical gate object is ignored.

## Correctness invariants

- Two insert-only writers for the same ID still produce exactly one commit.
- An external writer still invalidates a local claim-shard checkpoint.
- A failed multi-ID batch releases all earlier shards.
- An `Aborted` transaction cannot reclaim or commit its old locks.
- A release cannot overwrite a newer owner because it carries the exact
  `owned_version`.
- Every finite set of live writers is deadlock-free at the claim layer because
  all multi-shard acquisitions follow the same total order.
- Disjoint batches perform no operation against a common gate path.

## Test and evidence strategy

Add a failing storage-trace regression proving an explicit-ID add never reads
or writes `id-directory/claim-shards/GATE`. Retain the existing concurrent
same-ID, failed-batch release, stale-checkpoint, and aborted-transaction tests.
Add a many-writer disjoint-ID stress case with a start barrier and bounded
completion, then run the focused cell-WAL, concurrency, crash, and fault
suites.

This change is an implementation improvement, not a throughput claim.
Production promotion still requires a frozen 1/8/32/128-writer AWS matrix with
raw batch latency, operations/s, object requests, duplicate-race outcomes, and
fault recovery. The measurement runner must compare the exact gate-free source
against a disclosed control rather than reuse the earlier single-writer
diagnostic.


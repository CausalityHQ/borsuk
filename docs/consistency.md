# Consistency, durability, and multi-node operation

BORSUK is an embedded, object-storage-native engine. It has no server or
coordinator: every guarantee below is a property of how it writes objects to the
bucket. The model is small and easy to reason about, and the guarantees are
covered by `crates/borsuk/tests/consistency.rs`.

## The publication model

Every mutation — `add`, `upsert`, `delete`, `compact`, `purge`, `gc` — produces a
**new, immutable, content-addressed manifest version**. Segment payloads, routing
pages, tombstones, and the manifest table are written to fresh objects *before*
the index makes them visible. Visibility flips in a single step: an atomic swap
of the `CURRENT` pointer to the new manifest version.

```
write new segment/routing/tombstone objects   (invisible; new content-addressed keys)
        │
        ▼
compare-and-swap CURRENT: vN ──► vN+1          (the one linearization point)
```

The swap is a **conditional PUT** of `CURRENT` (if-match on its current
ETag/version). On object stores that support conditional writes — Amazon S3,
Google Cloud Storage, Azure Blob — this is a true compare-and-swap: two writers
racing to publish the next version cannot both win.

## Guarantees

**Atomic snapshot publication.** A reader never observes a half-applied change.
Because all new objects exist before the `CURRENT` swap and the swap is a single
conditional PUT, a manifest version is either fully visible or not visible at
all. `reopen_after_each_step_always_yields_a_consistent_snapshot` opens a fresh
handle after every write and always sees exactly the committed set.

**Snapshot-isolated readers.** A handle resolves `CURRENT` when it opens (and
after its own mutations) and then reads that immutable version's objects. A
concurrent writer publishing newer versions does not disturb an open reader —
it keeps serving its frozen snapshot until it is reopened.
(`readers_are_snapshot_isolated`.)

**Read-your-writes within a writer session.** After a mutation returns on a
handle, that handle points at the new version, so its subsequent reads observe
the write. (`read_your_writes_within_a_writer_session`.)

**Durability.** Nothing lives in the process. Once a mutation returns, its
objects and the swapped `CURRENT` are in the bucket; a dropped handle loses
nothing and a reopened index reflects every committed upsert and delete.
(`state_is_durable_across_reopen`.) Data durability itself is inherited from the
object store's own guarantees (e.g. S3's eleven nines).

**Crash recovery.** A crash mid-publish cannot corrupt the index. New objects are
written before the `CURRENT` swap, so a crash before the swap leaves `CURRENT`
pointing at the last good version — the partially written objects are simply
unreferenced and are reclaimed by `gc`. A crash after the swap has already
committed the new version. There is no write-ahead log to replay and no
half-updated manifest to repair.

**Multi-writer conflict detection.** Two writers that both try to publish the
next version race on the `CURRENT` conditional PUT; the loser receives a
`ConcurrentModification` error rather than silently clobbering the winner. Retry
by reopening (to pick up the winner's version) and reapplying. This requires a
store that honours conditional writes; a store without them degrades to
last-writer-wins, so run a single writer against such backends.

## Native contract (what to build on)

Rather than emulate every vendor's consistency options, BORSUK offers one clear
set of guarantees; adapters translate a vendor's `wait`/consistency flags onto
them and document the differences.

- Atomic snapshot publication (one conditional-PUT linearization point).
- Snapshot-isolated readers.
- Read-your-writes within a writer session.
- Optimistic multi-writer concurrency via `CURRENT` compare-and-swap.

## Multi-node deployment

The design scales out to many processes with no shared service:

- **Many readers.** Point any number of API servers or workers at the same
  bucket. Each opens its own handle, gets a snapshot, and serves reads with
  near-zero resident memory (paged routing). Add read throughput by adding
  stateless processes.
- **Shared cache (optional).** Each process may keep a local SSD read-through
  cache; content-addressed objects are immutable, so cache entries never go
  stale and can be shared or warmed freely.
- **Writers.** A single writer is the simplest and always safe. For multiple
  writers, rely on the `CURRENT` compare-and-swap for conflict detection (on a
  conditional-write store) and retry on `ConcurrentModification`; a lightweight
  external lease can serialize high write rates if desired.
- **No coordinator.** The bucket is the source of truth. There is no metadata
  service to run, scale, or fail over — only the object store.

This is the deployment story behind "bring your own bucket": the same index is
readable by 1 or 100 processes, from anywhere with access to the bucket, with the
control plane being the object store itself.

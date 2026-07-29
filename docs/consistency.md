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
maintenance: compare-and-swap CURRENT vN ──► vN+1
foreground: CAS one transaction-hashed collection frontier HEAD
```

The swap is a **conditional PUT** of `CURRENT` (if-match on its current
ETag/version). On object stores that support conditional writes — Amazon S3,
Google Cloud Storage, Azure Blob — this is a true compare-and-swap: two writers
racing to publish the next version cannot both win.

## Guarantees

**Atomic snapshot publication.** A reader never observes a half-applied change.
Maintenance publishes all new objects before one conditional `CURRENT` PUT.
Foreground writes publish every modality descriptor before one
transaction-hashed collection-frontier CAS replaces a reservation created
before lane preparation. Open/refresh brackets the fixed
frontier double-collect with `CURRENT` reads and retries if the catalog changed,
so it cannot combine a pre-flush base with a post-prune frontier.
`reopen_after_each_step_always_yields_a_consistent_snapshot` opens a fresh
handle after every write and always sees exactly the committed set.

**Snapshot-isolated readers.** A handle pins one coherent catalog/frontier view
when it opens or refreshes and then reads that view's immutable objects. A
concurrent writer publishing newer versions does not disturb an open reader —
it keeps serving its frozen view until explicitly refreshed or reopened.
(`readers_are_snapshot_isolated`.)

**Read-your-writes within a writer session.** After a mutation returns on a
handle, its root-authorized WAL overlay or materialized base contains the
mutation, so subsequent reads observe it.
(`read_your_writes_within_a_writer_session`.)

**Durability.** Nothing lives only in the process. Once a mutation returns, its
immutable objects and collection-frontier entry are in the bucket; maintenance
may already have materialized it under a newer `CURRENT`. A dropped handle
loses nothing and a reopened index reflects every committed upsert and delete.
(`state_is_durable_across_reopen`.)

**Durability and SLA — you inherit the bucket's.** BORSUK is an embedded library,
not a hosted service. It stores no data of its own outside the object store and
runs no always-on tier that could be the weak link in an availability chain. So
the index's durability and availability are, by construction, exactly those of
the bucket you point it at — there is no separate BORSUK SLA to reconcile against
your storage SLA. For **Amazon S3 Standard**, AWS publishes 99.999999999%
(eleven nines) of designed durability and a 99.9% availability service commitment
(designed for 99.99%); other backends publish their own figures —
**GCS Standard** and **Azure Blob (LRS, hot)** likewise document eleven nines of
durability and their own availability SLAs. Whatever store you choose, BORSUK's
numbers are that store's numbers. What BORSUK adds on top is the *correctness*
contract on this page — atomic publication, snapshot isolation, read-your-writes,
and crash recovery — so the bytes the store keeps durable are always a consistent
index, never a half-written one.

**Crash recovery.** A crash mid-publish cannot corrupt the index. New objects are
written before the `CURRENT` swap, so a crash before the swap leaves `CURRENT`
pointing at the last good version — the partially written objects are simply
unreferenced and are reclaimed by `gc`. A crash after the swap has already
committed the new version. BORSUK's write-ahead log follows the same rule: WAL
objects are immutable and content-addressed. Foreground mutations become
visible only when a checked commit pinning every modality descriptor
conditionally replaces its expiring reservation in one of 64 bounded
collection-frontier heads. A crash before that final head CAS leaves an
invisible reservation plus immutable lane objects. After the reservation
expires, GC CAS-removes it and detaches lane runs that have no stable root
authorization. A crash after the final CAS leaves the complete mutation
visible. There is
nothing to *replay* on recovery and no half-updated manifest to repair.

WAL payloads, frontier nodes, descriptors, and WAL-owned BM25 correction pages
carry their root transaction in the object path. GC excludes objects owned by
a live reservation or commit, including uploads that precede their lane-HEAD
CAS. Once an abandoned reservation expires, those transaction-scoped objects
can be reclaimed at the caller's requested `min_age`; there is no coarse
one-hour disk-retention floor. After flush, the materializing manifest retains
the consumed transaction's runs and metadata references. Retained manifest
versions therefore preserve readers pinned before flush for the requested time
since obsolescence, rather than merely measuring the object's older creation
time. The delete pass aborts if `CURRENT` advances after its retained-version
snapshot.

**Multi-writer conflict detection.** Foreground writers CAS-rebase only the
cell-lane and transaction-hashed collection-frontier heads they touch.
Maintenance writers that publish a new indexed base race on the `CURRENT`
conditional PUT; the loser receives a `ConcurrentModification` error rather
than silently clobbering the winner. Retry by reopening (to pick up the
winner's version) and reapplying maintenance. This requires a store that
honours conditional writes; a store without them degrades to last-writer-wins,
so run a single writer against such backends.

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

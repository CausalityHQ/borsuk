# Immutable Pending Commit Log Design

## Status and objective

BORSUK is pre-release, so this design replaces the experimental collection WAL
frontier format instead of retaining a compatibility reader. The objective is
to make a durable group acknowledgement independent of collection-wide
materialization while keeping exact visibility, crash recovery, multi-writer
last-write-wins ordering, recall, and garbage-collection safety.

The terminal v15 campaign is the motivating evidence. At 2K logical cells and
eight writers, 800 visible records required 75,013 S3 requests, write p95 was
350.925 ms, total throughput was 3.513 records/s, and read p95 was 60.942 s.
Ordinary four-record groups used 7--9 requests, while groups that crossed root
pressure used 5,001--30,792. Source tracing found three coupled causes:

- a commit synchronously materializes and prunes the global WAL when one of 64
  mutable frontier shards reaches eight commits;
- readers collect all 64 heads twice and restart the whole snapshot when any
  head changes; and
- every CAS rewrites a shard head containing all of its commits and
  reservations, producing quadratic coordination bytes as a shard fills.

## Alternatives

### Raise pressure thresholds or improve shard selection

Rejected. It postpones the same foreground global operation and cannot cover
the preregistered 5,125 groups per index without either an unbounded read tail
or another amplification cliff.

### Keep mutable heads and move only materialization to a background thread

Rejected as the final architecture. It removes the largest operation from the
acknowledgement path but retains 64-head snapshot amplification, whole-head CAS
rewrites, and difficult distributed capacity accounting. It is useful only as
a diagnostic experiment, not a production format.

### Immutable pending commits with background checkpoints

Selected. Each group publishes one immutable, self-contained commit record.
S3 conditional creation is the only serialized durability boundary for that
group; different writers never rewrite shared commit state. A background
checkpointer folds a stable pending set into immutable segments and publishes
one new collection catalog. Reads discover the bounded delta with one LIST
sequence instead of 128 coordination-object reads.

## Persistent format v3

The collection catalog identifies an immutable write epoch:

```text
collection/CURRENT
collection/snapshots/<checksum>
collection/write-epochs/<epoch>/STATE
collection/write-epochs/<epoch>/pending/<commit-id>.commit
```

`<commit-id>` is globally unique and carries the existing durable
last-write-wins generation. A pending object contains the collection snapshot
checksum and generation, schema fingerprint, transaction ID, modality
descriptors, immutable WAL run references and checksums, and creation time. It
is written with `If-None-Match: *`. All referenced payloads exist and validate
before this PUT. Successful creation is the group durability and visibility
acknowledgement; no manifest build, frontier scan, or pruning runs before the
receipt is returned.

`STATE` contains the epoch ID, schema fingerprint, open/sealed state, and
writer leases. A `GroupCommitWriter` acquires one renewable one-hour epoch
lease, amortized across groups. Schema-changing operations first seal the epoch,
wait for or revoke expired leases, drain every acknowledged pending commit,
publish the new schema/catalog, and open a new epoch. Therefore a commit cannot
be acknowledged into an epoch that a schema change has already finalized.

## Read snapshot protocol

A reader performs:

1. load `CURRENT` and its catalog (`C0`);
2. LIST every page under `C0.write_epoch/pending/` and validate each object;
3. load `CURRENT` again (`C1`);
4. accept the catalog plus pending set only when `C0 == C1`; otherwise retry.

S3's strong read-after-write and LIST consistency make the accepted pending set
a valid point between the two catalog reads. A commit acknowledged before the
LIST begins is present. A concurrent commit may be included or excluded, which
is valid snapshot isolation. A checkpoint changing `CURRENT` forces a retry.
Pending descriptors and WAL runs use checksum-keyed, single-flight bounded
caches. The implementation reports LIST pages, pending commits, pending bytes,
and snapshot retries.

The production target is at most one LIST page in steady state and two pages
at the hard overload boundary. A reader fails closed with an explicit backlog
error rather than performing an unbounded scan if more than 2,000 pending
objects are observed. This is a safety boundary, not the normal flow-control
mechanism.

## Checkpoint and deletion protocol

A checkpointer brackets a pending LIST with `CURRENT` exactly as a reader does,
materializes that immutable set into appropriately sized segments, and attempts
a conditional `CURRENT` publication. The new catalog contains a consumed fence
for every included commit ID. A losing checkpointer discards its unpublished
catalog; immutable duplicate build output is later GC eligible.

Only after the winning `CURRENT` publication may consumed pending objects be
deleted. Before each deletion batch, the checkpointer reloads the winning
catalog and verifies the commit ID is fenced. This ordering protects a reader
that loaded the old catalog: if deletion races its LIST, its second `CURRENT`
read changes and forces a retry. A reader on the new catalog sees the records
in segments and ignores any still-present pending object named by the consumed
fence, preventing duplicates during gradual deletion.

`GroupCommitWriter::drain()` waits until every commit acknowledged through the
call's captured high-water set is fenced by a published catalog. It does not
claim that later concurrent commits are drained. The background checkpointer
runs on pending-count, pending-byte, and maximum-age triggers. Publication
backpressure is honest: when observed backlog reaches 1,000 objects, appends
help or wait for a checkpoint; they never acknowledge and then report failure.
Because multiple processes can publish between observations, the 2,000-object
reader limit remains the fail-closed last line of defence.

## Crash recovery and garbage collection

- A crash before pending-object creation leaves unacknowledged immutable
  payloads. Store-clock-probed age GC may delete them only after the orphan
  grace period and after proving no pending object, catalog, or live build
  lease references them.
- A crash after pending creation leaves a complete authoritative commit that
  readers and checkpointers discover.
- A crash after catalog publication but before pending deletion is harmless:
  the consumed fence suppresses duplicates and deletion resumes later.
- A crash during an unpublished checkpoint leaves content-addressed objects
  reclaimable after the build lease and grace period expire.
- GC never uses a client wall clock as sole authority. It probes the object
  store clock and applies the existing conservative grace/fencing rules.

## API and component boundaries

- `storage.rs` owns `PendingCollectionCommit`, conditional publication,
  bracketed discovery, epoch state/leases, and fenced deletion.
- `index.rs` stages modality payloads, publishes a pending commit, merges a
  catalog with a pending snapshot, and exposes checkpoint/drain operations.
- `group_commit.rs` owns background checkpoint scheduling, backlog
  backpressure, lifecycle joining, and `drain()`.
- The current sharded frontier reservation/commit path is removed with the
  format change; no legacy reader or dual write remains.

## Exact qualification invariants

Development is test-driven. The implementation must prove:

1. a steady-state durable group uses immutable payload PUTs plus exactly one
   pending commit PUT and performs no frontier GET, HEAD, CAS rewrite, manifest
   publication, or foreground flush;
2. two writers publish disjoint commits without shared-object contention and a
   reopened reader observes both;
3. `CURRENT/LIST/CURRENT` retries across a checkpoint race and never loses or
   duplicates a record;
4. catalog publication before deletion and consumed-fence filtering hold under
   injected crashes at every boundary;
5. schema sealing cannot strand an acknowledged old-epoch commit;
6. `drain()` fences exactly its captured acknowledged set;
7. backlog at 1,000 triggers cooperative checkpointing and more than 2,000
   fails reads closed without an unbounded LIST;
8. orphan, losing-build, and consumed-pending GC retain every live object and
   reclaim only store-clock-aged unreachable objects;
9. last-write-wins generations, exact visibility, recall@1, crash, fault,
   consistency, and cell-WAL suites remain green.

A local structural benchmark must demonstrate bounded request counts across a
checkpoint boundary before AWS qualification. The next fresh AWS campaign
keeps the frozen 2K/16K by 1/8/32-writer, five-repetition matrix and gates:
write p95 below 200 ms, at least 5 records/s per writer, read p95 below 200 ms,
and recall@1 of 1.0. It additionally records pending LIST pages, backlog
high-water, checkpoint count/duration/requests, and foreground requests per
record. No result is publication eligible until the entire fresh matrix ends
successfully and passes the fail-closed validator.

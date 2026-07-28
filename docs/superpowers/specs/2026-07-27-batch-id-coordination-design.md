# Batch ID Coordination and Fenced WAL Commit

**Status:** Approved by the user's 2026-07-27 request to fix the diagnosed
explicit-ID ingest bottleneck.

## Context

The v14 WAL-layout campaign proved that explicit-ID ingest performs one
conditional object-store PUT per record before publishing its cell-WAL
transaction. A 5,000-row run emitted exactly 5,000 ID claims plus eight WAL
protocol PUTs per batch. Cell and lane sharding cannot hide that serial
per-record coordination.

BORSUK is unreleased. The repository policy permits replacing this persistent
protocol and rejecting experimental indexes created by earlier versions.

## Decision

Replace persistent per-ID generation-counter claims with routing-independent
batch claim shards. A fixed number of claim shards is derived from the record
ID hash and exists from index creation, independent of whether vector routing
has trained more than the bootstrap cell.

One mutation transaction has a fenced state:

```text
transactions/<transaction-id>/STATE
```

The checked binary state is either:

- `prepared`, with an expiry timestamp;
- `committing`, pinning the immutable transaction descriptor;
- `committed`, pinning the same descriptor; or
- `aborted`.

The state is created before acquiring claims. Transition from `prepared` to
`committing` is a conditional update against the exact state version and is the
writer fence. The existing create-only `COMMIT` marker remains the reader
visibility point, after which the state is best-effort advanced to `committed`.
A transaction that has been conditionally changed to `aborted` can never reach
the marker.

Each claim shard has one checked binary coordination object:

```text
id-directory/claim-shards/<shard>/LOCK
id-directory/claim-shards/GATE
```

It is either available or names the owning transaction. A request hashes and
deduplicates all of its IDs. It briefly owns `GATE` while the touched-shard I/O
executes in parallel, making acquisition all-or-none and preventing circular
wait from partially acquired batches. The gate is released before validation
or WAL preparation. A held claim is reclaimed only after its transaction is
committed, aborted, or its prepared state has expired and the reclaimer
successfully fences it by conditionally changing the state to `aborted`.

After acquiring every shard, the writer refreshes the collection manifest and
cell-WAL snapshot, then performs duplicate/existing-ID validation. A per-handle
version checkpoint may skip this refresh only when every touched shard still
has the version paired with the handle's current snapshot; any external writer
changes the available body's transaction revision and therefore its
content-derived ETag/version. It prepares record, tombstone, and ID-directory
runs, writes one immutable descriptor, fences the state `committing`, and
creates the commit marker. Independent immutable writes and cell/lane
publication groups use the bounded I/O pool. Claim release is a parallel
conditional update using the exact lock versions held by the writer. A crash
after the state transition is
recoverable because another writer completes the marker before reclaiming the
stale shard.

## Alternatives rejected

1. **Parallel per-ID claims.** This reduces elapsed time but retains O(rows)
   requests and cost.
2. **Last-writer-wins `add`.** This removes uniqueness coordination by changing
   insert-only semantics. It does not solve ordered upsert/delete generations
   and makes acknowledged duplicate inserts ambiguous.
3. **Routing-cell claim shards.** A fresh index has one bootstrap vector cell,
   so write concurrency would still depend on training the read topology.

## Data and concurrency boundaries

- Claim-shard count is fixed in the format and does not depend on corpus size.
- A batch performs at most one acquire and one release per touched shard.
- Generated-ID adds do not acquire explicit-ID claim shards.
- Immutable payload and frontier-node uploads remain content-addressed.
- Runs belonging to different cells may be uploaded in parallel after
  validation; the committed transaction state remains the only visibility
  fence and the commit marker remains the visibility boundary.
- Readers ignore prepared runs without a commit marker.
- Readers treat a marker with a missing, mismatched, or corrupt descriptor/run
  as hard corruption.

## Failure behavior

- Failure before state creation publishes nothing.
- Failure while acquiring claims releases only exact versions owned by the
  request and leaves the state aborted.
- Failure while preparing runs leaves invisible content-addressed objects.
- Failure to conditionally commit after fencing returns concurrent
  modification and cannot expose the transaction.
- Crash after `committing` is recovered by completing the idempotent commit
  marker before the owning claim is released.

## Tests and success gates

1. A 500-row explicit-ID add emits a request count bounded by the fixed claim
   shard count, not the row count.
2. Two handles concurrently inserting the same ID yield exactly one committed
   record and one insert error.
3. A failed multi-shard batch releases or fences every acquired shard.
4. An expired prepared transaction is aborted before its claim is reused and
   cannot subsequently commit.
5. Reopen, flush, compaction, delete, upsert, generated IDs, and the full
   feature matrix remain correct.
6. The local and S3 ingest A/B records exact request counts and throughput for
   explicit and generated IDs before production benchmarks resume.

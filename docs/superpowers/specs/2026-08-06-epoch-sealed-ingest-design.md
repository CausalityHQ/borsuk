# Epoch-Sealed Ingest Design

## Status

Approved by the standing pre-release architecture policy and production-readiness
objective. This design replaces the unreleased format-v26 inline lane HEAD; it
does not preserve compatibility with experimental indexes.

## Problem

Format v26 acknowledges one conditional lane-HEAD PUT, but the HEAD embeds every
unmaterialized WAL block. Each later acknowledgement rewrites the earlier vector
payloads. The terminal v31 Cohere 1M/768D cell measured 603.825 ms write p95,
951.618 ms active-tail read p95, 920,705,087 written bytes, and 80,124 write
requests for 3,072,000 input bytes. The source permits 8 MiB of inline payload
before spill and 64 MiB before hard backpressure, despite the campaign's 2 MiB
HEAD bound. Spill also occupies the append worker before it accepts the next
request, and periodic materialization barriers that worker.

The campaign has a separate correctness defect in its performance contract.
Thirty-two writers with four one-record operations in flight expose only 128
outstanding records. Little's Law limits that workload to 640 records/s at
200 ms response time, so it cannot prove a 10,000 records/s target. Production
bulk ingest must measure explicit record batches independently from scalar
latency.

## Decision

Replace inline HEAD publication with epoch-sealed, sequence-addressed immutable
WAL extents. The successful immutable extent PUT is the durability and
acknowledgement boundary. A small lane HEAD owns epochs and publishes an
off-path progress watermark; it never contains vector payload or an unbounded
descriptor list.

The design deliberately rejects two incremental alternatives:

- Background spill with inline HEADs removes worker blocking but retains
  quadratic HEAD rewrites and CAS-serial throughput.
- Immutable extents followed by a synchronous HEAD CAS are simple but require
  two dependent object-store round trips and retain a one-RTT-per-lane ceiling.

## Persistent format v27

For lane `L`, lease epoch `E`, and sequence `S`:

```text
lane-log/lanes/L/HEAD
lane-log/lanes/L/epochs/E/extents/S-<checksum>.wal
```

`HEAD` is a fenced, checksummed control record containing only:

- format version, lane, epoch, owner, and lease expiry;
- the latest asynchronously published contiguous durable sequence;
- the materialized sequence;
- bounded sealed-epoch summaries needed for recovery.

An extent is immutable, self-describing, and contains its lane, epoch, sequence,
generation range, record count, payload checksum, and records. Extent creation
uses create-only semantics. Retrying the same key succeeds only when the stored
checksum matches; a mismatch is a fencing violation.

The initial implementation retains the existing WAL table codec so the protocol
can be qualified independently. If framing overhead prevents the physical-write
amplification gate at qualified group sizes, a subsequent single-factor
experiment may replace it with a fixed-width binary WAL frame.

## Write state machine

1. `Unowned -> Sealing`: acquire `E + 1` by conditional HEAD update.
2. Wait through the prior lease expiry plus the configured skew guard.
3. Enumerate the prior epoch once, seal its maximum valid sequence, and publish
   that bounded recovery fact in HEAD.
4. `Open(E) -> InFlight(E,S)`: reserve sequence `S` locally and submit one
   immutable extent PUT. Different sequences may upload concurrently.
5. Acknowledge only after confirmed creation, or after verifying that an
   already-existing extent has the identical checksum.
6. Recheck the lease deadline after the PUT completes. A completion outside the
   lease guard is not acknowledged and fences the writer.
7. Advance the contiguous durable watermark in memory. Publish HEAD off-path at
   a bounded interval or sequence delta. A stale watermark may increase reader
   discovery work but cannot hide acknowledged extents.

Sequence gaps contain no acknowledged write. Recovery admits checksum-valid
extents within a sealed epoch even when an unacknowledged gap precedes them.
Last-write-wins order is `(epoch, sequence, record_ordinal)`, so a later epoch
always dominates a zombie from an older epoch.

## Read visibility

Readers first fetch the small lane HEAD. Two explicit consistency modes replace
implicit full-HEAD decoding:

- `Committed`: read through the published durable watermark. This is the
  bounded-staleness search default.
- `Linearizable`: additionally probe a bounded sequence window beyond the
  watermark to provide refresh-plus-read visibility for acknowledged writes.

Point reads compute the ownership lane from the ID and read only that lane.
Multi-lane search reads lane metadata in bounded parallelism. Materialization
publishes immutable delta search artifacts and advances `materialized_sequence`;
it does not rebuild the immutable corpus-wide global PQ for ordinary tail
drains.

## Backpressure and failure behavior

Admission checks happen before tickets enter a durable group. Each lane tracks
durable minus materialized bytes, extents, and records:

- the soft bound requests background materialization while continuing to admit;
- the hard bound returns a typed retryable backpressure error;
- a failed watermark update is retried off-path and does not revoke a durable
  extent acknowledgement;
- a persistent extent PUT, fencing, or lease error fails only the affected lane
  and preserves partial-lane receipts;
- drain waits for all captured extent uploads, materializes their exact frontier,
  publishes checkpoints, and returns only after reopen visibility is proven.

No background task may mutate the same HEAD concurrently with its lane owner.
Only the lane owner publishes HEAD; materializers return proposed monotonic
progress for the owner to include in a later CAS.

## Grouping and throughput contract

Hashing records across ownership lanes must not destroy producer-level group
commit. The dispatcher forms record batches before lane fan-out, and every lane
sub-batch can upload concurrently under its own epoch. Scalar and bulk workload
points remain separate:

- Scalar: one record per operation, write p95 below 200 ms; no 10,000 records/s
  claim is inferred.
- Bulk: 16 records per operation, 32 writers, and pipeline depth four expose
  2,048 outstanding records. Both acknowledgement and drain-inclusive throughput
  must reach 10,000 records/s while per-operation write p95 remains below
  200 ms.

The campaign records both operations/s and records/s, raw per-operation batch
length, extent group size, acknowledgement requests, full physical requests,
and input versus written bytes. It rejects a configured concurrency that cannot
mathematically express its target at the latency gate.

## Verification

Implementation proceeds test-first and must prove:

1. Extent PUT success is durable and visible after reopen with a deliberately
   stale watermark.
2. Same-key retry is idempotent; checksum mismatch fails closed.
3. A PUT completing after lease expiry is not acknowledged.
4. Epoch sealing excludes a late zombie while preserving every acknowledged
   prior-epoch extent.
5. HEAD encoded size remains constant as extent count grows.
6. A blocked watermark publisher or materializer does not block extent
   acknowledgement until admission reaches the hard tail bound.
7. Point reads touch one ownership lane; search fan-out is bounded.
8. Scalar, 16-record bulk, multi-lane partial failure, last-write-wins, drain,
   crash recovery, and reopen correctness all pass.
9. A local 768D structural smoke validates raw artifact reconciliation and the
   Little's-Law preflight before any AWS campaign.
10. Five paired AWS repetitions cover 2K/16K logical cells, 1/8/32 writers, and
    1/2/4/8 worker lanes without inspecting incomplete measurement CSVs.

Production defaults remain unfrozen until write p95, acknowledgement throughput,
drain-inclusive throughput, read p95, recall, memory, recovery, and physical
amplification gates pass from one committed revision.

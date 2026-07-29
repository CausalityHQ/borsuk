# Bounded sharded collection WAL frontier

## Goal

Make multimodal foreground visibility collection-atomic without scanning every
logical cell and WAL lane, while keeping writer contention and reader work
bounded under many long-lived writers.

## Implemented design

- Immutable per-cell/lane runs, descriptors, lane heads, and transaction fences
  remain the write, pruning, recovery, and WAL-run GC layout.
- Root visibility uses 64 transaction-hashed conditional objects:

  ```text
  collection/wal-frontier/<shard>/HEAD
  ```

- Each checked packed HEAD contains its generation plus canonical ordered lists
  of expiring reservations and complete collection commits. A commit pins the
  collection snapshot generation, schema fingerprint, and every participating
  modality descriptor.
- Publication order is:
  1. CAS-reserve the transaction in one bounded root HEAD;
  2. publish immutable modality runs, lane heads, and descriptors;
  3. CAS-replace the reservation with the complete collection commit; and
  4. acknowledge the mutation.
- A crash or failed CAS before step 3 grants no visibility. The one-hour
  reservation fences cleanup until expiry; after its CAS removal, GC detaches
  lane runs with no root authorization. A successful CAS exposes every
  modality together.
- WAL runs, frontier nodes, descriptors, and metadata-owned lexical pages use
  transaction-scoped paths. Live root truth protects the interval between
  immutable upload and lane-HEAD publication without a one-hour disk-retention
  floor. A materializing manifest retains consumed run identities so
  superseded manifest retention protects payload and metadata references for
  readers pinned before flush for `min_age` after obsolescence.
- Readers double-collect the 64 HEADs once and project the identical commit map
  into primary, named dense, sparse, text, hybrid, and late-interaction views.
- Cooperative full-tail materialization begins at eight live commits in any
  shard. A hard ceiling of 64 combined reservations and commits per shard
  rejects further visibility admission if maintenance is stalled, so
  open/refresh traversal is bounded.
- Flush and compaction CAS-remove a frontier entry only after every modality has
  consumed it. Consumed-run markers are then compacted from the manifests.
- Commits are embedded in HEAD and rebases overwrite the bounded coordination
  object. There are no immutable collection commits or frontier-node chains, so
  root publication cannot leak immutable history. A failed process can leave
  lane objects behind only behind a bounded reservation; after expiry, ordinary
  GC detaches and deletes them.

## Verification

1. Checked HEAD codec round trips, canonical commit ordering, shard validation,
   deterministic hashing, corruption rejection, and exact soft/hard limits.
2. A forced same-shard test proves the eighth commit refreshes all writer truth,
   materializes the shared tail, prunes the root, and removes consumed markers.
3. A deterministic counter proves open performs exactly two collections of 64
   root HEADs even with named modalities and regardless of logical-cell count.
4. Fault injection proves failed primary and multimodal HEAD publication is
   invisible and failed lane history becomes GC-reclaimable.
5. Explicit flush, direct compaction, paged compaction, and 32-writer
   flush/compaction overlap preserve every committed record.
6. The complete WAL, cell-WAL, named-vector, and fault suites pass on the v18
   embedded-head layout.

## Remaining gates

- Repeat the complete Rust, binding, and packaging gates on the final source.
- Independently review the final diff.
- Verify benchmark cache/scratch cleanup and executable free-disk guards.
- Run a fresh, complete AWS campaign from a new result prefix. Failed or partial
  campaigns remain ineligible for reporting.

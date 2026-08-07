# Multi-Instance Striped WAL Implementation Plan

> **For agentic workers:** use test-driven development and verification before
> completion. BORSUK is pre-release: replace v29 rather than retaining a legacy
> reader or migration path.

**Goal:** Allow independent processes and hosts to sustain concurrent durable
writes to one S3 collection with group-amortized coordination,
bounded recovery, last-write-wins ordering, or read visibility.

**Architecture:** Replace record-ownership lanes with leased writer stripes.
Each process-local commit worker claims one free persisted stripe and appends
create-only immutable extents only to that stripe. Record hashes choose among
the stripes owned by the current `GroupCommitWriter`, preserving per-process
ordering without requiring the same record to use the same stripe on every
host. Readers load a checked active-stripe directory and collect only the named
stripes, then merge by durable global generation, so records written through
different hosts remain visible and last-write-wins. A drain captures every
active stripe but advances only checkpoints
owned by its caller; cooperative foreign-stripe checkpointing is a separate
CAS-safe maintenance step and must not silently steal a live lease.

The striped extent protocol is lane-log format v30. The required active-stripe
directory is v31 and table format v28 rejects indexes created without it. Old
experimental readers are not retained.

## Production invariants

- An acknowledgement reserves one globally ordered generation range and then
  completes one checksum-verified create-only extent before the writer-stripe
  lease guard expires. The range CAS is per group, never per record.
- One live owner per physical stripe; multiple processes coexist by claiming
  distinct stripes. Exhaustion fails explicitly before starting workers.
- A process never needs leases for stripes it does not write.
- Sequential writes of one ID through one writer remain ordered. Writes through
  different processes resolve by the existing globally allocated generation,
  never wall-clock order or stripe number.
- Open and refresh read one checked directory plus the currently active stripe
  set, independent of vector count and extent history. Safe retirement is still
  required before this remains bounded across historical client churn.
- Drain must not checkpoint a foreign live stripe through an owner-only handle.
  Published manifest/delta coverage remains collection-wide and atomic.
- Text, sparse, named dense, and late-interaction writes remain fail-closed until
  the striped materializer supports their complete atomic transaction.
- Benchmarks must distinguish process-local producers from independent library
  instances. Thread counts are not evidence for horizontal writers.

## Task 1: Prove and remove exclusive-all-lanes startup

- [x] Add a RED integration test opening two live `GroupCommitWriter` instances
  on one object store and requiring both acknowledged records after reopen.
- [x] Verify RED is `ConcurrentModification` on `lane-log/lanes/0000/HEAD`.
- [x] Claim one free stripe per configured local worker instead of every
  deterministic record lane; release already claimed stripes if startup cannot
  obtain enough.
- [x] Route each record to a locally owned worker stripe and retain same-ID
  affinity inside one writer.
- [x] Replace per-stripe generations with one conditional, group-amortized
  global range allocator shared by ordinary upsert/delete and group commit;
  prove both stripe orders honor non-overlapping acknowledgement order.
- [x] Replace ownership-lane fanout assertions with writer-stripe invariants.
- [x] Verify independent writers, local grouping, acknowledgement request
  counts, reopen visibility, last-write-wins, and lease exhaustion.

## Task 2: Make drain and recovery stripe-safe

- [x] Add RED coverage where two live writers append, the first drains while the
  second remains active, the second appends again, and reopen returns all values
  exactly once with the latest same-ID generation.
- [x] Capture collection-wide materialization frontiers, but checkpoint only
  caller-owned stripes in the synchronous drain path.
- [x] Add a storage-level conditional foreign checkpoint primitive that merges
  only a monotonic materialized frontier while preserving owner, expiry,
  durable frontier, sealed epoch, and any concurrently published watermark.
- [x] Teach live writers to reconcile a checkpoint-only HEAD version change
  before renewal/watermark publication.
- [ ] Prove crash takeover, late-zombie exclusion, bounded probes, and GC remain
  correct under foreign materialization.

The 2026-08-07 two-instance structural runner exposed and now covers sequential
drains: the first client publishes the collection-wide materialization, then
conditionally checkpoints every captured stripe without changing its owner,
lease, or epoch. A live owner merges that checkpoint-only HEAD update on its
next watermark, renewal, or release. Focused fencing coverage and the full
lane-log/group-commit suites pass; the remaining unchecked item requires the
complete crash/fault/GC assurance gate before this task is closed.

## Task 3: Remove the eight-instance ceiling without fixed-pool read amplification

- [x] Add a versioned checked active-stripe directory so the persisted pool may
  exceed eight while refresh reads only active stripes plus a fixed control
  fanout.
- [x] Activate a successfully claimed stripe before returning its writer, so no
  acknowledged extent can exist outside directory visibility.
- [x] Order claim candidates from one checked directory read, inactive slots
  first with a per-instance randomized rotation, so fresh N-writer startup does
  not scan 1+2+...+N authoritative HEADs or herd on stripe zero.
- [ ] Retire quiescent stripes only behind a manifest-version fence that first
  forces readers pinned before the retirement boundary to refresh. Renewal,
  release, expiry, and takeover must preserve that invariant.
- [ ] Qualify 1, 8, and 32 independent processes against one S3 prefix. Preserve
  raw artifacts, exact per-process receipts, resource telemetry, and terminal
  markers.
- [ ] Reject any design whose point visibility, refresh GETs, or recovery work
  grows with historical writers or vector count.

## Task 4: Complete modality and scale qualification

- [ ] Extend striped group commit atomically across primary dense, named dense,
  sparse, text, and late-interaction payloads before advertising parity.
- [ ] Run typed-vector correctness and recall curves for every persisted element
  type; quantized storage must report accuracy against float ground truth.
- [ ] Run terminal uncached 1M realistic qualification before 100M scale.
- [ ] Run the preregistered 2K/16K logical-cell by 1/8/32 independent-writer
  matrix with five paired repetitions only from the frozen qualifying revision.
- [ ] Promote defaults only when acknowledged and drain-inclusive throughput,
  read p95, recall, resource bounds, crash recovery, and horizontal writer
  correctness all pass.

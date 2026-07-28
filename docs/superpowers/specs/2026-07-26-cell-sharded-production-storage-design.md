# Cell-Sharded Production Storage and Format Qualification

**Status:** Proposed design approved in conversation on 2026-07-26.
**Scope:** Production write topology, immutable storage boundaries, physical
format selection, and the qualification order that precedes large-scale and
publication benchmarks.

## Context

BORSUK is unreleased. As recorded in the repository `AGENTS.md`, backward
compatibility, old on-disk schemas, and legacy APIs must not constrain the
first production architecture. Old indexes may be rejected and rebuilt.

The current implementation already writes one immutable, content-addressed WAL
object per mutation batch. However, every object is appended to one ordered WAL
frontier in one manifest and becomes visible through one `CURRENT`
compare-and-swap. Object uploads do not collide, but concurrent writers based
on the same manifest version contend at publication. One writer wins and the
others must refresh and retry.

Normal segment objects may be Parquet or Vortex, but the current Vortex path
fetches a complete object before applying Vortex projections. The finalized
`srht-pq-scan` production path normally bypasses normal segment tables and uses
global Arrow IPC product-code bundles followed by exact Arrow IPC sidecar
reads. A format decision based only on synthetic table scans or native-reader
microbenchmarks therefore cannot select the production layout.

## Goals

1. Remove collection-wide write serialization for independent writes.
2. Preserve vector-space locality in unflushed data.
3. Keep mutation visibility atomic across every cell touched by one request.
4. Decouple stable write ownership from replaceable physical segments.
5. Permit Parquet, Vortex, Arrow IPC, or a purpose-built packed layout for each
   object role when end-to-end evidence supports it.
6. Freeze production defaults before large-scale, research, or external
   comparison benchmarks.
7. Make every performance result traceable to an exact architecture, source
   archive, configuration, and physical layout.

## Non-goals

- Reading experimental indexes produced by an earlier layout version.
- Preserving current constructor signatures or configuration names.
- Promoting Vortex, Parquet, or any other format from reputation or synthetic
  scans alone.
- Using physical segment identifiers as permanent write-shard identifiers.
- Adding a compatibility layer between the current single-frontier WAL and the
  new layout.

## Architecture

### Collection catalog and routing epochs

The collection catalog changes only when the routing topology changes. It
contains:

- the active routing epoch;
- the immutable coarse-routing descriptor and checksum;
- active and draining logical cell identifiers;
- the WAL lane count for each cell;
- physical-layout policy identifier;
- pointers to each cell's immutable base state.

A logical cell identifier is `(routing_epoch, cell_ordinal)`. It is stable for
the lifetime of that epoch and is not a physical segment ID. Compaction may
replace every physical segment in a cell without changing its logical ID.

The initial bulk-load path may use one bootstrap cell. `finish_bulk_load()`
trains and publishes the normal corpus-size-aware coarse topology, redistributes
the bootstrap data, and freezes routing epoch 1. Online writes remain
searchable during bootstrap. Production benchmark runs begin only after epoch
1 is finalized.

Topology replacement is explicit. A rebuild publishes a new epoch, marks the
old epoch draining, and queries merge both epochs until migration completes.
The first implementation does not perform automatic cell splits during a
foreground write.

### Cell WAL lanes

Each logical cell owns multiple independent append-only WAL lanes:

```text
cells/<epoch>/<cell>/wal/<lane>/runs/<checksum>.<format>
cells/<epoch>/<cell>/wal/<lane>/HEAD
```

The default is eight lanes per cell, configurable at index creation from 1 to
64. A writer chooses a lane by hashing its stable writer ID. If a caller does
not supply a writer ID, the handle creates one UUID for its lifetime.

Every mutation run is immutable and content-addressed. A lane `HEAD` is a
small conditional pointer to a persistent linked frontier. Writers targeting
different lanes never contend. Writers targeting the same lane rebase the new
run onto the latest head and retry its conditional update.

Flush thresholds apply to the aggregate live tail of a cell, retaining the
current starting limits of 64 runs, 16,384 records, or 32 MiB. The
qualification campaign may change these defaults, but it must measure query
tail cost, write throughput, object requests, and flush amplification together.

### Atomic multi-cell mutations

One public add, upsert, or delete request may touch multiple cells. It uses a
unique transaction ID:

1. validate and canonicalize the complete request;
2. route each mutation to its logical cell;
3. write immutable per-cell WAL runs;
4. publish prepared lane-frontier entries with conditional updates;
5. write one immutable transaction descriptor listing every run and expected
   generation;
6. publish one content-addressed commit marker only after every prepared entry
   is reachable.

Readers ignore prepared entries without a valid commit marker. A committed
transaction descriptor makes all its runs visible together. Query refresh
double-collects the selected lane heads and retries if any ETag changes during
snapshot construction. Because all prepared heads exist before the commit
marker, a reader that observes the commit can resolve the complete transaction.

Abandoned prepared entries are harmless and reclaimed by garbage collection
after the configured safety age. Retrying the same idempotency key reuses the
same transaction identity and cannot create duplicate visible mutations.

### Upserts, deletes, and generations

The current routing epoch assigns new vectors by the coarse vector router.
An immutable, hash-partitioned ID ownership directory maps record ID to logical
cell and visible generation. Directory partitions use the same prepared-run
and transaction-commit protocol, so an upsert can atomically:

- publish the new record in its newly routed cell;
- publish a generation tombstone in the previous cell; and
- update ID ownership.

Search merges by record ID and generation. A stale physical copy cannot win
over a newer committed generation. Delete writes a generation tombstone to the
owning cell and updates the directory. Directory repair can be reconstructed
from committed transaction descriptors and is not a source of record payloads.

Generation numbers are allocated from sixteen fixed, routing-independent shard
counters. A mutation batch groups IDs by the same hash used for insert claims,
reserves one monotonic range per touched shard in parallel, and assigns values
in request order. This preserves strictly increasing same-ID generations under
concurrent writers without creating or conditionally updating one object per
record. Unused values after a failed transaction are harmless gaps.

Every immutable run prepared for one `(cell, lane)` is linked to the preceding
frontier in memory and published by one conditional lane-head update. The
transaction descriptor still lists and checksums each typed run independently,
so batching mutable-pointer publication does not couple their physical codecs
or weaken all-runs-or-none visibility.

### Flush and compaction

A cell flush snapshots committed lane frontiers, materializes their records
into one or more immutable physical segments, builds the required sparse,
lexical, late-interaction, filter, and exact-vector sidecars, then conditionally
publishes a new cell base plus consumed-frontier boundary.

New writes continue into later lane heads while flush runs. A flush never
holds a collection-wide lock. Two flushers for the same cell arbitrate through
the cell base pointer; background maintenance leases avoid duplicated work but
correctness rests on conditional publication.

Compaction is cell-local. It replaces physical segments and may change their
format without changing the logical cell ID. Cross-cell reclustering occurs
only through an explicit new routing epoch.

### Query path

The finalized production path remains:

1. route the query through the active and draining routing epochs;
2. scan the selected global/cell product-code chunks;
3. collect a bounded shortlist;
4. exact-rerank from typed sidecars;
5. merge committed WAL-tail records for the selected logical cells;
6. suppress stale generations and maintain global top-k.

Filtered, exact, sparse, BM25, hybrid, and late-interaction queries use the
same committed cell snapshot. They may access different object families, but
cannot observe a different transaction frontier.

Query reports expose, per object family and physical format:

- requests and bytes read;
- cache hits, misses, and repairs;
- decode CPU time;
- rows decoded and rows retained;
- peak transient and retained bytes;
- WAL cells, lanes, runs, and records examined;
- snapshot retries and termination reason.

## Physical-layout policy

No format is globally canonical. The layout policy is resolved when an object
is written and the resolved format is persisted in its reference.

The qualification candidates are:

- Parquet;
- Vortex with a range-aware object-store reader;
- Arrow IPC for fixed-width candidate and exact-value buffers;
- a purpose-built fixed-header packed layout when neither general table format
  matches the access pattern.

The first qualification mixes formats at immutable object boundaries. It does
not duplicate every object in two formats. A dual representation is eligible
only if the measured read saving exceeds its write, storage, and maintenance
amplification.

Initial object roles are evaluated independently:

| Object role | Required access patterns |
|---|---|
| Collection/cell catalog | tiny point read, complete validation |
| WAL run | sequential write, bounded tail scan, projected mutation replay |
| Lane head/commit marker | tiny conditional point write/read |
| Routing pages | small projected point/range reads |
| Normal segment | projected scan, filter, range and occasional full decode |
| Product-code bundle | bounded fixed-width range scans |
| Exact-vector sidecar | sparse fixed-width row takes |
| Filter-index sidecar | metadata predicate lookup and negative pruning |
| Sparse/BM25 blocks | term lookup plus bounded postings/row projections |
| Late-interaction sidecar | entity/token range takes and SIMD MaxSim |
| Tombstone/ID directory | hash lookup, generation scan and compaction |

An `adaptive` policy is not a runtime guess. It is a checked, versioned table
mapping object role and size/schema class to a resolved physical format.
Changing the table changes the layout-policy version and requires fresh
qualification artifacts.

## Qualification order

### Phase 1: Correctness and concurrency

- Prove atomic multi-cell visibility, idempotent retry, lane-CAS recovery,
  snapshot consistency, crash recovery, flush/write overlap, compaction/write
  overlap, and garbage collection.
- Exercise dense, sparse, BM25, hybrid, late-interaction, FP8, binary, upsert,
  delete, reopen, and non-UTF8 IDs.
- Run at least 32 concurrent writers distributed across cells and concentrated
  into one hot cell.

### Phase 2: Access-trace capture

Capture real object accesses from the finalized, delta, filtered, exact,
sparse, BM25, hybrid, and late-interaction paths. The trace records logical
projection, row selection, ranges, object size, cache state, and downstream
materialization boundary.

### Phase 3: Physical-format replay

Replay identical checked traces against fresh objects for each eligible
format. Every arm must produce identical logical Arrow values and downstream
results. Measure local NVMe and S3 separately.

### Phase 4: End-to-end layout arms

Build fresh indexes and compare:

1. fixed Parquet table objects;
2. fixed Vortex where the schema is supported;
3. role-specific mixed layout;
4. mixed layout with range-aware readers;
5. purpose-built packed objects only for roles whose replay justified them.

Measure write throughput, flush and compaction amplification, index size,
requests, transferred bytes, CPU, RSS, cold/disk-cached/warm latency, recall,
and failure rate.

### Phase 5: Default freeze

A non-baseline placement is promoted only when:

- all logical and lifecycle tests pass;
- recall is not lower;
- the paired p95 latency confidence interval is below the baseline for the
  target workload;
- p99, CPU, RSS, request count, and bytes have no release-blocking regression;
- storage and write amplification stay within the limits declared in the
  qualification manifest;
- the win appears on at least two representative datasets;
- the exact layout-policy version and source archive are frozen.

If no candidate passes, the simpler baseline wins that object role.

## Benchmark sequencing

The currently running publication pilot predates this architecture and remains
diagnostic only. It cannot select defaults or support an external superiority
claim.

The required order is:

1. implement and verify the cell-sharded mutation architecture;
2. capture access traces;
3. qualify physical layouts and readers;
4. freeze the production layout policy and query defaults;
5. run large-dataset scale, soak, failure, and cost campaigns;
6. run the confirmatory Amazon S3 Vectors comparison;
7. publish only claims admitted by the frozen statistical decision rule.

No result crosses a routing, WAL, reader, physical-layout, codec, or default
change.

## Failure handling

- A failed WAL-object write publishes no prepared frontier entry.
- A lane-head conflict rebases and retries only that lane.
- A failed multi-cell preparation publishes no commit marker and is invisible.
- A committed transaction with a missing or corrupt run is a hard corruption
  error, never a partial result.
- A failed flush leaves the prior cell base and committed WAL frontier active.
- A failed compaction leaves the prior physical segments active.
- A routing-epoch migration failure leaves both old and new epochs queryable.
- Memory or I/O budget exhaustion returns a typed error; it does not silently
  omit active cells or committed WAL runs.

## Testing boundary

Unit and local integration tests establish logical correctness. S3-compatible
tests establish conditional-write behavior. AWS qualification establishes
performance and resource evidence. Publication benchmarks begin only after all
three layers pass on one exact revision.

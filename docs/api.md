# Experimental native API

BORSUK is unreleased. These Rust modules expose the current native generation
and lifecycle APIs. Read their signatures and fixtures at the same revision as
your checkout; APIs and stored formats can change.

## Build, publish, open, search

- `sq8_source` prepares normalized-source SQ8 records with explicit logical IDs
  and physical order. `two_bit_source::TwoBitSource` builds nomination records.
- `two_bit_build::TwoBitGenerationBuilder` assembles authenticated generation
  metadata. `build_with_semantic_router` binds a prepared semantic router.
- `two_bit_store::publish_two_bit_generation` uploads verified immutable
  metadata and conditionally updates the head. `read_two_bit_head` reads the
  namespace-bound authority used for opening and subsequent publication.
- `two_bit_generation::TwoBitGeneration::open_remote_from_head` opens pinned
  metadata. `search_with_store` selects candidates and scores SQ8 ranges.
- `two_bit_index::TwoBitIndex` exposes logical-ID search over populated or empty
  bases. `open_coordinated` holds a shared lifecycle lock for maintenance.

The search path is semantic directory → selected centroid leaves → two-bit
SOURCE shortlist → SQ8 range scoring. Queries use cosine normalization;
inputs must match the generation dimensions and be finite and nonzero.
Results rank the selected SQ8 candidates; they do not carry an exact
nearest-neighbor guarantee over the original vectors.

## Mutations and maintenance

`two_bit_mutations::apply_two_bit_mutations` conditionally publishes a sorted,
unique batch of updates or tombstones. `read_two_bit_mutations` recovers the
snapshot bound to the head. Pass that snapshot to `TwoBitIndex::search` to
include pending updates and suppress replaced or deleted base IDs before top-k.

`two_bit_compaction::compact_two_bit_index` merges canonical source and pending
mutations into a replacement generation. GC uses an owned, non-cloneable session:

```rust,ignore
let mut session =
    two_bit_gc::TwoBitGcSession::new(store, &prefix, &maintenance_directory)?;
let report = two_bit_gc::collect_two_bit_garbage(&mut session, limits).await?;
```

The constructor is fallible and binds the store, prefix and directory for the
session's lifetime. Reuse that session across capped calls. It retains the SDK's
unordered LIST stream until EOF, with at most one candidate key (up to 1024 bytes)
and its listed size. There is no lexical cursor, seen-set or collected listing.
Every call acquires the lifecycle lock and authenticated write fence and rebuilds
the current keep-set before inspecting entries, including a pending candidate.
Successful caps release the fence and lock so coordinated readers can reopen.

Scan caps count unknown, retained and pending candidates. The delete cap is
checked before consuming the next entry. A candidate that exceeds the remaining
byte budget is retained for another call; `pending_object_bytes` reports its size.
If it exceeds the whole per-call byte cap, `required_deleted_object_bytes` reports
the minimum larger budget needed. Repeating the same insufficient budget cannot
advance past it. Budget stops return successful capped reports and reopen writes;
an insufficient byte cap alone does not leave a durable fence. Reaching a cap
does not establish completion: `scan_complete` requires an actual EOF observation, possibly on the following call. After EOF,
the next call starts a new sweep; late uploads may need that sweep.

Admission, lock and keep-set failures do not advance traversal. A LIST error
discards the stream and reports `restart_required`; the next admitted call
restarts from the prefix. DELETE failure retains the candidate and durable fence,
sets `delete_outcome_unknown`, and excludes that operation from acknowledged
delete counters. Fence-release errors are failures even if all deletions finished;
a transport error can also leave the release outcome uncertain. Retry with the
same session and directory. Neither successful retries nor byte counters prove
physical reclamation in a versioned bucket.

One owned thread and a persistent current-thread Tokio runtime execute accepted
work, with a capacity-one request channel and busy rejection while work is active.
Dropping the awaiting future or its runtime does not cancel accepted work or
release its lock/fence. Dropping the session closes dispatch without joining;
accepted work finishes, then the worker destroys LIST inside its runtime and
shuts down. A dropped waiter loses its report. Store deadlines/retries remain
the caller's responsibility, and a hung operation can keep the worker alive.
Recreating a session or process starts a new traversal safely; progress is not
crash-durable.

The per-call payload admission reserves mutation memory plus 524288 bytes for
metadata, plus the private traversal structure and 1024 bytes for its candidate.
This is not a process memory bound. Separately charge immutable session bindings,
SDK LIST pages/transport buffers, runtime/allocator overhead, and up to three
threads with 2 MiB stacks each (one worker, at most two blocking helpers), including
while idle. The second helper slot permits fresh metadata I/O alongside blocking
LIST work. The pinned `object_store` 0.14.1 filesystem LIST uses blocking reads in
chunks of up to 1024 entries; that SDK buffer is outside the continuation payload.
Its raw `LocalFileSystem` does not support conditional head UPDATE, so filesystem
progress fixtures use real filesystem data/LIST with an in-memory CAS head;
they do not establish production filesystem CAS support.

ALL readers and maintenance must share the trusted lifecycle directory on one
host. Uncoordinated APIs and other hosts are outside this boundary. Lockfiles
must not be replaced, and other actors must not independently release the
worker's fence. GC retains current data, not historical rollback generations.
See [consistency and maintenance](consistency.md) for the coordination boundary.

## Resource admission

`TwoBitGenerationLimits` sets modeled memory, active queries, query scratch,
SOURCE bytes/GETs/concurrency, and SQ8 bytes/GETs/concurrency. Charge other
retained generations and mutation snapshots through `already_pinned_bytes`.
Payload admission is distinct from whole-process RSS; measure runtime,
allocator, and transport overhead separately.

## Code and fixtures

- [Public exports](../crates/borsuk/src/lib.rs)
- [Generation API](../crates/borsuk/src/two_bit_generation.rs)
- [Logical index](../crates/borsuk/src/two_bit_index.rs)
- [Build/publication/open and failure fixtures](../crates/borsuk/tests/two_bit_generation.rs)
- [Application IDs, recovery, compaction and GC fixtures](../crates/borsuk/tests/two_bit_application_ids.rs)
- [Delayed-delete fixtures](../crates/borsuk/tests/two_bit_gc_delayed_delete.rs)

The [Python](../python/README.md) and [TypeScript](../packages/borsuk/README.md)
packages have separate experimental API surfaces. Their examples do not
exercise this native generation path.

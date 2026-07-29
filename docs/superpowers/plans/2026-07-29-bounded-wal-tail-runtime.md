# Bounded collection WAL-tail runtime

## Goal

Make decoded unflushed WAL state independent of the number of logical cells
and concurrent queries before applying routing-aware WAL pruning.

## Invariants

- Immutable WAL runs are decoded once while overlapping callers need them.
- Decoded run retention has a fixed byte cap shared by the primary index and
  every named modality.
- Concurrent decode work has a separate fixed byte cap shared by the complete
  collection.
- A run larger than the decode cap is admitted alone and is not retained when
  it exceeds the cache policy.
- Cache identity includes the immutable object checksum; refresh never serves
  stale decoded data.
- Query, point-read, export, flush, and compaction semantics remain unchanged.

## Tasks

1. Add failing unit tests for collection sharing, byte-bounded eviction, and
   one-load behavior through the WAL-tail runtime.
2. Replace the whole-frontier `wal_tail_cache` with a collection-shared
   runtime containing:
   - per-run `DecodedObjectCache<Vec<VectorRecord>>`;
   - per-run `InFlightReads<Vec<VectorRecord>>`; and
   - a `ByteAdmissionGate` for transient decode bytes.
3. Add explicit open options for retained and in-flight WAL bytes, with
   corpus-independent production defaults.
4. Load each immutable record run through the runtime and concatenate shared
   decoded run values only for the caller that needs the complete tail.
5. Share the same runtime across the root and all named children at create and
   open.
6. Verify targeted WAL, named-vector, concurrency, and memory tests, then the
   complete crate suite and all-target Clippy.

## Follow-on

Once the memory invariant is green, move query routing before WAL materialization
and load only record runs in selected logical cells for bounded approximate
queries. Exact reads, maintenance, export, and point operations continue to
load the complete authorized tail.

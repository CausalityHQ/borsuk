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
mutations into a replacement generation. `two_bit_gc::collect_two_bit_garbage`
performs bounded cleanup under lifecycle coordination and a durable write fence.
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

# Logical-cell write quantizer implementation plan

## Goal

Remove the linear scan over every frozen logical-cell centroid from ordinary
post-finalization ingest while preserving stable WAL ownership semantics.

## Design

Build one lazy HNSW over the immutable logical-cell centroid catalog and cache
it by routing epoch. Small catalogs keep the flat exact path. Larger catalogs
route through the cached graph; physical segment replacement and manifest
version changes do not rebuild it because logical-cell topology is frozen for
the epoch.

## Tasks

- [x] Add a logical-cell quantizer cache to each index handle.
- [x] Route large frozen catalogs through the cache and retain bootstrap/flat
  behavior for empty, malformed, and small catalogs.
- [x] Add tests for cache reuse across manifest-version changes, normalized
  geometry, and stable cell selection.
- [x] Run cell-WAL, WAL, crash, fault, formatting, and Clippy gates.
- [x] Update the hardening audit without claiming throughput improvement.
- [ ] Commit the verified slice and fast-forward push it directly to
  `origin/main`.

## Promotion gate

Run the preregistered flat-versus-quantizer routing matrix at 2K and 16K cells
with 1/8/32 writers. Publish CPU, p50/p95 append latency, throughput, request
counts, routing distribution, and duplicate/fault correctness together.

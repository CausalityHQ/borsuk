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
- [x] Commit the verified slice (`009fcf5`) and fast-forward push it directly
  to `origin/main`.

## Promotion gate

Run the preregistered flat-versus-quantizer routing matrix at 2K and 16K cells
with 1/8/32 writers. Publish CPU, p50/p95 append latency, throughput, request
counts, routing distribution, and duplicate/fault correctness together.

## Benchmark harness checkpoint — 2026-07-31

- [x] Freeze the exact 2K/16K-cell, 1/8/32-writer, five-repetition schedule in
  `docs/research/logical-cell-routing-campaign.json`.
- [x] Add a fail-closed validator that checks the terminal marker before opening
  any CSV, requires every matrix cell and raw append sample, and rejects cohort,
  identity, finiteness, or correctness drift.
- [x] Add negative tests for incomplete campaigns, unequal paired cohorts,
  missing samples, non-finite timings, and failed correctness gates.
- [x] Add a source-identical, open-time flat-routing research control that uses
  the same persisted cell catalog and write path.
- [x] Add the paired flat-control and quantizer runner.
- [x] Run a local smoke that validates structurally but remains ineligible for
  production claims.
- [x] After AWS reauthentication and only after frozen publication v8 is
  terminal, launch fresh immutable v1 prefixes on the dedicated worker. Source
  SHA-256: `ea7322911393bec64f3153328bc412806546047a593f02ea8498dd3ba2564de8`;
  manifest SHA-256:
  `b07a617061245b3f60fe0f40948746fa0c2790c3e042b012a5c0c902e22644d1`.

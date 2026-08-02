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
  terminal, launch on the dedicated worker. Immutable v1 failed before index
  construction or measurement because `BORSUK_ROUTING_SMOKE=0` was interpreted
  as smoke mode. Corrected v2 then failed before index construction or
  measurement because the S3 client lacked the bucket region. Immutable v3
  runs from the identical revision `a4a4dcf` source with explicit region and
  source SHA-256
  `ff62ebb0641e9c115c0600f10eb1428e22d93fdadb37ee10b6b1f003b06bf8ef`
  and unchanged manifest SHA-256
  `b07a617061245b3f60fe0f40948746fa0c2790c3e042b012a5c0c902e22644d1`.

## AWS qualification checkpoint — 2026-08-02

- [x] Preserve v5 as a terminal failed attempt after
  `c2000/r01/w8/flat` reached the preregistered 1,800-second timeout.
- [x] Confirm failure from the terminal marker, campaign log, and resource
  telemetry only; do not inspect partial measurement CSVs or run the
  completion-only validator.
- [x] Record exit 124, 30m00.05s elapsed, 8% aggregate CPU, and 9,400,048
  voluntary context switches without turning partial data into a performance
  claim.
- [x] Add operation-stage, non-measurement progress telemetry that can localize
  remote WAL/object-store waits while retaining the identical paired write
  path. A 30-second stderr heartbeat reports aggregate started/completed
  counters for opens, warmups, routing, and appends plus ready/done writers;
  shutdown emits a final snapshot immediately. The completed local paired
  smoke ended with every counter balanced and remained structurally valid.
- [ ] Qualify a bounded multi-writer remote workload before launching another
  five-repetition 2K/16K by 1/8/32 matrix from a fresh immutable prefix.

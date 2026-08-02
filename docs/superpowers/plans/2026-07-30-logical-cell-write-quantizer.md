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
- [x] Qualify a bounded multi-writer remote workload before launching another
  five-repetition 2K/16K by 1/8/32 matrix from a fresh immutable prefix.
  The separate claim-ineligible diagnostic protocol is now preregistered at
  2K cells, eight writers, two warmups and five measured appends per writer,
  with a 600-second timeout and distinct terminal markers. Its fresh local
  filesystem run completed all 40 measured appends, emitted balanced progress
  counters, preserved raw/resource artifacts, and cannot authorize product
  claims. AWS diagnostic v1 is running from clean revision `ff07610` with
  source SHA-256
  `b6c4ab1f57afd9872e840e3922247a0f9ef8b3406194c1bb272155f30fcb2976`
  and manifest SHA-256
  `c41b1f16bb0871fc6c2ca943220f701520f9b689834574079582f6b3f2452d47`
  on the isolated `c7g.8xlarge` worker. The fresh result/index prefixes and
  exact launch record are preserved in the diagnostic attempt ledger. The
  terminal artifacts are structurally valid, but the gate failed on production
  viability: 40 appends took 21.553 seconds, p50/p95 were 2.718/6.549 seconds,
  and 13,362 storage requests imply about 334 requests per append. Reduce or
  amortize remote write-path request amplification before another full matrix.

## Request-amplification fix — 2026-08-02

- [x] Reproduce the cold-writer amplification locally: one explicit-ID append
  issued 146 GETs, 14 PUTs, and 7 HEADs because duplicate validation refreshed
  the complete 64-shard root frontier.
- [x] Introduce storage format v19, where every WAL mutation advances the
  affected ID-claim shard and an absent/unchanged shard is a durable write
  epoch.
- [x] Preserve insert-only semantics: generated-ID writes invalidate stale
  explicit writers, which refresh and reject a duplicate.
- [x] Bound the optimized cold explicit-ID append below 30 GETs and pass the
  cell-WAL, WAL, crash, fault, and format suites.
- [x] Repeat the bounded eight-writer diagnostic on fresh EC2/S3 prefixes from
  the exact delivered revision; do not promote the full matrix unless request
  count and latency improve materially. Diagnostic v2 is running from clean
  format-v19 revision `a6766b9`, source SHA-256
  `b25e0c1667bd7740d0195fdecfc732b8b694c49dc87515f82076baaa1da99e81`,
  and the unchanged diagnostic manifest on the isolated `c7g.8xlarge` worker.
  Terminal artifacts validate, but the gate failed: 14,656 requests, 1.437
  appends/s, and 7.278-second p95 did not improve on v1. Widen the lazy claim
  shard space to reduce cross-writer invalidations before diagnostic v3.

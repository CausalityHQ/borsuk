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

## Claim-collision fix — 2026-08-02

- [x] Expand format v20 claims from 16 to 4,096 logical epochs using 12 digest
  bits, packed sparsely into 22 lazy coordination pages; retain the independent
  fixed 16-shard generation allocator.
- [x] Cover every shard in a deterministic distribution test and retain stale
  generated-ID versus explicit-ID duplicate rejection.
- [x] Bound the local eight-writer, 40-append analogue below 1,000 GETs and a
  500-record explicit-ID batch below 100 PUTs.
- [x] Run a fresh diagnostic from the exact delivered
  revision and react to terminal request/latency evidence before any full
  matrix launch. Attempt v3 failed before index creation because its service
  environment omitted the AWS region. Attempt v4 is running from the same clean
  format-v20 revision `a311450`, source SHA-256
  `7e63acf53e15f75861cdfd671ca044d68f5a11706a4b7e7b9df826b37ed839b2`,
  and unchanged manifest on the isolated `c7g.8xlarge`, with both AWS region
  variables pinned to `eu-central-1`. Terminal v4 artifacts reconcile all 40
  samples and 6,130 requests with exit status 0; versus v1, requests fall
  54.1%, throughput rises 89.0%, and p95 falls 50.8%. Treat these as directional
  single-repetition diagnostic deltas, not stable effect sizes.
- [x] Stop production attempt v6 after its predecessor showed that the improved
  per-record S3 path was still not production viable. Preserve its explicit
  terminal failure marker and never inspect its partial measurement CSVs.

## Group-commit ingest — 2026-08-02

- [x] Add a cloneable process-local writer that gathers concurrent calls for a
  bounded delay or record count and publishes them through one existing durable
  `BorsukIndex::add` WAL transaction.
- [x] Keep acknowledgement semantics strict: every caller returns only after
  the shared root-visible transaction commits, and all callers receive failure
  if that transaction fails.
- [x] Prove eight concurrent one-record calls become one visible WAL record run,
  survive reopen, and retain all eight records.
- [x] Add a bounded AWS batch/group-commit qualification before freezing a new
  production matrix. Report records/s, caller p50/p95, batch fill, requests per
  record, and read visibility/recall together.
- [x] Add and run the structurally valid local qualification first. Eight
  producers and 160 one-record calls formed 20 groups of exactly eight, retained
  all 160 records after reopen, and achieved exact recall@1 of 1.0 for the 20
  frozen probes. It reached 191.1 records/s locally but still issued 40.525
  requests per record, so AWS launch is rejected until redundant per-cell lane
  and transaction-state publication is removed from the root-authorized path.
- [x] Introduce format v21 root-authorized staging: immutable runs plus one
  descriptor are staged without redundant lane heads or an inner commit marker;
  the collection-root CAS remains the sole ordinary visibility authority.
- [x] Introduce format v22 transaction bundles: one record object and one
  ID-directory object per mutation, with full exact scoring of the bounded live
  tail and physical cell assignment deferred to flush. WAL, upsert, crash,
  fault, format, and cell-WAL gates pass with post-reopen exact recall intact.
- [x] Remove the repeated strict-insert checkpoint refresh amplification.
  Reopened writers now snapshot the 22 packed claim pages while their current
  IDs are fenced, refresh once, and adopt that exact pre-refresh revision set;
  any later mutation changes its shard revision and still forces duplicate
  validation. The identical v22 local qualification improves from 226.2 to
  359.8 records/s, from 30.24 to 16.05 ms p95, and from 29.55 to 11.19 requests
  per record while retaining all 160 records after reopen and exact recall@1
  of 1.0. These are local diagnostic deltas, not product claims.
- [ ] Reduce the remaining root and claim-page coordination writes before AWS
  promotion, or demonstrate with the frozen bounded AWS diagnostic that the
  current 11.19-request/record path meets the production latency checkpoint.
  AWS group-commit diagnostic v6 completed from clean revision `1c5309c`,
  source SHA-256
  `afdffb282d9c8b8cf1f3e65a5acdeead9f6ae9b71c8a22d45c662d1f042a3297`,
  and manifest SHA-256
  `293c573172c5a210e6e759e03fa3ca1625501fbd79ad1fdc404d6cfb002c56eb`
  on the isolated `c7g.8xlarge` worker from fresh immutable prefixes. Its
  fail-closed validation reconciles all 160 records, 20 groups, 1,150 requests,
  complete visibility, and exact recall@1 1.0, but the production gate fails at
  7.732 records/s and 1.173-second p95. Do not launch the full matrix. Attempts
  v1-v5 are terminal infrastructure failures before index creation or
  measurement; their explicit failure markers and causes are preserved in the
  attempt ledger.
- [x] Add a claim-free dense `put` path and route process-local group commit
  through it. One monotonic generation orders a complete group, the generation
  fence is root-atomic with its record bundle, sequential and concurrent
  replacements expose one live version after reopen, and strict `add` retains
  duplicate rejection. Upload the checked descriptor concurrently with its
  immutable payloads, omit the put-only intermediate tombstone object, and skip
  previous-owner lookups for IDs replaced in the same bundle. The identical
  local cell improves from 359.8 to 528.5 records/s and from 11.19 to 2.525
  requests/record with 7.37 ms p95 and exact recall@1 still 1.0; AWS
  qualification remains required before any latency claim.
- [x] Validate AWS group-commit diagnostic v7 from clean revision `16b4ac4`,
  source SHA-256
  `187ffc4b895bf043c7a51c0f0b581cd3319c94eb099d43fb7c79f7fd389b653e`,
  and the unchanged manifest on the isolated `c7g.8xlarge`. Terminal artifacts
  pass the fail-closed validator with all 160 records visible, exact recall@1
  1.0, 20 groups of eight, and 320 reconciled requests. Claim-free group commit
  improves to 27.356 records/s and 307.23 ms p95, but still fails the sub-200 ms
  production gate; do not launch the full matrix.
- [x] Remove the redundant transaction-start `CURRENT` and immutable-snapshot
  reads. The open handle already pins the schema fingerprint and final root
  publication still validates it against current storage. Overlap the
  independent collection-root admission and last-write-wins generation
  reservation. The identical local structural diagnostic remains fully visible
  with exact recall@1 1.0 and reaches 528.5 records/s, 7.29 ms p95, and 2.138
  requests/record. Diagnostic v8 is running from clean revision `d51db4d`,
  source SHA-256
  `0ce5491eff78a74031715f6e208ac255634a8911aaec3abe43290b8a177f3671`,
  and the unchanged manifest on the isolated worker from fresh immutable result
  and index prefixes. Terminal artifacts pass fail-closed validation with all
  160 records visible, exact recall@1 1.0, 260 reconciled requests, 36.820
  records/s, and 387.43 ms p95. Request amplification improves to 1.625 per
  record, but latency still fails the sub-200 ms production gate.
- [x] Reuse the exact post-reservation root version for the uncontended final
  visibility CAS, falling back to read/rebase on actual contention. Read
  `CURRENT` once at final publication and reuse the handle's pinned generation
  while its snapshot checksum is unchanged; only fetch a changed immutable
  snapshot to retain concurrent-maintenance and schema fencing. The identical
  local structural diagnostic remains fully visible with exact recall@1 1.0,
  reaches 533.1 records/s and 6.94 ms p95, and falls from 342 to 286 requests
  (1.788 per record). Diagnostic v9 is running from clean revision `e8aa7bd`,
  source SHA-256
  `9a2de843be30a87e074bbadccb4c14888f56a37932c6c09fbcab5d67beb85f76`,
  and the unchanged manifest on the isolated worker from fresh immutable result
  and index prefixes. Inspect measurements only after a terminal marker and
  fail-closed validation.
